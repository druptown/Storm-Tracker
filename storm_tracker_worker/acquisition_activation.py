"""Closed, shadow-only activation policy for worker-owned acquisition.

The acquisition catalogue says which work can be described.  This module is
the narrower execution authority: every catalogue workload is accounted for,
and only an explicitly audited exact-raw handler can become executable.  It
contains no provider credentials and performs no imports of provider modules.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import os
from types import MappingProxyType
from typing import Any, Mapping, Protocol

from . import worker_acquisition_plan as acquisition


CONTRACT = "stv3-worker-acquisition-activation-v3"
MATRIX_METHOD = "closed_shadow_exact_raw_v3"
MAX_MATRIX_BYTES = 64 * 1024
SHADOW_CAPTURE_TARGET = (
    "storm_tracker_worker.acquisition_handler_child:execute_shadow_capture"
)
# Compatibility name retained for the first candidate's tests and readers.
METAR_TARGET = SHADOW_CAPTURE_TARGET

_ENV_REQUIREMENTS = MappingProxyType({
    "worker_secret.aemet_api_key": "STV3_AEMET_API_KEY",
    "worker_secret.awn_api_key": "STV3_AWN_API_KEY",
    "worker_secret.awn_application_key": "STV3_AWN_APPLICATION_KEY",
    "worker_secret.eumetsat_consumer_key": "STV3_EUMETSAT_CONSUMER_KEY",
    "worker_secret.eumetsat_consumer_secret": "STV3_EUMETSAT_CONSUMER_SECRET",
    "worker_secret.knmi_api_key": "STV3_KNMI_API_KEY",
    "worker_secret.netatmo_access_token": "STV3_NETATMO_ACCESS_TOKEN",
    "worker_secret.synoptic_api_token": "STV3_SYNOPTIC_TOKEN",
})

# These are deliberately not provider credentials.  They are explicit,
# worker-local evidence that the operator has separately reviewed the raw
# retention/cache obligation for a source.  Presence is only accepted when
# the environment value is the exact authority identifier, so a generic
# truthy option cannot silently authorize retention.
_AUTHORIZATION_ENV_REQUIREMENTS = MappingProxyType({
    "worker_authority.awn_private_raw_retention_v1": (
        "STV3_AWN_RAW_RETENTION_AUTHORITY",
        "worker_authority.awn_private_raw_retention_v1",
    ),
    "worker_authority.synoptic_raw_retention_terms_v1": (
        "STV3_SYNOPTIC_RAW_RETENTION_AUTHORITY",
        "worker_authority.synoptic_raw_retention_terms_v1",
    ),
    "worker_authority.rainviewer_cache_attribution_v1": (
        "STV3_RAINVIEWER_CACHE_ATTRIBUTION_AUTHORITY",
        "worker_authority.rainviewer_cache_attribution_v1",
    ),
    "worker_authority.kmi_appv4_raw_retention_v1": (
        "STV3_KMI_APPV4_RAW_RETENTION_AUTHORITY",
        "worker_authority.kmi_appv4_raw_retention_v1",
    ),
    "worker_authority.opera_raw_retention_v1": (
        "STV3_OPERA_RAW_RETENTION_AUTHORITY",
        "worker_authority.opera_raw_retention_v1",
    ),
})
AUTHORIZATION_REQUIREMENT_IDS = frozenset(_AUTHORIZATION_ENV_REQUIREMENTS)
_KNOWN_REQUIREMENT_IDS = frozenset(
    set(acquisition.KNOWN_SECRET_REQUIREMENT_IDS)
    | set(AUTHORIZATION_REQUIREMENT_IDS)
)


def _valid_environment_value(
    value: object,
    *,
    minimum: int = 1,
    maximum: int = 4096,
) -> bool:
    return (
        isinstance(value, str)
        and value == value.strip()
        and minimum <= len(value) <= maximum
        and value.isascii()
        and all(0x21 <= ord(character) < 0x7F for character in value)
    )


class ActivationError(ValueError):
    """The closed activation policy or a job assessment is inconsistent."""


class ActivationStatus(str, Enum):
    ACTIVE_SHADOW_RAW = "active_shadow_raw"
    BLOCKED_UNSUPPORTED = "blocked_unsupported"
    BLOCKED_UNWIRED = "blocked_unwired"


class SecretResolver(Protocol):
    """Worker-local credential presence check; values are never retained."""

    def resolve(self, requirement_id: str) -> str | None: ...


class NullSecretResolver:
    """Fail-closed resolver used when no worker secret source was injected."""

    def resolve(self, requirement_id: str) -> None:
        del requirement_id
        return None


class MappingSecretResolver:
    """Small injectable worker-side resolver, primarily for options adapters."""

    def __init__(self, values: Mapping[str, str]) -> None:
        if not isinstance(values, Mapping):
            raise TypeError("secret resolver values must be a mapping")
        unknown = set(values) - set(_KNOWN_REQUIREMENT_IDS)
        if unknown:
            raise ActivationError("secret resolver contains unknown requirement IDs")
        self._values = dict(values)

    def resolve(self, requirement_id: str) -> str | None:
        return self._values.get(requirement_id)


class EnvironmentSecretResolver:
    """Presence-only resolver for a fixed worker-local environment mapping.

    It deliberately returns a constant marker rather than the credential. The
    native handler reads the same environment variable or inseparable bundle
    inside the fresh child process; no value enters a scheduler job, request
    or diagnostic.
    """

    def __init__(self, environment: Mapping[str, str] | None = None) -> None:
        source = os.environ if environment is None else environment
        if not isinstance(source, Mapping):
            raise TypeError("worker secret environment must be a mapping")
        self._environment = source

    def resolve(self, requirement_id: str) -> str | None:
        authorization = _AUTHORIZATION_ENV_REQUIREMENTS.get(requirement_id)
        if authorization is not None:
            variable, exact_value = authorization
            value = self._environment.get(variable)
            if value != exact_value:
                return None
            return "worker_local_authority_present"
        if requirement_id == "worker_secret.hsaf_ftp_credentials":
            username = self._environment.get("STV3_HSAF_USERNAME")
            password = self._environment.get("STV3_HSAF_PASSWORD")
            if not (
                _valid_environment_value(
                    username, minimum=3, maximum=512
                )
                and _valid_environment_value(
                    password, minimum=8, maximum=4096
                )
            ):
                return None
            return "worker_environment_secret_present"
        if requirement_id == "worker_secret.meteofrance_oauth":
            static = self._environment.get("STV3_METEOFRANCE_API_TOKEN")
            application = self._environment.get(
                "STV3_METEOFRANCE_APPLICATION_ID"
            )
            if isinstance(application, str):
                application = application.removeprefix("Basic ").strip()
            if not (
                _valid_environment_value(
                    static, minimum=16, maximum=8192
                )
                or _valid_environment_value(
                    application, minimum=16, maximum=8192
                )
            ):
                return None
            return "worker_environment_secret_present"
        variable = _ENV_REQUIREMENTS.get(requirement_id)
        if variable is None:
            return None
        value = self._environment.get(variable)
        if requirement_id == "worker_secret.aemet_api_key":
            if not _valid_environment_value(
                value, minimum=16, maximum=4096
            ):
                return None
            return "worker_environment_secret_present"
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 4096
            or not value.isascii()
            or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in value)
        ):
            return None
        return "worker_environment_secret_present"


@dataclass(frozen=True, slots=True)
class ActivationRule:
    workload_id: str
    provider_id: str
    product_id: str
    status: ActivationStatus
    reason_code: str
    handler_id: str | None = None
    process_target: str | None = None
    required_roles: tuple[str, ...] = ()
    dynamic_role_pattern: str | None = None
    dynamic_role_minimum: int = 0
    dynamic_role_maximum: int = 0
    handler_payload_contract: str | None = None
    maximum_transport_bytes: int | None = None
    maximum_artifacts: int | None = None
    maximum_wall_seconds: int | None = None
    expected_scope_id: str | None = None
    expected_satellite_id: str | None = None
    secret_requirement_ids: tuple[str, ...] = ()
    secret_delivery: str = "none"
    authorization_requirement_ids: tuple[str, ...] = ()
    authorization_delivery: str = "none"
    authorization_unavailable_reason_code: str | None = None
    raw_capture_service_only: bool = False

    @property
    def executable(self) -> bool:
        return self.status is ActivationStatus.ACTIVE_SHADOW_RAW

    def as_dict(self) -> dict[str, Any]:
        return {
            "executable": self.executable,
            "handler_id": self.handler_id,
            "handler_payload_contract": self.handler_payload_contract,
            "authorization_delivery": self.authorization_delivery,
            "authorization_requirements": len(
                self.authorization_requirement_ids
            ),
            "authorization_unavailable_reason_code": (
                self.authorization_unavailable_reason_code
            ),
            "maximum_artifacts": self.maximum_artifacts,
            "maximum_transport_bytes": self.maximum_transport_bytes,
            "maximum_wall_seconds": self.maximum_wall_seconds,
            "product_id": self.product_id,
            "provider_id": self.provider_id,
            "reason_code": self.reason_code,
            "required_roles": list(self.required_roles),
            "dynamic_role_pattern": self.dynamic_role_pattern,
            "dynamic_role_minimum": self.dynamic_role_minimum,
            "dynamic_role_maximum": self.dynamic_role_maximum,
            "raw_capture_service_only": self.raw_capture_service_only,
            "satellite_bound": self.expected_satellite_id is not None,
            "secret_delivery": self.secret_delivery,
            "secret_requirements": len(self.secret_requirement_ids),
            "status": self.status.value,
            "workload_id": self.workload_id,
        }


@dataclass(frozen=True, slots=True)
class ActivationDecision:
    rule: ActivationRule
    executable: bool
    reason_code: str
    requirements_total: int
    requirements_available: int
    handler_payload: dict[str, Any] | None
    scope_binding_sha256: str

    def diagnostic(self) -> dict[str, Any]:
        return {
            "executable": self.executable,
            "reason_code": self.reason_code,
            "requirements_available": self.requirements_available,
            "requirements_total": self.requirements_total,
            "scope_binding_sha256": self.scope_binding_sha256,
            "status": self.rule.status.value,
            "workload_id": self.rule.workload_id,
        }


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as err:
        raise ActivationError("activation contract is not canonical JSON") from err


def _catalog_rows() -> tuple[dict[str, Any], ...]:
    snapshot = acquisition.acquisition_catalog_snapshot()
    rows = snapshot.get("workloads")
    if not isinstance(rows, list):
        raise RuntimeError("acquisition catalogue has no workload list")
    return tuple(dict(row) for row in rows)


def _blocked_reason(row: Mapping[str, Any]) -> str:
    workload_id = str(row["workload_id"])
    catalog_reason = row.get("blocked_reason")
    if isinstance(catalog_reason, str) and catalog_reason:
        return catalog_reason
    if workload_id == "radar.opera.dbzh":
        return "raw_capture_rights_and_memory_not_certified"
    return "exact_raw_production_handler_not_runtime_audited"


_RAINVIEWER_ROLES = tuple(
    sorted(
        ("manifest",)
        + tuple(
            f"{kind}_dx_{dx}_dy_{dy}"
            for kind in ("coverage", "radar")
            for dx in ("0", "m1", "p1")
            for dy in ("0", "m1", "p1")
        )
    )
)


def _build_rules() -> MappingProxyType[str, ActivationRule]:
    rules: dict[str, ActivationRule] = {}
    for row in _catalog_rows():
        workload_id = str(row["workload_id"])
        common = {
            "workload_id": workload_id,
            "provider_id": str(row["provider_id"]),
            "product_id": str(row["product_id"]),
        }
        if workload_id == "ground.metar.global":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_raw_shadow_capture_audited",
                handler_id="aviationweather_metar_cache_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("csv_gzip",),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=16 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=45,
                expected_scope_id="global",
            )
        elif workload_id == "radar.aemet.composite":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_multi_role_shadow_capture_audited",
                handler_id="aemet_opendata_three_role_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=(
                    "opendata_data_response",
                    "opendata_envelope_json",
                    "opendata_metadata_response",
                ),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=101 * 1024 * 1024,
                maximum_artifacts=3,
                maximum_wall_seconds=75,
                expected_scope_id="coverage:aemet_es",
                secret_requirement_ids=("worker_secret.aemet_api_key",),
                secret_delivery="worker_environment_presence_v1",
            )
        elif workload_id == "radar.dwd.radolan_rv":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="dedicated_tar_only_shadow_capture_audited",
                handler_id="dwd_rv_tar_exact_raw_only_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("rv_tar",),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=8 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=45,
                expected_scope_id="coverage:dwd_central_europe",
            )
        elif workload_id == "radar.knmi.rtcor":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="native_hdf5_shadow_capture_audited",
                handler_id="knmi_rtcor_hdf5_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("hdf5",),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=40 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=75,
                expected_scope_id="coverage:knmi_nl",
                secret_requirement_ids=("worker_secret.knmi_api_key",),
                secret_delivery="worker_environment_presence_v1",
            )
        elif workload_id == "ground.kmi.aws_10min":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_raw_shadow_capture_audited",
                handler_id="kmi_aws_10min_wfs_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("geoserver_response",),
                handler_payload_contract=(
                    "canonical_previous_ten_minute_window_v1"
                ),
                maximum_transport_bytes=32 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=50,
                expected_scope_id="coverage:kmi_aws_be",
                raw_capture_service_only=True,
            )
        elif workload_id == "radar.kmi.public_wcs_unknown_quantity":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_raw_shadow_capture_audited_unverified_semantics",
                handler_id="kmi_public_radar_wcs_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("geoserver_response",),
                handler_payload_contract=(
                    "canonical_scheduled_slot_timestamp_v1"
                ),
                maximum_transport_bytes=64 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=50,
                expected_scope_id="coverage:kmi_public_wcs_be",
                raw_capture_service_only=True,
            )
        elif workload_id == "radar.kmi.appv4_visual":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code=(
                    "exact_raw_shadow_capture_audited_unverified_visualisation"
                ),
                handler_id="kmi_appv4_visual_exact_raw_only_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("forecast_json", "radar_png"),
                handler_payload_contract="public_region_query_v1",
                maximum_transport_bytes=12 * 1024 * 1024,
                maximum_artifacts=2,
                maximum_wall_seconds=45,
                authorization_requirement_ids=(
                    "worker_authority.kmi_appv4_raw_retention_v1",
                ),
                authorization_delivery="worker_environment_exact_authority_v1",
                authorization_unavailable_reason_code=(
                    "kmi_appv4_raw_retention_contract_approval_required"
                ),
                raw_capture_service_only=True,
            )
        elif workload_id == "model.openmeteo.region":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code=(
                    "exact_raw_shadow_capture_audited_unverified_semantics"
                ),
                handler_id="openmeteo_forecast_unverified_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("response_bytes",),
                handler_payload_contract="openmeteo_target_query_batch_v1",
                maximum_transport_bytes=8 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=60,
                raw_capture_service_only=True,
            )
        elif workload_id == "model.openmeteo.italiameteo_icon2i.region":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code=(
                    "exact_raw_shadow_capture_audited_unverified_semantics"
                ),
                handler_id=(
                    "openmeteo_italiameteo_icon2i_unverified_exact_raw_v1"
                ),
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("response_bytes",),
                handler_payload_contract="public_region_query_v1",
                maximum_transport_bytes=4 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=60,
                raw_capture_service_only=True,
            )
        elif workload_id == "validation.geosphere.location":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code=(
                    "exact_raw_shadow_capture_audited_unverified_semantics"
                ),
                handler_id="geosphere_inca_location_unverified_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("response_bytes",),
                handler_payload_contract="public_region_query_v1",
                maximum_transport_bytes=4 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=60,
                raw_capture_service_only=True,
            )
        elif workload_id == "validation.italiameteo.dpc_sri_historical_catalog":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code=(
                    "exact_raw_shadow_capture_audited_unverified_semantics"
                ),
                handler_id=(
                    "italiameteo_dpc_sri_catalog_unverified_exact_raw_v1"
                ),
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("response_bytes",),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=4 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=60,
                expected_scope_id="global",
                raw_capture_service_only=True,
            )
        elif workload_id == "validation.meteolux.location":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code=(
                    "exact_raw_shadow_capture_audited_unverified_semantics"
                ),
                handler_id="meteolux_location_weather_unverified_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("response_bytes",),
                handler_payload_contract="public_region_query_v1",
                maximum_transport_bytes=4 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=60,
                raw_capture_service_only=True,
            )
        elif workload_id == "radar.dpc.sri":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_multi_role_shadow_capture_audited",
                handler_id="dpc_sri_catalog_geotiff_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("catalog_json", "geotiff"),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=32 * 1024 * 1024,
                maximum_artifacts=2,
                maximum_wall_seconds=75,
                expected_scope_id="coverage:dpc_it",
            )
        elif workload_id == "radar.metoffice.uk_rate":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_multi_role_shadow_capture_audited",
                handler_id="metoffice_uk_rate_listings_odim_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=(
                    "listing_current",
                    "listing_previous",
                    "odim_hdf5",
                ),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=32 * 1024 * 1024,
                maximum_artifacts=3,
                maximum_wall_seconds=90,
                expected_scope_id="coverage:metoffice_uk",
            )
        elif workload_id == "radar.meteofrance.dpradar":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_multi_role_shadow_capture_audited",
                handler_id="meteofrance_dpradar_catalog_hdf5_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("catalog_json", "hdf5"),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=48 * 1024 * 1024,
                maximum_artifacts=2,
                maximum_wall_seconds=90,
                expected_scope_id="coverage:meteofrance_fr",
                secret_requirement_ids=("worker_secret.meteofrance_oauth",),
                secret_delivery="worker_environment_presence_v1",
            )
        elif workload_id == "satellite.hsaf.h40b":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_multi_role_shadow_capture_audited",
                handler_id="hsaf_h40b_nlst_netcdf_gzip_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("listing_nlst", "netcdf_gzip"),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=66 * 1024 * 1024,
                maximum_artifacts=2,
                maximum_wall_seconds=90,
                expected_scope_id="coverage:hsaf_h40b_disk",
                secret_requirement_ids=(
                    "worker_secret.hsaf_ftp_credentials",
                ),
                secret_delivery="worker_environment_presence_v1",
            )
        elif workload_id == "satellite.noaa.err":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_multi_role_shadow_capture_audited",
                handler_id="noaa_err_listing_netcdf4_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("listing_xml", "netcdf4"),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=36 * 1024 * 1024,
                maximum_artifacts=2,
                maximum_wall_seconds=75,
                expected_scope_id="global",
            )
        elif workload_id == "satellite.goes18.rrqpe":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_multi_role_shadow_capture_audited",
                handler_id="noaa_goes18_rrqpe_listings_netcdf4_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=(
                    "listing_current",
                    "listing_previous",
                    "netcdf4",
                ),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=20 * 1024 * 1024,
                maximum_artifacts=3,
                maximum_wall_seconds=105,
                expected_scope_id="coverage:goes18",
                expected_satellite_id="G18",
            )
        elif workload_id == "satellite.goes19.rrqpe":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_multi_role_shadow_capture_audited",
                handler_id="noaa_goes19_rrqpe_listings_netcdf4_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=(
                    "listing_current",
                    "listing_previous",
                    "netcdf4",
                ),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=20 * 1024 * 1024,
                maximum_artifacts=3,
                maximum_wall_seconds=105,
                expected_scope_id="coverage:goes19",
                expected_satellite_id="G19",
            )
        elif workload_id == "lightning.goes18.glm":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_dynamic_role_shadow_capture_audited",
                handler_id="noaa_goes18_glm_listings_netcdf_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("listing_previous", "listing_current"),
                dynamic_role_pattern=r"^netcdf_(?P<index>0[0-9]|1[01])$",
                dynamic_role_minimum=1,
                dynamic_role_maximum=12,
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=68 * 1024 * 1024,
                maximum_artifacts=14,
                maximum_wall_seconds=105,
                expected_scope_id="coverage:goes18",
                expected_satellite_id="G18",
                raw_capture_service_only=True,
            )
        elif workload_id == "lightning.goes19.glm":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_dynamic_role_shadow_capture_audited",
                handler_id="noaa_goes19_glm_listings_netcdf_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("listing_previous", "listing_current"),
                dynamic_role_pattern=r"^netcdf_(?P<index>0[0-9]|1[01])$",
                dynamic_role_minimum=1,
                dynamic_role_maximum=12,
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=68 * 1024 * 1024,
                maximum_artifacts=14,
                maximum_wall_seconds=105,
                expected_scope_id="coverage:goes19",
                expected_satellite_id="G19",
                raw_capture_service_only=True,
            )
        elif workload_id == "lightning.eumetsat.li":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_multi_role_shadow_capture_audited",
                handler_id="eumetsat_mtg_li_search_netcdf_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("search_response", "netcdf4"),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=14 * 1024 * 1024,
                maximum_artifacts=2,
                maximum_wall_seconds=50,
                expected_scope_id="coverage:mtg_fci_disk",
                secret_requirement_ids=(
                    "worker_secret.eumetsat_consumer_key",
                    "worker_secret.eumetsat_consumer_secret",
                ),
                secret_delivery=(
                    "parent_oauth_client_credentials_to_ephemeral_child_bearer_v1"
                ),
                raw_capture_service_only=True,
            )
        elif workload_id == "ground.awn.authorized_devices":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_raw_shadow_capture_audited",
                handler_id="ambientweather_authorized_devices_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("response_json",),
                handler_payload_contract="exact_empty_object_v1",
                maximum_transport_bytes=8 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=45,
                expected_scope_id="global",
                secret_requirement_ids=(
                    "worker_secret.awn_api_key",
                    "worker_secret.awn_application_key",
                ),
                secret_delivery="worker_environment_presence_v1",
                authorization_requirement_ids=(
                    "worker_authority.awn_private_raw_retention_v1",
                ),
                authorization_delivery="worker_environment_exact_authority_v1",
                authorization_unavailable_reason_code=(
                    "authorized_private_raw_retention_approval_required"
                ),
                raw_capture_service_only=True,
            )
        elif workload_id == "ground.netatmo.region":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_raw_shadow_capture_audited",
                handler_id="netatmo_getpublicdata_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("response_json",),
                handler_payload_contract=(
                    "acquisition_query_authority_projection_v1"
                ),
                maximum_transport_bytes=8 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=45,
                secret_requirement_ids=(
                    "worker_secret.netatmo_access_token",
                ),
                secret_delivery="worker_environment_presence_v1",
                raw_capture_service_only=True,
            )
        elif workload_id == "ground.synoptic.region":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_raw_shadow_capture_audited",
                handler_id="synoptic_timeseries_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("response_json",),
                handler_payload_contract=(
                    "acquisition_query_authority_projection_v1"
                ),
                maximum_transport_bytes=16 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=45,
                secret_requirement_ids=("worker_secret.synoptic_api_token",),
                secret_delivery="worker_environment_presence_v1",
                authorization_requirement_ids=(
                    "worker_authority.synoptic_raw_retention_terms_v1",
                ),
                authorization_delivery="worker_environment_exact_authority_v1",
                authorization_unavailable_reason_code=(
                    "provider_raw_retention_terms_approval_required"
                ),
                raw_capture_service_only=True,
            )
        elif workload_id == "radar.rainviewer.visual":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_raw_shadow_capture_audited",
                handler_id="rainviewer_visual_region_exact_raw_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=_RAINVIEWER_ROLES,
                handler_payload_contract=(
                    "acquisition_query_authority_projection_v1"
                ),
                maximum_transport_bytes=38 * 1024 * 1024,
                maximum_artifacts=19,
                maximum_wall_seconds=75,
                authorization_requirement_ids=(
                    "worker_authority.rainviewer_cache_attribution_v1",
                ),
                authorization_delivery="worker_environment_exact_authority_v1",
                authorization_unavailable_reason_code=(
                    "api_rights_attribution_cache_approval_required"
                ),
                raw_capture_service_only=True,
            )
        elif workload_id == "radar.opera.dbzh":
            rule = ActivationRule(
                **common,
                status=ActivationStatus.ACTIVE_SHADOW_RAW,
                reason_code="exact_raw_shadow_capture_audited",
                handler_id="opera_dbzh_hdf5_exact_raw_only_v1",
                process_target=SHADOW_CAPTURE_TARGET,
                required_roles=("hdf5",),
                handler_payload_contract="canonical_scheduled_slot_timestamp_v1",
                maximum_transport_bytes=384 * 1024 * 1024,
                maximum_artifacts=1,
                maximum_wall_seconds=300,
                expected_scope_id="coverage:opera_europe",
                authorization_requirement_ids=(
                    "worker_authority.opera_raw_retention_v1",
                ),
                authorization_delivery=(
                    "worker_environment_exact_authority_v1"
                ),
                authorization_unavailable_reason_code=(
                    "provider_raw_retention_approval_required"
                ),
                raw_capture_service_only=True,
            )
        else:
            status = (
                ActivationStatus.BLOCKED_UNSUPPORTED
                if row["status"]
                == acquisition.CapabilityStatus.UNSUPPORTED_FAIL_CLOSED.value
                else ActivationStatus.BLOCKED_UNWIRED
            )
            rule = ActivationRule(
                **common,
                status=status,
                reason_code=_blocked_reason(row),
            )
        if workload_id in rules:
            raise RuntimeError("activation matrix contains a duplicate workload")
        rules[workload_id] = rule
    catalogue_ids = {str(row["workload_id"]) for row in _catalog_rows()}
    if set(rules) != catalogue_ids:
        raise RuntimeError("activation matrix is not catalogue-complete")
    return MappingProxyType(rules)


ACTIVATION_RULES = _build_rules()
DEFAULT_SHADOW_WORKLOAD_IDS = frozenset(
    workload_id
    for workload_id, rule in ACTIVATION_RULES.items()
    if rule.executable and not rule.raw_capture_service_only
)
RAW_CAPTURE_WORKLOAD_IDS = frozenset(
    workload_id
    for workload_id, rule in ACTIVATION_RULES.items()
    if rule.executable
)


# Audited but deliberately absent from ``ACTIVATION_RULES``.  The autonomous
# runtime does not yet carry a query authority into its child request, so
# placing these rules in the live shadow matrix would turn an isolated batch
# into implicit runtime wiring.  The dedicated candidate assessor below is the
# only activation path until that later integration is separately reviewed.
LOCATION_SHADOW_CANDIDATE_RULES = MappingProxyType({
    "ground.awn.authorized_devices": ActivationRule(
        workload_id="ground.awn.authorized_devices",
        provider_id="ambientweather",
        product_id="awn_authorized_devices_native_json",
        status=ActivationStatus.BLOCKED_UNWIRED,
        reason_code="authorized_private_raw_retention_approval_required",
        handler_id="ambientweather_authorized_devices_exact_raw_v1",
        process_target=SHADOW_CAPTURE_TARGET,
        required_roles=("response_json",),
        handler_payload_contract="exact_empty_object_v1",
        maximum_transport_bytes=8 * 1024 * 1024,
        maximum_artifacts=1,
        maximum_wall_seconds=45,
        expected_scope_id="global",
        secret_requirement_ids=(
            "worker_secret.awn_api_key",
            "worker_secret.awn_application_key",
        ),
        secret_delivery="worker_environment_presence_v1",
    ),
    "ground.netatmo.region": ActivationRule(
        workload_id="ground.netatmo.region",
        provider_id="netatmo",
        product_id="netatmo_getpublicdata_native_json",
        status=ActivationStatus.BLOCKED_UNWIRED,
        reason_code="worker_oauth_access_token_authority_unavailable",
        handler_id="netatmo_getpublicdata_exact_raw_v1",
        process_target=SHADOW_CAPTURE_TARGET,
        required_roles=("response_json",),
        handler_payload_contract="acquisition_query_authority_projection_v1",
        maximum_transport_bytes=8 * 1024 * 1024,
        maximum_artifacts=1,
        maximum_wall_seconds=45,
        secret_requirement_ids=("worker_secret.netatmo_access_token",),
        secret_delivery="worker_environment_presence_v1",
    ),
    "ground.synoptic.region": ActivationRule(
        workload_id="ground.synoptic.region",
        provider_id="synoptic",
        product_id="synoptic_timeseries_native_json",
        status=ActivationStatus.BLOCKED_UNWIRED,
        reason_code="provider_raw_retention_terms_approval_required",
        handler_id="synoptic_timeseries_exact_raw_v1",
        process_target=SHADOW_CAPTURE_TARGET,
        required_roles=("response_json",),
        handler_payload_contract="acquisition_query_authority_projection_v1",
        maximum_transport_bytes=16 * 1024 * 1024,
        maximum_artifacts=1,
        maximum_wall_seconds=45,
        secret_requirement_ids=("worker_secret.synoptic_api_token",),
        secret_delivery="worker_environment_presence_v1",
    ),
    "radar.rainviewer.visual": ActivationRule(
        workload_id="radar.rainviewer.visual",
        provider_id="rainviewer",
        product_id="rainviewer_unsmoothed_tiles_with_coverage",
        status=ActivationStatus.BLOCKED_UNWIRED,
        reason_code="api_rights_attribution_cache_approval_required",
        handler_id="rainviewer_visual_region_exact_raw_v1",
        process_target=SHADOW_CAPTURE_TARGET,
        required_roles=_RAINVIEWER_ROLES,
        handler_payload_contract="acquisition_query_authority_projection_v1",
        maximum_transport_bytes=38 * 1024 * 1024,
        maximum_artifacts=19,
        maximum_wall_seconds=75,
    ),
})


def assess_location_shadow_candidate(
    job: acquisition.AcquisitionJob,
    *,
    resolver: SecretResolver,
    handler_payload: dict[str, Any],
) -> ActivationDecision:
    """Assess one inert ground/visual candidate without live-matrix promotion."""

    try:
        valid = acquisition.validate_worker_acquisition_job(job)
        rule = LOCATION_SHADOW_CANDIDATE_RULES[valid.workload_id]
    except (acquisition.AcquisitionPlanError, KeyError) as err:
        raise ActivationError("job valt buiten de locatie-shadowbatch") from err
    if (
        valid.provider_id != rule.provider_id
        or valid.product_id != rule.product_id
        or valid.satellite_id is not None
        or valid.secret_requirement_ids != rule.secret_requirement_ids
        or valid.budget.max_transport_bytes != rule.maximum_transport_bytes
        or valid.budget.max_artifacts != rule.maximum_artifacts
        or valid.budget.max_wall_seconds != rule.maximum_wall_seconds
    ):
        raise ActivationError("locatie-shadowjob driftte van provider/product/budget")
    if rule.expected_scope_id == "global":
        if valid.scope_id != "global" or not valid.region_ids or not valid.target_ids:
            raise ActivationError("globale shadowjob mist zijn volledige scope")
    elif (
        len(valid.region_ids) != 1
        or valid.scope_id != valid.region_ids[0]
        or not valid.target_ids
    ):
        raise ActivationError("regionale shadowjob mist exact een gebonden regio")
    if type(handler_payload) is not dict:
        raise ActivationError("locatie-shadowpayload is niet exact een object")
    if rule.handler_payload_contract == "exact_empty_object_v1":
        if handler_payload:
            raise ActivationError("globale shadowpayload moet exact leeg zijn")
    elif not handler_payload:
        raise ActivationError("regionale shadowpayload ontbreekt")
    scope_payload = {
        "country_codes": list(valid.country_codes),
        "region_ids": list(valid.region_ids),
        "scope_id": valid.scope_id,
        "target_ids": list(valid.target_ids),
    }
    scope_sha = hashlib.sha256(_canonical(scope_payload)).hexdigest()
    available = _requirements_available(valid.secret_requirement_ids, resolver)
    total = len(valid.secret_requirement_ids)
    if not rule.executable:
        return ActivationDecision(
            rule, False, rule.reason_code, total, available, None, scope_sha,
        )
    if available != len(job.secret_requirement_ids):
        return ActivationDecision(
            rule, False, "secret_requirement_unavailable", total, available, None,
            scope_sha,
        )
    return ActivationDecision(
        rule, True, rule.reason_code, total, available, dict(handler_payload), scope_sha,
    )


def activation_matrix_snapshot() -> dict[str, Any]:
    """Return bounded policy facts, never handler paths or secret values."""

    rows = [ACTIVATION_RULES[key].as_dict() for key in sorted(ACTIVATION_RULES)]
    payload = {
        "active_shadow_raw": sum(row["executable"] for row in rows),
        "catalog_sha256": acquisition.CATALOG_SHA256,
        "contract": CONTRACT,
        "method": MATRIX_METHOD,
        "rules": rows,
        "workload_count": len(rows),
    }
    encoded = _canonical(payload)
    if len(encoded) > MAX_MATRIX_BYTES:
        raise ActivationError("activation matrix exceeds its diagnostic bound")
    payload["matrix_sha256"] = hashlib.sha256(encoded).hexdigest()
    return payload


def _requirements_available(
    requirement_ids: tuple[str, ...], resolver: SecretResolver
) -> int:
    available = 0
    for requirement_id in requirement_ids:
        if requirement_id not in _KNOWN_REQUIREMENT_IDS:
            raise ActivationError("job contains an unknown secret requirement")
        try:
            value = resolver.resolve(requirement_id)
        except Exception:
            value = None
        if value is None:
            continue
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 16 * 1024
            or "\x00" in value
        ):
            continue
        available += 1
        # Deliberately do not retain, hash, log or transport the value.
        value = None
    return available


def _bind_handler_payload(
    job: acquisition.AcquisitionJob,
    rule: ActivationRule,
) -> dict[str, Any] | None:
    """Validate every declared rule binding, including blocked candidates."""

    if rule.handler_payload_contract is None:
        if rule.executable:
            raise ActivationError("active handler has no payload contract")
        return None
    if (
        job.secret_requirement_ids != rule.secret_requirement_ids
        or job.satellite_id != rule.expected_satellite_id
        or job.budget.max_transport_bytes != rule.maximum_transport_bytes
        or job.budget.max_artifacts != rule.maximum_artifacts
        or job.budget.max_wall_seconds != rule.maximum_wall_seconds
    ):
        raise ActivationError("handler scope, secret contract or budget is not exact")
    if rule.expected_scope_id is not None and job.scope_id != rule.expected_scope_id:
        raise ActivationError("handler scope, secret contract or budget is not exact")
    if job.scope_id.startswith("coverage:") and (
        not job.region_ids or not job.target_ids
    ):
        raise ActivationError("coverage handler has no bound region or target")
    if rule.handler_payload_contract == "exact_empty_object_v1":
        return {}
    if rule.handler_payload_contract in {
        "acquisition_query_authority_projection_v1",
        "openmeteo_target_query_batch_v1",
        "public_region_query_v1",
    }:
        if (
            rule.expected_scope_id is not None
            or len(job.region_ids) != 1
            or job.scope_id != job.region_ids[0]
            or not job.target_ids
        ):
            raise ActivationError("location handler has no exact regional scope")
        # Coordinates are materialized only by acquisition_location_shadow's
        # guarded authority projection.  This empty marker is never a valid
        # native location payload and therefore fails closed if that guard is
        # accidentally bypassed.
        return {}
    if (
        rule.handler_payload_contract
        == "canonical_previous_ten_minute_window_v1"
    ):
        timestamp = job.scheduled_for
        if (
            not math.isfinite(timestamp)
            or not timestamp.is_integer()
            or int(timestamp) % 600 != 0
        ):
            raise ActivationError("canonical ten-minute slot binding is invalid")
        return {
            "window_end": timestamp,
            "window_start": timestamp - 600.0,
        }
    if rule.handler_payload_contract == "canonical_scheduled_slot_timestamp_v1":
        timestamp = job.scheduled_for
        if (
            not math.isfinite(timestamp)
            or not timestamp.is_integer()
            or int(timestamp) % 300 != 0
        ):
            raise ActivationError("canonical five-minute slot binding is invalid")
        return {"product_timestamp": timestamp}
    raise ActivationError("declared handler payload contract is not implemented")


def assess_acquisition_job(
    job: acquisition.AcquisitionJob,
    *,
    resolver: SecretResolver,
) -> ActivationDecision:
    """Bind one exact job to the closed matrix and a safe handler payload."""

    if not isinstance(job, acquisition.AcquisitionJob):
        raise ActivationError("activation requires an AcquisitionJob")
    try:
        job = acquisition.validate_worker_acquisition_job(job)
    except acquisition.AcquisitionPlanError as err:
        raise ActivationError("job identity or scope binding is invalid") from err
    try:
        rule = ACTIVATION_RULES[job.workload_id]
    except KeyError as err:
        raise ActivationError("job workload is absent from the closed matrix") from err
    if rule.provider_id != job.provider_id or rule.product_id != job.product_id:
        raise ActivationError("job provider/product drifted from activation matrix")
    handler_payload = _bind_handler_payload(job, rule)
    scope_payload = {
        "country_codes": list(job.country_codes),
        "region_ids": list(job.region_ids),
        "satellite_id": job.satellite_id,
        "scope_id": job.scope_id,
        "target_ids": list(job.target_ids),
    }
    scope_sha = hashlib.sha256(_canonical(scope_payload)).hexdigest()
    available = _requirements_available(job.secret_requirement_ids, resolver)
    authorization_available = _requirements_available(
        rule.authorization_requirement_ids,
        resolver,
    )
    total = len(job.secret_requirement_ids) + len(
        rule.authorization_requirement_ids
    )
    combined_available = available + authorization_available
    if authorization_available != len(rule.authorization_requirement_ids):
        if rule.authorization_unavailable_reason_code is None:
            raise ActivationError(
                "authorization requirement has no fail-closed reason"
            )
        return ActivationDecision(
            rule,
            False,
            rule.authorization_unavailable_reason_code,
            total,
            combined_available,
            None,
            scope_sha,
        )
    if available != len(job.secret_requirement_ids):
        return ActivationDecision(
            rule,
            False,
            "secret_requirement_unavailable",
            total,
            combined_available,
            None,
            scope_sha,
        )
    if not rule.executable:
        return ActivationDecision(
            rule,
            False,
            rule.reason_code,
            total,
            combined_available,
            None,
            scope_sha,
        )
    if handler_payload is None:
        raise ActivationError("active handler has no payload binding")
    return ActivationDecision(
        rule,
        True,
        rule.reason_code,
        total,
        combined_available,
        handler_payload,
        scope_sha,
    )


__all__ = [
    "ACTIVATION_RULES",
    "AUTHORIZATION_REQUIREMENT_IDS",
    "LOCATION_SHADOW_CANDIDATE_RULES",
    "CONTRACT",
    "DEFAULT_SHADOW_WORKLOAD_IDS",
    "ActivationDecision",
    "ActivationError",
    "ActivationRule",
    "ActivationStatus",
    "EnvironmentSecretResolver",
    "MappingSecretResolver",
    "NullSecretResolver",
    "RAW_CAPTURE_WORKLOAD_IDS",
    "SecretResolver",
    "SHADOW_CAPTURE_TARGET",
    "activation_matrix_snapshot",
    "assess_acquisition_job",
    "assess_location_shadow_candidate",
]
