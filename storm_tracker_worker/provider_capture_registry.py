"""Closed migration registry for provider-native capture and vNext admission.

This registry prevents an adapter from silently being forgotten during the
worker-only migration.  It is an audit/control-plane fact, not a provider
priority list.  A provider may be fetched for shadow comparison while still
being explicitly barred from vNext meteorological admission.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Iterable


METHOD = "worker_provider_capture_registry_v1"


class CaptureStatus(str, Enum):
    EXACT_RAW_CONNECTED = "exact_raw_connected"
    LEGACY_ARTIFACT_ONLY = "legacy_artifact_only"
    RESULT_ONLY = "result_only"


class ProductSemantics(str, Enum):
    QUANTITATIVE_RADAR = "quantitative_radar"
    CATEGORICAL_VISUALISATION = "categorical_visualisation"
    SATELLITE_PRECIPITATION = "satellite_precipitation"
    LIGHTNING = "lightning"
    GROUND_OBSERVATION = "ground_observation"
    MODEL_GUIDANCE = "model_guidance"


@dataclass(frozen=True, slots=True)
class ProviderCapturePolicy:
    job_type: str
    provider_id: str
    capture_status: CaptureStatus
    product_semantics: ProductSemantics
    exact_native_bytes_complete: bool
    vnext_operational_admission: bool
    reason_code: str
    required_roles: tuple[str, ...] = ()
    candidate_handler: str | None = None
    candidate_exact_raw_tested: bool = False
    candidate_required_roles: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.job_type.endswith("_fetch"):
            raise ValueError("providerjob moet op _fetch eindigen")
        if self.exact_native_bytes_complete != (
            self.capture_status is CaptureStatus.EXACT_RAW_CONNECTED
        ):
            raise ValueError("capturestatus en exact-native vlag spreken elkaar tegen")
        if self.vnext_operational_admission and not self.exact_native_bytes_complete:
            raise ValueError("vNext-admission vereist complete exacte raw bytes")
        if self.vnext_operational_admission and not self.required_roles:
            raise ValueError("vNext-admission vereist een expliciete memberset")
        if tuple(sorted(set(self.required_roles))) != self.required_roles:
            raise ValueError("required_roles moeten uniek en gesorteerd zijn")
        if tuple(sorted(set(self.candidate_required_roles))) != (
            self.candidate_required_roles
        ):
            raise ValueError(
                "candidate_required_roles moeten uniek en gesorteerd zijn"
            )
        if self.candidate_exact_raw_tested != bool(self.candidate_handler):
            raise ValueError(
                "geteste raw-kandidaat vereist precies één kandidaat-handler"
            )
        if self.candidate_exact_raw_tested and not self.candidate_required_roles:
            raise ValueError("geteste raw-kandidaat vereist een memberset")
        if self.candidate_handler is not None and (
            len(self.candidate_handler) > 256
            or ":" not in self.candidate_handler
            or any(
                character not in (
                    "abcdefghijklmnopqrstuvwxyz"
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:"
                )
                for character in self.candidate_handler
            )
        ):
            raise ValueError("kandidaat-handler is geen veilige importreferentie")
        if (
            self.capture_status is CaptureStatus.EXACT_RAW_CONNECTED
            and self.candidate_handler is not None
        ):
            raise ValueError("actieve exact-raw capture mag geen losse kandidaat houden")

    def to_dict(self) -> dict[str, object]:
        return {
            "job_type": self.job_type,
            "provider_id": self.provider_id,
            "capture_status": self.capture_status.value,
            "product_semantics": self.product_semantics.value,
            "exact_native_bytes_complete": self.exact_native_bytes_complete,
            "vnext_operational_admission": self.vnext_operational_admission,
            "reason_code": self.reason_code,
            "required_roles": list(self.required_roles),
            "candidate_handler": self.candidate_handler,
            "candidate_exact_raw_tested": self.candidate_exact_raw_tested,
            "candidate_required_roles": list(self.candidate_required_roles),
        }


def _policy(
    job_type: str,
    provider_id: str,
    status: CaptureStatus,
    semantics: ProductSemantics,
    reason_code: str,
    *,
    admission: bool = False,
    required_roles: tuple[str, ...] = (),
    candidate_handler: str | None = None,
    candidate_exact_raw_tested: bool = False,
    candidate_required_roles: tuple[str, ...] = (),
) -> ProviderCapturePolicy:
    return ProviderCapturePolicy(
        job_type=job_type,
        provider_id=provider_id,
        capture_status=status,
        product_semantics=semantics,
        exact_native_bytes_complete=(
            status is CaptureStatus.EXACT_RAW_CONNECTED
        ),
        vnext_operational_admission=admission,
        reason_code=reason_code,
        required_roles=required_roles,
        candidate_handler=candidate_handler,
        candidate_exact_raw_tested=candidate_exact_raw_tested,
        candidate_required_roles=candidate_required_roles,
    )


# This registry reports the legacy-live capture path, not the newer acquisition
# activation matrix.  In the legacy path only KMI appv4 and DWD RV currently
# reach the exact-byte store.  Separately, acquisition_activation.py has eleven
# locally bound ACTIVE_SHADOW_RAW rules (including H SAF); none is wired by the
# default worker service.  KMI appv4 has no certified quantitative semantics.
# DWD RV is a quantitative source product, but legacy component/intensity and
# fixed-quality assumptions remain uncertified.  Exact provenance alone never
# promotes a decoded result to operational radar evidence, and a tested
# candidate below must not be confused with legacy-live connectivity.
_POLICIES = (
    _policy(
        "aemet_radar_fetch", "aemet_radar",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.QUANTITATIVE_RADAR,
        "native_capture_not_connected",
    ),
    _policy(
        "dpc_radar_fetch", "dpc_radar",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.QUANTITATIVE_RADAR,
        "native_capture_not_connected",
        # Isolated DPC Radar v2 candidate retains the exact official SRI
        # catalog response and selected original GeoTIFF. The transient AWS
        # presigned URL is validated in memory and deliberately not retained.
        candidate_handler=(
            "storm_tracker_worker.dpc_native:capture_latest_dpc_sri"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=("catalog_json", "geotiff"),
    ),
    _policy(
        "dwd_radolan_fetch", "dwd_radolan",
        CaptureStatus.EXACT_RAW_CONNECTED,
        ProductSemantics.QUANTITATIVE_RADAR,
        "exact_raw_rv_tar_but_legacy_component_decoder_uncertified",
        admission=False,
        required_roles=("rv_tar",),
    ),
    _policy(
        "enterprise_rainrate_fetch", "noaa_enterprise_rainrate",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.SATELLITE_PRECIPITATION,
        "native_capture_not_connected",
        # Isolated candidate retains exact S3 listing XML plus the selected
        # final GLB-5 NetCDF-4 object. It is not a decoded/admitted source.
        candidate_handler=(
            "storm_tracker_worker.noaa_err_native:"
            "capture_latest_noaa_err_netcdf4"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=("listing_xml", "netcdf4"),
    ),
    _policy(
        "eumetsat_li_fetch", "eumetsat_li",
        CaptureStatus.RESULT_ONLY,
        ProductSemantics.LIGHTNING,
        "native_capture_not_connected",
    ),
    _policy(
        "goes_glm_fetch", "noaa_goes_glm",
        CaptureStatus.RESULT_ONLY,
        ProductSemantics.LIGHTNING,
        "native_capture_not_connected",
    ),
    _policy(
        "goes_rrqpe_fetch", "noaa_goes_rrqpe",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.SATELLITE_PRECIPITATION,
        "native_capture_not_connected",
        # Isolated candidate retains the exact previous/current UTC-hour S3
        # ListObjectsV2 XML bodies and the selected original G18/G19 RRQPEF
        # NetCDF-4. It inspects bounded structure only and reads no pixels.
        # This is audit metadata, never scheduler or service dispatch wiring.
        candidate_handler=(
            "storm_tracker_worker.goes_rrqpe_native:"
            "capture_latest_goes_rrqpe"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=(
            "listing_current",
            "listing_previous",
            "netcdf4",
        ),
    ),
    _policy(
        "hsaf_h40b_fetch", "hsaf_h40b",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.SATELLITE_PRECIPITATION,
        "native_capture_not_connected",
        # Isolated candidate retains the exact official FTP NLST selection
        # evidence and selected original H40B .nc.gz. This audit reference is
        # not imported or dispatched by the production service/scheduler.
        candidate_handler=(
            "storm_tracker_worker.hsaf_h40b_native:"
            "capture_latest_hsaf_h40b"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=("listing_nlst", "netcdf_gzip"),
    ),
    _policy(
        "kmi_fetch", "kmi",
        CaptureStatus.EXACT_RAW_CONNECTED,
        ProductSemantics.CATEGORICAL_VISUALISATION,
        "exact_raw_but_uncertified_visualisation_only",
        admission=False,
        required_roles=("forecast_json", "radar_png"),
    ),
    _policy(
        "knmi_fetch", "knmi",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.QUANTITATIVE_RADAR,
        "native_capture_not_connected",
        candidate_handler=(
            "storm_tracker_worker.knmi_native:capture_latest_knmi_hdf5"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=("hdf5",),
    ),
    _policy(
        "metar_fetch", "metar",
        CaptureStatus.RESULT_ONLY,
        ProductSemantics.GROUND_OBSERVATION,
        "native_capture_not_connected",
        # Kept true only with the real AWC gzip golden-cache test in
        # test_worker_metar_native_capture.py; this is not live admission.
        candidate_handler=(
            "storm_tracker_worker.metar_native:capture_latest_metar_cache"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=("csv_gzip",),
    ),
    _policy(
        "meteofrance_radar_fetch", "meteofrance_radar",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.QUANTITATIVE_RADAR,
        "native_capture_not_connected",
        # Isolated candidate retains the exact official DPRadar catalogue
        # response and selected original IPRN20_C_LFPW 500 m HDF5 body.  It
        # deliberately performs no pixel read because the public HDF5 field,
        # CRS, missing/undetect and QC mapping is not yet complete.
        candidate_handler=(
            "storm_tracker_worker.meteofrance_native:"
            "capture_latest_meteofrance_hdf5"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=("catalog_json", "hdf5"),
    ),
    _policy(
        "netatmo_fetch", "netatmo",
        CaptureStatus.RESULT_ONLY,
        ProductSemantics.GROUND_OBSERVATION,
        "native_capture_not_connected",
        # Isolated exact getpublicdata response capture. The candidate only
        # indexes station/module/timestamp structure and is neither a ground
        # observation decoder nor an operational handler.
        candidate_handler=(
            "storm_tracker_worker.netatmo_native:"
            "capture_netatmo_public_data"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=("response_json",),
    ),
    _policy(
        "odim_hdf5_fetch", "met_office_radar",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.QUANTITATIVE_RADAR,
        "native_capture_not_connected",
        # Isolated candidate retains the exact previous/current UTC-day S3
        # listings and the selected native UK 1 km ODIM RATE composite.  The
        # reference is audit metadata only; no production handler imports it.
        candidate_handler=(
            "storm_tracker_worker.met_office_native:"
            "capture_latest_met_office_odim"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=(
            "listing_current",
            "listing_previous",
            "odim_hdf5",
        ),
    ),
    _policy(
        "open_meteo_fetch", "open_meteo",
        CaptureStatus.RESULT_ONLY,
        ProductSemantics.MODEL_GUIDANCE,
        "native_capture_not_connected",
    ),
    _policy(
        "opera_hdf5_fetch", "opera",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.QUANTITATIVE_RADAR,
        "native_capture_not_connected",
        # Isolated v3 candidate retains one exact OPERA DBZH/QI ODIM HDF5
        # object with contract-bound catalog/revision evidence. This string is
        # audit metadata only and is not present in production handlers.
        candidate_handler=(
            "storm_tracker_worker.opera_native:capture_opera_dbzh_hdf5"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=("hdf5",),
    ),
    _policy(
        "rainviewer_fetch", "rainviewer",
        CaptureStatus.LEGACY_ARTIFACT_ONLY,
        ProductSemantics.CATEGORICAL_VISUALISATION,
        "native_capture_not_connected",
        # Isolated candidate captures one exact manifest plus complete 3x3
        # unsmoothed radar and coverage grids for one proven region horizon.
        # Frame time is map-generation time; this never promotes the visual
        # product to quantitative radar or motion evidence.
        candidate_handler=(
            "storm_tracker_worker.rainviewer_native:"
            "capture_latest_rainviewer_region"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=(
            "coverage_dx_0_dy_0",
            "coverage_dx_0_dy_m1",
            "coverage_dx_0_dy_p1",
            "coverage_dx_m1_dy_0",
            "coverage_dx_m1_dy_m1",
            "coverage_dx_m1_dy_p1",
            "coverage_dx_p1_dy_0",
            "coverage_dx_p1_dy_m1",
            "coverage_dx_p1_dy_p1",
            "manifest",
            "radar_dx_0_dy_0",
            "radar_dx_0_dy_m1",
            "radar_dx_0_dy_p1",
            "radar_dx_m1_dy_0",
            "radar_dx_m1_dy_m1",
            "radar_dx_m1_dy_p1",
            "radar_dx_p1_dy_0",
            "radar_dx_p1_dy_m1",
            "radar_dx_p1_dy_p1",
        ),
    ),
    _policy(
        "synoptic_fetch", "synoptic",
        CaptureStatus.RESULT_ONLY,
        ProductSemantics.GROUND_OBSERVATION,
        "native_capture_not_connected",
        candidate_handler=(
            "storm_tracker_worker.synoptic_native:"
            "capture_synoptic_timeseries"
        ),
        candidate_exact_raw_tested=True,
        candidate_required_roles=("response_json",),
    ),
)

PROVIDER_CAPTURE_POLICIES = MappingProxyType({
    policy.job_type: policy for policy in _POLICIES
})
PROVIDER_FETCH_JOB_TYPES = frozenset(PROVIDER_CAPTURE_POLICIES)
if len(PROVIDER_CAPTURE_POLICIES) != len(_POLICIES):
    raise RuntimeError("provider-captureregistry bevat dubbele jobtypes")


def capture_policy(job_type: str) -> ProviderCapturePolicy:
    try:
        return PROVIDER_CAPTURE_POLICIES[job_type]
    except KeyError as err:
        raise KeyError(f"onbekende providerfetchjob: {job_type}") from err


def assert_registry_complete(job_types: Iterable[str]) -> None:
    actual = frozenset(job_types)
    if actual != PROVIDER_FETCH_JOB_TYPES:
        raise RuntimeError(
            "provider-captureregistry wijkt af; "
            f"ontbrekend={sorted(actual-PROVIDER_FETCH_JOB_TYPES)}, "
            f"verweesd={sorted(PROVIDER_FETCH_JOB_TYPES-actual)}"
        )


def capture_matrix_snapshot() -> dict[str, object]:
    items = [
        PROVIDER_CAPTURE_POLICIES[job_type].to_dict()
        for job_type in sorted(PROVIDER_CAPTURE_POLICIES)
    ]
    return {
        "method": METHOD,
        "provider_fetch_jobs": len(items),
        "exact_raw_connected": sum(
            item["exact_native_bytes_complete"] for item in items
        ),
        "isolated_exact_raw_candidates": sum(
            item["candidate_exact_raw_tested"] for item in items
        ),
        "vnext_operational_admitted": sum(
            item["vnext_operational_admission"] for item in items
        ),
        "items": items,
    }


__all__ = [
    "CaptureStatus",
    "METHOD",
    "PROVIDER_CAPTURE_POLICIES",
    "PROVIDER_FETCH_JOB_TYPES",
    "ProductSemantics",
    "ProviderCapturePolicy",
    "assert_registry_complete",
    "capture_matrix_snapshot",
    "capture_policy",
]
