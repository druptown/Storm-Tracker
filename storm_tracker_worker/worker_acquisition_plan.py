"""Unwired deterministic worker-owned acquisition planning contract.

This module translates one already validated :class:`DesiredState` snapshot
into a bounded immutable plan for exact-native meteorological acquisition.
It deliberately contains no network client, provider handler, scheduler hook,
Home Assistant integration, credential value, or live registration.

The second half of the module is a small in-memory contract dispatcher.  It
exposes one job at a time, derives stable cadence-slot identities, validates
terminal receipts, and can be reconstructed from the same plan and receipts.
Durable outbox/receipt persistence and real handlers remain future wiring.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import re
import threading
from typing import Any, Iterable, Mapping, Sequence

from .desired_state import (
    CONTRACT as DESIRED_STATE_CONTRACT,
    DesiredState,
    Target,
    parse_desired_state,
)


PLAN_CONTRACT = "stv3-worker-acquisition-plan-v2"
REGION_CONTRACT = "stv3-worker-acquisition-region-v2"
INTENT_CONTRACT = "stv3-worker-acquisition-intent-v1"
JOB_CONTRACT = "stv3-worker-acquisition-job-v1"
RECEIPT_CONTRACT = "stv3-worker-acquisition-receipt-v1"
DISPATCH_CONTRACT = "stv3-worker-acquisition-dispatch-v1"
CATALOG_CONTRACT = "stv3-worker-acquisition-catalog-v2"
EVENT_INGRESS_CONTRACT = "stv3-worker-event-ingress-catalog-v1"
MIGRATION_CONTRACT = "stv3-worker-acquisition-catalog-migrations-v1"
METHOD = "worker_acquisition_plan_v3"
CADENCE_PROFILE_ID = "meteorological_truth_candidate_v2"
COLLECTOR_SCOPE_COVERAGE_RELEVANT = "coverage_relevant"
COLLECTOR_SCOPE_ALL_SOURCES = "all_sources"
COLLECTOR_SCOPES = frozenset({
    COLLECTOR_SCOPE_ALL_SOURCES,
    COLLECTOR_SCOPE_COVERAGE_RELEVANT,
})

MAX_REGIONS = 64
MAX_INTENTS = 512
MAX_PLAN_BYTES = 1024 * 1024
MAX_RECEIPTS = 4096
MAX_DISPATCH_JOBS = 512
MAX_DISPATCH_TRANSPORT_BYTES = 16 * 1024 * 1024 * 1024
MAX_DISPATCH_ARTIFACTS = 8192
MAX_DISPATCH_WALL_SECONDS = 24 * 60 * 60
MAX_IDENTIFIER_LENGTH = 192
EARTH_RADIUS_KM = 6371.0088

_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_.:/-]{0,191}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SECRET_REQUIREMENT = re.compile(r"^worker_secret\.[a-z][a-z0-9_]{0,63}$")


class AcquisitionPlanError(ValueError):
    """Base error for an invalid or internally conflicting plan contract."""


class AcquisitionSnapshotError(AcquisitionPlanError):
    """The supplied desired-state object is not its validated canonical form."""


class AcquisitionCapacityError(AcquisitionPlanError):
    """A hard plan, dispatch, or receipt capacity envelope was exceeded."""


class AcquisitionConflictError(AcquisitionPlanError):
    """A stable identity was reused for different immutable content."""


class WorkloadClass(str, Enum):
    RADAR = "radar"
    SATELLITE = "satellite"
    GROUND = "ground"
    MODEL = "model"
    LIGHTNING = "lightning"


class CapabilityStatus(str, Enum):
    CONTRACT_READY_UNWIRED = "contract_ready_unwired"
    EXTERNAL_CONTRACT_BLOCKED = "external_contract_blocked"
    DECOMMISSIONED = "decommissioned"
    UNSUPPORTED_FAIL_CLOSED = "unsupported_fail_closed"


class EventIngressStatus(str, Enum):
    OWNERSHIP_BLOCKED = "ownership_blocked"


@dataclass(frozen=True, slots=True)
class AcquisitionBudget:
    max_transport_bytes: int
    max_artifacts: int
    max_wall_seconds: int

    def as_dict(self) -> dict[str, int]:
        return {
            "max_artifacts": self.max_artifacts,
            "max_transport_bytes": self.max_transport_bytes,
            "max_wall_seconds": self.max_wall_seconds,
        }


@dataclass(frozen=True, slots=True)
class AcquisitionRegion:
    region_id: str
    center_latitude: float
    center_longitude: float
    radius_km: float
    target_ids: tuple[str, ...]
    country_codes: tuple[str, ...]
    region_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "center_latitude": self.center_latitude,
            "center_longitude": self.center_longitude,
            "country_codes": list(self.country_codes),
            "radius_km": self.radius_km,
            "region_id": self.region_id,
            "region_sha256": self.region_sha256,
            "target_ids": list(self.target_ids),
        }


@dataclass(frozen=True, slots=True)
class BlockedCapability:
    workload_id: str
    workload_class: str
    provider_id: str
    product_id: str
    reason_code: str

    def as_dict(self) -> dict[str, str]:
        return {
            "product_id": self.product_id,
            "provider_id": self.provider_id,
            "reason_code": self.reason_code,
            "workload_class": self.workload_class,
            "workload_id": self.workload_id,
        }


@dataclass(frozen=True, slots=True)
class AcquisitionIntent:
    intent_id: str
    workload_id: str
    workload_class: str
    provider_id: str
    product_id: str
    scope_id: str
    region_ids: tuple[str, ...]
    target_ids: tuple[str, ...]
    country_codes: tuple[str, ...]
    satellite_id: str | None
    cadence_seconds: int
    deadline_seconds: int
    priority: int
    budget: AcquisitionBudget | None
    secret_requirement_ids: tuple[str, ...]
    intent_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "budget": self.budget.as_dict(),
            "cadence_seconds": self.cadence_seconds,
            "country_codes": list(self.country_codes),
            "deadline_seconds": self.deadline_seconds,
            "intent_id": self.intent_id,
            "intent_sha256": self.intent_sha256,
            "priority": self.priority,
            "product_id": self.product_id,
            "provider_id": self.provider_id,
            "region_ids": list(self.region_ids),
            "satellite_id": self.satellite_id,
            "scope_id": self.scope_id,
            "secret_requirement_ids": list(self.secret_requirement_ids),
            "target_ids": list(self.target_ids),
            "workload_class": self.workload_class,
            "workload_id": self.workload_id,
        }


@dataclass(frozen=True, slots=True)
class AcquisitionPlan:
    snapshot_contract: str
    snapshot_sha256: str
    producer_id: str
    authority_epoch: int
    generation: int
    cadence_profile_id: str
    cadence_profile_sha256: str
    catalog_sha256: str
    collector_scope: str
    regions: tuple[AcquisitionRegion, ...]
    intents: tuple[AcquisitionIntent, ...]
    blocked_capabilities: tuple[BlockedCapability, ...]
    canonical_bytes: bytes
    plan_sha256: str

    def as_dict(self) -> dict[str, Any]:
        result = json.loads(self.canonical_bytes)
        result["plan_sha256"] = self.plan_sha256
        return result


@dataclass(frozen=True, slots=True)
class DispatchCapacity:
    max_jobs: int = 128
    max_transport_bytes: int = 2 * 1024 * 1024 * 1024
    max_artifacts: int = 2048
    max_wall_seconds: int = 4 * 60 * 60

    def as_dict(self) -> dict[str, int]:
        return {
            "max_artifacts": self.max_artifacts,
            "max_jobs": self.max_jobs,
            "max_transport_bytes": self.max_transport_bytes,
            "max_wall_seconds": self.max_wall_seconds,
        }


@dataclass(frozen=True, slots=True)
class AcquisitionJob:
    job_id: str
    job_sha256: str
    plan_sha256: str
    snapshot_sha256: str
    intent_id: str
    intent_sha256: str
    workload_id: str
    workload_class: str
    provider_id: str
    product_id: str
    scope_id: str
    region_ids: tuple[str, ...]
    target_ids: tuple[str, ...]
    country_codes: tuple[str, ...]
    satellite_id: str | None
    cadence_seconds: int
    slot_index: int
    scheduled_for: float
    deadline_at: float
    priority: int
    budget: AcquisitionBudget
    secret_requirement_ids: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "budget": self.budget.as_dict(),
            "cadence_seconds": self.cadence_seconds,
            "contract": JOB_CONTRACT,
            "country_codes": list(self.country_codes),
            "deadline_at": self.deadline_at,
            "intent_id": self.intent_id,
            "intent_sha256": self.intent_sha256,
            "job_id": self.job_id,
            "job_sha256": self.job_sha256,
            "plan_sha256": self.plan_sha256,
            "priority": self.priority,
            "product_id": self.product_id,
            "provider_id": self.provider_id,
            "region_ids": list(self.region_ids),
            "satellite_id": self.satellite_id,
            "scheduled_for": self.scheduled_for,
            "scope_id": self.scope_id,
            "secret_requirement_ids": list(self.secret_requirement_ids),
            "slot_index": self.slot_index,
            "snapshot_sha256": self.snapshot_sha256,
            "target_ids": list(self.target_ids),
            "workload_class": self.workload_class,
            "workload_id": self.workload_id,
        }


@dataclass(frozen=True, slots=True)
class AcquisitionReceipt:
    plan_sha256: str
    job_id: str
    job_sha256: str
    outcome: str
    completed_at: float
    artifact_receipt_sha256: str | None
    error_code: str | None
    receipt_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "artifact_receipt_sha256": self.artifact_receipt_sha256,
            "completed_at": self.completed_at,
            "contract": RECEIPT_CONTRACT,
            "error_code": self.error_code,
            "job_id": self.job_id,
            "job_sha256": self.job_sha256,
            "outcome": self.outcome,
            "plan_sha256": self.plan_sha256,
            "receipt_sha256": self.receipt_sha256,
        }


@dataclass(frozen=True, slots=True)
class DispatchBlock:
    subject_id: str
    provider_id: str
    product_id: str
    reason_code: str

    def as_dict(self) -> dict[str, str]:
        return {
            "product_id": self.product_id,
            "provider_id": self.provider_id,
            "reason_code": self.reason_code,
            "subject_id": self.subject_id,
        }


@dataclass(frozen=True, slots=True)
class DispatchSnapshot:
    plan_sha256: str
    as_of: float
    serial_concurrency: int
    ready_jobs: tuple[AcquisitionJob, ...]
    blocked: tuple[DispatchBlock, ...]
    deferred: tuple[DispatchBlock, ...]
    completed_job_ids: tuple[str, ...]
    capacity: DispatchCapacity
    dispatch_sha256: str

    @property
    def next_job(self) -> AcquisitionJob | None:
        return self.ready_jobs[0] if self.ready_jobs else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "blocked": [item.as_dict() for item in self.blocked],
            "capacity": self.capacity.as_dict(),
            "completed_job_ids": list(self.completed_job_ids),
            "contract": DISPATCH_CONTRACT,
            "deferred": [item.as_dict() for item in self.deferred],
            "dispatch_sha256": self.dispatch_sha256,
            "plan_sha256": self.plan_sha256,
            "ready_jobs": [item.as_dict() for item in self.ready_jobs],
            "serial_concurrency": self.serial_concurrency,
        }


@dataclass(frozen=True, slots=True)
class _WorkloadDefinition:
    workload_id: str
    workload_class: WorkloadClass
    provider_id: str
    product_id: str
    scope_mode: str
    coverage_id: str
    cadence_seconds: int
    deadline_seconds: int
    priority: int
    budget: AcquisitionBudget
    secret_requirement_ids: tuple[str, ...] = ()
    country_codes: tuple[str, ...] = ()
    coverage_bbox: tuple[float, float, float, float] | None = None
    satellite_id: str | None = None
    status: CapabilityStatus = CapabilityStatus.CONTRACT_READY_UNWIRED
    blocked_reason: str | None = None
    source_role: str = "candidate_meteorological_input"
    admission_eligible: bool = False
    durable_production_retention_eligible: bool = True


@dataclass(frozen=True, slots=True)
class _EventIngressDefinition:
    ingress_id: str
    workload_class: WorkloadClass
    provider_id: str
    product_id: str
    delivery_model: str
    status: EventIngressStatus
    blocked_reason: str
    absence_semantics: str = "no_absence_claim"
    admission_eligible: bool = False
    durable_production_retention_eligible: bool = False


@dataclass(frozen=True, slots=True)
class CatalogMigration:
    legacy_id: str
    replacement_ids: tuple[str, ...]
    mode: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "legacy_id": self.legacy_id,
            "mode": self.mode,
            "replacement_ids": list(self.replacement_ids),
        }


def _budget(mebibytes: int, artifacts: int, seconds: int) -> AcquisitionBudget:
    return AcquisitionBudget(mebibytes * 1024 * 1024, artifacts, seconds)


# This is a planning catalogue, not a handler registry and not an admission
# matrix.  CONTRACT_READY_UNWIRED means only that a deterministic acquisition
# request can be described.  It says nothing about decoder or meteorological
# admission maturity.
_WORKLOADS: tuple[_WorkloadDefinition, ...] = (
    _WorkloadDefinition(
        "radar.aemet.composite", WorkloadClass.RADAR, "aemet", "aemet_radar_native_composite",
        # Exact envelope + data + metadata ceilings are 64 KiB + 96 MiB +
        # 4 MiB.  Reserve the rounded-up complete three-role envelope; the
        # native handler owns one absolute 75 second deadline.
        "coverage", "aemet_es", 300, 240, 24, _budget(101, 3, 75),
        ("worker_secret.aemet_api_key",), ("ES",), (25.0, -20.0, 47.0, 8.0),
    ),
    _WorkloadDefinition(
        "radar.dpc.sri", WorkloadClass.RADAR, "dpc", "dpc_sri_original_geotiff",
        "coverage", "dpc_it", 300, 240, 23, _budget(32, 2, 75),
        (), ("IT",), (34.0, 5.0, 48.5, 20.0),
    ),
    _WorkloadDefinition(
        "radar.dwd.radolan_rv", WorkloadClass.RADAR, "dwd", "dwd_radolan_rv_native_tar",
        # The raw-only acquisition wrapper retains exactly one upstream tar
        # under an 8 MiB hard response ceiling; it never extracts or decodes
        # the HDF5 member.  Reserve that complete accepted transport envelope.
        "coverage", "dwd_central_europe", 300, 240, 20, _budget(8, 1, 45),
        (), ("AT", "BE", "CH", "CZ", "DE", "DK", "FR", "LU", "NL", "PL"),
        (45.0, -5.0, 58.0, 20.0),
    ),
    # KMI products never share an identity.  Public OpenData responses are
    # retained exactly even while their meteorological interpretation remains
    # unverified; the documented native RADQPE delivery contract is separate
    # and remains externally inaccessible.
    _WorkloadDefinition(
        "ground.kmi.aws_10min", WorkloadClass.GROUND, "kmi",
        "kmi_aws_10min_wfs_exact_response",
        "coverage", "kmi_aws_be", 600, 540, 49, _budget(32, 1, 50),
        (), ("BE",), (49.4, 2.4, 51.6, 6.5), None,
        source_role="exact_raw_ground_observation_unverified",
    ),
    _WorkloadDefinition(
        "radar.kmi.appv4_visual", WorkloadClass.RADAR, "kmi", "kmi_appv4_rendered_animation_image",
        "region", "kmi_appv4_be", 300, 240, 18, _budget(12, 2, 45),
        (), ("BE", "LU"), (46.5, -2.5, 53.0, 10.5), None,
        source_role="categorical_visualization_only",
        durable_production_retention_eligible=False,
    ),
    _WorkloadDefinition(
        "radar.kmi.public_wcs_unknown_quantity", WorkloadClass.RADAR, "kmi", "kmi_public_belgian_rainfall_composite_unknown_quantity_geotiff",
        # One exact public GeoServer response.  Quantity, unit, nodata and
        # scientific admission remain explicitly unverified, but no parsing
        # occurs before the bounded response is committed to the raw store.
        "coverage", "kmi_public_wcs_be", 300, 240, 18, _budget(64, 1, 50),
        (), ("BE", "LU"), (48.5, 2.0, 52.5, 7.0), None,
        source_role="exact_raw_radar_response_unverified",
    ),
    _WorkloadDefinition(
        "radar.kmi.radqpe_native", WorkloadClass.RADAR, "kmi", "kmi_radqpe_documented_native_hdf5",
        # The public user guide proves product/grid/cadence facts, but no
        # usable delivery contract, response ceiling or deadline.  ``None`` is
        # deliberate: fabricating an execution budget would be less safe than
        # keeping the externally blocked product non-dispatchable.
        "coverage", "kmi_radqpe_be", 300, 240, 18, None,
        ("worker_secret.kmi_radqpe_access",), ("BE", "LU"), (48.5, 2.0, 52.5, 7.0), None,
        CapabilityStatus.EXTERNAL_CONTRACT_BLOCKED,
        "external_native_radqpe_delivery_contract_and_credential_unavailable",
        source_role="quantitative_radar_candidate",
        durable_production_retention_eligible=False,
    ),
    _WorkloadDefinition(
        "radar.knmi.rtcor", WorkloadClass.RADAR, "knmi", "knmi_rtcor_5m_native_hdf5",
        # The native KNMI handler accepts one HDF5 object up to 40 MiB and
        # enforces a 75 second total deadline.
        "coverage", "knmi_nl", 300, 240, 19, _budget(40, 1, 75),
        ("worker_secret.knmi_api_key",), ("BE", "DE", "NL"), (49.0, 1.0, 55.5, 9.0),
    ),
    _WorkloadDefinition(
        "radar.meteofrance.dpradar", WorkloadClass.RADAR, "meteofrance", "meteofrance_dpradar_native_hdf5",
        "coverage", "meteofrance_fr", 300, 240, 22, _budget(48, 2, 90),
        ("worker_secret.meteofrance_oauth",), ("FR",), (40.0, -7.0, 53.0, 11.0),
    ),
    _WorkloadDefinition(
        "radar.metoffice.uk_rate", WorkloadClass.RADAR, "metoffice", "metoffice_uk_rate_native_odim_hdf5",
        "coverage", "metoffice_uk", 300, 240, 21, _budget(32, 3, 90),
        (), ("GB", "IE"), (43.0, -19.0, 64.0, 10.0),
    ),
    _WorkloadDefinition(
        "radar.opera.dbzh", WorkloadClass.RADAR, "opera", "opera_cirrus_dbzh_qi_native_odim_hdf5",
        # Keep the catalogue honest even while activation remains blocked:
        # native transport permits one 384 MiB object and owns a 300 second
        # total deadline.  No smaller reservation may precede that handler.
        "coverage", "opera_europe", 300, 300, 30, _budget(384, 1, 300),
        (), (), (30.0, -42.0, 69.0, 60.0),
    ),
    _WorkloadDefinition(
        "radar.rainviewer.visual", WorkloadClass.RADAR, "rainviewer", "rainviewer_unsmoothed_tiles_with_coverage",
        # Exact role set: one 2 MiB manifest plus eighteen 2 MiB PNGs.
        "region", "global", 600, 540, 39, _budget(38, 19, 75),
    ),
    _WorkloadDefinition(
        "validation.geosphere.location", WorkloadClass.MODEL, "geosphere", "geosphere_inca_point_nowcast_validation_json",
        "region", "geosphere_at", 900, 720, 80, _budget(4, 1, 60),
        (), ("AT",), (45.5, 8.1, 49.5, 17.75), None,
        source_role="model_location_validation",
        durable_production_retention_eligible=False,
    ),
    _WorkloadDefinition(
        "validation.italiameteo.dpc_sri_historical_catalog", WorkloadClass.MODEL, "italiameteo", "italiameteo_dpc_sri_historical_daily_catalog_json",
        "global", "italiameteo_dpc_sri_catalog", 86400, 82800, 90, _budget(4, 1, 60),
        (), ("IT",), (34.0, 5.0, 48.5, 20.0), None,
        source_role="historical_catalog_validation",
        durable_production_retention_eligible=False,
    ),
    _WorkloadDefinition(
        "model.openmeteo.italiameteo_icon2i.region", WorkloadClass.MODEL, "openmeteo", "openmeteo_italia_meteo_arpae_icon_2i_forecast_json",
        "region", "italiameteo_icon2i", 900, 720, 81, _budget(4, 1, 60),
        (), ("IT",), (35.0, 5.5, 48.5, 19.0), None,
        source_role="model_location_validation",
        durable_production_retention_eligible=False,
    ),
    _WorkloadDefinition(
        "validation.meteolux.location", WorkloadClass.MODEL, "meteolux", "meteolux_location_weather_validation_json",
        "region", "meteolux_lu", 900, 720, 80, _budget(4, 1, 60),
        (), ("LU",), (48.5, 4.5, 51.0, 7.5), None,
        source_role="model_location_validation",
        durable_production_retention_eligible=False,
    ),
    _WorkloadDefinition(
        "satellite.goes18.rrqpe", WorkloadClass.SATELLITE, "noaa_goes", "abi_l2_rrqpef_native_netcdf4",
        "satellite", "goes18", 600, 540, 40, _budget(20, 3, 105),
        (), (), (-65.0, -180.0, 65.0, -55.0), "G18",
    ),
    _WorkloadDefinition(
        "satellite.goes19.rrqpe", WorkloadClass.SATELLITE, "noaa_goes", "abi_l2_rrqpef_native_netcdf4",
        "satellite", "goes19", 600, 540, 40, _budget(20, 3, 105),
        (), (), (-65.0, -140.0, 65.0, 20.0), "G19",
    ),
    _WorkloadDefinition(
        "satellite.hsaf.h40b", WorkloadClass.SATELLITE, "hsaf", "hsaf_h40b_native_netcdf_gzip",
        # Native ceilings: 2 MiB listing + 64 MiB compressed product, two
        # durable roles and one absolute 90 second capture deadline.
        "coverage", "hsaf_h40b_disk", 600, 540, 43, _budget(66, 2, 90),
        ("worker_secret.hsaf_ftp_credentials",), (), (-70.0, -70.0, 70.0, 70.0),
    ),
    _WorkloadDefinition(
        "satellite.noaa.err", WorkloadClass.SATELLITE, "noaa", "enterprise_rainrate_glb5_native_netcdf4",
        "global", "global", 600, 540, 42, _budget(36, 2, 75),
    ),
    _WorkloadDefinition(
        "ground.awn.authorized_devices", WorkloadClass.GROUND, "ambientweather", "awn_authorized_devices_native_json",
        "global", "global", 300, 240, 53, _budget(8, 1, 45),
        (
            "worker_secret.awn_api_key",
            "worker_secret.awn_application_key",
        ),
    ),
    _WorkloadDefinition(
        "ground.metar.global", WorkloadClass.GROUND, "aviationweather", "metar_cache_native_csv_gzip",
        # The exact-native handler enforces a 16 MiB compressed-body ceiling.
        # Keep the planner reservation identical to that hard transport bound;
        # a smaller advertised budget would permit bytes to reach the raw
        # store before the global capacity ledger could account for them.
        "global", "global", 300, 240, 50, _budget(16, 1, 45),
    ),
    _WorkloadDefinition(
        "ground.netatmo.region", WorkloadClass.GROUND, "netatmo", "netatmo_getpublicdata_native_json",
        "region", "global", 300, 240, 51, _budget(8, 1, 45),
        ("worker_secret.netatmo_access_token",),
    ),
    _WorkloadDefinition(
        "ground.synoptic.region", WorkloadClass.GROUND, "synoptic", "synoptic_timeseries_native_json",
        # The exact-native response ceiling is 16 MiB; never under-reserve it.
        "region", "synoptic_coverage", 300, 240, 52, _budget(16, 1, 45),
        ("worker_secret.synoptic_api_token",), (), (10.0, -180.0, 75.0, -45.0),
    ),
    _WorkloadDefinition(
        "model.openmeteo.region", WorkloadClass.MODEL, "openmeteo", "openmeteo_forecast_native_json",
        "region", "global", 900, 720, 80, _budget(8, 1, 60),
        source_role="model_location_validation",
        durable_production_retention_eligible=False,
    ),
    _WorkloadDefinition(
        "lightning.goes18.glm", WorkloadClass.LIGHTNING, "noaa_goes", "goes18_glm_l2_native_netcdf4",
        # Two 4 MiB listings plus up to twelve 5 MiB dynamic members.
        "satellite", "goes18", 120, 105, 10, _budget(68, 14, 105),
        (), (), (-65.0, -180.0, 65.0, -55.0), "G18",
        source_role="lightning_event_candidate",
    ),
    _WorkloadDefinition(
        "lightning.goes19.glm", WorkloadClass.LIGHTNING, "noaa_goes", "goes19_glm_l2_native_netcdf4",
        "satellite", "goes19", 120, 105, 10, _budget(68, 14, 105),
        (), (), (-65.0, -140.0, 65.0, 20.0), "G19",
        source_role="lightning_event_candidate",
    ),
    _WorkloadDefinition(
        "lightning.eumetsat.li", WorkloadClass.LIGHTNING, "eumetsat", "mtg_li_native_netcdf4",
        # Exact 4 MiB search response plus one exact 10 MiB selected object.
        "coverage", "mtg_fci_disk", 60, 55, 12, _budget(14, 2, 50),
        (
            "worker_secret.eumetsat_consumer_key",
            "worker_secret.eumetsat_consumer_secret",
        ),
        (), (-70.0, -70.0, 70.0, 70.0),
    ),
)


# Push/event ingress is deliberately not a cadence workload.  In particular,
# no missing MQTT packet or quiet interval may be interpreted as an absence of
# lightning.  Exact worker-side storage, bounded backpressure and gap receipts
# are wired, but ownership remains blocked by the explicit participant/raw-
# retention authority and by the missing live QoS-0/host soak.  Decoding is not
# a prerequisite for this raw-only catalogue and remains unadmitted.
_EVENT_INGRESSES: tuple[_EventIngressDefinition, ...] = (
    _EventIngressDefinition(
        ingress_id="lightning.blitzortung.event_ingress",
        workload_class=WorkloadClass.LIGHTNING,
        provider_id="blitzortung",
        product_id="blitzortung_mqtt_qos0_raw_event_envelope",
        delivery_model="mqtt_push_event_stream",
        status=EventIngressStatus.OWNERSHIP_BLOCKED,
        blocked_reason=(
            "external_participant_raw_retention_authority_and_live_qos0_soak_missing"
        ),
    ),
)


# These are audit-only migration facts, never transparent dispatch aliases.
# Every rename changes product semantics and therefore requires rebuilding the
# desired plan; the ambiguous historical KMI/ItaliaMeteo identities fan out.
_CATALOG_MIGRATIONS: tuple[CatalogMigration, ...] = (
    CatalogMigration(
        legacy_id="lightning.blitzortung",
        mode="event_ingress_ownership_only_no_cadence_alias",
        replacement_ids=("lightning.blitzortung.event_ingress",),
    ),
    CatalogMigration(
        legacy_id="radar.geosphere.at",
        mode="semantic_rename_rebuild_required",
        replacement_ids=("validation.geosphere.location",),
    ),
    CatalogMigration(
        legacy_id="radar.italiameteo.it",
        mode="historical_and_model_split_rebuild_required",
        replacement_ids=(
            "model.openmeteo.italiameteo_icon2i.region",
            "validation.italiameteo.dpc_sri_historical_catalog",
        ),
    ),
    CatalogMigration(
        legacy_id="radar.kmi.radqpe",
        mode="ambiguous_three_product_split_rebuild_required",
        replacement_ids=(
            "radar.kmi.appv4_visual",
            "radar.kmi.public_wcs_unknown_quantity",
            "radar.kmi.radqpe_native",
        ),
    ),
    CatalogMigration(
        legacy_id="radar.meteolux.lu",
        mode="semantic_rename_rebuild_required",
        replacement_ids=("validation.meteolux.location",),
    ),
)


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as err:
        raise AcquisitionPlanError("contract bevat niet-canonieke JSON") from err


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _finite_time(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AcquisitionPlanError(f"{name} moet een tijdgetal zijn")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 2**53:
        raise AcquisitionPlanError(f"{name} ligt buiten de harde grens")
    return result


def _positive_integer(value: Any, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AcquisitionPlanError(f"{name} moet een integer zijn")
    if not 1 <= value <= maximum:
        raise AcquisitionPlanError(f"{name} ligt buiten de harde grens")
    return value


def _validate_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or len(value) > MAX_IDENTIFIER_LENGTH:
        raise AcquisitionPlanError(f"{name} is geen begrensde identifier")
    if _IDENTIFIER.fullmatch(value) is None:
        raise AcquisitionPlanError(f"{name} is geen canonieke identifier")
    return value


def _normal_float(value: float) -> float:
    result = round(float(value), 12)
    return 0.0 if result == 0.0 else result


def _haversine_km(first: Target, second: Target) -> float:
    lat1 = math.radians(first.latitude)
    lat2 = math.radians(second.latitude)
    dlat = lat2 - lat1
    dlon = math.radians(second.longitude - first.longitude)
    value = math.sin(dlat / 2.0) ** 2 + (
        math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(value)))


def _point_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    a = math.radians(lat1)
    b = math.radians(lat2)
    dlat = b - a
    dlon = math.radians(lon2 - lon1)
    value = math.sin(dlat / 2.0) ** 2 + (
        math.cos(a) * math.cos(b) * math.sin(dlon / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(value)))


def _spherical_center(targets: Sequence[Target]) -> tuple[float, float]:
    x = y = z = 0.0
    for target in targets:
        latitude = math.radians(target.latitude)
        longitude = math.radians(target.longitude)
        x += math.cos(latitude) * math.cos(longitude)
        y += math.cos(latitude) * math.sin(longitude)
        z += math.sin(latitude)
    length = math.sqrt(x * x + y * y + z * z)
    if length < 1e-12:
        return (_normal_float(targets[0].latitude), _normal_float(targets[0].longitude))
    latitude = math.degrees(math.atan2(z, math.sqrt(x * x + y * y)))
    longitude = math.degrees(math.atan2(y, x))
    return (_normal_float(latitude), _normal_float(longitude))


def _validated_snapshot(snapshot: DesiredState) -> DesiredState:
    if not isinstance(snapshot, DesiredState):
        raise AcquisitionSnapshotError("planner vereist een gevalideerd DesiredState-object")
    try:
        parsed = parse_desired_state(snapshot.canonical_bytes)
    except Exception as err:
        raise AcquisitionSnapshotError("desired-state canonical bytes zijn ongeldig") from err
    if parsed != snapshot or parsed.state_sha256 != snapshot.state_sha256:
        raise AcquisitionSnapshotError("desired-state object en canonical checksum verschillen")
    return parsed


def _validate_budget(budget: AcquisitionBudget) -> None:
    if not isinstance(budget, AcquisitionBudget):
        raise AcquisitionPlanError("workloadbudget heeft het verkeerde type")
    _positive_integer(
        budget.max_transport_bytes,
        "max_transport_bytes",
        MAX_DISPATCH_TRANSPORT_BYTES,
    )
    _positive_integer(budget.max_artifacts, "max_artifacts", MAX_DISPATCH_ARTIFACTS)
    _positive_integer(
        budget.max_wall_seconds,
        "max_wall_seconds",
        MAX_DISPATCH_WALL_SECONDS,
    )


def _definition_dict(definition: _WorkloadDefinition) -> dict[str, Any]:
    return {
        "admission_eligible": definition.admission_eligible,
        "blocked_reason": definition.blocked_reason,
        "budget": (
            definition.budget.as_dict() if definition.budget is not None else None
        ),
        "cadence_seconds": definition.cadence_seconds,
        "country_codes": list(definition.country_codes),
        "coverage_bbox": (
            list(definition.coverage_bbox)
            if definition.coverage_bbox is not None
            else None
        ),
        "coverage_id": definition.coverage_id,
        "deadline_seconds": definition.deadline_seconds,
        "durable_production_retention_eligible": (
            definition.durable_production_retention_eligible
        ),
        "execution_model": "cadence_pull",
        "priority": definition.priority,
        "product_id": definition.product_id,
        "provider_id": definition.provider_id,
        "satellite_id": definition.satellite_id,
        "scope_mode": definition.scope_mode,
        "secret_requirement_ids": list(definition.secret_requirement_ids),
        "source_role": definition.source_role,
        "status": definition.status.value,
        "workload_class": definition.workload_class.value,
        "workload_id": definition.workload_id,
    }


def _event_ingress_dict(definition: _EventIngressDefinition) -> dict[str, Any]:
    return {
        "absence_semantics": definition.absence_semantics,
        "admission_eligible": definition.admission_eligible,
        "blocked_reason": definition.blocked_reason,
        "cadence_seconds": None,
        "contract": EVENT_INGRESS_CONTRACT,
        "delivery_model": definition.delivery_model,
        "durable_production_retention_eligible": (
            definition.durable_production_retention_eligible
        ),
        "ingress_id": definition.ingress_id,
        "product_id": definition.product_id,
        "provider_id": definition.provider_id,
        "status": definition.status.value,
        "workload_class": definition.workload_class.value,
    }


def _validate_catalog() -> None:
    ids: set[str] = set()
    allowed_scopes = {"global", "coverage", "region", "satellite"}
    for definition in _WORKLOADS:
        for name, value in (
            ("workload_id", definition.workload_id),
            ("provider_id", definition.provider_id),
            ("product_id", definition.product_id),
            ("coverage_id", definition.coverage_id),
        ):
            _validate_identifier(value, name)
        if definition.workload_id in ids:
            raise RuntimeError("acquisitiecatalogus bevat een dubbel workload_id")
        ids.add(definition.workload_id)
        if definition.scope_mode not in allowed_scopes:
            raise RuntimeError("acquisitiecatalogus bevat een onbekende scope")
        if not 1 <= definition.cadence_seconds <= 7 * 24 * 60 * 60:
            raise RuntimeError("acquisitiecadans ligt buiten de grens")
        if not 1 <= definition.deadline_seconds <= definition.cadence_seconds:
            raise RuntimeError("acquisitiedeadline past niet binnen de cadans")
        if not 0 <= definition.priority <= 1000:
            raise RuntimeError("acquisitieprioriteit ligt buiten de grens")
        if definition.budget is not None:
            _validate_budget(definition.budget)
        elif definition.status is CapabilityStatus.CONTRACT_READY_UNWIRED:
            raise RuntimeError("dispatchbare capability mist een bewezen budget")
        if tuple(sorted(set(definition.country_codes))) != definition.country_codes:
            raise RuntimeError("country_codes moeten gesorteerd en uniek zijn")
        if tuple(sorted(set(definition.secret_requirement_ids))) != (
            definition.secret_requirement_ids
        ):
            raise RuntimeError("secret requirements moeten gesorteerd en uniek zijn")
        if any(
            _SECRET_REQUIREMENT.fullmatch(value) is None
            for value in definition.secret_requirement_ids
        ):
            raise RuntimeError("ongeldige abstracte secret-requirement identifier")
        blocked = definition.status is not CapabilityStatus.CONTRACT_READY_UNWIRED
        if blocked != bool(definition.blocked_reason):
            raise RuntimeError("geblokkeerde capability vereist precies een reden")
        if type(definition.admission_eligible) is not bool:
            raise RuntimeError("admission_eligible moet boolean zijn")
        if definition.admission_eligible:
            raise RuntimeError("planningcatalogus mag geen admission claimen")
        if type(definition.durable_production_retention_eligible) is not bool:
            raise RuntimeError("retention eligibility moet boolean zijn")
        _validate_identifier(definition.source_role, "source_role")
        if definition.satellite_id is not None and definition.scope_mode != "satellite":
            raise RuntimeError("satellite_id vereist satellietscope")
    blocked_ids = {
        item.workload_id
        for item in _WORKLOADS
        if item.status is not CapabilityStatus.CONTRACT_READY_UNWIRED
    }
    expected_blocked_ids = {
        "radar.kmi.radqpe_native",
    }
    if blocked_ids != expected_blocked_ids:
        raise RuntimeError("fail-closed workloadset is niet exact compleet")
    classes = {item.workload_class for item in _WORKLOADS}
    if classes != set(WorkloadClass):
        raise RuntimeError("acquisitiecatalogus mist een workloadklasse")

    ingress_ids: set[str] = set()
    for ingress in _EVENT_INGRESSES:
        for name, value in (
            ("ingress_id", ingress.ingress_id),
            ("provider_id", ingress.provider_id),
            ("product_id", ingress.product_id),
            ("delivery_model", ingress.delivery_model),
            ("absence_semantics", ingress.absence_semantics),
        ):
            _validate_identifier(value, name)
        if ingress.ingress_id in ingress_ids or ingress.ingress_id in ids:
            raise RuntimeError("event-ingresscatalogus bevat een dubbele identity")
        ingress_ids.add(ingress.ingress_id)
        if ingress.status is not EventIngressStatus.OWNERSHIP_BLOCKED:
            raise RuntimeError("event ingress mag nog geen ownership claimen")
        if not ingress.blocked_reason:
            raise RuntimeError("event ingress mist blocked reason")
        if ingress.absence_semantics != "no_absence_claim":
            raise RuntimeError("event ingress mag geen absenceclaim bevatten")
        if ingress.admission_eligible or ingress.durable_production_retention_eligible:
            raise RuntimeError("geblokkeerde event ingress mag geen productieclaim bevatten")

    known_replacements = ids | ingress_ids
    legacy_ids: set[str] = set()
    for migration in _CATALOG_MIGRATIONS:
        legacy_id = _validate_identifier(migration.legacy_id, "legacy_id")
        mode = _validate_identifier(migration.mode, "migration mode")
        replacements = migration.replacement_ids
        if (
            not replacements
            or replacements != tuple(sorted(set(replacements)))
        ):
            raise RuntimeError("migratiereplacements zijn niet canoniek")
        if legacy_id in legacy_ids or legacy_id in known_replacements:
            raise RuntimeError("legacy migratie-id is niet uniek of nog actief")
        legacy_ids.add(legacy_id)
        if any(
            _validate_identifier(value, "replacement_id") not in known_replacements
            for value in replacements
        ):
            raise RuntimeError("migratie verwijst naar onbekende replacement")
        if "rebuild_required" not in mode and "no_cadence_alias" not in mode:
            raise RuntimeError("migratie mag geen transparante alias zijn")


_validate_catalog()

_CATALOG_PAYLOAD = {
    "contract": CATALOG_CONTRACT,
    "event_ingresses": [
        _event_ingress_dict(item)
        for item in sorted(_EVENT_INGRESSES, key=lambda x: x.ingress_id)
    ],
    "method": METHOD,
    "migrations": {
        "contract": MIGRATION_CONTRACT,
        "entries": [
            item.as_dict()
            for item in sorted(_CATALOG_MIGRATIONS, key=lambda x: x.legacy_id)
        ],
        "transparent_aliases": False,
    },
    "workloads": [_definition_dict(item) for item in sorted(_WORKLOADS, key=lambda x: x.workload_id)],
}
CATALOG_SHA256 = _sha256(_CATALOG_PAYLOAD)
_CADENCE_PAYLOAD = {
    "cadence_profile_id": CADENCE_PROFILE_ID,
    "rules": [
        {
            "budget": item.budget.as_dict() if item.budget is not None else None,
            "cadence_seconds": item.cadence_seconds,
            "deadline_seconds": item.deadline_seconds,
            "priority": item.priority,
            "workload_id": item.workload_id,
        }
        for item in sorted(_WORKLOADS, key=lambda x: x.workload_id)
    ],
}
CADENCE_PROFILE_SHA256 = _sha256(_CADENCE_PAYLOAD)
KNOWN_SECRET_REQUIREMENT_IDS = frozenset(
    value for item in _WORKLOADS for value in item.secret_requirement_ids
)
EVENT_INGRESS_IDS = frozenset(item.ingress_id for item in _EVENT_INGRESSES)
CATALOG_MIGRATIONS = _CATALOG_MIGRATIONS


def acquisition_catalog_snapshot() -> dict[str, Any]:
    """Return bounded immutable capability facts without handler references."""

    result = dict(_CATALOG_PAYLOAD)
    result["catalog_sha256"] = CATALOG_SHA256
    result["cadence_profile_id"] = CADENCE_PROFILE_ID
    result["cadence_profile_sha256"] = CADENCE_PROFILE_SHA256
    return json.loads(_canonical_json(result))


def _build_regions(snapshot: DesiredState) -> tuple[AcquisitionRegion, ...]:
    targets = tuple(target for target in snapshot.targets if target.available)
    clusters: list[list[Target]] = []
    threshold = snapshot.settings.region_sharing_distance_km
    # Deterministic complete-link clustering avoids a long chain turning into
    # one physically huge query area.  Target order is canonical target_id.
    for target in targets:
        destination: list[Target] | None = None
        for cluster in clusters:
            if all(_haversine_km(target, member) <= threshold for member in cluster):
                destination = cluster
                break
        if destination is None:
            clusters.append([target])
        else:
            destination.append(target)
    if len(clusters) > MAX_REGIONS:
        raise AcquisitionCapacityError("desired-state levert te veel acquisitieregio's")
    regions: list[AcquisitionRegion] = []
    for cluster in clusters:
        ordered = tuple(sorted(cluster, key=lambda item: item.target_id))
        latitude, longitude = _spherical_center(ordered)
        radius = max(
            target.observation_radius_km
            + _point_distance_km(latitude, longitude, target.latitude, target.longitude)
            for target in ordered
        )
        countries = tuple(sorted({item.country_code for item in ordered if item.country_code}))
        identity = {
            "center_latitude": latitude,
            "center_longitude": longitude,
            "contract": REGION_CONTRACT,
            "country_codes": list(countries),
            "radius_km": _normal_float(radius),
            "target_ids": [item.target_id for item in ordered],
        }
        digest = _sha256(identity)
        regions.append(
            AcquisitionRegion(
                region_id=f"region:{digest}",
                center_latitude=latitude,
                center_longitude=longitude,
                radius_km=_normal_float(radius),
                target_ids=tuple(item.target_id for item in ordered),
                country_codes=countries,
                region_sha256=digest,
            )
        )
    return tuple(sorted(regions, key=lambda item: item.region_id))


def build_worker_acquisition_regions(
    snapshot: DesiredState,
) -> tuple[AcquisitionRegion, ...]:
    """Return the exact validated region partition without building intents."""

    return _build_regions(_validated_snapshot(snapshot))


def _bbox_contains(
    bbox: tuple[float, float, float, float] | None,
    region: AcquisitionRegion,
) -> bool:
    if bbox is None:
        return True
    south, west, north, east = bbox
    latitude = region.center_latitude
    longitude = region.center_longitude
    return south <= latitude <= north and west <= longitude <= east


def _coverage_regions(
    definition: _WorkloadDefinition,
    regions: tuple[AcquisitionRegion, ...],
) -> tuple[AcquisitionRegion, ...]:
    selected: list[AcquisitionRegion] = []
    countries = set(definition.country_codes)
    for region in regions:
        country_match = bool(countries.intersection(region.country_codes))
        if definition.country_codes:
            if country_match or (not region.country_codes and _bbox_contains(definition.coverage_bbox, region)):
                selected.append(region)
        elif _bbox_contains(definition.coverage_bbox, region):
            selected.append(region)
    return tuple(selected)


def _make_intent(
    definition: _WorkloadDefinition,
    scope_id: str,
    regions: Sequence[AcquisitionRegion],
) -> AcquisitionIntent:
    if definition.budget is None:
        raise AcquisitionPlanError("dispatchbare workload mist een bewezen budget")
    region_ids = tuple(sorted(item.region_id for item in regions))
    target_ids = tuple(sorted({value for item in regions for value in item.target_ids}))
    countries = tuple(sorted({value for item in regions for value in item.country_codes}))
    immutable = {
        "budget": definition.budget.as_dict(),
        "cadence_seconds": definition.cadence_seconds,
        "contract": INTENT_CONTRACT,
        "country_codes": list(countries),
        "deadline_seconds": definition.deadline_seconds,
        "priority": definition.priority,
        "product_id": definition.product_id,
        "provider_id": definition.provider_id,
        "region_ids": list(region_ids),
        "satellite_id": definition.satellite_id,
        "scope_id": scope_id,
        "secret_requirement_ids": list(definition.secret_requirement_ids),
        "target_ids": list(target_ids),
        "workload_class": definition.workload_class.value,
        "workload_id": definition.workload_id,
    }
    digest = _sha256(immutable)
    return AcquisitionIntent(
        intent_id=f"intent:{digest}",
        workload_id=definition.workload_id,
        workload_class=definition.workload_class.value,
        provider_id=definition.provider_id,
        product_id=definition.product_id,
        scope_id=scope_id,
        region_ids=region_ids,
        target_ids=target_ids,
        country_codes=countries,
        satellite_id=definition.satellite_id,
        cadence_seconds=definition.cadence_seconds,
        deadline_seconds=definition.deadline_seconds,
        priority=definition.priority,
        budget=definition.budget,
        secret_requirement_ids=definition.secret_requirement_ids,
        intent_sha256=digest,
    )


def _build_intents(
    snapshot: DesiredState,
    regions: tuple[AcquisitionRegion, ...],
    *,
    collector_scope: str,
) -> tuple[AcquisitionIntent, ...]:
    if not regions:
        return ()
    built: dict[tuple[str, str], AcquisitionIntent] = {}
    for definition in _WORKLOADS:
        if definition.status is not CapabilityStatus.CONTRACT_READY_UNWIRED:
            continue
        if (
            collector_scope == COLLECTOR_SCOPE_COVERAGE_RELEVANT
            and
            definition.workload_class is WorkloadClass.MODEL
            and not snapshot.settings.model_guidance_enabled
        ):
            continue
        # ``all_sources`` widens the source *classes* admitted for collection
        # (notably model guidance), never their geographic coverage.  A
        # country- or bbox-scoped provider must remain absent until at least
        # one current target falls inside its declared catalog scope.
        selected = _coverage_regions(definition, regions)
        scopes: list[tuple[str, tuple[AcquisitionRegion, ...]]] = []
        if definition.scope_mode == "global":
            # A globally deduplicated request may still describe a
            # geographically bounded catalog (for example the ItaliaMeteo
            # historical catalog).  Deduplication controls request count; it
            # must not bypass the source's country/bbox applicability.
            if selected:
                scopes.append(("global", selected))
        elif definition.scope_mode in {"coverage", "satellite"}:
            if selected:
                scopes.append((f"coverage:{definition.coverage_id}", selected))
        elif definition.scope_mode == "region":
            for region in selected:
                scopes.append((region.region_id, (region,)))
        else:
            raise AcquisitionPlanError("dispatchable workload heeft ongeldige scope")
        for scope_id, scope_regions in scopes:
            intent = _make_intent(definition, scope_id, scope_regions)
            key = (definition.workload_id, scope_id)
            previous = built.get(key)
            if previous is not None and previous != intent:
                raise AcquisitionConflictError("gededupliceerde workloadscope heeft conflict")
            built[key] = intent
            if len(built) > MAX_INTENTS:
                raise AcquisitionCapacityError("acquisitieplan overschrijdt de intentgrens")
    return tuple(
        sorted(
            built.values(),
            key=lambda item: (
                item.priority,
                item.workload_class,
                item.provider_id,
                item.product_id,
                item.scope_id,
                item.intent_id,
            ),
        )
    )


def _blocked_capabilities() -> tuple[BlockedCapability, ...]:
    return tuple(
        BlockedCapability(
            workload_id=item.workload_id,
            workload_class=item.workload_class.value,
            provider_id=item.provider_id,
            product_id=item.product_id,
            reason_code=str(item.blocked_reason),
        )
        for item in sorted(_WORKLOADS, key=lambda value: value.workload_id)
        if item.status is not CapabilityStatus.CONTRACT_READY_UNWIRED
    )


def _plan_payload(plan: AcquisitionPlan) -> dict[str, Any]:
    return {
        "authority_epoch": plan.authority_epoch,
        "blocked_capabilities": [item.as_dict() for item in plan.blocked_capabilities],
        "cadence_profile_id": plan.cadence_profile_id,
        "cadence_profile_sha256": plan.cadence_profile_sha256,
        "catalog_sha256": plan.catalog_sha256,
        "collector_scope": plan.collector_scope,
        "contract": PLAN_CONTRACT,
        "generation": plan.generation,
        "intents": [item.as_dict() for item in plan.intents],
        "method": METHOD,
        "producer_id": plan.producer_id,
        "regions": [item.as_dict() for item in plan.regions],
        "snapshot_contract": plan.snapshot_contract,
        "snapshot_sha256": plan.snapshot_sha256,
    }


def validate_collector_scope(value: str) -> str:
    """Return one closed collector scope or reject before plan construction."""

    if not isinstance(value, str) or value not in COLLECTOR_SCOPES:
        raise AcquisitionPlanError("collector_scope valt buiten het gesloten contract")
    return value


def build_worker_acquisition_plan(
    snapshot: DesiredState,
    *,
    collector_scope: str = COLLECTOR_SCOPE_COVERAGE_RELEVANT,
) -> AcquisitionPlan:
    """Build one immutable bounded plan from a canonical desired-state head."""

    state = _validated_snapshot(snapshot)
    scope = validate_collector_scope(collector_scope)
    regions = _build_regions(state)
    intents = _build_intents(state, regions, collector_scope=scope)
    provisional = AcquisitionPlan(
        snapshot_contract=DESIRED_STATE_CONTRACT,
        snapshot_sha256=state.state_sha256,
        producer_id=state.producer_id,
        authority_epoch=state.authority_epoch,
        generation=state.generation,
        cadence_profile_id=CADENCE_PROFILE_ID,
        cadence_profile_sha256=CADENCE_PROFILE_SHA256,
        catalog_sha256=CATALOG_SHA256,
        collector_scope=scope,
        regions=regions,
        intents=intents,
        blocked_capabilities=_blocked_capabilities(),
        canonical_bytes=b"",
        plan_sha256="",
    )
    canonical = _canonical_json(_plan_payload(provisional))
    if len(canonical) > MAX_PLAN_BYTES:
        raise AcquisitionCapacityError("canoniek acquisitieplan overschrijdt het bytebudget")
    digest = hashlib.sha256(canonical).hexdigest()
    return AcquisitionPlan(
        snapshot_contract=provisional.snapshot_contract,
        snapshot_sha256=provisional.snapshot_sha256,
        producer_id=provisional.producer_id,
        authority_epoch=provisional.authority_epoch,
        generation=provisional.generation,
        cadence_profile_id=provisional.cadence_profile_id,
        cadence_profile_sha256=provisional.cadence_profile_sha256,
        catalog_sha256=provisional.catalog_sha256,
        collector_scope=provisional.collector_scope,
        regions=provisional.regions,
        intents=provisional.intents,
        blocked_capabilities=provisional.blocked_capabilities,
        canonical_bytes=canonical,
        plan_sha256=digest,
    )


def validate_worker_acquisition_plan(plan: AcquisitionPlan) -> AcquisitionPlan:
    """Fail closed if any in-memory field, member checksum, or profile drifts."""

    if not isinstance(plan, AcquisitionPlan):
        raise AcquisitionPlanError("dispatcher vereist een AcquisitionPlan")
    if plan.cadence_profile_id != CADENCE_PROFILE_ID:
        raise AcquisitionConflictError("cadence profile-id is gedrift")
    if plan.cadence_profile_sha256 != CADENCE_PROFILE_SHA256:
        raise AcquisitionConflictError("cadence profilechecksum is gedrift")
    if plan.catalog_sha256 != CATALOG_SHA256:
        raise AcquisitionConflictError("cataloguschecksum is gedrift")
    validate_collector_scope(plan.collector_scope)
    if plan.snapshot_contract != DESIRED_STATE_CONTRACT:
        raise AcquisitionConflictError("desired-state contractbinding is gedrift")
    if not _SHA256.fullmatch(plan.snapshot_sha256):
        raise AcquisitionPlanError("snapshotchecksum is ongeldig")
    if not _SHA256.fullmatch(plan.plan_sha256):
        raise AcquisitionPlanError("planchecksum is ongeldig")
    if len(plan.regions) > MAX_REGIONS or len(plan.intents) > MAX_INTENTS:
        raise AcquisitionCapacityError("planmembers overschrijden de harde grens")
    if tuple(sorted(plan.regions, key=lambda item: item.region_id)) != plan.regions:
        raise AcquisitionConflictError("regio's staan niet in canonieke volgorde")
    if len({item.region_id for item in plan.regions}) != len(plan.regions):
        raise AcquisitionConflictError("plan bevat dubbele regio-identiteiten")
    known_region_ids = {item.region_id for item in plan.regions}
    known_target_ids = {target for item in plan.regions for target in item.target_ids}
    regions_by_id = {item.region_id: item for item in plan.regions}
    for region in plan.regions:
        if not _SHA256.fullmatch(region.region_sha256):
            raise AcquisitionPlanError("regiochecksum is ongeldig")
        if not region.region_id == f"region:{region.region_sha256}":
            raise AcquisitionConflictError("region identity en checksum verschillen")
        region_identity = {
            "center_latitude": region.center_latitude,
            "center_longitude": region.center_longitude,
            "contract": REGION_CONTRACT,
            "country_codes": list(region.country_codes),
            "radius_km": region.radius_km,
            "target_ids": list(region.target_ids),
        }
        if _sha256(region_identity) != region.region_sha256:
            raise AcquisitionConflictError("regiochecksum bindt de regiovelden niet")
        if tuple(sorted(set(region.target_ids))) != region.target_ids:
            raise AcquisitionConflictError("regiotargets zijn niet canoniek uniek")
        if tuple(sorted(set(region.country_codes))) != region.country_codes:
            raise AcquisitionConflictError("regiolanden zijn niet canoniek uniek")
        if not (-90.0 <= region.center_latitude <= 90.0):
            raise AcquisitionPlanError("regiolatitude ligt buiten de grens")
        if not (-180.0 <= region.center_longitude <= 180.0):
            raise AcquisitionPlanError("regiolongitude ligt buiten de grens")
        if not (0.0 < region.radius_km <= 20_100.0):
            raise AcquisitionPlanError("regioradius ligt buiten de grens")
    expected_intent_order = tuple(
        sorted(
            plan.intents,
            key=lambda item: (
                item.priority,
                item.workload_class,
                item.provider_id,
                item.product_id,
                item.scope_id,
                item.intent_id,
            ),
        )
    )
    if expected_intent_order != plan.intents:
        raise AcquisitionConflictError("intents staan niet in canonieke volgorde")
    if len({item.intent_id for item in plan.intents}) != len(plan.intents):
        raise AcquisitionConflictError("plan bevat dubbele intent-identiteiten")
    for intent in plan.intents:
        if not _SHA256.fullmatch(intent.intent_sha256):
            raise AcquisitionPlanError("intentchecksum is ongeldig")
        if intent.intent_id != f"intent:{intent.intent_sha256}":
            raise AcquisitionConflictError("intent identity en checksum verschillen")
        _validate_budget(intent.budget)
        intent_identity = {
            "budget": intent.budget.as_dict(),
            "cadence_seconds": intent.cadence_seconds,
            "contract": INTENT_CONTRACT,
            "country_codes": list(intent.country_codes),
            "deadline_seconds": intent.deadline_seconds,
            "priority": intent.priority,
            "product_id": intent.product_id,
            "provider_id": intent.provider_id,
            "region_ids": list(intent.region_ids),
            "satellite_id": intent.satellite_id,
            "scope_id": intent.scope_id,
            "secret_requirement_ids": list(intent.secret_requirement_ids),
            "target_ids": list(intent.target_ids),
            "workload_class": intent.workload_class,
            "workload_id": intent.workload_id,
        }
        if _sha256(intent_identity) != intent.intent_sha256:
            raise AcquisitionConflictError("intentchecksum bindt de intentvelden niet")
        if tuple(sorted(set(intent.region_ids))) != intent.region_ids:
            raise AcquisitionConflictError("intentregio's zijn niet canoniek uniek")
        if not set(intent.region_ids).issubset(known_region_ids):
            raise AcquisitionConflictError("intent verwijst naar onbekende regio")
        if tuple(sorted(set(intent.target_ids))) != intent.target_ids:
            raise AcquisitionConflictError("intenttargets zijn niet canoniek uniek")
        if not set(intent.target_ids).issubset(known_target_ids):
            raise AcquisitionConflictError("intent verwijst naar onbekend target")
        if tuple(sorted(set(intent.country_codes))) != intent.country_codes:
            raise AcquisitionConflictError("intentlanden zijn niet canoniek uniek")
        scoped_regions = tuple(regions_by_id[value] for value in intent.region_ids)
        expected_targets = tuple(
            sorted({target for region in scoped_regions for target in region.target_ids})
        )
        expected_countries = tuple(
            sorted({country for region in scoped_regions for country in region.country_codes})
        )
        if intent.target_ids != expected_targets:
            raise AcquisitionConflictError("intenttargets dekken de scoped regio's niet exact")
        if intent.country_codes != expected_countries:
            raise AcquisitionConflictError("intentlanden dekken de scoped regio's niet exact")
        matching_definition = next(
            (item for item in _WORKLOADS if item.workload_id == intent.workload_id),
            None,
        )
        if matching_definition is None:
            raise AcquisitionConflictError("intent verwijst naar onbekende workload")
        if matching_definition.status is not CapabilityStatus.CONTRACT_READY_UNWIRED:
            raise AcquisitionConflictError("unsupported capability werd dispatchbaar gemaakt")
        if (
            intent.workload_class != matching_definition.workload_class.value
            or intent.provider_id != matching_definition.provider_id
            or intent.product_id != matching_definition.product_id
            or intent.satellite_id != matching_definition.satellite_id
            or intent.cadence_seconds != matching_definition.cadence_seconds
            or intent.deadline_seconds != matching_definition.deadline_seconds
            or intent.priority != matching_definition.priority
            or intent.budget != matching_definition.budget
            or intent.secret_requirement_ids
            != matching_definition.secret_requirement_ids
        ):
            raise AcquisitionConflictError("intent wijkt af van de gebonden catalogusregel")
    if plan.blocked_capabilities != _blocked_capabilities():
        raise AcquisitionConflictError("fail-closed capabilitylijst is gedrift")
    expected = _canonical_json(_plan_payload(plan))
    if len(expected) > MAX_PLAN_BYTES:
        raise AcquisitionCapacityError("plan overschrijdt het bytebudget")
    if expected != plan.canonical_bytes:
        raise AcquisitionConflictError("planvelden en canonical bytes verschillen")
    if hashlib.sha256(expected).hexdigest() != plan.plan_sha256:
        raise AcquisitionConflictError("planchecksum komt niet overeen")
    return plan


def _validate_capacity(value: DispatchCapacity) -> DispatchCapacity:
    if not isinstance(value, DispatchCapacity):
        raise AcquisitionCapacityError("dispatcher vereist DispatchCapacity")
    _positive_integer(value.max_jobs, "max_jobs", MAX_DISPATCH_JOBS)
    _positive_integer(
        value.max_transport_bytes,
        "max_transport_bytes",
        MAX_DISPATCH_TRANSPORT_BYTES,
    )
    _positive_integer(value.max_artifacts, "max_artifacts", MAX_DISPATCH_ARTIFACTS)
    _positive_integer(
        value.max_wall_seconds,
        "max_wall_seconds",
        MAX_DISPATCH_WALL_SECONDS,
    )
    return value


def _validate_secret_availability(values: Iterable[str]) -> frozenset[str]:
    try:
        result = frozenset(values)
    except TypeError as err:
        raise AcquisitionPlanError("secret availability moet een verzameling identifiers zijn") from err
    if len(result) > len(KNOWN_SECRET_REQUIREMENT_IDS):
        raise AcquisitionPlanError("secret availability overschrijdt de identifiergrens")
    for value in result:
        if not isinstance(value, str) or _SECRET_REQUIREMENT.fullmatch(value) is None:
            raise AcquisitionPlanError("secret availability bevat geen abstracte identifier")
        if value not in KNOWN_SECRET_REQUIREMENT_IDS:
            raise AcquisitionPlanError("secret availability bevat een onbekende identifier")
    return result


def _job_for_slot(plan: AcquisitionPlan, intent: AcquisitionIntent, as_of: float) -> AcquisitionJob:
    slot_index = int(math.floor(as_of / intent.cadence_seconds))
    scheduled_for = float(slot_index * intent.cadence_seconds)
    deadline_at = scheduled_for + float(intent.deadline_seconds)
    identity = {
        "budget": intent.budget.as_dict(),
        "cadence_seconds": intent.cadence_seconds,
        "contract": JOB_CONTRACT,
        "country_codes": list(intent.country_codes),
        "deadline_at": deadline_at,
        "intent_id": intent.intent_id,
        "intent_sha256": intent.intent_sha256,
        "plan_sha256": plan.plan_sha256,
        "priority": intent.priority,
        "product_id": intent.product_id,
        "provider_id": intent.provider_id,
        "region_ids": list(intent.region_ids),
        "satellite_id": intent.satellite_id,
        "scheduled_for": scheduled_for,
        "scope_id": intent.scope_id,
        "secret_requirement_ids": list(intent.secret_requirement_ids),
        "slot_index": slot_index,
        "snapshot_sha256": plan.snapshot_sha256,
        "target_ids": list(intent.target_ids),
        "workload_class": intent.workload_class,
        "workload_id": intent.workload_id,
    }
    identity_sha = _sha256(identity)
    job_id = f"acquisition:{identity_sha}"
    record = dict(identity)
    record["job_id"] = job_id
    job_sha = _sha256(record)
    return AcquisitionJob(
        job_id=job_id,
        job_sha256=job_sha,
        plan_sha256=plan.plan_sha256,
        snapshot_sha256=plan.snapshot_sha256,
        intent_id=intent.intent_id,
        intent_sha256=intent.intent_sha256,
        workload_id=intent.workload_id,
        workload_class=intent.workload_class,
        provider_id=intent.provider_id,
        product_id=intent.product_id,
        scope_id=intent.scope_id,
        region_ids=intent.region_ids,
        target_ids=intent.target_ids,
        country_codes=intent.country_codes,
        satellite_id=intent.satellite_id,
        cadence_seconds=intent.cadence_seconds,
        slot_index=slot_index,
        scheduled_for=scheduled_for,
        deadline_at=deadline_at,
        priority=intent.priority,
        budget=intent.budget,
        secret_requirement_ids=intent.secret_requirement_ids,
    )


def build_worker_acquisition_slot_jobs(
    plan: AcquisitionPlan,
    *,
    as_of: float,
) -> tuple[AcquisitionJob, ...]:
    """Build every deterministic current slot identity without dispatch filtering."""

    valid_plan = validate_worker_acquisition_plan(plan)
    moment = _finite_time(as_of, "as_of")
    return tuple(
        _job_for_slot(valid_plan, intent, moment)
        for intent in valid_plan.intents
    )


def _validate_job(job: AcquisitionJob) -> AcquisitionJob:
    if not isinstance(job, AcquisitionJob):
        raise AcquisitionPlanError("job heeft het verkeerde type")
    if not _SHA256.fullmatch(job.plan_sha256) or not _SHA256.fullmatch(job.snapshot_sha256):
        raise AcquisitionPlanError("job mist een geldige plan- of snapshotchecksum")
    if not _SHA256.fullmatch(job.intent_sha256) or not _SHA256.fullmatch(job.job_sha256):
        raise AcquisitionPlanError("job mist een geldige memberchecksum")
    if not job.intent_id == f"intent:{job.intent_sha256}":
        raise AcquisitionConflictError("job intentidentity en checksum verschillen")
    _validate_budget(job.budget)
    identity = {
        "budget": job.budget.as_dict(),
        "cadence_seconds": job.cadence_seconds,
        "contract": JOB_CONTRACT,
        "country_codes": list(job.country_codes),
        "deadline_at": job.deadline_at,
        "intent_id": job.intent_id,
        "intent_sha256": job.intent_sha256,
        "plan_sha256": job.plan_sha256,
        "priority": job.priority,
        "product_id": job.product_id,
        "provider_id": job.provider_id,
        "region_ids": list(job.region_ids),
        "satellite_id": job.satellite_id,
        "scheduled_for": job.scheduled_for,
        "scope_id": job.scope_id,
        "secret_requirement_ids": list(job.secret_requirement_ids),
        "slot_index": job.slot_index,
        "snapshot_sha256": job.snapshot_sha256,
        "target_ids": list(job.target_ids),
        "workload_class": job.workload_class,
        "workload_id": job.workload_id,
    }
    identity_sha = _sha256(identity)
    if job.job_id != f"acquisition:{identity_sha}":
        raise AcquisitionConflictError("job identity bindt de jobvelden niet")
    record = dict(identity)
    record["job_id"] = job.job_id
    if _sha256(record) != job.job_sha256:
        raise AcquisitionConflictError("jobchecksum bindt de jobvelden niet")
    if job.scheduled_for != float(job.slot_index * job.cadence_seconds):
        raise AcquisitionConflictError("jobslot en scheduled_for verschillen")
    if not job.scheduled_for < job.deadline_at:
        raise AcquisitionPlanError("jobdeadline ligt niet na de slottijd")
    return job


def validate_worker_acquisition_job(job: AcquisitionJob) -> AcquisitionJob:
    """Validate the complete immutable slot-job identity and field binding."""

    return _validate_job(job)


_TERMINAL_OUTCOMES = frozenset({"succeeded", "terminal_failed", "deadline_expired"})


def make_worker_acquisition_receipt(
    job: AcquisitionJob,
    *,
    outcome: str,
    completed_at: float,
    artifact_receipt_sha256: str | None = None,
    error_code: str | None = None,
) -> AcquisitionReceipt:
    """Create a self-checksummed terminal receipt bound to one exact job."""

    job = _validate_job(job)
    if outcome not in _TERMINAL_OUTCOMES:
        raise AcquisitionPlanError("receipt outcome is niet terminaal ondersteund")
    completed = _finite_time(completed_at, "completed_at")
    if completed < job.scheduled_for:
        raise AcquisitionPlanError("receipt kan niet voor het cadansslot eindigen")
    if outcome == "deadline_expired":
        if completed < job.deadline_at:
            raise AcquisitionPlanError("deadline_expired vereist verstreken deadline")
    elif completed > job.deadline_at:
        raise AcquisitionPlanError("terminal receipt valt buiten de jobdeadline")
    if outcome == "succeeded":
        if not isinstance(artifact_receipt_sha256, str) or _SHA256.fullmatch(artifact_receipt_sha256) is None:
            raise AcquisitionPlanError("succesreceipt vereist een artifact-receiptchecksum")
        if error_code is not None:
            raise AcquisitionPlanError("succesreceipt mag geen error_code bevatten")
    else:
        if artifact_receipt_sha256 is not None:
            raise AcquisitionPlanError("faalreceipt mag geen artifact-receiptchecksum bevatten")
        if not isinstance(error_code, str) or _ERROR_CODE.fullmatch(error_code) is None:
            raise AcquisitionPlanError("faalreceipt vereist een begrensde error_code")
    payload = {
        "artifact_receipt_sha256": artifact_receipt_sha256,
        "completed_at": completed,
        "contract": RECEIPT_CONTRACT,
        "error_code": error_code,
        "job_id": job.job_id,
        "job_sha256": job.job_sha256,
        "outcome": outcome,
        "plan_sha256": job.plan_sha256,
    }
    receipt_sha = _sha256(payload)
    return AcquisitionReceipt(
        plan_sha256=job.plan_sha256,
        job_id=job.job_id,
        job_sha256=job.job_sha256,
        outcome=outcome,
        completed_at=completed,
        artifact_receipt_sha256=artifact_receipt_sha256,
        error_code=error_code,
        receipt_sha256=receipt_sha,
    )


def validate_worker_acquisition_receipt(
    receipt: AcquisitionReceipt,
    *,
    plan_sha256: str,
) -> AcquisitionReceipt:
    if not isinstance(receipt, AcquisitionReceipt):
        raise AcquisitionPlanError("receipt heeft het verkeerde type")
    if receipt.plan_sha256 != plan_sha256:
        raise AcquisitionConflictError("receipt hoort bij een ander plan")
    if not _SHA256.fullmatch(receipt.job_sha256) or not _SHA256.fullmatch(receipt.receipt_sha256):
        raise AcquisitionPlanError("receipt bevat een ongeldige checksum")
    _validate_identifier(receipt.job_id, "receipt.job_id")
    _finite_time(receipt.completed_at, "receipt.completed_at")
    if receipt.outcome not in _TERMINAL_OUTCOMES:
        raise AcquisitionPlanError("receipt outcome is ongeldig")
    payload = receipt.as_dict()
    payload.pop("receipt_sha256")
    if _sha256(payload) != receipt.receipt_sha256:
        raise AcquisitionConflictError("receiptchecksum komt niet overeen")
    if receipt.outcome == "succeeded":
        if receipt.artifact_receipt_sha256 is None or _SHA256.fullmatch(receipt.artifact_receipt_sha256) is None:
            raise AcquisitionPlanError("succesreceipt mist artifactbinding")
        if receipt.error_code is not None:
            raise AcquisitionPlanError("succesreceipt bevat een foutcode")
    else:
        if receipt.artifact_receipt_sha256 is not None:
            raise AcquisitionPlanError("faalreceipt bevat artifactbinding")
        if receipt.error_code is None or _ERROR_CODE.fullmatch(receipt.error_code) is None:
            raise AcquisitionPlanError("faalreceipt mist geldige foutcode")
    return receipt


def _validate_receipt_for_job(
    receipt: AcquisitionReceipt,
    job: AcquisitionJob,
) -> AcquisitionReceipt:
    valid_job = _validate_job(job)
    valid = validate_worker_acquisition_receipt(
        receipt,
        plan_sha256=valid_job.plan_sha256,
    )
    if valid.job_id != valid_job.job_id or valid.job_sha256 != valid_job.job_sha256:
        raise AcquisitionConflictError("receipt jobchecksum botst met de cadencejob")
    if valid.completed_at < valid_job.scheduled_for:
        raise AcquisitionConflictError("receipt eindigt voor het cadence-slot")
    if valid.outcome == "deadline_expired":
        if valid.completed_at < valid_job.deadline_at:
            raise AcquisitionConflictError("deadline-receipt is te vroeg")
    elif valid.completed_at > valid_job.deadline_at:
        raise AcquisitionConflictError("terminal receipt ligt na de jobdeadline")
    return valid


def _receipt_index(
    receipts: Iterable[AcquisitionReceipt],
    *,
    plan_sha256: str,
) -> dict[str, AcquisitionReceipt]:
    result: dict[str, AcquisitionReceipt] = {}
    count = 0
    for receipt in receipts:
        count += 1
        if count > MAX_RECEIPTS:
            raise AcquisitionCapacityError("receiptset overschrijdt de harde grens")
        valid = validate_worker_acquisition_receipt(receipt, plan_sha256=plan_sha256)
        previous = result.get(valid.job_id)
        if previous is not None and previous != valid:
            raise AcquisitionConflictError("job heeft twee verschillende terminal receipts")
        result[valid.job_id] = valid
    return result


def build_worker_acquisition_dispatch(
    plan: AcquisitionPlan,
    *,
    as_of: float,
    available_secret_requirement_ids: Iterable[str] = (),
    receipts: Iterable[AcquisitionReceipt] = (),
    capacity: DispatchCapacity = DispatchCapacity(),
) -> DispatchSnapshot:
    """Derive a deterministic bounded serial queue for the current slots."""

    valid_plan = validate_worker_acquisition_plan(plan)
    now = _finite_time(as_of, "as_of")
    limits = _validate_capacity(capacity)
    available = _validate_secret_availability(available_secret_requirement_ids)
    terminal = _receipt_index(receipts, plan_sha256=valid_plan.plan_sha256)
    ready: list[AcquisitionJob] = []
    blocked: list[DispatchBlock] = []
    deferred: list[DispatchBlock] = []
    completed: list[str] = []
    used_bytes = used_artifacts = used_seconds = 0
    capacity_closed = False
    for intent in valid_plan.intents:
        job = _job_for_slot(valid_plan, intent, now)
        receipt = terminal.get(job.job_id)
        if receipt is not None:
            _validate_receipt_for_job(receipt, job)
            completed.append(job.job_id)
            continue
        if now >= job.deadline_at:
            blocked.append(
                DispatchBlock(job.job_id, job.provider_id, job.product_id, "deadline_expired")
            )
            continue
        if not set(job.secret_requirement_ids).issubset(available):
            blocked.append(
                DispatchBlock(
                    job.job_id,
                    job.provider_id,
                    job.product_id,
                    "secret_requirement_unavailable",
                )
            )
            continue
        exceeds = (
            len(ready) + 1 > limits.max_jobs
            or used_bytes + job.budget.max_transport_bytes > limits.max_transport_bytes
            or used_artifacts + job.budget.max_artifacts > limits.max_artifacts
            or used_seconds + job.budget.max_wall_seconds > limits.max_wall_seconds
        )
        if capacity_closed or exceeds:
            capacity_closed = True
            deferred.append(
                DispatchBlock(
                    job.job_id,
                    job.provider_id,
                    job.product_id,
                    "dispatch_capacity_exhausted",
                )
            )
            continue
        ready.append(job)
        used_bytes += job.budget.max_transport_bytes
        used_artifacts += job.budget.max_artifacts
        used_seconds += job.budget.max_wall_seconds
    payload = {
        "as_of": now,
        "blocked": [item.as_dict() for item in blocked],
        "capacity": limits.as_dict(),
        "completed_job_ids": sorted(completed),
        "contract": DISPATCH_CONTRACT,
        "deferred": [item.as_dict() for item in deferred],
        "plan_sha256": valid_plan.plan_sha256,
        "ready_job_sha256": [item.job_sha256 for item in ready],
        "serial_concurrency": 1,
    }
    return DispatchSnapshot(
        plan_sha256=valid_plan.plan_sha256,
        as_of=now,
        serial_concurrency=1,
        ready_jobs=tuple(ready),
        blocked=tuple(blocked),
        deferred=tuple(deferred),
        completed_job_ids=tuple(sorted(completed)),
        capacity=limits,
        dispatch_sha256=_sha256(payload),
    )


class WorkerAcquisitionDispatcher:
    """In-memory one-at-a-time dispatcher with deterministic crash replay.

    Reconstructing this object with the same plan and terminal receipts yields
    the same next job.  ``claim_next`` never hands out a second concurrent job;
    repeated claims return the same in-flight identity until a receipt is
    accepted or its deadline expires.
    """

    def __init__(
        self,
        plan: AcquisitionPlan,
        *,
        available_secret_requirement_ids: Iterable[str] = (),
        receipts: Iterable[AcquisitionReceipt] = (),
        capacity: DispatchCapacity = DispatchCapacity(),
    ) -> None:
        self.plan = validate_worker_acquisition_plan(plan)
        self.capacity = _validate_capacity(capacity)
        self.available_secret_requirement_ids = _validate_secret_availability(
            available_secret_requirement_ids
        )
        self._receipts = _receipt_index(receipts, plan_sha256=self.plan.plan_sha256)
        self._inflight: AcquisitionJob | None = None
        self._lock = threading.Lock()

    def _snapshot(self, as_of: float) -> DispatchSnapshot:
        return build_worker_acquisition_dispatch(
            self.plan,
            as_of=as_of,
            available_secret_requirement_ids=self.available_secret_requirement_ids,
            receipts=self._receipts.values(),
            capacity=self.capacity,
        )

    def snapshot(self, *, as_of: float) -> DispatchSnapshot:
        with self._lock:
            return self._snapshot(as_of)

    def claim_next(self, *, as_of: float) -> AcquisitionJob | None:
        with self._lock:
            now = _finite_time(as_of, "as_of")
            if self._inflight is not None:
                if self._inflight.job_id in self._receipts or now >= self._inflight.deadline_at:
                    self._inflight = None
                else:
                    return self._inflight
            self._inflight = self._snapshot(now).next_job
            return self._inflight

    def accept_receipt(self, receipt: AcquisitionReceipt) -> AcquisitionReceipt:
        with self._lock:
            valid = validate_worker_acquisition_receipt(
                receipt, plan_sha256=self.plan.plan_sha256
            )
            if self._inflight is None or valid.job_id != self._inflight.job_id:
                raise AcquisitionConflictError("receipt hoort niet bij de seriële in-flight job")
            _validate_receipt_for_job(valid, self._inflight)
            previous = self._receipts.get(valid.job_id)
            if previous is not None and previous != valid:
                raise AcquisitionConflictError("in-flight job heeft al een andere receipt")
            if len(self._receipts) >= MAX_RECEIPTS and previous is None:
                raise AcquisitionCapacityError("dispatcher receiptcapaciteit is uitgeput")
            self._receipts[valid.job_id] = valid
            self._inflight = None
            return valid

    def receipts(self) -> tuple[AcquisitionReceipt, ...]:
        with self._lock:
            return tuple(self._receipts[key] for key in sorted(self._receipts))


__all__ = [
    "AcquisitionBudget",
    "AcquisitionCapacityError",
    "AcquisitionConflictError",
    "AcquisitionIntent",
    "AcquisitionJob",
    "AcquisitionPlan",
    "AcquisitionPlanError",
    "AcquisitionReceipt",
    "AcquisitionRegion",
    "AcquisitionSnapshotError",
    "BlockedCapability",
    "CADENCE_PROFILE_ID",
    "CADENCE_PROFILE_SHA256",
    "CATALOG_SHA256",
    "CATALOG_MIGRATIONS",
    "CatalogMigration",
    "CapabilityStatus",
    "COLLECTOR_SCOPE_ALL_SOURCES",
    "COLLECTOR_SCOPE_COVERAGE_RELEVANT",
    "COLLECTOR_SCOPES",
    "DispatchBlock",
    "DispatchCapacity",
    "DispatchSnapshot",
    "EVENT_INGRESS_CONTRACT",
    "EVENT_INGRESS_IDS",
    "EventIngressStatus",
    "KNOWN_SECRET_REQUIREMENT_IDS",
    "MAX_DISPATCH_JOBS",
    "MAX_INTENTS",
    "MAX_PLAN_BYTES",
    "MAX_RECEIPTS",
    "MAX_REGIONS",
    "METHOD",
    "PLAN_CONTRACT",
    "WorkerAcquisitionDispatcher",
    "WorkloadClass",
    "acquisition_catalog_snapshot",
    "build_worker_acquisition_dispatch",
    "build_worker_acquisition_plan",
    "build_worker_acquisition_regions",
    "build_worker_acquisition_slot_jobs",
    "make_worker_acquisition_receipt",
    "validate_worker_acquisition_plan",
    "validate_collector_scope",
    "validate_worker_acquisition_job",
    "validate_worker_acquisition_receipt",
]
