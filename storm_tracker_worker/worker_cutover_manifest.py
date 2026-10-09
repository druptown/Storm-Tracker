"""Closed, unwired manifest for removing meteorological work from Home Assistant.

The manifest is an audit contract.  It does not schedule, import or execute any
provider and is deliberately not imported by the production service.  Its job
is to make the worker-only cutover complete: every legacy HA workload is either
bound to an existing worker foundation or visibly blocked as a gap.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import re
from types import MappingProxyType


MANIFEST_VERSION = "worker-cutover-manifest-v5"

_ID = re.compile(r"^[a-z][a-z0-9_.:-]{0,127}$")


class CutoverState(str, Enum):
    FOUNDATION_ONLY = "foundation_only"
    SHADOW_CANDIDATE = "shadow_candidate"
    BLOCKED_GAP = "blocked_gap"
    HA_CONTROL_PLANE = "ha_control_plane"


@dataclass(frozen=True, slots=True)
class WorkloadContract:
    workload_id: str
    legacy_ha_entrypoints: tuple[str, ...]
    legacy_provider_modules: tuple[str, ...]
    provider_ids: tuple[str, ...]
    acquisition_workload_ids: tuple[str, ...]
    event_ingress_ids: tuple[str, ...]
    worker_job_types: tuple[str, ...]
    shadow_components: tuple[str, ...]
    state: CutoverState
    ha_allowed_after_cutover: bool
    cutover_gate: str

    def __post_init__(self) -> None:
        if _ID.fullmatch(self.workload_id) is None:
            raise ValueError("workload_id is not canonical")
        for label, values in (
            ("legacy_ha_entrypoints", self.legacy_ha_entrypoints),
            ("legacy_provider_modules", self.legacy_provider_modules),
            ("provider_ids", self.provider_ids),
            ("acquisition_workload_ids", self.acquisition_workload_ids),
            ("event_ingress_ids", self.event_ingress_ids),
            ("worker_job_types", self.worker_job_types),
            ("shadow_components", self.shadow_components),
        ):
            if tuple(sorted(set(values))) != values:
                raise ValueError(f"{label} must be sorted and unique")
            if any(not isinstance(value, str) or not value for value in values):
                raise ValueError(f"{label} contains an invalid value")
        if any(
            _ID.fullmatch(value) is None
            for value in self.acquisition_workload_ids
        ):
            raise ValueError("acquisition_workload_ids contains a non-canonical id")
        if any(_ID.fullmatch(value) is None for value in self.event_ingress_ids):
            raise ValueError("event_ingress_ids contains a non-canonical id")
        if not isinstance(self.state, CutoverState):
            raise TypeError("state is invalid")
        if type(self.ha_allowed_after_cutover) is not bool:
            raise TypeError("ha_allowed_after_cutover must be boolean")
        if self.ha_allowed_after_cutover != (
            self.state is CutoverState.HA_CONTROL_PLANE
        ):
            raise ValueError("only the explicit HA control plane may remain in HA")
        if (
            not isinstance(self.cutover_gate, str)
            or not self.cutover_gate
            or len(self.cutover_gate) > 512
        ):
            raise ValueError("cutover_gate is invalid")

    def to_mapping(self) -> dict[str, object]:
        return {
            "acquisition_workload_ids": list(self.acquisition_workload_ids),
            "cutover_gate": self.cutover_gate,
            "ha_allowed_after_cutover": self.ha_allowed_after_cutover,
            "legacy_ha_entrypoints": list(self.legacy_ha_entrypoints),
            "legacy_provider_modules": list(self.legacy_provider_modules),
            "event_ingress_ids": list(self.event_ingress_ids),
            "provider_ids": list(self.provider_ids),
            "shadow_components": list(self.shadow_components),
            "state": self.state.value,
            "worker_job_types": list(self.worker_job_types),
            "workload_id": self.workload_id,
        }


def _workload(
    workload_id: str,
    *,
    entrypoints: tuple[str, ...] = (),
    legacy_modules: tuple[str, ...] = (),
    providers: tuple[str, ...] = (),
    acquisition_workloads: tuple[str, ...] = (),
    event_ingresses: tuple[str, ...] = (),
    jobs: tuple[str, ...] = (),
    components: tuple[str, ...] = (),
    state: CutoverState,
    gate: str,
    ha_allowed: bool = False,
) -> WorkloadContract:
    return WorkloadContract(
        workload_id=workload_id,
        legacy_ha_entrypoints=tuple(sorted(entrypoints)),
        legacy_provider_modules=tuple(sorted(legacy_modules)),
        provider_ids=tuple(sorted(providers)),
        acquisition_workload_ids=tuple(sorted(acquisition_workloads)),
        event_ingress_ids=tuple(sorted(event_ingresses)),
        worker_job_types=tuple(sorted(jobs)),
        shadow_components=tuple(sorted(components)),
        state=state,
        ha_allowed_after_cutover=ha_allowed,
        cutover_gate=gate,
    )


_WORKLOADS = (
    _workload(
        "provider.national_radar",
        entrypoints=("_poll_national_providers",),
        legacy_modules=(
            "aemet_radar", "dpc_radar", "dwd_radolan", "met_office_radar",
            "meteofrance_radar",
        ),
        providers=(
            "aemet", "aemet_radar", "dpc", "dpc_radar", "dwd",
            "dwd_radolan", "met_office_radar", "meteofrance",
            "meteofrance_radar", "metoffice",
        ),
        acquisition_workloads=(
            "radar.aemet.composite", "radar.dpc.sri",
            "radar.dwd.radolan_rv", "radar.meteofrance.dpradar",
            "radar.metoffice.uk_rate",
        ),
        jobs=(
            "aemet_radar_fetch", "dpc_radar_fetch", "dwd_radolan_fetch",
            "meteofrance_radar_fetch", "odim_hdf5_fetch",
        ),
        components=(
            "aemet_product_gate", "dpc_native", "dwd_rv_quantitative",
            "met_office_native", "met_office_rate_quantitative",
            "meteofrance_native", "provider_capture_registry",
        ),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="all five cadence routes have exact-raw worker handlers and recovery; decoder, grid, replay, rights and meteorological admission certificates remain separate gates",
    ),
    _workload(
        "provider.benelux_comparison_radar",
        entrypoints=("_poll_kmi", "_poll_knmi", "_poll_radar_comparison"),
        legacy_modules=("kmi", "kmi_public_radar_composite", "knmi"),
        providers=("kmi", "knmi"),
        acquisition_workloads=(
            "ground.kmi.aws_10min",
            "radar.kmi.appv4_visual",
            "radar.kmi.public_wcs_unknown_quantity",
            "radar.kmi.radqpe_native",
            "radar.knmi.rtcor",
        ),
        jobs=("kmi_fetch", "knmi_fetch"),
        components=(
            "kmi_opendata_raw", "kmi_radqpe_contract", "knmi_native",
            "knmi_rtcor_quantitative",
        ),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="KNMI RTCOR, KMI app-v4 visual and public WCS have separate exact-raw worker routes; app-v4 is authority-gated, public WCS quantity is unverified and native RADQPE remains externally contract-blocked",
    ),
    _workload(
        "provider.regional_composites",
        entrypoints=("_poll_opera", "_poll_radar", "_poll_rv"),
        legacy_modules=("opera", "rainviewer"),
        providers=("opera", "rainviewer"),
        acquisition_workloads=("radar.opera.dbzh", "radar.rainviewer.visual"),
        jobs=("opera_hdf5_fetch", "rainviewer_fetch"),
        components=("opera_dbzh_quantitative", "opera_native", "rainviewer_native"),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="OPERA has a bounded 384 MiB streaming exact-raw route behind explicit retention authority; quantitative Z-R/QI proof remains absent and RainViewer remains categorical visualisation",
    ),
    _workload(
        "provider.satellite_precipitation",
        entrypoints=("_poll_radar",),
        legacy_modules=(
            "hsaf_h40b", "noaa_enterprise_rainrate", "noaa_goes_rrqpe",
        ),
        providers=(
            "hsaf", "hsaf_h40b", "noaa", "noaa_enterprise_rainrate",
            "noaa_goes", "noaa_goes_rrqpe",
        ),
        acquisition_workloads=(
            "satellite.goes18.rrqpe", "satellite.goes19.rrqpe",
            "satellite.hsaf.h40b", "satellite.noaa.err",
        ),
        jobs=("enterprise_rainrate_fetch", "goes_rrqpe_fetch", "hsaf_h40b_fetch"),
        components=("goes_rrqpe_native", "hsaf_h40b_native", "noaa_err_native", "noaa_err_quantitative"),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="all four cadence routes have exact-raw worker capture and recovery; native decoder/QC/grid validation, held-out events and scientific admission remain open",
    ),
    _workload(
        "provider.ground_networks",
        entrypoints=("_poll_netatmo",),
        legacy_modules=("netatmo",),
        providers=(
            "ambientweather", "aviationweather", "metar", "netatmo", "synoptic",
        ),
        acquisition_workloads=(
            "ground.awn.authorized_devices", "ground.metar.global",
            "ground.netatmo.region", "ground.synoptic.region",
        ),
        jobs=("metar_fetch", "netatmo_fetch", "synoptic_fetch"),
        components=(
            "awn_native", "canonical_ground_evidence_store",
            "metar_ground_evidence", "ground_fusion_shadow_runtime", "metar_native",
            "netatmo_ground_decoder", "netatmo_native",
            "synoptic_ground_decoder", "synoptic_native",
        ),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="AWN exact authorized-device JSON and ground fusion remain shadow only; instantaneous-rain semantics, global site/capability catalogues, footprint certificates, process isolation, retention and held-out proof remain open",
    ),
    _workload(
        "provider.model_guidance",
        entrypoints=("_poll_open_meteo",),
        legacy_modules=("open_meteo",),
        providers=("open_meteo", "openmeteo"),
        acquisition_workloads=(
            "model.openmeteo.italiameteo_icon2i.region",
            "model.openmeteo.region",
        ),
        jobs=("open_meteo_fetch",),
        components=("public_source_raw_capture",),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="both model routes have distinct exact-raw worker capture and recovery; run/model/variable interpretation and forecast-semantics admission remain absent",
    ),
    _workload(
        "provider.validation_only_nowcasts",
        entrypoints=("_poll_national_providers",),
        legacy_modules=("geosphere_at", "italiameteo", "meteolux"),
        providers=(
            "geosphere", "geosphere_at", "italiameteo", "meteolux",
        ),
        acquisition_workloads=(
            "validation.geosphere.location",
            "validation.italiameteo.dpc_sri_historical_catalog",
            "validation.meteolux.location",
        ),
        components=(
            "acquisition_capture_recovery", "acquisition_handler_child",
            "acquisition_location_shadow", "public_source_raw_capture",
        ),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="all three validation-only identities have exact-raw worker handlers and recovery; none may claim realtime radar, decoded weather or meteorological admission",
    ),
    _workload(
        "provider.lightning",
        entrypoints=("_poll_eumetsat_li", "_poll_goes_glm"),
        legacy_modules=("eumetsat_li", "noaa_goes_glm"),
        providers=("eumetsat", "eumetsat_li", "noaa_goes", "noaa_goes_glm"),
        acquisition_workloads=(
            "lightning.eumetsat.li", "lightning.goes18.glm",
            "lightning.goes19.glm",
        ),
        jobs=("eumetsat_li_fetch", "goes_glm_fetch"),
        components=("lightning_exact_raw",),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="all three cadence routes have bounded exact catalogue/object capture and recovery; durable decoded lightning evidence, held-out replay and admission remain open",
    ),
    _workload(
        "provider.blitzortung_push",
        legacy_modules=("blitzortung",),
        providers=("blitzortung",),
        event_ingresses=("lightning.blitzortung.event_ingress",),
        components=(
            "blitzortung_event_ingress", "blitzortung_mqtt_raw_sidecar",
            "blitzortung_raw_ingress", "raw_capture_service_composition",
        ),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="worker-side exact-raw MQTT ingress, bounded backpressure and gap receipts are wired; explicit participant/raw-retention authority, QoS-0 loss limitations, reclamation and live soak remain gates, and quiet intervals never prove absence",
    ),
    _workload(
        "control.worker_acquisition_planning",
        components=(
            "acquisition_activation", "acquisition_execution_store",
            "acquisition_handler_child", "acquisition_runtime",
            "acquisition_scheduler_bridge", "worker_acquisition_plan",
        ),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="raw_capture_shadow owns 27 cadence handlers with recovery and a separate authority-gated Blitzortung sidecar; target materialisation, continuous retention, provider/NUC soak and every downstream meteorological admission remain open",
    ),
    _workload(
        "compute.radar_normalisation",
        entrypoints=("_refresh_radar_overlays",),
        jobs=("radar_frame_from_provider_artifacts", "radar_frame_operational"),
        components=(
            "analysis_grid_transform", "canonical_radar_field_bridge",
            "dwd_rv_analysis_grid_certificate", "evidence_to_cells_shadow",
            "knmi_rtcor_analysis_grid_gate", "science_shadow_orchestrator",
        ),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="durable cell shadow has checkpoint/tail replay; remaining provider certificates, cross-CRS proof, hard process isolation, preemptive deadline and held-out multi-event replays are required",
    ),
    _workload(
        "compute.tracking_and_motion",
        entrypoints=("_poll_radar",),
        jobs=("motion_preflight", "tracking_operational"),
        components=(
            "cell_track", "cell_track_engine", "hierarchical_detection",
            "pyramidal_sparse_flow", "science_shadow_orchestrator",
        ),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="crash-durable state remains unadmitted until hard process isolation, preemptive deadline, external identity truth, real multi-region replays and NUC/HA/Z2M soak pass",
    ),
    _workload(
        "compute.virtual_probe_and_verification",
        entrypoints=("_poll_all", "_refresh_virtual_probes"),
        jobs=("virtual_probe_operational",),
        components=("forecast_verification", "virtual_probe"),
        state=CutoverState.FOUNDATION_ONLY,
        gate="worker-owned track/forecast facts and snapshot publication must replace HA cache evaluation",
    ),
    _workload(
        "compute.calibration_and_replay",
        entrypoints=("_flush_calibration_data", "_flush_calibration_data_impl"),
        jobs=(
            "calibration_corpus_ingest", "calibration_corpus_snapshot",
            "calibration_operational", "calibration_prepare_v2", "phase3_replay",
        ),
        components=("calibration_corpus", "calibration_stage", "radflood21_validation_contract"),
        state=CutoverState.FOUNDATION_ONLY,
        gate="worker-owned production plan, bounded retention and held-out event reports are required",
    ),
    _workload(
        "compute.geojson",
        entrypoints=("async_refresh_worker_geojson",),
        jobs=("geojson_operational",),
        state=CutoverState.FOUNDATION_ONLY,
        gate="GeoJSON must be derived once in the worker and projected from one bounded snapshot, never from an HA sensor read",
    ),
    _workload(
        "storage.legacy_region_and_observation_state",
        entrypoints=("_flush_region_observation_batches", "mcs_store.async_save_engine"),
        components=("canonical_evidence_store", "durable_processing", "meteorological_evidence_store"),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="the new canonical/track/event commits need one crash-tested worker transaction and migration/rollback proof",
    ),
    _workload(
        "publication.operational_snapshot",
        jobs=("operational_snapshot_commit", "operational_snapshot_head"),
        components=(
            "operational_snapshot_builder", "operational_snapshot_publication",
            "operational_snapshot_runtime", "operational_snapshot_store",
        ),
        state=CutoverState.SHADOW_CANDIDATE,
        gate="unknown snapshots are authority-bound and service-published; rich targets remain blocked until built atomically from admitted committed worker facts",
    ),
    _workload(
        "ha.desired_state_and_snapshot_projection",
        components=(
            "desired_state_builder", "desired_state_controller",
            "operational_snapshot", "worker_only_entry", "worker_only_runtime",
        ),
        state=CutoverState.HA_CONTROL_PLANE,
        gate="the only permitted HA runtime: target/config CAS plus O(1) immutable snapshot projection and availability",
        ha_allowed=True,
    ),
)


WORKLOADS = MappingProxyType({item.workload_id: item for item in _WORKLOADS})
if len(WORKLOADS) != len(_WORKLOADS):
    raise RuntimeError("duplicate workload identity")

_ACQUISITION_OWNER_PAIRS = tuple(
    (acquisition_workload_id, contract.workload_id)
    for contract in _WORKLOADS
    for acquisition_workload_id in contract.acquisition_workload_ids
)
ACQUISITION_WORKLOAD_OWNERS = MappingProxyType(dict(_ACQUISITION_OWNER_PAIRS))
if len(ACQUISITION_WORKLOAD_OWNERS) != len(_ACQUISITION_OWNER_PAIRS):
    raise RuntimeError("duplicate acquisition workload ownership")

_EVENT_INGRESS_OWNER_PAIRS = tuple(
    (event_ingress_id, contract.workload_id)
    for contract in _WORKLOADS
    for event_ingress_id in contract.event_ingress_ids
)
EVENT_INGRESS_OWNERS = MappingProxyType(dict(_EVENT_INGRESS_OWNER_PAIRS))
if len(EVENT_INGRESS_OWNERS) != len(_EVENT_INGRESS_OWNER_PAIRS):
    raise RuntimeError("duplicate event-ingress ownership")


def manifest_mapping() -> dict[str, object]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "workloads": [WORKLOADS[key].to_mapping() for key in sorted(WORKLOADS)],
    }


def manifest_sha256() -> str:
    payload = json.dumps(
        manifest_mapping(), allow_nan=False, ensure_ascii=False,
        separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def unresolved_workloads() -> tuple[WorkloadContract, ...]:
    return tuple(
        item for item in WORKLOADS.values()
        if item.state is not CutoverState.HA_CONTROL_PLANE
    )


__all__ = [
    "ACQUISITION_WORKLOAD_OWNERS", "EVENT_INGRESS_OWNERS", "CutoverState", "MANIFEST_VERSION",
    "WORKLOADS", "WorkloadContract",
    "manifest_mapping", "manifest_sha256", "unresolved_workloads",
]
