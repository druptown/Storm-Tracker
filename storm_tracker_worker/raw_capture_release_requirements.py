"""Portable RAW requirements and fail-closed target diagnostic evidence.

The existing :mod:`storm_tracker_worker.release_artifacts` v1 evidence remains
the authority for the integration and worker archives.  This module adds one
separate, deterministic v2 commit marker for a RAW release and one later,
target-specific diagnostic record.  The target record is deliberately not a
trust anchor and can never establish deployment readiness.  Portable
requirements never contain a selected runtime path, host identity, clock
value, secret, database, lock or other mutable runtime bytes.
"""
from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import secrets
import shutil
import stat
from typing import Any, BinaryIO, Final, Iterable, Iterator, Mapping, Protocol

from .release_artifacts import (
    BuiltReleaseCandidate,
    INTEGRATION_ROOT_NAME,
    RELEASE_EVIDENCE_CONTRACT,
    WORKER_ROOT_NAME,
    build_release_candidate,
    verify_release_candidate_evidence_bound_inputs,
)


RAW_RELEASE_REQUIREMENTS_CONTRACT: Final = (
    "stv3-raw-capture-release-requirements-v2"
)
TARGET_ACTIVATION_ATTESTATION_CONTRACT: Final = (
    "stv3-raw-capture-target-activation-attestation-v1"
)
TARGET_PROFILE_CONTRACT: Final = (
    "stv3-raw-capture-target-materialization-profile-v2"
)
SANITIZED_OPTIONS_CONTRACT: Final = "stv3-sanitized-supervisor-options-v1"
SANITIZED_OPTIONS_FILENAME: Final = (
    "storm_tracker_worker_raw_capture_shadow_pre_target_v1.json"
)
SANITIZED_OPTIONS_BYTES: Final = 1265
SANITIZED_OPTIONS_SHA256: Final = (
    "6f750d7a69b46b5fc4c7e8bd3f786824e085adf3fff6a39006a1d6603d96462b"
)
FROZEN_MATERIALIZER_SHA256: Final = (
    "495468707e2faee9185779b2308d40f893cc2fbe2bbe6db18f197e8d8a10d7df"
)
FROZEN_CATALOG_SHA256: Final = (
    "5f224f9aea2d5d77446a8df5b08dcaa44478611184fb312a7c5849399b9c4e6b"
)
FROZEN_ACTIVATION_MATRIX_SHA256: Final = (
    "21c5dafae8936a4fd4a00141156124959382b0442bb7bdfa77ac098061db0ab7"
)
FROZEN_RECOVERY_BINDING_COUNT: Final = 27
FROZEN_RECOVERY_INVENTORY_SHA256: Final = (
    "5b38cbc6d9ee66b6528987ac633b6ed15e48cad66deaf214a962ba278bfe2b1e"
)
FROZEN_RAW_CAPTURE_COMPOSITION_SHA256: Final = (
    "df65a977ddf45b5975b5c04815eea8d586264362c9604d66735adb275a1d5b2f"
)
FROZEN_WORKER_SERVICE_SHA256: Final = (
    "b7ea86cd0b6ead607181982d8cfce172548224f7d7b1117e619c5407575cc246"
)
FROZEN_RETENTION_GUARD_SHA256: Final = (
    "a1c102b8846097f7ddd6511a9acd76bf3568900ef7141b7675fa4bba866db174"
)
FROZEN_BLITZORTUNG_INGRESS_METADATA_SHA256: Final = (
    "d69bf757c4783c91d94fb6c91348e9c7081dc4a4c1ae28e5fcd3f52e0baa2481"
)
FROZEN_BLITZORTUNG_EVENT_RAW_LIMITS_SHA256: Final = (
    "a032d8ff63e8009a87013039dfb5e9485e0dc2cd62837b9af05dddaec8427d9c"
)
FROZEN_DERIVATION_FIXTURE_SHA256: Final = (
    "753bbe5629e08b9ccab4e86259870dc4ecec72c741f71c72f8d0ec6d71393dc5"
)
FROZEN_REQUIRED_SAME_FILESYSTEM_BYTES: Final = 20_694_695_936
FROZEN_CAPTURE_WINDOW_HOURS: Final = 1
FROZEN_HOST_OPERATIONAL_FREE_RESERVE_BYTES: Final = 2_147_483_648
FROZEN_SERVICE_CONFIGURED_PERSISTENT_MAX_BYTES: Final = 17_733_517_312
FROZEN_GUARD_CONFIGURED_PERSISTENT_MAX_BYTES: Final = 18_001_952_768
FROZEN_SELECTED_PROFILE: Final = "cadence_only"
FROZEN_BLITZORTUNG_ENABLED: Final = False
FROZEN_EVENT_CAPACITY_CLAIMED: Final = False
FROZEN_RETENTION_GUARD_CONTRACT: Final = (
    "stv3-worker-raw-capture-retention-guard-v2"
)
FROZEN_RETENTION_CAPACITY_POLICY: Final = (
    "config_bound_optional_event_envelope_reserved_fail_closed_v2"
)
MAX_SIGNED_INT64: Final = (1 << 63) - 1
FROZEN_DERIVATION_SENTINEL_TARGET_BYTES: Final = MAX_SIGNED_INT64
RAW_CAPTURE_RUNTIME_MODE: Final = "raw_capture_shadow"
MATERIALIZER_MEMBER_PATH: Final = (
    "storm_tracker_worker/raw_capture_activation_artifacts.py"
)
WORKER_PACKAGE_INVENTORY_CONTRACT: Final = (
    "stv3-raw-capture-worker-package-inventory-v1"
)
TARGET_FILESYSTEM_PROOF_CONTRACT: Final = (
    "stv3-raw-capture-target-filesystem-proof-v1"
)
RELEASE_AUTHORITY_CONTRACT: Final = "stv3-descriptor-relative-authority-v1"
RELEASE_AUTHORITY_MODULE_PATH: Final = "descriptor_relative_authority.py"
FROZEN_RELEASE_AUTHORITY_SHA256: Final = (
    "a621de8656355db573319c3105355996e462317e9a79a892d05704eeb3979858"
)

# Exact release-member membership for the complete worker package.  The v1
# evidence/archive verifier remains the byte authority; requirements bind every
# path, size and SHA-256 but deliberately make no dynamic-import/executability
# claim.  New or missing package members fail closed until this set is reviewed.
FROZEN_WORKER_PACKAGE_MEMBER_PATHS: Final = (
    ".dockerignore",
    ".env.example",
    "__init__.py",
    "acquisition_activation.py",
    "acquisition_capture_recovery.py",
    "acquisition_consumer_registry.py",
    "acquisition_control_reclamation.py",
    "acquisition_evidence_lifecycle.py",
    "acquisition_execution_store.py",
    "acquisition_handler_child.py",
    "acquisition_location_shadow.py",
    "acquisition_query_authority.py",
    "acquisition_runtime.py",
    "acquisition_scheduler_bridge.py",
    "acquisition_source_reclamation_apply_authority.py",
    "acquisition_source_reclamation_authority.py",
    "acquisition_source_reclamation_protocol.py",
    "acquisition_source_reclamation_views.py",
    "acquisition_source_retention.py",
    "aemet_native.py",
    "aemet_product_gate.py",
    "analysis_grid_transform.py",
    "archive_member_stream.py",
    "artifacts.py",
    "autonomous_scheduler.py",
    "awn.py",
    "awn_authorized_devices_evidence.py",
    "awn_native.py",
    "blitzortung_event_ingress.py",
    "blitzortung_mqtt_raw_sidecar.py",
    "blitzortung_raw_ingress.py",
    "bounded_archive_headers.py",
    "calibration.py",
    "calibration_corpus.py",
    "calibration_replay.py",
    "calibration_stage.py",
    "canonical_evidence_store.py",
    "canonical_ground_evidence_store.py",
    "canonical_radar_field_bridge.py",
    "certified_shadow_fact_pipeline.py",
    "city_survey_cli.py",
    "completion_output_archive.py",
    "completion_output_archive_adapters.py",
    "completion_route_adapters.py",
    "compose.yaml",
    "config.yaml",
    "cpu_limited_launcher.py",
    "cpu_topology.py",
    "desired_state.py",
    "Dockerfile",
    "DOCS.md",
    "dpc_native.py",
    "dwd_acquisition_completion_bridge.py",
    "dwd_rv_analysis_grid_certificate.py",
    "dwd_rv_capture_handoff.py",
    "dwd_rv_completion_consumer.py",
    "dwd_rv_completion_process_isolation.py",
    "dwd_rv_native.py",
    "dwd_rv_observation_replay.py",
    "dwd_rv_quantitative.py",
    "dwd_rv_science_sequence.py",
    "enterprise_rainrate.py",
    "evidence_to_cells_shadow.py",
    "external_annotation_pack.py",
    "geojson.py",
    "goes_rrqpe_native.py",
    "ground_coverage.py",
    "ground_fusion_shadow_process_isolation.py",
    "ground_fusion_shadow_runtime.py",
    "hard_decoder_isolation.py",
    "hsaf_h40b_native.py",
    "kmi_appv4_raw_capture.py",
    "kmi_opendata_raw.py",
    "knmi_native.py",
    "knmi_rtcor_analysis_grid_gate.py",
    "knmi_rtcor_completion_consumer.py",
    "knmi_rtcor_quantitative.py",
    "lightning.py",
    "lightning_exact_raw.py",
    "met_office_native.py",
    "met_office_rate_completion_consumer.py",
    "met_office_rate_quantitative.py",
    "metar.py",
    "metar_ground_completion_consumer.py",
    "metar_ground_evidence.py",
    "metar_ground_shadow_factory.py",
    "metar_native.py",
    "metar_survey_cli.py",
    "meteofrance_native.py",
    "model_guidance.py",
    "motion.py",
    "multi_provider_replay_matrix.py",
    "multi_source_cycle_assembler.py",
    "multi_source_ground_facts.py",
    "multi_source_nowcast_snapshot.py",
    "multi_source_operational_cycle.py",
    "multi_source_radar_facts.py",
    "multi_source_radar_snapshot.py",
    "multi_source_satellite_facts.py",
    "multi_source_snapshot.py",
    "native_capture.py",
    "netatmo.py",
    "netatmo_ground_decoder.py",
    "netatmo_native.py",
    "noaa_err_completion_consumer.py",
    "noaa_err_native.py",
    "noaa_err_quantitative.py",
    "offline_replay_acceptance.py",
    "offline_replay_nowcast_runner.py",
    "offline_replay_remaining_baseline_audit.py",
    "openmeteo_native.py",
    "openmeteo_usage_authority.py",
    "opera.py",
    "opera_dbzh_quantitative.py",
    "opera_native.py",
    "operational_fact_revision.py",
    "operational_fact_store.py",
    "operational_slo_evidence.py",
    "operational_snapshot_builder.py",
    "operational_snapshot_publication.py",
    "operational_snapshot_runtime.py",
    "operational_snapshot_store.py",
    "probe_core.py",
    "protocol.py",
    "provider_capture_registry.py",
    "provider_radar.py",
    "public_source_raw_capture.py",
    "radar.py",
    "radar_cycle_contract.py",
    "radar_cycle_store.py",
    "radar_frame.py",
    "radflood21_official_manifest.json",
    "radflood21_validation_contract.py",
    "rainviewer_native.py",
    "raw_artifacts.py",
    "raw_calibration_index.py",
    "raw_capacity_ledger.py",
    "raw_capture_activation_artifacts.py",
    "raw_capture_reclamation_controller.py",
    "raw_capture_release_requirements.py",
    "raw_capture_retention_guard.py",
    "raw_capture_service_composition.py",
    "raw_manifest_audit_completion_consumer.py",
    "release_artifacts.py",
    "replay.py",
    "requirements.txt",
    "run.sh",
    "satellite_radar.py",
    "science_operational_fact_bridge.py",
    "science_shadow_calibration_replay.py",
    "science_shadow_dwd_replay.py",
    "science_shadow_dwd_roi_baselines.py",
    "science_shadow_dwd_roi_export.py",
    "science_shadow_dwd_roi_replay.py",
    "science_shadow_dwd_roi_shared_translation.py",
    "science_shadow_orchestrator.py",
    "science_shadow_process_isolation.py",
    "scientific_validation.py",
    "serial_completion_dispatcher.py",
    "service.py",
    "survey_cli.py",
    "synop_survey_cli.py",
    "synoptic.py",
    "synoptic_ground_decoder.py",
    "synoptic_native.py",
    "terminal_raw_completion_router.py",
    "tracking.py",
    "tracking_core/__init__.py",
    "tracking_core/engine/__init__.py",
    "tracking_core/engine/advected_footprint_evidence.py",
    "tracking_core/engine/analysis_grid.py",
    "tracking_core/engine/artifact_capture_admission.py",
    "tracking_core/engine/artifact_retention_planner.py",
    "tracking_core/engine/association_features.py",
    "tracking_core/engine/association_graph.py",
    "tracking_core/engine/bounded_payload.py",
    "tracking_core/engine/canonical_ground_fusion_bridge.py",
    "tracking_core/engine/cell_track.py",
    "tracking_core/engine/cell_track_engine.py",
    "tracking_core/engine/cell_track_lifecycle.py",
    "tracking_core/engine/cell_track_state_codec.py",
    "tracking_core/engine/event_probability_calibration.py",
    "tracking_core/engine/forecast_verification.py",
    "tracking_core/engine/global_assignment.py",
    "tracking_core/engine/ground_fusion.py",
    "tracking_core/engine/ground_observation_adapters.py",
    "tracking_core/engine/ground_verification.py",
    "tracking_core/engine/hierarchical_detection.py",
    "tracking_core/engine/kmi_radqpe_contract.py",
    "tracking_core/engine/lucas_kanade_flow.py",
    "tracking_core/engine/meteorological_evidence_store.py",
    "tracking_core/engine/motion_identity_evidence.py",
    "tracking_core/engine/motion_prediction.py",
    "tracking_core/engine/multi_hypothesis_identity.py",
    "tracking_core/engine/nowcast_baselines.py",
    "tracking_core/engine/observation.py",
    "tracking_core/engine/observation_time_quality.py",
    "tracking_core/engine/physical_site_catalog.py",
    "tracking_core/engine/provider_artifact_contracts.py",
    "tracking_core/engine/provider_handoff.py",
    "tracking_core/engine/provider_quality_runtime.py",
    "tracking_core/engine/pyramidal_sparse_flow.py",
    "tracking_core/engine/pysteps_challenger.py",
    "tracking_core/engine/raw_artifact_store.py",
    "tracking_core/engine/recursive_track_estimator.py",
    "tracking_core/engine/shared_system_translation.py",
    "tracking_core/engine/storm.py",
    "tracking_core/engine/storm_engine.py",
    "tracking_core/engine/track_motion_prior.py",
    "tracking_core/engine/tracker_forecast_contract.py",
    "tracking_core/engine/trajectory.py",
    "tracking_core/engine/weather_system_grouping.py",
    "tracking_core/geometry/__init__.py",
    "tracking_core/geometry/bounding_box.py",
    "tracking_core/geometry/distance.py",
    "tracking_core/geometry/geocode.py",
    "tracking_core/geometry/hull.py",
    "tracking_core/geometry/polygon.py",
    "tracking_core/operational_snapshot.py",
    "validation_corpus.py",
    "virtual_probe.py",
    "worker_acquisition_plan.py",
    "worker_cutover_manifest.py",
    "worker_first_composition.py",
    "worker_first_ownership.py",
    "worker_first_runtime.py",
    "worker_first_service_activation.py",
    "worker_first_service_composition.py",
    "worker_only_bootstrap.py",
    "worker_only_cell_tracking.py",
    "worker_only_ground_processing.py",
    "worker_only_radar_processing.py",
    "worker_only_service_runtime.py",
)

MAX_DOCUMENT_BYTES: Final = 4 * 1024 * 1024
MAX_TARGET_ARTIFACT_BYTES: Final = 512 * 1024 * 1024
MAX_JSON_DEPTH: Final = 64
MAX_JSON_NODES: Final = 100_000
MAX_JSON_KEY_BYTES: Final = 256
MAX_JSON_STRING_BYTES: Final = 1024 * 1024
# The release pin above deliberately uses INT64_MAX only as an inert derivation
# sentinel.  A target attestation must bind a real target value and prove that
# live free space covers it; it may never inherit this sentinel as capacity.

REQUIREMENTS_BOUND_INPUT_KEYS: Final = (
    "evidence",
    "integration",
    "options",
    "requirements",
    "worker",
)
TARGET_ATTESTATION_BOUND_INPUT_KEYS: Final = (
    "attestation",
    "evidence",
    "integration",
    "options",
    "requirements",
    "target_authority",
    "target_capacity",
    "target_config",
    "target_lock",
    "target_manifest",
    "worker",
)
TARGET_ARTIFACT_INPUT_KEYS: Final = {
    "capacity_profile": "target_capacity",
    "ownership_lock_anchor": "target_lock",
    "raw_capture_config": "target_config",
    "worker_first_authority": "target_authority",
}
TARGET_ARTIFACT_ROLES: Final = tuple(sorted(TARGET_ARTIFACT_INPUT_KEYS))
TARGET_ARTIFACT_RELATIVE_PATHS: Final = {
    "capacity_profile": "artifacts/raw-capture-capacity-profile.json",
    "ownership_lock_anchor": "state/control/ownership.lock",
    "raw_capture_config": "artifacts/raw-capture-shadow-config.json",
    "worker_first_authority": "state/control/authority.json",
}
TARGET_MANIFEST_FIELDS: Final = frozenset({
    "activation_environment",
    "artifact_role",
    "artifacts",
    "bootstrap_receipt",
    "catalog_pins",
    "capacity_parameters",
    "contract",
    "deployment_performed",
    "host_path_pinned",
    "immutable_artifact_count",
    "materializer",
    "mutable_runtime_paths",
    "output_directory",
    "persistent_base_root",
    "portable_release_artifact",
    "raw_capture_config_contract",
    "raw_capture_runtime_contract",
    "reproducibility_scope",
    "runtime_mode",
    "target_materialization_required",
    "target_path_flavour",
})

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_SECRET_FIELD = re.compile(
    r"(?:^|_)(?:api_?key|access_?token|refresh_?token|password|passwd|"
    r"credential|private_?key|secret)(?:$|_)",
    re.IGNORECASE,
)
_FORBIDDEN_PORTABLE_KEYS: Final = frozenset({
    "authority_path",
    "config_path",
    "created_at",
    "device",
    "host",
    "hostname",
    "inode",
    "mtime",
    "output_directory",
    "persistent_base_root",
    "target_path",
    "timestamp",
})


class RawCaptureReleaseRequirementsError(RuntimeError):
    """Portable RAW evidence cannot be created or verified fail-closed."""


class _BoundMetadata(Protocol):
    size: int
    sha256: str


class BoundInputSource(Protocol):
    @property
    def keys(self) -> tuple[str, ...]: ...

    def require_exact_keys(self, expected_keys: Iterable[str]) -> None: ...

    def metadata(self, key: str) -> _BoundMetadata: ...

    def open_stream(self, key: str) -> AbstractContextManager[BinaryIO]: ...


@dataclass(frozen=True, slots=True)
class RawCapturePortablePinset:
    """Host-independent output of the closed capacity derivation."""

    activation_matrix_sha256: str
    blitzortung_ingress_limits_sha256: str
    blitzortung_raw_store_limits_sha256: str
    derivation_fixture_sha256: str
    capacity_contract: str
    capture_window_hours: int
    catalog_sha256: str
    materializer_contract: str
    materializer_sha256: str
    derivation_sentinel_target_bytes: int
    raw_capture_composition_sha256: str
    raw_capture_config_contract: str
    raw_capture_runtime_contract: str
    recovery_binding_count: int
    recovery_inventory_sha256: str
    required_same_filesystem_bytes: int
    retention_capacity_policy: str
    retention_guard_contract: str
    retention_guard_sha256: str
    selected_profile: str
    blitzortung_enabled: bool
    event_capacity_claimed: bool
    host_operational_free_reserve_bytes: int
    service_configured_persistent_max_bytes: int
    guard_configured_persistent_max_bytes: int
    target_manifest_contract: str
    worker_service_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "activation_matrix_sha256",
            "blitzortung_ingress_limits_sha256",
            "blitzortung_raw_store_limits_sha256",
            "derivation_fixture_sha256",
            "catalog_sha256",
            "materializer_sha256",
            "raw_capture_composition_sha256",
            "recovery_inventory_sha256",
            "retention_guard_sha256",
            "worker_service_sha256",
        ):
            _checked_sha256(getattr(self, name), name)
        for name in (
            "capacity_contract",
            "materializer_contract",
            "raw_capture_config_contract",
            "raw_capture_runtime_contract",
            "target_manifest_contract",
            "retention_guard_contract",
        ):
            _checked_contract(getattr(self, name), name)
        _positive_integer(
            self.capture_window_hours,
            "capture_window_hours",
            maximum=168,
        )
        _positive_integer(
            self.derivation_sentinel_target_bytes,
            "derivation_sentinel_target_bytes",
        )
        _positive_integer(
            self.required_same_filesystem_bytes,
            "required_same_filesystem_bytes",
        )
        _positive_integer(
            self.host_operational_free_reserve_bytes,
            "host_operational_free_reserve_bytes",
        )
        _positive_integer(
            self.service_configured_persistent_max_bytes,
            "service_configured_persistent_max_bytes",
        )
        _positive_integer(
            self.guard_configured_persistent_max_bytes,
            "guard_configured_persistent_max_bytes",
        )
        _positive_integer(
            self.recovery_binding_count,
            "recovery_binding_count",
            maximum=10_000,
        )
        if self.derivation_sentinel_target_bytes < self.required_same_filesystem_bytes:
            raise RawCaptureReleaseRequirementsError(
                "operator target is smaller than the derived filesystem requirement"
            )
        if self.selected_profile != FROZEN_SELECTED_PROFILE:
            raise RawCaptureReleaseRequirementsError(
                "selected target materialization profile is invalid"
            )
        if self.retention_capacity_policy != FROZEN_RETENTION_CAPACITY_POLICY:
            raise RawCaptureReleaseRequirementsError(
                "retention capacity policy is invalid"
            )
        if type(self.blitzortung_enabled) is not bool:
            raise RawCaptureReleaseRequirementsError(
                "Blitzortung profile selection is invalid"
            )
        if type(self.event_capacity_claimed) is not bool:
            raise RawCaptureReleaseRequirementsError(
                "event capacity claim is invalid"
            )

    def to_mapping(self) -> dict[str, object]:
        return {
            "activation": {
                "activation_matrix_sha256": self.activation_matrix_sha256,
                "catalog_sha256": self.catalog_sha256,
                "raw_capture_composition_sha256": (
                    self.raw_capture_composition_sha256
                ),
                "recovery_binding_count": self.recovery_binding_count,
                "recovery_inventory_sha256": self.recovery_inventory_sha256,
                "worker_service_sha256": self.worker_service_sha256,
            },
            "blitzortung": {
                "ingress_limits_sha256": (
                    self.blitzortung_ingress_limits_sha256
                ),
                "operational_admission": False,
                "raw_store_limits_sha256": (
                    self.blitzortung_raw_store_limits_sha256
                ),
            },
            "capacity": {
                "capture_window_hours": self.capture_window_hours,
                "contract": self.capacity_contract,
                "derivation_fixture_sha256": self.derivation_fixture_sha256,
                "derivation_sentinel_target_bytes": (
                    self.derivation_sentinel_target_bytes
                ),
                "minimum_required_same_filesystem_bytes": (
                    self.required_same_filesystem_bytes
                ),
                "target_capacity_claimed": False,
            },
            "materializer": {
                "contract": self.materializer_contract,
                "sha256": self.materializer_sha256,
            },
            "retention": {
                "blitzortung_enabled": self.blitzortung_enabled,
                "capacity_policy": self.retention_capacity_policy,
                "contract": self.retention_guard_contract,
                "event_capacity_claimed": self.event_capacity_claimed,
                "guard_configured_persistent_max_bytes": (
                    self.guard_configured_persistent_max_bytes
                ),
                "host_operational_free_reserve_bytes": (
                    self.host_operational_free_reserve_bytes
                ),
                "selected_profile": self.selected_profile,
                "service_configured_persistent_max_bytes": (
                    self.service_configured_persistent_max_bytes
                ),
                "source_sha256": self.retention_guard_sha256,
            },
            "runtime_contracts": {
                "config": self.raw_capture_config_contract,
                "runtime": self.raw_capture_runtime_contract,
                "target_manifest": self.target_manifest_contract,
            },
        }

    @classmethod
    def from_capacity_document(
        cls,
        capacity_document: Mapping[str, object],
        *,
        materializer_contract: str,
        materializer_sha256: str,
        raw_capture_config_contract: str,
        raw_capture_runtime_contract: str,
        retention_capacity_policy: str,
        retention_guard_contract: str,
        retention_guard_sha256: str,
        target_manifest_contract: str,
    ) -> "RawCapturePortablePinset":
        document = _mapping(capacity_document, "capacity_document")
        activation = _mapping(document.get("activation"), "capacity activation")
        derivation = _mapping(document.get("derivation"), "capacity derivation")
        event = _mapping(document.get("event_ingress"), "event ingress")
        ingress_limits = _mapping(
            event.get("ingress_limits"),
            "Blitzortung ingress limits",
        )
        raw_store_limits = _mapping(
            event.get("raw_store_limits"),
            "Blitzortung raw store limits",
        )
        same_filesystem = _mapping(
            document.get("same_filesystem_capacity"),
            "same-filesystem capacity",
        )
        service_capacity = _mapping(
            document.get("service_storage_capacity_diagnostics"),
            "service storage capacity diagnostics",
        )
        combined_persistent = _mapping(
            service_capacity.get("combined_persistent_volume"),
            "service configured persistent volume",
        )
        raw_capacity_profile = _mapping(
            document.get("raw_capacity_profile"),
            "raw capacity profile",
        )
        retention = _mapping(
            document.get("retention_guard"),
            "retention guard capacity binding",
        )
        service_envelope = _positive_integer(
            combined_persistent.get("configured_max_bytes"),
            "service_configured_persistent_max_bytes",
        )
        ledger_catalog = _positive_integer(
            raw_capacity_profile.get("max_catalog_bytes"),
            "capacity_ledger_catalog_max_bytes",
        )
        guard_envelope = service_envelope + ledger_catalog
        if (
            document.get("automatic_reclaim") is not False
            or document.get("capacity_exhaustion") != "fail_closed"
            or document.get("fail_closed_on_full") is not True
            or event.get("operational_admission") is not False
            or event.get("capacity_claimed") is not False
            or event.get("enabled") is not False
            or event.get("included_in_same_filesystem_requirement") is not False
            or event.get("limits_status") != "configured_dormant_unclaimed"
            or service_capacity.get("accounting_contract")
            != "config_bound_optional_event_capacity_v2"
            or service_capacity.get("host_operational_free_reserve_bytes")
            != FROZEN_HOST_OPERATIONAL_FREE_RESERVE_BYTES
            or _mapping(
                service_capacity.get("event"),
                "service event capacity diagnostics",
            ).get("capacity_claimed") is not False
            or same_filesystem.get("live_disk_free_space_verified") is not False
            or same_filesystem.get("blitzortung_enabled") is not False
            or same_filesystem.get(
                "operator_host_operational_free_reserve_bytes"
            ) != FROZEN_HOST_OPERATIONAL_FREE_RESERVE_BYTES
            or set(retention) != {
                "blitzortung_enabled",
                "capacity_policy",
                "contract",
                "event_capacity_claimed",
                "guard_configured_persistent_max_bytes",
                "host_operational_free_reserve_bytes",
                "selected_profile",
                "service_configured_persistent_max_bytes",
                "source_sha256",
            }
            or retention.get("blitzortung_enabled") is not False
            or retention.get("event_capacity_claimed") is not False
            or retention.get("selected_profile") != FROZEN_SELECTED_PROFILE
            or retention.get("capacity_policy") != retention_capacity_policy
            or retention.get("contract") != retention_guard_contract
            or retention.get("source_sha256") != retention_guard_sha256
            or retention.get("host_operational_free_reserve_bytes")
            != FROZEN_HOST_OPERATIONAL_FREE_RESERVE_BYTES
            or retention.get("service_configured_persistent_max_bytes")
            != service_envelope
            or retention.get("guard_configured_persistent_max_bytes")
            != guard_envelope
        ):
            raise RawCaptureReleaseRequirementsError(
                "capacity document does not retain the closed RAW safety posture"
            )
        capacity_body = _canonical(document)
        return cls(
            activation_matrix_sha256=_checked_sha256(
                activation.get("activation_matrix_sha256"),
                "activation_matrix_sha256",
            ),
            blitzortung_ingress_limits_sha256=_sha_bytes(
                _canonical(ingress_limits)
            ),
            blitzortung_raw_store_limits_sha256=_sha_bytes(
                _canonical(raw_store_limits)
            ),
            derivation_fixture_sha256=_sha_bytes(capacity_body),
            capacity_contract=_checked_contract(
                document.get("contract"),
                "capacity_contract",
            ),
            capture_window_hours=_positive_integer(
                derivation.get("capture_window_hours"),
                "capture_window_hours",
                maximum=168,
            ),
            catalog_sha256=_checked_sha256(
                activation.get("catalog_sha256"),
                "catalog_sha256",
            ),
            materializer_contract=materializer_contract,
            materializer_sha256=materializer_sha256,
            derivation_sentinel_target_bytes=_positive_integer(
                same_filesystem.get("operator_declared_target_bytes"),
                "derivation_sentinel_target_bytes",
            ),
            raw_capture_composition_sha256=_checked_sha256(
                activation.get("raw_capture_composition_sha256"),
                "raw_capture_composition_sha256",
            ),
            raw_capture_config_contract=raw_capture_config_contract,
            raw_capture_runtime_contract=raw_capture_runtime_contract,
            recovery_binding_count=_positive_integer(
                activation.get("recovery_binding_count"),
                "recovery_binding_count",
                maximum=10_000,
            ),
            recovery_inventory_sha256=_checked_sha256(
                activation.get("recovery_inventory_sha256"),
                "recovery_inventory_sha256",
            ),
            required_same_filesystem_bytes=_positive_integer(
                same_filesystem.get("required_same_filesystem_bytes"),
                "required_same_filesystem_bytes",
            ),
            retention_capacity_policy=str(retention.get("capacity_policy")),
            retention_guard_contract=str(retention.get("contract")),
            retention_guard_sha256=_checked_sha256(
                retention.get("source_sha256"),
                "retention_guard_sha256",
            ),
            selected_profile=str(retention.get("selected_profile")),
            blitzortung_enabled=retention.get("blitzortung_enabled"),
            event_capacity_claimed=retention.get("event_capacity_claimed"),
            host_operational_free_reserve_bytes=(
                _positive_integer(
                    retention.get("host_operational_free_reserve_bytes"),
                    "host_operational_free_reserve_bytes",
                )
            ),
            service_configured_persistent_max_bytes=_positive_integer(
                retention.get("service_configured_persistent_max_bytes"),
                "service_configured_persistent_max_bytes",
            ),
            guard_configured_persistent_max_bytes=_positive_integer(
                retention.get("guard_configured_persistent_max_bytes"),
                "guard_configured_persistent_max_bytes",
            ),
            target_manifest_contract=target_manifest_contract,
            worker_service_sha256=_checked_sha256(
                activation.get("worker_service_sha256"),
                "worker_service_sha256",
            ),
        )


def _require_frozen_pinset(pinset: RawCapturePortablePinset) -> None:
    expected = {
        "activation_matrix_sha256": FROZEN_ACTIVATION_MATRIX_SHA256,
        "blitzortung_ingress_limits_sha256": (
            FROZEN_BLITZORTUNG_INGRESS_METADATA_SHA256
        ),
        "blitzortung_raw_store_limits_sha256": (
            FROZEN_BLITZORTUNG_EVENT_RAW_LIMITS_SHA256
        ),
        "capacity_contract": "stv3-worker-raw-capture-capacity-artifact-v2",
        "capture_window_hours": FROZEN_CAPTURE_WINDOW_HOURS,
        "catalog_sha256": FROZEN_CATALOG_SHA256,
        "derivation_fixture_sha256": FROZEN_DERIVATION_FIXTURE_SHA256,
        "derivation_sentinel_target_bytes": (
            FROZEN_DERIVATION_SENTINEL_TARGET_BYTES
        ),
        "materializer_contract": (
            "stv3-worker-raw-capture-activation-materializer-v2"
        ),
        "materializer_sha256": FROZEN_MATERIALIZER_SHA256,
        "raw_capture_composition_sha256": (
            FROZEN_RAW_CAPTURE_COMPOSITION_SHA256
        ),
        "raw_capture_config_contract": (
            "stv3-worker-raw-capture-service-config-v5"
        ),
        "raw_capture_runtime_contract": (
            "stv3-worker-raw-capture-runtime-v5"
        ),
        "recovery_binding_count": FROZEN_RECOVERY_BINDING_COUNT,
        "recovery_inventory_sha256": FROZEN_RECOVERY_INVENTORY_SHA256,
        "required_same_filesystem_bytes": (
            FROZEN_REQUIRED_SAME_FILESYSTEM_BYTES
        ),
        "retention_capacity_policy": FROZEN_RETENTION_CAPACITY_POLICY,
        "retention_guard_contract": FROZEN_RETENTION_GUARD_CONTRACT,
        "retention_guard_sha256": FROZEN_RETENTION_GUARD_SHA256,
        "selected_profile": FROZEN_SELECTED_PROFILE,
        "blitzortung_enabled": FROZEN_BLITZORTUNG_ENABLED,
        "event_capacity_claimed": FROZEN_EVENT_CAPACITY_CLAIMED,
        "host_operational_free_reserve_bytes": (
            FROZEN_HOST_OPERATIONAL_FREE_RESERVE_BYTES
        ),
        "service_configured_persistent_max_bytes": (
            FROZEN_SERVICE_CONFIGURED_PERSISTENT_MAX_BYTES
        ),
        "guard_configured_persistent_max_bytes": (
            FROZEN_GUARD_CONFIGURED_PERSISTENT_MAX_BYTES
        ),
        "target_manifest_contract": (
            "stv3-worker-raw-capture-shadow-artifact-manifest-v2"
        ),
        "worker_service_sha256": FROZEN_WORKER_SERVICE_SHA256,
    }
    for name, value in expected.items():
        if getattr(pinset, name) != value:
            raise RawCaptureReleaseRequirementsError(
                f"frozen RAW release pin differs: {name}"
            )


def derive_frozen_raw_capture_release_pinset(
    *,
    materializer_path: Path,
) -> RawCapturePortablePinset:
    """Derive and reprove the frozen host-independent release fixture."""

    from . import raw_capture_activation_artifacts as materializer
    from . import raw_capture_retention_guard as retention_guard

    supplied_body = _read_file(
        Path(materializer_path),
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="RAW activation materializer",
    )
    imported_body = _read_file(
        Path(materializer.__file__).resolve(strict=True),
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="imported RAW activation materializer",
    )
    if (
        _sha_bytes(supplied_body) != FROZEN_MATERIALIZER_SHA256
        or _sha_bytes(imported_body) != FROZEN_MATERIALIZER_SHA256
    ):
        raise RawCaptureReleaseRequirementsError(
            "RAW activation materializer differs from the frozen source"
        )
    retention_guard_body = _read_file(
        Path(retention_guard.__file__).resolve(strict=True),
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="RAW retention guard",
    )
    if _sha_bytes(retention_guard_body) != FROZEN_RETENTION_GUARD_SHA256:
        raise RawCaptureReleaseRequirementsError(
            "RAW retention guard differs from the frozen source"
        )
    plan = materializer.derive_raw_capture_shadow_capacity(
        capture_window_hours=FROZEN_CAPTURE_WINDOW_HOURS,
        target_raw_capacity_bytes=FROZEN_DERIVATION_SENTINEL_TARGET_BYTES,
        blitzortung_enabled=FROZEN_BLITZORTUNG_ENABLED,
        operator_host_operational_free_reserve_bytes=(
            FROZEN_HOST_OPERATIONAL_FREE_RESERVE_BYTES
        ),
        expected_catalog_sha256=FROZEN_CATALOG_SHA256,
        expected_activation_matrix_sha256=(
            FROZEN_ACTIVATION_MATRIX_SHA256
        ),
    )
    pinset = RawCapturePortablePinset.from_capacity_document(
        plan.to_mapping(),
        materializer_contract=materializer.MATERIALIZER_CONTRACT,
        materializer_sha256=FROZEN_MATERIALIZER_SHA256,
        raw_capture_config_contract=(
            materializer.EXPECTED_RAW_CAPTURE_CONFIG_CONTRACT
        ),
        raw_capture_runtime_contract=(
            materializer.EXPECTED_RAW_CAPTURE_RUNTIME_CONTRACT
        ),
        retention_capacity_policy=retention_guard.CAPACITY_POLICY,
        retention_guard_contract=retention_guard.CONTRACT,
        retention_guard_sha256=FROZEN_RETENTION_GUARD_SHA256,
        target_manifest_contract=materializer.MANIFEST_CONTRACT,
    )
    _require_frozen_pinset(pinset)
    return pinset


@dataclass(frozen=True, slots=True)
class BuiltRawCaptureReleaseRequirements:
    core_release: BuiltReleaseCandidate
    requirements_path: Path
    requirements_sha256: str
    requirements_file_sha256: str


@dataclass(frozen=True, slots=True)
class BuiltTargetActivationAttestation:
    attestation_path: Path
    attestation_sha256: str
    attestation_file_sha256: str
    target_activation_manifest_sha256: str


@dataclass(frozen=True, slots=True)
class BuiltRawCaptureReleaseQuartet:
    """One fully staged RAW quartet published with requirements last."""

    core_release: BuiltReleaseCandidate
    requirements: BuiltRawCaptureReleaseRequirements


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (ArithmeticError, RecursionError, TypeError, ValueError) as error:
        raise RawCaptureReleaseRequirementsError(
            "release evidence is not canonical JSON"
        ) from error


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_reparse(metadata: os.stat_result) -> bool:
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(
        flag
        and int(getattr(metadata, "st_file_attributes", 0)) & int(flag)
    )


def _stat_witness(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_mode),
        int(metadata.st_nlink),
        int(metadata.st_size),
        int(metadata.st_mtime_ns),
    )


def _descriptor_change_witness(metadata: os.stat_result) -> tuple[int, ...]:
    """Add handle-local change time without comparing incompatible path APIs."""

    return (*_stat_witness(metadata), int(metadata.st_ctime_ns))


def _load_release_authority() -> Any:
    """Load the separately pinned release-tooling filesystem authority."""

    try:
        import descriptor_relative_authority as authority
    except (ImportError, OSError) as error:
        raise RawCaptureReleaseRequirementsError(
            "pinned descriptor-relative release authority is unavailable"
        ) from error
    source = Path(getattr(authority, "__file__", ""))
    try:
        metadata = source.lstat()
        if (
            source.name != RELEASE_AUTHORITY_MODULE_PATH
            or stat.S_ISLNK(metadata.st_mode)
            or _is_reparse(metadata)
            or not stat.S_ISREG(metadata.st_mode)
            or int(metadata.st_nlink) != 1
            or int(metadata.st_size) > 1024 * 1024
        ):
            raise OSError("release authority source is not a plain file")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(source, flags)
        try:
            opened = os.fstat(descriptor)
            body = bytearray()
            while True:
                chunk = os.read(descriptor, 1024 * 1024 - len(body) + 1)
                if not chunk:
                    break
                body.extend(chunk)
                if len(body) > 1024 * 1024:
                    raise OSError("release authority source exceeds bound")
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        lexical_after = source.lstat()
    except OSError as error:
        raise RawCaptureReleaseRequirementsError(
            "pinned release authority cannot be measured"
        ) from error
    if (
        _stat_witness(metadata) != _stat_witness(opened)
        or _stat_witness(opened) != _stat_witness(after)
        or _stat_witness(after) != _stat_witness(lexical_after)
        or _sha_bytes(bytes(body)) != FROZEN_RELEASE_AUTHORITY_SHA256
        or getattr(authority, "DESCRIPTOR_RELATIVE_AUTHORITY_CONTRACT", None)
        != RELEASE_AUTHORITY_CONTRACT
    ):
        raise RawCaptureReleaseRequirementsError(
            "descriptor-relative release authority differs from its freeze"
        )
    return authority


def _read_file(path: Path, *, maximum_bytes: int, label: str) -> bytes:
    """Read one regular file through one identity-pinned no-follow handle."""

    supplied = Path(path)
    if type(maximum_bytes) is not int or maximum_bytes < 0:
        raise RawCaptureReleaseRequirementsError(
            f"{label} has an invalid byte bound"
        )
    try:
        absolute = Path(os.path.abspath(supplied))
        authority_module = _load_release_authority()
        with authority_module.DirectoryAuthority.open_absolute(
            absolute.parent
        ) as authority:
            authority.reanchor(absolute.parent)
            with authority.open_file(absolute.name) as bound:
                before = bound.identity()
                if before.size < 0 or before.size > maximum_bytes:
                    raise RawCaptureReleaseRequirementsError(
                        f"{label} is not a bounded file"
                    )
                with bound.duplicate_binary("rb") as stream:
                    descriptor_before = os.fstat(stream.fileno())
                    chunks: list[bytes] = []
                    total = 0
                    while True:
                        request = min(1024 * 1024, before.size - total + 1)
                        if request <= 0:
                            raise RawCaptureReleaseRequirementsError(
                                f"{label} exceeds its pinned byte count"
                            )
                        chunk = stream.read(request)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > before.size:
                            raise RawCaptureReleaseRequirementsError(
                                f"{label} changed while read"
                            )
                        chunks.append(chunk)
                    descriptor_after = os.fstat(stream.fileno())
                body = b"".join(chunks)
                after = bound.identity()
                if (
                    not before.same_object(after)
                    or before != after
                    or before.size != descriptor_before.st_size
                    or _descriptor_change_witness(descriptor_before)
                    != _descriptor_change_witness(descriptor_after)
                ):
                    raise RawCaptureReleaseRequirementsError(
                        f"{label} changed while read"
                    )
            authority.reanchor(absolute.parent)
            lexical = authority.entry_identity(absolute.name)
            if not lexical.same_object(before) or lexical != before:
                raise RawCaptureReleaseRequirementsError(
                    f"{label} path identity changed while read"
                )
        if len(body) != before.size:
            raise RawCaptureReleaseRequirementsError(
                f"{label} changed while read"
            )
    except RawCaptureReleaseRequirementsError:
        raise
    except Exception as error:
        raise RawCaptureReleaseRequirementsError(
            f"{label} cannot be read safely"
        ) from error
    return bytes(body)


def _checked_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RawCaptureReleaseRequirementsError(f"{label} is not a SHA-256")
    return value


def _checked_contract(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("stv3-")
        or len(value) > 128
        or value != value.strip()
    ):
        raise RawCaptureReleaseRequirementsError(f"{label} is invalid")
    return value


def _positive_integer(
    value: object,
    label: str,
    *,
    maximum: int = MAX_SIGNED_INT64,
) -> int:
    if type(value) is not int or value <= 0 or value > maximum:
        raise RawCaptureReleaseRequirementsError(f"{label} is invalid")
    return value


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise RawCaptureReleaseRequirementsError(f"{label} is not a mapping")
    return value


def _strict_json(
    body: bytes,
    *,
    label: str,
    allow_floats: bool = False,
) -> dict[str, object]:
    duplicate = False

    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        nonlocal duplicate
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                duplicate = True
            result[key] = value
        return result

    def reject_float(token: str) -> object:
        raise ValueError(f"float forbidden: {token}")

    def finite_float(token: str) -> float:
        value = float(token)
        if not math.isfinite(value):
            raise ValueError("non-finite float")
        return value

    def reject_constant(token: str) -> object:
        raise ValueError(f"constant forbidden: {token}")

    def bounded_integer(token: str) -> int:
        value = int(token)
        if abs(value) > MAX_SIGNED_INT64:
            raise ValueError("integer out of range")
        return value

    try:
        document = json.loads(
            body.decode("ascii"),
            object_pairs_hook=unique,
            parse_constant=reject_constant,
            parse_float=finite_float if allow_floats else reject_float,
            parse_int=bounded_integer,
        )
    except (UnicodeError, ValueError, RecursionError) as error:
        raise RawCaptureReleaseRequirementsError(
            f"{label} is not strict bounded JSON"
        ) from error
    if duplicate or not isinstance(document, dict):
        raise RawCaptureReleaseRequirementsError(
            f"{label} has duplicate keys or is not an object"
        )
    pending: list[tuple[object, int]] = [(document, 1)]
    nodes = 0
    while pending:
        value, depth = pending.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise RawCaptureReleaseRequirementsError(
                f"{label} exceeds its JSON structure bound"
            )
        if isinstance(value, dict):
            for key, nested in value.items():
                try:
                    key_bytes = key.encode("ascii")
                except (AttributeError, UnicodeError) as error:
                    raise RawCaptureReleaseRequirementsError(
                        f"{label} has an invalid JSON key"
                    ) from error
                if len(key_bytes) > MAX_JSON_KEY_BYTES:
                    raise RawCaptureReleaseRequirementsError(
                        f"{label} has an invalid JSON key"
                    )
                pending.append((nested, depth + 1))
        elif isinstance(value, list):
            pending.extend((nested, depth + 1) for nested in value)
        elif isinstance(value, str):
            try:
                string_bytes = value.encode("ascii")
            except UnicodeError as error:
                raise RawCaptureReleaseRequirementsError(
                    f"{label} contains non-ASCII text"
                ) from error
            if len(string_bytes) > MAX_JSON_STRING_BYTES:
                raise RawCaptureReleaseRequirementsError(
                    f"{label} exceeds its JSON string bound"
                )
        elif value is None or isinstance(value, (bool, int)):
            continue
        elif allow_floats and isinstance(value, float) and math.isfinite(value):
            continue
        else:
            raise RawCaptureReleaseRequirementsError(
                f"{label} contains a forbidden JSON value"
            )
    return document


def _strict_canonical_document(body: bytes, *, label: str) -> dict[str, object]:
    if not body.endswith(b"\n"):
        raise RawCaptureReleaseRequirementsError(f"{label} lacks its final newline")
    document = _strict_json(body, label=label)
    if _canonical(document) + b"\n" != body:
        raise RawCaptureReleaseRequirementsError(f"{label} is not canonical")
    return document


def _reject_portability_leaks(value: object) -> None:
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            for key, nested in item.items():
                if key in _FORBIDDEN_PORTABLE_KEYS or _SECRET_FIELD.search(key):
                    raise RawCaptureReleaseRequirementsError(
                        "portable requirements contain target or secret metadata"
                    )
                pending.append(nested)
        elif isinstance(item, list):
            pending.extend(item)


def _reject_secret_fields(value: object) -> None:
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            for key, nested in item.items():
                if _SECRET_FIELD.search(key):
                    raise RawCaptureReleaseRequirementsError(
                        "target attestation contains secret metadata"
                    )
                pending.append(nested)
        elif isinstance(item, list):
            pending.extend(item)


@dataclass(frozen=True, slots=True)
class _MemoryMetadata:
    size: int
    sha256: str


class _MemoryBoundInputSource:
    """Immutable path-free source used after one descriptor-pinned snapshot."""

    def __init__(self, bodies: Mapping[str, bytes]) -> None:
        self._bodies = {
            str(key): bytes(value)
            for key, value in bodies.items()
        }

    @property
    def keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._bodies))

    def require_exact_keys(self, expected_keys: Iterable[str]) -> None:
        if tuple(sorted(expected_keys)) != self.keys:
            raise RawCaptureReleaseRequirementsError(
                "memory-bound input key set differs"
            )

    def metadata(self, key: str) -> _MemoryMetadata:
        body = self._bodies[key]
        return _MemoryMetadata(size=len(body), sha256=_sha_bytes(body))

    @contextmanager
    def open_stream(self, key: str) -> Iterator[BinaryIO]:
        yield io.BytesIO(self._bodies[key])


def _body_descriptor(body: bytes, *, filename: str | None = None) -> dict[str, object]:
    descriptor: dict[str, object] = {
        "bytes": len(body),
        "sha256": _sha_bytes(body),
    }
    if filename is not None:
        descriptor["filename"] = filename
    return descriptor


def _validate_filename(value: object, expected: str | None = None) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != Path(value).name
        or "/" in value
        or "\\" in value
        or len(value) > 255
    ):
        raise RawCaptureReleaseRequirementsError("release filename is invalid")
    if expected is not None and value != expected:
        raise RawCaptureReleaseRequirementsError("release filename differs")
    return value


def _validate_sanitized_options(body: bytes) -> dict[str, object]:
    if len(body) != SANITIZED_OPTIONS_BYTES or _sha_bytes(body) != SANITIZED_OPTIONS_SHA256:
        raise RawCaptureReleaseRequirementsError(
            "sanitized Supervisor options differ from their release pin"
        )
    document = _strict_json(body, label="sanitized Supervisor options")
    if set(document) != {"contract", "options", "secret_presence"}:
        raise RawCaptureReleaseRequirementsError(
            "sanitized Supervisor options have an unknown schema"
        )
    options = _mapping(document.get("options"), "sanitized options")
    presence = _mapping(document.get("secret_presence"), "secret presence")
    if (
        document.get("contract") != SANITIZED_OPTIONS_CONTRACT
        or options.get("runtime_mode") != RAW_CAPTURE_RUNTIME_MODE
        or options.get("worker_first_config_path") != ""
        or options.get("worker_first_config_sha256") != ""
        or options.get("worker_first_authority_sha256") != ""
        or any(type(value) is not bool for value in presence.values())
    ):
        raise RawCaptureReleaseRequirementsError(
            "sanitized Supervisor options do not describe pre-target RAW mode"
        )
    return document


def validate_frozen_sanitized_options_snapshot(
    path: Path,
) -> dict[str, object]:
    """Validate the exact secret-free pre-target Supervisor snapshot."""

    supplied = Path(path)
    if supplied.name != SANITIZED_OPTIONS_FILENAME:
        raise RawCaptureReleaseRequirementsError(
            "sanitized Supervisor options filename differs from its release pin"
        )
    body = _read_file(
        supplied,
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="sanitized Supervisor options",
    )
    return _validate_sanitized_options(body)


def _tree_file(
    evidence: Mapping[str, object],
    *,
    tree_name: str,
    relative_path: str,
) -> Mapping[str, object]:
    trees = _mapping(evidence.get("source_trees"), "release source trees")
    tree = _mapping(trees.get(tree_name), f"{tree_name} source tree")
    files = tree.get("files")
    if not isinstance(files, list):
        raise RawCaptureReleaseRequirementsError("release source tree lacks files")
    matches = [
        item
        for item in files
        if isinstance(item, Mapping) and item.get("path") == relative_path
    ]
    if len(matches) != 1:
        raise RawCaptureReleaseRequirementsError(
            "release source tree does not uniquely bind the materializer"
        )
    return matches[0]


def _worker_package_inventory(
    evidence: Mapping[str, object],
) -> dict[str, object]:
    trees = _mapping(evidence.get("source_trees"), "release source trees")
    worker_tree = _mapping(trees.get("worker"), "worker source tree")
    rows = worker_tree.get("files")
    if not isinstance(rows, list):
        raise RawCaptureReleaseRequirementsError(
            "worker package inventory is unavailable"
        )
    by_relative: dict[str, Mapping[str, object]] = {}
    for row in rows:
        item = _mapping(row, "worker package member")
        relative = item.get("path")
        if not isinstance(relative, str) or relative in by_relative:
            raise RawCaptureReleaseRequirementsError(
                "worker package member path is invalid or duplicated"
            )
        by_relative[relative] = item
    expected_paths = tuple(sorted(FROZEN_WORKER_PACKAGE_MEMBER_PATHS))
    if tuple(sorted(by_relative)) != expected_paths:
        raise RawCaptureReleaseRequirementsError(
            "worker package membership differs from its frozen release set"
        )
    members: list[dict[str, object]] = []
    digest = hashlib.sha256()
    for relative_path in expected_paths:
        item = by_relative[relative_path]
        size = item.get("size")
        sha256 = item.get("sha256")
        if (
            type(size) is not int
            or size < 0
            or size > MAX_TARGET_ARTIFACT_BYTES
        ):
            raise RawCaptureReleaseRequirementsError(
                "worker package member has an invalid archived size"
            )
        checked_sha = _checked_sha256(
            sha256,
            f"worker package member {relative_path}",
        )
        digest.update(relative_path.encode("ascii"))
        digest.update(b"\x00")
        digest.update(str(size).encode("ascii"))
        digest.update(b"\x00")
        digest.update(checked_sha.encode("ascii"))
        digest.update(b"\n")
        members.append({
            "bytes": size,
            "member_path": f"{WORKER_ROOT_NAME}/{relative_path}",
            "sha256": checked_sha,
        })
    by_path = {
        str(item["member_path"]): item
        for item in members
    }
    anchors = {
        MATERIALIZER_MEMBER_PATH: FROZEN_MATERIALIZER_SHA256,
        f"{WORKER_ROOT_NAME}/raw_capture_service_composition.py": (
            FROZEN_RAW_CAPTURE_COMPOSITION_SHA256
        ),
        f"{WORKER_ROOT_NAME}/raw_capture_retention_guard.py": (
            FROZEN_RETENTION_GUARD_SHA256
        ),
        f"{WORKER_ROOT_NAME}/service.py": FROZEN_WORKER_SERVICE_SHA256,
    }
    for member_path, expected_sha in anchors.items():
        if by_path.get(member_path, {}).get("sha256") != expected_sha:
            raise RawCaptureReleaseRequirementsError(
                f"archived worker execution anchor differs: {member_path}"
            )
    return {
        "complete_frozen_worker_package_membership_content_bound": True,
        "contract": WORKER_PACKAGE_INVENTORY_CONTRACT,
        "dynamic_import_executability_proven": False,
        "inventory_sha256": digest.hexdigest(),
        "member_count": len(members),
        "members": members,
        "runtime_import_closure_verified": False,
    }


def _validated_pinset_mapping(
    value: object,
) -> tuple[dict[str, object], RawCapturePortablePinset]:
    pinset = _mapping(value, "portable pinset")
    if set(pinset) != {
        "activation", "blitzortung", "capacity", "materializer",
        "retention", "runtime_contracts",
    }:
        raise RawCaptureReleaseRequirementsError(
            "portable pinset has an unknown schema"
        )
    activation = _mapping(pinset.get("activation"), "activation pinset")
    blitzortung = _mapping(pinset.get("blitzortung"), "Blitzortung pinset")
    capacity = _mapping(pinset.get("capacity"), "capacity pinset")
    materializer = _mapping(pinset.get("materializer"), "materializer pinset")
    retention = _mapping(pinset.get("retention"), "retention pinset")
    runtime = _mapping(pinset.get("runtime_contracts"), "runtime contracts")
    if set(activation) != {
        "activation_matrix_sha256",
        "catalog_sha256",
        "raw_capture_composition_sha256",
        "recovery_binding_count",
        "recovery_inventory_sha256",
        "worker_service_sha256",
    }:
        raise RawCaptureReleaseRequirementsError(
            "activation pinset has an unknown schema"
        )
    if (
        set(blitzortung) != {
            "ingress_limits_sha256",
            "operational_admission",
            "raw_store_limits_sha256",
        }
        or blitzortung.get("operational_admission") is not False
    ):
        raise RawCaptureReleaseRequirementsError(
            "Blitzortung pinset has an unknown or admitted schema"
        )
    if set(capacity) != {
        "capture_window_hours",
        "contract",
        "derivation_fixture_sha256",
        "derivation_sentinel_target_bytes",
        "minimum_required_same_filesystem_bytes",
        "target_capacity_claimed",
    }:
        raise RawCaptureReleaseRequirementsError(
            "capacity pinset has an unknown schema"
        )
    if set(materializer) != {"contract", "sha256"}:
        raise RawCaptureReleaseRequirementsError(
            "materializer pinset has an unknown schema"
        )
    if set(retention) != {
        "blitzortung_enabled",
        "capacity_policy",
        "contract",
        "event_capacity_claimed",
        "guard_configured_persistent_max_bytes",
        "host_operational_free_reserve_bytes",
        "selected_profile",
        "service_configured_persistent_max_bytes",
        "source_sha256",
    }:
        raise RawCaptureReleaseRequirementsError(
            "retention pinset has an unknown schema"
        )
    if set(runtime) != {"config", "runtime", "target_manifest"}:
        raise RawCaptureReleaseRequirementsError(
            "runtime-contract pinset has an unknown schema"
        )
    typed = RawCapturePortablePinset(
        activation_matrix_sha256=activation.get("activation_matrix_sha256"),
        blitzortung_ingress_limits_sha256=blitzortung.get(
            "ingress_limits_sha256"
        ),
        blitzortung_raw_store_limits_sha256=blitzortung.get(
            "raw_store_limits_sha256"
        ),
        derivation_fixture_sha256=capacity.get("derivation_fixture_sha256"),
        capacity_contract=capacity.get("contract"),
        capture_window_hours=capacity.get("capture_window_hours"),
        catalog_sha256=activation.get("catalog_sha256"),
        materializer_contract=materializer.get("contract"),
        materializer_sha256=materializer.get("sha256"),
        derivation_sentinel_target_bytes=capacity.get(
            "derivation_sentinel_target_bytes"
        ),
        raw_capture_composition_sha256=activation.get(
            "raw_capture_composition_sha256"
        ),
        raw_capture_config_contract=runtime.get("config"),
        raw_capture_runtime_contract=runtime.get("runtime"),
        recovery_binding_count=activation.get("recovery_binding_count"),
        recovery_inventory_sha256=activation.get("recovery_inventory_sha256"),
        required_same_filesystem_bytes=capacity.get(
            "minimum_required_same_filesystem_bytes"
        ),
        retention_capacity_policy=retention.get("capacity_policy"),
        retention_guard_contract=retention.get("contract"),
        retention_guard_sha256=retention.get("source_sha256"),
        selected_profile=retention.get("selected_profile"),
        blitzortung_enabled=retention.get("blitzortung_enabled"),
        event_capacity_claimed=retention.get("event_capacity_claimed"),
        host_operational_free_reserve_bytes=retention.get(
            "host_operational_free_reserve_bytes"
        ),
        service_configured_persistent_max_bytes=retention.get(
            "service_configured_persistent_max_bytes"
        ),
        guard_configured_persistent_max_bytes=retention.get(
            "guard_configured_persistent_max_bytes"
        ),
        target_manifest_contract=runtime.get("target_manifest"),
        worker_service_sha256=activation.get("worker_service_sha256"),
    )
    if typed.to_mapping() != dict(pinset):
        raise RawCaptureReleaseRequirementsError(
            "portable pinset is not canonical"
        )
    _require_frozen_pinset(typed)
    return dict(pinset), typed


def _snapshot_core_release(
    release: BuiltReleaseCandidate,
) -> tuple[dict[str, object], dict[str, bytes]]:
    bodies = {
        "evidence": _read_file(
            release.evidence_path,
            maximum_bytes=MAX_DOCUMENT_BYTES * 16,
            label="release evidence",
        ),
        "integration": _read_file(
            release.integration_archive,
            maximum_bytes=MAX_SIGNED_INT64,
            label="integration archive",
        ),
        "worker": _read_file(
            release.worker_archive,
            maximum_bytes=MAX_SIGNED_INT64,
            label="worker archive",
        ),
    }
    try:
        evidence = verify_release_candidate_evidence_bound_inputs(
            _MemoryBoundInputSource(bodies)
        )
    except Exception as error:
        raise RawCaptureReleaseRequirementsError(
            "v1 release bytes failed descriptor-bound verification"
        ) from error
    evidence_archives = _mapping(evidence.get("archives"), "v1 archives")
    integration = _mapping(
        evidence_archives.get("integration"),
        "v1 integration archive",
    )
    worker = _mapping(evidence_archives.get("worker"), "v1 worker archive")
    if (
        release.integration_sha256 != integration.get("sha256")
        or release.worker_sha256 != worker.get("sha256")
        or release.evidence_sha256 != evidence.get("evidence_sha256")
    ):
        raise RawCaptureReleaseRequirementsError(
            "v1 result identity differs from its descriptor-bound bytes"
        )
    return evidence, bodies


def _materializer_arguments(
    pinset: RawCapturePortablePinset,
) -> dict[str, object]:
    """Return the complete portable v2 argument subset for target execution."""

    _require_frozen_pinset(pinset)
    return {
        "blitzortung_enabled": False,
        "capture_window_hours": pinset.capture_window_hours,
        "expected_activation_matrix_sha256": pinset.activation_matrix_sha256,
        "expected_catalog_sha256": pinset.catalog_sha256,
        "operator_host_operational_free_reserve_bytes": (
            pinset.host_operational_free_reserve_bytes
        ),
    }


def _target_profile(pinset: RawCapturePortablePinset) -> dict[str, object]:
    """Return the exact non-host-bound cadence-only materialization profile."""

    _require_frozen_pinset(pinset)
    return {
        "blitzortung_enabled": False,
        "canonical_absolute_path_selected_at_target": True,
        "contract": TARGET_PROFILE_CONTRACT,
        "direct_child_output_required": True,
        "event_capacity_claimed": False,
        "host_identity_bound": False,
        "host_operational_free_reserve_bytes": (
            pinset.host_operational_free_reserve_bytes
        ),
        "path_semantics_selected_at_target": True,
        "reparse_or_symlink_allowed": False,
        "same_filesystem_layout_required": True,
        "selected_profile": pinset.selected_profile,
    }


def _release_requirements_document(
    *,
    release: BuiltReleaseCandidate,
    evidence: Mapping[str, object],
    release_bodies: Mapping[str, bytes],
    pinset: RawCapturePortablePinset,
    sanitized_options_path: Path,
) -> dict[str, object]:
    _require_frozen_pinset(pinset)
    integration_version = evidence.get("integration_version")
    worker_version = evidence.get("worker_version")
    if (
        not isinstance(integration_version, str)
        or _VERSION.fullmatch(integration_version) is None
        or not isinstance(worker_version, str)
        or _VERSION.fullmatch(worker_version) is None
    ):
        raise RawCaptureReleaseRequirementsError("release versions are invalid")
    materializer_relative = MATERIALIZER_MEMBER_PATH.removeprefix(
        f"{WORKER_ROOT_NAME}/"
    )
    materializer = _tree_file(
        evidence,
        tree_name="worker",
        relative_path=materializer_relative,
    )
    if materializer.get("sha256") != pinset.materializer_sha256:
        raise RawCaptureReleaseRequirementsError(
            "capacity pinset was not derived by the archived materializer"
        )
    archives = _mapping(evidence.get("archives"), "release archives")
    trees = _mapping(evidence.get("source_trees"), "release source trees")
    integration_archive = _mapping(
        archives.get("integration"),
        "integration archive",
    )
    worker_archive = _mapping(archives.get("worker"), "worker archive")
    integration_tree = _mapping(trees.get("integration"), "integration tree")
    worker_tree = _mapping(trees.get("worker"), "worker tree")
    options_body = _read_file(
        sanitized_options_path,
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="sanitized Supervisor options",
    )
    if sanitized_options_path.name != SANITIZED_OPTIONS_FILENAME:
        raise RawCaptureReleaseRequirementsError(
            "sanitized Supervisor options filename differs from its release pin"
        )
    _validate_sanitized_options(options_body)
    worker_package_inventory = _worker_package_inventory(evidence)
    unsigned: dict[str, object] = {
        "archives": {
            "integration": {
                **_body_descriptor(
                    release_bodies["integration"],
                    filename=release.integration_archive.name,
                ),
                "root_name": INTEGRATION_ROOT_NAME,
                "source_tree_sha256": integration_tree.get("content_sha256"),
            },
            "worker": {
                **_body_descriptor(
                    release_bodies["worker"],
                    filename=release.worker_archive.name,
                ),
                "root_name": WORKER_ROOT_NAME,
                "source_tree_sha256": worker_tree.get("content_sha256"),
            },
        },
        "contract": RAW_RELEASE_REQUIREMENTS_CONTRACT,
        "dashboard_output": False,
        "deployment_bundle_complete": False,
        "deployment_performed": False,
        "expected_build_id": (
            f"integration-{integration_version}-worker-{worker_version}"
        ),
        "integration_version": integration_version,
        "live_target_verified": False,
        "materializer": {
            "bytes": materializer.get("size"),
            "contract": pinset.materializer_contract,
            "member_path": MATERIALIZER_MEMBER_PATH,
            "sha256": pinset.materializer_sha256,
            "target_side_execution_required": True,
        },
        "materializer_arguments": _materializer_arguments(pinset),
        "meteorological_admission": False,
        "meteorological_output": False,
        "mutable_runtime_state_packaged": False,
        "pinset": pinset.to_mapping(),
        "production_enable_ready": False,
        "release_tooling": {
            "contract": RELEASE_AUTHORITY_CONTRACT,
            "module_path": RELEASE_AUTHORITY_MODULE_PATH,
            "sha256": FROZEN_RELEASE_AUTHORITY_SHA256,
            "target_tooling_external": True,
        },
        "release_evidence": {
            **_body_descriptor(
                release_bodies["evidence"],
                filename=release.evidence_path.name,
            ),
            "contract": RELEASE_EVIDENCE_CONTRACT,
            "internal_sha256": evidence.get("evidence_sha256"),
        },
        "runtime_mode": RAW_CAPTURE_RUNTIME_MODE,
        "worker_package_inventory": worker_package_inventory,
        "sanitized_supervisor_options": {
            "bytes": len(options_body),
            "contract": SANITIZED_OPTIONS_CONTRACT,
            "filename": SANITIZED_OPTIONS_FILENAME,
            "sha256": _sha_bytes(options_body),
        },
        "target_activation_manifest_sha256": None,
        "target_activation_ready": False,
        "target_artifact_roles": list(TARGET_ARTIFACT_ROLES),
        "target_live_verification_required": True,
        "target_materialization_performed": False,
        "target_materialization_required": True,
        "target_profile": _target_profile(pinset),
        "worker_version": worker_version,
    }
    for kind, archive in (
        ("integration", integration_archive),
        ("worker", worker_archive),
    ):
        descriptor = _mapping(
            _mapping(unsigned["archives"], "requirements archives").get(kind),
            f"requirements {kind} archive",
        )
        if (
            descriptor.get("filename") != archive.get("filename")
            or descriptor.get("bytes") != archive.get("bytes")
            or descriptor.get("sha256") != archive.get("sha256")
        ):
            raise RawCaptureReleaseRequirementsError(
                "requirements archive descriptor differs from v1 evidence"
            )
    _reject_portability_leaks(unsigned)
    return {
        **unsigned,
        "requirements_sha256": _sha_bytes(_canonical(unsigned)),
    }


def _publish_canonical_document(path: Path, document: Mapping[str, object]) -> None:
    """Publish one final marker under pinned authority, rolling back only ours."""

    destination = Path(path)
    absolute = Path(os.path.abspath(destination))
    if destination != absolute or destination.name != _validate_filename(
        destination.name
    ):
        raise RawCaptureReleaseRequirementsError(
            "release output path is not canonical absolute"
        )
    body = _canonical(document) + b"\n"
    authority_module = _load_release_authority()
    stage_name = f".stv3-raw-marker-{secrets.token_hex(16)}.json"
    stage_identity = None
    published_identity = None
    with authority_module.DirectoryAuthority.open_absolute(
        absolute.parent
    ) as authority:
        try:
            authority.reanchor(absolute.parent)
            if authority.exists(absolute.name):
                raise RawCaptureReleaseRequirementsError(
                    "release output already exists"
                )
            with authority.create_file(stage_name) as stage:
                stage.write_all(body)
                stage.flush()
                stage_identity = stage.identity()
            with authority.open_file(stage_name) as staged:
                if (
                    not staged.identity().same_object(stage_identity)
                    or staged.read_bounded(MAX_DOCUMENT_BYTES) != body
                    or staged.identity() != stage_identity
                ):
                    raise RawCaptureReleaseRequirementsError(
                        "staged release marker changed before publication"
                    )
            authority.reanchor(absolute.parent)
            authority.publish_noreplace(
                stage_name,
                absolute.name,
                expected_source=stage_identity,
            )
            published_identity = authority.entry_identity(absolute.name)
            if not published_identity.same_object(stage_identity):
                raise RawCaptureReleaseRequirementsError(
                    "published release marker identity differs"
                )
            if authority.read_file(
                absolute.name,
                maximum_bytes=MAX_DOCUMENT_BYTES,
            ) != body:
                raise RawCaptureReleaseRequirementsError(
                    "published release marker bytes differ"
                )
            authority.fsync()
            authority.reanchor(absolute.parent)
        except BaseException as error:
            cleanup_failures: list[BaseException] = []
            if published_identity is None and stage_identity is not None:
                candidate = getattr(error, "published_identity", None)
                if (
                    candidate is not None
                    and candidate.same_object(stage_identity)
                ):
                    published_identity = candidate
                else:
                    try:
                        candidate = authority.entry_identity(absolute.name)
                    except authority_module.AuthorityNotFound:
                        candidate = None
                    except BaseException as cleanup_error:
                        cleanup_failures.append(cleanup_error)
                        candidate = None
                    if (
                        candidate is not None
                        and candidate.same_object(stage_identity)
                    ):
                        published_identity = candidate
            if published_identity is not None:
                try:
                    current = authority.entry_identity(absolute.name)
                    if not current.same_object(published_identity):
                        raise RawCaptureReleaseRequirementsError(
                            "published marker ownership became indeterminate"
                        )
                    authority.remove_owned_tree(
                        absolute.name,
                        expected_root=published_identity,
                        maximum_entries=1,
                    )
                except authority_module.AuthorityNotFound:
                    pass
                except BaseException as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            elif stage_identity is not None:
                try:
                    current = authority.entry_identity(stage_name)
                    if current.same_object(stage_identity):
                        authority.remove_owned_tree(
                            stage_name,
                            expected_root=stage_identity,
                            maximum_entries=1,
                        )
                    else:
                        raise RawCaptureReleaseRequirementsError(
                            "staged marker ownership became indeterminate"
                        )
                except authority_module.AuthorityNotFound:
                    pass
                except BaseException as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            if cleanup_failures:
                raise RawCaptureReleaseRequirementsError(
                    "release marker publication failed and owned rollback "
                    "could not be proven"
                ) from BaseExceptionGroup(
                    "release marker publication and rollback failures",
                    [error, *cleanup_failures],
                )
            if isinstance(error, RawCaptureReleaseRequirementsError):
                raise
            raise RawCaptureReleaseRequirementsError(
                "release marker could not be published fail-closed"
            ) from error


def build_raw_capture_release_requirements(
    *,
    release: BuiltReleaseCandidate,
    pinset: RawCapturePortablePinset,
    sanitized_options_snapshot: Path,
) -> BuiltRawCaptureReleaseRequirements:
    """Write the fourth RAW release commit marker after verified v1 output."""

    if not isinstance(release, BuiltReleaseCandidate):
        raise RawCaptureReleaseRequirementsError("core release result is invalid")
    evidence, release_bodies = _snapshot_core_release(release)
    document = _release_requirements_document(
        release=release,
        evidence=evidence,
        release_bodies=release_bodies,
        pinset=pinset,
        sanitized_options_path=Path(sanitized_options_snapshot),
    )
    requirements_name = (
        "storm_tracker_raw_capture_release_v"
        f"{evidence['integration_version']}_worker_v{evidence['worker_version']}"
        "_requirements.json"
    )
    destination = release.evidence_path.parent / requirements_name
    requirements_body = _canonical(document) + b"\n"
    options_body = _read_file(
        Path(sanitized_options_snapshot),
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="sanitized Supervisor options",
    )
    precommit = verify_raw_capture_release_requirements_bound_inputs(
        _MemoryBoundInputSource({
            **release_bodies,
            "options": options_body,
            "requirements": requirements_body,
        })
    )
    if precommit != document:
        raise RawCaptureReleaseRequirementsError(
            "staged RAW requirements failed precommit verification"
        )
    _publish_canonical_document(destination, document)
    return BuiltRawCaptureReleaseRequirements(
        core_release=release,
        requirements_path=destination,
        requirements_sha256=str(document["requirements_sha256"]),
        requirements_file_sha256=_sha_bytes(requirements_body),
    )


def _rollback_raw_quartet(
    rollback_authority: Any,
    authority_module: Any,
    *,
    output_name: str,
    output_identity: object | None,
    created_output_identity: object | None,
    stage_name: str,
    stage_identity: object | None,
    names: tuple[str, str, str, str],
    source_identities: Mapping[str, object],
    published_identities: dict[str, object],
) -> list[BaseException]:
    """Remove only exact-owned quartet state and report every uncertainty."""

    cleanup_failures: list[BaseException] = []
    for name in reversed(names):
        expected_source = source_identities.get(name)
        published_identity = published_identities.get(name)
        relative = f"{output_name}/{name}"
        if published_identity is None and expected_source is not None:
            try:
                candidate = rollback_authority.entry_identity(relative)
            except authority_module.AuthorityNotFound:
                candidate = None
            except BaseException as cleanup_error:
                cleanup_failures.append(cleanup_error)
                candidate = None
            if (
                candidate is not None
                and candidate.same_object(expected_source)
            ):
                published_identity = candidate
                published_identities[name] = candidate
        if published_identity is None:
            continue
        try:
            current = rollback_authority.entry_identity(relative)
            if not current.same_object(published_identity):
                raise RawCaptureReleaseRequirementsError(
                    "published RAW quartet ownership became indeterminate"
                )
            rollback_authority.remove_owned_tree(
                relative,
                expected_root=published_identity,
                maximum_entries=1,
            )
            try:
                rollback_authority.entry_identity(relative)
            except authority_module.AuthorityNotFound:
                pass
            else:
                raise RawCaptureReleaseRequirementsError(
                    "published RAW quartet file survived rollback"
                )
        except authority_module.AuthorityNotFound:
            pass
        except BaseException as cleanup_error:
            cleanup_failures.append(cleanup_error)
    if stage_identity is not None:
        try:
            current = rollback_authority.entry_identity(stage_name)
            if not current.same_object(stage_identity):
                raise RawCaptureReleaseRequirementsError(
                    "RAW quartet stage ownership became indeterminate"
                )
            rollback_authority.remove_owned_tree(
                stage_name,
                expected_root=stage_identity,
                maximum_entries=16,
            )
        except authority_module.AuthorityNotFound:
            pass
        except BaseException as cleanup_error:
            cleanup_failures.append(cleanup_error)
    if output_identity is not None:
        try:
            current_output = rollback_authority.entry_identity(output_name)
        except authority_module.AuthorityNotFound:
            current_output = None
        except BaseException as cleanup_error:
            cleanup_failures.append(cleanup_error)
            current_output = None
        if current_output is not None:
            if not current_output.same_object(output_identity):
                cleanup_failures.append(
                    RawCaptureReleaseRequirementsError(
                        "RAW quartet output ownership became indeterminate"
                    )
                )
            else:
                try:
                    with rollback_authority.open_directory(output_name) as final:
                        if not final.identity().same_object(output_identity):
                            raise RawCaptureReleaseRequirementsError(
                                "RAW quartet output changed during rollback"
                            )
                        final.fsync()
                except BaseException as cleanup_error:
                    cleanup_failures.append(cleanup_error)
    if created_output_identity is not None:
        try:
            current_output = rollback_authority.entry_identity(output_name)
            if not current_output.same_object(created_output_identity):
                raise RawCaptureReleaseRequirementsError(
                    "created RAW output ownership became indeterminate"
                )
            rollback_authority.remove_owned_tree(
                output_name,
                expected_root=created_output_identity,
                maximum_entries=1,
            )
            try:
                rollback_authority.entry_identity(output_name)
            except authority_module.AuthorityNotFound:
                pass
            else:
                raise RawCaptureReleaseRequirementsError(
                    "created RAW output survived rollback"
                )
        except authority_module.AuthorityNotFound:
            pass
        except BaseException as cleanup_error:
            cleanup_failures.append(cleanup_error)
    try:
        rollback_authority.fsync()
    except BaseException as cleanup_error:
        cleanup_failures.append(cleanup_error)
    return cleanup_failures


def build_raw_capture_release_quartet(
    *,
    integration_root: Path,
    worker_root: Path,
    integration_version: str,
    worker_version: str,
    output_directory: Path,
    pinset: RawCapturePortablePinset,
    sanitized_options_snapshot: Path,
) -> BuiltRawCaptureReleaseQuartet:
    """Stage all four RAW release files privately; publish requirements last."""

    if (
        not isinstance(integration_version, str)
        or _VERSION.fullmatch(integration_version) is None
        or not isinstance(worker_version, str)
        or _VERSION.fullmatch(worker_version) is None
    ):
        raise RawCaptureReleaseRequirementsError(
            "RAW quartet versions are invalid"
        )
    output = Path(os.path.abspath(output_directory))
    if Path(output_directory) != output or output == Path(output.anchor):
        raise RawCaptureReleaseRequirementsError(
            "RAW quartet output must be canonical absolute"
        )
    names = (
        f"storm_tracker_v3_v{integration_version}_deploy.zip",
        f"storm_tracker_worker_v{worker_version}_deploy.tar.gz",
        (
            f"storm_tracker_release_v{integration_version}_worker_v"
            f"{worker_version}_evidence.json"
        ),
        (
            f"storm_tracker_raw_capture_release_v{integration_version}"
            f"_worker_v{worker_version}_requirements.json"
        ),
    )
    authority_module = _load_release_authority()
    stage_name = f".stv3-raw-quartet-{secrets.token_hex(16)}"
    stage_path = output.parent / stage_name
    stage_identity = None
    created_output_identity = None
    output_identity = None
    staged_release: BuiltReleaseCandidate | None = None
    staged_requirements: BuiltRawCaptureReleaseRequirements | None = None
    source_identities: dict[str, object] = {}
    published_identities: dict[str, object] = {}
    authority_root_identity = None
    with authority_module.DirectoryAuthority.open_absolute(
        output.parent
    ) as authority:
        try:
            authority_root_identity = authority.identity()
            authority.reanchor(output.parent)
            final = None
            try:
                try:
                    final = authority.open_directory(output.name)
                except authority_module.AuthorityNotFound:
                    try:
                        final = authority.create_directory(output.name)
                    except BaseException as create_error:
                        candidate = getattr(
                            create_error,
                            "created_identity",
                            None,
                        )
                        if candidate is not None:
                            created_output_identity = candidate
                        raise
                    created_output_identity = final.identity()
                output_identity = final.identity()
                if any(final.exists(name) for name in names):
                    raise RawCaptureReleaseRequirementsError(
                        "RAW quartet output already contains a release file"
                    )
            finally:
                if final is not None and not final.closed:
                    final.close()
            stage = authority.create_directory(stage_name)
            stage_identity = stage.identity()
            stage.close()
            authority.reanchor(output.parent)
            staged_release = build_release_candidate(
                integration_root=integration_root,
                worker_root=worker_root,
                integration_version=integration_version,
                worker_version=worker_version,
                output_directory=stage_path,
            )
            staged_requirements = build_raw_capture_release_requirements(
                release=staged_release,
                pinset=pinset,
                sanitized_options_snapshot=sanitized_options_snapshot,
            )
            verify_raw_capture_release_requirements(
                staged_requirements.requirements_path,
                sanitized_options_snapshot=sanitized_options_snapshot,
            )
            expected_hashes = {
                names[0]: staged_release.integration_sha256,
                names[1]: staged_release.worker_sha256,
                names[2]: _sha_bytes(
                    _read_file(
                        staged_release.evidence_path,
                        maximum_bytes=MAX_DOCUMENT_BYTES * 16,
                        label="staged v1 evidence",
                    )
                ),
                names[3]: staged_requirements.requirements_file_sha256,
            }
            for name in names:
                relative = f"{stage_name}/{name}"
                identity = authority.entry_identity(relative)
                body = authority.read_file(
                    relative,
                    maximum_bytes=(
                        MAX_DOCUMENT_BYTES * 16
                        if name.endswith(".json")
                        else 2 * 1024 * 1024 * 1024
                    ),
                )
                if _sha_bytes(body) != expected_hashes[name]:
                    raise RawCaptureReleaseRequirementsError(
                        "staged RAW quartet bytes differ before publication"
                    )
                source_identities[name] = identity
            with authority.open_directory(output.name) as final:
                if (
                    not final.identity().same_object(output_identity)
                    or any(final.exists(name) for name in names)
                ):
                    raise RawCaptureReleaseRequirementsError(
                        "RAW quartet output identity changed before publication"
                    )
            authority.reanchor(output.parent)
            for name in names:
                try:
                    authority.publish_noreplace(
                        f"{stage_name}/{name}",
                        f"{output.name}/{name}",
                        expected_source=source_identities[name],
                    )
                except BaseException as publish_error:
                    candidate = getattr(
                        publish_error,
                        "published_identity",
                        None,
                    )
                    if (
                        candidate is not None
                        and candidate.same_object(source_identities[name])
                    ):
                        published_identities[name] = candidate
                    raise
                published_identities[name] = source_identities[name]
                observed = authority.entry_identity(f"{output.name}/{name}")
                if not observed.same_object(source_identities[name]):
                    raise RawCaptureReleaseRequirementsError(
                        "published RAW quartet identity differs"
                    )
                published_identities[name] = observed
                published_body = authority.read_file(
                    f"{output.name}/{name}",
                    maximum_bytes=(
                        MAX_DOCUMENT_BYTES * 16
                        if name.endswith(".json")
                        else 2 * 1024 * 1024 * 1024
                    ),
                )
                if _sha_bytes(published_body) != expected_hashes[name]:
                    raise RawCaptureReleaseRequirementsError(
                        "published RAW quartet bytes differ"
                    )
            authority.remove_owned_tree(
                stage_name,
                expected_root=stage_identity,
                maximum_entries=1,
            )
            stage_identity = None
            with authority.open_directory(output.name) as final:
                if not final.identity().same_object(output_identity):
                    raise RawCaptureReleaseRequirementsError(
                        "RAW quartet output identity changed after publication"
                    )
                for name in names:
                    observed = final.entry_identity(name)
                    if not observed.same_object(source_identities[name]):
                        raise RawCaptureReleaseRequirementsError(
                            "RAW quartet final identity changed before commit"
                        )
                    final_body = final.read_file(
                        name,
                        maximum_bytes=(
                            MAX_DOCUMENT_BYTES * 16
                            if name.endswith(".json")
                            else 2 * 1024 * 1024 * 1024
                        ),
                    )
                    if _sha_bytes(final_body) != expected_hashes[name]:
                        raise RawCaptureReleaseRequirementsError(
                            "RAW quartet final bytes changed before commit"
                        )
                final.fsync()
            authority.fsync()
            authority.reanchor(output.parent)
            authority.close()
        except BaseException as error:
            cleanup_failures: list[BaseException] = []
            rollback_authority = None
            replacement_authority = None
            if authority.closed:
                try:
                    replacement_authority = (
                        authority_module.DirectoryAuthority.open_absolute(
                            output.parent
                        )
                    )
                    if (
                        authority_root_identity is None
                        or not replacement_authority.identity().same_object(
                            authority_root_identity
                        )
                    ):
                        raise RawCaptureReleaseRequirementsError(
                            "RAW quartet parent changed before close rollback"
                        )
                    rollback_authority = replacement_authority
                except BaseException as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            else:
                rollback_authority = authority
            if rollback_authority is not None:
                cleanup_failures.extend(_rollback_raw_quartet(
                    rollback_authority,
                    authority_module,
                    output_name=output.name,
                    output_identity=output_identity,
                    created_output_identity=created_output_identity,
                    stage_name=stage_name,
                    stage_identity=stage_identity,
                    names=names,
                    source_identities=source_identities,
                    published_identities=published_identities,
                ))
            if (
                replacement_authority is not None
                and not replacement_authority.closed
            ):
                try:
                    replacement_authority.close()
                except BaseException as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            if not authority.closed:
                try:
                    authority.close()
                except BaseException as cleanup_error:
                    cleanup_failures.append(cleanup_error)
            if cleanup_failures:
                raise RawCaptureReleaseRequirementsError(
                    "RAW quartet failed and exact-owned rollback was incomplete"
                ) from BaseExceptionGroup(
                    "RAW quartet failure and rollback failures",
                    [error, *cleanup_failures],
                )
            if isinstance(error, RawCaptureReleaseRequirementsError):
                raise
            raise RawCaptureReleaseRequirementsError(
                "RAW quartet orchestration failed before a trusted commit"
            ) from error
    if staged_release is None or staged_requirements is None:
        raise RawCaptureReleaseRequirementsError(
            "RAW quartet orchestration produced no result"
        )
    final_release = BuiltReleaseCandidate(
        integration_archive=output / names[0],
        worker_archive=output / names[1],
        evidence_path=output / names[2],
        integration_sha256=staged_release.integration_sha256,
        worker_sha256=staged_release.worker_sha256,
        evidence_sha256=staged_release.evidence_sha256,
    )
    final_requirements = BuiltRawCaptureReleaseRequirements(
        core_release=final_release,
        requirements_path=output / names[3],
        requirements_sha256=staged_requirements.requirements_sha256,
        requirements_file_sha256=(
            staged_requirements.requirements_file_sha256
        ),
    )
    return BuiltRawCaptureReleaseQuartet(
        core_release=final_release,
        requirements=final_requirements,
    )


def _validate_requirements_document(
    document: Mapping[str, object],
    *,
    evidence: Mapping[str, object],
    evidence_file: Mapping[str, object],
    integration_file: Mapping[str, object],
    worker_file: Mapping[str, object],
    options_file: Mapping[str, object],
) -> dict[str, object]:
    expected_keys = {
        "archives",
        "contract",
        "dashboard_output",
        "deployment_bundle_complete",
        "deployment_performed",
        "expected_build_id",
        "integration_version",
        "live_target_verified",
        "materializer",
        "materializer_arguments",
        "meteorological_admission",
        "meteorological_output",
        "mutable_runtime_state_packaged",
        "pinset",
        "production_enable_ready",
        "release_evidence",
        "release_tooling",
        "requirements_sha256",
        "runtime_mode",
        "worker_package_inventory",
        "sanitized_supervisor_options",
        "target_activation_manifest_sha256",
        "target_activation_ready",
        "target_artifact_roles",
        "target_live_verification_required",
        "target_materialization_performed",
        "target_materialization_required",
        "target_profile",
        "worker_version",
    }
    if set(document) != expected_keys:
        raise RawCaptureReleaseRequirementsError(
            "RAW release requirements have an unknown schema"
        )
    unsigned = dict(document)
    supplied_sha = unsigned.pop("requirements_sha256", None)
    if (
        document.get("contract") != RAW_RELEASE_REQUIREMENTS_CONTRACT
        or supplied_sha != _sha_bytes(_canonical(unsigned))
        or document.get("deployment_bundle_complete") is not False
        or document.get("deployment_performed") is not False
        or document.get("target_materialization_required") is not True
        or document.get("target_materialization_performed") is not False
        or document.get("target_live_verification_required") is not True
        or document.get("target_activation_manifest_sha256") is not None
        or document.get("target_activation_ready") is not False
        or document.get("live_target_verified") is not False
        or document.get("runtime_mode") != RAW_CAPTURE_RUNTIME_MODE
        or document.get("mutable_runtime_state_packaged") is not False
        or document.get("meteorological_admission") is not False
        or document.get("meteorological_output") is not False
        or document.get("dashboard_output") is not False
        or document.get("production_enable_ready") is not False
        or document.get("target_artifact_roles") != list(TARGET_ARTIFACT_ROLES)
    ):
        raise RawCaptureReleaseRequirementsError(
            "RAW release requirements make an invalid readiness claim"
        )
    _reject_portability_leaks(unsigned)
    _pinset_mapping, typed_pinset = _validated_pinset_mapping(
        document.get("pinset")
    )
    integration_version = document.get("integration_version")
    worker_version = document.get("worker_version")
    if (
        integration_version != evidence.get("integration_version")
        or worker_version != evidence.get("worker_version")
        or document.get("expected_build_id")
        != f"integration-{integration_version}-worker-{worker_version}"
    ):
        raise RawCaptureReleaseRequirementsError(
            "RAW requirements release identity differs from v1 evidence"
        )
    release_evidence = _mapping(
        document.get("release_evidence"),
        "requirements release evidence",
    )
    if set(release_evidence) != {
        "bytes", "contract", "filename", "internal_sha256", "sha256"
    }:
        raise RawCaptureReleaseRequirementsError(
            "requirements release-evidence descriptor is invalid"
        )
    if (
        release_evidence.get("contract") != RELEASE_EVIDENCE_CONTRACT
        or release_evidence.get("filename")
        != (
            f"storm_tracker_release_v{integration_version}_worker_v"
            f"{worker_version}_evidence.json"
        )
        or release_evidence.get("bytes") != evidence_file.get("bytes")
        or release_evidence.get("sha256") != evidence_file.get("sha256")
        or release_evidence.get("internal_sha256")
        != evidence.get("evidence_sha256")
    ):
        raise RawCaptureReleaseRequirementsError(
            "requirements do not bind exact v1 evidence bytes"
        )
    release_tooling = _mapping(
        document.get("release_tooling"),
        "requirements release tooling",
    )
    if release_tooling != {
        "contract": RELEASE_AUTHORITY_CONTRACT,
        "module_path": RELEASE_AUTHORITY_MODULE_PATH,
        "sha256": FROZEN_RELEASE_AUTHORITY_SHA256,
        "target_tooling_external": True,
    }:
        raise RawCaptureReleaseRequirementsError(
            "requirements release tooling differs from its freeze"
        )
    archives = _mapping(document.get("archives"), "requirements archives")
    evidence_archives = _mapping(evidence.get("archives"), "evidence archives")
    evidence_trees = _mapping(evidence.get("source_trees"), "evidence trees")
    if set(archives) != {"integration", "worker"}:
        raise RawCaptureReleaseRequirementsError(
            "requirements archive set is invalid"
        )
    for kind, observed, root_name in (
        ("integration", integration_file, INTEGRATION_ROOT_NAME),
        ("worker", worker_file, WORKER_ROOT_NAME),
    ):
        descriptor = _mapping(archives.get(kind), f"requirements {kind}")
        archive = _mapping(evidence_archives.get(kind), f"evidence {kind}")
        tree = _mapping(evidence_trees.get(kind), f"evidence {kind} tree")
        if set(descriptor) != {
            "bytes", "filename", "root_name", "sha256", "source_tree_sha256"
        }:
            raise RawCaptureReleaseRequirementsError(
                "requirements archive descriptor is invalid"
            )
        if (
            descriptor.get("bytes") != observed.get("bytes")
            or descriptor.get("sha256") != observed.get("sha256")
            or descriptor.get("filename") != archive.get("filename")
            or descriptor.get("bytes") != archive.get("bytes")
            or descriptor.get("sha256") != archive.get("sha256")
            or descriptor.get("root_name") != root_name
            or descriptor.get("source_tree_sha256")
            != tree.get("content_sha256")
        ):
            raise RawCaptureReleaseRequirementsError(
                "requirements archive binding differs from v1 evidence"
            )
    options = _mapping(
        document.get("sanitized_supervisor_options"),
        "sanitized Supervisor options descriptor",
    )
    if (
        set(options) != {"bytes", "contract", "filename", "sha256"}
        or options.get("bytes") != options_file.get("bytes")
        or options.get("sha256") != options_file.get("sha256")
        or options.get("contract") != SANITIZED_OPTIONS_CONTRACT
        or options.get("filename") != SANITIZED_OPTIONS_FILENAME
    ):
        raise RawCaptureReleaseRequirementsError(
            "requirements sanitized-options binding differs"
        )
    materializer = _mapping(document.get("materializer"), "materializer")
    if set(materializer) != {
        "bytes", "contract", "member_path", "sha256",
        "target_side_execution_required",
    }:
        raise RawCaptureReleaseRequirementsError(
            "requirements materializer descriptor is invalid"
        )
    tree_item = _tree_file(
        evidence,
        tree_name="worker",
        relative_path=MATERIALIZER_MEMBER_PATH.removeprefix(
            f"{WORKER_ROOT_NAME}/"
        ),
    )
    if (
        materializer.get("member_path") != MATERIALIZER_MEMBER_PATH
        or materializer.get("bytes") != tree_item.get("size")
        or materializer.get("sha256") != tree_item.get("sha256")
        or materializer.get("target_side_execution_required") is not True
        or materializer.get("contract") != typed_pinset.materializer_contract
        or materializer.get("sha256") != typed_pinset.materializer_sha256
    ):
        raise RawCaptureReleaseRequirementsError(
            "requirements materializer differs from the worker archive"
        )
    materializer_arguments = _mapping(
        document.get("materializer_arguments"),
        "materializer arguments",
    )
    if dict(materializer_arguments) != _materializer_arguments(typed_pinset):
        raise RawCaptureReleaseRequirementsError(
            "requirements materializer arguments differ from the frozen profile"
        )
    worker_package_inventory = _mapping(
        document.get("worker_package_inventory"),
        "worker package inventory",
    )
    if dict(worker_package_inventory) != _worker_package_inventory(evidence):
        raise RawCaptureReleaseRequirementsError(
            "requirements worker package inventory differs from archive"
        )
    target_profile = _mapping(document.get("target_profile"), "target profile")
    if dict(target_profile) != _target_profile(typed_pinset):
        raise RawCaptureReleaseRequirementsError(
            "requirements target profile is not portable"
        )
    return dict(document)


def verify_raw_capture_release_requirements(
    requirements_path: Path,
    *,
    sanitized_options_snapshot: Path,
) -> dict[str, object]:
    """Verify a local RAW quartet and its separately retained options input."""

    path = Path(requirements_path)
    body = _read_file(
        path,
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="RAW release requirements",
    )
    document = _strict_canonical_document(
        body,
        label="RAW release requirements",
    )
    release_evidence = _mapping(document.get("release_evidence"), "release evidence")
    archives = _mapping(document.get("archives"), "release archives")
    integration = _mapping(archives.get("integration"), "integration archive")
    worker = _mapping(archives.get("worker"), "worker archive")
    evidence_name = _validate_filename(release_evidence.get("filename"))
    integration_name = _validate_filename(integration.get("filename"))
    worker_name = _validate_filename(worker.get("filename"))
    directory = path.parent
    evidence_path = directory / evidence_name
    integration_path = directory / integration_name
    worker_path = directory / worker_name
    if Path(sanitized_options_snapshot).name != SANITIZED_OPTIONS_FILENAME:
        raise RawCaptureReleaseRequirementsError(
            "sanitized Supervisor options filename differs from its release pin"
        )
    options_body = _read_file(
        Path(sanitized_options_snapshot),
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="sanitized Supervisor options",
    )
    _validate_sanitized_options(options_body)
    source = _MemoryBoundInputSource({
        "evidence": _read_file(
            evidence_path,
            maximum_bytes=MAX_DOCUMENT_BYTES * 16,
            label="release evidence",
        ),
        "integration": _read_file(
            integration_path,
            maximum_bytes=MAX_SIGNED_INT64,
            label="integration archive",
        ),
        "options": options_body,
        "requirements": body,
        "worker": _read_file(
            worker_path,
            maximum_bytes=MAX_SIGNED_INT64,
            label="worker archive",
        ),
    })
    verified = verify_raw_capture_release_requirements_bound_inputs(source)
    expected_name = (
        f"storm_tracker_raw_capture_release_v{verified['integration_version']}"
        f"_worker_v{verified['worker_version']}_requirements.json"
    )
    _validate_filename(path.name, expected_name)
    return verified


class _BoundSubview:
    def __init__(self, source: BoundInputSource, keys: Iterable[str]) -> None:
        self._source = source
        self._keys = tuple(sorted(keys))

    @property
    def keys(self) -> tuple[str, ...]:
        return self._keys

    def require_exact_keys(self, expected_keys: Iterable[str]) -> None:
        if tuple(sorted(expected_keys)) != self._keys:
            raise RawCaptureReleaseRequirementsError(
                "bound subview has an unexpected key set"
            )

    def metadata(self, key: str) -> _BoundMetadata:
        if key not in self._keys:
            raise RawCaptureReleaseRequirementsError("bound subview key is absent")
        return self._source.metadata(key)

    def open_stream(self, key: str) -> AbstractContextManager[BinaryIO]:
        if key not in self._keys:
            raise RawCaptureReleaseRequirementsError("bound subview key is absent")
        return self._source.open_stream(key)


def _bound_descriptor(source: BoundInputSource, key: str) -> dict[str, object]:
    try:
        metadata = source.metadata(key)
        size = metadata.size
        sha256 = metadata.sha256
    except Exception as error:
        raise RawCaptureReleaseRequirementsError(
            "bound RAW input metadata is unavailable"
        ) from error
    if (
        type(size) is not int
        or size < 0
        or size > MAX_SIGNED_INT64
        or not isinstance(sha256, str)
        or _SHA256.fullmatch(sha256) is None
    ):
        raise RawCaptureReleaseRequirementsError(
            "bound RAW input metadata is invalid"
        )
    return {"bytes": size, "sha256": sha256}


def _read_bound(
    source: BoundInputSource,
    key: str,
    *,
    maximum_bytes: int,
) -> tuple[bytes, dict[str, object]]:
    """Read one bound input from one immutable metadata snapshot."""

    descriptor = _bound_descriptor(source, key)
    if descriptor["bytes"] > maximum_bytes:
        raise RawCaptureReleaseRequirementsError("bound RAW input exceeds its limit")
    try:
        with source.open_stream(key) as stream:
            digest = hashlib.sha256()
            body = bytearray()
            while True:
                remaining = maximum_bytes - len(body)
                chunk = stream.read(min(1024 * 1024, remaining + 1))
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise RawCaptureReleaseRequirementsError(
                        "bound RAW stream is not binary"
                    )
                body.extend(chunk)
                digest.update(chunk)
                if len(body) > maximum_bytes:
                    raise RawCaptureReleaseRequirementsError(
                        "bound RAW input exceeds its limit"
                    )
    except RawCaptureReleaseRequirementsError:
        raise
    except Exception as error:
        raise RawCaptureReleaseRequirementsError(
            "bound RAW input changed while read"
        ) from error
    if (
        len(body) != descriptor["bytes"]
        or digest.hexdigest() != descriptor["sha256"]
    ):
        raise RawCaptureReleaseRequirementsError(
            "bound RAW input differs from its seal"
        )
    return bytes(body), descriptor


def _seal_bound_inputs(
    source: BoundInputSource,
    maximum_bytes_by_key: Mapping[str, int],
) -> tuple[dict[str, bytes], dict[str, dict[str, object]]]:
    """Consume every caller-controlled metadata/stream pair exactly once."""

    bodies: dict[str, bytes] = {}
    descriptors: dict[str, dict[str, object]] = {}
    for key in sorted(maximum_bytes_by_key):
        body, descriptor = _read_bound(
            source,
            key,
            maximum_bytes=maximum_bytes_by_key[key],
        )
        bodies[key] = body
        descriptors[key] = descriptor
    return bodies, descriptors


def verify_raw_capture_release_requirements_bound_inputs(
    source: BoundInputSource,
) -> dict[str, object]:
    """Verify the RAW requirements using exact descriptor-bound streams."""

    try:
        source.require_exact_keys(REQUIREMENTS_BOUND_INPUT_KEYS)
    except Exception as error:
        raise RawCaptureReleaseRequirementsError(
            "bound RAW requirements inputs have an unexpected key set"
        ) from error
    bodies, descriptors = _seal_bound_inputs(source, {
        "evidence": MAX_DOCUMENT_BYTES * 16,
        "integration": MAX_SIGNED_INT64,
        "options": MAX_DOCUMENT_BYTES,
        "requirements": MAX_DOCUMENT_BYTES,
        "worker": MAX_SIGNED_INT64,
    })
    requirements_body = bodies["requirements"]
    document = _strict_canonical_document(
        requirements_body,
        label="bound RAW release requirements",
    )
    options_body = bodies["options"]
    _validate_sanitized_options(options_body)
    sealed_source = _MemoryBoundInputSource(bodies)
    try:
        evidence = verify_release_candidate_evidence_bound_inputs(
            _BoundSubview(
                sealed_source,
                ("evidence", "integration", "worker"),
            )
        )
    except Exception as error:
        raise RawCaptureReleaseRequirementsError(
            "bound v1 release inputs failed verification"
        ) from error
    return _validate_requirements_document(
        document,
        evidence=evidence,
        evidence_file=descriptors["evidence"],
        integration_file=descriptors["integration"],
        worker_file=descriptors["worker"],
        options_file=descriptors["options"],
    )


def _target_manifest_artifacts(
    target_manifest: Mapping[str, object],
) -> dict[str, Mapping[str, object]]:
    rows = target_manifest.get("artifacts")
    if not isinstance(rows, list) or len(rows) != len(TARGET_ARTIFACT_ROLES):
        raise RawCaptureReleaseRequirementsError(
            "target activation manifest artifact set is invalid"
        )
    result: dict[str, Mapping[str, object]] = {}
    observed_paths: list[str] = []
    for row in rows:
        item = _mapping(row, "target artifact")
        if set(item) != {"bytes", "path", "role", "sha256"}:
            raise RawCaptureReleaseRequirementsError(
                "target activation artifact descriptor is invalid"
            )
        role = item.get("role")
        if role not in TARGET_ARTIFACT_ROLES or role in result:
            raise RawCaptureReleaseRequirementsError(
                "target activation artifact role is invalid"
            )
        _positive_integer(
            item.get("bytes"),
            "target artifact bytes",
            maximum=MAX_TARGET_ARTIFACT_BYTES,
        )
        _checked_sha256(item.get("sha256"), "target artifact sha256")
        if item.get("path") != TARGET_ARTIFACT_RELATIVE_PATHS[str(role)]:
            raise RawCaptureReleaseRequirementsError(
                "target activation artifact path differs from materializer"
            )
        observed_paths.append(str(item.get("path")))
        result[str(role)] = item
    if set(result) != set(TARGET_ARTIFACT_ROLES):
        raise RawCaptureReleaseRequirementsError(
            "target activation artifact roles are incomplete"
        )
    if observed_paths != sorted(observed_paths):
        raise RawCaptureReleaseRequirementsError(
            "target activation artifacts are not canonically ordered"
        )
    return result


def _target_manifest_path_pair(
    target_manifest: Mapping[str, object],
) -> tuple[object, object]:
    flavour = target_manifest.get("target_path_flavour")
    output_value = target_manifest.get("output_directory")
    base_value = target_manifest.get("persistent_base_root")
    if (
        flavour not in {"posix", "windows"}
        or not isinstance(output_value, str)
        or not isinstance(base_value, str)
    ):
        raise RawCaptureReleaseRequirementsError(
            "target activation manifest path profile is invalid"
        )
    path_type = PureWindowsPath if flavour == "windows" else PurePosixPath
    output = path_type(output_value)
    base = path_type(base_value)
    if (
        not output.is_absolute()
        or not base.is_absolute()
        or output == base
        or output.parent != base
        or str(output) != output_value
        or str(base) != base_value
    ):
        raise RawCaptureReleaseRequirementsError(
            "target activation root paths are not canonical direct children"
        )
    return output, base


def _validate_target_manifest_bindings(
    target_manifest: Mapping[str, object],
    requirements: Mapping[str, object],
) -> dict[str, Mapping[str, object]]:
    if set(target_manifest) != TARGET_MANIFEST_FIELDS:
        raise RawCaptureReleaseRequirementsError(
            "target activation manifest has an unknown schema"
        )
    pinset = _mapping(requirements.get("pinset"), "requirements pinset")
    activation = _mapping(pinset.get("activation"), "activation pinset")
    capacity = _mapping(pinset.get("capacity"), "capacity pinset")
    retention = _mapping(pinset.get("retention"), "retention pinset")
    materializer = _mapping(requirements.get("materializer"), "materializer")
    materializer_arguments = _mapping(
        requirements.get("materializer_arguments"),
        "requirements materializer arguments",
    )
    runtime_contracts = _mapping(
        pinset.get("runtime_contracts"),
        "runtime contracts",
    )
    parameters = _mapping(
        target_manifest.get("capacity_parameters"),
        "target capacity parameters",
    )
    target_declared_bytes = _positive_integer(
        parameters.get("operator_declared_target_bytes"),
        "target operator-declared capacity",
        maximum=MAX_SIGNED_INT64 - 1,
    )
    minimum_required = _positive_integer(
        capacity.get("minimum_required_same_filesystem_bytes"),
        "minimum required same-filesystem bytes",
    )
    output, _base = _target_manifest_path_pair(target_manifest)
    if (
        target_manifest.get("contract") != runtime_contracts.get("target_manifest")
        or target_manifest.get("artifact_role") != "target_side_materialization"
        or target_manifest.get("deployment_performed") is not False
        or target_manifest.get("portable_release_artifact") is not False
        or target_manifest.get("host_path_pinned") is not True
        or target_manifest.get("immutable_artifact_count") != 4
        or target_manifest.get("target_materialization_required") is not True
        or target_manifest.get("reproducibility_scope")
        != "same_canonical_target_root_and_path_semantics_only"
        or target_manifest.get("runtime_mode") != RAW_CAPTURE_RUNTIME_MODE
        or target_manifest.get("raw_capture_config_contract")
        != runtime_contracts.get("config")
        or target_manifest.get("raw_capture_runtime_contract")
        != runtime_contracts.get("runtime")
        or target_manifest.get("catalog_pins") != {
            "activation_matrix_sha256": activation.get(
                "activation_matrix_sha256"
            ),
            "catalog_sha256": activation.get("catalog_sha256"),
        }
        or set(parameters) != {
            "blitzortung_enabled",
            "capture_window_hours",
            "operator_host_operational_free_reserve_bytes",
            "operator_declared_target_bytes",
        }
        or parameters.get("blitzortung_enabled") is not False
        or parameters.get("capture_window_hours")
        != capacity.get("capture_window_hours")
        or parameters.get("operator_host_operational_free_reserve_bytes")
        != retention.get("host_operational_free_reserve_bytes")
        or materializer_arguments != {
            "blitzortung_enabled": parameters.get("blitzortung_enabled"),
            "capture_window_hours": parameters.get("capture_window_hours"),
            "expected_activation_matrix_sha256": activation.get(
                "activation_matrix_sha256"
            ),
            "expected_catalog_sha256": activation.get("catalog_sha256"),
            "operator_host_operational_free_reserve_bytes": parameters.get(
                "operator_host_operational_free_reserve_bytes"
            ),
        }
        or target_declared_bytes < minimum_required
    ):
        raise RawCaptureReleaseRequirementsError(
            "target activation manifest differs from portable requirements"
        )
    manifest_materializer = _mapping(
        target_manifest.get("materializer"),
        "target materializer",
    )
    if (
        set(manifest_materializer) != {
            "capacity_derivation_target_path_independent",
            "contract",
            "module_path",
            "module_sha256",
            "target_side_execution_required",
        }
        or
        manifest_materializer.get("contract") != materializer.get("contract")
        or manifest_materializer.get("module_path")
        != materializer.get("member_path")
        or manifest_materializer.get("module_sha256")
        != materializer.get("sha256")
        or manifest_materializer.get("target_side_execution_required") is not True
        or manifest_materializer.get(
            "capacity_derivation_target_path_independent"
        ) is not True
    ):
        raise RawCaptureReleaseRequirementsError(
            "target activation manifest materializer differs"
        )
    artifacts = _target_manifest_artifacts(target_manifest)
    activation_environment = _mapping(
        target_manifest.get("activation_environment"),
        "target activation environment",
    )
    config_path = output.joinpath(
        *PurePosixPath(TARGET_ARTIFACT_RELATIVE_PATHS["raw_capture_config"]).parts
    )
    if (
        set(activation_environment) != {
            "STV3_RUNTIME_MODE",
            "STV3_WORKER_FIRST_AUTHORITY_SHA256",
            "STV3_WORKER_FIRST_CONFIG_PATH",
            "STV3_WORKER_FIRST_CONFIG_SHA256",
        }
        or activation_environment.get("STV3_RUNTIME_MODE")
        != RAW_CAPTURE_RUNTIME_MODE
        or activation_environment.get("STV3_WORKER_FIRST_CONFIG_PATH")
        != str(config_path)
        or activation_environment.get("STV3_WORKER_FIRST_CONFIG_SHA256")
        != artifacts["raw_capture_config"].get("sha256")
        or activation_environment.get("STV3_WORKER_FIRST_AUTHORITY_SHA256")
        != artifacts["worker_first_authority"].get("sha256")
    ):
        raise RawCaptureReleaseRequirementsError(
            "target activation environment differs from artifact bytes"
        )
    bootstrap = _mapping(
        target_manifest.get("bootstrap_receipt"),
        "target bootstrap receipt",
    )
    if (
        set(bootstrap) != {
            "authority_scope_sha256",
            "authority_sha256",
            "config_sha256",
            "contract",
            "live_wiring",
            "process_authority_sha256",
            "store_binding_sha256",
            "store_count",
        }
        or bootstrap.get("live_wiring") is not False
        or bootstrap.get("authority_sha256")
        != artifacts["worker_first_authority"].get("sha256")
    ):
        raise RawCaptureReleaseRequirementsError(
            "target bootstrap receipt differs from artifact bytes"
        )
    for key in (
        "authority_scope_sha256",
        "config_sha256",
        "process_authority_sha256",
        "store_binding_sha256",
    ):
        _checked_sha256(bootstrap.get(key), f"target bootstrap {key}")
    _positive_integer(
        bootstrap.get("store_count"),
        "target bootstrap store count",
        maximum=1_000,
    )
    mutable_paths = target_manifest.get("mutable_runtime_paths")
    if not isinstance(mutable_paths, list) or len(mutable_paths) != 13:
        raise RawCaptureReleaseRequirementsError(
            "target mutable runtime path inventory is incomplete"
        )
    seen_roles: set[str] = set()
    seen_paths: set[str] = set()
    for item in mutable_paths:
        row = _mapping(item, "target mutable runtime path")
        relative = row.get("path")
        role = row.get("role")
        if (
            set(row) != {"hash_policy", "path", "role"}
            or row.get("hash_policy") != "excluded_mutable_runtime_state"
            or not isinstance(relative, str)
            or not isinstance(role, str)
            or role in seen_roles
            or relative in seen_paths
        ):
            raise RawCaptureReleaseRequirementsError(
                "target mutable runtime path descriptor is invalid"
            )
        pure = PurePosixPath(relative)
        if pure.is_absolute() or any(
            component in {"", ".", ".."} for component in pure.parts
        ) or "\\" in relative:
            raise RawCaptureReleaseRequirementsError(
                "target mutable runtime path escapes its root"
            )
        seen_roles.add(role)
        seen_paths.add(relative)
    if any(role.startswith("event.blitzortung_") for role in seen_roles):
        raise RawCaptureReleaseRequirementsError(
            "dormant event paths entered the cadence-only target inventory"
        )
    if [row.get("role") for row in mutable_paths] != sorted(seen_roles):
        raise RawCaptureReleaseRequirementsError(
            "target mutable runtime path inventory is not ordered"
        )
    return artifacts


def _validate_target_config_document(
    body: bytes,
    *,
    requirements: Mapping[str, object],
    target_manifest: Mapping[str, object],
) -> dict[str, object]:
    document = _strict_json(
        body,
        label="target raw-capture config",
        allow_floats=True,
    )
    if _canonical(document) != body:
        raise RawCaptureReleaseRequirementsError(
            "target raw-capture config is not canonical"
        )
    worker_first = _mapping(
        document.get("worker_first"),
        "target worker-first config",
    )
    bootstrap = _mapping(
        target_manifest.get("bootstrap_receipt"),
        "target bootstrap receipt",
    )
    _pinset, typed = _validated_pinset_mapping(requirements.get("pinset"))
    acquisition = _mapping(
        document.get("acquisition"),
        "target acquisition config",
    )
    cadence_limits = _mapping(
        acquisition.get("raw_store_limits"),
        "target cadence raw-store limits",
    )
    capacity_profile = _mapping(
        acquisition.get("capacity_profile"),
        "target cadence capacity profile",
    )
    event = _mapping(
        _mapping(document.get("event_ingress"), "target event config").get(
            "blitzortung"
        ),
        "target Blitzortung config",
    )
    retention = _mapping(document.get("retention"), "target retention config")
    cadence_service_envelope = (
        _positive_integer(
            cadence_limits.get("max_total_bytes"),
            "target cadence raw-store maximum",
        )
        + _positive_integer(
            cadence_limits.get("max_catalog_bytes"),
            "target cadence raw-store catalog maximum",
        )
    )
    guard_envelope = cadence_service_envelope + _positive_integer(
        capacity_profile.get("max_catalog_bytes"),
        "target cadence ledger catalog maximum",
    )
    if (
        document.get("contract")
        != typed.raw_capture_config_contract
        or document.get("contract")
        != target_manifest.get("raw_capture_config_contract")
        or document.get("live_wiring") is not True
        or _sha_bytes(_canonical(worker_first))
        != bootstrap.get("config_sha256")
        or document.get("dashboard_output") is not False
        or document.get("meteorological_admission") is not False
        or document.get("meteorological_output") is not False
        or document.get("production_enable_ready") is not False
        or event.get("enabled") is not False
        or event.get("capacity_claimed") is not False
        or event.get("operational_admission") is not False
        or retention.get("automatic_reclaim") is not False
        or retention.get("capacity_exhaustion") != "fail_closed"
        or retention.get("source_reclaim_authorized") is not False
        or retention.get("host_operational_free_reserve_bytes")
        != typed.host_operational_free_reserve_bytes
        or cadence_service_envelope
        != typed.service_configured_persistent_max_bytes
        or guard_envelope != typed.guard_configured_persistent_max_bytes
    ):
        raise RawCaptureReleaseRequirementsError(
            "target raw-capture config differs from bootstrap authority"
        )
    _reject_secret_fields(document)
    return document


def _validate_target_capacity_document(
    body: bytes,
    *,
    requirements: Mapping[str, object],
    target_manifest: Mapping[str, object],
) -> tuple[dict[str, object], int, int]:
    document = _strict_json(
        body,
        label="target capacity document",
        allow_floats=True,
    )
    if _canonical(document) != body:
        raise RawCaptureReleaseRequirementsError(
            "target capacity document is not canonical"
        )
    _pinset, typed = _validated_pinset_mapping(requirements.get("pinset"))
    activation = _mapping(document.get("activation"), "target activation pins")
    event = _mapping(document.get("event_ingress"), "target event ingress")
    ingress_limits = _mapping(
        event.get("ingress_limits"),
        "target Blitzortung ingress limits",
    )
    raw_store_limits = _mapping(
        event.get("raw_store_limits"),
        "target Blitzortung raw store limits",
    )
    service_capacity = _mapping(
        document.get("service_storage_capacity_diagnostics"),
        "target service storage capacity diagnostics",
    )
    service_combined = _mapping(
        service_capacity.get("combined_persistent_volume"),
        "target service configured persistent volume",
    )
    service_event = _mapping(
        service_capacity.get("event"),
        "target service event capacity diagnostics",
    )
    raw_capacity_profile = _mapping(
        document.get("raw_capacity_profile"),
        "target raw capacity profile",
    )
    retention = _mapping(
        document.get("retention_guard"),
        "target retention guard capacity binding",
    )
    derivation = _mapping(document.get("derivation"), "target derivation")
    same_filesystem = _mapping(
        document.get("same_filesystem_capacity"),
        "target same-filesystem capacity",
    )
    parameters = _mapping(
        target_manifest.get("capacity_parameters"),
        "target capacity parameters",
    )
    declared = _positive_integer(
        same_filesystem.get("operator_declared_target_bytes"),
        "target declared capacity",
        maximum=MAX_SIGNED_INT64 - 1,
    )
    required = _positive_integer(
        same_filesystem.get("required_same_filesystem_bytes"),
        "target required capacity",
    )
    if (
        document.get("contract") != typed.capacity_contract
        or document.get("automatic_reclaim") is not False
        or document.get("capacity_exhaustion") != "fail_closed"
        or document.get("fail_closed_on_full") is not True
        or derivation.get("capture_window_hours")
        != typed.capture_window_hours
        or activation.get("activation_matrix_sha256")
        != typed.activation_matrix_sha256
        or activation.get("catalog_sha256") != typed.catalog_sha256
        or activation.get("recovery_binding_count")
        != typed.recovery_binding_count
        or activation.get("recovery_inventory_sha256")
        != typed.recovery_inventory_sha256
        or activation.get("raw_capture_composition_sha256")
        != typed.raw_capture_composition_sha256
        or activation.get("worker_service_sha256")
        != typed.worker_service_sha256
        or _sha_bytes(_canonical(ingress_limits))
        != typed.blitzortung_ingress_limits_sha256
        or _sha_bytes(_canonical(raw_store_limits))
        != typed.blitzortung_raw_store_limits_sha256
        or event.get("operational_admission") is not False
        or event.get("capacity_claimed") is not False
        or event.get("enabled") is not False
        or event.get("included_in_same_filesystem_requirement") is not False
        or event.get("limits_status") != "configured_dormant_unclaimed"
        or service_capacity.get("accounting_contract")
        != "config_bound_optional_event_capacity_v2"
        or service_capacity.get("host_operational_free_reserve_bytes")
        != typed.host_operational_free_reserve_bytes
        or service_combined.get("configured_max_bytes")
        != typed.service_configured_persistent_max_bytes
        or service_event.get("capacity_claimed") is not False
        or service_event.get("enabled") is not False
        or service_event.get("object_max_bytes") != 0
        or service_event.get("object_max_entries") != 0
        or dict(retention) != {
            "blitzortung_enabled": typed.blitzortung_enabled,
            "capacity_policy": typed.retention_capacity_policy,
            "contract": typed.retention_guard_contract,
            "event_capacity_claimed": typed.event_capacity_claimed,
            "guard_configured_persistent_max_bytes": (
                typed.guard_configured_persistent_max_bytes
            ),
            "host_operational_free_reserve_bytes": (
                typed.host_operational_free_reserve_bytes
            ),
            "selected_profile": typed.selected_profile,
            "service_configured_persistent_max_bytes": (
                typed.service_configured_persistent_max_bytes
            ),
            "source_sha256": typed.retention_guard_sha256,
        }
        or _positive_integer(
            raw_capacity_profile.get("max_catalog_bytes"),
            "target capacity-ledger catalog maximum",
        ) + typed.service_configured_persistent_max_bytes
        != typed.guard_configured_persistent_max_bytes
        or same_filesystem.get("live_disk_free_space_verified") is not False
        or same_filesystem.get("blitzortung_enabled") is not False
        or same_filesystem.get("configured_event_raw_quota") != 0
        or same_filesystem.get(
            "operator_host_operational_free_reserve_bytes"
        ) != typed.host_operational_free_reserve_bytes
        or declared != parameters.get("operator_declared_target_bytes")
        or parameters.get("blitzortung_enabled") is not False
        or parameters.get("capture_window_hours") != typed.capture_window_hours
        or parameters.get("operator_host_operational_free_reserve_bytes")
        != typed.host_operational_free_reserve_bytes
        or required != typed.required_same_filesystem_bytes
        or declared < required
    ):
        raise RawCaptureReleaseRequirementsError(
            "target capacity document differs from the frozen policy pins"
        )
    return document, declared, required


def _path_identity(path: Path) -> str:
    return os.path.normcase(str(path))


def _canonical_existing_directory(path: Path, *, label: str) -> Path:
    supplied = Path(path)
    if not supplied.is_absolute() or supplied == Path(supplied.anchor):
        raise RawCaptureReleaseRequirementsError(
            f"{label} is not an absolute non-root directory"
        )
    try:
        resolved = supplied.resolve(strict=True)
        for component in (supplied, *supplied.parents):
            metadata = component.lstat()
            if (
                stat.S_ISLNK(metadata.st_mode)
                or _is_reparse(metadata)
                or not stat.S_ISDIR(metadata.st_mode)
            ):
                raise RawCaptureReleaseRequirementsError(
                    f"{label} has a link, reparse point or non-directory ancestor"
                )
    except RawCaptureReleaseRequirementsError:
        raise
    except (OSError, RuntimeError) as error:
        raise RawCaptureReleaseRequirementsError(
            f"{label} is unavailable"
        ) from error
    if _path_identity(supplied) != _path_identity(resolved):
        raise RawCaptureReleaseRequirementsError(f"{label} is not canonical")
    return resolved


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _nearest_existing_target_node(candidate: Path, output: Path) -> Path:
    try:
        relative = candidate.relative_to(output)
    except ValueError as error:
        raise RawCaptureReleaseRequirementsError(
            "target runtime path escapes the materialized output"
        ) from error
    current = output
    nearest = output
    for index, component in enumerate(relative.parts):
        current = current / component
        if not os.path.lexists(current):
            break
        try:
            metadata = current.lstat()
            resolved = current.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise RawCaptureReleaseRequirementsError(
                "target runtime path cannot be reproved"
            ) from error
        if (
            stat.S_ISLNK(metadata.st_mode)
            or _is_reparse(metadata)
            or _path_identity(current) != _path_identity(resolved)
            or (
                index < len(relative.parts) - 1
                and not stat.S_ISDIR(metadata.st_mode)
            )
        ):
            raise RawCaptureReleaseRequirementsError(
                "target runtime path has a link, reparse point or invalid parent"
            )
        nearest = current
    return nearest


def _observe_filesystem_device(path: Path) -> int:
    try:
        value = int(path.stat(follow_symlinks=False).st_dev)
    except (OSError, TypeError, ValueError) as error:
        raise RawCaptureReleaseRequirementsError(
            "target filesystem device cannot be observed"
        ) from error
    if value < 0:
        raise RawCaptureReleaseRequirementsError(
            "target filesystem device is invalid"
        )
    return value


def _observe_available_bytes(path: Path) -> int:
    try:
        value = int(shutil.disk_usage(path).free)
    except (OSError, TypeError, ValueError) as error:
        raise RawCaptureReleaseRequirementsError(
            "target filesystem capacity cannot be observed"
        ) from error
    return _positive_integer(value, "target filesystem available bytes")


def _target_live_filesystem_proof(
    target_manifest: Mapping[str, object],
    *,
    manifest_path: Path,
    evidence_output_directory: Path,
) -> dict[str, object]:
    expected_flavour = "windows" if os.name == "nt" else "posix"
    if target_manifest.get("target_path_flavour") != expected_flavour:
        raise RawCaptureReleaseRequirementsError(
            "target manifest path semantics differ from the verifier host"
        )
    target_output = _canonical_existing_directory(
        Path(str(target_manifest.get("output_directory"))),
        label="target materialization output",
    )
    persistent_base = _canonical_existing_directory(
        Path(str(target_manifest.get("persistent_base_root"))),
        label="target persistent base",
    )
    if target_output.parent != persistent_base:
        raise RawCaptureReleaseRequirementsError(
            "target output is not a direct persistent-base child"
        )
    supplied_manifest = Path(manifest_path)
    expected_manifest = (
        target_output
        / "artifacts"
        / "raw-capture-shadow-sha256-manifest.json"
    )
    if _path_identity(supplied_manifest) != _path_identity(expected_manifest):
        raise RawCaptureReleaseRequirementsError(
            "target manifest path differs from its materialized output"
        )
    evidence_output = _canonical_existing_directory(
        Path(evidence_output_directory),
        label="target attestation evidence output",
    )
    if (
        _paths_overlap(evidence_output, persistent_base)
        or _paths_overlap(evidence_output, target_output)
    ):
        raise RawCaptureReleaseRequirementsError(
            "target attestation evidence output overlaps runtime deletion roots"
        )
    candidates = [
        *_target_artifact_paths(supplied_manifest, target_manifest).values(),
    ]
    mutable = target_manifest.get("mutable_runtime_paths")
    assert isinstance(mutable, list)
    for item in mutable:
        row = _mapping(item, "target mutable runtime path")
        relative = PurePosixPath(str(row.get("path")))
        candidates.append(target_output.joinpath(*relative.parts))
    nodes = [persistent_base, target_output]
    nodes.extend(
        _nearest_existing_target_node(candidate, target_output)
        for candidate in candidates
    )
    devices = {_observe_filesystem_device(node) for node in nodes}
    if len(devices) != 1:
        raise RawCaptureReleaseRequirementsError(
            "target RAW paths do not share one filesystem device"
        )
    return {
        "canonical_absolute_paths_verified": False,
        "contract": TARGET_FILESYSTEM_PROOF_CONTRACT,
        "diagnostic_filesystem_device": next(iter(devices)),
        "diagnostic_observation_only": True,
        "diagnostic_observation_sufficient_for_readiness": False,
        "diagnostic_target_path_count": len(nodes),
        "evidence_output_directory": str(evidence_output),
        "evidence_output_external_to_runtime": False,
        "identity_bound": False,
        "mutable_runtime_state_packaged": False,
        "persistent_base_root": str(persistent_base),
        "reparse_or_symlink_absent": False,
        "same_filesystem_layout_verified": False,
        "target_artifacts_reverified": False,
        "target_output_directory": str(target_output),
        "target_path_flavour": expected_flavour,
    }


def _validated_target_filesystem_proof(value: object) -> dict[str, object]:
    proof = _mapping(value, "target filesystem proof")
    expected_keys = {
        "canonical_absolute_paths_verified",
        "contract",
        "diagnostic_filesystem_device",
        "diagnostic_observation_only",
        "diagnostic_observation_sufficient_for_readiness",
        "diagnostic_target_path_count",
        "evidence_output_directory",
        "evidence_output_external_to_runtime",
        "identity_bound",
        "mutable_runtime_state_packaged",
        "persistent_base_root",
        "reparse_or_symlink_absent",
        "same_filesystem_layout_verified",
        "target_artifacts_reverified",
        "target_output_directory",
        "target_path_flavour",
    }
    if (
        set(proof) != expected_keys
        or proof.get("contract") != TARGET_FILESYSTEM_PROOF_CONTRACT
        or proof.get("canonical_absolute_paths_verified") is not False
        or proof.get("diagnostic_observation_only") is not True
        or proof.get("diagnostic_observation_sufficient_for_readiness") is not False
        or proof.get("evidence_output_external_to_runtime") is not False
        or proof.get("identity_bound") is not False
        or proof.get("mutable_runtime_state_packaged") is not False
        or proof.get("reparse_or_symlink_absent") is not False
        or proof.get("same_filesystem_layout_verified") is not False
        or proof.get("target_artifacts_reverified") is not False
        or proof.get("target_path_flavour") not in {"posix", "windows"}
        or type(proof.get("diagnostic_filesystem_device")) is not int
        or int(proof.get("diagnostic_filesystem_device", -1)) < 0
    ):
        raise RawCaptureReleaseRequirementsError(
            "target filesystem proof has an invalid schema"
        )
    count = _positive_integer(
        proof.get("diagnostic_target_path_count"),
        "diagnostic target path count",
        maximum=10_000,
    )
    if count != 19:
        raise RawCaptureReleaseRequirementsError(
            "target filesystem diagnostic path count is incomplete"
        )
    flavour = str(proof["target_path_flavour"])
    path_type = PureWindowsPath if flavour == "windows" else PurePosixPath
    try:
        evidence = path_type(str(proof["evidence_output_directory"]))
        base = path_type(str(proof["persistent_base_root"]))
        output = path_type(str(proof["target_output_directory"]))
    except (TypeError, ValueError) as error:
        raise RawCaptureReleaseRequirementsError(
            "target filesystem proof paths are invalid"
        ) from error
    if (
        not evidence.is_absolute()
        or not base.is_absolute()
        or not output.is_absolute()
        or str(evidence) != proof.get("evidence_output_directory")
        or str(base) != proof.get("persistent_base_root")
        or str(output) != proof.get("target_output_directory")
        or output.parent != base
        or evidence == base
        or evidence == output
        or evidence in base.parents
        or base in evidence.parents
        or evidence in output.parents
        or output in evidence.parents
    ):
        raise RawCaptureReleaseRequirementsError(
            "target filesystem proof paths overlap or are not canonical"
        )
    return dict(proof)


def _attestation_document(
    *,
    requirements: Mapping[str, object],
    requirements_descriptor: Mapping[str, object],
    target_manifest: Mapping[str, object],
    target_manifest_descriptor: Mapping[str, object],
    artifact_descriptors: Mapping[str, Mapping[str, object]],
    filesystem_proof: Mapping[str, object],
) -> dict[str, object]:
    manifest_artifacts = _validate_target_manifest_bindings(
        target_manifest,
        requirements,
    )
    capacity = _mapping(
        _mapping(requirements.get("pinset"), "requirements pinset").get(
            "capacity"
        ),
        "requirements capacity pin",
    )
    parameters = _mapping(
        target_manifest.get("capacity_parameters"),
        "target capacity parameters",
    )
    declared = _positive_integer(
        parameters.get("operator_declared_target_bytes"),
        "target declared capacity",
        maximum=MAX_SIGNED_INT64 - 1,
    )
    required = _positive_integer(
        capacity.get("minimum_required_same_filesystem_bytes"),
        "minimum target capacity",
    )
    if declared < required:
        raise RawCaptureReleaseRequirementsError(
            "target declared RAW capacity is below the release requirement"
        )
    artifacts: dict[str, object] = {}
    for role in TARGET_ARTIFACT_ROLES:
        observed = _mapping(artifact_descriptors.get(role), role)
        manifest = manifest_artifacts[role]
        if (
            observed.get("bytes") != manifest.get("bytes")
            or observed.get("sha256") != manifest.get("sha256")
        ):
            raise RawCaptureReleaseRequirementsError(
                "target artifact bytes differ from their activation manifest"
            )
        artifacts[role] = {
            "bytes": observed.get("bytes"),
            "sha256": observed.get("sha256"),
        }
    checked_filesystem_proof = _validated_target_filesystem_proof(
        filesystem_proof
    )
    unsigned: dict[str, object] = {
        "artifacts": artifacts,
        "contract": TARGET_ACTIVATION_ATTESTATION_CONTRACT,
        "capacity_proof": {
            "contract": "stv3-raw-capture-target-filesystem-capacity-proof-v1",
            "declared_target_bytes": declared,
            "diagnostic_observation_only": True,
            "diagnostic_observation_sufficient_for_readiness": False,
            "requirement_satisfied": False,
            "required_same_filesystem_bytes": required,
            "trusted_available_bytes_observed": False,
        },
        "dashboard_output": False,
        "deployment_bundle_complete": False,
        "deployment_performed": False,
        "filesystem_proof": checked_filesystem_proof,
        "meteorological_admission": False,
        "meteorological_output": False,
        "production_enable_ready": False,
        "requirements": {
            "bytes": requirements_descriptor.get("bytes"),
            "requirements_sha256": requirements.get("requirements_sha256"),
            "sha256": requirements_descriptor.get("sha256"),
        },
        "target_activation_manifest": {
            "bytes": target_manifest_descriptor.get("bytes"),
            "sha256": target_manifest_descriptor.get("sha256"),
        },
        "target_live_verification_required": True,
        "target_materialization_performed": False,
        "trusted_target_attestation_unavailable": True,
    }
    _reject_secret_fields(unsigned)
    return {
        **unsigned,
        "attestation_sha256": _sha_bytes(_canonical(unsigned)),
    }


def _validate_attestation_document(
    document: Mapping[str, object],
    *,
    requirements: Mapping[str, object],
    requirements_descriptor: Mapping[str, object],
    target_manifest: Mapping[str, object],
    target_manifest_descriptor: Mapping[str, object],
    artifact_descriptors: Mapping[str, Mapping[str, object]],
    observed_filesystem_proof: Mapping[str, object] | None = None,
) -> dict[str, object]:
    expected = _attestation_document(
        requirements=requirements,
        requirements_descriptor=requirements_descriptor,
        target_manifest=target_manifest,
        target_manifest_descriptor=target_manifest_descriptor,
        artifact_descriptors=artifact_descriptors,
        filesystem_proof=(
            observed_filesystem_proof
            if observed_filesystem_proof is not None
            else _mapping(
                document.get("filesystem_proof"),
                "target filesystem proof",
            )
        ),
    )
    if dict(document) != expected:
        raise RawCaptureReleaseRequirementsError(
            "target activation attestation differs from bound target bytes"
        )
    return dict(document)


def _target_artifact_paths(
    target_manifest_path: Path,
    target_manifest: Mapping[str, object],
) -> dict[str, Path]:
    supplied = Path(target_manifest_path)
    output = supplied.parent.parent.resolve(strict=True)
    rows = _target_manifest_artifacts(target_manifest)
    result: dict[str, Path] = {}
    for role, row in rows.items():
        relative = row.get("path")
        if (
            not isinstance(relative, str)
            or not relative
            or "\\" in relative
            or relative.startswith("/")
            or ".." in Path(relative).parts
        ):
            raise RawCaptureReleaseRequirementsError(
                "target artifact relative path is invalid"
            )
        candidate = output.joinpath(*relative.split("/"))
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(output)
        except (OSError, RuntimeError, ValueError) as error:
            raise RawCaptureReleaseRequirementsError(
                "target artifact escapes its materialized root"
            ) from error
        if candidate.is_symlink() or resolved != candidate:
            raise RawCaptureReleaseRequirementsError(
                "target artifact path is not canonical"
            )
        result[role] = resolved
    return result


def _verified_live_target_snapshot(
    *,
    requirements: Mapping[str, object],
    target_activation_manifest: Path,
    evidence_output_directory: Path,
) -> dict[str, object]:
    from .raw_capture_activation_artifacts import (
        verify_raw_capture_shadow_artifacts,
    )

    manifest_path = Path(target_activation_manifest)
    try:
        first_native = verify_raw_capture_shadow_artifacts(manifest_path)
    except Exception as error:
        raise RawCaptureReleaseRequirementsError(
            "target activation materialization failed its native verifier"
        ) from error
    manifest_body = _read_file(
        manifest_path,
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="target activation manifest",
    )
    target_manifest = _strict_json(
        manifest_body,
        label="target activation manifest",
    )
    if (
        _canonical(target_manifest) != manifest_body
        or first_native.get("manifest_sha256") != _sha_bytes(manifest_body)
    ):
        raise RawCaptureReleaseRequirementsError(
            "target activation manifest changed after native verification"
        )
    paths = _target_artifact_paths(manifest_path, target_manifest)
    artifact_descriptors: dict[str, Mapping[str, object]] = {}
    artifact_bodies: dict[str, bytes] = {}
    for role, path in paths.items():
        body = _read_file(
            path,
            maximum_bytes=MAX_TARGET_ARTIFACT_BYTES,
            label=f"target artifact {role}",
        )
        artifact_bodies[role] = body
        artifact_descriptors[role] = {
            "bytes": len(body),
            "sha256": _sha_bytes(body),
        }
    _validate_target_config_document(
        artifact_bodies["raw_capture_config"],
        requirements=requirements,
        target_manifest=target_manifest,
    )
    _capacity, declared, required = _validate_target_capacity_document(
        artifact_bodies["capacity_profile"],
        requirements=requirements,
        target_manifest=target_manifest,
    )
    try:
        second_native = verify_raw_capture_shadow_artifacts(manifest_path)
    except Exception as error:
        raise RawCaptureReleaseRequirementsError(
            "target activation materialization failed native reverification"
        ) from error
    manifest_sha = _sha_bytes(manifest_body)
    if second_native.get("manifest_sha256") != manifest_sha:
        raise RawCaptureReleaseRequirementsError(
            "target activation manifest changed during native reverification"
        )
    if _read_file(
        manifest_path,
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="target activation manifest reproof",
    ) != manifest_body:
        raise RawCaptureReleaseRequirementsError(
            "target activation manifest changed after native reverification"
        )
    for role, path in paths.items():
        if _read_file(
            path,
            maximum_bytes=MAX_TARGET_ARTIFACT_BYTES,
            label=f"target artifact reproof {role}",
        ) != artifact_bodies[role]:
            raise RawCaptureReleaseRequirementsError(
                "target artifact changed after native reverification"
            )
    filesystem_proof = _target_live_filesystem_proof(
        target_manifest,
        manifest_path=manifest_path,
        evidence_output_directory=evidence_output_directory,
    )
    observed_available = _observe_available_bytes(manifest_path.parent.parent)
    if observed_available < declared:
        raise RawCaptureReleaseRequirementsError(
            "target filesystem free space is below declared RAW capacity"
        )
    return {
        "artifact_bodies": artifact_bodies,
        "artifact_descriptors": artifact_descriptors,
        "declared_capacity": declared,
        "filesystem_proof": filesystem_proof,
        "manifest": target_manifest,
        "manifest_body": manifest_body,
        "manifest_sha256": manifest_sha,
        "observed_available_bytes": observed_available,
        "required_capacity": required,
    }


def _target_attestation_filename(
    requirements: Mapping[str, object],
    *,
    requirements_file_sha256: str,
    manifest_sha256: str,
) -> str:
    return (
        f"stv3_raw_activation_i{requirements['integration_version']}"
        f"_w{requirements['worker_version']}_r{requirements_file_sha256}"
        f"_m{manifest_sha256}.json"
    )


def build_raw_capture_target_activation_attestation(
    *,
    requirements_path: Path,
    sanitized_options_snapshot: Path,
    target_activation_manifest: Path,
    output_directory: Path,
) -> BuiltTargetActivationAttestation:
    """Build one fail-closed target diagnostic from local observations."""

    requirements = verify_raw_capture_release_requirements(
        requirements_path,
        sanitized_options_snapshot=sanitized_options_snapshot,
    )
    resolved_output = _canonical_existing_directory(
        Path(output_directory),
        label="target attestation evidence output",
    )
    snapshot = _verified_live_target_snapshot(
        requirements=requirements,
        target_activation_manifest=target_activation_manifest,
        evidence_output_directory=resolved_output,
    )
    requirements_body = _read_file(
        Path(requirements_path),
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="RAW release requirements",
    )
    # Reprove every existing target artifact/runtime node, ancestor and device
    # at the commit boundary.  The native verifier checks document bindings;
    # this second trusted observation closes the later path/capacity window.
    final_filesystem_proof = _target_live_filesystem_proof(
        _mapping(snapshot["manifest"], "target manifest"),
        manifest_path=Path(target_activation_manifest),
        evidence_output_directory=resolved_output,
    )
    if final_filesystem_proof != snapshot["filesystem_proof"]:
        raise RawCaptureReleaseRequirementsError(
            "target filesystem layout changed before attestation commit"
        )
    final_available = _observe_available_bytes(
        Path(target_activation_manifest).parent.parent
    )
    if final_available < int(snapshot["declared_capacity"]):
        raise RawCaptureReleaseRequirementsError(
            "target filesystem capacity changed before attestation commit"
        )
    document = _attestation_document(
        requirements=requirements,
        requirements_descriptor={
            "bytes": len(requirements_body),
            "sha256": _sha_bytes(requirements_body),
        },
        target_manifest=_mapping(snapshot["manifest"], "target manifest"),
        target_manifest_descriptor={
            "bytes": len(snapshot["manifest_body"]),
            "sha256": snapshot["manifest_sha256"],
        },
        artifact_descriptors=_mapping(
            snapshot["artifact_descriptors"],
            "target artifact descriptors",
        ),
        filesystem_proof=_mapping(
            snapshot["filesystem_proof"],
            "target filesystem proof",
        ),
    )
    verified_precommit = _validate_attestation_document(
        document,
        requirements=requirements,
        requirements_descriptor={
            "bytes": len(requirements_body),
            "sha256": _sha_bytes(requirements_body),
        },
        target_manifest=_mapping(snapshot["manifest"], "target manifest"),
        target_manifest_descriptor={
            "bytes": len(snapshot["manifest_body"]),
            "sha256": snapshot["manifest_sha256"],
        },
        artifact_descriptors=_mapping(
            snapshot["artifact_descriptors"],
            "target artifact descriptors",
        ),
        observed_filesystem_proof=_mapping(
            snapshot["filesystem_proof"],
            "target filesystem proof",
        ),
    )
    if verified_precommit != document:
        raise RawCaptureReleaseRequirementsError(
            "target attestation failed live precommit verification"
        )
    requirements_file_sha = _sha_bytes(requirements_body)
    destination = resolved_output / _target_attestation_filename(
        requirements,
        requirements_file_sha256=requirements_file_sha,
        manifest_sha256=str(snapshot["manifest_sha256"]),
    )
    _publish_canonical_document(destination, document)
    attestation_body = _canonical(document) + b"\n"
    return BuiltTargetActivationAttestation(
        attestation_path=destination,
        attestation_sha256=str(document["attestation_sha256"]),
        attestation_file_sha256=_sha_bytes(attestation_body),
        target_activation_manifest_sha256=str(snapshot["manifest_sha256"]),
    )


def verify_raw_capture_target_activation_attestation(
    attestation_path: Path,
    *,
    requirements_path: Path,
    sanitized_options_snapshot: Path,
    target_activation_manifest: Path,
) -> dict[str, object]:
    """Validate a target diagnostic without granting deployment readiness."""

    requirements = verify_raw_capture_release_requirements(
        requirements_path,
        sanitized_options_snapshot=sanitized_options_snapshot,
    )
    attestation_file = Path(attestation_path)
    evidence_output = _canonical_existing_directory(
        attestation_file.parent,
        label="target attestation evidence output",
    )
    snapshot = _verified_live_target_snapshot(
        requirements=requirements,
        target_activation_manifest=target_activation_manifest,
        evidence_output_directory=evidence_output,
    )
    requirements_body = _read_file(
        Path(requirements_path),
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="RAW release requirements",
    )
    attestation_body = _read_file(
        attestation_file,
        maximum_bytes=MAX_DOCUMENT_BYTES,
        label="target activation attestation",
    )
    attestation = _strict_canonical_document(
        attestation_body,
        label="target activation attestation",
    )
    expected_name = _target_attestation_filename(
        requirements,
        requirements_file_sha256=_sha_bytes(requirements_body),
        manifest_sha256=str(snapshot["manifest_sha256"]),
    )
    _validate_filename(attestation_file.name, expected_name)
    verified = _validate_attestation_document(
        attestation,
        requirements=requirements,
        requirements_descriptor={
            "bytes": len(requirements_body),
            "sha256": _sha_bytes(requirements_body),
        },
        target_manifest=_mapping(snapshot["manifest"], "target manifest"),
        target_manifest_descriptor={
            "bytes": len(snapshot["manifest_body"]),
            "sha256": snapshot["manifest_sha256"],
        },
        artifact_descriptors=_mapping(
            snapshot["artifact_descriptors"],
            "target artifact descriptors",
        ),
        observed_filesystem_proof=_mapping(
            snapshot["filesystem_proof"],
            "target filesystem proof",
        ),
    )
    capacity_proof = _mapping(
        verified.get("capacity_proof"),
        "target capacity proof",
    )
    if int(snapshot["observed_available_bytes"]) < int(
        capacity_proof.get("declared_target_bytes", MAX_SIGNED_INT64)
    ):
        raise RawCaptureReleaseRequirementsError(
            "current target capacity no longer covers the attestation"
        )
    return verified


def verify_raw_capture_target_activation_composite_bound_inputs(
    source: BoundInputSource,
) -> dict[str, object]:
    """Bind offline bytes without adopting forgeable live-filesystem claims."""

    try:
        source.require_exact_keys(TARGET_ATTESTATION_BOUND_INPUT_KEYS)
    except Exception as error:
        raise RawCaptureReleaseRequirementsError(
            "target attestation inputs have an unexpected key set"
        ) from error
    bodies, descriptors = _seal_bound_inputs(source, {
        "attestation": MAX_DOCUMENT_BYTES,
        "evidence": MAX_DOCUMENT_BYTES * 16,
        "integration": MAX_SIGNED_INT64,
        "options": MAX_DOCUMENT_BYTES,
        "requirements": MAX_DOCUMENT_BYTES,
        "target_authority": MAX_TARGET_ARTIFACT_BYTES,
        "target_capacity": MAX_TARGET_ARTIFACT_BYTES,
        "target_config": MAX_TARGET_ARTIFACT_BYTES,
        "target_lock": MAX_TARGET_ARTIFACT_BYTES,
        "target_manifest": MAX_DOCUMENT_BYTES,
        "worker": MAX_SIGNED_INT64,
    })
    sealed_source = _MemoryBoundInputSource(bodies)
    requirements = verify_raw_capture_release_requirements_bound_inputs(
        _BoundSubview(sealed_source, REQUIREMENTS_BOUND_INPUT_KEYS)
    )
    attestation_body = bodies["attestation"]
    attestation = _strict_canonical_document(
        attestation_body,
        label="target activation attestation",
    )
    target_manifest_body = bodies["target_manifest"]
    target_manifest = _strict_json(
        target_manifest_body,
        label="target activation manifest",
    )
    if _canonical(target_manifest) != target_manifest_body:
        raise RawCaptureReleaseRequirementsError(
            "target activation manifest is not canonical"
        )
    artifacts: dict[str, Mapping[str, object]] = {}
    artifact_bodies: dict[str, bytes] = {}
    for role, key in TARGET_ARTIFACT_INPUT_KEYS.items():
        artifact_bodies[role] = bodies[key]
        artifacts[role] = descriptors[key]
    _validate_target_config_document(
        artifact_bodies["raw_capture_config"],
        requirements=requirements,
        target_manifest=target_manifest,
    )
    _validate_target_capacity_document(
        artifact_bodies["capacity_profile"],
        requirements=requirements,
        target_manifest=target_manifest,
    )
    verified_attestation = _validate_attestation_document(
        attestation,
        requirements=requirements,
        requirements_descriptor=descriptors["requirements"],
        target_manifest=target_manifest,
        target_manifest_descriptor=descriptors["target_manifest"],
        artifact_descriptors=artifacts,
    )
    return {
        "attestation": verified_attestation,
        "deployment_bundle_complete": False,
        "deployment_performed": False,
        "requirements": requirements,
        "target_attestation_bytes_bound": True,
        "target_activation_manifest_sha256": descriptors["target_manifest"][
            "sha256"
        ],
        "target_live_verification_required": True,
        "trusted_target_attestation_unavailable": True,
    }


__all__ = [
    "BuiltRawCaptureReleaseRequirements",
    "BuiltRawCaptureReleaseQuartet",
    "BuiltTargetActivationAttestation",
    "FROZEN_ACTIVATION_MATRIX_SHA256",
    "FROZEN_CAPTURE_WINDOW_HOURS",
    "FROZEN_CATALOG_SHA256",
    "FROZEN_DERIVATION_FIXTURE_SHA256",
    "FROZEN_EVENT_CAPACITY_CLAIMED",
    "FROZEN_GUARD_CONFIGURED_PERSISTENT_MAX_BYTES",
    "FROZEN_HOST_OPERATIONAL_FREE_RESERVE_BYTES",
    "FROZEN_MATERIALIZER_SHA256",
    "FROZEN_RETENTION_GUARD_SHA256",
    "FROZEN_SELECTED_PROFILE",
    "FROZEN_SERVICE_CONFIGURED_PERSISTENT_MAX_BYTES",
    "FROZEN_WORKER_PACKAGE_MEMBER_PATHS",
    "FROZEN_REQUIRED_SAME_FILESYSTEM_BYTES",
    "MATERIALIZER_MEMBER_PATH",
    "RAW_CAPTURE_RUNTIME_MODE",
    "RAW_RELEASE_REQUIREMENTS_CONTRACT",
    "REQUIREMENTS_BOUND_INPUT_KEYS",
    "RawCapturePortablePinset",
    "RawCaptureReleaseRequirementsError",
    "SANITIZED_OPTIONS_BYTES",
    "SANITIZED_OPTIONS_CONTRACT",
    "SANITIZED_OPTIONS_FILENAME",
    "SANITIZED_OPTIONS_SHA256",
    "TARGET_ACTIVATION_ATTESTATION_CONTRACT",
    "TARGET_ATTESTATION_BOUND_INPUT_KEYS",
    "TARGET_PROFILE_CONTRACT",
    "build_raw_capture_release_requirements",
    "build_raw_capture_release_quartet",
    "build_raw_capture_target_activation_attestation",
    "derive_frozen_raw_capture_release_pinset",
    "validate_frozen_sanitized_options_snapshot",
    "verify_raw_capture_release_requirements",
    "verify_raw_capture_release_requirements_bound_inputs",
    "verify_raw_capture_target_activation_attestation",
    "verify_raw_capture_target_activation_composite_bound_inputs",
]
