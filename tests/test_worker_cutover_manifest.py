from __future__ import annotations

import ast
import hashlib
import inspect
import json
from pathlib import Path
import re

from storm_tracker_worker import (
    provider_capture_registry,
    service,
    worker_acquisition_plan,
)
from storm_tracker_worker.worker_cutover_manifest import (
    ACQUISITION_WORKLOAD_OWNERS,
    EVENT_INGRESS_OWNERS,
    CutoverState,
    WORKLOADS,
    manifest_mapping,
    manifest_sha256,
    unresolved_workloads,
)


ROOT = Path(__file__).resolve().parents[1]


def test_manifest_is_closed_deterministic_and_content_addressed():
    mapping = manifest_mapping()
    encoded = json.dumps(
        mapping, allow_nan=False, ensure_ascii=False,
        separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")
    assert manifest_sha256() == hashlib.sha256(encoded).hexdigest()
    assert [item["workload_id"] for item in mapping["workloads"]] == sorted(WORKLOADS)
    assert manifest_sha256() == manifest_sha256()


def test_only_one_explicit_bounded_control_plane_may_remain_in_ha():
    allowed = [
        item for item in WORKLOADS.values() if item.ha_allowed_after_cutover
    ]
    assert [item.workload_id for item in allowed] == [
        "ha.desired_state_and_snapshot_projection"
    ]
    assert allowed[0].state is CutoverState.HA_CONTROL_PLANE
    assert all(
        not item.provider_ids
        and not item.acquisition_workload_ids
        and not item.event_ingress_ids
        and not item.legacy_provider_modules
        and not item.worker_job_types
        for item in allowed
    )


def test_every_capture_registry_provider_is_visible_in_cutover_manifest():
    registered = {
        policy.provider_id
        for policy in provider_capture_registry.PROVIDER_CAPTURE_POLICIES.values()
    }
    manifested = {
        provider
        for item in WORKLOADS.values()
        for provider in item.provider_ids
    }
    assert registered <= manifested


def test_every_acquisition_workload_and_provider_is_owned_exactly_once():
    catalog = worker_acquisition_plan.acquisition_catalog_snapshot()["workloads"]
    expected_workloads = {item["workload_id"] for item in catalog}
    expected_providers = {item["provider_id"] for item in catalog}

    owners: dict[str, list[str]] = {}
    for contract in WORKLOADS.values():
        for workload_id in contract.acquisition_workload_ids:
            owners.setdefault(workload_id, []).append(contract.workload_id)

    assert set(owners) == expected_workloads
    assert all(len(owner_ids) == 1 for owner_ids in owners.values())
    assert dict(ACQUISITION_WORKLOAD_OWNERS) == {
        workload_id: owner_ids[0]
        for workload_id, owner_ids in owners.items()
    }
    manifested_providers = {
        provider
        for contract in WORKLOADS.values()
        for provider in contract.provider_ids
    }
    assert expected_providers <= manifested_providers


def test_every_event_ingress_is_owned_exactly_once_outside_cadence_catalogue():
    catalog = worker_acquisition_plan.acquisition_catalog_snapshot()
    ingress_ids = {item["ingress_id"] for item in catalog["event_ingresses"]}
    cadence_ids = {item["workload_id"] for item in catalog["workloads"]}
    owners: dict[str, list[str]] = {}
    for contract in WORKLOADS.values():
        for ingress_id in contract.event_ingress_ids:
            owners.setdefault(ingress_id, []).append(contract.workload_id)
    assert ingress_ids == {"lightning.blitzortung.event_ingress"}
    assert not ingress_ids.intersection(cadence_ids)
    assert owners == {
        "lightning.blitzortung.event_ingress": ["provider.blitzortung_push"]
    }
    assert dict(EVENT_INGRESS_OWNERS) == {
        "lightning.blitzortung.event_ingress": "provider.blitzortung_push"
    }
    ingress = catalog["event_ingresses"][0]
    assert ingress["absence_semantics"] == "no_absence_claim"
    assert ingress["cadence_seconds"] is None


def test_every_legacy_provider_module_is_classified_exactly_once():
    internal_modules = {
        "__init__",
        "base",
        "engine_radar_policy",
        "lifecycle",
        "odim_hdf5",
        "radar_fusion",
        "radar_policy",
        "raster_components",
        "registry",
    }
    provider_directory = ROOT / "custom_components/storm_tracker_v3/providers"
    expected_modules = {
        path.stem for path in provider_directory.glob("*.py")
    } - internal_modules
    owners: dict[str, list[str]] = {}
    for contract in WORKLOADS.values():
        for module_name in contract.legacy_provider_modules:
            owners.setdefault(module_name, []).append(contract.workload_id)

    assert set(owners) == expected_modules
    assert all(len(owner_ids) == 1 for owner_ids in owners.values())


def test_validation_only_ha_fetches_have_raw_worker_ownership_without_admission():
    validation = WORKLOADS["provider.validation_only_nowcasts"]
    assert validation.state is CutoverState.SHADOW_CANDIDATE
    assert validation.worker_job_types == ()
    assert validation.provider_ids == (
        "geosphere", "geosphere_at", "italiameteo", "meteolux",
    )
    assert validation.acquisition_workload_ids == (
        "validation.geosphere.location",
        "validation.italiameteo.dpc_sri_historical_catalog",
        "validation.meteolux.location",
    )
    assert {
        "acquisition_capture_recovery",
        "acquisition_handler_child",
        "acquisition_location_shadow",
        "public_source_raw_capture",
    } == set(validation.shadow_components)
    assert "none may claim realtime radar" in validation.cutover_gate
    source = (ROOT / "custom_components/storm_tracker_v3/__init__.py").read_text(
        encoding="utf-8"
    )
    for provider in (
        "GeoSphereAustriaProvider", "ItaliaMeteoRadarProvider", "MeteoLuxProvider"
    ):
        assert re.search(
            rf"provider_lifecycle\.register\(\s*{provider}", source
        )


def test_kmi_three_product_split_and_italiameteo_split_have_explicit_owners():
    benelux = WORKLOADS["provider.benelux_comparison_radar"]
    assert {
        "radar.kmi.appv4_visual",
        "radar.kmi.public_wcs_unknown_quantity",
        "radar.kmi.radqpe_native",
    } <= set(benelux.acquisition_workload_ids)
    assert "kmi_fetch" in benelux.worker_job_types
    assert {"kmi", "kmi_public_radar_composite"} <= set(
        benelux.legacy_provider_modules
    )

    validation = WORKLOADS["provider.validation_only_nowcasts"]
    assert (
        "validation.italiameteo.dpc_sri_historical_catalog"
        in validation.acquisition_workload_ids
    )
    model = WORKLOADS["provider.model_guidance"]
    assert (
        "model.openmeteo.italiameteo_icon2i.region"
        in model.acquisition_workload_ids
    )


def test_all_legacy_interval_callbacks_are_explicitly_accounted_for():
    source = (ROOT / "custom_components/storm_tracker_v3/__init__.py").read_text(
        encoding="utf-8"
    )
    scheduled = {
        callback
        for callback in ("_poll_all", "_poll_netatmo", "_poll_eumetsat_li", "_poll_goes_glm")
        if f"hass, {callback}, timedelta(" in source
    }
    manifested = {
        entry
        for item in WORKLOADS.values()
        for entry in item.legacy_ha_entrypoints
    }
    assert scheduled == {
        "_poll_all", "_poll_eumetsat_li", "_poll_goes_glm", "_poll_netatmo"
    }
    assert scheduled <= manifested


def test_every_meteorological_production_job_is_owned_by_a_manifest_row():
    control = {
        "desired_state_apply", "desired_state_claim", "desired_state_snapshot",
        "operational_snapshot_commit", "operational_snapshot_head", "ping",
    }
    manifested = {
        job
        for item in WORKLOADS.values()
        for job in item.worker_job_types
    }
    assert set(service.PRODUCTION_JOB_HANDLERS) - control <= manifested


def test_manifest_itself_has_no_runtime_or_provider_import_surface():
    import storm_tracker_worker.worker_cutover_manifest as module

    source = inspect.getsource(module)
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(
                alias.name.split(".", 1)[0] for alias in node.names
            )
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
    assert imported_roots <= {
        "__future__", "dataclasses", "enum", "hashlib", "json", "re",
        "types",
    }


def test_unresolved_list_never_hides_blocked_gaps():
    unresolved = unresolved_workloads()
    assert len(unresolved) == len(WORKLOADS) - 1
    blocked = {item.workload_id for item in unresolved if item.state is CutoverState.BLOCKED_GAP}
    assert blocked == set()
    blitzortung = WORKLOADS["provider.blitzortung_push"]
    assert blitzortung.state is CutoverState.SHADOW_CANDIDATE
    assert blitzortung.shadow_components == (
        "blitzortung_event_ingress",
        "blitzortung_mqtt_raw_sidecar",
        "blitzortung_raw_ingress",
        "raw_capture_service_composition",
    )
    assert blitzortung.worker_job_types == ()
    assert blitzortung.acquisition_workload_ids == ()
    assert blitzortung.event_ingress_ids == (
        "lightning.blitzortung.event_ingress",
    )
    assert "quiet intervals never prove absence" in blitzortung.cutover_gate


def test_event_catalog_no_longer_claims_missing_implemented_ingress_primitives():
    catalog = worker_acquisition_plan.acquisition_catalog_snapshot()
    ingress = catalog["event_ingresses"][0]
    assert ingress["status"] == "ownership_blocked"
    assert ingress["blocked_reason"] == (
        "external_participant_raw_retention_authority_and_live_qos0_soak_missing"
    )
    stale_claims = (
        "backpressure_gap_receipts",
        "decoder_and_soak_missing",
    )
    assert not any(claim in ingress["blocked_reason"] for claim in stale_claims)


def test_worker_acquisition_shadow_runtime_is_visible_but_cannot_claim_cutover():
    workload = WORKLOADS["control.worker_acquisition_planning"]
    assert workload.state is CutoverState.SHADOW_CANDIDATE
    assert {
        "acquisition_activation",
        "acquisition_execution_store",
        "acquisition_handler_child",
        "acquisition_runtime",
        "acquisition_scheduler_bridge",
        "worker_acquisition_plan",
    } == set(workload.shadow_components)
    assert workload.worker_job_types == ()
    assert workload.provider_ids == ()
    assert not workload.ha_allowed_after_cutover
    assert "target materialisation" in workload.cutover_gate


def test_end_to_end_radar_cell_shadow_chain_is_manifested_with_gates_open():
    workload = WORKLOADS["compute.radar_normalisation"]
    assert workload.state is CutoverState.SHADOW_CANDIDATE
    assert {
        "dwd_rv_analysis_grid_certificate",
        "evidence_to_cells_shadow",
        "knmi_rtcor_analysis_grid_gate",
        "science_shadow_orchestrator",
    } <= set(workload.shadow_components)
    assert "hard process isolation" in workload.cutover_gate


def test_ground_and_snapshot_shadow_statuses_are_explicitly_unadmitted():
    ground = WORKLOADS["provider.ground_networks"]
    assert ground.state is CutoverState.SHADOW_CANDIDATE
    assert "ground_fusion_shadow_runtime" in ground.shadow_components
    assert "held-out" in ground.cutover_gate

    publication = WORKLOADS["publication.operational_snapshot"]
    assert publication.state is CutoverState.SHADOW_CANDIDATE
    assert "operational_snapshot_runtime" in publication.shadow_components
    assert "rich targets remain blocked" in publication.cutover_gate
