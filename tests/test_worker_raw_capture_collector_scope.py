from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_runtime as runtime_module
from storm_tracker_worker import acquisition_scheduler_bridge as bridge
from storm_tracker_worker import desired_state
from storm_tracker_worker import worker_acquisition_plan as acquisition
from storm_tracker_worker.raw_artifacts import RawArtifactLimits
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityProfile,
)


NOW = 1_800_000_000.0


def _desired_payload(*, model_guidance_enabled: bool = False) -> dict:
    return {
        "authority_epoch": 1,
        "contract": desired_state.CONTRACT,
        "generation": 1,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "producer_id": "ha-raw-collector-scope-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": model_guidance_enabled,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Home",
            "kind": "home",
            "latitude": 51.04,
            "location_accuracy_m": None,
            "longitude": 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW - 60.0,
            "target_id": "zone.home",
        }],
    }


def _state(*, model_guidance_enabled: bool = False) -> desired_state.DesiredState:
    return desired_state.parse_desired_state(
        _desired_payload(model_guidance_enabled=model_guidance_enabled)
    )


def _runtime_capacity_contract() -> tuple[RawCapacityProfile, RawArtifactLimits]:
    active = tuple(
        rule for rule in activation.ACTIVATION_RULES.values() if rule.executable
    )
    assert active
    required_bytes = sum(int(rule.maximum_transport_bytes or 0) for rule in active)
    required_entries = sum(int(rule.maximum_artifacts or 0) for rule in active)
    maximum_object_bytes = max(
        int(rule.maximum_transport_bytes or 0) for rule in active
    )
    global_bytes = max(required_bytes * 2, maximum_object_bytes * 2)
    global_entries = max(required_entries * 2, 256)
    providers = tuple(sorted({rule.provider_id for rule in active}))
    profile = RawCapacityProfile(
        global_max_bytes=global_bytes,
        global_max_entries=global_entries,
        providers=tuple(
            ProviderCapacityLimit(provider, global_bytes, global_entries)
            for provider in providers
        ),
        max_reservations=max(len(active) * 2, 100),
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )
    limits = RawArtifactLimits(
        max_object_bytes=maximum_object_bytes,
        max_total_bytes=global_bytes,
        max_object_entries=global_entries,
        max_staging_entries=max(required_entries, 64),
        max_receipt_entries=global_entries,
        max_capture_entries=max(len(active) * 2, 128),
        max_capture_members=max(
            1,
            max(len(rule.required_roles) for rule in active),
        ),
        max_catalog_bytes=8 * 1024 * 1024,
    )
    return profile, limits


def _runtime_paths(tmp_path: Path) -> runtime_module.ShadowRuntimePaths:
    return runtime_module.ShadowRuntimePaths(
        desired_state=(tmp_path / "desired.sqlite3").resolve(),
        scheduler=(tmp_path / "scheduler.sqlite3").resolve(),
        capacity_ledger=(tmp_path / "capacity.sqlite3").resolve(),
        execution_receipts=(tmp_path / "execution.sqlite3").resolve(),
        raw_store_root=(tmp_path / "raw").resolve(),
    )


def test_default_scope_remains_the_old_coverage_relevant_selection() -> None:
    state = _state(model_guidance_enabled=False)

    implicit = acquisition.build_worker_acquisition_plan(state)
    explicit = acquisition.build_worker_acquisition_plan(
        state,
        collector_scope=acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT,
    )

    assert implicit == explicit
    assert implicit.collector_scope == acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT
    selected = {intent.workload_id for intent in implicit.intents}
    assert "model.openmeteo.region" not in selected
    assert {
        "radar.aemet.composite",
        "radar.dpc.sri",
        "satellite.goes18.rrqpe",
    }.isdisjoint(selected)


def test_all_sources_keeps_geographic_scope_without_model_gate() -> None:
    coverage = acquisition.build_worker_acquisition_plan(
        _state(model_guidance_enabled=False),
        collector_scope=acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT,
    )
    all_sources = acquisition.build_worker_acquisition_plan(
        _state(model_guidance_enabled=False),
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )
    coverage_ids = {intent.workload_id for intent in coverage.intents}
    planned = {intent.workload_id for intent in all_sources.intents}

    assert planned == coverage_ids | {"model.openmeteo.region"}
    assert "model.openmeteo.region" in planned
    assert {
        "radar.aemet.composite",
        "radar.dpc.sri",
        "satellite.goes18.rrqpe",
    }.isdisjoint(planned)
    assert all(
        intent.region_ids == (all_sources.regions[0].region_id,)
        for intent in all_sources.intents
    )


@pytest.mark.parametrize(
    ("country_code", "latitude", "longitude", "expected", "excluded"),
    (
        (
            "ES", 40.4168, -3.7038,
            {"radar.aemet.composite"},
            {"radar.dpc.sri", "radar.kmi.appv4_visual", "radar.metoffice.uk_rate"},
        ),
        (
            "IT", 41.9028, 12.4964,
            {"radar.dpc.sri", "model.openmeteo.italiameteo_icon2i.region"},
            {"radar.aemet.composite", "radar.kmi.appv4_visual", "radar.metoffice.uk_rate"},
        ),
        (
            "US", 41.8781, -87.6298,
            {"ground.synoptic.region", "lightning.goes18.glm", "satellite.goes18.rrqpe"},
            {"radar.aemet.composite", "radar.dpc.sri", "radar.kmi.appv4_visual"},
        ),
        (
            "GB", 53.4808, -2.2426,
            {"radar.metoffice.uk_rate"},
            {"radar.aemet.composite", "radar.dpc.sri", "satellite.goes18.rrqpe"},
        ),
    ),
)
def test_all_sources_only_plans_location_applicable_workloads(
    country_code: str,
    latitude: float,
    longitude: float,
    expected: set[str],
    excluded: set[str],
) -> None:
    payload = _desired_payload(model_guidance_enabled=False)
    target = payload["targets"][0]
    target.update({
        "country_code": country_code,
        "latitude": latitude,
        "longitude": longitude,
    })
    state = desired_state.parse_desired_state(payload)
    plan = acquisition.build_worker_acquisition_plan(
        state,
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )
    planned = {intent.workload_id for intent in plan.intents}

    assert expected <= planned
    assert excluded.isdisjoint(planned)


@pytest.mark.parametrize(
    ("latitude", "longitude", "expected", "excluded"),
    (
        (
            50.8503, 4.3517,
            {
                "ground.kmi.aws_10min",
                "radar.dwd.radolan_rv",
                "radar.kmi.appv4_visual",
                "radar.kmi.public_wcs_unknown_quantity",
                "radar.knmi.rtcor",
                "radar.metoffice.uk_rate",
                "radar.opera.dbzh",
            },
            {
                "radar.aemet.composite",
                "radar.dpc.sri",
                "satellite.goes18.rrqpe",
            },
        ),
        (
            52.3676, 4.9041,
            {
                "radar.dwd.radolan_rv",
                "radar.kmi.appv4_visual",
                "radar.kmi.public_wcs_unknown_quantity",
                "radar.knmi.rtcor",
                "radar.metoffice.uk_rate",
                "radar.opera.dbzh",
            },
            {
                "ground.kmi.aws_10min",
                "radar.aemet.composite",
                "radar.dpc.sri",
            },
        ),
        (
            48.8566, 2.3522,
            {
                "radar.dwd.radolan_rv",
                "radar.kmi.appv4_visual",
                "radar.kmi.public_wcs_unknown_quantity",
                "radar.meteofrance.dpradar",
                "radar.opera.dbzh",
            },
            {
                "ground.kmi.aws_10min",
                "radar.aemet.composite",
                "radar.dpc.sri",
                "satellite.goes18.rrqpe",
            },
        ),
        (
            54.18293, 12.18277,
            {"radar.dwd.radolan_rv", "radar.opera.dbzh"},
            {
                "ground.kmi.aws_10min",
                "radar.aemet.composite",
                "radar.dpc.sri",
                "radar.kmi.appv4_visual",
                "radar.knmi.rtcor",
                "radar.meteofrance.dpradar",
                "radar.metoffice.uk_rate",
            },
        ),
        (
            53.4808, -2.2426,
            {"radar.metoffice.uk_rate"},
            {"radar.aemet.composite", "radar.dpc.sri", "satellite.goes18.rrqpe"},
        ),
        (
            41.8781, -87.6298,
            {"ground.synoptic.region", "lightning.goes18.glm", "satellite.goes18.rrqpe"},
            {"radar.aemet.composite", "radar.dpc.sri", "radar.metoffice.uk_rate"},
        ),
        (
            40.4168, -3.7038,
            {"radar.aemet.composite"},
            {"radar.dpc.sri", "radar.metoffice.uk_rate", "satellite.goes18.rrqpe"},
        ),
        (
            41.9028, 12.4964,
            {"radar.dpc.sri", "model.openmeteo.italiameteo_icon2i.region"},
            {"radar.aemet.composite", "radar.metoffice.uk_rate", "satellite.goes18.rrqpe"},
        ),
    ),
)
def test_all_sources_uses_bounding_boxes_for_countryless_test_tracker(
    latitude: float,
    longitude: float,
    expected: set[str],
    excluded: set[str],
) -> None:
    """The managed MQTT test tracker intentionally has no country attribute."""

    payload = _desired_payload(model_guidance_enabled=False)
    target = payload["targets"][0]
    target.update({
        "country_code": None,
        "kind": "test",
        "latitude": latitude,
        "longitude": longitude,
        "target_id": "test_tracker",
    })
    state = desired_state.parse_desired_state(payload)
    plan = acquisition.build_worker_acquisition_plan(
        state,
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )
    planned = {intent.workload_id for intent in plan.intents}

    assert expected <= planned
    assert excluded.isdisjoint(planned)


def test_canonical_plan_bytes_and_plan_hash_bind_collector_scope() -> None:
    state = _state(model_guidance_enabled=False)
    coverage = acquisition.build_worker_acquisition_plan(
        state,
        collector_scope=acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT,
    )
    all_sources = acquisition.build_worker_acquisition_plan(
        state,
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )

    assert json.loads(coverage.canonical_bytes)["collector_scope"] == (
        acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT
    )
    assert json.loads(all_sources.canonical_bytes)["collector_scope"] == (
        acquisition.COLLECTOR_SCOPE_ALL_SOURCES
    )
    assert coverage.canonical_bytes != all_sources.canonical_bytes
    assert coverage.plan_sha256 != all_sources.plan_sha256
    assert coverage.plan_sha256 == hashlib.sha256(coverage.canonical_bytes).hexdigest()
    assert all_sources.plan_sha256 == hashlib.sha256(
        all_sources.canonical_bytes
    ).hexdigest()
    assert acquisition.validate_worker_acquisition_plan(coverage) is coverage
    assert acquisition.validate_worker_acquisition_plan(all_sources) is all_sources

    forged = replace(
        all_sources,
        collector_scope=acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT,
    )
    with pytest.raises(acquisition.AcquisitionConflictError):
        acquisition.validate_worker_acquisition_plan(forged)


def test_unknown_scope_fails_before_runtime_constructs_scheduler_or_stores(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def _must_not_construct(name: str):
        def _bomb(*_args, **_kwargs):
            calls.append(name)
            raise AssertionError(f"{name} was constructed before scope validation")

        return _bomb

    monkeypatch.setattr(
        runtime_module,
        "DesiredStateStore",
        _must_not_construct("desired_state_store"),
    )
    monkeypatch.setattr(
        runtime_module.scheduler_module,
        "AutonomousScheduler",
        _must_not_construct("scheduler"),
    )
    monkeypatch.setattr(
        runtime_module,
        "RawCapacityLedger",
        _must_not_construct("capacity_ledger"),
    )
    monkeypatch.setattr(
        runtime_module,
        "ExecutionReceiptStore",
        _must_not_construct("execution_store"),
    )
    profile, limits = _runtime_capacity_contract()

    with pytest.raises(acquisition.AcquisitionPlanError, match="collector_scope"):
        runtime_module.create_shadow_candidate(
            _runtime_paths(tmp_path),
            capacity_profile=profile,
            raw_store_limits=limits,
            collector_scope="unknown-scope",
        )

    assert calls == []
    assert list(tmp_path.iterdir()) == []


def test_scheduler_builder_and_runtime_share_all_sources_scope(
    tmp_path: Path,
) -> None:
    state = _state(model_guidance_enabled=False)
    plan = acquisition.build_worker_acquisition_plan(
        state,
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )
    schedule = bridge.acquisition_scheduler_plan_builder(
        state,
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )
    profile, limits = _runtime_capacity_contract()
    runtime = runtime_module.create_shadow_candidate(
        _runtime_paths(tmp_path),
        capacity_profile=profile,
        raw_store_limits=limits,
        allowed_workload_ids=frozenset(
            rule.workload_id
            for rule in activation.ACTIVATION_RULES.values()
            if rule.executable
        ),
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
        clock=lambda: NOW,
    )
    runtime.desired_state_store.claim_producer(
        state.producer_id,
        "claim-raw-all-sources",
        claimed_at=NOW - 100.0,
    )
    runtime.desired_state_store.apply(
        _desired_payload(model_guidance_enabled=False),
        received_at=NOW - 59.0,
    )

    assert {intent.payload["workload_id"] for intent in schedule} == {
        intent.workload_id for intent in plan.intents
    }
    assert {intent.payload["plan_sha256"] for intent in schedule} == {
        plan.plan_sha256
    }
    reconciled = runtime.scheduler.reconcile(now=NOW)
    diagnostics = runtime.diagnostics()

    assert reconciled.intent_count == len(schedule)
    assert diagnostics["collector_scope"] == acquisition.COLLECTOR_SCOPE_ALL_SOURCES
    assert diagnostics["scheduler"]["active_intent_count"] == len(schedule)
    assert diagnostics["scheduler"]["active_plan_sha256"] is not None
