from __future__ import annotations

from dataclasses import replace
import hashlib
import inspect
import json
from pathlib import Path
import threading

import pytest

from storm_tracker_worker import desired_state as desired
from storm_tracker_worker import worker_acquisition_plan as acquisition


ARTIFACT_SHA = "a" * 64


def _target(
    target_id: str,
    latitude: float,
    longitude: float,
    country: str | None,
    *,
    available: bool = True,
    radius_km: float = 250.0,
    observed_at: float = 1_800_000_000.0,
) -> dict:
    return {
        "available": available,
        "country_code": country,
        "display_name": target_id.replace(".", " ").title(),
        "kind": "home" if target_id.startswith("zone.") else "person",
        "latitude": latitude,
        "location_accuracy_m": None,
        "longitude": longitude,
        "observation_radius_km": radius_km,
        "position_observed_at": observed_at if available else None,
        "target_id": target_id,
    }


def _snapshot(
    targets: list[dict] | None = None,
    *,
    generation: int = 1,
    model: bool = True,
    sharing_km: float = 150.0,
) -> desired.DesiredState:
    if targets is None:
        targets = [
            _target("zone.home", 51.04, 4.48, "BE"),
            _target("person.new_york", 40.7128, -74.006, "US"),
        ]
    return desired.parse_desired_state(
        {
            "authority_epoch": 7,
            "contract": desired.CONTRACT,
            "generation": generation,
            "issued_at": 1_800_000_010.0 + generation,
            "previous_state_sha256": None,
            "producer_id": "ha-boot-contract-test",
            "settings": {
                "ground_radius_km": 20.0,
                "lightning_source_mode": "auto",
                "model_guidance_enabled": model,
                "region_sharing_distance_km": sharing_km,
            },
            "targets": targets,
        }
    )


def _plan(**kwargs) -> acquisition.AcquisitionPlan:
    return acquisition.build_worker_acquisition_plan(_snapshot(**kwargs))


def _dispatch(
    plan: acquisition.AcquisitionPlan,
    *,
    at: float = 1_800_000_000.0,
    secrets=acquisition.KNOWN_SECRET_REQUIREMENT_IDS,
    receipts=(),
    capacity=acquisition.DispatchCapacity(),
) -> acquisition.DispatchSnapshot:
    return acquisition.build_worker_acquisition_dispatch(
        plan,
        as_of=at,
        available_secret_requirement_ids=secrets,
        receipts=receipts,
        capacity=capacity,
    )


def test_catalog_is_closed_bounded_and_covers_all_workload_classes():
    catalog = acquisition.acquisition_catalog_snapshot()
    assert catalog["catalog_sha256"] == acquisition.CATALOG_SHA256
    assert catalog["cadence_profile_sha256"] == acquisition.CADENCE_PROFILE_SHA256
    assert {item["workload_class"] for item in catalog["workloads"]} == {
        item.value for item in acquisition.WorkloadClass
    }
    assert len(catalog["workloads"]) == len(
        {item["workload_id"] for item in catalog["workloads"]}
    )
    assert len(json.dumps(catalog, sort_keys=True).encode()) < 64 * 1024
    assert all(item["deadline_seconds"] <= item["cadence_seconds"] for item in catalog["workloads"])
    assert all(
        item["budget"] is None or item["budget"]["max_transport_bytes"] > 0
        for item in catalog["workloads"]
    )
    assert len(catalog["workloads"]) == 27
    assert [item["ingress_id"] for item in catalog["event_ingresses"]] == [
        "lightning.blitzortung.event_ingress"
    ]
    assert "lightning.blitzortung" not in {
        item["workload_id"] for item in catalog["workloads"]
    }
    assert catalog["event_ingresses"][0]["cadence_seconds"] is None
    assert catalog["event_ingresses"][0]["absence_semantics"] == "no_absence_claim"
    assert catalog["migrations"]["transparent_aliases"] is False


def test_blocked_products_are_visible_and_never_become_intents():
    plan = _plan()
    blocked = {item.workload_id: item for item in plan.blocked_capabilities}
    assert set(blocked) == {"radar.kmi.radqpe_native"}
    assert not set(blocked).intersection(item.workload_id for item in plan.intents)
    catalog = acquisition.acquisition_catalog_snapshot()["workloads"]
    statuses = {
        item["workload_id"]: item["status"]
        for item in catalog
        if item["workload_id"] in blocked
    }
    assert statuses["radar.kmi.radqpe_native"] == "api_required"
    assert set(statuses.values()) == {"api_required"}


def test_kmi_products_are_semantically_separate_and_non_admitted():
    rows = {
        row["workload_id"]: row
        for row in acquisition.acquisition_catalog_snapshot()["workloads"]
        if row["provider_id"] == "kmi"
    }
    assert set(rows) == {
        "ground.kmi.aws_10min",
        "radar.kmi.appv4_visual",
        "radar.kmi.public_wcs_unknown_quantity",
        "radar.kmi.radqpe_native",
    }
    assert len({row["product_id"] for row in rows.values()}) == 4
    assert rows["ground.kmi.aws_10min"]["source_role"] == (
        "exact_raw_ground_observation_unverified"
    )
    assert rows["radar.kmi.appv4_visual"]["source_role"] == (
        "categorical_visualization_only"
    )
    assert rows["radar.kmi.public_wcs_unknown_quantity"]["source_role"] == (
        "exact_raw_radar_response_unverified"
    )
    assert rows["radar.kmi.radqpe_native"]["source_role"] == (
        "quantitative_radar_candidate"
    )
    assert rows["radar.kmi.radqpe_native"]["budget"] is None
    assert rows["radar.kmi.public_wcs_unknown_quantity"]["budget"] == {
        "max_artifacts": 1,
        "max_transport_bytes": 64 * 1024 * 1024,
        "max_wall_seconds": 50,
    }
    assert all(row["admission_eligible"] is False for row in rows.values())
    assert rows["ground.kmi.aws_10min"][
        "durable_production_retention_eligible"
    ] is True
    assert rows["radar.kmi.public_wcs_unknown_quantity"][
        "durable_production_retention_eligible"
    ] is True
    assert rows["radar.kmi.appv4_visual"][
        "durable_production_retention_eligible"
    ] is False
    assert rows["radar.kmi.appv4_visual"]["status"] == "contract_ready_unwired"
    assert rows["radar.kmi.appv4_visual"]["budget"] == {
        "max_artifacts": 2,
        "max_transport_bytes": 12 * 1024 * 1024,
        "max_wall_seconds": 45,
    }
    assert rows["radar.kmi.radqpe_native"][
        "durable_production_retention_eligible"
    ] is False


def test_catalog_budgets_match_exact_native_role_envelopes():
    rows = {
        row["workload_id"]: row
        for row in acquisition.acquisition_catalog_snapshot()["workloads"]
    }
    assert rows["radar.aemet.composite"]["budget"] == {
        "max_artifacts": 3,
        "max_transport_bytes": 101 * 1024 * 1024,
        "max_wall_seconds": 75,
    }
    assert rows["satellite.hsaf.h40b"]["budget"] == {
        "max_artifacts": 2,
        "max_transport_bytes": 66 * 1024 * 1024,
        "max_wall_seconds": 90,
    }
    for workload_id in ("lightning.goes18.glm", "lightning.goes19.glm"):
        assert rows[workload_id]["budget"] == {
            "max_artifacts": 14,
            "max_transport_bytes": 68 * 1024 * 1024,
            "max_wall_seconds": 105,
        }
        assert rows[workload_id]["status"] == "contract_ready_unwired"


def test_legacy_ids_require_explicit_rebuild_and_never_alias_dispatch():
    migrations = {
        item["legacy_id"]: item
        for item in acquisition.acquisition_catalog_snapshot()["migrations"]["entries"]
    }
    assert set(migrations) == {
        "lightning.blitzortung",
        "radar.geosphere.at",
        "radar.italiameteo.it",
        "radar.kmi.radqpe",
        "radar.meteolux.lu",
    }
    assert migrations["radar.kmi.radqpe"]["replacement_ids"] == [
        "radar.kmi.appv4_visual",
        "radar.kmi.public_wcs_unknown_quantity",
        "radar.kmi.radqpe_native",
    ]
    assert migrations["radar.italiameteo.it"]["replacement_ids"] == [
        "model.openmeteo.italiameteo_icon2i.region",
        "validation.italiameteo.dpc_sri_historical_catalog",
    ]
    active_ids = {
        item["workload_id"]
        for item in acquisition.acquisition_catalog_snapshot()["workloads"]
    }
    assert not set(migrations).intersection(active_ids)


def test_plan_is_byte_deterministic_and_target_input_order_independent():
    targets = [
        _target("zone.home", 51.04, 4.48, "BE"),
        _target("person.new_york", 40.7128, -74.006, "US"),
    ]
    first = acquisition.build_worker_acquisition_plan(_snapshot(targets))
    second = acquisition.build_worker_acquisition_plan(_snapshot(list(reversed(targets))))
    assert first == second
    assert first.canonical_bytes == second.canonical_bytes
    assert first.plan_sha256 == hashlib.sha256(first.canonical_bytes).hexdigest()
    assert acquisition.validate_worker_acquisition_plan(first) is first


def test_plan_binds_snapshot_generation_catalog_and_cadence_profile():
    first = _plan(generation=1)
    second = _plan(generation=2)
    assert first.snapshot_sha256 != second.snapshot_sha256
    assert first.plan_sha256 != second.plan_sha256
    assert first.generation == 1
    assert first.authority_epoch == 7
    assert first.catalog_sha256 == acquisition.CATALOG_SHA256
    assert first.cadence_profile_id == acquisition.CADENCE_PROFILE_ID
    assert first.cadence_profile_sha256 == acquisition.CADENCE_PROFILE_SHA256
    decoded = json.loads(first.canonical_bytes)
    assert decoded["snapshot_sha256"] == first.snapshot_sha256
    assert decoded["cadence_profile_sha256"] == first.cadence_profile_sha256


def test_liveness_only_snapshot_refresh_keeps_region_and_intent_identities():
    first = _plan(generation=1)
    refreshed = _plan(generation=2)

    assert first.snapshot_sha256 != refreshed.snapshot_sha256
    assert first.plan_sha256 != refreshed.plan_sha256
    assert [item.region_id for item in first.regions] == [
        item.region_id for item in refreshed.regions
    ]
    assert [item.intent_id for item in first.intents] == [
        item.intent_id for item in refreshed.intents
    ]


def test_forged_or_nonvalidated_desired_state_is_rejected():
    valid = _snapshot()
    with pytest.raises(acquisition.AcquisitionSnapshotError):
        acquisition.build_worker_acquisition_plan(valid.as_dict())
    forged = replace(valid, state_sha256="f" * 64)
    with pytest.raises(acquisition.AcquisitionSnapshotError, match="verschillen"):
        acquisition.build_worker_acquisition_plan(forged)
    forged_bytes = replace(valid, canonical_bytes=b'{"broken":true}')
    with pytest.raises(acquisition.AcquisitionSnapshotError, match="canonical"):
        acquisition.build_worker_acquisition_plan(forged_bytes)


def test_close_targets_share_one_region_and_region_workloads_are_deduplicated():
    plan = _plan(
        targets=[
            _target("zone.home", 51.04, 4.48, "BE"),
            _target("person.nearby", 51.10, 4.60, "BE"),
        ],
        sharing_km=50.0,
    )
    assert len(plan.regions) == 1
    assert plan.regions[0].target_ids == ("person.nearby", "zone.home")
    for workload in ("ground.netatmo.region", "radar.rainviewer.visual", "model.openmeteo.region"):
        assert sum(item.workload_id == workload for item in plan.intents) == 1


def test_distant_targets_keep_separate_regions_but_global_and_coverage_fetches_dedupe():
    plan = _plan(
        targets=[
            _target("zone.antwerp", 51.22, 4.40, "BE"),
            _target("zone.arlon", 49.68, 5.81, "BE"),
        ],
        sharing_km=50.0,
    )
    assert len(plan.regions) == 2
    assert sum(item.workload_id == "ground.netatmo.region" for item in plan.intents) == 2
    assert sum(item.workload_id == "ground.metar.global" for item in plan.intents) == 1
    assert sum(item.workload_id == "satellite.noaa.err" for item in plan.intents) == 1
    openmeteo = [
        item
        for item in plan.intents
        if item.workload_id == "model.openmeteo.region"
    ]
    assert len(openmeteo) == 1
    assert openmeteo[0].target_ids == ("zone.antwerp", "zone.arlon")
    kmi = [item for item in plan.intents if item.provider_id == "kmi"]
    assert {item.workload_id for item in kmi} == {
        "ground.kmi.aws_10min",
        "radar.kmi.appv4_visual",
        "radar.kmi.public_wcs_unknown_quantity",
    }
    coverage_kmi = [
        item for item in kmi if item.workload_id != "radar.kmi.appv4_visual"
    ]
    assert all(
        set(item.target_ids) == {"zone.antwerp", "zone.arlon"}
        for item in coverage_kmi
    )
    appv4 = [
        item for item in kmi if item.workload_id == "radar.kmi.appv4_visual"
    ]
    assert len(appv4) == 2
    assert {item.target_ids for item in appv4} == {
        ("zone.antwerp",),
        ("zone.arlon",),
    }


def test_complete_link_clustering_prevents_transitive_puppy_chain_region():
    # About 44 km between neighbours and 88 km between endpoints.
    plan = _plan(
        targets=[
            _target("zone.a", 50.0, 4.0, "BE", radius_km=10.0),
            _target("zone.b", 50.4, 4.0, "BE", radius_km=10.0),
            _target("zone.c", 50.8, 4.0, "BE", radius_km=10.0),
        ],
        sharing_km=50.0,
    )
    assert len(plan.regions) == 2
    assert sorted(len(item.target_ids) for item in plan.regions) == [1, 2]


def test_unavailable_targets_create_no_region_or_work_for_their_location():
    plan = _plan(
        targets=[
            _target("zone.home", 51.04, 4.48, "BE"),
            _target("person.offline", 40.7128, -74.006, "US", available=False),
        ]
    )
    assert len(plan.regions) == 1
    assert all("person.offline" not in item.target_ids for item in plan.intents)
    assert not any(item.provider_id == "synoptic" for item in plan.intents)


def test_empty_available_snapshot_is_bounded_and_does_not_invent_jobs():
    plan = _plan(targets=[])
    assert plan.regions == ()
    assert plan.intents == ()
    assert len(plan.blocked_capabilities) == 1
    dispatch = _dispatch(plan)
    assert dispatch.next_job is None
    assert dispatch.ready_jobs == ()


def test_model_setting_controls_only_model_acquisition():
    enabled = _plan(model=True)
    disabled = _plan(model=False)
    assert any(item.workload_class == "model" for item in enabled.intents)
    assert not any(item.workload_class == "model" for item in disabled.intents)
    enabled_non_model = {item.workload_id for item in enabled.intents if item.workload_class != "model"}
    disabled_non_model = {item.workload_id for item in disabled.intents}
    assert enabled_non_model == disabled_non_model


def test_plan_has_each_requested_class_for_cross_continent_targets():
    plan = _plan()
    assert {item.workload_class for item in plan.intents} == {
        "radar", "satellite", "ground", "model", "lightning"
    }
    assert {item.satellite_id for item in plan.intents if item.satellite_id} == {"G18", "G19"}


@pytest.mark.parametrize(
    "field,value",
    [
        ("catalog_sha256", "1" * 64),
        ("cadence_profile_sha256", "2" * 64),
        ("plan_sha256", "3" * 64),
        ("canonical_bytes", b"{}"),
    ],
)
def test_plan_header_or_canonical_tamper_is_rejected(field, value):
    plan = _plan()
    with pytest.raises(acquisition.AcquisitionConflictError):
        acquisition.validate_worker_acquisition_plan(replace(plan, **{field: value}))


def test_region_and_intent_member_tamper_is_rejected_even_if_outer_bytes_are_rebuilt():
    plan = _plan()
    region = replace(plan.regions[0], center_latitude=plan.regions[0].center_latitude + 0.1)
    tampered = replace(plan, regions=(region,) + plan.regions[1:])
    with pytest.raises(acquisition.AcquisitionConflictError, match="regiochecksum"):
        acquisition.validate_worker_acquisition_plan(tampered)

    intent = replace(
        plan.intents[0],
        deadline_seconds=plan.intents[0].deadline_seconds + 1,
    )
    tampered = replace(plan, intents=(intent,) + plan.intents[1:])
    with pytest.raises(acquisition.AcquisitionConflictError, match="intentchecksum"):
        acquisition.validate_worker_acquisition_plan(tampered)


def test_plan_and_catalog_never_contain_secret_values_or_handler_references():
    plan = _plan()
    encoded = plan.canonical_bytes.lower()
    assert b"authorization" not in encoded
    assert b"bearer " not in encoded
    assert b"client_secret" not in encoded
    assert b"handler" not in encoded
    assert b"module:" not in encoded
    for intent in plan.intents:
        assert all(value.startswith("worker_secret.") for value in intent.secret_requirement_ids)
    assert set(value for item in plan.intents for value in item.secret_requirement_ids).issubset(
        acquisition.KNOWN_SECRET_REQUIREMENT_IDS
    )


def test_missing_secret_requirement_blocks_without_revealing_or_removing_workload():
    plan = _plan()
    no_secrets = _dispatch(plan, secrets=())
    blocked_ids = {item.subject_id for item in no_secrets.blocked}
    secret_jobs = [
        acquisition._job_for_slot(plan, item, no_secrets.as_of)
        for item in plan.intents
        if item.secret_requirement_ids
    ]
    assert secret_jobs
    assert {item.job_id for item in secret_jobs}.issubset(blocked_ids)
    assert all(item.reason_code == "secret_requirement_unavailable" for item in no_secrets.blocked)
    with_secrets = _dispatch(plan)
    assert not any(item.reason_code == "secret_requirement_unavailable" for item in with_secrets.blocked)


@pytest.mark.parametrize(
    "values",
    [
        {"actual-secret-value"},
        {"worker_secret.unknown"},
        {"Bearer abc"},
        {1},
    ],
)
def test_dispatch_accepts_only_known_abstract_secret_requirement_ids(values):
    with pytest.raises(acquisition.AcquisitionPlanError, match="secret availability"):
        _dispatch(_plan(), secrets=values)


def test_job_identity_is_stable_within_slot_and_changes_at_next_cadence_slot():
    plan = _plan(targets=[_target("zone.home", 51.04, 4.48, "BE")])
    first = _dispatch(plan, at=1_800_000_000.0)
    replay = _dispatch(plan, at=1_800_000_001.0)
    assert [item.job_id for item in first.ready_jobs] == [item.job_id for item in replay.ready_jobs]
    assert [item.job_sha256 for item in first.ready_jobs] == [item.job_sha256 for item in replay.ready_jobs]
    lightning = next(item for item in first.ready_jobs if item.workload_class == "lightning")
    next_slot = _dispatch(plan, at=lightning.scheduled_for + lightning.cadence_seconds)
    advanced = next(item for item in next_slot.ready_jobs if item.intent_id == lightning.intent_id)
    assert advanced.slot_index == lightning.slot_index + 1
    assert advanced.job_id != lightning.job_id
    assert advanced.plan_sha256 == plan.plan_sha256
    assert advanced.snapshot_sha256 == plan.snapshot_sha256


def test_forged_job_fields_cannot_be_signed_into_a_receipt():
    job = _dispatch(_plan()).ready_jobs[0]
    forged = replace(job, product_id="forged.product")
    with pytest.raises(acquisition.AcquisitionConflictError, match="job identity"):
        acquisition.make_worker_acquisition_receipt(
            forged,
            outcome="succeeded",
            completed_at=forged.scheduled_for,
            artifact_receipt_sha256=ARTIFACT_SHA,
        )


def test_dispatch_snapshot_is_deterministic_for_exact_replay():
    plan = _plan()
    first = _dispatch(plan)
    second = _dispatch(plan)
    assert first == second
    assert first.dispatch_sha256 == second.dispatch_sha256
    assert first.as_dict() == second.as_dict()


def test_deadline_is_strict_and_expired_job_never_enters_ready_queue():
    plan = _plan(targets=[_target("zone.home", 51.04, 4.48, "BE")])
    intent = next(item for item in plan.intents if item.workload_id == "lightning.eumetsat.li")
    slot_start = 1_800_000_000.0
    before = _dispatch(plan, at=slot_start + intent.deadline_seconds - 0.001)
    assert any(item.intent_id == intent.intent_id for item in before.ready_jobs)
    boundary = _dispatch(plan, at=slot_start + intent.deadline_seconds)
    expected = acquisition._job_for_slot(plan, intent, boundary.as_of)
    assert expected.job_id not in {item.job_id for item in boundary.ready_jobs}
    assert any(
        item.subject_id == expected.job_id and item.reason_code == "deadline_expired"
        for item in boundary.blocked
    )


def test_capacity_admits_only_a_deterministic_prefix_and_exposes_every_deferred_job():
    plan = _plan()
    generous = _dispatch(plan)
    assert len(generous.ready_jobs) > 3
    first_budget = generous.ready_jobs[0].budget
    limits = acquisition.DispatchCapacity(
        max_jobs=1,
        max_transport_bytes=first_budget.max_transport_bytes,
        max_artifacts=first_budget.max_artifacts,
        max_wall_seconds=first_budget.max_wall_seconds,
    )
    limited = _dispatch(plan, capacity=limits)
    assert [item.job_id for item in limited.ready_jobs] == [generous.ready_jobs[0].job_id]
    assert len(limited.deferred) == len(generous.ready_jobs) - 1
    assert all(item.reason_code == "dispatch_capacity_exhausted" for item in limited.deferred)
    assert limited.serial_concurrency == 1


@pytest.mark.parametrize(
    "capacity",
    [
        acquisition.DispatchCapacity(max_jobs=0),
        acquisition.DispatchCapacity(max_transport_bytes=0),
        acquisition.DispatchCapacity(max_artifacts=0),
        acquisition.DispatchCapacity(max_wall_seconds=0),
        acquisition.DispatchCapacity(max_jobs=acquisition.MAX_DISPATCH_JOBS + 1),
    ],
)
def test_invalid_dispatch_capacity_fails_before_any_job(capacity):
    with pytest.raises(acquisition.AcquisitionPlanError):
        _dispatch(_plan(), capacity=capacity)


def test_success_receipt_binds_job_and_artifact_then_suppresses_exact_replay():
    plan = _plan(targets=[_target("zone.home", 51.04, 4.48, "BE")])
    initial = _dispatch(plan)
    job = initial.ready_jobs[0]
    receipt = acquisition.make_worker_acquisition_receipt(
        job,
        outcome="succeeded",
        completed_at=job.scheduled_for + 1.0,
        artifact_receipt_sha256=ARTIFACT_SHA,
    )
    assert acquisition.validate_worker_acquisition_receipt(
        receipt, plan_sha256=plan.plan_sha256
    ) == receipt
    replay = _dispatch(plan, receipts=(receipt,))
    assert job.job_id not in {item.job_id for item in replay.ready_jobs}
    assert job.job_id in replay.completed_job_ids
    assert receipt.receipt_sha256 == hashlib.sha256(
        json.dumps(
            {key: value for key, value in receipt.as_dict().items() if key != "receipt_sha256"},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


def test_terminal_failure_and_deadline_receipt_semantics_are_fail_closed():
    job = _dispatch(_plan()).ready_jobs[0]
    failed = acquisition.make_worker_acquisition_receipt(
        job,
        outcome="terminal_failed",
        completed_at=job.scheduled_for + 1.0,
        error_code="native_contract_failed",
    )
    assert failed.artifact_receipt_sha256 is None
    expired = acquisition.make_worker_acquisition_receipt(
        job,
        outcome="deadline_expired",
        completed_at=job.deadline_at,
        error_code="deadline_expired",
    )
    assert expired.outcome == "deadline_expired"
    with pytest.raises(acquisition.AcquisitionPlanError, match="artifact"):
        acquisition.make_worker_acquisition_receipt(
            job,
            outcome="succeeded",
            completed_at=job.scheduled_for,
        )
    with pytest.raises(acquisition.AcquisitionPlanError, match="error_code"):
        acquisition.make_worker_acquisition_receipt(
            job,
            outcome="terminal_failed",
            completed_at=job.scheduled_for,
        )
    with pytest.raises(acquisition.AcquisitionPlanError, match="deadline"):
        acquisition.make_worker_acquisition_receipt(
            job,
            outcome="succeeded",
            completed_at=job.deadline_at + 0.001,
            artifact_receipt_sha256=ARTIFACT_SHA,
        )


def test_receipt_checksum_plan_and_current_job_conflicts_are_rejected():
    plan = _plan()
    job = _dispatch(plan).ready_jobs[0]
    receipt = acquisition.make_worker_acquisition_receipt(
        job,
        outcome="succeeded",
        completed_at=job.scheduled_for,
        artifact_receipt_sha256=ARTIFACT_SHA,
    )
    with pytest.raises(acquisition.AcquisitionConflictError, match="ander plan"):
        acquisition.validate_worker_acquisition_receipt(
            receipt, plan_sha256="b" * 64
        )
    with pytest.raises(acquisition.AcquisitionConflictError, match="receiptchecksum"):
        acquisition.validate_worker_acquisition_receipt(
            replace(receipt, completed_at=receipt.completed_at + 1.0),
            plan_sha256=plan.plan_sha256,
        )
    wrong_job = replace(receipt, job_sha256="c" * 64, receipt_sha256="0" * 64)
    wrong_payload = wrong_job.as_dict()
    wrong_payload.pop("receipt_sha256")
    wrong_job = replace(wrong_job, receipt_sha256=acquisition._sha256(wrong_payload))
    with pytest.raises(acquisition.AcquisitionConflictError, match="jobchecksum"):
        _dispatch(plan, receipts=(wrong_job,))


def test_exact_duplicate_receipts_dedupe_but_conflicting_terminal_receipts_do_not():
    plan = _plan()
    job = _dispatch(plan).ready_jobs[0]
    first = acquisition.make_worker_acquisition_receipt(
        job,
        outcome="succeeded",
        completed_at=job.scheduled_for,
        artifact_receipt_sha256=ARTIFACT_SHA,
    )
    replay = _dispatch(plan, receipts=(first, first))
    assert replay.completed_job_ids == (job.job_id,)
    second = acquisition.make_worker_acquisition_receipt(
        job,
        outcome="terminal_failed",
        completed_at=job.scheduled_for,
        error_code="permanent_failure",
    )
    with pytest.raises(acquisition.AcquisitionConflictError, match="twee verschillende"):
        _dispatch(plan, receipts=(first, second))


def test_stateful_dispatcher_is_strictly_serial_and_repeated_claim_is_idempotent():
    plan = _plan()
    dispatcher = acquisition.WorkerAcquisitionDispatcher(
        plan,
        available_secret_requirement_ids=acquisition.KNOWN_SECRET_REQUIREMENT_IDS,
    )
    first = dispatcher.claim_next(as_of=1_800_000_000.0)
    assert first is not None
    assert dispatcher.claim_next(as_of=1_800_000_001.0) == first
    receipt = acquisition.make_worker_acquisition_receipt(
        first,
        outcome="succeeded",
        completed_at=first.scheduled_for + 1.0,
        artifact_receipt_sha256=ARTIFACT_SHA,
    )
    dispatcher.accept_receipt(receipt)
    second = dispatcher.claim_next(as_of=1_800_000_001.0)
    assert second is not None
    assert second.job_id != first.job_id
    assert len(dispatcher.receipts()) == 1


def test_parallel_thread_claims_can_observe_only_one_inflight_identity():
    dispatcher = acquisition.WorkerAcquisitionDispatcher(
        _plan(),
        available_secret_requirement_ids=acquisition.KNOWN_SECRET_REQUIREMENT_IDS,
    )
    barrier = threading.Barrier(12)
    results = []
    lock = threading.Lock()

    def claim():
        barrier.wait()
        value = dispatcher.claim_next(as_of=1_800_000_000.0)
        with lock:
            results.append(value.job_id if value else None)

    threads = [threading.Thread(target=claim) for _ in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()
    assert len(set(results)) == 1
    assert results[0] is not None


def test_dispatcher_reconstruction_from_receipts_replays_same_next_job():
    plan = _plan()
    first_dispatcher = acquisition.WorkerAcquisitionDispatcher(
        plan,
        available_secret_requirement_ids=acquisition.KNOWN_SECRET_REQUIREMENT_IDS,
    )
    first = first_dispatcher.claim_next(as_of=1_800_000_000.0)
    receipt = acquisition.make_worker_acquisition_receipt(
        first,
        outcome="succeeded",
        completed_at=first.scheduled_for,
        artifact_receipt_sha256=ARTIFACT_SHA,
    )
    first_dispatcher.accept_receipt(receipt)
    expected = first_dispatcher.claim_next(as_of=1_800_000_001.0)
    reconstructed = acquisition.WorkerAcquisitionDispatcher(
        plan,
        available_secret_requirement_ids=acquisition.KNOWN_SECRET_REQUIREMENT_IDS,
        receipts=first_dispatcher.receipts(),
    )
    assert reconstructed.claim_next(as_of=1_800_000_001.0) == expected


def test_dispatcher_rejects_out_of_order_receipt_and_releases_expired_inflight():
    plan = _plan()
    dispatcher = acquisition.WorkerAcquisitionDispatcher(
        plan,
        available_secret_requirement_ids=acquisition.KNOWN_SECRET_REQUIREMENT_IDS,
    )
    first = dispatcher.claim_next(as_of=1_800_000_000.0)
    other = _dispatch(plan).ready_jobs[1]
    other_receipt = acquisition.make_worker_acquisition_receipt(
        other,
        outcome="succeeded",
        completed_at=other.scheduled_for,
        artifact_receipt_sha256=ARTIFACT_SHA,
    )
    with pytest.raises(acquisition.AcquisitionConflictError, match="in-flight"):
        dispatcher.accept_receipt(other_receipt)
    after_expiry = dispatcher.claim_next(as_of=first.deadline_at)
    assert after_expiry is None or after_expiry.job_id != first.job_id


def test_dispatch_and_receipt_sets_are_bounded_before_unbounded_growth(monkeypatch):
    plan = _plan()
    job = _dispatch(plan).ready_jobs[0]
    receipt = acquisition.make_worker_acquisition_receipt(
        job,
        outcome="succeeded",
        completed_at=job.scheduled_for,
        artifact_receipt_sha256=ARTIFACT_SHA,
    )
    monkeypatch.setattr(acquisition, "MAX_RECEIPTS", 2)
    synthetic = []
    for digit in ("1", "2"):
        value = replace(
            receipt,
            job_id="acquisition:" + digit * 64,
            receipt_sha256="0" * 64,
        )
        payload = value.as_dict()
        payload.pop("receipt_sha256")
        synthetic.append(replace(value, receipt_sha256=acquisition._sha256(payload)))
    unique = (receipt, *synthetic)
    # Capacity is checked before parsing a third record into the index.
    with pytest.raises(acquisition.AcquisitionCapacityError, match="receiptset"):
        _dispatch(plan, receipts=unique)


def test_module_is_unwired_network_free_and_contains_no_real_handler_dispatch():
    source_path = Path(acquisition.__file__)
    source = source_path.read_text(encoding="utf-8")
    assert "urllib" not in source
    assert "requests" not in source
    assert "import socket" not in source
    assert "PRODUCTION_JOB_HANDLERS" not in source
    assert "provider_capture_registry" not in source
    assert "autonomous_scheduler" not in source
    assert "service.py" not in source
    assert "custom_components" not in source
    assert "worker_acquisition_plan" not in Path(
        "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")
    assert "worker_acquisition_plan" not in Path(
        "storm_tracker_worker/provider_capture_registry.py"
    ).read_text(encoding="utf-8")


def test_public_api_has_no_network_or_handler_parameter():
    for function in (
        acquisition.build_worker_acquisition_plan,
        acquisition.build_worker_acquisition_dispatch,
        acquisition.make_worker_acquisition_receipt,
    ):
        parameters = set(inspect.signature(function).parameters)
        assert not parameters.intersection(
            {"url", "endpoint", "session", "client", "handler", "token", "api_key"}
        )
