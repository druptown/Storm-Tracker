"""Adversarial tests for the unwired acquisition/scheduler binding."""
from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re

import pytest

from storm_tracker_worker import acquisition_scheduler_bridge as bridge
from storm_tracker_worker import autonomous_scheduler as scheduler
from storm_tracker_worker import desired_state as desired
from storm_tracker_worker import worker_acquisition_plan as acquisition


NOW = 1_800_000_000.0
_SECRET_SHAPED_KEY = re.compile(
    r"(^|[_-])(password|passwd|secret|api[_-]?key|access[_-]?token|"
    r"refresh[_-]?token|authorization|cookie)([_-]|$)",
    re.IGNORECASE,
)


def _desired_payload() -> dict:
    return {
        "authority_epoch": 7,
        "contract": desired.CONTRACT,
        "generation": 1,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "producer_id": "ha-bridge-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Thuis",
            "kind": "home",
            "latitude": 51.04,
            "location_accuracy_m": None,
            "longitude": 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW - 60.0,
            "target_id": "zone.home",
        }],
    }


def _state() -> desired.DesiredState:
    return desired.parse_desired_state(_desired_payload())


def _plan() -> acquisition.AcquisitionPlan:
    return acquisition.build_worker_acquisition_plan(_state())


def _store(tmp_path: Path) -> desired.DesiredStateStore:
    store = desired.DesiredStateStore(tmp_path / "desired.sqlite3")
    authority_epoch = _desired_payload()["authority_epoch"]
    for epoch in range(1, authority_epoch + 1):
        store.claim_producer(
            "ha-bridge-test",
            f"bridge-claim-{epoch}",
            claimed_at=NOW - 130.0 + epoch,
        )
    store.apply(_desired_payload(), received_at=NOW - 59.0)
    return store


def _engine(
    tmp_path: Path,
    store: desired.DesiredStateStore,
    *,
    lease_seconds: float = 90.0,
) -> scheduler.AutonomousScheduler:
    return scheduler.AutonomousScheduler(
        tmp_path / "scheduler.sqlite3",
        desired_state_store=store,
        plan_builder=bridge.acquisition_scheduler_plan_builder,
        lease_seconds=lease_seconds,
    )


def _walk(value):
    yield value
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _walk(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _walk(item)


def test_complete_plan_translation_is_byte_deterministic_and_schedule_exact():
    plan = _plan()
    first = bridge.build_acquisition_schedule_intents(plan)
    second = bridge.build_acquisition_schedule_intents(plan)

    assert first == second
    assert len(first) == len(plan.intents) > 0
    assert tuple(item.intent_id for item in first) == tuple(
        sorted(item.intent_id for item in plan.intents)
    )
    source = {item.intent_id: item for item in plan.intents}
    for scheduled in first:
        intent = source[scheduled.intent_id]
        assert scheduled.job_type == bridge.JOB_TYPE
        assert (
            scheduled.cadence_seconds,
            scheduled.deadline_seconds,
            scheduled.priority,
        ) == (
            intent.cadence_seconds,
            intent.deadline_seconds,
            intent.priority,
        )
        payload = scheduled.payload
        assert payload["plan_sha256"] == plan.plan_sha256
        assert payload["snapshot_sha256"] == plan.snapshot_sha256
        assert payload["intent_sha256"] == intent.intent_sha256
        assert payload["workload_id"] == intent.workload_id
        assert payload["workload_class"] == intent.workload_class
        assert payload["provider_id"] == intent.provider_id
        assert payload["product_id"] == intent.product_id
        assert payload["scope_id"] == intent.scope_id
        assert payload["region_ids"] == list(intent.region_ids)
        assert payload["target_ids"] == list(intent.target_ids)
        assert payload["country_codes"] == list(intent.country_codes)
        assert payload["satellite_id"] == intent.satellite_id
        assert payload["budget"] == intent.budget.as_dict()
        assert payload["abstract_requirement_ids"] == list(
            intent.secret_requirement_ids
        )
        base = dict(payload)
        digest = base.pop("bridge_payload_sha256")
        assert digest == hashlib.sha256(
            json.dumps(
                base,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        ).hexdigest()


def test_scheduler_payloads_have_no_secret_shaped_fields_or_credential_values():
    intents = bridge.build_acquisition_schedule_intents(_plan())
    allowed_requirements = acquisition.KNOWN_SECRET_REQUIREMENT_IDS
    for intent in intents:
        for value in _walk(dict(intent.payload)):
            if isinstance(value, str) and _SECRET_SHAPED_KEY.search(value):
                assert value in allowed_requirements
        keys = [
            value
            for value in _walk(dict(intent.payload))
            if isinstance(value, str)
        ]
        assert not any(
            _SECRET_SHAPED_KEY.search(key)
            for key in intent.payload
        )
        assert "hunter2" not in keys
        assert "actual-token-value" not in keys


@pytest.mark.parametrize(
    ("constant", "limit"),
    (
        ("MAX_PLAN_INTENTS", 1),
        ("MAX_INTENT_PAYLOAD_BYTES", 100),
        ("MAX_PLAN_BYTES", 100),
    ),
)
def test_scheduler_capacity_fails_whole_plan_before_any_outbox_work(
    tmp_path,
    monkeypatch,
    constant,
    limit,
):
    store = _store(tmp_path)
    monkeypatch.setattr(scheduler, constant, limit)
    engine = _engine(tmp_path, store)

    with pytest.raises(scheduler.ScheduleIntentError, match="plan_builder_failed"):
        engine.reconcile(now=NOW)

    # The bridge throws while AutonomousScheduler is still building the plan;
    # no scheduler database or prefix of outbox rows can exist.
    assert not (tmp_path / "scheduler.sqlite3").exists()


def test_real_store_scheduler_restart_reconcile_claim_and_recovery_are_exact(
    tmp_path,
):
    store = _store(tmp_path)
    plan = acquisition.build_worker_acquisition_plan(store.load_active())
    first_engine = _engine(tmp_path, store, lease_seconds=10.0)
    first = first_engine.reconcile(now=NOW)
    assert first.inserted_count == len(plan.intents)

    reopened = _engine(tmp_path, store, lease_seconds=10.0)
    replay = reopened.reconcile(now=NOW + 1.0)
    assert replay.inserted_count == 0
    assert replay.idempotent_count == len(plan.intents)
    claimed = reopened.claim_next("bridge-serial", now=NOW + 2.0)
    assert claimed is not None
    bound = bridge.bind_claimed_acquisition_job(plan, claimed)
    assert bridge.validate_bound_acquisition_claim(plan, claimed, bound) is bound
    assert bound.acquisition_job.scheduled_for == claimed.scheduled_for
    assert bound.acquisition_job.deadline_at == claimed.deadline_at
    assert bound.binding_receipt.autoscheduler_job_id == claimed.job_id
    assert bound.binding_receipt.acquisition_job_id == bound.acquisition_job.job_id
    assert (
        bound.binding_receipt.acquisition_job_sha256
        == bound.acquisition_job.job_sha256
    )

    while_leased = _engine(tmp_path, store, lease_seconds=10.0)
    assert while_leased.claim_next("other-owner", now=NOW + 3.0) is None

    recovered = while_leased.reconcile(now=NOW + 13.0)
    assert recovered.recovered_lease_count == 1
    claimed_again = while_leased.claim_next("bridge-serial", now=NOW + 15.0)
    assert claimed_again is not None
    assert claimed_again.job_id == claimed.job_id
    assert claimed_again.payload == claimed.payload
    assert claimed_again.attempt_count == 2
    rebound = bridge.bind_claimed_acquisition_job(plan, claimed_again)
    assert rebound.acquisition_job == bound.acquisition_job
    assert rebound.binding_receipt.binding_sha256 != (
        bound.binding_receipt.binding_sha256
    )


def test_binding_receipt_is_payload_and_lease_token_free(tmp_path):
    store = _store(tmp_path)
    plan = acquisition.build_worker_acquisition_plan(store.load_active())
    engine = _engine(tmp_path, store)
    engine.reconcile(now=NOW)
    claimed = engine.claim_next("bridge-serial", now=NOW + 1.0)
    assert claimed is not None

    bound = bridge.bind_claimed_acquisition_job(plan, claimed)
    encoded = json.dumps(bound.binding_receipt.as_dict(), sort_keys=True)
    assert claimed.lease_token not in encoded
    assert json.dumps(claimed.payload, sort_keys=True) not in encoded
    assert "payload" not in bound.binding_receipt.as_dict()
    assert bound.binding_receipt.autoscheduler_payload_sha256 == hashlib.sha256(
        json.dumps(
            claimed.payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


def _claimed(tmp_path):
    store = _store(tmp_path)
    plan = acquisition.build_worker_acquisition_plan(store.load_active())
    engine = _engine(tmp_path, store)
    engine.reconcile(now=NOW)
    claimed = engine.claim_next("bridge-serial", now=NOW + 1.0)
    assert claimed is not None
    return plan, claimed


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: replace(value, job_id="autosched:" + "f" * 64),
        lambda value: replace(value, job_type="different_job"),
        lambda value: replace(value, priority=value.priority + 1),
        lambda value: replace(value, scheduled_for=value.scheduled_for + 1.0),
        lambda value: replace(value, deadline_at=value.deadline_at + 1.0),
        lambda value: replace(value, desired_state_sha256="f" * 64),
        lambda value: replace(
            value,
            payload={**value.payload, "plan_sha256": "f" * 64},
        ),
        lambda value: replace(
            value,
            payload={**value.payload, "target_ids": ["zone.forged"]},
        ),
        lambda value: replace(
            value,
            payload={
                **value.payload,
                "abstract_requirement_ids": ["worker_secret.forged"],
            },
        ),
        lambda value: replace(
            value,
            payload={
                **value.payload,
                "budget": {**value.payload["budget"], "max_artifacts": 999},
            },
        ),
    ),
)
def test_changed_job_payload_slot_deadline_targets_requirements_or_budget_fails_closed(
    tmp_path,
    mutation,
):
    plan, claimed = _claimed(tmp_path)
    with pytest.raises(bridge.AcquisitionSchedulerConflictError):
        bridge.bind_claimed_acquisition_job(plan, mutation(claimed))


def test_changed_claim_or_binding_fails_revalidation(tmp_path):
    plan, claimed = _claimed(tmp_path)
    bound = bridge.bind_claimed_acquisition_job(plan, claimed)

    changed_claim = replace(claimed, lease_token="f" * 64)
    with pytest.raises(bridge.AcquisitionSchedulerConflictError):
        bridge.validate_bound_acquisition_claim(plan, changed_claim, bound)

    changed_receipt = replace(
        bound.binding_receipt,
        acquisition_job_sha256="f" * 64,
    )
    with pytest.raises(bridge.AcquisitionSchedulerConflictError):
        bridge.validate_bound_acquisition_claim(
            plan,
            claimed,
            replace(bound, binding_receipt=changed_receipt),
        )

    changed_job = replace(bound.acquisition_job, target_ids=("zone.forged",))
    with pytest.raises(bridge.AcquisitionSchedulerConflictError):
        bridge.validate_bound_acquisition_claim(
            plan,
            claimed,
            replace(bound, acquisition_job=changed_job),
        )


def test_altered_plan_or_intent_conflict_is_rejected_before_translation():
    plan = _plan()
    with pytest.raises(bridge.AcquisitionSchedulerConflictError):
        bridge.build_acquisition_schedule_intents(
            replace(plan, plan_sha256="f" * 64)
        )
    changed_intent = replace(plan.intents[0], target_ids=())
    changed_plan = replace(
        plan,
        intents=(changed_intent,) + plan.intents[1:],
    )
    with pytest.raises(bridge.AcquisitionSchedulerConflictError):
        bridge.build_acquisition_schedule_intents(changed_plan)


def test_bridge_stays_pure_and_only_the_explicit_shadow_candidate_wires_it():
    path = Path(bridge.__file__)
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    forbidden = {
        "service",
        "config",
        "provider_capture_registry",
        "requests",
        "aiohttp",
        "httpx",
        "urllib",
        "socket",
        "paho",
    }
    assert not any(
        name in forbidden or any(name.startswith(f"{item}.") for item in forbidden)
        for name in imported
    )
    assert "AutonomousScheduler(" not in source
    assert "DesiredStateStore(" not in source
    assert "admission" not in source.lower()
    assert "forecast" not in source.lower()

    repository = path.parent.parent
    source_roots = (
        repository / "custom_components" / "storm_tracker_v3",
        repository / "storm_tracker_worker",
        repository / "tests",
    )
    references = []
    for source_root in source_roots:
        for candidate in source_root.rglob("*.py"):
            if candidate in {path, Path(__file__)}:
                continue
            if "acquisition_scheduler_bridge" in candidate.read_text(
                encoding="utf-8", errors="ignore"
            ):
                references.append(candidate)
    expected = {
        repository / "storm_tracker_worker" / "acquisition_runtime.py",
        repository / "storm_tracker_worker" / "acquisition_evidence_lifecycle.py",
        repository / "storm_tracker_worker" / "dwd_acquisition_completion_bridge.py",
        repository / "storm_tracker_worker" / "worker_cutover_manifest.py",
        repository / "tests" / "test_worker_acquisition_evidence_lifecycle.py",
        repository / "tests" / "test_worker_acquisition_shadow_runtime.py",
        repository / "tests" / "test_worker_cutover_manifest.py",
    }
    # These inert completion authorities consume only durable terminal receipts.
    # They call the bridge's pure reconstruction/digest helpers to re-prove those
    # receipts; none is imported by service.py or turns the bridge into live
    # scheduler, publication, fact, or admission wiring.
    expected.update(
        {
            repository
            / "storm_tracker_worker"
            / "knmi_rtcor_completion_consumer.py",
            repository
            / "storm_tracker_worker"
            / "metar_ground_completion_consumer.py",
            repository
            / "storm_tracker_worker"
            / "met_office_rate_completion_consumer.py",
            repository / "storm_tracker_worker" / "noaa_err_completion_consumer.py",
                repository
                / "storm_tracker_worker"
                / "raw_manifest_audit_completion_consumer.py",
                repository
                / "storm_tracker_worker"
                / "raw_capture_release_requirements.py",
                repository / "storm_tracker_worker" / "terminal_raw_completion_router.py",
            }
        )
    # These tests use the same pure schedule translation only to construct the
    # scheduler receipt fixtures consumed by the inert authorities above.
    expected.update(
        {
                repository / "tests" / "test_worker_noaa_err_completion_consumer.py",
                repository / "tests" / "test_worker_raw_capture_collector_scope.py",
                repository / "tests" / "test_worker_terminal_raw_completion_router.py",
        }
    )
    assert set(references) == expected
    assert repository / "storm_tracker_worker" / "service.py" not in references
    assert repository / "storm_tracker_worker" / "service.py" not in references
