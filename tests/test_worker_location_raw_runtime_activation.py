"""Production raw-runtime proofs for the four audited location handlers."""
from __future__ import annotations

import hashlib
import json

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_location_shadow as shadow
from storm_tracker_worker import desired_state as desired
from storm_tracker_worker import worker_acquisition_plan as acquisition
from storm_tracker_worker.raw_artifacts import RawArtifactLimits


NOW = 1_800_000_010.0
SLOT = 1_800_000_000.0
LOCATION_WORKLOADS = (
    "ground.netatmo.region",
    "ground.synoptic.region",
    "radar.kmi.appv4_visual",
    "radar.rainviewer.visual",
)
KMI_PUBLIC_WORKLOADS = (
    "ground.kmi.aws_10min",
    "radar.kmi.public_wcs_unknown_quantity",
)
ALL_REQUIREMENTS = {
    "worker_secret.netatmo_access_token": "present",
    "worker_secret.synoptic_api_token": "present",
    "worker_authority.synoptic_raw_retention_terms_v1": "present",
    "worker_authority.rainviewer_cache_attribution_v1": "present",
    "worker_authority.kmi_appv4_raw_retention_v1": "present",
}


def _canonical(value: dict) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _state(
    *,
    country_code: str = "BE",
    latitude: float = 51.04,
    longitude: float = 4.48,
) -> desired.DesiredState:
    return desired.parse_desired_state({
        "authority_epoch": 21,
        "contract": desired.CONTRACT,
        "generation": 3,
        "issued_at": NOW,
        "previous_state_sha256": "4" * 64,
        "producer_id": "location-raw-runtime-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": country_code,
            "display_name": "must-never-leave-parent",
            "kind": "home",
            "latitude": latitude,
            "location_accuracy_m": None,
            "longitude": longitude,
            "observation_radius_km": 10.0,
            "position_observed_at": SLOT,
            "target_id": "zone.private_home",
        }],
    })


def _limits() -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=40 * 1024 * 1024,
        max_total_bytes=80 * 1024 * 1024,
        max_object_entries=64,
        max_staging_entries=8,
        max_receipt_entries=128,
        max_capture_entries=64,
        max_capture_members=32,
        max_catalog_bytes=16 * 1024 * 1024,
    )


def _all_source_contexts():
    belgium_state = _state()
    default = acquisition.build_worker_acquisition_plan(belgium_state)
    belgium_plan = acquisition.build_worker_acquisition_plan(
        belgium_state,
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )
    belgium_jobs = {
        item.workload_id: item
        for item in acquisition.build_worker_acquisition_slot_jobs(
            belgium_plan,
            as_of=SLOT,
        )
        if item.workload_id in LOCATION_WORKLOADS
    }
    assert belgium_plan.collector_scope == acquisition.COLLECTOR_SCOPE_ALL_SOURCES
    assert belgium_plan.plan_sha256 != default.plan_sha256
    assert "ground.synoptic.region" not in {
        item.workload_id for item in default.intents
    }
    assert set(belgium_jobs) == set(LOCATION_WORKLOADS) - {
        "ground.synoptic.region"
    }

    # Synoptic's documented coverage is North America.  Prove it separately
    # with an applicable location instead of weakening the production geo gate
    # merely to make a Belgian fixture dispatch it.
    north_america_state = _state(
        country_code="US",
        latitude=40.0,
        longitude=-105.0,
    )
    north_america_plan = acquisition.build_worker_acquisition_plan(
        north_america_state,
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )
    north_america_jobs = {
        item.workload_id: item
        for item in acquisition.build_worker_acquisition_slot_jobs(
            north_america_plan,
            as_of=SLOT,
        )
    }
    assert "ground.synoptic.region" in north_america_jobs

    return {
        workload_id: (
            north_america_state,
            north_america_plan,
            north_america_jobs[workload_id],
        ) if workload_id == "ground.synoptic.region" else (
            belgium_state,
            belgium_plan,
            belgium_jobs[workload_id],
        )
        for workload_id in LOCATION_WORKLOADS
    }


def test_promoted_rules_retain_exact_handler_specs_and_fail_closed_gates():
    contexts = _all_source_contexts()
    jobs = {key: value[2] for key, value in contexts.items()}
    for workload_id in LOCATION_WORKLOADS:
        rule = activation.ACTIVATION_RULES[workload_id]
        assert rule.executable
        assert rule.raw_capture_service_only
        assert workload_id not in activation.DEFAULT_SHADOW_WORKLOAD_IDS
        assert workload_id in activation.RAW_CAPTURE_WORKLOAD_IDS
        assert rule.process_target == activation.SHADOW_CAPTURE_TARGET
        assert rule.handler_id is not None
        assert rule.maximum_transport_bytes == jobs[workload_id].budget.max_transport_bytes
        assert rule.maximum_artifacts == jobs[workload_id].budget.max_artifacts
        assert rule.maximum_wall_seconds == jobs[workload_id].budget.max_wall_seconds

    no_authority = activation.MappingSecretResolver({
        key: value
        for key, value in ALL_REQUIREMENTS.items()
        if key.startswith("worker_secret.")
    })
    expected = {
        "ground.synoptic.region": (
            "provider_raw_retention_terms_approval_required"
        ),
        "radar.kmi.appv4_visual": (
            "kmi_appv4_raw_retention_contract_approval_required"
        ),
        "radar.rainviewer.visual": (
            "api_rights_attribution_cache_approval_required"
        ),
    }
    for workload_id, reason in expected.items():
        decision = activation.assess_acquisition_job(
            jobs[workload_id],
            resolver=no_authority,
        )
        assert not decision.executable
        assert decision.reason_code == reason

    netatmo = activation.assess_acquisition_job(
        jobs["ground.netatmo.region"],
        resolver=activation.MappingSecretResolver({}),
    )
    assert not netatmo.executable
    assert netatmo.reason_code == "secret_requirement_unavailable"


def test_environment_authority_requires_the_exact_explicit_value():
    jobs = {
        key: value[2] for key, value in _all_source_contexts().items()
    }
    variable = "STV3_RAINVIEWER_CACHE_ATTRIBUTION_AUTHORITY"
    wrong = activation.assess_acquisition_job(
        jobs["radar.rainviewer.visual"],
        resolver=activation.EnvironmentSecretResolver({variable: "true"}),
    )
    assert not wrong.executable
    assert wrong.reason_code == "api_rights_attribution_cache_approval_required"

    exact = activation.assess_acquisition_job(
        jobs["radar.rainviewer.visual"],
        resolver=activation.EnvironmentSecretResolver({
            variable: "worker_authority.rainviewer_cache_attribution_v1",
        }),
    )
    assert exact.executable

    kmi_variable = "STV3_KMI_APPV4_RAW_RETENTION_AUTHORITY"
    kmi_wrong = activation.assess_acquisition_job(
        jobs["radar.kmi.appv4_visual"],
        resolver=activation.EnvironmentSecretResolver({kmi_variable: "true"}),
    )
    assert not kmi_wrong.executable
    assert kmi_wrong.reason_code == (
        "kmi_appv4_raw_retention_contract_approval_required"
    )
    kmi_exact = activation.assess_acquisition_job(
        jobs["radar.kmi.appv4_visual"],
        resolver=activation.EnvironmentSecretResolver({
            kmi_variable: "worker_authority.kmi_appv4_raw_retention_v1",
        }),
    )
    assert kmi_exact.executable


def test_kmi_public_rules_are_raw_service_only_with_exact_payloads():
    state = _state()
    plan = acquisition.build_worker_acquisition_plan(
        state,
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )
    jobs = {
        item.workload_id: item
        for item in acquisition.build_worker_acquisition_slot_jobs(
            plan,
            as_of=SLOT,
        )
        if item.workload_id in KMI_PUBLIC_WORKLOADS
    }
    assert set(jobs) == set(KMI_PUBLIC_WORKLOADS)
    resolver = activation.MappingSecretResolver({})
    aws = activation.assess_acquisition_job(
        jobs["ground.kmi.aws_10min"],
        resolver=resolver,
    )
    radar = activation.assess_acquisition_job(
        jobs["radar.kmi.public_wcs_unknown_quantity"],
        resolver=resolver,
    )
    assert aws.executable and radar.executable
    assert aws.handler_payload == {
        "window_end": SLOT,
        "window_start": SLOT - 600.0,
    }
    assert radar.handler_payload == {}
    for workload_id in KMI_PUBLIC_WORKLOADS:
        rule = activation.ACTIVATION_RULES[workload_id]
        assert rule.raw_capture_service_only
        assert workload_id in activation.RAW_CAPTURE_WORKLOAD_IDS
        assert workload_id not in activation.DEFAULT_SHADOW_WORKLOAD_IDS


@pytest.mark.parametrize("workload_id", LOCATION_WORKLOADS)
def test_all_source_runtime_request_is_guarded_exact_and_identity_private(
    tmp_path,
    workload_id,
):
    state, plan, job = _all_source_contexts()[workload_id]
    resolver = activation.MappingSecretResolver(ALL_REQUIREMENTS)
    request = shadow.build_runtime_guarded_shadow_child_request(
        state,
        plan,
        job,
        evaluated_at=NOW,
        resolver=resolver,
        raw_store_root=(tmp_path / workload_id).resolve(),
        raw_store_limits=_limits(),
    )
    guard = request["execution_guard"]
    guard_body = {
        key: value for key, value in guard.items() if key != "guard_sha256"
    }
    assert guard["guard_sha256"] == hashlib.sha256(
        _canonical(guard_body)
    ).hexdigest()
    assert guard["payload_sha256"] == hashlib.sha256(
        _canonical(request["handler_payload"])
    ).hexdigest()
    assert request["workload_id"] == workload_id
    assert request["handler_id"] == (
        activation.ACTIVATION_RULES[workload_id].handler_id
    )
    assert len(guard["authority_sha256"]) == 64

    encoded = json.dumps(request, sort_keys=True)
    assert "zone.private_home" not in encoded
    assert "must-never-leave-parent" not in encoded
    diagnostic = activation.assess_acquisition_job(
        job,
        resolver=resolver,
    ).diagnostic()
    diagnostic_text = json.dumps(diagnostic, sort_keys=True)
    assert "zone.private_home" not in diagnostic_text
    assert "must-never-leave-parent" not in diagnostic_text

    payload = request["handler_payload"]
    if workload_id == "radar.rainviewer.visual":
        assert tuple(payload) == ("regions",)
        assert len(payload["regions"]) == 1
    elif workload_id == "radar.kmi.appv4_visual":
        assert set(payload) == {"contract", "latitude", "longitude", "query_key"}
        assert payload["contract"] == "stv3-public-source-region-query-v1"
    else:
        assert set(payload) == {
            "latitude",
            "longitude",
            "query_key",
            "query_kind",
            "radius_km",
        }


def test_runtime_helper_blocks_missing_secret_or_authority_before_request(tmp_path):
    contexts = _all_source_contexts()
    cases = (
        (
            "ground.netatmo.region",
            {},
            "secret_requirement_unavailable",
        ),
        (
            "ground.synoptic.region",
            {"worker_secret.synoptic_api_token": "present"},
            "provider_raw_retention_terms_approval_required",
        ),
        (
            "radar.rainviewer.visual",
            {},
            "api_rights_attribution_cache_approval_required",
        ),
        (
            "radar.kmi.appv4_visual",
            {},
            "kmi_appv4_raw_retention_contract_approval_required",
        ),
    )
    for workload_id, values, reason in cases:
        state, plan, job = contexts[workload_id]
        with pytest.raises(shadow.LocationShadowBlockedError) as caught:
            shadow.build_runtime_guarded_shadow_child_request(
                state,
                plan,
                job,
                evaluated_at=NOW,
                resolver=activation.MappingSecretResolver(values),
                raw_store_root=(tmp_path / workload_id).resolve(),
                raw_store_limits=_limits(),
            )
        assert caught.value.reason_code == reason
