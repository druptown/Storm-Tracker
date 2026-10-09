from __future__ import annotations

import hashlib
import json
from pathlib import Path

from storm_tracker_worker import openmeteo_native
from storm_tracker_worker.acquisition_activation import (
    ACTIVATION_RULES,
    ActivationStatus,
)
from storm_tracker_worker.acquisition_capture_recovery import RECOVERY_BINDINGS
from storm_tracker_worker.terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
)


REPOSITORY = Path(__file__).resolve().parents[1]
BLOCKER_PATH = REPOSITORY / "docs" / "WORKER_OPENMETEO_ACTIVATION_BLOCKER.json"
WORKLOAD_ID = "model.openmeteo.region"


def _blocker() -> dict[str, object]:
    raw = BLOCKER_PATH.read_bytes()
    assert len(raw) <= 16 * 1024
    decoded = json.loads(raw)
    assert type(decoded) is dict
    return decoded


def test_machine_readable_blocker_is_closed_bounded_and_private() -> None:
    blocker = _blocker()

    assert blocker["contract"] == "stv3-openmeteo-atomic-activation-blocker-v1"
    assert blocker["decision"] == "blocked_no_partial_activation"
    assert blocker["failed_gate"] == {
        "gate_id": "fixed_attributable_upstream_model_source_per_region",
        "reason_code": (
            "current_global_json_profile_has_no_fail_closed_fixed_model_authority"
        ),
        "requirements_not_proven": [
            "one_fixed_upstream_model_id_for_every_authorized_coordinate",
            "model_specific_support_for_every_requested_variable",
            "response_bound_proof_of_the_selected_model_id",
            "resolved_upstream_model_source_credit_in_query_and_native_receipt",
        ],
    }
    sources = blocker["official_sources"]
    assert type(sources) is list and len(sources) == 4
    assert all(
        type(source) is dict
        and str(source["url"]).startswith("https://")
        and "?" not in str(source["url"])
        for source in sources
    )
    serialized = json.dumps(blocker, sort_keys=True).lower()
    assert "192.168." not in serialized
    assert "private_" not in serialized
    assert "aqt:" not in serialized
    assert "acq:" not in serialized
    assert '"latitude":' not in serialized
    assert '"longitude":' not in serialized


def test_historical_blocker_hashes_are_explicitly_superseded_by_raw_capture() -> None:
    blocker = _blocker()
    observed = blocker["observed_component_sha256"]
    assert type(observed) is dict and len(observed) == 11

    changed: list[str] = []
    for relative_path, expected_sha256 in observed.items():
        assert type(relative_path) is str
        assert type(expected_sha256) is str and len(expected_sha256) == 64
        component = (REPOSITORY / relative_path).resolve(strict=True)
        component.relative_to(REPOSITORY.resolve(strict=True))
        if hashlib.sha256(component.read_bytes()).hexdigest() != expected_sha256:
            changed.append(relative_path)
    assert "storm_tracker_worker/acquisition_activation.py" in changed
    assert "storm_tracker_worker/acquisition_capture_recovery.py" in changed


def test_openmeteo_is_raw_only_without_claiming_the_failed_model_gate() -> None:
    blocker = _blocker()
    preserved = blocker["preserved_runtime_state"]
    rule = ACTIVATION_RULES[WORKLOAD_ID]
    active = {
        workload_id
        for workload_id, candidate in ACTIVATION_RULES.items()
        if candidate.status is ActivationStatus.ACTIVE_SHADOW_RAW
    }

    assert preserved["active_shadow_raw_workloads"] == 11
    assert preserved["openmeteo_executable"] is False
    assert preserved["recovery_bindings"] == 11
    assert rule.status is ActivationStatus.ACTIVE_SHADOW_RAW
    assert rule.executable is True
    assert rule.raw_capture_service_only is True
    assert rule.reason_code == (
        "exact_raw_shadow_capture_audited_unverified_semantics"
    )
    assert rule.handler_id == "openmeteo_forecast_unverified_exact_raw_v1"
    assert WORKLOAD_ID in {
        binding.workload_id for binding in RECOVERY_BINDINGS.values()
    }
    assert WORKLOAD_ID in TERMINAL_RAW_CONSUMER_CATALOG
    assert TERMINAL_RAW_CONSUMER_CATALOG[
        WORKLOAD_ID
    ].consumer_kind == "raw_manifest_audit"
    assert set(TERMINAL_RAW_CONSUMER_CATALOG) == active
    assert preserved["partial_activation_performed"] is False
    assert preserved["service_main_ha_mqtt_wiring_changed"] is False


def test_current_native_contract_still_exposes_the_failed_model_gate() -> None:
    blocker = _blocker()
    policy = json.loads(openmeteo_native.OPENMETEO_ATTRIBUTION_POLICY_CANONICAL)
    params = openmeteo_native._request_parameters((
        {
            "latitude": 50.0,
            "longitude": 4.0,
            "query_key": "aqt:aaaaaaaa:z:aaaaaaaa:z:aaaaaaaa:z:aaaaaaaa:z:aaaaaaaa:z:aaaa",
            "query_kind": "local",
        },
    ))

    assert "models" not in params
    assert openmeteo_native.MINUTELY_15[4] == "lightning_potential"
    assert policy["upstream_model_credit_resolution_required"] is True
    assert any(
        finding["evidence_id"] == "json_response_does_not_prove_model"
        for finding in blocker["findings"]
    )
