"""Fail-closed proofs for the inert ground/visual exact-raw batch."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import MappingProxyType

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_capture_recovery as recovery
from storm_tracker_worker import acquisition_handler_child as child
from storm_tracker_worker import acquisition_location_shadow as shadow
from storm_tracker_worker import acquisition_query_authority as authority
from storm_tracker_worker import rainviewer_native, synoptic_native
from storm_tracker_worker import desired_state as desired
from storm_tracker_worker import worker_acquisition_plan as acquisition
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactLimits,
    RawArtifactStore,
)
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)


NOW = 1_800_000_010.0
ORIGIN_JOB = "acquisition:" + "a" * 64


def _target(
    target_id: str = "zone.home",
    *,
    latitude: float = 51.04,
    longitude: float = 4.48,
    kind: str = "home",
    observation_radius_km: float = 10.0,
    accuracy_m: float | None = None,
    position_observed_at: float = 1_800_000_000.0,
) -> dict:
    return {
        "available": True,
        "country_code": "US" if longitude < -20.0 else "BE",
        "display_name": "must-never-leave-parent",
        "kind": kind,
        "latitude": latitude,
        "location_accuracy_m": accuracy_m,
        "longitude": longitude,
        "observation_radius_km": observation_radius_km,
        "position_observed_at": position_observed_at,
        "target_id": target_id,
    }


def _state(
    target: dict | None = None,
    *,
    ground_radius_km: float = 20.0,
) -> desired.DesiredState:
    return desired.parse_desired_state({
        "authority_epoch": 17,
        "contract": desired.CONTRACT,
        "generation": 9,
        "issued_at": NOW,
        "previous_state_sha256": "1" * 64,
        "producer_id": "location-shadow-test",
        "settings": {
            "ground_radius_km": ground_radius_km,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [target or _target()],
    })


def _plan_job(
    state: desired.DesiredState,
    workload_id: str,
) -> tuple[acquisition.AcquisitionPlan, acquisition.AcquisitionJob]:
    plan = acquisition.build_worker_acquisition_plan(state)
    jobs = tuple(
        item
        for item in acquisition.build_worker_acquisition_slot_jobs(
            plan, as_of=1_800_000_000.0
        )
        if item.workload_id == workload_id
    )
    assert len(jobs) == 1
    return plan, jobs[0]


def _state_for(workload_id: str) -> desired.DesiredState:
    if workload_id == "ground.synoptic.region":
        return _state(_target(latitude=40.7128, longitude=-74.006))
    return _state()


def _query(
    state: desired.DesiredState,
    plan: acquisition.AcquisitionPlan,
    job: acquisition.AcquisitionJob,
) -> authority.AcquisitionQueryAuthority:
    return authority.build_acquisition_query_authority(
        state, plan, job, evaluated_at=NOW
    )


def _limits() -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=20 * 1024 * 1024,
        max_total_bytes=64 * 1024 * 1024,
        max_object_entries=64,
        max_staging_entries=8,
        max_receipt_entries=128,
        max_capture_entries=64,
        max_capture_members=32,
        max_catalog_bytes=16 * 1024 * 1024,
    )


def _resolver(workload_id: str) -> activation.MappingSecretResolver:
    values = {
        "ground.synoptic.region": {
            "worker_secret.synoptic_api_token": "present",
        },
    }.get(workload_id, {})
    return activation.MappingSecretResolver(values)


def _guarded_request(
    tmp_path: Path,
    workload_id: str,
    *,
    state: desired.DesiredState | None = None,
) -> tuple[dict, desired.DesiredState, acquisition.AcquisitionJob]:
    state = state or _state_for(workload_id)
    plan, job = _plan_job(state, workload_id)
    query = _query(state, plan, job)
    raw_root = (tmp_path / workload_id).resolve()
    if not raw_root.exists():
        RawArtifactStore(raw_root, limits=_limits()).initialize()
    request = shadow.build_guarded_shadow_child_request(
        state,
        plan,
        job,
        query_authority=query,
        evaluated_at=NOW,
        resolver=_resolver(workload_id),
        raw_store_root=raw_root,
        raw_store_limits=_limits(),
    )
    return request, state, job


def _activate_for_contract_test(monkeypatch, workload_id: str):
    rules = dict(activation.LOCATION_SHADOW_CANDIDATE_RULES)
    rules[workload_id] = replace(
        rules[workload_id],
        status=activation.ActivationStatus.ACTIVE_SHADOW_RAW,
        reason_code="test_only_dormant_contract_execution",
    )
    monkeypatch.setattr(
        activation,
        "LOCATION_SHADOW_CANDIDATE_RULES",
        MappingProxyType(rules),
    )
    return rules[workload_id]


def test_candidate_activation_child_recovery_contracts_are_exact_and_inert():
    assert len(child._HANDLERS) == len(recovery.RECOVERY_BINDING_CATALOG)  # noqa: SLF001
    recovery.validate_dormant_recovery_bindings(
        recovery.LOCATION_SHADOW_RECOVERY_BINDINGS,
        activation.LOCATION_SHADOW_CANDIDATE_RULES,
    )
    assert recovery.LOCATION_SHADOW_RECOVERY_BINDINGS[
        "rainviewer_visual_region_exact_raw_v1"
    ].required_roles == rainviewer_native.RAINVIEWER_CANDIDATE_REQUIRED_ROLES
    for workload_id, rule in activation.LOCATION_SHADOW_CANDIDATE_RULES.items():
        spec = child._HANDLERS[rule.handler_id]  # noqa: SLF001
        binding = recovery.RECOVERY_BINDING_CATALOG[rule.handler_id]
        assert spec.workload_id == binding.workload_id == workload_id
        assert spec.ledger_provider == binding.ledger_provider == rule.provider_id
        assert spec.native_provider == binding.raw_provider
        assert spec.capture_product == binding.capture_product
        assert spec.capture_fetch_suffix == binding.capture_suffix
        assert tuple(item.role for item in spec.roles) == rule.required_roles
        assert binding.required_roles == rule.required_roles
        assert spec.maximum_transport_bytes == binding.maximum_transport_bytes
        assert binding.maximum_transport_bytes == rule.maximum_transport_bytes
        assert len(spec.roles) == binding.maximum_artifacts == rule.maximum_artifacts
        assert binding.job_identity_kind == "safe_native_alias_v1"
        assert child._recovery_binding(spec) == binding  # noqa: SLF001
        live_rule = activation.ACTIVATION_RULES[workload_id]
        assert live_rule.executable
        assert live_rule.raw_capture_service_only
        assert live_rule.handler_id == rule.handler_id

    netatmo = activation.LOCATION_SHADOW_CANDIDATE_RULES["ground.netatmo.region"]
    assert not netatmo.executable
    assert netatmo.reason_code == "worker_oauth_access_token_authority_unavailable"
    assert not any(
        value.executable
        for value in activation.LOCATION_SHADOW_CANDIDATE_RULES.values()
    )
    assert activation.LOCATION_SHADOW_CANDIDATE_RULES[
        "ground.synoptic.region"
    ].reason_code == "provider_raw_retention_terms_approval_required"
    assert activation.LOCATION_SHADOW_CANDIDATE_RULES[
        "radar.rainviewer.visual"
    ].reason_code == "api_rights_attribution_cache_approval_required"

    drifted = dict(activation.LOCATION_SHADOW_CANDIDATE_RULES)
    drifted["ground.synoptic.region"] = replace(
        drifted["ground.synoptic.region"], required_roles=("wrong_role",)
    )
    with pytest.raises(RuntimeError, match="drifted"):
        recovery.validate_dormant_recovery_bindings(
            recovery.LOCATION_SHADOW_RECOVERY_BINDINGS,
            drifted,
        )

    for module_path in (
        Path(shadow.__file__),
        Path(child.__file__),
        Path(recovery.__file__),
    ):
        text = module_path.read_text(encoding="utf-8")
        assert "custom_components" not in text
        assert "mqtt" not in text.lower()


def test_activation_preserves_every_catalog_block_reason_verbatim():
    rows = {
        row["workload_id"]: row
        for row in acquisition.acquisition_catalog_snapshot()["workloads"]
    }
    for workload_id, rule in activation.ACTIVATION_RULES.items():
        catalog_reason = rows[workload_id].get("blocked_reason")
        if not rule.executable and catalog_reason:
            assert rule.reason_code == catalog_reason


def test_safe_native_alias_is_pure_restart_stable_and_rejects_caller_aliases():
    jobs = [f"acquisition:{value:064x}" for value in range(1, 257)]
    aliases = [authority.safe_native_job_id(value) for value in jobs]
    assert len(set(aliases)) == len(aliases)
    assert all(alias.startswith("acq:") and ":z:" in alias for alias in aliases)
    assert aliases == [authority.safe_native_job_id(value) for value in jobs]
    for invalid in (
        aliases[0],
        "acquisition:" + "A" * 64,
        "acquisition:" + "0" * 63,
        "0123456789abcdef0123456789abcdef",
        "acquisition:0123456789abcdef0123456789abcdef",
    ):
        with pytest.raises(ValueError):
            authority.safe_native_job_id(invalid)


def test_location_accuracy_is_included_in_ground_and_visual_coverage():
    for workload_id, field in (
        ("ground.synoptic.region", "ground_required_radius_km"),
        ("radar.rainviewer.visual", "radar_horizon_km"),
    ):
        location = (
            {"latitude": 40.7128, "longitude": -74.006}
            if workload_id == "ground.synoptic.region"
            else {"latitude": 51.04, "longitude": 4.48}
        )
        base = _state(_target(kind="person", accuracy_m=0.0, **location))
        uncertain = _state(_target(kind="person", accuracy_m=5_000.0, **location))
        base_plan, base_job = _plan_job(base, workload_id)
        uncertain_plan, uncertain_job = _plan_job(uncertain, workload_id)
        base_value = getattr(
            _query(base, base_plan, base_job).region_queries[0], field
        )
        uncertain_value = getattr(
            _query(uncertain, uncertain_plan, uncertain_job).region_queries[0], field
        )
        assert uncertain_value >= base_value + 5.0


def test_dynamic_target_without_accuracy_fails_closed():
    state = _state(_target(
        kind="person",
        accuracy_m=None,
        latitude=40.7128,
        longitude=-74.006,
    ))
    plan, job = _plan_job(state, "ground.synoptic.region")
    with pytest.raises(
        authority.AcquisitionQueryAuthorityError,
        match="location_accuracy_m",
    ):
        _query(state, plan, job)


def test_synoptic_five_decimal_wire_center_has_outward_radius_compensation():
    state = _state(_target(latitude=40.1234567, longitude=-74.7654321))
    plan, job = _plan_job(state, "ground.synoptic.region")
    grant = _query(state, plan, job)
    region = grant.region_queries[0]
    payload = grant.synoptic_payload()
    center_shift = authority._distance_km(  # noqa: SLF001
        region.center_latitude,
        region.center_longitude,
        float(f"{payload['latitude']:.5f}"),
        float(f"{payload['longitude']:.5f}"),
    )
    assert payload["latitude"] == float(f"{region.center_latitude:.5f}")
    assert payload["longitude"] == float(f"{region.center_longitude:.5f}")
    assert payload["radius_km"] >= region.ground_required_radius_km + center_shift


@pytest.mark.parametrize(
    "workload_id",
    (
        "ground.synoptic.region",
        "radar.rainviewer.visual",
    ),
)
def test_child_rechecks_guard_then_returns_only_bounded_hash_evidence(
    monkeypatch, tmp_path, workload_id
):
    _activate_for_contract_test(monkeypatch, workload_id)
    request, _state_value, job = _guarded_request(tmp_path, workload_id)

    def materialize(
        spec,
        *,
        job_id,
        store,
        handler_payload=None,
        execution_deadline_at=None,
    ):
        del handler_payload, execution_deadline_at
        native_id = authority.safe_native_job_id(job_id)
        sources = {}
        receipt_ids = {}
        for index, role in enumerate(spec.roles, start=1):
            suffix = role.fetch_suffix + ("1" if role.receipt_attempt_limit else "")
            receipt = store.store(
                provider=spec.native_provider,
                product=role.raw_product,
                fetch_id=f"{native_id}{suffix}",
                content_type=sorted(role.allowed_content_types)[0],
                received_at=NOW + index,
                upstream_id=f"provider-evidence-{index}",
                provenance={
                    "representation": NATIVE_REPRESENTATION,
                    "capture_stage": NATIVE_CAPTURE_STAGE,
                    "collector": "technical_child_test",
                    "collector_version": "technical-child-test-v1",
                    "test": "technical-child-only",
                },
                payload=f"{role.role}-{index}".encode("ascii"),
            )
            receipt_ids[role.role] = receipt.receipt_id
            source = {
                "role": role.role,
                f"{spec.source_hash_prefix}receipt_id": receipt.receipt_id,
                f"{spec.source_hash_prefix}content_sha256": receipt.content_sha256,
                f"{spec.source_hash_prefix}receipt_sha256": receipt.receipt_sha256,
            }
            if not spec.source_size_optional:
                source["size_bytes"] = receipt.size_bytes
            sources[role.role] = source
        capture = store.commit_capture(
            provider=spec.native_provider,
            product=spec.capture_product,
            fetch_id=f"{native_id}{spec.capture_fetch_suffix}",
            upstream_capture_id="provider-capture-evidence",
            product_timestamp=None,
            members=receipt_ids,
        )
        provenance = {
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_member_count": len(spec.roles),
            spec.source_field: sources,
        }
        result = {
            spec.decode_field: spec.decode_status,
            "capture_state": spec.capture_state,
            "weather_state": "unknown",
            "operational_admission_eligible": False,
            "raw_provenance": provenance,
        }
        if spec.result_provider is not None:
            result["provider"] = spec.result_provider
        return result

    monkeypatch.setattr(child, "_run_native", materialize)
    result = child.execute_shadow_capture(request, _clock=lambda: NOW + 1.0)
    encoded = json.dumps(result, sort_keys=True)
    assert result["status"] == "captured_shadow_raw"
    assert result["operational_admission_eligible"] is False
    for forbidden in (
        job.job_id,
        authority.safe_native_job_id(job.job_id),
        request["execution_guard"]["authority_sha256"],
        str(request["handler_payload"].get("latitude", "never")),
        str(request["handler_payload"].get("longitude", "never")),
        str(request["handler_payload"].get("query_key", "never")),
        "must-never-leave-parent",
    ):
        if forbidden is not None:
            assert forbidden not in encoded


def test_child_revalidates_dynamic_freshness_immediately_before_native_io(
    monkeypatch, tmp_path
):
    _activate_for_contract_test(monkeypatch, "ground.synoptic.region")
    state = _state(_target(
        target_id="person.mobile", kind="person", accuracy_m=5.0,
        latitude=40.7128, longitude=-74.006,
        position_observed_at=1_799_999_200.0,
    ))
    request, _, _ = _guarded_request(
        tmp_path, "ground.synoptic.region", state=state
    )
    valid_until = request["execution_guard"]["dynamic_position_valid_until"]
    calls = iter((NOW + 1.0, valid_until + 0.001))
    invoked = False

    def must_not_run(*args, **kwargs):
        nonlocal invoked
        invoked = True
        raise AssertionError("network boundary was reached")

    monkeypatch.setattr(child, "_run_native", must_not_run)
    with pytest.raises(ValueError, match="not fresh"):
        child.execute_shadow_capture(request, _clock=lambda: next(calls))
    assert not invoked


def test_child_rejects_rechecksummed_native_alias_forgery(monkeypatch, tmp_path):
    _activate_for_contract_test(monkeypatch, "ground.synoptic.region")
    request, _, _ = _guarded_request(tmp_path, "ground.synoptic.region")
    forged = dict(request)
    guard = dict(request["execution_guard"])
    guard["native_job_id_sha256"] = "f" * 64
    body = {key: value for key, value in guard.items() if key != "guard_sha256"}
    guard["guard_sha256"] = hashlib.sha256(json.dumps(
        body, allow_nan=False, ensure_ascii=False,
        separators=(",", ":"), sort_keys=True,
    ).encode()).hexdigest()
    forged["execution_guard"] = guard
    monkeypatch.setattr(
        child, "_run_native", lambda *args, **kwargs: pytest.fail("native I/O reached")
    )
    with pytest.raises(ValueError, match="native identity drifted"):
        child.execute_shadow_capture(forged, _clock=lambda: NOW + 1.0)


def test_large_radius_tile_and_dateline_require_explicit_fanout(tmp_path):
    # Synoptic cannot silently clip a large authorised ground circle.
    state = _state(
        _target(latitude=40.7128, longitude=-74.006),
        ground_radius_km=60.0,
    )
    plan, job = _plan_job(state, "ground.synoptic.region")
    with pytest.raises(shadow.LocationShadowBlockedError) as synoptic_error:
        shadow.build_guarded_shadow_child_request(
            state, plan, job,
            query_authority=_query(state, plan, job),
            evaluated_at=NOW,
            resolver=_resolver("ground.synoptic.region"),
            raw_store_root=(tmp_path / "synoptic").resolve(),
            raw_store_limits=_limits(),
        )
    assert synoptic_error.value.reason_code == "provider_radius_fanout_required"

    # RainViewer may not pretend that one 3x3 tile matrix covers a huge target.
    rv_state = _state(_target(
        observation_radius_km=1_000.0,
        accuracy_m=1_000.0,
    ))
    rv_plan, rv_job = _plan_job(rv_state, "radar.rainviewer.visual")
    with pytest.raises(shadow.LocationShadowBlockedError) as rv_error:
        shadow.build_guarded_shadow_child_request(
            rv_state, rv_plan, rv_job,
            query_authority=_query(rv_state, rv_plan, rv_job),
            evaluated_at=NOW,
            resolver=_resolver("radar.rainviewer.visual"),
            raw_store_root=(tmp_path / "rainviewer").resolve(),
            raw_store_limits=_limits(),
        )
    assert rv_error.value.reason_code == "provider_tile_fanout_required"

    with pytest.raises(authority.AcquisitionQueryCoverageError, match="datumgrens"):
        authority._netatmo_spherical_bbox(0.0, 179.99, 20.0)  # noqa: SLF001
    with pytest.raises(authority.AcquisitionQueryCoverageError, match="pool"):
        authority._netatmo_spherical_bbox(89.99, 0.0, 20.0)  # noqa: SLF001


def _profile(binding: recovery.RawRecoveryBinding) -> RawCapacityProfile:
    return RawCapacityProfile(
        global_max_bytes=64 * 1024 * 1024,
        global_max_entries=64,
        providers=(ProviderCapacityLimit(
            binding.ledger_provider, 64 * 1024 * 1024, 64
        ),),
        max_reservations=16,
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )


def _materialize_exact_provider(
    store: RawArtifactStore,
    binding: recovery.RawRecoveryBinding,
    *,
    origin_job_id: str,
) -> None:
    native_id = authority.safe_native_job_id(origin_job_id)
    if binding.capture_provenance_kind == "synoptic_v5":
        payload = {
            "latitude": 40.7128,
            "longitude": -74.006,
            "query_key": "aqr:safe:z:recovery",
            "query_kind": "local",
            "radius_km": 20.0,
        }
        query = synoptic_native._validate_query_payload(payload)  # noqa: SLF001
        context = synoptic_native._query_context(query)  # noqa: SLF001
        exact = b'{"SUMMARY":{"RESPONSE_CODE":1}}'
        content = hashlib.sha256(exact).hexdigest()
        attempt = 3
        receipt = store.store(
            provider="synoptic",
            product=synoptic_native.SYNOPTIC_JSON_PRODUCT,
            fetch_id=f"{native_id}:response_json:attempt:{attempt}",
            content_type="application/json",
            received_at=NOW,
            upstream_id=synoptic_native._expected_upstream_id(  # noqa: SLF001
                context, content, request_attempt=attempt
            ),
            provenance=synoptic_native._native_provenance(  # noqa: SLF001
                context, request_attempt=attempt
            ),
            payload=exact,
        )
        query_sha = synoptic_native._query_context_sha256(context)  # noqa: SLF001
        store.commit_capture(
            provider="synoptic",
            product=synoptic_native.SYNOPTIC_CAPTURE_PRODUCT,
            fetch_id=f"{native_id}:capture",
            upstream_capture_id=(
                f"{synoptic_native.SYNOPTIC_CAPTURE_PRODUCT}"
                f"|query_sha256={query_sha}|content_sha256={content}"
            ),
            product_timestamp=None,
            members={"response_json": receipt.receipt_id},
        )
        return

    frame_timestamp = 1_800_000_000.0
    frame_path = "/v2/radar/1800000000"
    latitude, longitude, horizon = 51.04, 4.48, 10.0
    center_x, center_y = rainviewer_native._center_tile(latitude, longitude)  # noqa: SLF001
    identity = type("Identity", (), {
        "frame_path": frame_path,
        "frame_generation_timestamp": frame_timestamp,
    })()
    receipts = {}
    manifest = b'{"generated":1800000000}'
    manifest_receipt = store.store(
        provider="rainviewer",
        product=rainviewer_native.RAINVIEWER_MANIFEST_PRODUCT,
        fetch_id=f"{native_id}:manifest",
        content_type="application/json",
        received_at=NOW,
        upstream_id=rainviewer_native._manifest_upstream_id(manifest),  # noqa: SLF001
        provenance=rainviewer_native._manifest_provenance(),  # noqa: SLF001
        payload=manifest,
    )
    receipts["manifest"] = manifest_receipt.receipt_id
    token = {"m1": -1, "0": 0, "p1": 1}
    for member in binding.members:
        if member.role == "manifest":
            continue
        kind, _, dx_token, _, dy_token = member.role.split("_")
        x = (center_x + token[dx_token]) % (2**rainviewer_native.RAINVIEWER_ZOOM)
        y = center_y + token[dy_token]
        exact = b"\x89PNG\r\n\x1a\n" + member.role.encode("ascii")
        receipt = store.store(
            provider="rainviewer",
            product=member.raw_product,
            fetch_id=f"{native_id}:{member.role}",
            content_type="image/png",
            received_at=NOW,
            upstream_id=rainviewer_native._tile_upstream_id(  # noqa: SLF001
                identity, kind=kind, x=x, y=y
            ),
            provenance=rainviewer_native._tile_provenance(  # noqa: SLF001
                identity,
                kind=kind,
                x=x,
                y=y,
                center_lat=latitude,
                center_lon=longitude,
                horizon_km=horizon,
            ),
            payload=exact,
        )
        receipts[member.role] = receipt.receipt_id
    store.commit_capture(
        provider="rainviewer",
        product=rainviewer_native.RAINVIEWER_CAPTURE_PRODUCT,
        fetch_id=f"{native_id}:capture",
        upstream_capture_id=rainviewer_native._upstream_capture_id(  # noqa: SLF001
            identity,
            center_x=center_x,
            center_y=center_y,
            lat=latitude,
            lon=longitude,
            horizon_km=horizon,
        ),
        product_timestamp=frame_timestamp,
        members=receipts,
    )


@pytest.mark.parametrize(
    "workload_id",
    (
        "ground.synoptic.region",
        "radar.rainviewer.visual",
    ),
)
def test_safe_alias_recovery_is_exact_and_restart_stable(tmp_path, workload_id):
    state = _state_for(workload_id)
    _plan, job = _plan_job(state, workload_id)
    rule = replace(
        activation.LOCATION_SHADOW_CANDIDATE_RULES[workload_id],
        status=activation.ActivationStatus.ACTIVE_SHADOW_RAW,
        reason_code="test_only_dormant_contract_execution",
    )
    binding = recovery.RECOVERY_BINDING_CATALOG[rule.handler_id]
    store_path = (tmp_path / "raw").resolve()
    ledger_path = (tmp_path / "capacity.sqlite3").resolve()
    store = RawArtifactStore(store_path, limits=_limits())
    store.initialize()
    profile = _profile(binding)
    ledger = RawCapacityLedger(ledger_path, profile=profile, clock=lambda: NOW)
    reservation_id = "acqcap:" + hashlib.sha256(job.job_id.encode()).hexdigest()
    ledger.reserve(
        reservation_id=reservation_id,
        provider=binding.ledger_provider,
        job_identity=job.job_id,
        requested_bytes=binding.maximum_transport_bytes,
        requested_entries=binding.maximum_artifacts,
    )
    _materialize_exact_provider(store, binding, origin_job_id=job.job_id)
    bindings = {binding.handler_id: binding}
    rules = {workload_id: rule}
    first = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
        recovery_bindings=bindings,
        activation_rules=rules,
    ).reconcile_job(job=job, rule=rule, reservation_id=reservation_id)
    assert first is not None and not first.already_committed

    restarted_store = RawArtifactStore(store_path, limits=_limits())
    restarted_store.initialize()
    restarted = recovery.CaptureCapacityReconciler(
        raw_store=restarted_store,
        capacity_ledger=RawCapacityLedger(
            ledger_path, profile=profile, clock=lambda: NOW
        ),
        recovery_bindings=bindings,
        activation_rules=rules,
    ).audit_existing()
    assert restarted.capture_count == 1
    assert restarted.already_committed_count == 1
    assert restarted.incomplete_receipt_count == 0


def test_recovery_rejects_safe_alias_from_a_different_origin(tmp_path):
    state = _state_for("ground.synoptic.region")
    _plan, job = _plan_job(state, "ground.synoptic.region")
    rule = replace(
        activation.LOCATION_SHADOW_CANDIDATE_RULES[job.workload_id],
        status=activation.ActivationStatus.ACTIVE_SHADOW_RAW,
        reason_code="test_only_dormant_contract_execution",
    )
    binding = recovery.RECOVERY_BINDING_CATALOG[rule.handler_id]
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()
    ledger = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=_profile(binding),
        clock=lambda: NOW,
    )
    ledger.reserve(
        reservation_id="acqcap:" + "e" * 64,
        provider=binding.ledger_provider,
        job_identity=job.job_id,
        requested_bytes=binding.maximum_transport_bytes,
        requested_entries=1,
    )
    other_origin = "acquisition:" + "f" * 64
    _materialize_exact_provider(store, binding, origin_job_id=other_origin)
    with pytest.raises(
        recovery.CaptureRecoveryIntegrityError,
        match="without a durable prior origin",
    ):
        recovery.CaptureCapacityReconciler(
            raw_store=store,
            capacity_ledger=ledger,
            recovery_bindings={binding.handler_id: binding},
            activation_rules={job.workload_id: rule},
        ).audit_existing()
