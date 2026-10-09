from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
from types import SimpleNamespace

import pytest

from storm_tracker_worker import acquisition_query_authority as authority
from storm_tracker_worker import desired_state as desired
from storm_tracker_worker import netatmo_native
from storm_tracker_worker import rainviewer_native
from storm_tracker_worker import synoptic_native
from storm_tracker_worker import worker_acquisition_plan as acquisition
from storm_tracker_worker.raw_artifacts import RawArtifactStore


EVALUATED_AT = 1_800_000_010.0


def _target(
    target_id: str,
    latitude: float,
    longitude: float,
    *,
    kind: str = "home",
    radius_km: float = 250.0,
) -> dict:
    return {
        "available": True,
        "country_code": "US" if longitude < -20.0 else "BE",
        "display_name": f"Private display {target_id}",
        "kind": kind,
        "latitude": latitude,
        "location_accuracy_m": 7.5 if kind in {"person", "device_tracker"} else None,
        "longitude": longitude,
        "observation_radius_km": radius_km,
        "position_observed_at": 1_800_000_000.0,
        "target_id": target_id,
    }


def _state(
    targets: list[dict] | None = None,
    *,
    ground_radius_km: float = 20.0,
    sharing_km: float = 150.0,
) -> desired.DesiredState:
    if targets is None:
        targets = [_target("zone.home", 51.04, 4.48)]
    return desired.parse_desired_state(
        {
            "authority_epoch": 9,
            "contract": desired.CONTRACT,
            "generation": 4,
            "issued_at": 1_800_000_010.0,
            "previous_state_sha256": "1" * 64,
            "producer_id": "ha-query-authority-test",
            "settings": {
                "ground_radius_km": ground_radius_km,
                "lightning_source_mode": "auto",
                "model_guidance_enabled": True,
                "region_sharing_distance_km": sharing_km,
            },
            "targets": targets,
        }
    )


def _plan(state: desired.DesiredState) -> acquisition.AcquisitionPlan:
    return acquisition.build_worker_acquisition_plan(state)


def _job(
    plan: acquisition.AcquisitionPlan,
    workload_id: str,
    *,
    as_of: float = 1_800_000_000.0,
) -> acquisition.AcquisitionJob:
    matches = tuple(
        item
        for item in acquisition.build_worker_acquisition_slot_jobs(
            plan,
            as_of=as_of,
        )
        if item.workload_id == workload_id
    )
    assert len(matches) == 1
    return matches[0]


def _build(
    state: desired.DesiredState,
    workload_id: str,
) -> authority.AcquisitionQueryAuthority:
    plan = _plan(state)
    return authority.build_acquisition_query_authority(
        state,
        plan,
        _job(plan, workload_id),
        evaluated_at=EVALUATED_AT,
    )


def test_authority_is_byte_deterministic_and_omits_target_names_and_ids():
    targets = [
        _target("zone.secret_home", 51.04, 4.48),
        _target("person.secret_person", 51.05, 4.49, kind="person"),
    ]
    first_state = _state(targets)
    second_state = _state(list(reversed(targets)))
    first = _build(first_state, "model.openmeteo.region")
    second = _build(second_state, "model.openmeteo.region")

    assert first == second
    assert first.authority_sha256 == hashlib.sha256(first.canonical_bytes).hexdigest()
    assert json.loads(first.canonical_bytes)["method"] == authority.METHOD
    assert b"Private display" not in first.canonical_bytes
    assert b"zone.secret_home" not in first.canonical_bytes
    assert b"person.secret_person" not in first.canonical_bytes
    assert len(first.canonical_bytes) < authority.MAX_AUTHORITY_BYTES


def test_authority_rebuilds_plan_and_rejects_forged_state_plan_or_job():
    state = _state()
    plan = _plan(state)
    job = _job(plan, "ground.netatmo.region")

    with pytest.raises(authority.AcquisitionQueryAuthorityError, match="desired-state"):
        authority.build_acquisition_query_authority(
            replace(state, state_sha256="f" * 64),
            plan,
            job,
            evaluated_at=EVALUATED_AT,
        )

    forged_region = replace(plan.regions[0], center_latitude=50.0)
    with pytest.raises(authority.AcquisitionQueryAuthorityError, match="plan"):
        authority.build_acquisition_query_authority(
            state,
            replace(plan, regions=(forged_region,)),
            job,
            evaluated_at=EVALUATED_AT,
        )

    other_state = _state([_target("zone.other", 50.85, 4.35)])
    other_plan = _plan(other_state)
    other_job = _job(other_plan, "ground.netatmo.region")
    with pytest.raises(authority.AcquisitionQueryAuthorityError, match="job"):
        authority.build_acquisition_query_authority(
            state,
            plan,
            other_job,
            evaluated_at=EVALUATED_AT,
        )


def test_public_emitters_revalidate_dataclass_and_canonical_bytes():
    valid = _build(_state(), "ground.netatmo.region")
    forged_region = replace(valid.region_queries[0], center_latitude=0.0)
    forged = replace(valid, region_queries=(forged_region,))
    with pytest.raises(authority.AcquisitionQueryAuthorityError, match="checksum"):
        forged.netatmo_payload()
    with pytest.raises(authority.AcquisitionQueryAuthorityError):
        forged.diagnostics()


def test_dynamic_position_expires_at_actual_job_evaluation_time():
    state = _state(
        [_target("person.stale", 40.7128, -74.006, kind="person")]
    )
    plan = _plan(state)
    evaluated = 1_800_001_001.0
    job = _job(plan, "ground.synoptic.region", as_of=evaluated)
    with pytest.raises(authority.AcquisitionQueryAuthorityError, match="verlopen"):
        authority.build_acquisition_query_authority(
            state,
            plan,
            job,
            evaluated_at=evaluated,
        )


def test_fictitious_tracker_is_dynamic_and_never_labelled_local():
    state = _state([_target("test.houston", 29.7604, -95.3698, kind="test")])
    result = _build(state, "ground.synoptic.region")
    assert result.synoptic_payload()["query_kind"] == "roaming"


def test_single_target_ground_payloads_match_both_native_contracts():
    state = _state()
    plan = _plan(state)
    netatmo = authority.build_acquisition_query_authority(
        state,
        plan,
        _job(plan, "ground.netatmo.region"),
        evaluated_at=EVALUATED_AT,
    )
    synoptic_state = _state(
        [_target("zone.new_york", 40.7128, -74.0060)]
    )
    synoptic_plan = _plan(synoptic_state)
    synoptic = authority.build_acquisition_query_authority(
        synoptic_state,
        synoptic_plan,
        _job(synoptic_plan, "ground.synoptic.region"),
        evaluated_at=EVALUATED_AT,
    )

    netatmo_payload = netatmo.netatmo_payload()
    synoptic_payload = synoptic.synoptic_payload()
    assert netatmo_payload["radius_km"] == 20.0
    assert synoptic_payload["radius_km"] == 20.0
    assert netatmo_payload["query_kind"] == "local"
    assert synoptic_payload["query_kind"] == "local"
    assert re.search(r"[0-9a-f]{32}", netatmo_payload["query_key"]) is None
    assert re.search(r"[0-9a-f]{32}", synoptic_payload["query_key"]) is None

    parsed_netatmo = netatmo_native._validate_query_payload(
        netatmo_payload,
        "x" * 64,
    )
    parsed_synoptic = synoptic_native._validate_query_payload(synoptic_payload)
    assert parsed_netatmo.radius_km == 20.0
    assert parsed_synoptic.radius_km == 20.0


def test_roaming_target_produces_roaming_region_without_target_coordinates():
    state = _state(
        [_target("person.roaming", 40.7128, -74.0060, kind="device_tracker")]
    )
    result = _build(state, "ground.synoptic.region")
    payload = result.synoptic_payload()
    assert payload["query_kind"] == "roaming"
    assert result.target_queries == ()
    assert b"person.roaming" not in result.canonical_bytes
    assert b"Private display" not in result.canonical_bytes


def test_ground_query_covers_all_region_targets_or_fails_closed_for_provider():
    state = _state(
        [
            _target("zone.a", 40.70, -74.00),
            _target("zone.b", 40.80, -74.00),
        ],
        sharing_km=50.0,
    )
    plan = _plan(state)
    assert len(plan.regions) == 1
    netatmo = authority.build_acquisition_query_authority(
        state,
        plan,
        _job(plan, "ground.netatmo.region"),
        evaluated_at=EVALUATED_AT,
    )
    synoptic = authority.build_acquisition_query_authority(
        state,
        plan,
        _job(plan, "ground.synoptic.region"),
        evaluated_at=EVALUATED_AT,
    )
    required = netatmo.region_queries[0].ground_required_radius_km
    assert 25.0 < required < 27.0
    with pytest.raises(authority.AcquisitionQueryCoverageError, match="opsplitsing"):
        netatmo.netatmo_payload()
    assert synoptic.synoptic_payload()["radius_km"] == required


def test_rainviewer_payload_is_derived_only_from_exact_region_authority():
    state = _state([_target("zone.home", 51.04, 4.48, radius_km=250.0)])
    plan = _plan(state)
    job = _job(plan, "radar.rainviewer.visual")
    result = authority.build_acquisition_query_authority(
        state,
        plan,
        job,
        evaluated_at=EVALUATED_AT,
    )
    payload = result.rainviewer_payload()
    assert payload == {
        "regions": [
            {
                "center_lat": plan.regions[0].center_latitude,
                "center_lon": plan.regions[0].center_longitude,
                "horizon_km": 250.0,
            }
        ]
    }
    parsed = rainviewer_native._job_identity(
        SimpleNamespace(job_id=job.job_id, payload=payload)
    )
    assert parsed == (
        job.job_id,
        plan.regions[0].center_latitude,
        plan.regions[0].center_longitude,
        250.0,
    )


def test_rainviewer_out_of_contract_geometry_fails_closed():
    state = _state(
        [
            _target("zone.a", 0.0, 0.0, radius_km=1000.0),
            _target("zone.b", 0.0, 1.0, radius_km=1000.0),
        ],
        sharing_km=200.0,
    )
    result = _build(state, "radar.rainviewer.visual")
    assert result.region_queries[0].radar_horizon_km > 1000.0
    with pytest.raises(authority.AcquisitionQueryCoverageError, match="horizon"):
        result.rainviewer_payload()


def test_rainviewer_guaranteed_tile_coverage_is_checked_before_native_call():
    result = _build(
        _state([_target("zone.high_latitude", 80.0, 0.0, radius_km=250.0)]),
        "radar.rainviewer.visual",
    )
    with pytest.raises(authority.AcquisitionQueryCoverageError, match="3x3"):
        result.rainviewer_payload()


def test_netatmo_dateline_crossing_fails_before_native_call():
    result = _build(
        _state([_target("zone.dateline", 10.0, 179.99)]),
        "ground.netatmo.region",
    )
    with pytest.raises(authority.AcquisitionQueryCoverageError, match="datumgrens"):
        result.netatmo_payload()


def test_openmeteo_batch_contains_every_exact_target_without_plain_ids():
    state = _state(
        [
            _target("zone.home", 51.04, 4.48),
            _target("person.near", 51.05, 4.49, kind="person"),
        ]
    )
    result = _build(state, "model.openmeteo.region")
    batch = result.openmeteo_shadow_query_batch()
    assert batch["contract"] == authority.OPENMETEO_BATCH_CONTRACT
    assert len(batch["queries"]) == 2
    assert batch["operational_handler_compatible"] is False
    assert batch["target_resolution_required"] is True
    assert {item["query_kind"] for item in batch["queries"]} == {"local", "roaming"}
    encoded = json.dumps(batch, sort_keys=True)
    assert "zone.home" not in encoded
    assert "person.near" not in encoded
    resolved = result.resolve_target_ids(state)
    assert set(resolved) == {item.query_key for item in result.target_queries}
    assert set(resolved.values()) == {"person.near", "zone.home"}


def test_openmeteo_target_resolver_rejects_another_desired_state():
    state = _state()
    result = _build(state, "model.openmeteo.region")
    other = _state([_target("zone.other", 50.85, 4.35)])
    with pytest.raises(authority.AcquisitionQueryAuthorityError, match="andere"):
        result.resolve_target_ids(other)


def test_payload_method_cannot_be_reused_for_another_workload():
    result = _build(_state(), "ground.netatmo.region")
    with pytest.raises(authority.AcquisitionQueryAuthorityError, match="workload"):
        result.rainviewer_payload()
    with pytest.raises(authority.AcquisitionQueryAuthorityError, match="workload"):
        result.openmeteo_shadow_query_batch()


def test_diagnostics_are_bounded_and_coordinate_free():
    result = _build(
        _state([_target("person.private", 40.7128, -74.006, kind="person")]),
        "ground.synoptic.region",
    )
    diagnostics = result.diagnostics()
    encoded = json.dumps(diagnostics, sort_keys=True, separators=(",", ":"))
    assert len(encoded.encode()) < 2048
    assert "40.7128" not in encoded
    assert "-74.006" not in encoded
    assert "person.private" not in encoded
    assert diagnostics["region_query_count"] == 1
    assert diagnostics["target_query_count"] == 0


def test_closed_batch_builds_only_location_workloads_and_rebuilds_plan_once(
    monkeypatch,
):
    state = _state(
        [
            _target("zone.belgium", 51.04, 4.48),
            _target("person.new_york", 40.7128, -74.006, kind="person"),
        ],
        sharing_km=50.0,
    )
    plan = _plan(state)
    real_rebuild = authority.build_worker_acquisition_plan
    calls = []

    def rebuild(value):
        calls.append(value.state_sha256)
        return real_rebuild(value)

    monkeypatch.setattr(authority, "build_worker_acquisition_plan", rebuild)
    batch = authority.build_acquisition_query_authorities(
        state,
        plan,
        evaluated_at=EVALUATED_AT,
    )
    built = batch.authorities
    assert calls == [state.state_sha256]
    assert built
    assert batch.blocked == ()
    assert {item.workload_id for item in built}.issubset(
        {
            "ground.netatmo.region",
            "ground.synoptic.region",
            "model.openmeteo.region",
            "radar.rainviewer.visual",
        }
    )


def test_batch_validator_rejects_drop_reorder_duplicate_and_block_tamper():
    state = _state(
        [
            _target("zone.fresh", 51.04, 4.48),
            _target("person.stale", 40.7128, -74.006, kind="person"),
        ],
        sharing_km=50.0,
    )
    plan = _plan(state)
    batch = authority.build_acquisition_query_authorities(
        state,
        plan,
        evaluated_at=1_800_001_001.0,
    )
    assert len(batch.authorities) > 1
    assert batch.blocked
    assert authority.validate_acquisition_query_batch(batch) is batch

    variants = [
        replace(batch, authorities=batch.authorities[:-1]),
        replace(batch, authorities=tuple(reversed(batch.authorities))),
        replace(batch, authorities=batch.authorities + (batch.authorities[0],)),
        replace(
            batch,
            blocked=(replace(batch.blocked[0], reason_code="forged_reason"),)
            + batch.blocked[1:],
        ),
    ]
    for forged in variants:
        with pytest.raises(authority.AcquisitionQueryAuthorityError):
            authority.validate_acquisition_query_batch(forged)
        with pytest.raises(authority.AcquisitionQueryAuthorityError):
            forged.diagnostics()


def test_stale_roaming_region_is_blocked_without_suppressing_fresh_region():
    stale = _target("person.stale", 40.7128, -74.006, kind="person")
    fresh = _target("zone.fresh", 51.04, 4.48)
    state = _state([fresh, stale], sharing_km=50.0)
    plan = _plan(state)
    evaluated = 1_800_001_001.0
    batch = authority.build_acquisition_query_authorities(
        state,
        plan,
        evaluated_at=evaluated,
    )
    assert batch.authorities
    assert all(
        item.desired_state_sha256 == state.state_sha256
        for item in batch.authorities
    )
    assert {item.reason_code for item in batch.blocked} == {
        "dynamic_position_expired"
    }
    fresh_region_sha = next(
        item.region_sha256
        for item in plan.regions
        if item.target_ids == ("zone.fresh",)
    )
    assert any(
        any(query.source_region_sha256 == fresh_region_sha for query in item.region_queries)
        or any(
            query.target_ref_sha256
            == authority._target_reference(state.state_sha256, "zone.fresh")
            for query in item.target_queries
        )
        for item in batch.authorities
    )
    assert batch.diagnostics()["blocked_reason_counts"] == {
        "dynamic_position_expired": len(batch.blocked)
    }


def test_synoptic_wire_radius_never_shrinks_authorised_kilometres():
    for millimetres in range(1_000, 50_001):
        radius_km = millimetres / 1000.0
        query = synoptic_native._validate_query_payload(
            {
                "latitude": 40.7128,
                "longitude": -74.006,
                "query_key": "aqr:safe:radius:test",
                "query_kind": "local",
                "radius_km": radius_km,
            }
        )
        wire = synoptic_native._query_parameters(query)[0][1]
        wire_miles = float(wire.rsplit(",", 1)[1])
        assert wire_miles * 1.609344 >= radius_km


def test_rainviewer_authority_geometry_matches_native_matrix_exactly():
    for latitude in (-75.0, -45.0, 0.0, 45.0, 75.0):
        for longitude in (-179.0, -90.0, 0.0, 90.0, 179.0):
            center = authority._rainviewer_center_tile(latitude, longitude)
            native_center = rainviewer_native._center_tile(latitude, longitude)
            assert center == native_center
            assert authority._rainviewer_guaranteed_radius_km(
                latitude,
                longitude,
            ) == rainviewer_native._guaranteed_coverage_radius_km(
                lat=latitude,
                lon=longitude,
                center_x=native_center[0],
                center_y=native_center[1],
            )


def test_netatmo_authority_bbox_matches_native_wire_matrix_exactly():
    assert authority.EARTH_RADIUS_KM == netatmo_native.EARTH_RADIUS_KM
    credential = "fixture-credential-without-hex-identity"
    for latitude in (-75.0, -45.0, 0.0, 45.0, 75.0):
        for longitude in (-170.0, -30.0, 0.0, 30.0, 170.0):
            for radius_km in (1.0, 10.0, 20.0):
                expected = authority._netatmo_spherical_bbox(
                    latitude,
                    longitude,
                    radius_km,
                )
                native = netatmo_native._validate_query_payload(
                    {
                        "latitude": latitude,
                        "longitude": longitude,
                        "query_key": "aqr:geometry:z:matrix",
                        "query_kind": "local",
                        "radius_km": radius_km,
                    },
                    credential,
                )
                assert expected == (
                    native.lat_ne,
                    native.lon_ne,
                    native.lat_sw,
                    native.lon_sw,
                )


def test_safe_native_job_id_and_authority_payload_reach_full_synoptic_capture(
    monkeypatch,
    tmp_path,
):
    state = _state([_target("zone.new_york", 40.7128, -74.006)])
    plan = _plan(state)
    job = _job(plan, "ground.synoptic.region")
    result = authority.build_acquisition_query_authority(
        state,
        plan,
        job,
        evaluated_at=EVALUATED_AT,
    )
    raw = (Path(__file__).parent / "fixtures" / "synoptic_timeseries_ground_golden.json").read_bytes()
    monkeypatch.setenv(
        synoptic_native.SYNOPTIC_CREDENTIAL_ENV,
        "0123456789abcdef0123456789abcdef",
    )
    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *args, **kwargs: synoptic_native.SynopticHttpResponse(
            payload=raw,
            content_type="application/json",
            received_at=EVALUATED_AT,
        ),
    )
    capture = synoptic_native.capture_synoptic_timeseries(
        SimpleNamespace(job_id=result.native_job_id, payload=result.synoptic_payload()),
        raw_store=RawArtifactStore(tmp_path / "raw"),
    )
    assert capture["raw_provenance"]["raw_capture_id"]
    assert capture["operational_admission_eligible"] is False


def test_safe_native_job_id_cannot_reconstruct_synoptic_credential_identity():
    credential = "0123456789abcdef0123456789abcdef"
    state = _state([_target("zone.new_york", 40.7128, -74.006)])
    plan = _plan(state)
    job = _job(plan, "ground.synoptic.region")
    result = authority.build_acquisition_query_authority(
        state,
        plan,
        job,
        evaluated_at=EVALUATED_AT,
    )

    with pytest.raises(ValueError, match="credentialachtige identiteit"):
        synoptic_native._validate_job_id(credential)
    with pytest.raises(ValueError, match="credentialachtige identiteit"):
        synoptic_native._validate_job_id(f"acquisition:{credential}{credential}")

    assert credential not in result.native_job_id
    assert credential not in result.canonical_bytes.decode("utf-8")
    assert ":z:" in result.native_job_id
    assert not synoptic_native._contains_reconstructed_credential_identity(
        result.native_job_id
    )
    assert synoptic_native._validate_job_id(result.native_job_id) == result.native_job_id
    query_key = result.synoptic_payload()["query_key"]
    assert ":z:" in query_key
    assert not synoptic_native._contains_reconstructed_credential_identity(query_key)


def test_location_change_changes_every_downstream_authority_identity():
    first = _build(
        _state([_target("person.mobile", 51.04, 4.48, kind="person")]),
        "model.openmeteo.region",
    )
    second = _build(
        _state([_target("person.mobile", 51.041, 4.482, kind="person")]),
        "model.openmeteo.region",
    )
    assert first.desired_state_sha256 != second.desired_state_sha256
    assert first.plan_sha256 != second.plan_sha256
    assert first.job_sha256 != second.job_sha256
    assert first.authority_sha256 != second.authority_sha256
    assert first.target_queries[0].target_query_sha256 != second.target_queries[0].target_query_sha256


def test_module_is_inert_and_has_no_live_or_ha_imports():
    source = authority.__file__
    text = Path(source).read_text(encoding="utf-8")
    forbidden = (
        "custom_components",
        "service.py",
        "asyncio.create_task",
        "aiohttp",
        "paho",
        "mqtt",
        "requests",
        "urllib.request",
    )
    assert not any(token in text for token in forbidden)
