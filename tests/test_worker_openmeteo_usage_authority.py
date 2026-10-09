from __future__ import annotations

from contextlib import closing
from dataclasses import replace
import copy
import errno
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import threading

import pytest

from storm_tracker_worker import acquisition_query_authority as query_authority
from storm_tracker_worker import desired_state
from storm_tracker_worker import openmeteo_usage_authority as usage_module
from storm_tracker_worker import worker_acquisition_plan
from storm_tracker_worker.openmeteo_usage_authority import (
    DEFAULT_INTERNAL_LIMITS,
    FETCH_AUTHORIZATION_SECONDS,
    MAXIMUM_ROWS_BEFORE_TERMS_DUE,
    MINIMUM_MAIN_CATALOG_BYTES,
    MINIMUM_PHYSICAL_ENVELOPE_BYTES,
    OPENMETEO_CATALOG_CADENCE_SECONDS,
    OPENMETEO_CATALOG_FETCH_SLOTS_PER_UTC_DAY,
    OPENMETEO_PRICING_POLICY_CANONICAL,
    OPENMETEO_PRICING_POLICY_ID,
    OPENMETEO_PRICING_POLICY_SHA256,
    OPENMETEO_VARIABLE_COUNT,
    RETENTION_SECONDS,
    TERMS_REVIEW_DUE_AT,
    TERMS_REVIEWED_AT,
    USAGE_UNITS_PER_LOCATION,
    OpenMeteoUsageAuthority,
    OpenMeteoUsageClockRollback,
    OpenMeteoUsageConflict,
    OpenMeteoUsageIntegrityError,
    OpenMeteoUsageLimitExceeded,
    OpenMeteoUsageOwnerBusy,
    OpenMeteoUsageProfile,
    OpenMeteoUsageTermsExpired,
)


NOW = 1_786_234_500
CAPTURE_SHA = "c" * 64


def _target(index: int, *, seed: int) -> dict:
    return {
        "available": True,
        "country_code": "BE",
        "display_name": f"Private target {seed}-{index}",
        "kind": "home",
        "latitude": 50.0 + seed / 100_000 + index / 10_000,
        "location_accuracy_m": None,
        "longitude": 4.0 + seed / 100_000 + index / 10_000,
        "observation_radius_km": 250.0,
        "position_observed_at": float(NOW - 10),
        "target_id": f"zone.private_{seed}_{index}",
    }


def _authority(count: int = 1, *, seed: int = 0):
    state = desired_state.parse_desired_state({
        "authority_epoch": 9,
        "contract": desired_state.CONTRACT,
        "generation": seed + 1,
        "issued_at": float(NOW),
        "previous_state_sha256": hashlib.sha256(
            f"previous-{seed}".encode("ascii")
        ).hexdigest(),
        "producer_id": "openmeteo-usage-authority-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [_target(index, seed=seed) for index in range(count)],
    })
    plan = worker_acquisition_plan.build_worker_acquisition_plan(state)
    jobs = tuple(
        item
        for item in worker_acquisition_plan.build_worker_acquisition_slot_jobs(
            plan,
            as_of=float(NOW),
        )
        if item.workload_id == "model.openmeteo.region"
    )
    assert len(jobs) == 1
    return query_authority.build_acquisition_query_authority(
        state,
        plan,
        jobs[0],
        evaluated_at=float(NOW),
    )


def _database(tmp_path: Path, *, name: str = "usage.sqlite3") -> Path:
    return (tmp_path / name).resolve()


def _reserve(lease, authority):
    batch = authority.openmeteo_shadow_query_batch()
    return lease.reserve(
        authority=authority,
        expected_authority_sha256=authority.authority_sha256,
        query_batch=batch,
    )


def _begin(lease, reservation):
    return lease.begin_fetch(
        reservation_id=reservation.reservation_id,
        expected_state_sha256=reservation.state_sha256,
    )


def _spawn_hold(path: str, ready, release, result) -> None:
    try:
        authority = OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
        lease = authority.acquire()
        result.put(("held", lease.diagnostics()["owner_backend"]))
        ready.set()
        if not release.wait(20.0):
            result.put(("timeout", "release"))
        lease.release()
    except BaseException as error:
        result.put(("error", type(error).__name__, str(error)))
        ready.set()


def _spawn_attempt(path: str, result) -> None:
    try:
        authority = OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
        with authority.acquire():
            result.put(("acquired",))
    except OpenMeteoUsageOwnerBusy as error:
        result.put(("busy", str(error)))
    except BaseException as error:
        result.put(("error", type(error).__name__, str(error)))


def _spawn_reservation_crash(
    path: str,
    authority,
    batch: dict,
    fault_point: str,
) -> None:
    def crash(point: str) -> None:
        if point == fault_point:
            os._exit(73)

    sidecar = OpenMeteoUsageAuthority(
        path,
        clock=lambda: float(NOW),
        fault_injector=crash,
    )
    with sidecar.acquire() as lease:
        lease.reserve(
            authority=authority,
            expected_authority_sha256=authority.authority_sha256,
            query_batch=batch,
        )


def _spawn_begin_crash(
    path: str,
    reservation_id: str,
    expected_state_sha256: str,
    fault_point: str,
) -> None:
    def crash(point: str) -> None:
        if point == fault_point:
            os._exit(74)

    sidecar = OpenMeteoUsageAuthority(
        path,
        clock=lambda: float(NOW),
        fault_injector=crash,
    )
    with sidecar.acquire() as lease:
        lease.begin_fetch(
            reservation_id=reservation_id,
            expected_state_sha256=expected_state_sha256,
        )


def _close_queue(queue) -> None:
    queue.close()
    queue.join_thread()


def test_profile_and_pricing_policy_are_canonical_conservative_and_fixed():
    pricing = json.loads(OPENMETEO_PRICING_POLICY_CANONICAL)
    assert json.dumps(
        pricing,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii") == OPENMETEO_PRICING_POLICY_CANONICAL
    assert hashlib.sha256(OPENMETEO_PRICING_POLICY_CANONICAL).hexdigest() == (
        OPENMETEO_PRICING_POLICY_SHA256
    )
    assert OPENMETEO_PRICING_POLICY_ID == (
        "omp1:" + OPENMETEO_PRICING_POLICY_SHA256
    )
    assert pricing["terms_policy_id"] == (
        usage_module.OPENMETEO_ATTRIBUTION_POLICY_ID
    )
    assert pricing["source_url"] == "https://open-meteo.com/en/pricing"
    assert pricing["cost_formula"] == "ceil(variable_count/10)*query_count"
    assert OPENMETEO_VARIABLE_COUNT == 19
    assert USAGE_UNITS_PER_LOCATION == 2
    assert OPENMETEO_CATALOG_CADENCE_SECONDS == 900
    assert OPENMETEO_CATALOG_FETCH_SLOTS_PER_UTC_DAY == 96
    profile = usage_module.DEFAULT_USAGE_PROFILE.to_mapping()
    assert profile["catalog_cadence_seconds"] == 900
    assert profile["catalog_fetch_slots_per_utc_day"] == 96
    assert profile["acquisition_cadence_profile_sha256"] == (
        worker_acquisition_plan.CADENCE_PROFILE_SHA256
    )
    assert profile["acquisition_catalog_sha256"] == (
        worker_acquisition_plan.CATALOG_SHA256
    )
    assert profile["sustainable_locations_at_catalog_cadence"] == 5
    assert TERMS_REVIEW_DUE_AT == 1_788_825_600
    assert TERMS_REVIEWED_AT == 1_786_233_600
    assert MAXIMUM_ROWS_BEFORE_TERMS_DUE == 15_000
    assert MINIMUM_MAIN_CATALOG_BYTES == 34_914_304
    assert MINIMUM_PHYSICAL_ENVELOPE_BYTES == 46_552_406
    assert profile["maximum_rows_before_terms_due"] == 15_000
    assert profile["minimum_main_catalog_bytes"] == 34_914_304
    assert profile["minimum_physical_envelope_bytes"] == 46_552_406
    assert profile["catalog_main_fraction"] == {
        "denominator": 4,
        "numerator": 3,
    }
    assert profile["max_catalog_bytes"] == 64 * 1024 * 1024
    for kind, internal in DEFAULT_INTERNAL_LIMITS.items():
        assert internal <= usage_module.PUBLISHED_LIMITS[kind] // 5


def test_catalog_cadence_sustainability_is_visible_before_activation(
    tmp_path: Path,
):
    projected = _authority(6, seed=6)
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path), clock=lambda: float(NOW)
    )
    with sidecar.acquire() as lease:
        reservation = _reserve(lease, projected)
        diagnostics = lease.diagnostics()
        assert reservation.cost_units == 12
        assert diagnostics["sustainable_locations_at_catalog_cadence"] == 5
        assert diagnostics["catalog_cadence_seconds"] == 900
        assert diagnostics["catalog_fetch_slots_per_utc_day"] == 96
        assert diagnostics["acquisition_cadence_profile_sha256"] == (
            worker_acquisition_plan.CADENCE_PROFILE_SHA256
        )
        assert 96 * 5 * USAGE_UNITS_PER_LOCATION <= 1_000
        assert 96 * reservation.cost_units > 1_000


@pytest.mark.parametrize(
    "query_count,expected_units,accepted",
    [(1, 2, True), (15, 30, True), (16, 32, False), (64, 128, False)],
)
def test_exact_batch_size_drives_conservative_metering(
    tmp_path: Path,
    query_count: int,
    expected_units: int,
    accepted: bool,
):
    projected = _authority(query_count, seed=query_count)
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path),
        clock=lambda: float(NOW),
    )
    with sidecar.acquire() as lease:
        if not accepted:
            with pytest.raises(OpenMeteoUsageLimitExceeded, match="minute"):
                _reserve(lease, projected)
            assert lease.audit()["reservation_rows"] == 0
            return
        reservation = _reserve(lease, projected)
        assert reservation.query_count == query_count
        assert reservation.variable_count == 19
        assert reservation.cost_units == expected_units
        assert lease.diagnostics()["window_usage"]["minute"] == {
            "charged_units": expected_units,
            "internal_limit_units": 30,
        }


def test_reservation_permit_commit_and_retries_are_exact_and_privacy_safe(
    tmp_path: Path,
):
    projected = _authority(seed=1)
    batch = projected.openmeteo_shadow_query_batch()
    database = _database(tmp_path)
    sidecar = OpenMeteoUsageAuthority(database, clock=lambda: float(NOW))
    with sidecar.acquire() as lease:
        first = _reserve(lease, projected)
        second = _reserve(lease, projected)
        assert second == first
        assert first.authority_sha256 == projected.authority_sha256
        assert first.query_batch_sha256 == hashlib.sha256(
            json.dumps(
                batch,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
        ).hexdigest()
        permit = _begin(lease, first)
        assert permit.new_fetch_authorized is True
        assert permit.authority_sha256 == first.authority_sha256
        assert permit.query_batch_sha256 == first.query_batch_sha256
        assert permit.native_request_sha256 == first.native_request_sha256
        assert permit.profile_sha256 == first.profile_sha256
        assert permit.charged_units == first.cost_units == 2

        started = lease.load(first.reservation_id)
        assert started is not None
        replay = lease.begin_fetch(
            reservation_id=first.reservation_id,
            expected_state_sha256=first.state_sha256,
        )
        assert replay.permit_sha256 == permit.permit_sha256
        assert replay.new_fetch_authorized is False
        committed = lease.commit(
            reservation_id=first.reservation_id,
            expected_state_sha256=started.state_sha256,
            capture_receipt_sha256=CAPTURE_SHA,
        )
        retried = lease.commit(
            reservation_id=first.reservation_id,
            expected_state_sha256=started.state_sha256,
            capture_receipt_sha256=CAPTURE_SHA,
        )
        assert retried == committed
        assert committed.status == "COMMITTED"
        diagnostics = json.dumps(lease.diagnostics(), sort_keys=True)
        assert projected.native_job_id not in diagnostics
        assert "50.00001" not in diagnostics
        assert "4.00001" not in diagnostics
        assert lease.audit()["committed_count"] == 1
    raw_catalog = database.read_bytes()
    assert b"50.00001" not in raw_catalog
    assert b"zone.private" not in raw_catalog


def test_forged_batch_authority_or_expected_sha_is_rejected_without_charge(
    tmp_path: Path,
):
    projected = _authority(seed=2)
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path), clock=lambda: float(NOW)
    )
    with sidecar.acquire() as lease:
        batch = copy.deepcopy(projected.openmeteo_shadow_query_batch())
        batch["queries"][0]["latitude"] += 0.01
        with pytest.raises(OpenMeteoUsageConflict, match="binding"):
            lease.reserve(
                authority=projected,
                expected_authority_sha256=projected.authority_sha256,
                query_batch=batch,
            )
        with pytest.raises(OpenMeteoUsageConflict, match="binding"):
            lease.reserve(
                authority=projected,
                expected_authority_sha256="f" * 64,
                query_batch=projected.openmeteo_shadow_query_batch(),
            )
        forged = replace(projected, authority_sha256="e" * 64)
        with pytest.raises(OpenMeteoUsageConflict, match="binding"):
            lease.reserve(
                authority=forged,
                expected_authority_sha256="e" * 64,
                query_batch=projected.openmeteo_shadow_query_batch(),
            )
        assert lease.audit()["reservation_rows"] == 0


def test_four_windows_are_charged_atomically_and_failures_never_release(
    tmp_path: Path,
):
    profile = OpenMeteoUsageProfile(
        minute_units=6,
        hour_units=6,
        day_units=6,
        month_units=6,
    )
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path),
        profile=profile,
        clock=lambda: float(NOW),
    )
    with sidecar.acquire() as lease:
        reservations = [_reserve(lease, _authority(seed=index + 10)) for index in range(3)]
        before = lease.diagnostics()
        with pytest.raises(OpenMeteoUsageLimitExceeded, match="minute"):
            _reserve(lease, _authority(seed=99))
        assert lease.diagnostics() == before
        failed = lease.fail(
            reservation_id=reservations[0].reservation_id,
            expected_state_sha256=reservations[0].state_sha256,
            reason_code="cancelled",
        )
        assert failed.status == "FAILED"
        after = lease.diagnostics()
        assert after["failed_count"] == 1
        assert all(
            item["charged_units"] == 6
            for item in after["window_usage"].values()
        )


def test_permit_expiry_and_terms_review_block_fetch_but_allow_terminalization(
    tmp_path: Path,
):
    clock = [float(TERMS_REVIEW_DUE_AT - FETCH_AUTHORIZATION_SECONDS - 10)]
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path), clock=lambda: clock[0]
    )
    with sidecar.acquire() as lease:
        expired = _reserve(lease, _authority(seed=20))
        clock[0] += FETCH_AUTHORIZATION_SECONDS
        with pytest.raises(OpenMeteoUsageTermsExpired, match="authorization"):
            _begin(lease, expired)
        failed = lease.fail(
            reservation_id=expired.reservation_id,
            expected_state_sha256=expired.state_sha256,
            reason_code="authorization_expired",
        )
        assert failed.status == "FAILED"

        clock[0] = TERMS_REVIEW_DUE_AT - 1
        started_reservation = _reserve(lease, _authority(seed=21))
        permit = _begin(lease, started_reservation)
        assert permit.new_fetch_authorized is True
        started = lease.load(started_reservation.reservation_id)
        assert started is not None
        clock[0] = TERMS_REVIEW_DUE_AT
        with pytest.raises(OpenMeteoUsageTermsExpired, match="review"):
            _reserve(lease, _authority(seed=22))
        committed = lease.commit(
            reservation_id=started.reservation_id,
            expected_state_sha256=started.state_sha256,
            capture_receipt_sha256=CAPTURE_SHA,
        )
        assert committed.status == "COMMITTED"
        assert lease.diagnostics()["terms_review_required"] is True


def test_clock_before_terms_policy_review_cannot_create_usage(tmp_path: Path):
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path), clock=lambda: float(TERMS_REVIEWED_AT - 1)
    )
    with sidecar.acquire() as lease:
        with pytest.raises(
            OpenMeteoUsageClockRollback,
            match="before_terms_review",
        ):
            _reserve(lease, _authority(seed=23))
        assert lease.audit()["reservation_rows"] == 0
        assert lease.diagnostics()["terms_review_required"] is True


def test_observed_terms_deadline_is_sticky_across_clock_rollback_and_restart(
    tmp_path: Path,
):
    clock = [float(TERMS_REVIEW_DUE_AT)]
    path = _database(tmp_path)
    sidecar = OpenMeteoUsageAuthority(path, clock=lambda: clock[0])
    with sidecar.acquire() as lease:
        with pytest.raises(OpenMeteoUsageTermsExpired, match="review"):
            _reserve(lease, _authority(seed=24))
        assert lease.diagnostics()["terms_review_required"] is True
        clock[0] = float(TERMS_REVIEW_DUE_AT - 1)
        with pytest.raises(OpenMeteoUsageTermsExpired, match="review"):
            _reserve(lease, _authority(seed=24))

    reopened = OpenMeteoUsageAuthority(path, clock=lambda: clock[0])
    with reopened.acquire() as lease:
        with pytest.raises(OpenMeteoUsageTermsExpired, match="review"):
            _reserve(lease, _authority(seed=24))
        assert lease.audit()["reservation_rows"] == 0


def test_clock_rollback_blocks_reserve_and_permit_but_can_close_usage(
    tmp_path: Path,
):
    clock = [float(NOW)]
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path), clock=lambda: clock[0]
    )
    with sidecar.acquire() as lease:
        first = _reserve(lease, _authority(seed=30))
        clock[0] += 10
        _begin(lease, first)
        started = lease.load(first.reservation_id)
        assert started is not None
        clock[0] -= 5
        with pytest.raises(OpenMeteoUsageClockRollback):
            _reserve(lease, _authority(seed=31))
        closed = lease.fail(
            reservation_id=started.reservation_id,
            expected_state_sha256=started.state_sha256,
            reason_code="recovery_unresolved",
        )
        assert closed.terminal_at == NOW + 10
        diagnostics = lease.diagnostics()
        assert diagnostics["clock_rollback_detected"] is True
        assert diagnostics["clock_rollback_terminalizations"] == 1
        clock[0] = NOW + 10
        recovered = _reserve(lease, _authority(seed=31))
        assert recovered.status == "RESERVED"


def test_terminal_cas_allows_exactly_one_concurrent_outcome(tmp_path: Path):
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path), clock=lambda: float(NOW)
    )
    with sidecar.acquire() as lease:
        reserved = _reserve(lease, _authority(seed=40))
        _begin(lease, reserved)
        started = lease.load(reserved.reservation_id)
        assert started is not None
        barrier = threading.Barrier(2)
        outcomes: list[str] = []
        lock = threading.Lock()

        def commit() -> None:
            barrier.wait()
            try:
                lease.commit(
                    reservation_id=started.reservation_id,
                    expected_state_sha256=started.state_sha256,
                    capture_receipt_sha256=CAPTURE_SHA,
                )
                outcome = "COMMITTED"
            except OpenMeteoUsageConflict:
                outcome = "CONFLICT"
            with lock:
                outcomes.append(outcome)

        def fail() -> None:
            barrier.wait()
            try:
                lease.fail(
                    reservation_id=started.reservation_id,
                    expected_state_sha256=started.state_sha256,
                    reason_code="transport_failed",
                )
                outcome = "FAILED"
            except OpenMeteoUsageConflict:
                outcome = "CONFLICT"
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=commit), threading.Thread(target=fail)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10.0)
        assert all(not thread.is_alive() for thread in threads)
        assert sorted(outcomes).count("CONFLICT") == 1
        assert set(outcomes) in (
            {"COMMITTED", "CONFLICT"},
            {"FAILED", "CONFLICT"},
        )
        assert lease.audit()["reservation_rows"] == 1


def test_concurrent_begin_fetch_authorizes_exactly_one_network_attempt(
    tmp_path: Path,
):
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path), clock=lambda: float(NOW)
    )
    with sidecar.acquire() as lease:
        reserved = _reserve(lease, _authority(seed=41))
        barrier = threading.Barrier(2)
        outcomes: list[bool] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def begin() -> None:
            barrier.wait()
            try:
                result = _begin(lease, reserved).new_fetch_authorized
                with lock:
                    outcomes.append(result)
            except BaseException as error:
                with lock:
                    errors.append(error)

        threads = [threading.Thread(target=begin) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10.0)
        assert all(not thread.is_alive() for thread in threads)
        assert errors == []
        assert sorted(outcomes) == [False, True]
        assert lease.audit()["reserved_count"] == 1


def test_same_process_and_cross_process_owner_are_exclusive(tmp_path: Path):
    path = _database(tmp_path)
    first = OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    second = OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    first_lease = first.acquire()
    try:
        with pytest.raises(OpenMeteoUsageOwnerBusy):
            second.acquire()

        context = multiprocessing.get_context("spawn")
        result = context.Queue()
        contender = context.Process(
            target=_spawn_attempt,
            args=(str(path), result),
        )
        contender.start()
        contender.join(15.0)
        assert contender.exitcode == 0
        assert result.get(timeout=5.0) == ("busy", "usage_owner_busy")
        _close_queue(result)
    finally:
        first_lease.release()
    with second.acquire() as successor:
        assert successor.diagnostics()["reservation_rows"] == 0


def test_owner_is_released_by_os_after_process_exit(tmp_path: Path):
    path = _database(tmp_path)
    OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    result = context.Queue()
    holder = context.Process(
        target=_spawn_hold,
        args=(str(path), ready, release, result),
    )
    holder.start()
    try:
        assert ready.wait(10.0)
        assert result.get(timeout=5.0)[0] == "held"
        holder.terminate()
        holder.join(10.0)
        assert holder.exitcode is not None
    finally:
        if holder.is_alive():
            release.set()
            holder.join(5.0)
        if holder.is_alive():
            holder.terminate()
            holder.join(5.0)
    with OpenMeteoUsageAuthority(
        path, clock=lambda: float(NOW)
    ).acquire() as lease:
        assert lease.diagnostics()["reservation_rows"] == 0
    _close_queue(result)


@pytest.mark.parametrize(
    "fault_point,persists",
    [("reserve.before_commit", False), ("reserve.after_commit", True)],
)
def test_real_process_crash_keeps_whole_or_no_reservation(
    tmp_path: Path,
    fault_point: str,
    persists: bool,
):
    path = _database(tmp_path)
    projected = _authority(seed=50)
    batch = projected.openmeteo_shadow_query_batch()
    OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=_spawn_reservation_crash,
        args=(str(path), projected, batch, fault_point),
    )
    process.start()
    process.join(20.0)
    assert process.exitcode == 73
    reopened = OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    with reopened.acquire() as lease:
        audit = lease.audit()
        assert audit["reservation_rows"] == int(persists)
        if persists:
            replay = _reserve(lease, projected)
            assert replay.status == "RESERVED"


@pytest.mark.parametrize(
    "fault_point,persists",
    [("begin_fetch.before_commit", False), ("begin_fetch.after_commit", True)],
)
def test_real_process_crash_never_reissues_a_durable_fetch_permit(
    tmp_path: Path,
    fault_point: str,
    persists: bool,
):
    path = _database(tmp_path)
    projected = _authority(seed=55)
    sidecar = OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    with sidecar.acquire() as lease:
        reserved = _reserve(lease, projected)

    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=_spawn_begin_crash,
        args=(
            str(path),
            reserved.reservation_id,
            reserved.state_sha256,
            fault_point,
        ),
    )
    process.start()
    process.join(20.0)
    assert process.exitcode == 74

    reopened = OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    with reopened.acquire() as lease:
        loaded = lease.load(reserved.reservation_id)
        assert loaded is not None
        assert (loaded.fetch_started_at is not None) is persists
        replay = _begin(lease, reserved)
        assert replay.new_fetch_authorized is (not persists)


@pytest.mark.parametrize(
    "fault_point,persists",
    [("reserve.before_commit", False), ("reserve.after_commit", True)],
)
def test_disk_full_is_atomic_and_durable_retry_is_idempotent(
    tmp_path: Path,
    fault_point: str,
    persists: bool,
):
    def disk_full(point: str) -> None:
        if point == fault_point:
            raise OSError(errno.ENOSPC, "synthetic full disk")

    path = _database(tmp_path)
    projected = _authority(seed=60)
    sidecar = OpenMeteoUsageAuthority(
        path,
        clock=lambda: float(NOW),
        fault_injector=disk_full,
    )
    with sidecar.acquire() as lease:
        with pytest.raises(OpenMeteoUsageLimitExceeded, match="disk_full"):
            _reserve(lease, projected)
    reopened = OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    with reopened.acquire() as lease:
        assert lease.audit()["reservation_rows"] == int(persists)
        if persists:
            assert _reserve(lease, projected).status == "RESERVED"


def test_schema_aggregate_and_delete_tamper_fail_closed(tmp_path: Path):
    path = _database(tmp_path)
    projected = _authority(seed=70)
    sidecar = OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    with sidecar.acquire() as lease:
        _reserve(lease, projected)
    with closing(sqlite3.connect(path)) as database, database:
        database.create_function("usage_prune_authorized", 0, lambda: 0)
        with pytest.raises(sqlite3.DatabaseError):
            database.execute("DELETE FROM reservations")
        database.execute(
            "UPDATE window_usage SET charged_units=charged_units+1,"
            "reservation_count=reservation_count+1 WHERE window_kind='minute'"
        )
    with pytest.raises(OpenMeteoUsageIntegrityError):
        OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))

    other = _database(tmp_path, name="schema.sqlite3")
    OpenMeteoUsageAuthority(other, clock=lambda: float(NOW))
    with closing(sqlite3.connect(other)) as database, database:
        database.execute("DROP INDEX reservations_prune_order")
    with pytest.raises(OpenMeteoUsageIntegrityError, match="schema"):
        OpenMeteoUsageAuthority(other, clock=lambda: float(NOW))


def test_profile_and_pricing_policy_drift_are_rejected(tmp_path: Path, monkeypatch):
    path = _database(tmp_path)
    OpenMeteoUsageAuthority(path, clock=lambda: float(NOW))
    smaller = OpenMeteoUsageProfile(
        minute_units=20,
        hour_units=200,
        day_units=800,
        month_units=10_000,
    )
    with pytest.raises(OpenMeteoUsageIntegrityError, match="anchor|manifest"):
        OpenMeteoUsageAuthority(
            path,
            profile=smaller,
            clock=lambda: float(NOW),
        )
    original_pricing_policy_id = usage_module.OPENMETEO_PRICING_POLICY_ID
    monkeypatch.setattr(
        usage_module,
        "OPENMETEO_PRICING_POLICY_ID",
        "omp1:" + "f" * 64,
    )
    with pytest.raises(OpenMeteoUsageIntegrityError, match="policy"):
        OpenMeteoUsageAuthority(
            _database(tmp_path, name="pricing-drift.sqlite3"),
            clock=lambda: float(NOW),
        )
    monkeypatch.setattr(
        usage_module,
        "OPENMETEO_PRICING_POLICY_ID",
        original_pricing_policy_id,
    )
    monkeypatch.setattr(
        usage_module,
        "ACQUISITION_CADENCE_PROFILE_SHA256",
        "f" * 64,
    )
    with pytest.raises(OpenMeteoUsageIntegrityError, match="policy"):
        OpenMeteoUsageAuthority(
            _database(tmp_path, name="cadence-drift.sqlite3"),
            clock=lambda: float(NOW),
        )


def test_profiles_cannot_expand_caps_or_underprovision_terms_envelope():
    with pytest.raises(ValueError, match="day_units"):
        OpenMeteoUsageProfile(day_units=1_001)
    with pytest.raises(ValueError, match="max_reservations"):
        OpenMeteoUsageProfile(max_reservations=14_999)
    with pytest.raises(ValueError, match="max_catalog_bytes"):
        OpenMeteoUsageProfile(
            max_catalog_bytes=MINIMUM_PHYSICAL_ENVELOPE_BYTES - 1
        )


def test_terminal_retention_prunes_only_after_every_window_is_historical(
    tmp_path: Path,
):
    clock = [float(NOW)]
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path), clock=lambda: clock[0]
    )
    with sidecar.acquire() as lease:
        first = _reserve(lease, _authority(seed=80))
        lease.fail(
            reservation_id=first.reservation_id,
            expected_state_sha256=first.state_sha256,
            reason_code="cancelled",
        )
        before = lease.prune()
        assert before["pruned_rows"] == 0
        assert lease.audit()["failed_count"] == 1

        clock[0] = NOW + RETENTION_SECONDS + 40 * 86_400
        pruned = lease.prune()
        assert pruned["pruned_rows"] == 1
        audit = lease.audit()
        assert audit["reservation_rows"] == 0
        assert audit["window_rows"] == 0
        assert audit["pruned_count"] == 1
        assert pruned["prune_chain_sha256"] != (
            usage_module._PRUNE_GENESIS_SHA256
        )

    unresolved_clock = [float(NOW)]
    unresolved_sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path, name="unresolved.sqlite3"),
        clock=lambda: unresolved_clock[0],
    )
    with unresolved_sidecar.acquire() as lease:
        terminal = _reserve(lease, _authority(seed=82))
        lease.fail(
            reservation_id=terminal.reservation_id,
            expected_state_sha256=terminal.state_sha256,
            reason_code="cancelled",
        )
        unresolved = _reserve(lease, _authority(seed=81))
        unresolved_clock[0] += RETENTION_SECONDS + 40 * 86_400
        assert lease.prune()["pruned_rows"] == 1
        assert lease.load(unresolved.reservation_id) == unresolved
        audit = lease.audit()
        assert audit["reserved_count"] == 1
        assert audit["window_rows"] == 4


def test_bounded_prune_batch_reconciles_partially_retained_windows(
    tmp_path: Path,
    monkeypatch,
):
    clock = [float(NOW)]
    sidecar = OpenMeteoUsageAuthority(
        _database(tmp_path), clock=lambda: clock[0]
    )
    with sidecar.acquire() as lease:
        reservations = [
            _reserve(lease, _authority(seed=90 + index))
            for index in range(3)
        ]
        for reservation in reservations:
            lease.fail(
                reservation_id=reservation.reservation_id,
                expected_state_sha256=reservation.state_sha256,
                reason_code="cancelled",
            )
        clock[0] += RETENTION_SECONDS + 40 * 86_400
        monkeypatch.setattr(usage_module, "PRUNE_BATCH_ROWS", 2)
        assert lease.prune()["pruned_rows"] == 2
        first_audit = lease.audit()
        assert first_audit["failed_count"] == 1
        assert first_audit["window_rows"] == 4
        assert lease.prune()["pruned_rows"] == 1
        second_audit = lease.audit()
        assert second_audit["reservation_rows"] == 0
        assert second_audit["window_rows"] == 0


def test_bounded_real_row_scale_fits_the_pinned_physical_envelope(
    tmp_path: Path,
):
    row_count = 256
    clock = [float(NOW)]
    path = _database(tmp_path, name="capacity.sqlite3")
    sidecar = OpenMeteoUsageAuthority(path, clock=lambda: clock[0])
    baseline_bytes = path.stat().st_size
    assert baseline_bytes <= usage_module.PROFILE_CAPACITY_BASE_BYTES

    with sidecar.acquire() as lease:
        for index in range(row_count):
            clock[0] = float(NOW + index * 60)
            _reserve(lease, _authority(seed=2_000 + index))
        assert lease.audit()["reservation_rows"] == row_count

    catalog_bytes = path.stat().st_size
    observed_growth = catalog_bytes - baseline_bytes
    assert observed_growth > 0
    assert observed_growth <= (
        row_count * usage_module.PROFILE_BYTES_PER_RESERVATION_BOUND
    )
    assert MINIMUM_MAIN_CATALOG_BYTES == (
        usage_module.PROFILE_CAPACITY_BASE_BYTES
        + MAXIMUM_ROWS_BEFORE_TERMS_DUE
        * usage_module.PROFILE_BYTES_PER_RESERVATION_BOUND
    )
    assert MINIMUM_PHYSICAL_ENVELOPE_BYTES == (MINIMUM_MAIN_CATALOG_BYTES * 4 + 2) // 3
    assert usage_module.DEFAULT_USAGE_PROFILE.max_catalog_bytes >= (
        MINIMUM_PHYSICAL_ENVELOPE_BYTES
    )


def test_module_is_inert_private_and_not_live_wired():
    root = Path(__file__).resolve().parents[1]
    forbidden = (
        root / "storm_tracker_worker" / "service.py",
        root / "storm_tracker_worker" / "acquisition_activation.py",
        root / "storm_tracker_worker" / "acquisition_capture_recovery.py",
        root / "storm_tracker_worker" / "acquisition_handler_child.py",
        root / "storm_tracker_worker" / "terminal_raw_completion_router.py",
        root / "storm_tracker_worker" / "worker_first_service_composition.py",
        root / "custom_components" / "storm_tracker_v3" / "__init__.py",
    )
    for path in forbidden:
        text = path.read_text(encoding="utf-8")
        assert "openmeteo_usage_authority" not in text
        assert "OpenMeteoUsageAuthority" not in text
        assert "begin_fetch" not in text or "openmeteo" not in text.casefold()
    source = Path(usage_module.__file__).read_text(encoding="utf-8").casefold()
    assert "urllib" not in source
    assert "aiohttp" not in source
    assert "homeassistant" not in source
    assert "mqtt" not in source
