"""Focused contracts for adaptive parallel raw acquisition admission."""
from __future__ import annotations

from types import SimpleNamespace
import sqlite3
import threading
import time

import pytest

from storm_tracker_worker import acquisition_runtime as runtime_module


class _Capacity:
    def __init__(self) -> None:
        self.reserved_count = 0
        self.reserved_entries = 0

    def stats(self) -> dict[str, int]:
        return {
            "reserved_count": self.reserved_count,
            "reserved_entries": self.reserved_entries,
        }


class _Clock:
    def __init__(self) -> None:
        self.value = 1_800_000_000.0

    def __call__(self) -> float:
        return self.value


def _controller(workload_count: int = 12):
    runtime = object.__new__(runtime_module.AcquisitionShadowRuntime)
    runtime._selection = SimpleNamespace(workload_ids=frozenset(
        f"provider.product_{index}" for index in range(workload_count)
    ))
    runtime._stop_event = threading.Event()
    runtime._batch_lock = threading.Lock()
    runtime._claim_lock = threading.Lock()
    runtime._state_lock = threading.RLock()
    runtime._batch_claimed_workload_ids = set()
    runtime._batch_active = False
    runtime._batch_reconciled = False
    runtime._priority_local_attempted_since_general = set()
    runtime._capacity_ledger = _Capacity()
    runtime._clock = _Clock()
    runtime._parallel_mode = "recovery"
    runtime._parallel_cooldown_until = 0.0
    runtime._parallel_previous_mean_runtime_seconds = None
    runtime._parallel_last_batch_duration_seconds = 0.0
    runtime._parallel_last_batch_mean_runtime_seconds = 0.0
    runtime._parallel_last_batch_job_count = 0
    runtime._parallel_last_batch_lane_limit = 5
    runtime._parallel_last_unhealthy_reasons = ()
    return runtime


def _install_fake_tick(
    runtime, *, count: int, deadline: bool = False, release_at: int | None = None
):
    gate = threading.Lock()
    release = threading.Event()
    state = {"next": 0, "active": 0, "peak": 0}

    def tick():
        with gate:
            index = state["next"]
            if index >= count:
                return runtime_module.ShadowTickResult(
                    "idle",
                    "no_ready_serial_job",
                    None,
                    None,
                    None,
                    None,
                    False,
                )
            state["next"] += 1
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            if release_at is not None and state["active"] >= release_at:
                release.set()
        try:
            if release_at is None:
                time.sleep(0.02)
            else:
                assert release.wait(1.0), "expected acquisition lanes did not start"
            failed = deadline and index == 0
            return runtime_module.ShadowTickResult(
                "blocked" if failed else "completed_shadow_raw",
                "deadline_expired" if failed else "shadow_raw_captured",
                "expired" if failed else "completed",
                None,
                1,
                f"provider.product_{index}",
                True,
            )
        finally:
            with gate:
                state["active"] -= 1

    runtime.tick = tick
    return state


def test_cold_start_uses_five_lanes_then_expands_after_healthy_batch():
    runtime = _controller(workload_count=12)
    cold = _install_fake_tick(runtime, count=12, release_at=5)

    first = runtime.tick_all_due()

    assert sum(item.workload_id is not None for item in first) == 12
    assert cold["peak"] == 5
    assert runtime._parallel_last_batch_lane_limit == 5
    assert runtime._parallel_mode == "expanded"

    expanded = _install_fake_tick(runtime, count=12, release_at=12)
    second = runtime.tick_all_due()

    assert sum(item.workload_id is not None for item in second) == 12
    assert expanded["peak"] == 12
    assert runtime._parallel_last_batch_lane_limit == 12


def test_each_rolling_batch_reopens_the_local_priority_cohort():
    runtime = _controller(workload_count=8)
    runtime._priority_local_attempted_since_general = {
        "ground.kmi.aws_10min",
        "radar.dwd.radolan_rv",
        "radar.kmi.public_wcs_unknown_quantity",
    }
    observed_at_first_tick = []

    def tick():
        observed_at_first_tick.append(
            frozenset(runtime._priority_local_attempted_since_general)
        )
        return runtime_module.ShadowTickResult(
            "idle",
            "no_ready_serial_job",
            None,
            None,
            None,
            None,
            False,
        )

    runtime.tick = tick

    runtime.tick_all_due()

    assert observed_at_first_tick
    assert set(observed_at_first_tick) == {frozenset()}


def test_deadline_pressure_returns_to_five_lanes_until_cooldown():
    runtime = _controller(workload_count=10)
    runtime._parallel_mode = "expanded"
    pressured = _install_fake_tick(
        runtime, count=10, deadline=True, release_at=10
    )

    runtime.tick_all_due()

    assert pressured["peak"] == 10
    assert runtime._parallel_mode == "recovery"
    assert runtime._parallel_last_unhealthy_reasons == ("deadline_miss",)
    cooldown_until = runtime._parallel_cooldown_until

    cooling = _install_fake_tick(runtime, count=10, release_at=5)
    runtime.tick_all_due()
    assert cooling["peak"] == 5
    assert runtime._parallel_mode == "recovery"

    runtime._clock.value = cooldown_until
    cooldown_batch = _install_fake_tick(runtime, count=10, release_at=5)
    runtime.tick_all_due()
    assert cooldown_batch["peak"] == 5
    assert runtime._parallel_mode == "expanded"

    recovered = _install_fake_tick(runtime, count=10, release_at=10)
    runtime.tick_all_due()
    assert recovered["peak"] == 10


def test_growing_commit_backlog_forces_safe_recovery_mode():
    runtime = _controller(workload_count=8)
    runtime._parallel_mode = "expanded"
    _install_fake_tick(runtime, count=8)
    original_tick = runtime.tick

    def tick_with_backlog():
        result = original_tick()
        if result.workload_id is not None:
            runtime._capacity_ledger.reserved_count = 1
            runtime._capacity_ledger.reserved_entries = 1
        return result

    runtime.tick = tick_with_backlog
    runtime.tick_all_due()

    assert runtime._parallel_mode == "recovery"
    assert runtime._parallel_last_unhealthy_reasons == ("commit_backlog",)


def test_scheduler_conflict_isolated_to_one_lane_without_aborting_batch():
    runtime = _controller(workload_count=6)
    gate = threading.Lock()
    state = {"raised": False, "next": 0}

    def tick():
        with gate:
            if not state["raised"]:
                state["raised"] = True
                raise runtime_module.scheduler_module.SchedulerConflict(
                    "scheduler_retry_conflict"
                )
            index = state["next"]
            if index >= 5:
                return runtime_module.ShadowTickResult(
                    "idle", "no_ready_serial_job", None, None, None, None, False
                )
            state["next"] += 1
        return runtime_module.ShadowTickResult(
            "completed_shadow_raw",
            "shadow_raw_captured",
            "completed",
            None,
            1,
            f"provider.product_{index}",
            True,
        )

    runtime.tick = tick

    batch = runtime.tick_all_due()

    assert sum(item.workload_id is not None for item in batch) == 5
    assert any(
        item.reason_code == "parallel_scheduler_conflict_retry" for item in batch
    )
    assert runtime._parallel_last_unhealthy_reasons == ("scheduler_conflict",)


def test_scheduler_clock_error_isolated_to_one_lane_without_aborting_batch():
    runtime = _controller(workload_count=2)
    gate = threading.Lock()
    state = {"raised": False}

    def tick():
        with gate:
            if not state["raised"]:
                state["raised"] = True
                raise runtime_module.scheduler_module.SchedulerClockError(
                    "clock_regressed"
                )
        return runtime_module.ShadowTickResult(
            "idle", "no_ready_serial_job", None, None, None, None, False
        )

    runtime.tick = tick

    batch = runtime.tick_all_due()

    assert any(
        item.reason_code == "parallel_scheduler_clock_retry" for item in batch
    )
    assert runtime._parallel_last_unhealthy_reasons == ("scheduler_clock",)


def test_recoverable_desired_state_change_ends_only_affected_lane():
    runtime = _controller(workload_count=6)
    gate = threading.Lock()
    state = {"raised": False, "next": 0}

    def tick():
        with gate:
            if not state["raised"]:
                state["raised"] = True
                raise runtime_module.scheduler_module.SchedulerDesiredStateError(
                    "desired_state_changed_during_plan"
                )
            index = state["next"]
            if index >= 5:
                return runtime_module.ShadowTickResult(
                    "idle", "no_ready_serial_job", None, None, None, None, False
                )
            state["next"] += 1
        return runtime_module.ShadowTickResult(
            "completed_shadow_raw",
            "shadow_raw_captured",
            "completed",
            None,
            1,
            f"provider.product_{index}",
            True,
        )

    runtime.tick = tick

    batch = runtime.tick_all_due()

    assert sum(item.workload_id is not None for item in batch) == 5
    assert any(
        item.reason_code == "desired_state_changed_during_plan" for item in batch
    )
    assert runtime._parallel_last_unhealthy_reasons == ()


def test_unrecoverable_desired_state_error_remains_fail_closed():
    runtime = _controller(workload_count=2)

    def tick():
        raise runtime_module.scheduler_module.SchedulerDesiredStateError(
            "desired_state_inconsistent"
        )

    runtime.tick = tick

    with pytest.raises(
        runtime_module.scheduler_module.SchedulerDesiredStateError,
        match="desired_state_inconsistent",
    ):
        runtime.tick_all_due()

    assert runtime._parallel_last_unhealthy_reasons == ("batch_exception",)


def test_unexpected_batch_failure_exposes_only_closed_safe_category(caplog):
    runtime = _controller(workload_count=2)

    def tick():
        raise sqlite3.OperationalError("do-not-log-this-upstream-text")

    runtime.tick = tick

    with caplog.at_level("ERROR"):
        try:
            runtime.tick_all_due()
        except sqlite3.OperationalError:
            pass
        else:  # pragma: no cover - assertion guard
            raise AssertionError("batch failure must remain fail-closed")

    assert runtime._parallel_last_unhealthy_reasons == ("batch_sqlite_error",)
    assert "class=OperationalError reason=batch_sqlite_error" in caplog.text
    assert "do-not-log-this-upstream-text" not in caplog.text


def test_parallel_terminal_transitions_cannot_reorder_scheduler_clock():
    """A slower earlier completion must commit before a later timestamp."""

    runtime = _controller(workload_count=2)
    runtime._clock = lambda: 0.0
    second_entered = threading.Event()

    class _Scheduler:
        def __init__(self) -> None:
            self.lock = threading.Lock()
            self.last_now = 0.0
            self.transitions: list[float] = []

        def complete(self, job_id, _lease_token, *, now):
            if job_id == "first":
                # Without the runtime claim lock the second transition enters
                # here, commits time 3, then makes this time 2 regress.
                second_entered.wait(0.05)
            else:
                second_entered.set()
            with self.lock:
                if now < self.last_now:
                    raise runtime_module.scheduler_module.SchedulerClockError(
                        "clock_regressed"
                    )
                self.last_now = now
                self.transitions.append(now)
            return SimpleNamespace(status="completed")

    scheduler = _Scheduler()
    runtime._scheduler = scheduler
    errors: list[BaseException] = []

    def finish(job_id: str, now: float) -> None:
        try:
            runtime._complete(
                SimpleNamespace(job_id=job_id, lease_token=f"lease-{job_id}"),
                now=now,
            )
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    first = threading.Thread(target=finish, args=("first", 2.0))
    second = threading.Thread(target=finish, args=("second", 3.0))
    first.start()
    time.sleep(0.01)
    second.start()
    first.join()
    second.join()

    assert errors == []
    assert scheduler.transitions == [2.0, 3.0]
