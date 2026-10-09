"""Adversarial proof for candidate quantitative-decoder isolation."""
from __future__ import annotations

import ast
import inspect
import multiprocessing
import os
from pathlib import Path
import threading
import textwrap
import time

import pytest

from storm_tracker_worker import hard_decoder_isolation as isolation
from storm_tracker_worker.hard_decoder_isolation import (
    DecoderEffectContract,
    DecoderResourceLimits,
    HardDecoderExecutor,
)


ROOT = Path(__file__).parents[1]
TARGET = "tests.helpers.hard_decoder_targets"
SAFE = DecoderEffectContract(
    contract_id="test.pure.v1",
    hard_kill_safe=True,
    durable_effects="none",
    spawns_descendants=False,
)


def _executor(**changes) -> HardDecoderExecutor:
    process_slot_policy = changes.pop("process_slot_policy", "exclusive")
    values = {
        "wall_seconds": 8.0,
        "cpu_seconds": 4.0,
        "memory_bytes": 512 * 1024 * 1024,
        "max_request_bytes": 1024,
        "max_result_bytes": 4096,
    }
    values.update(changes)
    return HardDecoderExecutor(
        limits=DecoderResourceLimits(**values),
        process_slot_policy=process_slot_policy,
    )


def _run_cross_process_blocker(marker: str, outcome_path: str) -> None:
    contract = DecoderEffectContract(
        contract_id="test.cross-process.v1",
        hard_kill_safe=True,
        durable_effects="idempotent",
        spawns_descendants=False,
    )
    result = _executor(wall_seconds=1.6).execute(
        target=f"{TARGET}:mark_then_sleep_forever",
        request={"path": marker},
        effect_contract=contract,
    )
    Path(outcome_path).write_text(result.outcome, encoding="ascii")


def test_executor_refuses_a_non_spawn_context() -> None:
    class ForkLikeContext:
        @staticmethod
        def get_start_method():
            return "fork"

    with pytest.raises(ValueError, match="requires spawn"):
        HardDecoderExecutor(context=ForkLikeContext())


def test_environment_allowlist_must_be_immutable_and_canonical() -> None:
    with pytest.raises(TypeError, match="frozenset"):
        HardDecoderExecutor(environment_allowlist={"PATH"})
    with pytest.raises(TypeError, match="canonical"):
        HardDecoderExecutor(environment_allowlist=frozenset({"Path"}))


def test_process_slot_policy_is_closed() -> None:
    with pytest.raises(ValueError, match="process_slot_policy"):
        HardDecoderExecutor(process_slot_policy="unbounded")


def test_independent_io_child_does_not_contend_for_decoder_singleton() -> None:
    executor = _executor(process_slot_policy="independent")
    assert isolation._GLOBAL_JOB_SLOT.acquire(blocking=False)
    try:
        result = executor.execute(
            target=f"{TARGET}:echo",
            request={"parallel": "raw-io"},
            effect_contract=SAFE,
        )
    finally:
        isolation._GLOBAL_JOB_SLOT.release()

    assert result.outcome == "succeeded"
    assert result.result["echo"] == {"parallel": "raw-io"}
    assert isinstance(result.result["pid"], int)


def test_independent_io_children_execute_concurrently() -> None:
    barrier = threading.Barrier(3)
    results = []

    def run_child(label: str) -> None:
        barrier.wait(timeout=4.0)
        results.append(
            _executor(process_slot_policy="independent").execute(
                target=f"{TARGET}:sleep_then_echo",
                request={"label": label, "seconds": 1.0},
                effect_contract=SAFE,
            )
        )

    threads = [
        threading.Thread(target=run_child, args=(label,), daemon=True)
        for label in ("first", "second")
    ]
    for thread in threads:
        thread.start()
    started = time.monotonic()
    barrier.wait(timeout=4.0)
    for thread in threads:
        thread.join(timeout=6.0)
    elapsed = time.monotonic() - started

    assert all(not thread.is_alive() for thread in threads)
    assert len(results) == 2
    assert {item.outcome for item in results} == {"succeeded"}
    assert len({item.result["pid"] for item in results}) == 2
    assert elapsed < 3.0


def test_closed_child_environment_excludes_parent_credentials(
    monkeypatch,
) -> None:
    monkeypatch.setenv("STV3_MQTT_PASSWORD", "mqtt-secret-sentinel")
    monkeypatch.setenv("STV3_KNMI_API_KEY", "provider-secret-sentinel")
    monkeypatch.setenv("STV3_WORKER_FIRST_CONFIG_PATH", "private-config-path")
    executor = HardDecoderExecutor(
        limits=DecoderResourceLimits(
            wall_seconds=8.0,
            cpu_seconds=4.0,
            memory_bytes=512 * 1024 * 1024,
            max_request_bytes=1024,
            max_result_bytes=4096,
        ),
        environment_allowlist=frozenset({"PATH"}),
    )

    result = executor.execute(
        target=f"{TARGET}:environment_presence",
        request={
            "keys": [
                "PATH",
                "STV3_KNMI_API_KEY",
                "STV3_MQTT_PASSWORD",
                "STV3_WORKER_FIRST_CONFIG_PATH",
            ]
        },
        effect_contract=SAFE,
    )

    assert result.outcome == "succeeded"
    assert result.result == {
        "PATH": "PATH" in os.environ,
        "STV3_KNMI_API_KEY": False,
        "STV3_MQTT_PASSWORD": False,
        "STV3_WORKER_FIRST_CONFIG_PATH": False,
    }
    assert os.environ["STV3_MQTT_PASSWORD"] == "mqtt-secret-sentinel"
    assert os.environ["STV3_KNMI_API_KEY"] == "provider-secret-sentinel"
    assert os.environ["STV3_WORKER_FIRST_CONFIG_PATH"] == (
        "private-config-path"
    )


def test_closed_environment_is_applied_at_the_os_spawn_boundary(
    monkeypatch,
) -> None:
    monkeypatch.setenv("STV3_MQTT_PASSWORD", "bootstrap-secret")
    observed = {}

    class ObservingProcess:
        @staticmethod
        def start():
            observed.update(os.environ)

    executor = HardDecoderExecutor(
        environment_allowlist=frozenset({"PATH"})
    )
    executor._start_process(ObservingProcess())

    assert observed == (
        {"PATH": os.environ["PATH"]} if "PATH" in os.environ else {}
    )
    assert os.environ["STV3_MQTT_PASSWORD"] == "bootstrap-secret"


def test_ephemeral_environment_override_exists_only_during_spawn(
    monkeypatch,
) -> None:
    key = "STV3_NETATMO_ACCESS_TOKEN"
    monkeypatch.setenv(key, "ambient-parent-token")
    observed = {}

    class ObservingProcess:
        @staticmethod
        def start():
            observed.update(os.environ)

    executor = HardDecoderExecutor(
        environment_allowlist=frozenset({"PATH", key})
    )
    override = "fresh-parent-broker-token"
    executor._start_process(
        ObservingProcess(),
        environment_overrides={key: override},
    )

    assert observed[key] == override
    assert os.environ[key] == "ambient-parent-token"
    assert override not in repr(executor.__dict__)


def test_ephemeral_environment_override_restores_parent_on_spawn_failure(
    monkeypatch,
) -> None:
    key = "STV3_NETATMO_ACCESS_TOKEN"
    monkeypatch.setenv(key, "ambient-parent-token")

    class FailingProcess:
        @staticmethod
        def start():
            assert os.environ[key] == "one-shot-token"
            raise OSError("safe synthetic spawn failure")

    executor = HardDecoderExecutor(
        environment_allowlist=frozenset({key})
    )
    with pytest.raises(OSError, match="spawn failure"):
        executor._start_process(
            FailingProcess(),
            environment_overrides={key: "one-shot-token"},
        )
    assert os.environ[key] == "ambient-parent-token"


@pytest.mark.parametrize(
    "overrides",
    (
        {"STV3_MQTT_PASSWORD": "forbidden"},
        {"STV3_NETATMO_ACCESS_TOKEN": "contains space"},
        {"STV3_NETATMO_ACCESS_TOKEN": "x" * 8193},
        {"STV3_NETATMO_ACCESS_TOKEN": "non-ascii-é"},
    ),
)
def test_ephemeral_environment_override_is_closed_and_bounded(overrides) -> None:
    executor = HardDecoderExecutor(
        environment_allowlist=frozenset({"STV3_NETATMO_ACCESS_TOKEN"})
    )
    result = executor.execute(
        target=f"{TARGET}:echo",
        request={},
        effect_contract=SAFE,
        environment_overrides=overrides,
    )
    assert result.outcome == "invalid_request"
    assert result.detail_code == "environment_override_invalid"
    assert all(value not in repr(result) for value in overrides.values())


def test_ephemeral_override_is_not_a_child_argument_or_ipc_request() -> None:
    source = inspect.getsource(HardDecoderExecutor._execute_locked)
    tree = ast.parse(textwrap.dedent(source))
    process_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "Process"
    ]
    assert len(process_calls) == 1
    arguments = next(
        keyword.value
        for keyword in process_calls[0].keywords
        if keyword.arg == "args"
    )
    assert "environment_overrides" not in ast.unparse(arguments)


def test_eumetsat_child_gets_ephemeral_bearer_but_never_parent_seeds(
    monkeypatch,
) -> None:
    ephemeral = "STV3_EUMETSAT_EPHEMERAL_ACCESS_TOKEN"
    consumer_key = "STV3_EUMETSAT_CONSUMER_KEY"
    consumer_secret = "STV3_EUMETSAT_CONSUMER_SECRET"
    monkeypatch.setenv(consumer_key, "parent-consumer-key")
    monkeypatch.setenv(consumer_secret, "parent-consumer-secret")
    executor = HardDecoderExecutor(
        limits=DecoderResourceLimits(
            wall_seconds=8.0,
            cpu_seconds=4.0,
            memory_bytes=512 * 1024 * 1024,
            max_request_bytes=1024,
            max_result_bytes=4096,
        ),
        environment_allowlist=frozenset({ephemeral}),
    )

    result = executor.execute(
        target=f"{TARGET}:environment_presence",
        request={"keys": [ephemeral, consumer_key, consumer_secret]},
        effect_contract=SAFE,
        environment_overrides={
            ephemeral: "eumetsat-ephemeral-access-token-9137",
        },
    )

    assert result.outcome == "succeeded"
    assert result.result == {
        ephemeral: True,
        consumer_key: False,
        consumer_secret: False,
    }
    assert ephemeral not in os.environ
    assert os.environ[consumer_key] == "parent-consumer-key"
    assert os.environ[consumer_secret] == "parent-consumer-secret"


def test_default_child_environment_contract_remains_backward_compatible(
    monkeypatch,
) -> None:
    monkeypatch.setenv("STV3_TEST_LEGACY_ENVIRONMENT", "present")
    result = _executor().execute(
        target=f"{TARGET}:environment_presence",
        request={"keys": ["STV3_TEST_LEGACY_ENVIRONMENT"]},
        effect_contract=SAFE,
    )

    assert result.outcome == "succeeded"
    assert result.result == {"STV3_TEST_LEGACY_ENVIRONMENT": True}


def test_success_is_json_only_and_every_job_gets_a_fresh_pid() -> None:
    executor = _executor()
    first = executor.execute(
        target=f"{TARGET}:echo", request={"frame": 1}, effect_contract=SAFE
    )
    second = executor.execute(
        target=f"{TARGET}:echo", request={"frame": 2}, effect_contract=SAFE
    )

    assert first.outcome == second.outcome == "succeeded"
    assert first.result["echo"] == {"frame": 1}
    assert second.result["echo"] == {"frame": 2}
    assert first.result["pid"] != second.result["pid"]


def test_hard_wall_deadline_kills_hang_and_next_job_still_runs() -> None:
    executor = _executor(wall_seconds=0.8, cpu_seconds=4.0)
    blocked = executor.execute(
        target=f"{TARGET}:sleep_forever", request={}, effect_contract=SAFE
    )
    recovered = executor.execute(
        target=f"{TARGET}:echo", request={"after": "kill"}, effect_contract=SAFE
    )

    assert blocked.outcome == "deadline_exceeded"
    assert blocked.detail_code == "hard_wall_deadline"
    assert blocked.elapsed_seconds < 3.0
    assert recovered.outcome == "succeeded"
    assert recovered.result["echo"] == {"after": "kill"}


def test_parent_cancellation_hard_kills_child_and_releases_every_slot() -> None:
    executor = _executor(wall_seconds=8.0, cpu_seconds=4.0)
    cancel = threading.Event()
    holder = []
    thread = threading.Thread(
        target=lambda: holder.append(
            executor.execute(
                target=f"{TARGET}:sleep_forever",
                request={},
                effect_contract=SAFE,
                cancel_event=cancel,
            )
        )
    )
    thread.start()
    deadline = time.monotonic() + 4.0
    while not isolation._GLOBAL_JOB_SLOT.locked() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert isolation._GLOBAL_JOB_SLOT.locked()

    cancel.set()
    thread.join(timeout=4.0)
    recovered = executor.execute(
        target=f"{TARGET}:echo",
        request={"after": "cancel"},
        effect_contract=SAFE,
    )

    assert not thread.is_alive()
    assert [item.outcome for item in holder] == ["cancelled"]
    assert holder[0].detail_code == "parent_cancelled"
    assert recovered.outcome == "succeeded"
    assert recovered.result["echo"] == {"after": "cancel"}


def test_abrupt_child_crash_is_not_weather_and_does_not_poison_slot() -> None:
    executor = _executor()
    crashed = executor.execute(
        target=f"{TARGET}:crash", request={}, effect_contract=SAFE
    )
    recovered = executor.execute(
        target=f"{TARGET}:echo", request={}, effect_contract=SAFE
    )

    assert crashed.outcome == "child_crashed"
    assert crashed.result is None
    assert crashed.child_exit_code not in {None, 0}
    assert recovered.outcome == "succeeded"


def test_result_and_request_bounds_fail_closed() -> None:
    result = _executor(max_result_bytes=64).execute(
        target=f"{TARGET}:oversized_result",
        request={"bytes": 1_000_000},
        effect_contract=SAFE,
    )
    request = _executor(max_request_bytes=16).execute(
        target=f"{TARGET}:echo",
        request={"provider": "x" * 100},
        effect_contract=SAFE,
    )

    assert result.outcome == "result_too_large"
    assert result.result is None
    assert request.outcome == "invalid_request"
    assert request.detail_code == "request_too_large"


def test_non_json_result_and_input_are_rejected_without_pickle() -> None:
    invalid_result = _executor().execute(
        target=f"{TARGET}:non_json_result", request={}, effect_contract=SAFE
    )
    invalid_request = _executor().execute(
        target=f"{TARGET}:echo", request={"bad": {1, 2}}, effect_contract=SAFE
    )

    assert invalid_result.outcome == "result_not_json"
    assert invalid_request.outcome == "invalid_request"
    assert invalid_request.detail_code == "request_not_canonical_json"


def test_exception_text_and_request_secret_never_cross_boundary() -> None:
    secret = "SYNOPTIC-SECRET-9f6a"
    result = _executor().execute(
        target=f"{TARGET}:raise_with_secret",
        request={"secret": secret},
        effect_contract=SAFE,
    )

    assert result.outcome == "decoder_failed"
    assert secret not in repr(result)
    assert "credential" not in repr(result)


def test_effect_contract_blocks_unverified_writer_before_spawn(tmp_path) -> None:
    marker = tmp_path / "must-not-exist"
    unsafe = DecoderEffectContract(
        contract_id="test.unsafe.v1",
        hard_kill_safe=False,
        durable_effects="transactional",
        spawns_descendants=False,
    )
    result = _executor().execute(
        target=f"{TARGET}:write_marker",
        request={"path": str(marker)},
        effect_contract=unsafe,
    )

    assert result.outcome == "unsafe_effect_contract"
    assert not marker.exists()


def test_descendant_spawning_contract_is_rejected_before_spawn(tmp_path) -> None:
    marker = tmp_path / "descendant-policy-must-not-run"
    descendants = DecoderEffectContract(
        contract_id="test.descendants.v1",
        hard_kill_safe=True,
        durable_effects="idempotent",
        spawns_descendants=True,
    )
    result = _executor().execute(
        target=f"{TARGET}:write_marker",
        request={"path": str(marker)},
        effect_contract=descendants,
    )

    assert result.outcome == "unsafe_effect_contract"
    assert not marker.exists()


def test_process_wide_slot_allows_exactly_one_decoder_job() -> None:
    blocking = _executor(wall_seconds=1.2)
    result_holder = []
    thread = threading.Thread(
        target=lambda: result_holder.append(
            blocking.execute(
                target=f"{TARGET}:sleep_forever",
                request={},
                effect_contract=SAFE,
            )
        )
    )
    thread.start()
    deadline = time.monotonic() + 1.0
    while not isolation._GLOBAL_JOB_SLOT.locked() and time.monotonic() < deadline:
        time.sleep(0.01)

    competing = _executor().execute(
        target=f"{TARGET}:echo", request={}, effect_contract=SAFE
    )
    thread.join(timeout=4.0)

    assert competing.outcome == "busy"
    assert competing.detail_code == "decoder_slot_occupied"
    assert [item.outcome for item in result_holder] == ["deadline_exceeded"]


def test_os_slot_excludes_a_decoder_from_another_parent_process(tmp_path) -> None:
    marker = tmp_path / "other-parent-decoder-started"
    outcome_path = tmp_path / "other-parent-outcome"
    process = multiprocessing.get_context("spawn").Process(
        target=_run_cross_process_blocker,
        args=(str(marker), str(outcome_path)),
    )
    process.start()
    try:
        deadline = time.monotonic() + 8.0
        while not marker.exists() and time.monotonic() < deadline:
            if not process.is_alive():
                break
            time.sleep(0.02)
        assert marker.exists()

        competing = _executor().execute(
            target=f"{TARGET}:echo", request={}, effect_contract=SAFE
        )
        assert competing.outcome == "busy"
        assert competing.detail_code == "decoder_process_slot_occupied"

        process.join(timeout=8.0)
        assert not process.is_alive()
        assert process.exitcode == 0
        assert outcome_path.read_text(encoding="ascii") == "deadline_exceeded"
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=2.0)
        if process.is_alive() and hasattr(process, "kill"):
            process.kill()
            process.join(timeout=2.0)


def test_unsupported_platform_fails_before_context_or_target(
    monkeypatch, tmp_path
) -> None:
    marker = tmp_path / "unsupported-must-not-run"
    monkeypatch.setattr(isolation, "_platform_family", lambda: "unsupported")
    result = _executor().execute(
        target=f"{TARGET}:write_marker",
        request={"path": str(marker)},
        effect_contract=SAFE,
    )

    assert result.outcome == "unsupported_platform"
    assert result.detail_code == "hard_kernel_boundary_unavailable"
    assert not marker.exists()


@pytest.mark.skipif(
    isolation._platform_family() != "windows",
    reason="Windows Job Object setup path",
)
def test_windows_job_assignment_failure_never_releases_target(
    monkeypatch, tmp_path
) -> None:
    marker = tmp_path / "job-object-failure-must-not-run"

    def fail_assignment(_pid, _limits):
        raise OSError("synthetic Job Object assignment failure")

    monkeypatch.setattr(isolation, "_WindowsJobBoundary", fail_assignment)
    result = _executor().execute(
        target=f"{TARGET}:write_marker",
        request={"path": str(marker)},
        effect_contract=SAFE,
    )

    assert result.outcome == "boundary_setup_failed"
    assert result.detail_code == "windows_job_assignment_failed"
    assert not marker.exists()


def test_cpu_limit_is_distinct_from_deadline_and_crash() -> None:
    support = isolation.hard_boundary_support()
    if not support.supported:
        pytest.skip("no hard kernel boundary on this platform")
    result = _executor(wall_seconds=6.0, cpu_seconds=1.0).execute(
        target=f"{TARGET}:burn_cpu", request={}, effect_contract=SAFE
    )

    assert result.outcome == "cpu_limit_exceeded"
    assert result.outcome not in {"deadline_exceeded", "child_crashed"}


def test_memory_limit_is_distinct_where_kernel_reports_it() -> None:
    support = isolation.hard_boundary_support()
    if not support.supported:
        pytest.skip("no hard kernel boundary on this platform")
    result = _executor(
        wall_seconds=8.0,
        cpu_seconds=6.0,
        memory_bytes=192 * 1024 * 1024,
    ).execute(
        target=f"{TARGET}:exhaust_memory",
        request={"chunk_bytes": 8 * 1024 * 1024},
        effect_contract=SAFE,
    )

    assert result.outcome == "memory_limit_exceeded"
    assert result.outcome not in {"deadline_exceeded", "child_crashed"}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_candidate_executor_is_not_wired_into_service_or_home_assistant() -> None:
    candidate = "storm_tracker_worker.hard_decoder_isolation"
    service_imports = _imports(ROOT / "storm_tracker_worker" / "service.py")
    ha_imports = set()
    for path in (ROOT / "custom_components" / "storm_tracker_v3").rglob("*.py"):
        ha_imports.update(_imports(path))

    assert candidate not in service_imports
    assert candidate not in ha_imports
