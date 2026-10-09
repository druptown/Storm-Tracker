"""Closed timeout hierarchy for acquisition inside the service-owned lane."""
from __future__ import annotations

from dataclasses import fields
import inspect

import pytest

from storm_tracker_worker import acquisition_activation
from storm_tracker_worker import acquisition_capture_recovery
from storm_tracker_worker import acquisition_runtime
from storm_tracker_worker import worker_first_service_composition as service_composition


def test_candidate_rule_executor_and_service_timeouts_form_one_closed_hierarchy():
    candidate_handlers = set(acquisition_capture_recovery.RECOVERY_BINDING_CATALOG)
    candidate_rules = tuple(
        rule for rule in acquisition_activation.ACTIVATION_RULES.values()
        if rule.handler_id in candidate_handlers
    )
    assert candidate_rules
    rule_walls = tuple(int(rule.maximum_wall_seconds or 0) for rule in candidate_rules)
    assert max(rule_walls) == acquisition_runtime.MAX_EXECUTOR_WALL_SECONDS == 300.0
    assert all(0 < value <= acquisition_runtime.MAX_EXECUTOR_WALL_SECONDS
               for value in rule_walls)
    assert service_composition.ACQUISITION_LANE_TIMEOUT_SECONDS == 615.0
    assert (
        service_composition.ACQUISITION_LANE_TIMEOUT_SECONDS
        - (2.0 * acquisition_runtime.MAX_EXECUTOR_WALL_SECONDS)
    ) == 15.0


def test_service_config_default_is_exact_and_cannot_undercut_or_expand_lane():
    field = next(
        item for item in fields(service_composition.WorkerFirstServiceCompositionConfig)
        if item.name == "acquisition_timeout_seconds"
    )
    assert field.default == service_composition.ACQUISITION_LANE_TIMEOUT_SECONDS
    boundary = service_composition.ACQUISITION_LANE_TIMEOUT_SECONDS
    assert service_composition._duration(
        boundary,
        "acquisition_timeout_seconds",
        minimum=boundary,
        maximum=boundary,
    ) == boundary
    for value in (boundary - 0.001, boundary + 0.001):
        with pytest.raises(
            service_composition.WorkerFirstServiceCompositionConfigError,
            match="acquisition_timeout_seconds_invalid",
        ):
            service_composition._duration(
                value,
                "acquisition_timeout_seconds",
                minimum=boundary,
                maximum=boundary,
            )


def test_service_tick_uses_its_closed_acquisition_timeout_not_a_literal():
    source = inspect.getsource(service_composition.WorkerFirstServiceRuntime.async_tick)
    assert "self._acquisition.tick" in source
    assert "timeout_seconds=self._acquisition_timeout" in source
    assert "timeout_code=\"acquisition_tick_timeout\"" in source


@pytest.mark.parametrize(
    "requested,expected",
    (
        (0.01, 0.05),
        (75.0, 75.0),
        (105.0, 105.0),
        (300.0, 300.0),
        (450.0, 300.0),
    ),
)
def test_process_executor_is_hard_bounded_below_service_lane(requested, expected):
    executor = acquisition_runtime._default_executor_factory(requested)
    assert executor._limits.wall_seconds == expected
    assert executor._limits.cpu_seconds == expected
    assert (
        executor._limits.wall_seconds
        < service_composition.ACQUISITION_LANE_TIMEOUT_SECONDS
    )
