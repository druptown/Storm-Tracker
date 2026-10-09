"""Focused contracts for opt-in normal-HA warning dispatch."""
from __future__ import annotations

import copy
from dataclasses import FrozenInstanceError
import importlib.util
from itertools import repeat
from pathlib import Path
import sys
import types

import pytest


ROOT = Path(__file__).resolve().parent.parent
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"


@pytest.fixture(scope="module")
def warning_module():
    package = sys.modules.get("storm_tracker_v3")
    if package is None:
        package = types.ModuleType("storm_tracker_v3")
        package.__path__ = [str(COMPONENT)]
        sys.modules["storm_tracker_v3"] = package

    if "storm_tracker_v3.const" not in sys.modules:
        const_spec = importlib.util.spec_from_file_location(
            "storm_tracker_v3.const", COMPONENT / "const.py"
        )
        const_module = importlib.util.module_from_spec(const_spec)
        sys.modules["storm_tracker_v3.const"] = const_module
        const_spec.loader.exec_module(const_module)

    module_name = "storm_tracker_v3.warning_dispatcher"
    module = sys.modules.get(module_name)
    if module is None:
        spec = importlib.util.spec_from_file_location(
            module_name, COMPONENT / "warning_dispatcher.py"
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    return module


class Clock:
    def __init__(self, now=1_000.0):
        self.now = float(now)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += float(seconds)


class MemoryStore:
    def __init__(self, payload=None):
        self.payload = copy.deepcopy(payload)
        self.saves = []

    async def async_load(self):
        return copy.deepcopy(self.payload)

    async def async_save(self, payload):
        self.payload = copy.deepcopy(payload)
        self.saves.append(copy.deepcopy(payload))


class Services:
    def __init__(self, *, available=True, fail=False):
        self.available = available
        self.fail = fail
        self.calls = []

    def has_service(self, domain, service):
        return self.available and (domain, service) == (
            "notify", "mobile_app_testtelefoon"
        )

    async def async_call(
        self, domain, service, service_data, *, blocking=False
    ):
        self.calls.append({
            "domain": domain,
            "service": service,
            "service_data": copy.deepcopy(service_data),
            "blocking": blocking,
        })
        if self.fail:
            raise RuntimeError("synthetic notify failure")


class Bus:
    def __init__(self):
        self.events = []

    def async_fire(self, event_type, event_data):
        self.events.append((event_type, copy.deepcopy(event_data)))


class Hass:
    def __init__(self, services=None):
        self.services = services or Services()
        self.bus = Bus()


def _status(eta=60.0, **overrides):
    value = {
        "status": "naderend",
        "warning_eligible": True,
        "forecast_available": True,
        "eta_reliable": True,
        "forecast_block_reason": None,
        "passage_classification": "raak",
        "motion_confidence": "Matig",
        "reliability_label": "Matig",
        "eta_minutes": eta,
        "storm_id": "storm-7",
        "tracked_system_distance_km": 18.456,
        "forecast_confidence_percent": 64.0,
        "forecast_intensity_label": "matig",
    }
    value.update(overrides)
    return value


def _evaluation(module, *, eta=60.0, status=None, age=120.0, **kwargs):
    return module.build_warning_evaluation(
        status or _status(eta),
        target_id=kwargs.pop("target_id", "home"),
        target_label=kwargs.pop("target_label", "Thuis"),
        latitude=kwargs.pop("latitude", 51.050412),
        longitude=kwargs.pop("longitude", 4.451987),
        observed_at=kwargs.pop("observed_at", 1_000.0),
        data_age_seconds=age,
        runtime_admitted=kwargs.pop("runtime_admitted", True),
        target_available=kwargs.pop("target_available", True),
        radar_covered=kwargs.pop("radar_covered", True),
        **kwargs,
    )


def test_snapshot_is_immutable_bounded_provider_neutral_and_staged(
    warning_module,
):
    expected = ((90, "vroeg"), (45, "nabij"), (15, "imminent"))
    for eta, stage in expected:
        evaluation = _evaluation(warning_module, eta=eta)
        assert evaluation.reason == "eligible"
        assert evaluation.snapshot.warning_stage == stage

    snapshot = _evaluation(
        warning_module,
        eta=15,
        target_label="  Thuis   met   extra   spaties  ",
    ).snapshot
    assert snapshot.latitude == 51.05
    assert snapshot.longitude == 4.452
    with pytest.raises(FrozenInstanceError):
        snapshot.warning_stage = "vroeg"

    payload = snapshot.event_payload()
    assert payload["official_warning"] is False
    assert payload["advisory_type"] == "local_model_forecast"
    assert payload["warning_eligible"] is True
    assert payload["forecast_available"] is True
    assert not ({"provider", "provider_ids", "address", "secret"} & payload.keys())
    assert len(payload) <= 24


@pytest.mark.parametrize(
    ("status", "kwargs", "reason"),
    [
        (_status(warning_eligible="true"), {}, "warning_not_eligible"),
        (_status(forecast_available=False), {}, "forecast_unavailable"),
        (_status(eta_reliable=False), {}, "eta_not_reliable"),
        (_status(forecast_block_reason="stale"), {}, "forecast_blocked"),
        (_status(passage_classification="mist"), {}, "passage_not_targeting"),
        (_status(motion_confidence="Laag"), {}, "motion_confidence_below_moderate"),
        (_status(reliability_label="Laag"), {}, "reliability_below_moderate"),
        (_status(status="onvoldoende_data"), {}, "status_not_approaching"),
        (_status(), {"age": 1_201.0}, "data_stale"),
        (_status(), {"age": None}, "freshness_unknown"),
        (_status(), {"runtime_admitted": False}, "runtime_not_admitted"),
        (_status(), {"target_available": False}, "target_unavailable"),
        (_status(), {"radar_covered": False}, "radar_not_covered"),
        (None, {}, "status_invalid"),
    ],
)
def test_unknown_stale_blocked_unadmitted_and_low_confidence_fail_closed(
    warning_module, status, kwargs, reason
):
    if status is None:
        evaluation = warning_module.build_warning_evaluation(
            status,
            target_id="home",
            target_label="Thuis",
            latitude=51.05,
            longitude=4.45,
            observed_at=1_000.0,
            data_age_seconds=120.0,
            runtime_admitted=True,
            target_available=True,
            radar_covered=True,
        )
    else:
        evaluation = _evaluation(warning_module, status=status, **kwargs)
    assert evaluation.snapshot is None
    assert evaluation.clear_allowed is False
    assert evaluation.reason == reason


@pytest.mark.asyncio
async def test_opt_in_and_exact_service_are_required_without_sent_claim(
    warning_module,
):
    candidate = _evaluation(warning_module)

    disabled_hass = Hass()
    disabled_store = MemoryStore()
    disabled = warning_module.WarningDispatcher(
        disabled_hass,
        enabled=False,
        notify_service="notify.mobile_app_testtelefoon",
        store=disabled_store,
    )
    await disabled.async_initialize()
    result = await disabled.async_process([candidate])
    assert result[0].reason == "opt_in_disabled"
    assert disabled_hass.services.calls == []
    assert disabled_hass.bus.events == []

    missing_hass = Hass(Services(available=False))
    missing_store = MemoryStore()
    missing = warning_module.WarningDispatcher(
        missing_hass,
        enabled=True,
        notify_service="notify.mobile_app_testtelefoon",
        store=missing_store,
    )
    await missing.async_initialize()
    first = await missing.async_process([candidate])
    assert first[0].reason == "notify_service_unavailable"
    assert missing_store.payload is None
    assert missing_hass.bus.events == []
    assert missing.diagnostics["status"] == "service_unavailable"

    invalid = warning_module.WarningDispatcher(
        Hass(), enabled=True, notify_service="persistent_notification.create",
        store=MemoryStore(),
    )
    await invalid.async_initialize()
    invalid_result = await invalid.async_process([candidate])
    assert invalid_result[0].reason == "notify_service_missing_or_invalid"


@pytest.mark.asyncio
async def test_real_notify_call_then_exact_sent_event_and_failed_call_retries(
    warning_module,
):
    clock = Clock()
    services = Services(fail=True)
    hass = Hass(services)
    store = MemoryStore()
    dispatcher = warning_module.WarningDispatcher(
        hass,
        enabled=True,
        notify_service="notify.mobile_app_testtelefoon",
        store=store,
        clock=clock,
    )
    await dispatcher.async_initialize()
    candidate = _evaluation(warning_module, eta=12)

    failed = await dispatcher.async_process([candidate])
    assert failed[0].reason == "notify_call_failed"
    assert len(services.calls) == 1
    assert hass.bus.events == []
    assert store.payload is None

    services.fail = False
    sent = await dispatcher.async_process([candidate])
    assert sent[0] == warning_module.DispatchResult(
        "sent", "sent", "imminent"
    )
    assert len(services.calls) == 2
    call = services.calls[-1]
    assert (call["domain"], call["service"], call["blocking"]) == (
        "notify", "mobile_app_testtelefoon", True
    )
    notify_data = call["service_data"]
    assert "geen officiële waarschuwing" in notify_data["message"]
    assert len(notify_data["message"]) <= warning_module.MAX_MESSAGE_LENGTH
    assert notify_data["data"]["official_warning"] is False

    assert hass.bus.events == [(
        "storm_tracker_v3_warning_sent",
        candidate.snapshot.event_payload(),
    )]
    assert len(store.payload["claims"]) == 1
    assert dispatcher.diagnostics["status"] == "sent"
    assert dispatcher.diagnostics["sent_count"] == 1


@pytest.mark.asyncio
async def test_target_storm_stage_dedupe_cooldown_and_escalation(
    warning_module,
):
    clock = Clock()
    hass = Hass()
    dispatcher = warning_module.WarningDispatcher(
        hass,
        enabled=True,
        notify_service="notify.mobile_app_testtelefoon",
        store=MemoryStore(),
        clock=clock,
    )
    await dispatcher.async_initialize()

    early = _evaluation(warning_module, eta=70)
    assert (await dispatcher.async_process([early]))[0].status == "sent"
    duplicate = await dispatcher.async_process([early])
    assert duplicate[0].reason == "target_storm_stage_already_sent"

    clock.advance(60)
    nearby = _evaluation(warning_module, eta=30, observed_at=clock())
    assert (await dispatcher.async_process([nearby]))[0].status == "sent"

    other_status = _status(30, storm_id="storm-8")
    other = _evaluation(
        warning_module, status=other_status, observed_at=clock()
    )
    cooldown = await dispatcher.async_process([other])
    assert cooldown[0].reason == "target_cooldown_active"

    clock.advance(30 * 60)
    other = _evaluation(
        warning_module, status=other_status, observed_at=clock()
    )
    assert (await dispatcher.async_process([other]))[0].status == "sent"
    imminent = _evaluation(
        warning_module,
        status=_status(10, storm_id="storm-8"),
        observed_at=clock(),
    )
    assert (await dispatcher.async_process([imminent]))[0].status == "sent"
    assert len(hass.services.calls) == 4
    assert [event[1]["warning_stage"] for event in hass.bus.events] == [
        "vroeg", "nabij", "nabij", "imminent"
    ]


@pytest.mark.asyncio
async def test_restart_persistence_trusted_clear_and_rearm(warning_module):
    clock = Clock()
    store = MemoryStore()
    first_hass = Hass()
    first = warning_module.WarningDispatcher(
        first_hass,
        enabled=True,
        notify_service="notify.mobile_app_testtelefoon",
        store=store,
        clock=clock,
    )
    await first.async_initialize()
    candidate = _evaluation(warning_module, eta=60)
    assert (await first.async_process([candidate]))[0].status == "sent"

    restarted_hass = Hass()
    restarted = warning_module.WarningDispatcher(
        restarted_hass,
        enabled=True,
        notify_service="notify.mobile_app_testtelefoon",
        store=store,
        clock=clock,
    )
    await restarted.async_initialize()
    deduped = await restarted.async_process([candidate])
    assert deduped[0].reason == "target_storm_stage_already_sent"
    assert restarted_hass.services.calls == []

    stale = _evaluation(
        warning_module, status=_status(status="droog"), age=1_500
    )
    assert (await restarted.async_process([stale]))[0].reason == "data_stale"
    assert (await restarted.async_process([candidate]))[0].reason == (
        "target_storm_stage_already_sent"
    )

    clear = _evaluation(
        warning_module, status=_status(status="droog"), observed_at=clock()
    )
    assert (await restarted.async_process([clear]))[0].status == "clear_pending"
    clock.advance(warning_module.CLEAR_CONFIRMATION_SECONDS)
    clear = _evaluation(
        warning_module, status=_status(status="droog"), observed_at=clock()
    )
    assert (await restarted.async_process([clear]))[0] == (
        warning_module.DispatchResult("clear", "rearmed")
    )

    # Re-arm maakt dezelfde target/storm/stage opnieuw geldig, maar de
    # targetcooldown blijft gelden tot dertig minuten na de echte verzending.
    clock.advance(
        warning_module.DEFAULT_COOLDOWN_SECONDS
        - warning_module.CLEAR_CONFIRMATION_SECONDS
    )
    rearmed = _evaluation(warning_module, eta=60, observed_at=clock())
    assert (await restarted.async_process([rearmed]))[0].status == "sent"
    assert len(restarted_hass.services.calls) == 1


@pytest.mark.asyncio
async def test_invalid_or_oversized_restart_state_fails_closed(warning_module):
    hass = Hass()
    store = MemoryStore({"version": 99, "targets": {}, "claims": []})
    dispatcher = warning_module.WarningDispatcher(
        hass,
        enabled=True,
        notify_service="notify.mobile_app_testtelefoon",
        store=store,
    )
    await dispatcher.async_initialize()
    assert dispatcher.diagnostics["status"] == "state_error"
    assert dispatcher.diagnostics["state_loaded"] is False
    result = await dispatcher.async_process([_evaluation(warning_module)])
    assert result[0].reason == "persistent_state_not_loaded"
    assert hass.services.calls == []
    assert hass.bus.events == []


@pytest.mark.asyncio
async def test_evaluation_input_is_hard_bounded_before_materialization(
    warning_module,
):
    hass = Hass()
    dispatcher = warning_module.WarningDispatcher(
        hass,
        enabled=True,
        notify_service="notify.mobile_app_testtelefoon",
        store=MemoryStore(),
    )
    await dispatcher.async_initialize()
    candidate = _evaluation(warning_module)
    result = await dispatcher.async_process(repeat(candidate))
    assert result == (warning_module.DispatchResult(
        "error", "too_many_targets"
    ),)
    assert hass.services.calls == []
    assert hass.bus.events == []
