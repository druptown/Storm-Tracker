from __future__ import annotations

import ast
import asyncio
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import signal
import sys
import types
from types import SimpleNamespace

import pytest

from storm_tracker_worker import service
from storm_tracker_worker.blitzortung_event_ingress import (
    BlitzortungEventIngressLimits,
)
from storm_tracker_worker.raw_artifacts import RawArtifactLimits
from storm_tracker_worker.worker_first_service_activation import (
    LEGACY_MODE,
    WorkerFirstServiceActivationPlan,
    WorkerFirstServiceActivationError,
)
from storm_tracker_worker.worker_first_composition import (
    initialize_worker_first_composition_authority,
)
from storm_tracker_worker.worker_first_ownership import WorkerFirstOwnershipFence
from test_worker_first_service_activation import (
    _canonical,
    _service_config,
    _write_config,
)


@pytest.fixture()
def mqtt_stub(monkeypatch):
    events = []
    clients = []
    state = SimpleNamespace(
        client_error=None,
        credentials_error=None,
        tls_error=None,
        connect_error=None,
        will_error=None,
    )

    class Client:
        def __init__(self, **_kwargs):
            if state.client_error is not None:
                raise state.client_error
            clients.append(self)
            events.append("client")

        def username_pw_set(self, *_args):
            events.append("credentials")
            if state.credentials_error is not None:
                raise state.credentials_error

        def tls_set(self):
            events.append("tls")
            if state.tls_error is not None:
                raise state.tls_error

        def will_set(self, *_args, **_kwargs):
            events.append("will")
            if state.will_error is not None:
                raise state.will_error

        def connect(self, *_args, **_kwargs):
            events.append("connect")
            if state.connect_error is not None:
                raise state.connect_error

        def subscribe(self, *_args, **_kwargs):
            return (0, 1)

        def publish(self, *_args, **_kwargs):
            return SimpleNamespace(rc=0, is_published=lambda: True)

    mqtt_module = types.ModuleType("paho.mqtt.client")
    mqtt_module.Client = Client
    mqtt_module.CallbackAPIVersion = SimpleNamespace(VERSION2=2)
    mqtt_module.MQTTv311 = 4
    mqtt_module.MQTT_ERR_SUCCESS = 0
    mqtt_package = types.ModuleType("paho.mqtt")
    mqtt_package.client = mqtt_module
    paho_package = types.ModuleType("paho")
    paho_package.mqtt = mqtt_package
    monkeypatch.setitem(sys.modules, "paho", paho_package)
    monkeypatch.setitem(sys.modules, "paho.mqtt", mqtt_package)
    monkeypatch.setitem(sys.modules, "paho.mqtt.client", mqtt_module)
    return SimpleNamespace(events=events, clients=clients, state=state)


class _BootstrapRuntime:
    def __init__(self, events, *, diagnostics_error=None):
        self.events = events
        self.diagnostics_error = diagnostics_error
        self.stop_calls = 0

    async def async_start(self):
        return None

    async def async_tick(self):
        return None

    async def async_stop(self):
        self.stop_calls += 1
        self.events.append("runtime_stop")
        return {
            "state": "stopped",
            "heavy_lane_active": False,
            "shutdown_complete": True,
            "ownership_fence_held": False,
        }

    def diagnostics(self):
        if self.diagnostics_error is not None:
            raise self.diagnostics_error
        return {"state": "new"}


def _legacy_plan() -> WorkerFirstServiceActivationPlan:
    return WorkerFirstServiceActivationPlan(
        runtime_mode=LEGACY_MODE,
        config_path=None,
        config_sha256=None,
        authority_sha256=None,
        composition_config=None,
    )


def test_absent_mode_returns_exact_legacy_runtime_kwargs(monkeypatch) -> None:
    plan = _legacy_plan()
    monkeypatch.setattr(
        service,
        "load_worker_first_service_activation",
        lambda environment=None: plan,
    )

    selected, kwargs = service._main_runtime_kwargs({})

    assert selected is plan
    assert kwargs == {
        "job_handlers": service.PRODUCTION_JOB_HANDLERS,
        "job_effect_contracts": service.PRODUCTION_JOB_EFFECT_CONTRACTS,
        "require_effect_contracts": True,
        "operational_snapshot_runtime_factory": (
            service.OperationalSnapshotRuntime
        ),
    }
    assert "worker_first_runtime_factory" not in kwargs


def test_canary_selection_has_no_legacy_heavy_runtime_and_binds_pins(
    monkeypatch,
) -> None:
    config = object()
    plan = SimpleNamespace(
        worker_first_selected=True,
        composition_config=config,
        config_sha256="a" * 64,
        authority_sha256="b" * 64,
    )
    monkeypatch.setattr(
        service,
        "load_worker_first_service_activation",
        lambda environment=None: plan,
    )
    calls = []
    runtime = object()

    def build(selected, **kwargs):
        calls.append((selected, kwargs))
        return runtime

    monkeypatch.setattr(service, "_build_worker_first_service_canary", build)
    selected, kwargs = service._main_runtime_kwargs({
        "STV3_RUNTIME_MODE": "worker_first_canary",
    })

    assert selected is plan
    assert set(kwargs) == {
        "process_isolation",
        "require_effect_contracts",
        "worker_first_runtime_factory",
    }
    assert kwargs["process_isolation"] is False
    assert kwargs["require_effect_contracts"] is True
    assert all(
        key not in kwargs
        for key in (
            "job_handlers",
            "job_effect_contracts",
            "operational_snapshot_runtime_factory",
            "autonomous_scheduler",
        )
    )
    publisher = object()
    status_supplier = object()
    assert kwargs["worker_first_runtime_factory"](
        publisher=publisher,
        status_supplier=status_supplier,
    ) is runtime
    assert calls == [(
        config,
        {
            "expected_config_sha256": "a" * 64,
            "expected_authority_sha256": "b" * 64,
            "publisher": publisher,
            "status_supplier": status_supplier,
        },
    )]


def test_raw_capture_selection_binds_all_active_sources_and_worker_secrets(
    monkeypatch,
) -> None:
    config = SimpleNamespace(blitzortung_enabled=True)
    plan = SimpleNamespace(
        raw_capture_selected=True,
        worker_first_selected=False,
        composition_config=config,
        config_sha256="c" * 64,
        authority_sha256="d" * 64,
    )
    monkeypatch.setattr(
        service,
        "load_worker_first_service_activation",
        lambda environment=None: plan,
    )
    calls = []

    class Runtime:
        def __init__(self):
            self.control_calls = []

        def handle_control_job(self, job_type, payload):
            self.control_calls.append((job_type, payload))
            return {"job_type": job_type}

    runtime = Runtime()

    def build(selected, **kwargs):
        calls.append((selected, kwargs))
        return runtime

    monkeypatch.setattr(service, "_build_raw_capture_service", build)
    selected, kwargs = service._main_runtime_kwargs({
        "STV3_RUNTIME_MODE": "raw_capture_shadow",
        "STV3_AEMET_API_KEY": "aemet-key-value-12345",
        "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY": (
            "worker_authority.blitzortung_participant_raw_retention_v1"
        ),
        "STV3_KNMI_API_KEY": "knmi-key",
    })

    assert selected is plan
    assert set(kwargs["job_handlers"]) == service.RAW_CAPTURE_CONTROL_JOB_TYPES
    assert set(kwargs["job_effect_contracts"]) == (
        service.RAW_CAPTURE_CONTROL_JOB_TYPES
    )
    assert kwargs["process_isolation"] is False
    assert kwargs["require_effect_contracts"] is True
    assert kwargs["worker_first_runtime_mode"] == "raw_capture_shadow"
    assert kwargs["worker_first_allowed_workload_ids"] == tuple(sorted(
        kwargs["worker_first_allowed_workload_ids"]
    ))
    assert kwargs["worker_first_allowed_workload_ids"]
    assert "operational_snapshot_runtime_factory" not in kwargs
    assert "autonomous_scheduler" not in kwargs

    assert kwargs["worker_first_runtime_factory"](
        publisher=object(),
        status_supplier=lambda: {},
    ) is runtime
    assert calls[0][0] is config
    build_kwargs = calls[0][1]
    assert build_kwargs["expected_config_sha256"] == "c" * 64
    assert build_kwargs["expected_authority_sha256"] == "d" * 64
    assert build_kwargs["executor_factory"] is (
        service._raw_capture_executor_factory
    )
    assert build_kwargs["clock"] is service.time.time
    assert build_kwargs["netatmo_access_token_broker"] is None
    assert build_kwargs["eumetsat_access_token_broker"] is None
    assert build_kwargs["blitzortung_access_retention_authority"] == (
        "worker_authority.blitzortung_participant_raw_retention_v1"
    )
    assert build_kwargs["secret_resolver"].resolve(
        "worker_secret.aemet_api_key"
    ) == "worker_environment_secret_present"

    job = service.Job(
        job_id="control-1",
        job_type="desired_state_snapshot",
        deadline=service.time.time() + 10,
        payload={},
    )
    assert kwargs["job_handlers"][job.job_type](job) == {
        "job_type": job.job_type,
    }
    assert runtime.control_calls == [("desired_state_snapshot", {})]


def test_worker_only_selection_keeps_all_sources_and_binds_snapshot_callbacks(
    monkeypatch,
) -> None:
    config = SimpleNamespace(blitzortung_enabled=False)
    plan = SimpleNamespace(
        raw_capture_selected=False,
        worker_only_selected=True,
        worker_first_selected=False,
        composition_config=config,
        config_sha256="e" * 64,
        authority_sha256="f" * 64,
    )
    monkeypatch.setattr(
        service,
        "load_worker_first_service_activation",
        lambda environment=None: plan,
    )
    calls = []

    class Runtime:
        @staticmethod
        def handle_control_job(_job_type, _payload):
            return {}

    def build(selected, **kwargs):
        calls.append((selected, kwargs))
        return Runtime()

    monkeypatch.setattr(service, "_build_worker_only_service", build)
    _selected, kwargs = service._main_runtime_kwargs({})
    publisher = object()
    status_supplier = lambda: {}
    kwargs["worker_first_runtime_factory"](
        publisher=publisher,
        status_supplier=status_supplier,
    )

    assert kwargs["worker_first_runtime_mode"] == "worker_only"
    assert kwargs["worker_first_allowed_workload_ids"]
    assert calls[0][0] is config
    assert calls[0][1]["publisher"] is publisher
    assert calls[0][1]["status_supplier"] is status_supplier
    assert calls[0][1]["expected_config_sha256"] == "e" * 64
    assert calls[0][1]["expected_authority_sha256"] == "f" * 64


def test_raw_capture_event_disabled_never_reads_authority_environment(
    monkeypatch,
) -> None:
    config = SimpleNamespace(blitzortung_enabled=False)
    plan = SimpleNamespace(
        raw_capture_selected=True,
        worker_first_selected=False,
        composition_config=config,
        config_sha256="c" * 64,
        authority_sha256="d" * 64,
    )
    monkeypatch.setattr(
        service,
        "load_worker_first_service_activation",
        lambda environment=None: plan,
    )
    calls = []

    class Runtime:
        @staticmethod
        def handle_control_job(_job_type, _payload):
            return {}

    def build(selected, **kwargs):
        calls.append((selected, kwargs))
        return Runtime()

    class TrackingEnvironment(dict):
        def __init__(self):
            super().__init__({
                service.RAW_CAPTURE_BLITZORTUNG_AUTHORITY_ENV: (
                    "must-not-be-read"
                )
            })
            self.get_calls: list[str] = []

        def get(self, key, default=None):
            self.get_calls.append(key)
            return super().get(key, default)

    environment = TrackingEnvironment()
    monkeypatch.setattr(service, "_build_raw_capture_service", build)

    _plan, kwargs = service._main_runtime_kwargs(environment)
    kwargs["worker_first_runtime_factory"](
        publisher=object(),
        status_supplier=lambda: {},
    )

    assert calls[-1][1]["blitzortung_access_retention_authority"] is None
    assert service.RAW_CAPTURE_BLITZORTUNG_AUTHORITY_ENV not in (
        environment.get_calls
    )


def test_raw_capture_netatmo_broker_requires_all_three_worker_oauth_seeds(
    monkeypatch,
) -> None:
    config = SimpleNamespace(blitzortung_enabled=True)
    plan = SimpleNamespace(
        raw_capture_selected=True,
        worker_first_selected=False,
        composition_config=config,
        config_sha256="c" * 64,
        authority_sha256="d" * 64,
    )
    monkeypatch.setattr(
        service,
        "load_worker_first_service_activation",
        lambda environment=None: plan,
    )
    calls = []

    class Runtime:
        @staticmethod
        def handle_control_job(_job_type, _payload):
            return {}

    def build(selected, **kwargs):
        calls.append((selected, kwargs))
        return Runtime()

    monkeypatch.setattr(service, "_build_raw_capture_service", build)
    seeds = {
        "STV3_NETATMO_CLIENT_ID": "client-id",
        "STV3_NETATMO_CLIENT_SECRET": "client-secret",
        "STV3_NETATMO_REFRESH_TOKEN": "refresh-token",
    }
    _plan, kwargs = service._main_runtime_kwargs(seeds)
    kwargs["worker_first_runtime_factory"](
        publisher=object(),
        status_supplier=lambda: {},
    )
    assert calls[-1][1]["netatmo_access_token_broker"] is (
        service._broker_raw_capture_netatmo_access_token
    )

    for omitted in seeds:
        calls.clear()
        incomplete = {key: value for key, value in seeds.items() if key != omitted}
        _plan, kwargs = service._main_runtime_kwargs(incomplete)
        kwargs["worker_first_runtime_factory"](
            publisher=object(),
            status_supplier=lambda: {},
        )
        assert calls[-1][1]["netatmo_access_token_broker"] is None


def test_eumetsat_parent_broker_returns_only_ephemeral_child_bearer(
    monkeypatch,
) -> None:
    from storm_tracker_worker import lightning_exact_raw as lightning

    raw_token = "eumetsat-ephemeral-access-token-9137"
    observed = {}

    class Token:
        closed = False

        def authorization_value(self):
            return "Bearer " + raw_token

        def close(self):
            self.closed = True

    token = Token()

    def mint(consumer_key, consumer_secret, *, deadline_at):
        observed.update({
            "consumer_key": consumer_key,
            "consumer_secret": consumer_secret,
            "deadline_at": deadline_at,
        })
        return token

    monkeypatch.setattr(
        lightning,
        "mint_eumetsat_access_token_parent",
        mint,
    )
    monkeypatch.setattr(service.time, "time", lambda: 1000.0)
    monkeypatch.setattr(service.time, "monotonic", lambda: 2000.0)
    environment = {
        "STV3_EUMETSAT_CONSUMER_KEY": "consumer-key",
        "STV3_EUMETSAT_CONSUMER_SECRET": "consumer-secret",
    }
    broker = service._eumetsat_access_token_broker(environment)
    assert callable(broker)
    assert broker(1034.5) == raw_token
    assert token.closed is True
    assert observed == {
        "consumer_key": "consumer-key",
        "consumer_secret": "consumer-secret",
        "deadline_at": 2034.5,
    }
    assert raw_token not in repr(broker)


@pytest.mark.parametrize(
    ("error_factory", "expected"),
    (
        (
            lambda module, secret: module.LightningRawTransportError(secret),
            "ParentCredentialTemporaryError",
        ),
        (
            lambda module, secret: module.EumetsatOAuthError(secret),
            "ParentCredentialPermanentError",
        ),
        (
            lambda module, secret: module.LightningRawCapacityError(secret),
            "ParentCredentialPermanentError",
        ),
        (
            lambda module, secret: module.LightningRawContractError(secret),
            "ParentCredentialPermanentError",
        ),
    ),
)
def test_eumetsat_parent_broker_classifies_failures_without_detail(
    monkeypatch,
    error_factory,
    expected,
) -> None:
    from storm_tracker_worker import acquisition_runtime
    from storm_tracker_worker import lightning_exact_raw as lightning

    secret = "private-eumetsat-response-detail"

    def mint(*_args, **_kwargs):
        raise error_factory(lightning, secret)

    monkeypatch.setattr(
        lightning,
        "mint_eumetsat_access_token_parent",
        mint,
    )
    monkeypatch.setattr(service.time, "time", lambda: 1000.0)
    monkeypatch.setattr(service.time, "monotonic", lambda: 2000.0)
    broker = service._eumetsat_access_token_broker({
        "STV3_EUMETSAT_CONSUMER_KEY": "consumer-key",
        "STV3_EUMETSAT_CONSUMER_SECRET": "consumer-secret",
    })
    with pytest.raises(getattr(acquisition_runtime, expected)) as caught:
        broker(1034.5)
    assert secret not in str(caught.value)
    assert secret not in repr(caught.value)


@pytest.mark.parametrize(
    ("deadline", "expected"),
    (
        (1000.0, "TimeoutError"),
        (1050.01, "ParentCredentialPermanentError"),
        (True, "ParentCredentialPermanentError"),
        (float("nan"), "ParentCredentialPermanentError"),
    ),
)
def test_eumetsat_parent_broker_rejects_unbounded_or_expired_deadline(
    monkeypatch,
    deadline,
    expected,
) -> None:
    from storm_tracker_worker import acquisition_runtime
    from storm_tracker_worker import lightning_exact_raw as lightning

    monkeypatch.setattr(service.time, "time", lambda: 1000.0)
    monkeypatch.setattr(service.time, "monotonic", lambda: 2000.0)
    monkeypatch.setattr(
        lightning,
        "mint_eumetsat_access_token_parent",
        lambda *_args, **_kwargs: pytest.fail("invalid deadline reached mint"),
    )
    broker = service._eumetsat_access_token_broker({
        "STV3_EUMETSAT_CONSUMER_KEY": "consumer-key",
        "STV3_EUMETSAT_CONSUMER_SECRET": "consumer-secret",
    })

    error_type = (
        TimeoutError
        if expected == "TimeoutError"
        else getattr(acquisition_runtime, expected)
    )
    with pytest.raises(error_type):
        broker(deadline)


def test_real_canary_constructor_has_only_ping_and_no_legacy_owner(
    mqtt_stub,
) -> None:
    runtime = _BootstrapRuntime(mqtt_stub.events)

    def build(**_kwargs):
        mqtt_stub.events.append("build")
        return runtime

    selected = service.WorkerService(
        host="mqtt",
        process_isolation=False,
        require_effect_contracts=True,
        receipt_path=None,
        worker_first_runtime_factory=build,
    )

    assert set(selected.job_handlers) == {"ping"}
    assert set(selected.job_effect_contracts) == {"ping"}
    assert selected._executor is None
    assert selected._autonomous_scheduler is None
    assert selected._receipt_store is None
    assert selected._runtime_role == "worker_first"
    assert mqtt_stub.events.index("build") < mqtt_stub.events.index("connect")


def test_raw_capture_constructor_has_only_ping_and_three_control_jobs(
    mqtt_stub,
) -> None:
    runtime = _BootstrapRuntime(mqtt_stub.events)
    runtime.handle_control_job = lambda job_type, payload: {
        "job_type": job_type,
        "payload": payload,
    }
    handlers = {
        job_type: (lambda _job: {})
        for job_type in service.RAW_CAPTURE_CONTROL_JOB_TYPES
    }
    effects = {
        "desired_state_apply": service._TRANSACTIONAL_DESIRED_STATE_EFFECT,
        "desired_state_claim": service._TRANSACTIONAL_DESIRED_STATE_EFFECT,
        "desired_state_snapshot": service._IDEMPOTENT_READ_EFFECT,
    }

    selected = service.WorkerService(
        host="mqtt",
        job_handlers=handlers,
        job_effect_contracts=effects,
        process_isolation=False,
        require_effect_contracts=True,
        receipt_path=None,
        worker_first_runtime_factory=lambda **_kwargs: runtime,
        worker_first_runtime_mode="raw_capture_shadow",
        worker_first_allowed_workload_ids=("radar.dwd.radolan_rv",),
    )

    assert set(selected.job_handlers) == {
        "ping",
        *service.RAW_CAPTURE_CONTROL_JOB_TYPES,
    }
    assert set(selected.job_effect_contracts) == set(selected.job_handlers)
    assert selected._executor is None
    assert selected._autonomous_scheduler is None
    assert selected._receipt_store is None
    assert selected._worker_first_runtime_mode == "raw_capture_shadow"


def test_worker_first_constructor_rejects_legacy_receipt_owner(
    mqtt_stub,
) -> None:
    with pytest.raises(ValueError, match="legacy service-eigenaars"):
        service.WorkerService(
            host="mqtt",
            process_isolation=False,
            receipt_path="legacy.sqlite3",
            worker_first_runtime_factory=lambda **_kwargs: object(),
        )


def test_worker_first_constructor_rejects_process_executor(mqtt_stub) -> None:
    with pytest.raises(ValueError, match="legacy service-eigenaars"):
        service.WorkerService(
            host="mqtt",
            process_isolation=True,
            worker_first_runtime_factory=lambda **_kwargs: object(),
        )


def test_falsey_canary_factory_is_still_selected(mqtt_stub) -> None:
    runtime = _BootstrapRuntime(mqtt_stub.events)

    class Factory:
        def __bool__(self):
            return False

        def __call__(self, **_kwargs):
            mqtt_stub.events.append("falsey_build")
            return runtime

    selected = service.WorkerService(
        host="mqtt",
        process_isolation=False,
        require_effect_contracts=True,
        worker_first_runtime_factory=Factory(),
    )

    assert selected._operational_snapshot_runtime is runtime
    assert "falsey_build" in mqtt_stub.events


def test_canary_builder_failure_is_fixed_and_never_connects(
    mqtt_stub,
    caplog,
) -> None:
    secret = "private-builder-detail"

    def fail_build(**_kwargs):
        mqtt_stub.events.append("build")
        raise ValueError(secret)

    with pytest.raises(service._WorkerFirstRuntimeFatalError) as captured:
        service.WorkerService(
            host="mqtt",
            process_isolation=False,
            require_effect_contracts=True,
            worker_first_runtime_factory=fail_build,
        )

    assert str(captured.value) == "worker_first_runtime_bootstrap_failed"
    assert secret not in str(captured.value)
    assert secret not in caplog.text
    assert "class=ValueError reason=unclassified" in caplog.text
    assert (
        "Worker-first bootstrap abort: "
        "code=worker_first_runtime_bootstrap_failed"
    ) in caplog.text
    assert "connect" not in mqtt_stub.events


@pytest.mark.parametrize(
    ("failure", "kwargs"),
    (
        ("client_error", {}),
        ("credentials_error", {"username": "worker"}),
        ("tls_error", {"tls": True}),
    ),
)
def test_canary_mqtt_setup_failure_is_fixed_before_builder(
    mqtt_stub,
    failure,
    kwargs,
) -> None:
    secret = f"private-{failure}-detail"
    setattr(mqtt_stub.state, failure, ValueError(secret))
    built = []

    with pytest.raises(service._WorkerFirstRuntimeFatalError) as captured:
        service.WorkerService(
            host="mqtt",
            process_isolation=False,
            require_effect_contracts=True,
            worker_first_runtime_factory=lambda **_kwargs: built.append(True),
            **kwargs,
        )

    assert str(captured.value) == "worker_first_mqtt_setup_failed"
    assert secret not in str(captured.value)
    assert built == []
    assert "connect" not in mqtt_stub.events


def test_canary_diagnostics_failure_releases_runtime_before_connect(
    mqtt_stub,
) -> None:
    secret = "private-diagnostics-detail"
    runtime = _BootstrapRuntime(
        mqtt_stub.events,
        diagnostics_error=ValueError(secret),
    )

    with pytest.raises(service._WorkerFirstRuntimeFatalError) as captured:
        service.WorkerService(
            host="mqtt",
            process_isolation=False,
            require_effect_contracts=True,
            worker_first_runtime_factory=lambda **_kwargs: runtime,
        )

    assert str(captured.value) == "worker_first_runtime_bootstrap_failed"
    assert secret not in str(captured.value)
    assert runtime.stop_calls == 1
    assert "runtime_stop" in mqtt_stub.events
    assert "connect" not in mqtt_stub.events


def test_canary_connect_failure_releases_runtime_and_is_fixed(
    mqtt_stub,
) -> None:
    secret = "private-connect-detail"
    mqtt_stub.state.connect_error = ValueError(secret)
    runtime = _BootstrapRuntime(mqtt_stub.events)

    with pytest.raises(service._WorkerFirstRuntimeFatalError) as captured:
        service.WorkerService(
            host="mqtt",
            process_isolation=False,
            require_effect_contracts=True,
            worker_first_runtime_factory=lambda **_kwargs: runtime,
        )

    assert str(captured.value) == "worker_first_mqtt_connect_failed"
    assert secret not in str(captured.value)
    assert runtime.stop_calls == 1
    assert mqtt_stub.events[-2:] == ["connect", "runtime_stop"]


def test_real_activation_chain_releases_fence_after_connect_failure(
    mqtt_stub,
    monkeypatch,
    tmp_path,
) -> None:
    from storm_tracker_worker import acquisition_evidence_lifecycle

    monkeypatch.setattr(
        acquisition_evidence_lifecycle.AcquisitionEvidenceLifecycleStore,
        "_default_directory_fsync",
        staticmethod(lambda _path: None),
    )
    config = _service_config(tmp_path / "root")
    receipt = initialize_worker_first_composition_authority(
        config.worker_first,
        expected_config_sha256=config.worker_first.fingerprint,
        clock=lambda: 1_700_000_000.0,
    )
    _path, environment = _write_config(
        tmp_path,
        _canonical(config.to_mapping()),
    )
    environment["STV3_WORKER_FIRST_AUTHORITY_SHA256"] = (
        receipt.authority_sha256
    )
    plan, kwargs = service._main_runtime_kwargs(environment)
    real_builder = service._build_worker_first_service_canary
    captured = {}

    def build(*args, **builder_kwargs):
        runtime = real_builder(*args, **builder_kwargs)
        captured["runtime"] = runtime
        captured["before"] = runtime.diagnostics()
        return runtime

    monkeypatch.setattr(service, "_build_worker_first_service_canary", build)
    mqtt_stub.state.connect_error = RuntimeError("private-connect-detail")
    runtime = None
    try:
        with pytest.raises(service._WorkerFirstRuntimeFatalError) as error:
            service.WorkerService(
                host="mqtt",
                receipt_path=None,
                **kwargs,
            )
        runtime = captured["runtime"]
        before = captured["before"]
        assert plan.composition_config == config
        assert str(error.value) == "worker_first_mqtt_connect_failed"
        assert before["ownership_fence_held"] is True

        after = runtime.diagnostics()
        assert after["state"] == "stopped"
        assert after["heavy_lane_active"] is False
        assert after["shutdown_complete"] is True
        assert after["ownership_fence_held"] is False
        assert after["coordinator_stop"]["terminal"] is True
        assert after["coordinator_stop"]["fence_held"] is False

        fence = WorkerFirstOwnershipFence(
            config.worker_first.paths.authority_file,
            expected_authority_sha256=receipt.authority_sha256,
        )
        lease = fence.acquire()
        lease.release()
    finally:
        runtime = runtime or captured.get("runtime")
        if runtime is not None:
            asyncio.run(runtime.async_stop())


def test_real_raw_capture_chain_releases_fence_after_connect_failure(
    mqtt_stub,
    monkeypatch,
    tmp_path,
) -> None:
    from storm_tracker_worker import acquisition_evidence_lifecycle
    from storm_tracker_worker import raw_capture_retention_guard
    from storm_tracker_worker.raw_capture_service_composition import (
        RawCaptureServiceConfig,
        RawCaptureServiceConfigError,
    )
    from test_worker_raw_capture_collector_scope import (
        _runtime_capacity_contract,
    )

    monkeypatch.setattr(
        acquisition_evidence_lifecycle.AcquisitionEvidenceLifecycleStore,
        "_default_directory_fsync",
        staticmethod(lambda _path: None),
    )
    monkeypatch.setattr(
        raw_capture_retention_guard.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(
            total=2**62,
            used=0,
            free=2**62,
        ),
    )
    canary = _service_config(tmp_path / "raw-root")
    capacity_profile, raw_store_limits = _runtime_capacity_contract()
    event_limits = BlitzortungEventIngressLimits()
    config = RawCaptureServiceConfig(
        worker_first=canary.worker_first,
        source_paths=canary.source_paths,
        capacity_profile=capacity_profile,
        raw_store_limits=raw_store_limits,
        blitzortung_control_root=(
            tmp_path / "raw-root" / "source" / "blitzortung-control"
        ).resolve(),
        blitzortung_raw_store_root=(
            tmp_path / "raw-root" / "source" / "blitzortung-raw"
        ).resolve(),
        blitzortung_raw_store_limits=RawArtifactLimits(),
        blitzortung_ingress_limits=event_limits,
        blitzortung_enabled=True,
        host_operational_free_reserve_bytes=2 * 1024 * 1024 * 1024,
    )
    truth = {
        "dashboard_output": False,
        "downstream_consumption_required": False,
        "meteorological_output": False,
        "raw_store_commit_is_terminal": True,
        "terminal_router_wired": False,
    }
    mapping = config.to_mapping()
    assert mapping["contract"] == "stv3-worker-raw-capture-service-config-v6"
    event_config = mapping["event_ingress"]["blitzortung"]
    assert event_config["control_root"] != event_config["raw_store_root"]
    assert event_config["raw_store_root"] != (
        mapping["acquisition"]["paths"]["raw_store_root"]
    )
    assert event_config["raw_store_limits"] == {
        "max_capture_entries": config.blitzortung_raw_store_limits.max_capture_entries,
        "max_capture_members": config.blitzortung_raw_store_limits.max_capture_members,
        "max_catalog_bytes": config.blitzortung_raw_store_limits.max_catalog_bytes,
        "max_object_bytes": config.blitzortung_raw_store_limits.max_object_bytes,
        "max_object_entries": config.blitzortung_raw_store_limits.max_object_entries,
        "max_receipt_entries": config.blitzortung_raw_store_limits.max_receipt_entries,
        "max_staging_entries": config.blitzortung_raw_store_limits.max_staging_entries,
        "max_total_bytes": config.blitzortung_raw_store_limits.max_total_bytes,
    }
    assert RawCaptureServiceConfig.from_mapping(mapping) == config
    storage_capacity = config.storage_capacity_diagnostics
    assert storage_capacity["cadence"]["ledger_accounted"] is True
    assert storage_capacity["event"]["ledger_accounted"] is False
    assert storage_capacity["event"][
        "control_staging_residue_max_bytes"
    ] == (event_limits.control_receipt_reserve + 2) * 128 * 1024
    assert storage_capacity["event"][
        "control_staging_residue_entry_capacity"
    ] == event_limits.control_receipt_reserve + 2
    assert storage_capacity["combined_persistent_volume"][
        "same_filesystem_capacity_claim"
    ] is True
    assert {key: mapping[key] for key in truth} == truth
    legacy = dict(mapping)
    legacy["contract"] = "stv3-worker-raw-capture-service-config-v1"
    with pytest.raises(
        RawCaptureServiceConfigError,
        match="raw_capture_config_authority_invalid",
    ):
        RawCaptureServiceConfig.from_mapping(legacy)
    for key, expected in truth.items():
        drifted = dict(mapping)
        drifted[key] = not expected
        with pytest.raises(
            RawCaptureServiceConfigError,
            match="raw_capture_config_authority_invalid",
        ):
            RawCaptureServiceConfig.from_mapping(drifted)
    missing_truth = dict(mapping)
    del missing_truth["terminal_router_wired"]
    with pytest.raises(
        RawCaptureServiceConfigError,
        match="raw_capture_config_schema_invalid",
    ):
        RawCaptureServiceConfig.from_mapping(missing_truth)
    aliased_event_store = deepcopy(mapping)
    aliased_event_store["event_ingress"]["blitzortung"][
        "raw_store_root"
    ] = mapping["acquisition"]["paths"]["raw_store_root"]
    with pytest.raises(
        RawCaptureServiceConfigError,
        match="raw_capture_event_ingress_path_alias",
    ):
        RawCaptureServiceConfig.from_mapping(aliased_event_store)
    drifted_claim = deepcopy(mapping)
    drifted_claim["event_ingress"]["blitzortung"][
        "capacity_claimed"
    ] = False
    with pytest.raises(
        RawCaptureServiceConfigError,
        match="raw_capture_config_authority_invalid",
    ):
        RawCaptureServiceConfig.from_mapping(drifted_claim)
    undersized_host_reserve = deepcopy(mapping)
    undersized_host_reserve["retention"][
        "host_operational_free_reserve_bytes"
    ] = 2 * 1024 * 1024 * 1024 - 1
    with pytest.raises(
        RawCaptureServiceConfigError,
        match="raw_capture_host_operational_free_reserve_invalid",
    ):
        RawCaptureServiceConfig.from_mapping(undersized_host_reserve)
    receipt = initialize_worker_first_composition_authority(
        config.worker_first,
        expected_config_sha256=config.worker_first.fingerprint,
        clock=lambda: 1_700_000_000.0,
    )
    disabled_config = replace(config, blitzortung_enabled=False)
    forbidden_factory_calls = []

    def forbidden_sidecar_factory(**kwargs):
        forbidden_factory_calls.append(kwargs)
        raise AssertionError("disabled_event_factory_must_not_run")

    held_fence = WorkerFirstOwnershipFence(
        disabled_config.worker_first.paths.authority_file,
        expected_authority_sha256=receipt.authority_sha256,
    ).acquire()
    try:
        with pytest.raises(
            RawCaptureServiceConfigError,
            match="raw_capture_event_ingress_authority_invalid",
        ):
            service._build_raw_capture_service(
                config,
                expected_config_sha256=config.fingerprint,
                expected_authority_sha256=receipt.authority_sha256,
                blitzortung_access_retention_authority=None,
                blitzortung_sidecar_factory=forbidden_sidecar_factory,
            )
        with pytest.raises(
            RawCaptureServiceConfigError,
            match="raw_capture_event_ingress_authority_forbidden",
        ):
            service._build_raw_capture_service(
                disabled_config,
                expected_config_sha256=disabled_config.fingerprint,
                expected_authority_sha256=receipt.authority_sha256,
                blitzortung_access_retention_authority="forbidden",
                blitzortung_sidecar_factory=forbidden_sidecar_factory,
            )
        assert forbidden_factory_calls == []
    finally:
        held_fence.release()

    disabled_runtime = service._build_raw_capture_service(
        disabled_config,
        expected_config_sha256=disabled_config.fingerprint,
        expected_authority_sha256=receipt.authority_sha256,
        blitzortung_access_retention_authority=None,
        blitzortung_sidecar_factory=forbidden_sidecar_factory,
    )
    try:
        disabled_diagnostics = disabled_runtime.diagnostics()
        disabled_event = disabled_diagnostics["event_ingress"][
            "blitzortung"
        ]
        assert disabled_event["enabled"] is False
        assert disabled_event["capacity_claimed"] is False
        assert disabled_event["sidecar"] is None
        assert forbidden_factory_calls == []
        disabled_storage = disabled_diagnostics["storage_capacity"]
        assert disabled_storage["event"] == {
            "capacity_claimed": False,
            "catalog_max_bytes": 0,
            "control_receipt_max_bytes": 0,
            "control_receipt_storage_max_bytes": 0,
            "control_staging_residue_entry_capacity": 0,
            "control_staging_residue_max_bytes": 0,
            "enabled": False,
            "ledger_accounted": False,
            "object_max_bytes": 0,
            "object_max_entries": 0,
        }
        disabled_cadence = disabled_storage["cadence"]
        assert disabled_storage["combined_persistent_volume"][
            "configured_max_bytes"
        ] == (
            disabled_cadence["object_max_bytes"]
            + disabled_cadence["catalog_max_bytes"]
        )
        assert disabled_diagnostics["retention_guard"]["capacity"][
            "configured_persistent_max_bytes"
        ] == (
            disabled_storage["combined_persistent_volume"][
                "configured_max_bytes"
            ]
            + disabled_diagnostics["retention_guard"]["capacity"][
                "cadence_ledger_catalog_max_bytes"
            ]
        )
        assert disabled_storage[
            "host_operational_free_reserve_bytes"
        ] == 2 * 1024 * 1024 * 1024
    finally:
        asyncio.run(disabled_runtime.async_stop())
    path = (tmp_path / "raw-input" / "config.json").resolve()
    path.parent.mkdir(parents=True)
    payload = _canonical(config.to_mapping())
    path.write_bytes(payload)
    environment = {
        "STV3_RUNTIME_MODE": "raw_capture_shadow",
        "STV3_WORKER_FIRST_CONFIG_PATH": str(path),
        "STV3_WORKER_FIRST_CONFIG_SHA256": config.fingerprint,
        "STV3_WORKER_FIRST_AUTHORITY_SHA256": receipt.authority_sha256,
        "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY": (
            "worker_authority.blitzortung_participant_raw_retention_v1"
        ),
    }
    plan, kwargs = service._main_runtime_kwargs(environment)
    real_builder = service._build_raw_capture_service
    captured = {}

    def build(*args, **builder_kwargs):
        runtime = real_builder(*args, **builder_kwargs)
        captured["runtime"] = runtime
        captured["before"] = runtime.diagnostics()
        return runtime

    monkeypatch.setattr(service, "_build_raw_capture_service", build)
    mqtt_stub.state.connect_error = RuntimeError("private-connect-detail")
    runtime = None
    try:
        with pytest.raises(service._WorkerFirstRuntimeFatalError) as error:
            service.WorkerService(host="mqtt", receipt_path=None, **kwargs)
        runtime = captured["runtime"]
        assert plan.composition_config == config
        assert str(error.value) == "worker_first_mqtt_connect_failed"
        assert captured["before"]["mode"] == "service_raw_capture_shadow"
        assert captured["before"]["contract"] == (
            "stv3-worker-raw-capture-runtime-v6"
        )
        assert {
            key: captured["before"][key] for key in truth
        } == truth
        assert captured["before"]["ownership_fence_held"] is True
        assert set(captured["before"]["control_job_types"]) == (
            service.RAW_CAPTURE_CONTROL_JOB_TYPES
        )
        configuration = captured["before"]["acquisition_configuration"]
        providers = captured["before"]["provider_configuration"]
        blocked = captured["before"]["blocked_catalog"]
        storage = captured["before"]["capture_storage"]
        assert configuration["configured_workload_ids"] == list(
            config.allowed_workload_ids
        )
        assert providers["configured_count"] == len(
            config.allowed_workload_ids
        )
        assert len(providers["workloads"]) == len(
            config.allowed_workload_ids
        )
        assert all(
            row["handler_configured"] is True
            and "captured" not in row
            for row in providers["workloads"]
        )
        assert blocked["count"] == len(blocked["workload_ids"])
        assert sum(blocked["reason_counts"].values()) == blocked["count"]
        assert storage == {
            "committed_bytes": 0,
            "committed_count": 0,
            "committed_entries": 0,
            "ledger_scope": "cadence_only_event_store_excluded",
            "reserved_bytes": 0,
            "reserved_count": 0,
            "reserved_entries": 0,
        }

        after = runtime.diagnostics()
        assert after["state"] == "stopped"
        assert after["heavy_lane_active"] is False
        assert after["shutdown_complete"] is True
        assert after["ownership_fence_held"] is False
    finally:
        if runtime is not None:
            asyncio.run(runtime.async_stop())


def test_canary_will_failure_releases_runtime_before_connect(
    mqtt_stub,
) -> None:
    secret = "private-will-detail"
    mqtt_stub.state.will_error = ValueError(secret)
    runtime = _BootstrapRuntime(mqtt_stub.events)

    with pytest.raises(service._WorkerFirstRuntimeFatalError) as captured:
        service.WorkerService(
            host="mqtt",
            process_isolation=False,
            require_effect_contracts=True,
            worker_first_runtime_factory=lambda **_kwargs: runtime,
        )

    assert str(captured.value) == "worker_first_runtime_bootstrap_failed"
    assert secret not in str(captured.value)
    assert runtime.stop_calls == 1
    assert "connect" not in mqtt_stub.events


def test_cleanup_failure_cannot_replace_fixed_bootstrap_fatal(
    mqtt_stub,
    monkeypatch,
) -> None:
    secret = "private-cleanup-detail"
    mqtt_stub.state.connect_error = ValueError("private-connect-detail")
    runtime = _BootstrapRuntime(mqtt_stub.events)

    def fail_cleanup(self):
        raise RuntimeError(secret)

    monkeypatch.setattr(
        service.WorkerService,
        "_stop_worker_first_runtime_before_run",
        fail_cleanup,
    )
    with pytest.raises(service._WorkerFirstRuntimeFatalError) as captured:
        service.WorkerService(
            host="mqtt",
            process_isolation=False,
            require_effect_contracts=True,
            worker_first_runtime_factory=lambda **_kwargs: runtime,
        )

    assert str(captured.value) == "worker_first_mqtt_connect_failed"
    assert secret not in str(captured.value)


@pytest.mark.parametrize(
    "missing",
    ("composition_config", "config_sha256", "authority_sha256"),
)
def test_incomplete_canary_plan_fails_before_factory_creation(
    monkeypatch,
    missing,
) -> None:
    values = {
        "worker_first_selected": True,
        "composition_config": object(),
        "config_sha256": "a" * 64,
        "authority_sha256": "b" * 64,
    }
    values[missing] = None
    monkeypatch.setattr(
        service,
        "load_worker_first_service_activation",
        lambda environment=None: SimpleNamespace(**values),
    )

    with pytest.raises(
        RuntimeError,
        match="^worker_first_activation_plan_incomplete$",
    ):
        service._main_runtime_kwargs({})


def test_loader_failure_never_constructs_service(monkeypatch) -> None:
    constructed = []

    def fail_loader(environment=None):
        raise WorkerFirstServiceActivationError("config_sha256_mismatch")

    monkeypatch.setattr(
        service,
        "load_worker_first_service_activation",
        fail_loader,
    )
    monkeypatch.setattr(
        service,
        "WorkerService",
        lambda **kwargs: constructed.append(kwargs),
    )

    with pytest.raises(
        WorkerFirstServiceActivationError,
        match="^config_sha256_mismatch$",
    ):
        service.main()

    assert constructed == []


@pytest.mark.parametrize("worker_first", (False, True))
def test_main_passes_exact_selected_root_and_registers_signals(
    monkeypatch,
    worker_first,
) -> None:
    plan = SimpleNamespace(worker_first_selected=worker_first)
    runtime_kwargs = (
        {
            "process_isolation": False,
            "require_effect_contracts": True,
            "worker_first_runtime_factory": object(),
        }
        if worker_first
        else {
            "job_handlers": service.PRODUCTION_JOB_HANDLERS,
            "job_effect_contracts": service.PRODUCTION_JOB_EFFECT_CONTRACTS,
            "require_effect_contracts": True,
            "operational_snapshot_runtime_factory": (
                service.OperationalSnapshotRuntime
            ),
        }
    )
    monkeypatch.setattr(
        service,
        "_main_runtime_kwargs",
        lambda: (plan, runtime_kwargs),
    )
    monkeypatch.setenv("STV3_MQTT_HOST", "mqtt")
    built = []

    class FakeService:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            built.append(self)

        def stop(self, *_args):
            pass

    signals = []
    monkeypatch.setattr(service, "WorkerService", FakeService)
    monkeypatch.setattr(
        service.signal,
        "signal",
        lambda kind, handler: signals.append((kind, handler)),
    )
    monkeypatch.setattr(
        service,
        "_run_service_main",
        lambda selected: built.append(("run", selected)),
    )

    service.main()

    instance = built[0]
    assert built[1] == ("run", instance)
    assert signals == [
        (signal.SIGTERM, instance.stop),
        (signal.SIGINT, instance.stop),
    ]
    assert instance.kwargs["host"] == "mqtt"
    assert instance.kwargs["require_effect_contracts"] is True
    assert instance.kwargs["receipt_path"] == (
        None
        if worker_first
        else "/data/storm_tracker_worker_receipts.sqlite3"
    )
    for key, value in runtime_kwargs.items():
        assert instance.kwargs[key] is value
    if worker_first:
        assert "job_handlers" not in instance.kwargs
        assert "job_effect_contracts" not in instance.kwargs
        assert "operational_snapshot_runtime_factory" not in instance.kwargs
        assert "autonomous_scheduler" not in instance.kwargs


def test_selection_test_has_no_live_or_deploy_primitives() -> None:
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add((node.module or "").split(".", 1)[0])
    assert imports.isdisjoint({"requests", "urllib", "socket", "subprocess"})
