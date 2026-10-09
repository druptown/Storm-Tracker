"""Standalone contract tests for the worker-only Blitzortung RAW sidecar.

The suite uses a deterministic fake paho client.  It never opens a socket and
never decodes a Blitzortung payload: the only admitted outcome is an exact
schema-3 raw artifact plus privacy-safe control diagnostics.
"""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from storm_tracker_worker.blitzortung_event_ingress import (
    ACCESS_RETENTION_AUTHORITY_ID,
    BlitzortungEventIngressLimits,
)
from storm_tracker_worker.blitzortung_mqtt_raw_sidecar import (
    BROKER_HOST,
    BROKER_PORT,
    DIAGNOSTICS_SCHEMA,
    MAX_SUBSCRIPTION_TOPICS,
    RAW_SHADOW_LIMITATIONS,
    BlitzortungMqttRawSidecar,
    BlitzortungMqttRawSidecarConfigError,
    BlitzortungRegion,
    regional_subscription_topics,
)
from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore


DESIRED_SHA256 = hashlib.sha256(b"desired-state-v1").hexdigest()


class _Clock:
    def __init__(self, value: float = 1_800_000_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        self.value += 0.001
        return self.value


class _FakeClient:
    def __init__(self, client_id: str) -> None:
        self.client_id = client_id
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None
        self.connect_calls: list[tuple[str, int, int]] = []
        self.subscribe_calls: list[tuple[str, int]] = []
        self.unsubscribe_calls: list[str] = []
        self.disconnect_calls = 0
        self.loop_start_calls = 0
        self.loop_stop_calls = 0

    def connect_async(self, host: str, port: int, keepalive: int) -> int:
        self.connect_calls.append((host, port, keepalive))
        return 0

    def loop_start(self) -> None:
        self.loop_start_calls += 1

    def loop_stop(self) -> None:
        self.loop_stop_calls += 1

    def subscribe(self, topic: str, *, qos: int) -> tuple[int, int]:
        self.subscribe_calls.append((topic, qos))
        return (0, len(self.subscribe_calls))

    def unsubscribe(self, topic: str) -> tuple[int, int]:
        self.unsubscribe_calls.append(topic)
        return (0, len(self.unsubscribe_calls))

    def disconnect(self) -> int:
        self.disconnect_calls += 1
        return 0

    def fire_connect(self, reason_code: object = 0) -> None:
        assert callable(self.on_connect)
        self.on_connect(self, None, None, reason_code, None)

    def fire_disconnect(self, reason_code: object = 1) -> None:
        assert callable(self.on_disconnect)
        self.on_disconnect(self, None, None, reason_code, None)

    def fire_message(
        self,
        payload: object,
        *,
        topic: str,
        qos: object = 0,
        retain: object = False,
    ) -> None:
        assert callable(self.on_message)
        self.on_message(
            self,
            None,
            SimpleNamespace(
                payload=payload,
                topic=topic,
                qos=qos,
                retain=retain,
            ),
        )


class _ClientFactory:
    def __init__(self) -> None:
        self.clients: list[_FakeClient] = []

    def __call__(self, client_id: str) -> _FakeClient:
        client = _FakeClient(client_id)
        self.clients.append(client)
        return client


def _raw_limits() -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=1024 * 1024,
        max_total_bytes=8 * 1024 * 1024,
        max_object_entries=512,
        max_staging_entries=16,
        max_receipt_entries=2048,
        max_capture_entries=2048,
        max_capture_members=4,
        max_catalog_bytes=8 * 1024 * 1024,
    )


def _ingress_limits(**changes: Any) -> BlitzortungEventIngressLimits:
    base = BlitzortungEventIngressLimits(
        max_queue_entries=8,
        max_control_receipts=256,
        max_retained_payload_entries=128,
        max_retained_payload_bytes=4 * 1024 * 1024,
        max_sessions=64,
        control_receipt_reserve=8,
        claim_lease_seconds=30.0,
        lock_timeout_seconds=0.25,
    )
    return replace(base, **changes)


def _nonce_factory(seed: int = 1):
    counter = seed

    def factory(size: int) -> bytes:
        nonlocal counter
        assert size == 32
        counter += 1
        return bytes([counter % 251 + 1]) * size

    return factory


def _region(
    latitude: float = 51.05,
    longitude: float = 4.47,
    radius_km: float = 20.0,
) -> BlitzortungRegion:
    return BlitzortungRegion(latitude, longitude, radius_km)


def _sidecar(
    tmp_path: Path,
    *,
    authority: str | None = ACCESS_RETENTION_AUTHORITY_ID,
    ingress_limits: BlitzortungEventIngressLimits | None = None,
    raw_limits: RawArtifactLimits | None = None,
    client_factory: _ClientFactory | None = None,
    clock: _Clock | None = None,
    nonce_seed: int = 1,
) -> tuple[BlitzortungMqttRawSidecar, _ClientFactory, Path, Path]:
    clients = client_factory or _ClientFactory()
    control_root = (tmp_path / "blitzortung-control").resolve()
    raw_root = (tmp_path / "raw").resolve()
    sidecar = BlitzortungMqttRawSidecar(
        control_root=control_root,
        raw_store_root=raw_root,
        raw_store_limits=raw_limits or _raw_limits(),
        ingress_limits=ingress_limits or _ingress_limits(),
        access_retention_authority=authority,
        client_factory=clients,
        clock=clock or _Clock(),
        nonce_factory=_nonce_factory(nonce_seed),
        directory_fsync=lambda _path: None,
    )
    return sidecar, clients, control_root, raw_root


def _connect(
    sidecar: BlitzortungMqttRawSidecar,
    clients: _ClientFactory,
    regions: tuple[BlitzortungRegion, ...] = (_region(),),
) -> tuple[_FakeClient, tuple[str, ...]]:
    diagnostics = sidecar.start(
        regions,
        desired_state_sha256=DESIRED_SHA256,
    )
    assert diagnostics["state"] == "connecting"
    assert len(clients.clients) == 1
    client = clients.clients[0]
    assert client.connect_calls == [(BROKER_HOST, BROKER_PORT, 60)]
    client.fire_connect()
    topics = regional_subscription_topics(regions)
    assert client.subscribe_calls == [(topic, 0) for topic in topics]
    assert sidecar.diagnostics()["state"] == "running_exact_raw"
    return client, topics


def test_regional_topics_are_deterministic_bounded_and_never_global() -> None:
    regions = (_region(), _region(51.06, 4.48, 20.0))
    first = regional_subscription_topics(regions)
    second = regional_subscription_topics(reversed(regions))
    assert first == second == tuple(sorted(set(first)))
    assert 0 < len(first) <= MAX_SUBSCRIPTION_TOPICS
    assert "blitzortung/1.1/#" not in first
    assert all(topic.startswith("blitzortung/1.1/") for topic in first)
    assert all(topic.endswith("/#") for topic in first)


@pytest.mark.parametrize(
    "arguments",
    (
        (91.0, 4.0, 20.0),
        (51.0, 181.0, 20.0),
        (51.0, 4.0, 0.99),
        (51.0, 4.0, 500.01),
        (True, 4.0, 20.0),
    ),
)
def test_region_factory_fails_closed(arguments: tuple[object, object, object]) -> None:
    with pytest.raises(BlitzortungMqttRawSidecarConfigError):
        BlitzortungRegion(*arguments)  # type: ignore[arg-type]


def test_missing_authority_creates_no_store_client_or_network(tmp_path: Path) -> None:
    sidecar, clients, control_root, raw_root = _sidecar(
        tmp_path,
        authority=None,
    )
    diagnostics = sidecar.start(
        (_region(),),
        desired_state_sha256=DESIRED_SHA256,
    )
    assert diagnostics["state"] == "authority_unavailable"
    assert diagnostics["access_retention_authority"] == "unavailable"
    assert diagnostics["network_started"] is False
    assert clients.clients == []
    assert not control_root.exists()
    assert not raw_root.exists()


def test_wrong_authority_is_rejected_before_side_effects(tmp_path: Path) -> None:
    clients = _ClientFactory()
    with pytest.raises(
        BlitzortungMqttRawSidecarConfigError,
        match="blitzortung_access_retention_authority_invalid",
    ):
        _sidecar(
            tmp_path,
            authority="worker_authority.blitzortung_boolean_bypass",
            client_factory=clients,
        )
    assert clients.clients == []
    assert not (tmp_path / "blitzortung-control").exists()
    assert not (tmp_path / "raw").exists()


def test_empty_regions_wait_without_opening_storage_or_network(tmp_path: Path) -> None:
    sidecar, clients, control_root, raw_root = _sidecar(tmp_path)
    diagnostics = sidecar.start((), desired_state_sha256=DESIRED_SHA256)
    assert diagnostics["state"] == "waiting_for_regions"
    assert diagnostics["regional_subscription_count"] == 0
    assert clients.clients == []
    assert not control_root.exists()
    assert not raw_root.exists()


def test_exact_callback_bytes_are_durable_before_inert_completion(
    tmp_path: Path,
) -> None:
    sidecar, clients, _control_root, raw_root = _sidecar(tmp_path)
    client, topics = _connect(sidecar, clients)
    payload = b'not-json:\x00\xff{"lat":"must-not-be-parsed"}'
    delivery_topic = topics[0][:-1] + "event"
    client.fire_message(payload, topic=delivery_topic)

    diagnostics = sidecar.diagnostics()
    assert diagnostics["payload_parsed"] is False
    assert diagnostics["meteorological_admission"] is False
    assert diagnostics["queue_depth"] == 0
    assert diagnostics["retained_payload_entries"] == 1
    assert diagnostics["retained_payload_bytes"] == len(payload)
    assert diagnostics["callback_counts"]["inert_completions"] == 1
    assert diagnostics["ingress_counters"]["deliveries_accepted"] == 1
    assert diagnostics["ingress_counters"]["deliveries_completed"] == 1

    store = RawArtifactStore(raw_root, limits=_raw_limits())
    store.initialize()
    receipts = store.accounting_receipts()
    assert len(receipts) == 1
    _receipt, stored = store.load_receipt_bytes(receipts[0].receipt_id)
    assert stored == payload


def test_invalid_callback_is_a_durable_gap_and_not_a_weather_absence(
    tmp_path: Path,
) -> None:
    sidecar, clients, _control_root, raw_root = _sidecar(tmp_path)
    client, topics = _connect(sidecar, clients)
    client.fire_message(bytearray(b"mutable"), topic=topics[0][:-1] + "event")

    diagnostics = sidecar.diagnostics()
    assert diagnostics["state"] == "running_exact_raw"
    assert diagnostics["callback_counts"]["callback_failures"] == 1
    assert diagnostics["ingress_counters"]["drops"] == 1
    assert diagnostics["ingress_counters"]["transport_gaps"] == 1
    assert diagnostics["silence_interpretation"] == (
        "unknown_not_an_absence_observation"
    )
    assert "event_ingress_transport_gap_observed" in diagnostics["alarm_codes"]
    store = RawArtifactStore(raw_root, limits=_raw_limits())
    store.initialize()
    assert store.accounting_receipts() == ()


def test_unreceiptable_callback_refusal_latches_and_disconnects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sidecar, clients, _control_root, _raw_root = _sidecar(tmp_path)
    client, topics = _connect(sidecar, clients)
    authority = sidecar._authority
    assert authority is not None

    def fail_refusal(*_args: object, **_kwargs: object) -> str:
        raise OSError("fixture-control-write-failure")

    monkeypatch.setattr(authority, "record_callback_refusal", fail_refusal)
    client.fire_message(bytearray(b"mutable"), topic=topics[0][:-1] + "event")
    diagnostics = sidecar.diagnostics()
    assert diagnostics["state"] == "control_receipt_degraded"
    assert diagnostics["fault_latched"] is True
    assert diagnostics["connected"] is False
    assert diagnostics["last_error_code"] == (
        "event_ingress_refusal_receipt_failed"
    )
    assert client.disconnect_calls == 1


def test_retained_capacity_refusal_is_durable_and_fault_latched(
    tmp_path: Path,
) -> None:
    limits = _ingress_limits(max_retained_payload_bytes=3)
    sidecar, clients, _control_root, raw_root = _sidecar(
        tmp_path,
        ingress_limits=limits,
    )
    client, topics = _connect(sidecar, clients)
    delivery_topic = topics[0][:-1] + "event"
    client.fire_message(b"abc", topic=delivery_topic)
    client.fire_message(b"d", topic=delivery_topic)

    diagnostics = sidecar.diagnostics()
    assert diagnostics["state"] == "capacity_blocked"
    assert diagnostics["fault_latched"] is True
    assert diagnostics["last_error_code"] == "event_ingress_capacity_blocked"
    assert diagnostics["capacity"]["retained_payload_byte_headroom"] == 0
    assert diagnostics["ingress_counters"]["drops"] == 1
    assert diagnostics["ingress_counters"]["transport_gaps"] == 1
    assert client.disconnect_calls == 1
    store = RawArtifactStore(raw_root, limits=_raw_limits())
    store.initialize()
    assert len(store.accounting_receipts()) == 1


def test_disconnect_and_reconnect_are_explicit_unknown_coverage_gaps(
    tmp_path: Path,
) -> None:
    sidecar, clients, _control_root, _raw_root = _sidecar(tmp_path)
    client, _topics = _connect(sidecar, clients)
    client.fire_disconnect()
    disconnected = sidecar.diagnostics()
    assert disconnected["state"] == "disconnected_reconnecting"
    assert disconnected["connected"] is False
    assert disconnected["ingress_counters"]["transport_gaps"] == 1

    client.fire_connect()
    reconnected = sidecar.diagnostics()
    assert reconnected["state"] == "running_exact_raw"
    assert reconnected["connected"] is True
    assert reconnected["ingress_counters"]["sessions_started"] == 2
    assert reconnected["silence_interpretation"] == (
        "unknown_not_an_absence_observation"
    )


def test_region_update_rebinds_subscriptions_and_desired_state(tmp_path: Path) -> None:
    sidecar, clients, _control_root, _raw_root = _sidecar(tmp_path)
    client, old_topics = _connect(sidecar, clients)
    new_regions = (_region(50.85, 4.35, 15.0),)
    new_desired = hashlib.sha256(b"desired-state-v2").hexdigest()
    new_topics = regional_subscription_topics(new_regions)

    diagnostics = sidecar.update_regions(
        new_regions,
        desired_state_sha256=new_desired,
    )
    assert diagnostics["state"] == "running_exact_raw"
    assert diagnostics["regional_subscription_count"] == len(new_topics)
    assert set(client.unsubscribe_calls) == set(old_topics) - set(new_topics)
    subscribed_after_connect = [topic for topic, _qos in client.subscribe_calls]
    assert set(subscribed_after_connect[len(old_topics):]) == (
        set(new_topics) - set(old_topics)
    )
    assert diagnostics["ingress_counters"]["sessions_started"] == 2
    assert diagnostics["ingress_counters"]["sessions_ended"] == 1


def test_region_update_subscription_failure_latches_root_cause(
    tmp_path: Path,
) -> None:
    sidecar, clients, _control_root, _raw_root = _sidecar(tmp_path)
    client, _old_topics = _connect(sidecar, clients)

    def refuse_unsubscribe(topic: str) -> tuple[int, int]:
        client.unsubscribe_calls.append(topic)
        return (1, len(client.unsubscribe_calls))

    client.unsubscribe = refuse_unsubscribe  # type: ignore[method-assign]
    diagnostics = sidecar.update_regions(
        (_region(-33.86, 151.21, 20.0),),
        desired_state_sha256=hashlib.sha256(b"desired-state-rebind").hexdigest(),
    )

    assert diagnostics["state"] == "subscription_unavailable"
    assert diagnostics["fault_latched"] is True
    assert diagnostics["last_error_code"] == "event_ingress_subscription_failed"
    assert diagnostics["alarm_codes"] == [
        "event_ingress_fault_latched",
        "event_ingress_transport_gap_observed",
    ]
    assert client.disconnect_calls == 1

    client.fire_disconnect()
    after_disconnect = sidecar.diagnostics()
    assert after_disconnect["state"] == "subscription_unavailable"
    assert after_disconnect["last_error_code"] == (
        "event_ingress_subscription_failed"
    )
    assert after_disconnect["network_started"] is False


def test_empty_then_restored_regions_create_a_fresh_transport(tmp_path: Path) -> None:
    sidecar, clients, _control_root, _raw_root = _sidecar(tmp_path)
    first, _topics = _connect(sidecar, clients)
    waiting = sidecar.update_regions((), desired_state_sha256=DESIRED_SHA256)
    assert waiting["state"] == "waiting_for_regions"
    assert waiting["network_started"] is False
    assert first.disconnect_calls == 1

    restored = sidecar.update_regions(
        (_region(),),
        desired_state_sha256=DESIRED_SHA256,
    )
    assert restored["state"] == "connecting"
    assert len(clients.clients) == 2
    assert clients.clients[1] is not first


def test_transient_network_start_failure_can_retry_same_desired_state(
    tmp_path: Path,
) -> None:
    class _FailOnceFactory(_ClientFactory):
        def __call__(self, client_id: str) -> _FakeClient:
            client = super().__call__(client_id)
            if len(self.clients) == 1:
                client.connect_async = lambda *_args: 1  # type: ignore[method-assign]
            return client

    factory = _FailOnceFactory()
    sidecar, clients, _control_root, _raw_root = _sidecar(
        tmp_path,
        client_factory=factory,
    )
    first = sidecar.start(
        (_region(),),
        desired_state_sha256=DESIRED_SHA256,
    )
    assert first["state"] == "network_unavailable"
    assert first["fault_latched"] is False
    retried = sidecar.update_regions(
        (_region(),),
        desired_state_sha256=DESIRED_SHA256,
    )
    assert retried["state"] == "connecting"
    assert len(clients.clients) == 2


def test_restart_reopens_same_append_only_roots_and_preserves_raw(
    tmp_path: Path,
) -> None:
    first, first_clients, _control_root, raw_root = _sidecar(
        tmp_path,
        nonce_seed=1,
    )
    client, topics = _connect(first, first_clients)
    client.fire_message(b"first-process", topic=topics[0][:-1] + "event")
    stopped = first.stop()
    assert stopped["state"] == "stopped"

    second, second_clients, _control_root, _raw_root = _sidecar(
        tmp_path,
        nonce_seed=100,
    )
    second_client, topics = _connect(second, second_clients)
    second_client.fire_message(b"second-process", topic=topics[0][:-1] + "event")
    diagnostics = second.diagnostics()
    assert diagnostics["retained_payload_entries"] == 2
    assert diagnostics["ingress_counters"]["deliveries_completed"] == 2
    store = RawArtifactStore(raw_root, limits=_raw_limits())
    store.initialize()
    assert len(store.accounting_receipts()) == 2
    second.stop()


def test_diagnostics_are_bounded_privacy_safe_and_expose_headroom(
    tmp_path: Path,
) -> None:
    sidecar, clients, control_root, raw_root = _sidecar(tmp_path)
    client, topics = _connect(sidecar, clients)
    secret_payload = b"unique-secret-shaped-payload"
    client.fire_message(secret_payload, topic=topics[0][:-1] + "event")
    diagnostics = sidecar.diagnostics()
    encoded = json.dumps(diagnostics, sort_keys=True)

    assert diagnostics["diagnostics_schema"] == DIAGNOSTICS_SCHEMA
    assert diagnostics["raw_shadow_limitations"] == list(RAW_SHADOW_LIMITATIONS)
    assert diagnostics["capacity"]["queue_headroom"] == (
        diagnostics["capacity"]["queue_capacity"]
    )
    assert diagnostics["capacity"]["control_intake_headroom"] >= 0
    assert diagnostics["capacity"]["retained_payload_byte_headroom"] >= 0
    assert len(encoded.encode("utf-8")) < 64 * 1024
    for forbidden in (
        secret_payload.decode("ascii"),
        str(control_root),
        str(raw_root),
        topics[0],
        DESIRED_SHA256,
        "password",
        "token",
    ):
        assert forbidden not in encoded


def test_stop_is_idempotent_and_closes_network_and_authority(tmp_path: Path) -> None:
    sidecar, clients, _control_root, _raw_root = _sidecar(tmp_path)
    client, _topics = _connect(sidecar, clients)
    first = sidecar.stop()
    second = sidecar.stop()
    assert first["state"] == second["state"] == "stopped"
    assert first["connected"] is second["connected"] is False
    assert client.disconnect_calls == 1
    assert client.loop_stop_calls == 1
    assert second["callback_counts"]["stops"] == 2


def test_sidecar_source_has_no_payload_decoder_or_ha_mqtt_raw_publish() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "storm_tracker_worker"
        / "blitzortung_mqtt_raw_sidecar.py"
    ).read_text(encoding="utf-8")
    lowered = source.lower()
    assert "json.loads" not in lowered
    assert "homeassistant" not in lowered
    assert "custom_components" not in lowered
    assert "client.publish" not in lowered
