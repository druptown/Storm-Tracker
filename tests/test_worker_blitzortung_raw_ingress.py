from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, fields, replace
import hashlib
import json
from pathlib import Path

import pytest

from storm_tracker_worker.blitzortung_raw_ingress import (
    ABSENCE_SEMANTICS,
    MAX_PAYLOAD_BYTES,
    METHOD,
    REFERENCE_SCHEMA,
    TRANSPORT_COMPLETENESS,
    BlitzortungMqttEnvelope,
    BlitzortungRawIngressValidationError,
    persist_blitzortung_mqtt_envelope,
)
from storm_tracker_worker.raw_artifacts import (
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCapacityError,
    RawArtifactConflictError,
    RawArtifactEntryCapacityError,
    RawArtifactIntegrityError,
    RawArtifactLimits,
    RawArtifactStore,
)


NOW = 1_785_610_123.25


def _envelope(payload: bytes = b'{"lat":51.0,"lon":4.0}') -> BlitzortungMqttEnvelope:
    return BlitzortungMqttEnvelope(
        payload=payload,
        broker_identity_sha256="a" * 64,
        session_identity_sha256="b" * 64,
        delivery_identity_sha256="c" * 64,
        topic="blitzortung/1.1/u/1/5/3/event",
        qos=0,
        retain=False,
        received_at=NOW,
        desired_state_sha256="d" * 64,
        subscription_set_sha256="e" * 64,
    )


def _store(tmp_path: Path, *, limits: RawArtifactLimits | None = None):
    result = RawArtifactStore((tmp_path / "raw").resolve(), limits=limits)
    result.initialize()
    return result


def _capture(tmp_path: Path, envelope: BlitzortungMqttEnvelope | None = None):
    store = _store(tmp_path)
    reference = persist_blitzortung_mqtt_envelope(
        store, _envelope() if envelope is None else envelope
    )
    return store, reference


def test_exact_payload_is_durable_before_any_interpretation(tmp_path):
    payload = b'\xff\x00not-json\x80{"lat":null}'
    store, reference = _capture(tmp_path, _envelope(payload))

    assert store.read_bytes(reference.receipt_id) == payload
    assert reference.schema == REFERENCE_SCHEMA
    assert reference.method == METHOD
    assert reference.raw_store_schema_version == RAW_ARTIFACT_SCHEMA_VERSION == 3
    assert reference.content_sha256 == hashlib.sha256(payload).hexdigest()
    assert reference.size_bytes == len(payload)
    assert reference.payload_parsed is False
    assert reference.meteorological_interpretation == "not_performed"
    assert reference.operational_admission is False
    assert reference.transport_completeness == TRANSPORT_COMPLETENESS
    assert reference.absence_semantics == ABSENCE_SEMANTICS
    assert reference.absence_semantics != "no_lightning"

    receipt = store.load_receipt(reference.receipt_id)
    provenance = receipt.provenance
    assert provenance == {
        "absence_semantics": ABSENCE_SEMANTICS,
        "broker_delivery_acknowledged": False,
        "broker_identity_sha256": "a" * 64,
        "capture_stage": "before_decode",
        "collector": "blitzortung_raw_ingress",
        "collector_version": "blitzortung-raw-ingress-v1",
        "delivery_identity_sha256": "c" * 64,
        "desired_state_sha256": "d" * 64,
        "meteorological_interpretation": "not_performed",
        "mqtt_protocol_profile": "mqtt_v3_1_1_delivery_envelope",
        "operational_admission": False,
        "payload_parsed": False,
        "qos": 0,
        "received_at": NOW,
        "representation": "upstream_native_bytes",
        "retain": False,
        "session_identity_sha256": "b" * 64,
        "source_transport": "mqtt",
        "subscription_set_sha256": "e" * 64,
        "topic": "blitzortung/1.1/u/1/5/3/event",
        "transport_completeness": TRANSPORT_COMPLETENESS,
    }
    capture = store.load_capture(reference.capture_id)
    assert capture.complete is True
    assert len(capture.members) == 1
    assert capture.members[0].role == "mqtt_payload"
    assert capture.members[0].receipt_id == reference.receipt_id
    assert capture.capture_receipt_sha256 == reference.capture_receipt_sha256
    assert capture.manifest_sha256 == reference.manifest_sha256


def test_reference_is_payload_path_and_secret_free_content_bound_and_immutable(
    tmp_path,
):
    payload = b"unique-exact-payload-that-must-not-escape"
    _, reference = _capture(tmp_path, _envelope(payload))
    metadata = reference.to_metadata()
    serialized = json.dumps(metadata, sort_keys=True, separators=(",", ":"))

    assert "payload" not in metadata
    assert "artifact_path" not in serialized
    assert payload.decode("ascii") not in serialized
    assert not any(
        name in {field.name for field in fields(reference)}
        for name in ("password", "token", "secret", "authorization", "api_key")
    )
    body = dict(metadata)
    claimed = body.pop("reference_sha256")
    canonical = json.dumps(
        body,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    assert claimed == hashlib.sha256(canonical).hexdigest()
    with pytest.raises(FrozenInstanceError):
        reference.topic = "changed"  # type: ignore[misc]
    with pytest.raises(
        BlitzortungRawIngressValidationError, match="referencechecksum"
    ):
        replace(reference, reference_sha256="0" * 64)
    with pytest.raises(
        BlitzortungRawIngressValidationError, match="referencechecksum"
    ):
        replace(reference, topic="blitzortung/1.1/u/1/5/4/event")
    with pytest.raises(
        BlitzortungRawIngressValidationError, match="verboden"
    ):
        replace(reference, operational_admission=True)
    metadata["topic"] = "changed"
    assert reference.topic != "changed"


def test_exact_retry_is_idempotent(tmp_path):
    store = _store(tmp_path)
    first = persist_blitzortung_mqtt_envelope(store, _envelope())
    retry = persist_blitzortung_mqtt_envelope(store, _envelope())

    assert retry == first
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: replace(value, payload=b"mutated"),
        lambda value: replace(value, topic="blitzortung/1.1/u/1/5/4/event"),
        lambda value: replace(value, received_at=NOW + 1),
        lambda value: replace(value, retain=True),
        lambda value: replace(value, desired_state_sha256="f" * 64),
        lambda value: replace(value, subscription_set_sha256="f" * 64),
    ),
)
def test_same_delivery_identity_with_mutated_fact_is_visible_conflict(
    tmp_path, mutation
):
    store = _store(tmp_path)
    persist_blitzortung_mqtt_envelope(store, _envelope())

    with pytest.raises(RawArtifactConflictError):
        persist_blitzortung_mqtt_envelope(store, mutation(_envelope()))
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1


def test_same_bytes_under_another_delivery_identity_are_distinct_receipts(tmp_path):
    store = _store(tmp_path)
    first = persist_blitzortung_mqtt_envelope(store, _envelope())
    second = persist_blitzortung_mqtt_envelope(
        store,
        replace(_envelope(), delivery_identity_sha256="f" * 64),
    )

    assert second.receipt_id != first.receipt_id
    assert second.capture_id != first.capture_id
    assert second.content_sha256 == first.content_sha256
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 2


def test_external_object_mutation_is_detected_on_retry(tmp_path):
    store, reference = _capture(tmp_path)
    artifact_path = Path(store.load_receipt(reference.receipt_id).artifact_path)
    artifact_path.write_bytes(b"tampered-object")

    with pytest.raises(RawArtifactIntegrityError):
        persist_blitzortung_mqtt_envelope(store, _envelope())


@pytest.mark.parametrize(
    "envelope,match",
    (
        (replace(_envelope(), payload=bytearray(b"bytes")), "immutable bytes"),
        (replace(_envelope(), payload=b"x" * (MAX_PAYLOAD_BYTES + 1)), "ingressgrens"),
        (replace(_envelope(), broker_identity_sha256="A" * 64), "lower-case SHA-256"),
        (replace(_envelope(), session_identity_sha256="a" * 63), "lower-case SHA-256"),
        (replace(_envelope(), delivery_identity_sha256=7), "lower-case SHA-256"),
        (replace(_envelope(), desired_state_sha256="g" * 64), "lower-case SHA-256"),
        (replace(_envelope(), subscription_set_sha256=""), "lower-case SHA-256"),
        (replace(_envelope(), qos=1), "QoS 0"),
        (replace(_envelope(), qos=True), "integer 0"),
        (replace(_envelope(), retain=0), "boolean"),
        (replace(_envelope(), received_at=float("nan")), "eindig"),
        (replace(_envelope(), received_at=True), "eindig"),
        (replace(_envelope(), received_at=-1), "eindig"),
        (replace(_envelope(), topic=""), "bytegrens"),
        (replace(_envelope(), topic="blitzortung/1.1/u/+/event"), "concreet"),
        (replace(_envelope(), topic="blitzortung/1.1/u/#"), "concreet"),
        (replace(_envelope(), topic="other/1.1/u/event"), "concreet"),
        (replace(_envelope(), topic="blitzortung/1.1/token/event"), "credential"),
        (replace(_envelope(), topic="blitzortung/1.1/u/evil\x00event"), "concreet"),
    ),
)
def test_invalid_or_unbounded_envelope_fails_before_store_work(
    tmp_path, envelope, match
):
    store = _store(tmp_path)
    with pytest.raises(BlitzortungRawIngressValidationError, match=match):
        persist_blitzortung_mqtt_envelope(store, envelope)
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["capture_count"] == 0


def test_wrong_argument_types_fail_closed(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(BlitzortungRawIngressValidationError, match="envelope"):
        persist_blitzortung_mqtt_envelope(store, object())  # type: ignore[arg-type]
    with pytest.raises(BlitzortungRawIngressValidationError, match="raw_store"):
        persist_blitzortung_mqtt_envelope(object(), _envelope())  # type: ignore[arg-type]


def test_empty_payload_is_preserved_but_still_not_interpreted(tmp_path):
    store, reference = _capture(tmp_path, _envelope(b""))
    assert reference.size_bytes == 0
    assert store.read_bytes(reference.receipt_id) == b""
    assert reference.meteorological_interpretation == "not_performed"
    assert reference.absence_semantics == "unknown_transport_coverage"


def test_raw_store_capacity_error_propagates_without_receipt_or_absence(tmp_path):
    limits = RawArtifactLimits(max_object_bytes=4, max_total_bytes=8)
    store = _store(tmp_path, limits=limits)
    with pytest.raises(RawArtifactCapacityError) as raised:
        persist_blitzortung_mqtt_envelope(store, _envelope(b"12345"))
    assert raised.value.scope == "object"
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["capture_count"] == 0


def test_capture_capacity_failure_is_explicit_and_retryable(tmp_path):
    limits = RawArtifactLimits(max_capture_entries=1)
    store = _store(tmp_path, limits=limits)
    persist_blitzortung_mqtt_envelope(store, _envelope())
    second = replace(_envelope(), delivery_identity_sha256="f" * 64)

    with pytest.raises(RawArtifactEntryCapacityError) as raised:
        persist_blitzortung_mqtt_envelope(store, second)
    assert raised.value.scope == "captures"
    # Exact bytes/receipt were already made durable, but no complete capture or
    # meteorological result was fabricated.  Identical retry remains possible
    # after an operator provides capacity.
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1


def test_storage_oserror_is_not_swallowed_or_relabelled(tmp_path, monkeypatch):
    store = _store(tmp_path)
    commit_called = False

    def fail_store(**_kwargs):
        raise OSError("simulated disk failure")

    def mark_commit(**_kwargs):
        nonlocal commit_called
        commit_called = True
        raise AssertionError("capture must not run after store failure")

    monkeypatch.setattr(store, "store", fail_store)
    monkeypatch.setattr(store, "commit_capture", mark_commit)
    with pytest.raises(OSError, match="disk failure"):
        persist_blitzortung_mqtt_envelope(store, _envelope())
    assert commit_called is False


def test_module_has_no_parser_transport_queue_or_runtime_wiring():
    repo = Path(__file__).resolve().parents[1]
    module_path = repo / "storm_tracker_worker" / "blitzortung_raw_ingress.py"
    source = module_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"loads", "load"}
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id not in {"Queue", "SimpleQueue"}

    assert imported <= {
        "__future__",
        "dataclasses",
        "hashlib",
        "json",
        "math",
        "re",
        "raw_artifacts",
    }
    lowered = source.lower()
    assert "asyncio.queue" not in lowered
    assert "put_nowait" not in lowered
    assert "urlopen" not in lowered
    assert "paho" not in lowered
    assert "mqtt.client" not in lowered
    assert "custom_components" not in lowered

    for live_path in (
        repo / "storm_tracker_worker" / "service.py",
        repo / "storm_tracker_worker" / "provider_capture_registry.py",
        repo / "custom_components" / "storm_tracker_v3" / "__init__.py",
    ):
        assert "blitzortung_raw_ingress" not in live_path.read_text(
            encoding="utf-8"
        )
