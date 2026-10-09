from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import errno
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from threading import Event

import pytest

import storm_tracker_worker.blitzortung_event_ingress as ingress_module

from storm_tracker_worker.blitzortung_event_ingress import (
    ABSENCE_SEMANTICS,
    CONTRACT,
    INGRESS_ID,
    PROMOTION_GATES,
    TRANSPORT_COMPLETENESS,
    BlitzortungEventIngressAuthority,
    BlitzortungEventIngressBackpressure,
    BlitzortungEventIngressBlocked,
    BlitzortungEventIngressBusy,
    BlitzortungEventIngressCapacityError,
    BlitzortungEventIngressConflict,
    BlitzortungEventIngressIntegrityError,
    BlitzortungEventIngressLimits,
    BlitzortungEventIngressValidationError,
    BlitzortungRetentionDecision,
    blitzortung_event_ingress_candidate_contract,
)
from storm_tracker_worker.blitzortung_raw_ingress import BlitzortungMqttEnvelope
from storm_tracker_worker.raw_artifacts import (
    RawArtifactCapacityError,
    RawArtifactLimits,
    RawArtifactStore,
    RawArtifactIntegrityError,
)


NOW = 1_785_610_000.0
SYNC = lambda _path: None


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("ascii")).hexdigest()


def _raw(
    tmp_path: Path,
    *,
    max_object_bytes: int = 1024 * 1024,
) -> RawArtifactStore:
    store = RawArtifactStore(
        (tmp_path / "raw").resolve(),
        limits=RawArtifactLimits(
            max_object_bytes=max_object_bytes,
            max_total_bytes=4 * 1024 * 1024,
            max_object_entries=128,
            max_staging_entries=8,
            max_receipt_entries=256,
            max_capture_entries=256,
            max_capture_members=4,
            max_catalog_bytes=4 * 1024 * 1024,
        ),
    )
    store.initialize()
    return store


def _decision() -> BlitzortungRetentionDecision:
    return BlitzortungRetentionDecision.synthetic_fixture(
        source_terms_sha256=_sha("fixture-terms"),
        provenance_authority_sha256=_sha("fixture-provenance"),
        valid_from=NOW - 100.0,
        valid_until=NOW + 10_000.0,
    )


def _authority(
    tmp_path: Path,
    *,
    raw: RawArtifactStore | None = None,
    limits: BlitzortungEventIngressLimits | None = None,
    synthetic: bool = True,
    decision: BlitzortungRetentionDecision | None = None,
    fault=None,
) -> BlitzortungEventIngressAuthority:
    return BlitzortungEventIngressAuthority(
        (tmp_path / "event-ingress").resolve(),
        raw_store=raw or _raw(tmp_path),
        limits=limits,
        retention_decision=(_decision() if decision is None and synthetic else decision),
        synthetic_fixture_mode=synthetic,
        clock=lambda: NOW + 100.0,
        directory_fsync=SYNC,
        fault_injector=fault,
    )


def _begin(
    authority: BlitzortungEventIngressAuthority,
    *,
    session: str = "session-1",
    connected_at: float = NOW,
):
    return authority.begin_session(
        broker_identity_sha256=_sha("public-broker"),
        session_identity_sha256=_sha(session),
        desired_state_sha256=_sha("desired-state"),
        subscription_set_sha256=_sha("subscription-set"),
        connection_provenance_sha256=_sha(f"connection:{session}"),
        connected_at=connected_at,
    )


def _envelope(
    *,
    session: str = "session-1",
    delivery: str = "delivery-1",
    payload: bytes = b'{"lat":51.0,"lon":4.0}',
    received_at: float = NOW + 1.0,
) -> BlitzortungMqttEnvelope:
    return BlitzortungMqttEnvelope(
        payload=payload,
        broker_identity_sha256=_sha("public-broker"),
        session_identity_sha256=_sha(session),
        delivery_identity_sha256=_sha(delivery),
        topic="blitzortung/1.1/u/1/5/3/event",
        qos=0,
        retain=False,
        received_at=received_at,
        desired_state_sha256=_sha("desired-state"),
        subscription_set_sha256=_sha("subscription-set"),
    )


def test_candidate_is_inert_and_public_retention_fails_closed_with_receipt(tmp_path):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw, synthetic=False, decision=None)
    handle = _begin(authority)

    with pytest.raises(BlitzortungEventIngressBlocked, match="retention is blocked"):
        authority.offer_delivery(handle, _envelope(), callback_sequence=0)

    diagnostics = authority.diagnostics()
    assert diagnostics["ownership_status"] == "ownership_blocked"
    assert diagnostics["production_retention_authorized"] is False
    assert diagnostics["operational_admission"] is False
    assert diagnostics["counters"]["deliveries_received"] == 1
    assert diagnostics["counters"]["drops"] == 1
    assert diagnostics["counters"]["transport_gaps"] == 1
    assert raw.stats()["receipt_count"] == 0
    authority.close()

    reopened = _authority(tmp_path, raw=raw, synthetic=False, decision=None)
    assert reopened.diagnostics()["counters"]["drops"] == 1
    reopened.close()


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: replace(value, delivery_identity_sha256="A" * 64),
        lambda value: replace(value, payload=bytearray(b"mutable")),
        lambda value: replace(value, payload=b"x" * (256 * 1024 + 1)),
        lambda value: replace(value, topic="blitzortung/1.1/u/+/event"),
        lambda value: replace(value, topic="blitzortung/1.1/token/event"),
        lambda value: replace(value, qos=1),
        lambda value: replace(value, retain=0),
        lambda value: replace(value, received_at=float("nan")),
    ),
)
def test_invalid_envelope_fails_before_even_a_control_intent(tmp_path, mutation):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw, synthetic=False)
    handle = _begin(authority)
    before = authority.diagnostics()["control_receipts"]
    with pytest.raises(BlitzortungEventIngressValidationError):
        authority.offer_delivery(handle, mutation(_envelope()), callback_sequence=0)
    after = authority.diagnostics()["control_receipts"]
    assert after == before
    assert raw.stats()["receipt_count"] == 0
    authority.close()


def test_synthetic_exact_bytes_are_raw_before_queue_claim_and_never_weather(tmp_path):
    payload = b"\xff\x00not-json\x80exact"
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw)
    handle = _begin(authority)

    result = authority.offer_delivery(
        handle,
        _envelope(payload=payload),
        callback_sequence=0,
    )

    assert result.status == "accepted_shadow_raw"
    assert result.operational_admission is False
    assert result.absence_semantics == ABSENCE_SEMANTICS
    assert result.raw_reference is not None
    assert raw.read_bytes(result.raw_reference.receipt_id) == payload
    claim = authority.claim_next(
        consumer_identity_sha256=_sha("bounded-decoder-candidate"),
        now=NOW + 2.0,
    )
    assert claim is not None
    assert claim.raw_reference == result.raw_reference
    assert claim.operational_admission is False
    assert claim.absence_semantics == ABSENCE_SEMANTICS
    authority.complete_claim(claim, completed_at=NOW + 3.0)
    diagnostics = authority.diagnostics()
    assert diagnostics["queue_depth"] == 0
    assert diagnostics["counters"]["deliveries_accepted"] == 1
    assert diagnostics["counters"]["deliveries_completed"] == 1
    authority.close()


def test_queue_full_creates_durable_drop_without_second_raw_write(tmp_path):
    limits = BlitzortungEventIngressLimits(
        max_queue_entries=1,
        max_control_receipts=64,
        max_retained_payload_entries=8,
        max_retained_payload_bytes=1024 * 1024,
        max_sessions=8,
        control_receipt_reserve=6,
    )
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw, limits=limits)
    handle = _begin(authority)
    authority.offer_delivery(handle, _envelope(delivery="delivery-1"), callback_sequence=0)

    with pytest.raises(BlitzortungEventIngressBackpressure, match="queue full"):
        authority.offer_delivery(
            handle,
            _envelope(delivery="delivery-2", received_at=NOW + 2.0),
            callback_sequence=1,
        )
    diagnostics = authority.diagnostics()
    assert diagnostics["queue_depth"] == 1
    assert diagnostics["counters"]["drops"] == 1
    assert diagnostics["counters"]["transport_gaps"] == 1
    assert raw.stats()["receipt_count"] == 1
    authority.close()

    reopened = _authority(tmp_path, raw=raw, limits=limits)
    assert reopened.diagnostics()["queue_depth"] == 1
    assert reopened.diagnostics()["counters"]["drops"] == 1
    reopened.close()


def test_duplicate_out_of_order_and_callback_gap_are_accounted_separately(tmp_path):
    authority = _authority(tmp_path)
    handle = _begin(authority)
    first = _envelope(delivery="delivery-2")
    authority.offer_delivery(handle, first, callback_sequence=2)
    authority.offer_delivery(
        handle,
        _envelope(delivery="delivery-1", received_at=NOW + 2.0),
        callback_sequence=1,
    )
    duplicate = authority.offer_delivery(handle, first, callback_sequence=2)
    authority.offer_delivery(
        handle,
        _envelope(delivery="delivery-5", received_at=NOW + 3.0),
        callback_sequence=5,
    )

    counters = authority.diagnostics()["counters"]
    assert duplicate.status == "exact_duplicate"
    assert duplicate.duplicate is True
    assert counters["deliveries_accepted"] == 3
    assert counters["duplicates"] == 1
    assert counters["out_of_order"] == 1
    assert counters["sequence_gaps"] == 1
    assert counters["callback_positions_missing"] == 2
    assert counters["transport_gaps"] == 1
    authority.close()


def test_mutated_delivery_identity_is_a_durable_conflict_not_duplicate(tmp_path):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw)
    handle = _begin(authority)
    first = _envelope()
    authority.offer_delivery(handle, first, callback_sequence=0)

    with pytest.raises(BlitzortungEventIngressConflict, match="identity conflict"):
        authority.offer_delivery(
            handle,
            replace(first, payload=b"mutated"),
            callback_sequence=0,
        )
    counters = authority.diagnostics()["counters"]
    assert counters["conflicts"] == 1
    assert counters["drops"] == 1
    assert raw.stats()["receipt_count"] == 1
    authority.close()


def test_reconnect_increments_epoch_and_ends_prior_coverage_unknown(tmp_path):
    authority = _authority(tmp_path)
    first = _begin(authority, session="session-1")
    second = _begin(authority, session="session-2", connected_at=NOW + 5.0)

    assert first.epoch == 1
    assert second.epoch == 2
    diagnostics = authority.diagnostics()
    assert diagnostics["current_session_epoch"] == 2
    assert diagnostics["counters"]["sessions_started"] == 2
    assert diagnostics["counters"]["sessions_ended"] == 1
    assert diagnostics["counters"]["transport_gaps"] == 1
    assert diagnostics["silence_interpretation"] == "unknown_not_an_absence_observation"
    with pytest.raises(BlitzortungEventIngressConflict, match="current epoch"):
        authority.offer_delivery(first, _envelope(), callback_sequence=0)
    authority.close()


def test_invalid_reconnect_input_does_not_end_the_current_session(tmp_path):
    authority = _authority(tmp_path)
    current = _begin(authority)
    before = authority.diagnostics()

    with pytest.raises(BlitzortungEventIngressValidationError):
        authority.begin_session(
            broker_identity_sha256="not-a-sha",
            session_identity_sha256=_sha("invalid-replacement"),
            desired_state_sha256=_sha("desired-state"),
            subscription_set_sha256=_sha("subscription-set"),
            connection_provenance_sha256=_sha("invalid-provenance"),
            connected_at=NOW + 2.0,
        )

    after = authority.diagnostics()
    assert after["control_receipts"] == before["control_receipts"]
    assert after["current_session_epoch"] == current.epoch
    assert after["counters"]["sessions_ended"] == 0
    result = authority.offer_delivery(
        current,
        _envelope(),
        callback_sequence=0,
    )
    assert result.status == "accepted_shadow_raw"
    authority.close()


def test_session_epoch_capacity_does_not_end_the_current_session(tmp_path):
    limits = BlitzortungEventIngressLimits(
        max_queue_entries=2,
        max_control_receipts=32,
        max_retained_payload_entries=4,
        max_retained_payload_bytes=1024,
        max_sessions=1,
        control_receipt_reserve=6,
    )
    authority = _authority(tmp_path, limits=limits)
    current = _begin(authority)
    before = authority.diagnostics()["control_receipts"]

    with pytest.raises(BlitzortungEventIngressCapacityError, match="session epoch"):
        _begin(authority, session="forbidden-second", connected_at=NOW + 2.0)

    diagnostics = authority.diagnostics()
    assert diagnostics["control_receipts"] == before
    assert diagnostics["current_session_epoch"] == current.epoch
    assert diagnostics["counters"]["sessions_ended"] == 0
    authority.close()


def test_silent_connected_interval_never_becomes_no_lightning(tmp_path):
    authority = _authority(tmp_path)
    _begin(authority)
    serialized = json.dumps(authority.diagnostics(), sort_keys=True)
    assert "no_lightning" not in serialized
    assert "unknown_not_an_absence_observation" in serialized
    assert ABSENCE_SEMANTICS == "unknown_transport_coverage"
    assert TRANSPORT_COMPLETENESS == "best_effort_qos0_unacknowledged"
    authority.close()


def test_crash_after_intent_recovers_a_durable_gap_on_restart(tmp_path):
    armed = {"point": None}

    def fault(point: str) -> None:
        if point == armed["point"]:
            raise KeyboardInterrupt("simulated cancellation")

    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw, fault=fault)
    handle = _begin(authority)
    armed["point"] = "offer.after_intent"
    with pytest.raises(KeyboardInterrupt, match="cancellation"):
        authority.offer_delivery(handle, _envelope(), callback_sequence=0)
    authority.close()
    assert raw.stats()["receipt_count"] == 0

    reopened = _authority(tmp_path, raw=raw)
    diagnostics = reopened.diagnostics()
    assert diagnostics["pending_intents"] == 0
    assert diagnostics["counters"]["crash_recoveries"] == 1
    assert diagnostics["counters"]["drops"] == 1
    assert diagnostics["counters"]["transport_gaps"] >= 2
    reopened.close()


def test_crash_after_raw_persist_keeps_bytes_and_retry_is_idempotent(tmp_path):
    armed = {"point": None}

    def fault(point: str) -> None:
        if point == armed["point"]:
            raise KeyboardInterrupt("simulated process death")

    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw, fault=fault)
    handle = _begin(authority)
    envelope = _envelope()
    armed["point"] = "offer.after_raw_persist"
    with pytest.raises(KeyboardInterrupt, match="process death"):
        authority.offer_delivery(handle, envelope, callback_sequence=0)
    authority.close()
    assert raw.stats()["receipt_count"] == 1
    assert raw.stats()["capture_count"] == 1

    reopened = _authority(tmp_path, raw=raw)
    handle2 = _begin(reopened, connected_at=NOW + 200.0)
    result = reopened.offer_delivery(handle2, envelope, callback_sequence=0)
    assert result.status == "accepted_shadow_raw"
    assert raw.stats()["receipt_count"] == 1
    assert raw.stats()["capture_count"] == 1
    assert reopened.diagnostics()["counters"]["crash_recoveries"] == 1
    reopened.close()


def test_published_control_receipt_survives_return_path_crash(tmp_path):
    armed = {"point": None}

    def fault(point: str) -> None:
        if point == armed["point"]:
            raise OSError(errno.EIO, "after publish")

    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw, fault=fault)
    armed["point"] = "receipt.after_publish"
    with pytest.raises(OSError, match="after publish"):
        _begin(authority)
    with pytest.raises(BlitzortungEventIngressIntegrityError, match="reopen"):
        authority.diagnostics()
    authority.close()

    reopened = _authority(tmp_path, raw=raw)
    assert reopened.diagnostics()["counters"]["sessions_started"] == 1
    assert reopened.diagnostics()["counters"]["sessions_ended"] == 1
    reopened.close()


def test_partial_stage_is_removed_only_after_exclusive_restart(tmp_path):
    armed = {"point": None}

    def fault(point: str) -> None:
        if point == armed["point"]:
            raise OSError(errno.ENOSPC, "control filesystem full")

    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw, fault=fault)
    armed["point"] = "receipt.after_stage_fsync"
    with pytest.raises(OSError, match="filesystem full"):
        _begin(authority)
    stages = tuple((tmp_path / "event-ingress" / "staging").iterdir())
    assert len(stages) == 1
    authority.close()

    reopened = _authority(tmp_path, raw=raw)
    assert tuple((tmp_path / "event-ingress" / "staging").iterdir()) == ()
    assert reopened.diagnostics()["counters"]["sessions_started"] == 0
    reopened.close()


def test_raw_storage_full_creates_durable_refusal_and_propagates_error(tmp_path):
    raw = _raw(tmp_path, max_object_bytes=4)
    authority = _authority(tmp_path, raw=raw)
    handle = _begin(authority)

    with pytest.raises(RawArtifactCapacityError):
        authority.offer_delivery(
            handle,
            _envelope(payload=b"12345"),
            callback_sequence=0,
        )
    diagnostics = authority.diagnostics()
    assert diagnostics["counters"]["raw_failures"] == 1
    assert diagnostics["counters"]["drops"] == 1
    assert raw.stats()["receipt_count"] == 0
    authority.close()

    reopened = _authority(tmp_path, raw=raw)
    assert reopened.diagnostics()["counters"]["raw_failures"] == 1
    reopened.close()


def test_receipt_tamper_is_detected_before_recovery_or_mutation(tmp_path):
    authority = _authority(tmp_path)
    _begin(authority)
    authority.close()
    receipt = sorted((tmp_path / "event-ingress" / "receipts").iterdir())[1]
    payload = bytearray(receipt.read_bytes())
    payload[payload.index(b"session_started")] = ord("x")
    receipt.write_bytes(bytes(payload))

    with pytest.raises(BlitzortungEventIngressIntegrityError):
        _authority(tmp_path, raw=_raw(tmp_path / "second"))


def test_rehashed_claim_receipt_cannot_substitute_its_bound_consumer(tmp_path):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw)
    handle = _begin(authority)
    authority.offer_delivery(handle, _envelope(), callback_sequence=0)
    claim = authority.claim_next(
        consumer_identity_sha256=_sha("original-consumer"),
        now=NOW + 2.0,
    )
    assert claim is not None
    authority.close()

    receipt = sorted((tmp_path / "event-ingress" / "receipts").iterdir())[-1]
    document = json.loads(receipt.read_text(encoding="ascii"))
    assert document["kind"] == "delivery_claimed"
    document["data"]["consumer_identity_sha256"] = _sha("substituted-consumer")
    document.pop("receipt_sha256")
    body = json.dumps(
        document,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    forged_sha = hashlib.sha256(body).hexdigest()
    document["receipt_sha256"] = forged_sha
    payload = json.dumps(
        document,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    forged_path = receipt.with_name(
        f"{int(document['sequence']):020d}.{forged_sha}.json"
    )
    receipt.unlink()
    forged_path.write_bytes(payload)

    with pytest.raises(BlitzortungEventIngressIntegrityError, match="claim token"):
        _authority(tmp_path, raw=raw)


def test_reopen_rejects_retention_policy_or_raw_store_substitution(tmp_path):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw)
    authority.close()
    changed_decision = BlitzortungRetentionDecision.synthetic_fixture(
        source_terms_sha256=_sha("different-fixture-terms"),
        provenance_authority_sha256=_sha("fixture-provenance"),
        valid_from=NOW - 100.0,
        valid_until=NOW + 10_000.0,
    )
    with pytest.raises(BlitzortungEventIngressIntegrityError, match="profile differs"):
        _authority(tmp_path, raw=raw, decision=changed_decision)
    with pytest.raises(BlitzortungEventIngressIntegrityError, match="profile differs"):
        _authority(tmp_path, raw=_raw(tmp_path / "other-raw-root"))

    correct = _authority(tmp_path, raw=raw)
    correct.close()


def test_single_process_and_os_owner_is_exclusive(tmp_path):
    raw = _raw(tmp_path)
    first = _authority(tmp_path, raw=raw)
    with pytest.raises(BlitzortungEventIngressBusy, match="process-owned"):
        _authority(tmp_path, raw=raw)
    first.close()
    second = _authority(tmp_path, raw=raw)
    second.close()


def test_os_owner_lock_refuses_a_separate_process(tmp_path):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw)
    script = r"""
import hashlib
from pathlib import Path
import sys

from storm_tracker_worker.blitzortung_event_ingress import (
    BlitzortungEventIngressAuthority,
    BlitzortungEventIngressBusy,
    BlitzortungEventIngressIntegrityError,
    BlitzortungRetentionDecision,
)
from storm_tracker_worker.raw_artifacts import (
    RawArtifactLimits,
    RawArtifactStore,
)

def sha(text):
    return hashlib.sha256(text.encode("ascii")).hexdigest()

raw = RawArtifactStore(
    Path(sys.argv[2]),
    limits=RawArtifactLimits(
        max_object_bytes=1024 * 1024,
        max_total_bytes=4 * 1024 * 1024,
        max_object_entries=128,
        max_staging_entries=8,
        max_receipt_entries=256,
        max_capture_entries=256,
        max_capture_members=4,
        max_catalog_bytes=4 * 1024 * 1024,
    ),
)
raw.initialize()
decision = BlitzortungRetentionDecision.synthetic_fixture(
    source_terms_sha256=sha("fixture-terms"),
    provenance_authority_sha256=sha("fixture-provenance"),
    valid_from=1785610000.0 - 100.0,
    valid_until=1785610000.0 + 10000.0,
)
try:
    contender = BlitzortungEventIngressAuthority(
        Path(sys.argv[1]),
        raw_store=raw,
        retention_decision=decision,
        synthetic_fixture_mode=True,
        clock=lambda: 1785610000.0 + 100.0,
        directory_fsync=lambda _path: None,
    )
except (BlitzortungEventIngressBusy, BlitzortungEventIngressIntegrityError):
    print("exclusive-refusal")
    raise SystemExit(0)
else:
    contender.close()
    print("unexpected-owner")
    raise SystemExit(4)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str((tmp_path / "event-ingress").resolve()),
            str(raw.root.resolve()),
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=10.0,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "exclusive-refusal"
    authority.close()


def test_owner_rejects_a_pid_boundary(tmp_path, monkeypatch):
    authority = _authority(tmp_path)
    owner_pid = ingress_module.os.getpid()
    monkeypatch.setattr(ingress_module.os, "getpid", lambda: owner_pid + 1)
    with pytest.raises(BlitzortungEventIngressBusy, match="process boundary"):
        authority.diagnostics()
    monkeypatch.undo()
    authority.close()


def test_tampered_queued_raw_object_is_rejected_before_claim(tmp_path):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw)
    handle = _begin(authority)
    result = authority.offer_delivery(handle, _envelope(), callback_sequence=0)
    assert result.raw_reference is not None
    receipt = raw.load_receipt(result.raw_reference.receipt_id)
    assert receipt is not None
    Path(receipt.artifact_path).write_bytes(b"tampered-raw-object")

    with pytest.raises(RawArtifactIntegrityError):
        authority.claim_next(
            consumer_identity_sha256=_sha("consumer"),
            now=NOW + 2.0,
        )
    assert authority.diagnostics()["inflight_claims"] == 0
    assert authority.diagnostics()["queue_depth"] == 1
    authority.close()


def test_threaded_offers_are_serialized_and_bounded(tmp_path):
    limits = BlitzortungEventIngressLimits(
        max_queue_entries=8,
        max_control_receipts=128,
        max_retained_payload_entries=8,
        max_retained_payload_bytes=1024 * 1024,
        max_sessions=8,
        control_receipt_reserve=8,
    )
    authority = _authority(tmp_path, limits=limits)
    handle = _begin(authority)

    def offer(index: int):
        return authority.offer_delivery(
            handle,
            _envelope(
                delivery=f"thread-{index}",
                received_at=NOW + 1.0 + index,
            ),
            callback_sequence=index,
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = tuple(pool.map(offer, range(8)))
    assert all(result.status == "accepted_shadow_raw" for result in results)
    diagnostics = authority.diagnostics()
    assert diagnostics["queue_depth"] == 8
    assert diagnostics["counters"]["deliveries_accepted"] == 8
    authority.close()


def test_close_waits_for_an_inflight_durable_offer(tmp_path):
    offer_paused = Event()
    resume_offer = Event()
    close_started = Event()

    def fault(point: str) -> None:
        if point == "offer.after_intent":
            offer_paused.set()
            assert resume_offer.wait(timeout=5.0)

    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw, fault=fault)
    handle = _begin(authority)

    def close_authority() -> None:
        close_started.set()
        authority.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        offered = pool.submit(
            authority.offer_delivery,
            handle,
            _envelope(),
            callback_sequence=0,
        )
        assert offer_paused.wait(timeout=5.0)
        closing = pool.submit(close_authority)
        assert close_started.wait(timeout=5.0)
        assert not closing.done()
        resume_offer.set()
        assert offered.result(timeout=5.0).status == "accepted_shadow_raw"
        closing.result(timeout=5.0)

    reopened = _authority(tmp_path, raw=raw)
    diagnostics = reopened.diagnostics()
    assert diagnostics["queue_depth"] == 1
    assert diagnostics["counters"]["crash_recoveries"] == 0
    reopened.close()


def test_claim_is_released_after_consumer_cancellation_and_process_restart(tmp_path):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw)
    handle = _begin(authority)
    authority.offer_delivery(handle, _envelope(), callback_sequence=0)
    claim = authority.claim_next(
        consumer_identity_sha256=_sha("consumer"), now=NOW + 2.0
    )
    assert claim is not None
    authority.release_claim(
        claim,
        now=NOW + 3.0,
        reason="consumer_cancellation",
    )
    again = authority.claim_next(
        consumer_identity_sha256=_sha("consumer"), now=NOW + 4.0
    )
    assert again is not None
    authority.close()

    reopened = _authority(tmp_path, raw=raw)
    diagnostics = reopened.diagnostics()
    assert diagnostics["inflight_claims"] == 0
    assert diagnostics["queue_depth"] == 1
    assert diagnostics["counters"]["claims_recovered"] == 2
    final = reopened.claim_next(
        consumer_identity_sha256=_sha("consumer"), now=NOW + 200.0
    )
    assert final is not None
    reopened.complete_claim(final, completed_at=NOW + 201.0)
    reopened.close()


def test_active_claim_is_visible_only_to_its_bound_consumer(tmp_path):
    authority = _authority(tmp_path)
    handle = _begin(authority)
    authority.offer_delivery(handle, _envelope(), callback_sequence=0)
    consumer_a = _sha("consumer-a")
    claim = authority.claim_next(
        consumer_identity_sha256=consumer_a,
        now=NOW + 2.0,
    )
    assert claim is not None
    assert claim.consumer_identity_sha256 == consumer_a
    before = authority.diagnostics()["control_receipts"]

    with pytest.raises(BlitzortungEventIngressBusy, match="another consumer"):
        authority.claim_next(
            consumer_identity_sha256=_sha("consumer-b"),
            now=NOW + 3.0,
        )

    assert authority.diagnostics()["control_receipts"] == before
    assert authority.claim_next(
        consumer_identity_sha256=consumer_a,
        now=NOW + 3.0,
    ) == claim
    authority.complete_claim(claim, completed_at=NOW + 4.0)
    authority.close()


def test_forged_claim_fields_cannot_publish_a_store_bricking_completion(tmp_path):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw)
    handle = _begin(authority)
    authority.offer_delivery(handle, _envelope(), callback_sequence=0)
    claim = authority.claim_next(
        consumer_identity_sha256=_sha("consumer"),
        now=NOW + 2.0,
    )
    assert claim is not None
    before = authority.diagnostics()["control_receipts"]
    forged = replace(claim, queue_receipt_sha256=_sha("forged-queue-head"))

    with pytest.raises(BlitzortungEventIngressConflict, match="fields differ"):
        authority.complete_claim(forged, completed_at=NOW + 3.0)

    assert authority.diagnostics()["control_receipts"] == before
    authority.complete_claim(claim, completed_at=NOW + 3.0)
    authority.close()
    reopened = _authority(tmp_path, raw=raw)
    assert reopened.diagnostics()["queue_depth"] == 0
    reopened.close()


def test_expired_claim_cannot_complete_and_is_durably_requeued(tmp_path):
    limits = BlitzortungEventIngressLimits(
        max_queue_entries=2,
        max_control_receipts=64,
        max_retained_payload_entries=4,
        max_retained_payload_bytes=1024,
        max_sessions=4,
        control_receipt_reserve=6,
        claim_lease_seconds=5.0,
    )
    authority = _authority(tmp_path, limits=limits)
    handle = _begin(authority)
    authority.offer_delivery(handle, _envelope(), callback_sequence=0)
    claim = authority.claim_next(
        consumer_identity_sha256=_sha("consumer-a"),
        now=NOW + 2.0,
    )
    assert claim is not None

    with pytest.raises(BlitzortungEventIngressConflict, match="lease expired"):
        authority.complete_claim(claim, completed_at=claim.expires_at)

    diagnostics = authority.diagnostics()
    assert diagnostics["queue_depth"] == 1
    assert diagnostics["inflight_claims"] == 0
    assert diagnostics["counters"]["deliveries_completed"] == 0
    assert diagnostics["counters"]["claims_recovered"] == 1
    replacement = authority.claim_next(
        consumer_identity_sha256=_sha("consumer-b"),
        now=claim.expires_at + 1.0,
    )
    assert replacement is not None
    authority.complete_claim(
        replacement,
        completed_at=replacement.claimed_at + 1.0,
    )
    authority.close()


def test_completion_reverifies_raw_bytes_after_claim(tmp_path):
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw)
    handle = _begin(authority)
    result = authority.offer_delivery(handle, _envelope(), callback_sequence=0)
    assert result.raw_reference is not None
    claim = authority.claim_next(
        consumer_identity_sha256=_sha("consumer"),
        now=NOW + 2.0,
    )
    assert claim is not None
    receipt = raw.load_receipt(result.raw_reference.receipt_id)
    assert receipt is not None
    Path(receipt.artifact_path).write_bytes(b"tampered-after-claim")

    with pytest.raises(RawArtifactIntegrityError):
        authority.complete_claim(claim, completed_at=NOW + 3.0)

    diagnostics = authority.diagnostics()
    assert diagnostics["queue_depth"] == 1
    assert diagnostics["inflight_claims"] == 1
    assert diagnostics["counters"]["deliveries_completed"] == 0
    authority.release_claim(
        claim,
        now=NOW + 4.0,
        reason="consumer_failure",
    )
    authority.close()


def test_retained_payload_cap_refuses_without_evicting_prior_raw(tmp_path):
    limits = BlitzortungEventIngressLimits(
        max_queue_entries=2,
        max_control_receipts=64,
        max_retained_payload_entries=2,
        max_retained_payload_bytes=10,
        max_sessions=8,
        control_receipt_reserve=6,
    )
    raw = _raw(tmp_path)
    authority = _authority(tmp_path, raw=raw, limits=limits)
    handle = _begin(authority)
    authority.offer_delivery(
        handle, _envelope(delivery="a", payload=b"123456"), callback_sequence=0
    )
    with pytest.raises(BlitzortungEventIngressCapacityError, match="byte capacity"):
        authority.offer_delivery(
            handle,
            _envelope(delivery="b", payload=b"abcdef", received_at=NOW + 2.0),
            callback_sequence=1,
        )
    assert raw.stats()["receipt_count"] == 1
    diagnostics = authority.diagnostics()
    assert diagnostics["retained_payload_bytes"] == 6
    assert diagnostics["retention_mode"] == "append_only_no_reclamation"
    authority.close()


def test_control_capacity_latches_before_recovery_reserve_is_consumed(tmp_path):
    limits = BlitzortungEventIngressLimits(
        max_queue_entries=2,
        max_control_receipts=16,
        max_retained_payload_entries=4,
        max_retained_payload_bytes=1024,
        max_sessions=8,
        control_receipt_reserve=4,
    )
    authority = _authority(tmp_path, limits=limits)
    for index in range(5):
        handle = _begin(authority, session=f"session-{index}", connected_at=NOW + index)
        authority.end_session(
            handle,
            disconnected_at=NOW + index + 0.5,
            reason="operator_stop",
        )
    handle = _begin(authority, session="capacity-stop", connected_at=NOW + 20.0)
    with pytest.raises(BlitzortungEventIngressCapacityError, match="reserve"):
        authority.end_session(
            handle,
            disconnected_at=NOW + 20.5,
            reason="operator_stop",
        )
    diagnostics = authority.diagnostics()
    assert diagnostics["control_capacity_latched"] is True
    assert diagnostics["control_receipts"] <= limits.max_control_receipts
    authority.close()


def test_diagnostics_are_payload_topic_path_and_identity_free(tmp_path):
    secret_like_payload = b"unique-private-looking-fixture-payload"
    authority = _authority(tmp_path)
    handle = _begin(authority, session="unique-session-name")
    authority.offer_delivery(
        handle,
        _envelope(session="unique-session-name", payload=secret_like_payload),
        callback_sequence=0,
    )
    serialized = json.dumps(authority.diagnostics(), sort_keys=True)
    assert secret_like_payload.decode("ascii") not in serialized
    assert "unique-session-name" not in serialized
    assert "blitzortung/1.1" not in serialized
    assert str(tmp_path) not in serialized
    assert _sha("public-broker") not in serialized
    assert "password" not in serialized.lower()
    authority.close()


def test_receipts_are_canonical_content_addressed_contiguous_and_hash_chained(tmp_path):
    authority = _authority(tmp_path)
    handle = _begin(authority)
    authority.offer_delivery(handle, _envelope(), callback_sequence=0)
    authority.end_session(
        handle, disconnected_at=NOW + 2.0, reason="broker_disconnect"
    )
    authority.close()

    previous = "0" * 64
    for sequence, path in enumerate(
        sorted((tmp_path / "event-ingress" / "receipts").iterdir()), start=1
    ):
        document = json.loads(path.read_text(encoding="ascii"))
        assert document["sequence"] == sequence
        assert document["previous_receipt_sha256"] == previous
        claimed = document.pop("receipt_sha256")
        canonical = json.dumps(
            document,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        assert hashlib.sha256(canonical).hexdigest() == claimed
        assert claimed in path.name
        previous = claimed


def test_candidate_contract_is_content_bound_and_lists_exact_promotion_gates():
    contract = blitzortung_event_ingress_candidate_contract()
    assert contract["contract"] == CONTRACT
    assert contract["ingress_id"] == INGRESS_ID
    assert contract["ownership_status"] == "ownership_blocked"
    assert contract["production_retention_authorized"] is False
    assert contract["network_wired"] is False
    assert contract["decoder_wired"] is False
    assert tuple(contract["promotion_gates"]) == PROMOTION_GATES
    body = dict(contract)
    claimed = body.pop("contract_sha256")
    canonical = json.dumps(
        body,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    assert hashlib.sha256(canonical).hexdigest() == claimed


def test_module_has_no_network_decoder_service_ha_or_live_wiring():
    repo = Path(__file__).resolve().parents[1]
    module_path = repo / "storm_tracker_worker" / "blitzortung_event_ingress.py"
    source = module_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")

    def qualified_name(node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            parent = qualified_name(node.value)
            return f"{parent}.{node.attr}" if parent else node.attr
        return ""

    forbidden_import_roots = {
        "custom_components",
        "homeassistant",
        "paho",
        "socket",
        "urllib.request",
    }
    assert not any(
        imported == root or imported.startswith(f"{root}.")
        for imported in imports
        for root in forbidden_import_roots
    )
    calls = {
        qualified_name(node.func)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
    }
    assert not any(
        call in {"asyncio.Queue", "mqtt.Client", "urlopen"}
        or call.startswith(("paho.", "socket.", "urllib.request."))
        for call in calls
    )
    assert not any(name.endswith("service") for name in imports)
    for live_path in (
        repo / "storm_tracker_worker" / "service.py",
        repo / "storm_tracker_worker" / "acquisition_runtime.py",
        repo / "custom_components" / "storm_tracker_v3" / "__init__.py",
    ):
        assert "blitzortung_event_ingress" not in live_path.read_text(
            encoding="utf-8"
        )
