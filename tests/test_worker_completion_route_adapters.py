from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

import test_worker_dwd_rv_completion_consumer as dwd_fixture

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    CONSUMPTION_RECEIPT_SCHEMA,
    CompletionLease,
    ConsumptionReceipt,
    desired_authority_proof,
)
from storm_tracker_worker.completion_route_adapters import (
    AdapterPhaseResult,
    CompletionRouteAdapterIntegrityError,
    DwdCompletionRouteAdapter,
    HeavyChildPermit,
    HeavyChildPermitError,
    PERMANENT_EVENT_REASONS,
    PermanentEventFailure,
    RawManifestCompletionRouteAdapter,
    ROUTE_CATALOG_CANONICAL,
    ROUTE_CATALOG_SHA256,
    ROUTE_ORDER,
    _composition_store_bindings,
    validate_completion_route_catalog,
)
from storm_tracker_worker.raw_manifest_audit_completion_consumer import (
    RawManifestAuditRunResult,
)
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.desired_state import DesiredStateStore
from storm_tracker_worker.terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
)
from storm_tracker_worker.worker_first_runtime import PipelineOutcome


EXPECTED_ROUTE_CATALOG_SHA256 = (
    "01aa4be0065d23ab246842aa7904b53578d81be70da82546f51fe6bcc154bd67"
)


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _desired(generation: int = 1):
    return parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": generation,
        "issued_at": 900.0 + generation,
        "previous_state_sha256": None,
        "producer_id": "completion-adapter-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Home",
            "kind": "home",
            "latitude": 51.04,
            "location_accuracy_m": None,
            "longitude": 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": 900.0,
            "target_id": "zone.home",
        }],
    })


def _lease(desired) -> CompletionLease:
    workload_id = "radar.dwd.radolan_rv"
    binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
    event_sha = _sha("dwd-event")
    return CompletionLease(
        event_id="completion-" + event_sha,
        event_sha256=event_sha,
        producer_receipt_sha256=_sha("producer"),
        acquisition_job_id="acq:dwd-adapter-test",
        consumer_id=binding.consumer_id,
        stream_id=binding.stream_id,
        stream_sequence=1,
        desired_state_sha256=desired.state_sha256,
        authority_sha256=desired_authority_proof(desired)["authority_sha256"],
        content_sha256=_sha("content"),
        size_bytes=10,
        attempt_count=1,
        lease_token="private-dwd-lease-token",
        lease_expires_at=1300.0,
        proof={},
        payload=b"private",
    )


def _consumption(lease: CompletionLease, output_sha256: str) -> ConsumptionReceipt:
    body = {
        "consumer_id": lease.consumer_id,
        "created_at": 1000.0,
        "event_id": lease.event_id,
        "event_sha256": lease.event_sha256,
        "output_sha256": output_sha256,
        "producer_receipt_sha256": lease.producer_receipt_sha256,
        "schema": CONSUMPTION_RECEIPT_SCHEMA,
        "stream_id": lease.stream_id,
        "stream_sequence": lease.stream_sequence,
    }
    receipt_sha = hashlib.sha256(_canonical(body)).hexdigest()
    return ConsumptionReceipt(
        receipt_sha,
        lease.event_id,
        lease.event_sha256,
        lease.producer_receipt_sha256,
        lease.consumer_id,
        lease.stream_id,
        lease.stream_sequence,
        output_sha256,
        1000.0,
    )


class DesiredSequence:
    def __init__(self, values):
        self.values = list(values)
        self.calls = 0

    def load_active(self):
        value = self.values[min(self.calls, len(self.values) - 1)]
        self.calls += 1
        return value


class FakeLifecycle:
    def __init__(self, lease, loads=None):
        self.lease = lease
        self.loads = list(loads or [lease])
        self.load_calls = 0
        self.ack_calls = 0
        self.reject_calls = 0

    def load_leased(self, event_id, lease_token):
        assert event_id == self.lease.event_id
        assert lease_token == self.lease.lease_token
        value = self.loads[min(self.load_calls, len(self.loads) - 1)]
        self.load_calls += 1
        return value

    def acknowledge(self, event_id, lease_token, *, output_sha256, now):
        self.ack_calls += 1
        return _consumption(self.lease, output_sha256)

    def reject_leased(
        self,
        event_id,
        lease_token,
        *,
        expected_event_sha256,
        reason_code,
        active_desired_state_sha256,
        now,
    ):
        self.reject_calls += 1
        return _consumption(self.lease, _sha("rejection"))


class FakeAuthority:
    def __init__(self):
        self.calls = 0
        self.corrupt = False
        self.verified = SimpleNamespace(
            output_sha256=_sha("output"),
            output_receipt_sha256=_sha("output-receipt"),
            process_receipt_sha256=_sha("process-receipt"),
            disposition="withheld",
        )

    def verify(self, *, lease, desired_state, outcome):
        self.calls += 1
        if self.corrupt:
            raise CompletionRouteAdapterIntegrityError(
                "DWD durable output changed after final barrier"
            )
        return self.verified


class FakeReproofStore:
    def __init__(self, authority):
        self.authority = authority
        self.calls = 0
        self.assert_calls = 0
        self.identity_sha256 = _sha("fake-dwd-output-store-identity")

    def assert_instance_held(self, expected_identity_sha256=None):
        self.assert_calls += 1
        if expected_identity_sha256 != self.identity_sha256:
            raise CompletionRouteAdapterIntegrityError(
                "DWD output store identity authority changed"
            )
        return self.identity_sha256

    @contextmanager
    def completion_reproof_lease(self, **kwargs):
        self.calls += 1
        if self.authority.corrupt:
            raise CompletionRouteAdapterIntegrityError(
                "DWD durable output changed before fenced reproof"
            )
        yield SimpleNamespace(
            output_sha256=kwargs["output_sha256"],
            durable_output_receipt_sha256=kwargs[
                "durable_output_receipt_sha256"
            ],
            durable_process_receipt_sha256=kwargs[
                "durable_process_receipt_sha256"
            ],
        )
        if self.authority.corrupt:
            raise CompletionRouteAdapterIntegrityError(
                "DWD durable output changed while reproof was fenced"
            )


def _dwd_adapter(*, lifecycle, desired_store, authority, fault=None, clock=None):
    adapter = object.__new__(DwdCompletionRouteAdapter)
    adapter.workload_id = "radar.dwd.radolan_rv"
    binding = TERMINAL_RAW_CONSUMER_CATALOG[adapter.workload_id]
    adapter.consumer_id = binding.consumer_id
    adapter.stream_id = binding.stream_id
    adapter.lease_seconds = 300.0
    adapter.fingerprint = _sha("dwd-adapter")
    adapter._lifecycle = lifecycle
    adapter._desired = desired_store
    adapter._authority = authority
    adapter._clock = clock or (lambda: 1000.0)
    adapter.clock = adapter._clock

    def process_leased(lease, desired, cancel_event):
        return PipelineOutcome(
            output_sha256=_sha("output"),
            disposition="withheld",
            fact_revision_staged=False,
            process_receipt_sha256=_sha("process-receipt"),
        )

    reproof_store = FakeReproofStore(authority)
    adapter._consumer = SimpleNamespace(
        process_leased=process_leased,
        _fault=(fault or (lambda _point: None)),
        output_store=reproof_store,
        process_runner=SimpleNamespace(authority_sha256=_sha("isolation")),
    )
    adapter._output_store_identity_sha256 = reproof_store.identity_sha256
    return adapter


def _permit(
    workload_id="radar.dwd.radolan_rv", phase="single"
):
    nonce = object()
    return (
        HeavyChildPermit(
            tick_sha256=_sha("tick"),
            expected_workload_id=workload_id,
            expected_phase=phase,
            nonce=nonce,
        ),
        nonce,
    )


COMPLETION_CORE_CRASH_CHILD = (
    Path(__file__).resolve().parent
    / "helpers"
    / "completion_core_os_exit73_child.py"
)
_DWD_ROUTE_ADAPTER_CRASH_POINTS = (
    "adapter.before_final_output_verify",
    "adapter.after_final_output_verify_before_barrier",
)


def _durable_dwd_adapter_environment(
    root: Path,
    *,
    fault_injector=None,
):
    """Build or reopen the fixture's exact persistent DWD route authorities."""

    env = dwd_fixture._environment(root, consumer_fault=fault_injector)
    desired_store = DesiredStateStore(
        (root / "route-adapter-desired.sqlite3").resolve()
    )
    active = desired_store.load_active()
    if active is None:
        desired_store.claim_producer(
            env["desired"].producer_id,
            "completion-route-adapter-crash-claim",
            claimed_at=dwd_fixture.EVALUATED_AT - 120.0,
        )
        desired_store.apply(
            env["desired"].as_dict(),
            received_at=dwd_fixture.EVALUATED_AT - 60.0,
        )
    else:
        assert active == env["desired"]
    env["desired_store"] = desired_store
    return env


def _real_dwd_route_adapter(env) -> DwdCompletionRouteAdapter:
    return DwdCompletionRouteAdapter(
        consumer=env["consumer"],
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired_store"],
        clock=env["clock"],
    )


def run_dwd_route_adapter_crash_child(
    root: Path,
    checkpoint: str,
    crash,
) -> None:
    assert checkpoint in _DWD_ROUTE_ADAPTER_CRASH_POINTS
    env = _durable_dwd_adapter_environment(root, fault_injector=crash)
    lease = env["lifecycle"].claim_next(
        dwd_fixture.CONSUMER_ID,
        dwd_fixture.STREAM_ID,
        "completion-route-crash-owner",
        lease_seconds=300.0,
        now=env["clock"](),
    )
    assert lease is not None
    permit, nonce = _permit()
    _real_dwd_route_adapter(env).begin(
        lease,
        permit=permit,
        permit_nonce=nonce,
        cancel_event=threading.Event(),
    )


def _run_dwd_route_adapter_crash(
    root: Path,
    checkpoint: str,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            sys.executable,
            str(COMPLETION_CORE_CRASH_CHILD),
            "route-adapter",
            str(root),
            checkpoint,
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        check=False,
        timeout=240.0,
    )


def test_route_catalog_is_exact_hash_bound_eleven_routes():
    validate_completion_route_catalog()


def test_route_binding_requires_persistent_completion_output_instance(tmp_path):
    path = (tmp_path / "output.sqlite3").resolve()
    path.touch()
    missing = SimpleNamespace(
        runner=SimpleNamespace(output_store=SimpleNamespace(path=path))
    )
    with pytest.raises(
        CompletionRouteAdapterIntegrityError,
        match="no exact instance SHA-256",
    ):
        _composition_store_bindings(missing)

    bound = SimpleNamespace(
        runner=SimpleNamespace(output_store=SimpleNamespace(
            path=path,
            store_instance_sha256=_sha("persistent-output-instance"),
        ))
    )
    bindings = _composition_store_bindings(bound)
    assert len(bindings) == 1
    assert bindings[0]["persistent_instance_authority"] is True
    assert bindings[0]["roles"] == ["runner.output_store"]
    assert len(bindings[0]["instance_authority_sha256"]) == 64
    assert ROUTE_CATALOG_SHA256 == EXPECTED_ROUTE_CATALOG_SHA256
    assert hashlib.sha256(ROUTE_CATALOG_CANONICAL).hexdigest() == ROUTE_CATALOG_SHA256
    assert ROUTE_ORDER == tuple(TERMINAL_RAW_CONSUMER_CATALOG)
    assert len(ROUTE_ORDER) == len(TERMINAL_RAW_CONSUMER_CATALOG)


def test_heavy_child_permit_is_one_shot_and_route_scoped():
    workload_id = ROUTE_ORDER[0]
    permit, nonce = _permit(workload_id)
    permit.consume(workload_id=workload_id, phase="single", nonce=nonce)
    assert permit.consumed
    with pytest.raises(HeavyChildPermitError):
        permit.consume(workload_id=workload_id, phase="single", nonce=nonce)
    second, second_nonce = _permit(workload_id)
    with pytest.raises(HeavyChildPermitError):
        second.consume(
            workload_id=workload_id, phase="single", nonce=object()
        )
    assert not second.consumed
    second.consume(
        workload_id=workload_id, phase="single", nonce=second_nonce
    )

    cross_route, cross_route_nonce = _permit(workload_id)
    with pytest.raises(HeavyChildPermitError):
        cross_route.consume(
            workload_id=ROUTE_ORDER[1],
            phase="single",
            nonce=cross_route_nonce,
        )
    assert not cross_route.consumed

    cross_phase, cross_phase_nonce = _permit(workload_id)
    with pytest.raises(HeavyChildPermitError):
        cross_phase.consume(
            workload_id=workload_id,
            phase="initial",
            nonce=cross_phase_nonce,
        )
    assert not cross_phase.consumed


@pytest.mark.parametrize(
    ("status", "phase"),
    [("idle", "single"), ("retryable", "unknown")],
)
def test_adapter_phase_result_rejects_runtime_status_or_phase_forgery(
    status, phase
):
    with pytest.raises(CompletionRouteAdapterIntegrityError):
        AdapterPhaseResult(
            status,
            "forged_result",
            ROUTE_ORDER[0],
            _sha("event"),
            phase,
            0,
        )


def test_raw_desired_race_before_child_reports_zero_and_leaves_permit():
    desired = _desired()
    workload_id = next(
        route
        for route in ROUTE_ORDER
        if TERMINAL_RAW_CONSUMER_CATALOG[route].consumer_kind
        == "raw_manifest_audit"
    )
    binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
    dwd_lease = _lease(desired)
    lease = replace(
        dwd_lease,
        event_id="completion-" + _sha("raw-event"),
        event_sha256=_sha("raw-event"),
        consumer_id=binding.consumer_id,
        stream_id=binding.stream_id,
        lease_token="private-raw-race-token",
    )
    adapter = object.__new__(RawManifestCompletionRouteAdapter)
    adapter.workload_id = workload_id
    adapter.consumer_id = binding.consumer_id
    adapter.stream_id = binding.stream_id
    adapter.lease_seconds = 300.0
    adapter.fingerprint = _sha("raw-adapter")
    adapter.clock = lambda: 1000.0

    class Desired:
        def load_active(self):
            return desired

    def begin_leased(held, cancel_event, *, before_child):
        # This is the consumer's second desired check racing after the
        # adapter-level check.  No before_child callback may run.
        return RawManifestAuditRunResult(
            "rejected",
            "desired_state_superseded_before_processing",
            held.event_id,
            consumption_receipt_sha256=_sha("raw-rejection-receipt"),
            phase_child_invocations=0,
        )

    adapter._consumer = SimpleNamespace(
        desired=Desired(),
        lifecycle=object(),
        _clock=lambda: 1000.0,
        begin_leased=begin_leased,
    )
    permit, nonce = _permit(workload_id, "initial")
    result = adapter.begin(
        lease,
        permit=permit,
        permit_nonce=nonce,
        cancel_event=threading.Event(),
    )
    assert result.status == "rejected"
    assert result.phase == "initial"
    assert result.child_invocations == 0
    assert not permit.consumed


def test_permanent_failure_reason_is_closed_but_not_quarantine_authority():
    desired = _desired()
    lease = _lease(desired)
    failure = PermanentEventFailure(
        lease=lease,
        reason_code="source_raw_integrity_failed",
        failure_stage="source_raw",
        route_profile_sha256=_sha("route-profile"),
    )
    assert set(PERMANENT_EVENT_REASONS) == {
        "canonical_evidence_integrity_failed",
        "completion_reference_integrity_failed",
        "decoder_output_integrity_failed",
        "source_raw_integrity_failed",
        "unsupported_completion_contract",
    }
    assert failure.quarantine_authorized is False
    with pytest.raises(ValueError):
        PermanentEventFailure(
            lease=lease,
            reason_code="deadline_expired",
            failure_stage="source_raw",
            route_profile_sha256=_sha("route-profile"),
        )


def test_dwd_three_barriers_double_verify_and_exact_receipt_before_ack():
    desired = _desired()
    lease = _lease(desired)
    lifecycle = FakeLifecycle(lease)
    desired_store = DesiredSequence([desired, desired, desired])
    authority = FakeAuthority()
    adapter = _dwd_adapter(
        lifecycle=lifecycle,
        desired_store=desired_store,
        authority=authority,
    )
    permit, nonce = _permit()
    result = adapter.begin(
        lease,
        permit=permit,
        permit_nonce=nonce,
        cancel_event=threading.Event(),
    )
    assert result.status == "acknowledged"
    assert result.child_invocations == 1
    assert lifecycle.load_calls == 3
    assert desired_store.calls == 3
    assert authority.calls == 1
    assert adapter._consumer.output_store.calls == 1
    assert adapter._consumer.output_store.assert_calls == 2
    assert lifecycle.ack_calls == 1
    assert result.consumption_receipt_sha256 == _consumption(
        lease, _sha("output")
    ).receipt_sha256


def test_dwd_desired_or_lease_flip_at_final_barrier_never_acknowledges():
    desired = _desired()
    changed = _desired(2)
    lease = _lease(desired)

    desired_lifecycle = FakeLifecycle(lease)
    desired_sequence = DesiredSequence([desired, desired, desired])

    def switch_desired(point):
        if point == "adapter.after_final_output_verify_before_barrier":
            desired_sequence.values[-1] = changed

    desired_adapter = _dwd_adapter(
        lifecycle=desired_lifecycle,
        desired_store=desired_sequence,
        authority=FakeAuthority(),
        fault=switch_desired,
    )
    permit, nonce = _permit()
    rejected = desired_adapter.begin(
        lease, permit=permit, permit_nonce=nonce,
        cancel_event=threading.Event(),
    )
    assert rejected.status == "rejected"
    assert desired_lifecycle.reject_calls == 1
    assert desired_lifecycle.ack_calls == 0

    changed_lease = replace(lease, attempt_count=2)
    lease_lifecycle = FakeLifecycle(lease, [lease, lease, changed_lease])
    lease_adapter = _dwd_adapter(
        lifecycle=lease_lifecycle,
        desired_store=DesiredSequence([desired, desired, desired]),
        authority=FakeAuthority(),
    )
    permit, nonce = _permit()
    with pytest.raises(
        CompletionRouteAdapterIntegrityError,
        match="parent-held lifecycle lease changed",
    ):
        lease_adapter.begin(
            lease, permit=permit, permit_nonce=nonce,
            cancel_event=threading.Event(),
        )
    assert lease_lifecycle.ack_calls == 0


def test_dwd_final_fault_boundary_catches_cancel_and_output_mutation():
    desired = _desired()
    lease = _lease(desired)

    cancellation = threading.Event()
    cancel_lifecycle = FakeLifecycle(lease)
    cancel_authority = FakeAuthority()

    def cancel_after_barrier(point):
        if point == "adapter.after_final_output_verify_before_barrier":
            cancellation.set()

    cancel_adapter = _dwd_adapter(
        lifecycle=cancel_lifecycle,
        desired_store=DesiredSequence([desired, desired, desired]),
        authority=cancel_authority,
        fault=cancel_after_barrier,
    )
    permit, nonce = _permit()
    cancelled = cancel_adapter.begin(
        lease, permit=permit, permit_nonce=nonce,
        cancel_event=cancellation,
    )
    assert cancelled.status == "retryable"
    assert cancelled.reason_code == "cancelled"
    assert cancel_authority.calls == 1
    assert cancel_lifecycle.ack_calls == 0

    mutation_lifecycle = FakeLifecycle(lease)
    mutation_authority = FakeAuthority()

    def mutate_after_barrier(point):
        if point == "adapter.before_final_output_verify":
            mutation_authority.corrupt = True

    mutation_adapter = _dwd_adapter(
        lifecycle=mutation_lifecycle,
        desired_store=DesiredSequence([desired, desired, desired]),
        authority=mutation_authority,
        fault=mutate_after_barrier,
    )
    permit, nonce = _permit()
    with pytest.raises(
        CompletionRouteAdapterIntegrityError,
        match="changed before fenced reproof",
    ):
        mutation_adapter.begin(
            lease, permit=permit, permit_nonce=nonce,
            cancel_event=threading.Event(),
        )
    assert mutation_authority.calls == 1
    assert mutation_lifecycle.ack_calls == 0


@pytest.mark.parametrize(
    "checkpoint",
    _DWD_ROUTE_ADAPTER_CRASH_POINTS,
    ids=(
        "before-final-output-verify",
        "after-final-output-verify-before-barrier",
    ),
)
def test_native_os_exit73_dwd_final_verify_reopens_and_acks_once(
    tmp_path: Path,
    checkpoint: str,
) -> None:
    root = (tmp_path / checkpoint.replace(".", "-")).resolve()
    root.mkdir()

    crashed = _run_dwd_route_adapter_crash(root, checkpoint)
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    env = _durable_dwd_adapter_environment(root)
    lease = env["lifecycle"].load_leased(
        env["producer"].event_id,
        f"{1:064x}",
    )
    assert lease is not None
    durable = env["outputs"].load(lease.event_id)
    assert durable is not None
    verified = env["outputs"].verify_for_completion(
        event_id=lease.event_id,
        event_sha256=lease.event_sha256,
        desired_state_sha256=env["desired"].state_sha256,
        authority_sha256=lease.authority_sha256,
        consumer_id=dwd_fixture.CONSUMER_ID,
        stream_id=dwd_fixture.STREAM_ID,
        durable_output_receipt_sha256=durable.receipt_sha256,
        durable_process_receipt_sha256=durable.process_receipt_sha256,
        process_isolation_authority_sha256=env["runner"].authority_sha256,
        output_sha256=durable.output_sha256,
        disposition=durable.disposition,
        staged_fact_revision=None,
    )
    assert verified.durable_output_receipt_sha256 == durable.receipt_sha256
    before = (
        durable,
        env["outputs"].diagnostics(),
        env["evidence"].stats(),
        env["artifacts"].object_count(),
        env["handoff"].stats(),
    )
    assert before[1]["healthy"] is True
    assert before[1]["receipts"] == 1
    assert before[2]["decoder_runs"] == 1
    assert before[3] == 1
    assert env["handoff"].integrity_check()["healthy"] is True
    crashed_lifecycle = env["lifecycle"].diagnostics()
    assert crashed_lifecycle["hot_completions"] == 1
    assert crashed_lifecycle["outbox_states"]["leased"] == 1
    assert crashed_lifecycle["outbox_states"]["consumed"] == 0

    permit, nonce = _permit()
    result = _real_dwd_route_adapter(env).begin(
        lease,
        permit=permit,
        permit_nonce=nonce,
        cancel_event=threading.Event(),
    )
    assert result.status == "acknowledged"
    assert result.output_sha256 == durable.output_sha256
    assert result.output_receipt_sha256 == durable.receipt_sha256
    assert result.process_receipt_sha256 == durable.process_receipt_sha256
    assert result.consumption_receipt_sha256 is not None
    consumed = env["lifecycle"].diagnostics()
    assert consumed["hot_completions"] == 1
    assert consumed["outbox_states"]["leased"] == 0
    assert consumed["outbox_states"]["consumed"] == 1
    assert env["outputs"].load(lease.event_id) == durable
    assert (
        env["outputs"].diagnostics(),
        env["evidence"].stats(),
        env["artifacts"].object_count(),
        env["handoff"].stats(),
    ) == before[1:]

    serialized = json.dumps(durable.as_dict(), sort_keys=True).casefold()
    assert str(root).casefold() not in serialized
    for forbidden in (
        "access_token",
        "api_key",
        "authorization",
        "lease_token",
        "password",
        "refresh_token",
        "http://",
        "https://",
    ):
        assert forbidden not in serialized

    replay = _durable_dwd_adapter_environment(root)
    assert replay["outputs"].load(lease.event_id) == durable
    assert replay["outputs"].diagnostics() == before[1]
    assert replay["evidence"].stats() == before[2]
    assert replay["artifacts"].object_count() == before[3]
    assert replay["handoff"].stats() == before[4]
    assert replay["handoff"].integrity_check()["healthy"] is True
    assert replay["lifecycle"].diagnostics()["hot_completions"] == 1
    assert replay["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 1
    assert replay["lifecycle"].claim_next(
        dwd_fixture.CONSUMER_ID,
        dwd_fixture.STREAM_ID,
        "no-growth-owner",
        lease_seconds=300.0,
        now=replay["clock"](),
    ) is None
