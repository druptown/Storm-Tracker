"""Pure, unwired binding between acquisition plans and the durable scheduler.

The bridge is deliberately limited to deterministic contract translation.  It
does not resolve credentials, contact providers, execute handlers, translate
terminal receipts, or register itself with the worker service.  A caller may
use :func:`build_acquisition_schedule_intents` as an
``AutonomousScheduler`` plan builder, but no runtime does so in this module.

The acquisition plan remains the semantic authority.  Scheduler payloads are
self-checksummed descriptions of its immutable intents and contain only
abstract credential-requirement identifiers, never credential values.  A
leased scheduler job can subsequently be reconstructed as the exact
``AcquisitionJob`` for the same cadence slot.  The resulting binding receipt
contains hashes and scheduling metadata only; it never repeats the scheduler
payload or the lease token.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Any, Mapping

from . import autonomous_scheduler as scheduler
from . import worker_acquisition_plan as acquisition


CONTRACT = "stv3-worker-acquisition-scheduler-bridge-v1"
PAYLOAD_CONTRACT = "stv3-worker-acquisition-schedule-payload-v1"
BINDING_RECEIPT_CONTRACT = "stv3-worker-acquisition-claim-binding-v1"
JOB_TYPE = "meteorological_acquisition"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class AcquisitionSchedulerBridgeError(ValueError):
    """Base error for a plan/scheduler binding that cannot be proven exact."""


class AcquisitionSchedulerCapacityError(AcquisitionSchedulerBridgeError):
    """The complete plan does not fit the scheduler's hard contract limits."""


class AcquisitionSchedulerConflictError(AcquisitionSchedulerBridgeError):
    """Two supposedly identical plan, intent, slot, job, or claim views drift."""


@dataclass(frozen=True, slots=True)
class AcquisitionClaimBindingReceipt:
    """Payload-free cryptographic cross-binding of one exact leased job."""

    plan_sha256: str
    snapshot_sha256: str
    intent_id: str
    intent_sha256: str
    autoscheduler_job_id: str
    autoscheduler_slot_index: int
    autoscheduler_scheduled_for: float
    autoscheduler_deadline_at: float
    autoscheduler_payload_sha256: str
    autoscheduler_claim_sha256: str
    autoscheduler_attempt_count: int
    autoscheduler_lease_token_sha256: str
    autoscheduler_lease_expires_at: float
    acquisition_job_id: str
    acquisition_job_sha256: str
    acquisition_slot_index: int
    acquisition_scheduled_for: float
    acquisition_deadline_at: float
    binding_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquisition_deadline_at": self.acquisition_deadline_at,
            "acquisition_job_id": self.acquisition_job_id,
            "acquisition_job_sha256": self.acquisition_job_sha256,
            "acquisition_scheduled_for": self.acquisition_scheduled_for,
            "acquisition_slot_index": self.acquisition_slot_index,
            "autoscheduler_attempt_count": self.autoscheduler_attempt_count,
            "autoscheduler_claim_sha256": self.autoscheduler_claim_sha256,
            "autoscheduler_deadline_at": self.autoscheduler_deadline_at,
            "autoscheduler_job_id": self.autoscheduler_job_id,
            "autoscheduler_lease_expires_at": self.autoscheduler_lease_expires_at,
            "autoscheduler_lease_token_sha256": (
                self.autoscheduler_lease_token_sha256
            ),
            "autoscheduler_payload_sha256": self.autoscheduler_payload_sha256,
            "autoscheduler_scheduled_for": self.autoscheduler_scheduled_for,
            "autoscheduler_slot_index": self.autoscheduler_slot_index,
            "binding_sha256": self.binding_sha256,
            "contract": BINDING_RECEIPT_CONTRACT,
            "intent_id": self.intent_id,
            "intent_sha256": self.intent_sha256,
            "plan_sha256": self.plan_sha256,
            "snapshot_sha256": self.snapshot_sha256,
        }


@dataclass(frozen=True, slots=True)
class BoundAcquisitionClaim:
    """The exact acquisition job and its payload-free claim binding receipt."""

    acquisition_job: acquisition.AcquisitionJob
    binding_receipt: AcquisitionClaimBindingReceipt


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as err:
        raise AcquisitionSchedulerConflictError(
            "bridgecontract is niet-canonieke JSON"
        ) from err


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _finite_float(value: Any, name: str) -> float:
    if type(value) is not float or not math.isfinite(value) or value < 0.0:
        raise AcquisitionSchedulerConflictError(f"{name} is geen eindige float")
    return value


def _schedule_payload(
    plan: acquisition.AcquisitionPlan,
    intent: acquisition.AcquisitionIntent,
) -> dict[str, Any]:
    base = {
        "abstract_requirement_ids": list(intent.secret_requirement_ids),
        "budget": intent.budget.as_dict(),
        "cadence_seconds": intent.cadence_seconds,
        "contract": PAYLOAD_CONTRACT,
        "country_codes": list(intent.country_codes),
        "deadline_seconds": intent.deadline_seconds,
        "intent_id": intent.intent_id,
        "intent_sha256": intent.intent_sha256,
        "plan_sha256": plan.plan_sha256,
        "priority": intent.priority,
        "product_id": intent.product_id,
        "provider_id": intent.provider_id,
        "region_ids": list(intent.region_ids),
        "satellite_id": intent.satellite_id,
        "scope_id": intent.scope_id,
        "snapshot_sha256": plan.snapshot_sha256,
        "target_ids": list(intent.target_ids),
        "workload_class": intent.workload_class,
        "workload_id": intent.workload_id,
    }
    payload = dict(base)
    payload["bridge_payload_sha256"] = _sha256(base)
    return payload


def _preflight_scheduler_capacity(
    intents: tuple[scheduler.ScheduleIntent, ...],
) -> None:
    """Mirror both documented scheduler byte guards before any outbox call.

    The scheduler still validates the returned values itself.  This preflight
    exists so an oversized acquisition plan is rejected as one whole value;
    it never returns a dispatchable prefix.
    """

    if len(intents) > scheduler.MAX_PLAN_INTENTS:
        raise AcquisitionSchedulerCapacityError(
            "acquisitieplan overschrijdt MAX_PLAN_INTENTS"
        )
    seen: set[str] = set()
    total_bytes = 0
    plan_view: list[dict[str, Any]] = []
    for intent in intents:
        if intent.intent_id in seen:
            raise AcquisitionSchedulerConflictError(
                "acquisitieplan bevat een dubbel scheduler-intent"
            )
        seen.add(intent.intent_id)
        payload_bytes = _canonical_json(intent.payload)
        if len(payload_bytes) > scheduler.MAX_INTENT_PAYLOAD_BYTES:
            raise AcquisitionSchedulerCapacityError(
                "acquisitie-intent overschrijdt MAX_INTENT_PAYLOAD_BYTES"
            )
        total_bytes += len(payload_bytes) + 256
        if total_bytes > scheduler.MAX_PLAN_BYTES:
            raise AcquisitionSchedulerCapacityError(
                "acquisitieplan overschrijdt het incrementele schedulerbudget"
            )
        plan_view.append({
            "cadence_seconds": intent.cadence_seconds,
            "deadline_seconds": intent.deadline_seconds,
            "intent_id": intent.intent_id,
            "job_type": intent.job_type,
            "payload": intent.payload,
            "priority": intent.priority,
        })
    plan_view.sort(key=lambda item: item["intent_id"])
    if len(_canonical_json(plan_view)) > scheduler.MAX_PLAN_BYTES:
        raise AcquisitionSchedulerCapacityError(
            "canoniek schedulerplan overschrijdt MAX_PLAN_BYTES"
        )


def build_acquisition_schedule_intents(
    plan: acquisition.AcquisitionPlan,
) -> tuple[scheduler.ScheduleIntent, ...]:
    """Translate one validated plan completely or fail before outbox work.

    The tuple order is canonical by acquisition intent ID.  Cadence, deadline,
    and priority are copied exactly and never inferred by the scheduler.
    """

    try:
        valid_plan = acquisition.validate_worker_acquisition_plan(plan)
    except acquisition.AcquisitionPlanError as err:
        raise AcquisitionSchedulerConflictError(
            "acquisitieplan faalt de bronvalidatie"
        ) from err
    values = tuple(
        scheduler.ScheduleIntent(
            intent_id=intent.intent_id,
            job_type=JOB_TYPE,
            cadence_seconds=intent.cadence_seconds,
            deadline_seconds=intent.deadline_seconds,
            priority=intent.priority,
            payload=_schedule_payload(valid_plan, intent),
        )
        for intent in sorted(valid_plan.intents, key=lambda item: item.intent_id)
    )
    _preflight_scheduler_capacity(values)
    return values


def acquisition_scheduler_plan_builder(
    state: Any,
    *,
    collector_scope: str = acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT,
) -> tuple[scheduler.ScheduleIntent, ...]:
    """Pure adapter suitable for explicit injection into a scheduler instance.

    Importing this function has no side effect and does not install it anywhere.
    """

    try:
        plan = acquisition.build_worker_acquisition_plan(
            state,
            collector_scope=collector_scope,
        )
    except acquisition.AcquisitionPlanError as err:
        raise AcquisitionSchedulerConflictError(
            "desired-state kan geen exact acquisitieplan vormen"
        ) from err
    return build_acquisition_schedule_intents(plan)


def _expected_autoscheduler_job_id(
    *,
    snapshot_sha256: str,
    intent_id: str,
    cadence_seconds: int,
    slot_index: int,
) -> str:
    identity = {
        "cadence_seconds": cadence_seconds,
        "contract": scheduler.CONTRACT,
        "desired_state_sha256": snapshot_sha256,
        "intent_id": intent_id,
        "slot_index": slot_index,
    }
    return f"autosched:{_sha256(identity)}"


def _reconstruct_acquisition_job(
    plan: acquisition.AcquisitionPlan,
    *,
    intent_id: str,
    scheduled_for: float,
) -> acquisition.AcquisitionJob:
    # Use the acquisition module's public complete slot builder so this bridge
    # does not duplicate job identity or accidentally filter expired/secreted
    # jobs while performing durable accounting.
    try:
        jobs = acquisition.build_worker_acquisition_slot_jobs(
            plan,
            as_of=scheduled_for,
        )
    except acquisition.AcquisitionPlanError as err:
        raise AcquisitionSchedulerCapacityError(
            "acquisitiejob kan niet volledig via de publieke dispatcher worden herbouwd"
        ) from err
    matches = tuple(
        job
        for job in jobs
        if job.intent_id == intent_id and job.scheduled_for == scheduled_for
    )
    if len(matches) != 1:
        raise AcquisitionSchedulerConflictError(
            "schedulerclaim correspondeert niet met exact een acquisitiejob"
        )
    return matches[0]


def reconstruct_acquisition_job_for_slot(
    plan: acquisition.AcquisitionPlan,
    *,
    intent_id: str,
    scheduled_for: float,
) -> acquisition.AcquisitionJob:
    """Rebuild one historical/current slot job from its immutable plan facts."""

    return _reconstruct_acquisition_job(
        plan,
        intent_id=intent_id,
        scheduled_for=scheduled_for,
    )


def expected_autoscheduler_job_id(
    *,
    snapshot_sha256: str,
    intent_id: str,
    cadence_seconds: int,
    slot_index: int,
) -> str:
    """Expose the payload-free stable scheduler identity for accounting."""

    return _expected_autoscheduler_job_id(
        snapshot_sha256=snapshot_sha256,
        intent_id=intent_id,
        cadence_seconds=cadence_seconds,
        slot_index=slot_index,
    )


def _derive_bound_claim(
    plan: acquisition.AcquisitionPlan,
    claimed: scheduler.ClaimedJob,
) -> BoundAcquisitionClaim:
    try:
        valid_plan = acquisition.validate_worker_acquisition_plan(plan)
    except acquisition.AcquisitionPlanError as err:
        raise AcquisitionSchedulerConflictError(
            "claim verwijst naar een ongeldig acquisitieplan"
        ) from err
    if not isinstance(claimed, scheduler.ClaimedJob):
        raise AcquisitionSchedulerConflictError("claim heeft niet het verwachte type")

    scheduled_for = _finite_float(claimed.scheduled_for, "scheduled_for")
    deadline_at = _finite_float(claimed.deadline_at, "deadline_at")
    lease_expires_at = _finite_float(
        claimed.lease_expires_at, "lease_expires_at"
    )
    if claimed.desired_state_sha256 != valid_plan.snapshot_sha256:
        raise AcquisitionSchedulerConflictError("claim en snapshotchecksum verschillen")
    if claimed.job_type != JOB_TYPE:
        raise AcquisitionSchedulerConflictError("claim heeft een ander jobtype")
    if (
        isinstance(claimed.attempt_count, bool)
        or not isinstance(claimed.attempt_count, int)
        or not 1 <= claimed.attempt_count <= 1_000_000
    ):
        raise AcquisitionSchedulerConflictError("claim heeft een ongeldig attemptnummer")
    if not isinstance(claimed.lease_token, str) or _SHA256.fullmatch(
        claimed.lease_token
    ) is None:
        raise AcquisitionSchedulerConflictError("claim heeft een ongeldig leasetoken")
    if not scheduled_for < lease_expires_at <= deadline_at:
        raise AcquisitionSchedulerConflictError("claimlease valt buiten het jobvenster")

    expected_intents = build_acquisition_schedule_intents(valid_plan)
    if not isinstance(claimed.payload, Mapping):
        raise AcquisitionSchedulerConflictError("claimpayload is geen object")
    claimed_intent_id = claimed.payload.get("intent_id")
    matching = tuple(
        value for value in expected_intents if value.intent_id == claimed_intent_id
    )
    if len(matching) != 1:
        raise AcquisitionSchedulerConflictError(
            "claimpayload verwijst niet naar exact een planintent"
        )
    expected = matching[0]
    if dict(claimed.payload) != dict(expected.payload):
        raise AcquisitionSchedulerConflictError("claimpayload is van het plan gedrift")
    if claimed.priority != expected.priority:
        raise AcquisitionSchedulerConflictError("claimpriority is van het plan gedrift")
    slot_index = int(math.floor(scheduled_for / expected.cadence_seconds))
    if scheduled_for != float(slot_index * expected.cadence_seconds):
        raise AcquisitionSchedulerConflictError("claim ligt niet exact op een cadansslot")
    expected_deadline = scheduled_for + float(expected.deadline_seconds)
    if deadline_at != expected_deadline:
        raise AcquisitionSchedulerConflictError("claimdeadline is van de cadans gedrift")
    expected_job_id = _expected_autoscheduler_job_id(
        snapshot_sha256=valid_plan.snapshot_sha256,
        intent_id=expected.intent_id,
        cadence_seconds=expected.cadence_seconds,
        slot_index=slot_index,
    )
    if claimed.job_id != expected_job_id:
        raise AcquisitionSchedulerConflictError("autoscheduler-jobidentity is gedrift")

    acquisition_job = _reconstruct_acquisition_job(
        valid_plan,
        intent_id=expected.intent_id,
        scheduled_for=scheduled_for,
    )
    if (
        acquisition_job.plan_sha256 != valid_plan.plan_sha256
        or acquisition_job.snapshot_sha256 != valid_plan.snapshot_sha256
        or acquisition_job.intent_sha256 != claimed.payload["intent_sha256"]
        or acquisition_job.slot_index != slot_index
        or acquisition_job.scheduled_for != scheduled_for
        or acquisition_job.deadline_at != deadline_at
        or acquisition_job.priority != claimed.priority
    ):
        raise AcquisitionSchedulerConflictError(
            "autoschedulerclaim en acquisitiejob zijn niet exact gelijk gescheduled"
        )

    payload_sha256 = hashlib.sha256(_canonical_json(claimed.payload)).hexdigest()
    lease_token_sha256 = hashlib.sha256(claimed.lease_token.encode("ascii")).hexdigest()
    claim_identity = {
        "attempt_count": claimed.attempt_count,
        "deadline_at": deadline_at,
        "desired_state_sha256": claimed.desired_state_sha256,
        "job_id": claimed.job_id,
        "job_type": claimed.job_type,
        "lease_expires_at": lease_expires_at,
        "lease_token_sha256": lease_token_sha256,
        "payload_sha256": payload_sha256,
        "priority": claimed.priority,
        "scheduled_for": scheduled_for,
        "slot_index": slot_index,
    }
    claim_sha256 = _sha256(claim_identity)
    binding_payload = {
        "acquisition_deadline_at": acquisition_job.deadline_at,
        "acquisition_job_id": acquisition_job.job_id,
        "acquisition_job_sha256": acquisition_job.job_sha256,
        "acquisition_scheduled_for": acquisition_job.scheduled_for,
        "acquisition_slot_index": acquisition_job.slot_index,
        "autoscheduler_attempt_count": claimed.attempt_count,
        "autoscheduler_claim_sha256": claim_sha256,
        "autoscheduler_deadline_at": deadline_at,
        "autoscheduler_job_id": claimed.job_id,
        "autoscheduler_lease_expires_at": lease_expires_at,
        "autoscheduler_lease_token_sha256": lease_token_sha256,
        "autoscheduler_payload_sha256": payload_sha256,
        "autoscheduler_scheduled_for": scheduled_for,
        "autoscheduler_slot_index": slot_index,
        "contract": BINDING_RECEIPT_CONTRACT,
        "intent_id": acquisition_job.intent_id,
        "intent_sha256": acquisition_job.intent_sha256,
        "plan_sha256": acquisition_job.plan_sha256,
        "snapshot_sha256": acquisition_job.snapshot_sha256,
    }
    receipt = AcquisitionClaimBindingReceipt(
        plan_sha256=acquisition_job.plan_sha256,
        snapshot_sha256=acquisition_job.snapshot_sha256,
        intent_id=acquisition_job.intent_id,
        intent_sha256=acquisition_job.intent_sha256,
        autoscheduler_job_id=claimed.job_id,
        autoscheduler_slot_index=slot_index,
        autoscheduler_scheduled_for=scheduled_for,
        autoscheduler_deadline_at=deadline_at,
        autoscheduler_payload_sha256=payload_sha256,
        autoscheduler_claim_sha256=claim_sha256,
        autoscheduler_attempt_count=claimed.attempt_count,
        autoscheduler_lease_token_sha256=lease_token_sha256,
        autoscheduler_lease_expires_at=lease_expires_at,
        acquisition_job_id=acquisition_job.job_id,
        acquisition_job_sha256=acquisition_job.job_sha256,
        acquisition_slot_index=acquisition_job.slot_index,
        acquisition_scheduled_for=acquisition_job.scheduled_for,
        acquisition_deadline_at=acquisition_job.deadline_at,
        binding_sha256=_sha256(binding_payload),
    )
    return BoundAcquisitionClaim(acquisition_job, receipt)


def bind_claimed_acquisition_job(
    plan: acquisition.AcquisitionPlan,
    claimed: scheduler.ClaimedJob,
) -> BoundAcquisitionClaim:
    """Validate and cross-bind one scheduler lease to its acquisition job."""

    return _derive_bound_claim(plan, claimed)


def validate_bound_acquisition_claim(
    plan: acquisition.AcquisitionPlan,
    claimed: scheduler.ClaimedJob,
    bound: BoundAcquisitionClaim,
) -> BoundAcquisitionClaim:
    """Re-derive the complete binding and reject any altered field or hash."""

    if not isinstance(bound, BoundAcquisitionClaim):
        raise AcquisitionSchedulerConflictError("binding heeft niet het verwachte type")
    expected = _derive_bound_claim(plan, claimed)
    if bound != expected:
        raise AcquisitionSchedulerConflictError("claimbinding is van de brondata gedrift")
    return bound


__all__ = [
    "AcquisitionClaimBindingReceipt",
    "AcquisitionSchedulerBridgeError",
    "AcquisitionSchedulerCapacityError",
    "AcquisitionSchedulerConflictError",
    "BINDING_RECEIPT_CONTRACT",
    "BoundAcquisitionClaim",
    "CONTRACT",
    "JOB_TYPE",
    "PAYLOAD_CONTRACT",
    "acquisition_scheduler_plan_builder",
    "bind_claimed_acquisition_job",
    "build_acquisition_schedule_intents",
    "expected_autoscheduler_job_id",
    "reconstruct_acquisition_job_for_slot",
    "validate_bound_acquisition_claim",
]
