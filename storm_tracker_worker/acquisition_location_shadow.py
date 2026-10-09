"""Authority-to-child preparation for guarded raw-only capture.

This module is intentionally not imported by the service or autonomous
runtime.  It proves one current desired-state/plan/job/query-authority chain,
projects only the provider input that an exact native capture needs, and adds
a child-side wall-clock guard.  Coordinates occur only in that ephemeral
provider input; diagnostics and child results remain hash/counter only.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping

from . import acquisition_activation as activation
from . import acquisition_handler_child as child
from .acquisition_query_authority import (
    AcquisitionQueryAuthority,
    AcquisitionQueryCoverageError,
    build_acquisition_query_authority,
    safe_native_job_id,
    validate_acquisition_query_authority,
)
from .desired_state import (
    MAX_CLOCK_FUTURE_SKEW_SECONDS,
    MAX_DYNAMIC_POSITION_AGE_SECONDS,
    DesiredState,
    parse_desired_state,
)
from .raw_artifacts import RawArtifactLimits
from .worker_acquisition_plan import (
    AcquisitionJob,
    AcquisitionPlan,
    COLLECTOR_SCOPE_COVERAGE_RELEVANT,
    build_worker_acquisition_plan,
    build_worker_acquisition_slot_jobs,
    validate_worker_acquisition_job,
    validate_worker_acquisition_plan,
)


CONTRACT = "stv3-location-shadow-execution-guard-v1"
_DYNAMIC_KINDS = frozenset({"device_tracker", "person", "test"})
_PUBLIC_SOURCE_LOCATION_WORKLOADS = frozenset({
    "model.openmeteo.region",
    "model.openmeteo.italiameteo_icon2i.region",
    "radar.kmi.appv4_visual",
    "validation.geosphere.location",
    "validation.meteolux.location",
})
_LOCATION_WORKLOADS = frozenset({
    "ground.netatmo.region",
    "ground.synoptic.region",
    "radar.rainviewer.visual",
    *_PUBLIC_SOURCE_LOCATION_WORKLOADS,
})
_NO_QUERY_WORKLOADS = frozenset({
    "validation.italiameteo.dpc_sri_historical_catalog",
})
GUARDED_RAW_WORKLOAD_IDS = frozenset(
    set(_LOCATION_WORKLOADS) | set(_NO_QUERY_WORKLOADS)
)
_LIMIT_FIELDS = (
    "max_capture_entries",
    "max_capture_members",
    "max_catalog_bytes",
    "max_object_bytes",
    "max_object_entries",
    "max_receipt_entries",
    "max_staging_entries",
    "max_total_bytes",
)


class LocationShadowError(ValueError):
    """The isolated location batch cannot prove an exact execution grant."""


class LocationShadowBlockedError(LocationShadowError):
    """A provider needs explicit fanout or fresher location authority."""

    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


def _canonical(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            dict(value), allow_nan=False, ensure_ascii=False,
            separators=(",", ":"), sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as err:
        raise LocationShadowError("shadowgrant is niet canoniek JSON") from err


def _sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _finite_time(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LocationShadowError(f"{name} is geen geldige workertijd")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 2**53:
        raise LocationShadowError(f"{name} valt buiten het workertijdcontract")
    return result


def _validated_chain(
    desired_state: DesiredState,
    plan: AcquisitionPlan,
    job: AcquisitionJob,
    *,
    evaluated_at: float,
) -> tuple[DesiredState, AcquisitionPlan, AcquisitionJob, float]:
    if not isinstance(desired_state, DesiredState):
        raise LocationShadowError("shadowgrant vereist een canonieke desired-state")
    try:
        state = parse_desired_state(desired_state.canonical_bytes)
        valid_plan = validate_worker_acquisition_plan(plan)
        rebuilt = (
            build_worker_acquisition_plan(state)
            if valid_plan.collector_scope
            == COLLECTOR_SCOPE_COVERAGE_RELEVANT
            else build_worker_acquisition_plan(
                state,
                collector_scope=valid_plan.collector_scope,
            )
        )
        valid_job = validate_worker_acquisition_job(job)
    except Exception as err:
        raise LocationShadowError("desired-state/plan/job-keten is ongeldig") from err
    if state != desired_state or valid_plan != rebuilt:
        raise LocationShadowError("desired-state en acquisitieplan verschillen")
    evaluated = _finite_time(evaluated_at, "evaluated_at")
    matches = tuple(
        item
        for item in build_worker_acquisition_slot_jobs(valid_plan, as_of=evaluated)
        if item.job_id == valid_job.job_id
    )
    if matches != (valid_job,) or valid_job.snapshot_sha256 != state.state_sha256:
        raise LocationShadowError("shadowjob behoort niet tot het huidige slot")
    if not valid_job.scheduled_for <= evaluated < valid_job.deadline_at:
        raise LocationShadowBlockedError("job_deadline_unavailable")
    return state, valid_plan, valid_job, evaluated


def _dynamic_window(
    state: DesiredState,
    job: AcquisitionJob,
) -> tuple[float | None, float | None]:
    targets = {item.target_id: item for item in state.targets}
    dynamic = []
    for target_id in job.target_ids:
        target = targets.get(target_id)
        if target is None or not target.available:
            raise LocationShadowError("shadowjobtarget ontbreekt of is niet beschikbaar")
        if target.kind in _DYNAMIC_KINDS:
            if target.position_observed_at is None:
                raise LocationShadowBlockedError("dynamic_position_unavailable")
            dynamic.append(float(target.position_observed_at))
    if not dynamic:
        return None, None
    return (
        max(value - MAX_CLOCK_FUTURE_SKEW_SECONDS for value in dynamic),
        min(value + MAX_DYNAMIC_POSITION_AGE_SECONDS for value in dynamic),
    )


def _provider_payload(
    workload_id: str,
    authority: AcquisitionQueryAuthority,
) -> dict[str, Any]:
    try:
        if workload_id == "ground.netatmo.region":
            return authority.netatmo_payload()
        if workload_id == "ground.synoptic.region":
            return authority.synoptic_payload()
        if workload_id == "radar.rainviewer.visual":
            return authority.rainviewer_payload()
        if workload_id == "model.openmeteo.region":
            return authority.openmeteo_shadow_query_batch()
        if workload_id in {
            "model.openmeteo.italiameteo_icon2i.region",
            "radar.kmi.appv4_visual",
            "validation.geosphere.location",
            "validation.meteolux.location",
        }:
            return authority.public_source_region_payload()
    except AcquisitionQueryCoverageError as err:
        text = str(err).lower()
        if workload_id == "ground.netatmo.region" and (
            "datumgrens" in text or "pool" in text
        ):
            reason = "provider_geometry_fanout_required"
        elif workload_id == "radar.rainviewer.visual":
            reason = "provider_tile_fanout_required"
        else:
            reason = "provider_radius_fanout_required"
        raise LocationShadowBlockedError(reason) from err
    raise LocationShadowError("workload heeft geen locatieprojectie")


def _limits_mapping(limits: RawArtifactLimits) -> dict[str, int]:
    if not isinstance(limits, RawArtifactLimits):
        raise LocationShadowError("raw-storelimieten hebben het verkeerde type")
    result = {name: getattr(limits, name) for name in _LIMIT_FIELDS}
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0
           for value in result.values()):
        raise LocationShadowError("raw-storelimieten zijn niet positief en exact")
    return result


def _build_guarded_shadow_child_request(
    desired_state: DesiredState,
    plan: AcquisitionPlan,
    job: AcquisitionJob,
    *,
    query_authority: AcquisitionQueryAuthority | None,
    evaluated_at: float,
    resolver: activation.SecretResolver,
    raw_store_root: str | Path,
    raw_store_limits: RawArtifactLimits,
    live_activation: bool,
) -> dict[str, Any]:
    """Prepare one child request from immutable parent authorities."""

    state, valid_plan, valid_job, evaluated = _validated_chain(
        desired_state, plan, job, evaluated_at=evaluated_at
    )
    if valid_job.workload_id in _LOCATION_WORKLOADS:
        if not isinstance(query_authority, AcquisitionQueryAuthority):
            raise LocationShadowBlockedError("query_authority_unavailable")
        validate_acquisition_query_authority(query_authority)
        rebuilt = build_acquisition_query_authority(
            state, valid_plan, valid_job, evaluated_at=evaluated
        )
        if query_authority != rebuilt:
            raise LocationShadowError("query-authority is niet exact herleidbaar")
        payload = _provider_payload(valid_job.workload_id, rebuilt)
        authority_sha256: str | None = rebuilt.authority_sha256
        valid_from, valid_until = _dynamic_window(state, valid_job)
    elif valid_job.workload_id in _NO_QUERY_WORKLOADS:
        if query_authority is not None:
            raise LocationShadowError(
                "globale raw capture accepteert geen locatie-authority"
            )
        payload = {}
        authority_sha256 = None
        valid_from = valid_until = None
    else:
        raise LocationShadowError("job valt buiten de gesloten shadowbatch")

    if live_activation:
        decision = activation.assess_acquisition_job(
            valid_job,
            resolver=resolver,
        )
        if (
            decision.rule.handler_payload_contract in {
                "acquisition_query_authority_projection_v1",
                "openmeteo_target_query_batch_v1",
                "public_region_query_v1",
            }
            and decision.handler_payload not in ({}, None)
        ):
            raise LocationShadowError(
                "live activation materialiseerde onverwacht locatiegegevens"
            )
    elif valid_job.workload_id in _PUBLIC_SOURCE_LOCATION_WORKLOADS:
        decision = activation.assess_acquisition_job(
            valid_job, resolver=resolver
        )
    else:
        decision = activation.assess_location_shadow_candidate(
            valid_job, resolver=resolver, handler_payload=payload
        )
    if not decision.executable:
        raise LocationShadowBlockedError(decision.reason_code)
    payload_sha256 = hashlib.sha256(_canonical(payload)).hexdigest()
    native_job_id = safe_native_job_id(valid_job.job_id)
    guard_body = {
        "authority_sha256": authority_sha256,
        "contract": CONTRACT,
        "deadline_at": valid_job.deadline_at,
        "dynamic_position_valid_from": valid_from,
        "dynamic_position_valid_until": valid_until,
        "evaluated_at": evaluated,
        "job_sha256": valid_job.job_sha256,
        "native_job_id_sha256": hashlib.sha256(native_job_id.encode("ascii")).hexdigest(),
        "payload_sha256": payload_sha256,
        "scheduled_for": valid_job.scheduled_for,
    }
    guard = {**guard_body, "guard_sha256": _sha256(guard_body)}
    root = Path(raw_store_root)
    if not root.is_absolute():
        raise LocationShadowError("raw-storepad moet absoluut zijn")
    return {
        "acquisition_job_id": valid_job.job_id,
        "contract": child.CONTRACT,
        "execution_guard": guard,
        "handler_id": decision.rule.handler_id,
        "handler_payload": payload,
        "raw_store_limits": _limits_mapping(raw_store_limits),
        "raw_store_root": str(root),
        "workload_id": valid_job.workload_id,
    }


def build_guarded_shadow_child_request(
    desired_state: DesiredState,
    plan: AcquisitionPlan,
    job: AcquisitionJob,
    *,
    query_authority: AcquisitionQueryAuthority | None,
    evaluated_at: float,
    resolver: activation.SecretResolver,
    raw_store_root: str | Path,
    raw_store_limits: RawArtifactLimits,
) -> dict[str, Any]:
    """Prepare one inert candidate request for the isolated contract tests."""

    return _build_guarded_shadow_child_request(
        desired_state,
        plan,
        job,
        query_authority=query_authority,
        evaluated_at=evaluated_at,
        resolver=resolver,
        raw_store_root=raw_store_root,
        raw_store_limits=raw_store_limits,
        live_activation=False,
    )


def build_runtime_guarded_shadow_child_request(
    desired_state: DesiredState,
    plan: AcquisitionPlan,
    job: AcquisitionJob,
    *,
    evaluated_at: float,
    resolver: activation.SecretResolver,
    raw_store_root: str | Path,
    raw_store_limits: RawArtifactLimits,
) -> dict[str, Any]:
    """Build the only live request form for the guarded raw handlers.

    The caller supplies no coordinates and cannot supply a prebuilt query.
    This boundary rebuilds the desired-state/plan/job chain with the plan's
    collector scope, derives a fresh authority for the current job window and
    then rechecks that authority while constructing the execution guard.
    """

    state, valid_plan, valid_job, evaluated = _validated_chain(
        desired_state,
        plan,
        job,
        evaluated_at=evaluated_at,
    )
    if valid_job.workload_id not in GUARDED_RAW_WORKLOAD_IDS:
        raise LocationShadowError("job valt buiten de live guarded raw-set")
    query_authority = (
        build_acquisition_query_authority(
            state,
            valid_plan,
            valid_job,
            evaluated_at=evaluated,
        )
        if valid_job.workload_id in _LOCATION_WORKLOADS
        else None
    )
    return _build_guarded_shadow_child_request(
        state,
        valid_plan,
        valid_job,
        query_authority=query_authority,
        evaluated_at=evaluated,
        resolver=resolver,
        raw_store_root=raw_store_root,
        raw_store_limits=raw_store_limits,
        live_activation=True,
    )


__all__ = [
    "CONTRACT",
    "GUARDED_RAW_WORKLOAD_IDS",
    "LocationShadowBlockedError",
    "LocationShadowError",
    "build_guarded_shadow_child_request",
    "build_runtime_guarded_shadow_child_request",
]
