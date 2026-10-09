"""Opt-in waarschuwingdispatch voor de expliciete normale-HA-runtime.

Deze module consumeert uitsluitend de provider-neutrale nowcaststatus.  Hij
doet geen meteorologische inferentie en presenteert meldingen nooit als een
officiële waarschuwing.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import hashlib
from itertools import islice
import math
import re
import time
from typing import Any

from .const import WARNING_SENT_EVENT


WARNING_SNAPSHOT_SCHEMA = "storm_tracker_warning_snapshot_v1"
WARNING_STATE_VERSION = 1
WARNING_STAGES = ("vroeg", "nabij", "imminent")
_STAGE_RANK = {stage: rank for rank, stage in enumerate(WARNING_STAGES)}
_TRUSTED_CLEAR_STATUSES = frozenset({
    "droog",
    "wegtrekkend",
    "langs_trekkend",
    "bevestigd",
    "waargenomen",
    "nadert_regio",
})
MAX_DATA_AGE_SECONDS = 20 * 60
DEFAULT_COOLDOWN_SECONDS = 30 * 60
CLEAR_CONFIRMATION_SECONDS = 10 * 60
STATE_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_TARGET_STATES = 64
MAX_CLAIMS = 256
MAX_IDENTIFIER_LENGTH = 128
MAX_TARGET_LABEL_LENGTH = 64
MAX_MESSAGE_LENGTH = 420
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_NOTIFY_SERVICE_RE = re.compile(r"^notify\.([a-z0-9_]+)$")


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _bounded_identifier(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value or len(value) > MAX_IDENTIFIER_LENGTH:
        return None
    if any(ord(character) < 32 for character in value):
        return None
    return value


def _bounded_text(value: Any, maximum: int) -> str | None:
    if not isinstance(value, str):
        return None
    compact = " ".join(value[: maximum * 4].split())
    if not compact:
        return None
    return compact[:maximum]


def _rounded_coordinate(value: Any, *, latitude: bool) -> float | None:
    number = _finite_number(value)
    if number is None:
        return None
    lower, upper = (-90.0, 90.0) if latitude else (-180.0, 180.0)
    if not lower <= number <= upper:
        return None
    # Ongeveer honderd meter: voldoende voor interne kalibratie, zonder een
    # exacte woning- of trackerpositie in het event te publiceren.
    return round(number, 3)


def _opaque(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _warning_stage(eta_minutes: float) -> str:
    if eta_minutes <= 15.0:
        return "imminent"
    if eta_minutes <= 45.0:
        return "nabij"
    return "vroeg"


@dataclass(frozen=True, slots=True)
class WarningSnapshot:
    """Immutable, bounded snapshot that authorized one notification."""

    observed_at: float
    target_id: str
    target_label: str
    storm_id: str
    warning_stage: str
    status: str
    eta_minutes: float
    distance_km: float | None
    passage_classification: str
    motion_confidence: str
    reliability_label: str
    forecast_confidence_percent: float
    forecast_intensity_label: str | None
    latitude: float | None
    longitude: float | None
    data_age_seconds: float

    def event_payload(self) -> dict[str, Any]:
        """Return the exact bounded snapshot consumed by calibration."""

        return {
            "schema": WARNING_SNAPSHOT_SCHEMA,
            "observed_at": self.observed_at,
            "target_id": self.target_id,
            "target_label": self.target_label,
            "storm_id": self.storm_id,
            "warning_stage": self.warning_stage,
            "status": self.status,
            "warning_eligible": True,
            "forecast_available": True,
            "eta_minutes": self.eta_minutes,
            "distance_km": self.distance_km,
            "passage_classification": self.passage_classification,
            "motion_confidence": self.motion_confidence,
            "reliability_label": self.reliability_label,
            "forecast_confidence_percent": (
                self.forecast_confidence_percent
            ),
            "forecast_intensity_label": self.forecast_intensity_label,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "data_age_seconds": self.data_age_seconds,
            "advisory_type": "local_model_forecast",
            "official_warning": False,
        }


@dataclass(frozen=True, slots=True)
class WarningEvaluation:
    """One fail-closed target evaluation for the dispatcher."""

    target_id: str
    observed_at: float
    snapshot: WarningSnapshot | None
    clear_allowed: bool
    reason: str


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """Observable result without exposing target or storm identifiers."""

    status: str
    reason: str
    warning_stage: str | None = None


def build_warning_evaluation(
    status: Mapping[str, Any],
    *,
    target_id: str,
    target_label: str,
    latitude: float | None,
    longitude: float | None,
    observed_at: float,
    data_age_seconds: float | None,
    runtime_admitted: bool,
    target_available: bool,
    radar_covered: bool,
) -> WarningEvaluation:
    """Project one local nowcast into a strict provider-neutral candidate."""

    safe_target_id = _bounded_identifier(target_id)
    safe_observed_at = _finite_number(observed_at)
    if (
        safe_target_id is None
        or safe_observed_at is None
        or safe_observed_at < 0.0
    ):
        return WarningEvaluation("", 0.0, None, False, "invalid_identity")

    def blocked(reason: str, *, clear: bool = False) -> WarningEvaluation:
        return WarningEvaluation(
            safe_target_id,
            safe_observed_at,
            None,
            clear,
            reason,
        )

    if not isinstance(status, Mapping):
        return blocked("status_invalid")

    if runtime_admitted is not True:
        return blocked("runtime_not_admitted")
    if target_available is not True:
        return blocked("target_unavailable")
    if radar_covered is not True:
        return blocked("radar_not_covered")
    age = _finite_number(data_age_seconds)
    if age is None:
        return blocked("freshness_unknown")
    if age < -60.0 or age > MAX_DATA_AGE_SECONDS:
        return blocked("data_stale")
    age = max(0.0, age)

    state = status.get("status")
    if state in _TRUSTED_CLEAR_STATUSES:
        return blocked("trusted_clear", clear=True)
    if state != "naderend":
        return blocked("status_not_approaching")
    # Alleen echte bools gelden. Strings als "true" mogen nooit dispatch
    # autoriseren wanneer ze uit een verkeerd template of stale state komen.
    if status.get("warning_eligible") is not True:
        return blocked("warning_not_eligible")
    if status.get("forecast_available") is not True:
        return blocked("forecast_unavailable")
    if status.get("eta_reliable") is not True:
        return blocked("eta_not_reliable")
    if status.get("forecast_block_reason") not in (None, ""):
        return blocked("forecast_blocked")
    passage = status.get("passage_classification")
    if passage not in {"raak", "rand"}:
        return blocked("passage_not_targeting")
    motion_confidence = status.get("motion_confidence")
    if motion_confidence not in {"Matig", "Hoog"}:
        return blocked("motion_confidence_below_moderate")
    reliability_label = status.get("reliability_label")
    if reliability_label not in {"Matig", "Hoog"}:
        return blocked("reliability_below_moderate")

    eta = _finite_number(status.get("eta_minutes"))
    if eta is None or not 0.0 <= eta <= 90.0:
        return blocked("eta_outside_horizon")
    storm_id = _bounded_identifier(
        status.get("storm_id") or status.get("tracked_system_storm_id")
    )
    if storm_id is None:
        return blocked("storm_identity_missing")
    forecast_confidence = _finite_number(
        status.get("forecast_confidence_percent")
    )
    if (
        forecast_confidence is None
        or not 0.0 <= forecast_confidence <= 100.0
    ):
        return blocked("forecast_confidence_invalid")

    distance = _finite_number(
        status.get("tracked_system_distance_km", status.get("distance_km"))
    )
    if distance is not None and distance < 0.0:
        distance = None
    label = (
        _bounded_text(target_label, MAX_TARGET_LABEL_LENGTH)
        or "geselecteerd doel"
    )
    intensity = _bounded_text(
        status.get("forecast_intensity_label"), 32
    )
    snapshot = WarningSnapshot(
        observed_at=round(safe_observed_at, 3),
        target_id=safe_target_id,
        target_label=label,
        storm_id=storm_id,
        warning_stage=_warning_stage(eta),
        status="naderend",
        eta_minutes=round(eta, 1),
        distance_km=round(distance, 1) if distance is not None else None,
        passage_classification=passage,
        motion_confidence=motion_confidence,
        reliability_label=reliability_label,
        forecast_confidence_percent=round(forecast_confidence, 1),
        forecast_intensity_label=intensity,
        latitude=_rounded_coordinate(latitude, latitude=True),
        longitude=_rounded_coordinate(longitude, latitude=False),
        data_age_seconds=round(age, 1),
    )
    return WarningEvaluation(
        safe_target_id,
        safe_observed_at,
        snapshot,
        False,
        "eligible",
    )


class WarningDispatcher:
    """Durable opt-in state machine around one HA notify service."""

    def __init__(
        self,
        hass,
        *,
        enabled: bool,
        notify_service: str | None,
        store,
        clock=time.time,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
    ) -> None:
        if not isinstance(enabled, bool):
            raise TypeError("enabled moet exact bool zijn")
        cooldown = _finite_number(cooldown_seconds)
        if cooldown is None or cooldown < 0.0:
            raise ValueError("cooldown_seconds is ongeldig")
        self._hass = hass
        self._enabled = enabled
        self._store = store
        self._clock = clock
        self._cooldown_seconds = cooldown
        self._notify_service = self._parse_notify_service(notify_service)
        self._targets: dict[str, dict[str, Any]] = {}
        self._claims: list[dict[str, Any]] = []
        self._sent_count = 0
        self._state_loaded = False
        self._diagnostics: dict[str, Any] = {}
        self._set_diagnostic(
            "initializing" if enabled else "disabled",
            "awaiting_state" if enabled else "opt_in_disabled",
        )

    @staticmethod
    def _parse_notify_service(value: str | None) -> str | None:
        if value in (None, ""):
            return None
        if not isinstance(value, str):
            return None
        if len(value) > MAX_IDENTIFIER_LENGTH + len("notify."):
            return None
        match = _NOTIFY_SERVICE_RE.fullmatch(value.strip())
        return match.group(1) if match else None

    @property
    def diagnostics(self) -> dict[str, Any]:
        return dict(self._diagnostics)

    def _set_diagnostic(
        self,
        status: str,
        reason: str,
        *,
        snapshot: WarningSnapshot | None = None,
        service_available: bool | None = None,
    ) -> None:
        self._diagnostics = {
            "status": status,
            "reason": reason,
            "enabled": self._enabled,
            "notify_service_configured": self._notify_service is not None,
            "notify_service_available": service_available,
            "state_loaded": self._state_loaded,
            "sent_count": self._sent_count,
            "last_evaluated_at": (
                snapshot.observed_at if snapshot is not None else self._clock()
            ),
            "last_warning_stage": (
                snapshot.warning_stage if snapshot is not None else None
            ),
            "official_warning": False,
        }

    async def async_initialize(self) -> None:
        """Restore bounded dedupe/re-arm state; invalid state fails closed."""

        try:
            payload = await self._store.async_load()
            if payload is not None:
                self._restore(payload)
            self._state_loaded = True
            self._prune(self._clock())
            self._set_diagnostic(
                "ready" if self._enabled else "disabled",
                "ready" if self._enabled else "opt_in_disabled",
            )
        except Exception as err:
            self._targets = {}
            self._claims = []
            self._state_loaded = False
            self._set_diagnostic("state_error", type(err).__name__)

    def _restore(self, payload: Mapping[str, Any]) -> None:
        if (
            not isinstance(payload, Mapping)
            or payload.get("version") != WARNING_STATE_VERSION
        ):
            raise ValueError("ongeldige warning-stateversie")
        raw_targets = payload.get("targets", {})
        raw_claims = payload.get("claims", [])
        if not isinstance(raw_targets, Mapping) or not isinstance(raw_claims, list):
            raise ValueError("ongeldige warning-statestructuur")
        if len(raw_targets) > MAX_TARGET_STATES or len(raw_claims) > MAX_CLAIMS:
            raise ValueError("warning-state overschrijdt harde grens")

        targets: dict[str, dict[str, Any]] = {}
        for key, value in raw_targets.items():
            if not isinstance(key, str) or not _HASH_RE.fullmatch(key):
                raise ValueError("ongeldige targethash")
            if not isinstance(value, Mapping):
                raise ValueError("ongeldige targetstate")
            generation = value.get("generation")
            if (
                isinstance(generation, bool)
                or not isinstance(generation, int)
                or not 0 <= generation <= 1_000_000_000
            ):
                raise ValueError("ongeldige re-armgeneratie")
            active_storm = value.get("active_storm")
            if active_storm is not None and (
                not isinstance(active_storm, str)
                or not _HASH_RE.fullmatch(active_storm)
            ):
                raise ValueError("ongeldige stormhash")
            highest_stage = value.get("highest_stage")
            if highest_stage not in (*WARNING_STAGES, None):
                raise ValueError("ongeldige warningstage")
            restored = {
                "generation": max(0, generation),
                "active_storm": active_storm,
                "highest_stage": highest_stage,
                "last_sent_at": self._optional_timestamp(
                    value.get("last_sent_at")
                ),
                "clear_since": self._optional_timestamp(
                    value.get("clear_since")
                ),
                "last_seen_at": self._required_timestamp(
                    value.get("last_seen_at")
                ),
            }
            targets[key] = restored

        claims: list[dict[str, Any]] = []
        for value in raw_claims:
            if not isinstance(value, Mapping):
                raise ValueError("ongeldige warningclaim")
            target_hash = value.get("target")
            storm_hash = value.get("storm")
            stage = value.get("stage")
            generation = value.get("generation")
            if (
                not isinstance(target_hash, str)
                or not _HASH_RE.fullmatch(target_hash)
                or not isinstance(storm_hash, str)
                or not _HASH_RE.fullmatch(storm_hash)
                or stage not in WARNING_STAGES
                or isinstance(generation, bool)
                or not isinstance(generation, int)
                or not 0 <= generation <= 1_000_000_000
            ):
                raise ValueError("ongeldige warningclaimvelden")
            claims.append({
                "target": target_hash,
                "storm": storm_hash,
                "stage": stage,
                "generation": max(0, generation),
                "sent_at": self._required_timestamp(value.get("sent_at")),
            })
        self._targets = targets
        self._claims = claims

    @staticmethod
    def _optional_timestamp(value: Any) -> float | None:
        if value is None:
            return None
        return WarningDispatcher._required_timestamp(value)

    @staticmethod
    def _required_timestamp(value: Any) -> float:
        timestamp = _finite_number(value)
        if timestamp is None or timestamp < 0.0:
            raise ValueError("ongeldige timestamp")
        return timestamp

    def _prune(self, now: float) -> None:
        cutoff = now - STATE_TTL_SECONDS
        self._claims = sorted(
            (
                claim for claim in self._claims
                if float(claim["sent_at"]) >= cutoff
            ),
            key=lambda claim: float(claim["sent_at"]),
        )[-MAX_CLAIMS:]
        retained = sorted(
            (
                (key, value) for key, value in self._targets.items()
                if float(value["last_seen_at"]) >= cutoff
            ),
            key=lambda item: float(item[1]["last_seen_at"]),
        )[-MAX_TARGET_STATES:]
        self._targets = dict(retained)

    def _serialized_state(self) -> dict[str, Any]:
        self._prune(self._clock())
        return {
            "version": WARNING_STATE_VERSION,
            "targets": {
                key: dict(value) for key, value in self._targets.items()
            },
            "claims": [dict(claim) for claim in self._claims],
        }

    async def _async_save(self) -> None:
        await self._store.async_save(self._serialized_state())

    def _target_state(self, target_hash: str, now: float) -> dict[str, Any]:
        state = self._targets.get(target_hash)
        if state is None:
            state = {
                "generation": 0,
                "active_storm": None,
                "highest_stage": None,
                "last_sent_at": None,
                "clear_since": None,
                "last_seen_at": now,
            }
            self._targets[target_hash] = state
        state["last_seen_at"] = now
        return state

    async def _async_note_clear(
        self, evaluation: WarningEvaluation
    ) -> DispatchResult:
        target_hash = _opaque(evaluation.target_id)
        now = self._clock()
        state = self._targets.get(target_hash)
        if state is None or state.get("active_storm") is None:
            self._set_diagnostic("clear", "already_armed")
            return DispatchResult("clear", "already_armed")
        state["last_seen_at"] = now
        clear_since = state.get("clear_since")
        if clear_since is None:
            state["clear_since"] = now
            await self._async_save()
            self._set_diagnostic("clear_pending", "clear_confirmation_started")
            return DispatchResult("clear_pending", "clear_confirmation_started")
        if now - float(clear_since) < CLEAR_CONFIRMATION_SECONDS:
            self._set_diagnostic("clear_pending", "clear_confirmation_waiting")
            return DispatchResult("clear_pending", "clear_confirmation_waiting")
        state.update({
            "generation": (int(state["generation"]) + 1) % 1_000_000_001,
            "active_storm": None,
            "highest_stage": None,
            "clear_since": None,
            "last_seen_at": now,
        })
        await self._async_save()
        self._set_diagnostic("clear", "rearmed")
        return DispatchResult("clear", "rearmed")

    def _claim_exists(
        self,
        *,
        target_hash: str,
        storm_hash: str,
        stage: str,
        generation: int,
    ) -> bool:
        return any(
            claim["target"] == target_hash
            and claim["storm"] == storm_hash
            and claim["stage"] == stage
            and int(claim["generation"]) == generation
            for claim in self._claims
        )

    def _notification_payload(self, snapshot: WarningSnapshot) -> dict[str, Any]:
        stage_text = {
            "vroeg": "Neerslag wordt binnen negentig minuten verwacht",
            "nabij": "Neerslag nadert",
            "imminent": "Neerslag is vrijwel ter plaatse",
        }[snapshot.warning_stage]
        passage = "raakt" if snapshot.passage_classification == "raak" else "schampt"
        intensity = (
            f", verwachte intensiteit {snapshot.forecast_intensity_label}"
            if snapshot.forecast_intensity_label else ""
        )
        message = (
            "Eigen Storm Tracker-modelverwachting, geen officiële "
            f"waarschuwing: {snapshot.target_label} {passage}; circa "
            f"{snapshot.eta_minutes:g} minuten{intensity}."
        )[:MAX_MESSAGE_LENGTH]
        tag = _opaque(
            f"{snapshot.target_id}|{snapshot.storm_id}"
        )[:16]
        return {
            "title": f"Storm Tracker - {stage_text}"[:96],
            "message": message,
            "data": {
                "tag": f"stv3-{tag}",
                "warning_stage": snapshot.warning_stage,
                "official_warning": False,
            },
        }

    async def _async_dispatch(
        self, snapshot: WarningSnapshot
    ) -> DispatchResult:
        if not self._enabled:
            self._set_diagnostic("disabled", "opt_in_disabled", snapshot=snapshot)
            return DispatchResult("blocked", "opt_in_disabled")
        if not self._state_loaded:
            self._set_diagnostic(
                "state_error", "persistent_state_not_loaded",
                snapshot=snapshot,
            )
            return DispatchResult("blocked", "persistent_state_not_loaded")
        if self._notify_service is None:
            self._set_diagnostic(
                "not_configured", "notify_service_missing_or_invalid",
                snapshot=snapshot, service_available=False,
            )
            return DispatchResult("blocked", "notify_service_missing_or_invalid")
        try:
            service_available = self._hass.services.has_service(
                "notify", self._notify_service
            )
        except Exception as err:
            self._set_diagnostic(
                "service_error", type(err).__name__, snapshot=snapshot,
                service_available=False,
            )
            return DispatchResult("error", "service_registry_error")
        if not service_available:
            self._set_diagnostic(
                "service_unavailable", "notify_service_unavailable",
                snapshot=snapshot, service_available=False,
            )
            return DispatchResult("blocked", "notify_service_unavailable")

        now = self._clock()
        target_hash = _opaque(snapshot.target_id)
        storm_hash = _opaque(snapshot.storm_id)
        state = self._target_state(target_hash, now)
        if state.get("active_storm") != storm_hash:
            state.update({
                "active_storm": storm_hash,
                "highest_stage": None,
                "clear_since": None,
            })
        else:
            state["clear_since"] = None
        generation = int(state["generation"])
        if self._claim_exists(
            target_hash=target_hash,
            storm_hash=storm_hash,
            stage=snapshot.warning_stage,
            generation=generation,
        ):
            self._set_diagnostic(
                "deduplicated", "target_storm_stage_already_sent",
                snapshot=snapshot, service_available=True,
            )
            return DispatchResult(
                "deduplicated", "target_storm_stage_already_sent",
                snapshot.warning_stage,
            )
        highest = state.get("highest_stage")
        stage_rank = _STAGE_RANK[snapshot.warning_stage]
        if highest is not None and stage_rank <= _STAGE_RANK[highest]:
            self._set_diagnostic(
                "deduplicated", "stage_regression_blocked", snapshot=snapshot,
                service_available=True,
            )
            return DispatchResult(
                "deduplicated", "stage_regression_blocked",
                snapshot.warning_stage,
            )
        escalation = highest is not None and stage_rank > _STAGE_RANK[highest]
        last_sent_at = state.get("last_sent_at")
        if (
            not escalation
            and last_sent_at is not None
            and now - float(last_sent_at) < self._cooldown_seconds
        ):
            self._set_diagnostic(
                "cooldown", "target_cooldown_active", snapshot=snapshot,
                service_available=True,
            )
            return DispatchResult(
                "cooldown", "target_cooldown_active", snapshot.warning_stage
            )

        try:
            await self._hass.services.async_call(
                "notify",
                self._notify_service,
                self._notification_payload(snapshot),
                blocking=True,
            )
        except Exception as err:
            self._set_diagnostic(
                "notify_error", type(err).__name__, snapshot=snapshot,
                service_available=True,
            )
            return DispatchResult("error", "notify_call_failed")

        # De claim ontstaat uitsluitend nadat HA de echte notify-call zonder
        # fout heeft afgehandeld. Een mislukte servicecall blijft retrybaar.
        state.update({
            "highest_stage": snapshot.warning_stage,
            "last_sent_at": now,
            "clear_since": None,
            "last_seen_at": now,
        })
        self._claims.append({
            "target": target_hash,
            "storm": storm_hash,
            "stage": snapshot.warning_stage,
            "generation": generation,
            "sent_at": now,
        })
        self._sent_count += 1
        persistence_error = None
        try:
            await self._async_save()
        except Exception as err:
            persistence_error = type(err).__name__
        self._hass.bus.async_fire(
            WARNING_SENT_EVENT, snapshot.event_payload()
        )
        self._set_diagnostic(
            "sent" if persistence_error is None else "sent_state_error",
            "sent" if persistence_error is None else persistence_error,
            snapshot=snapshot,
            service_available=True,
        )
        return DispatchResult("sent", "sent", snapshot.warning_stage)

    async def async_process(
        self, evaluations: Iterable[WarningEvaluation]
    ) -> tuple[DispatchResult, ...]:
        """Process a bounded target set in deterministic input order."""

        try:
            items = tuple(islice(iter(evaluations), MAX_TARGET_STATES + 1))
        except TypeError:
            self._set_diagnostic("input_error", "evaluations_not_iterable")
            return (DispatchResult("error", "evaluations_not_iterable"),)
        if len(items) > MAX_TARGET_STATES:
            self._set_diagnostic("input_error", "too_many_targets")
            return (DispatchResult("error", "too_many_targets"),)
        results = []
        for evaluation in items:
            if not isinstance(evaluation, WarningEvaluation):
                results.append(DispatchResult("error", "invalid_evaluation"))
                continue
            if evaluation.snapshot is None:
                if evaluation.clear_allowed:
                    results.append(await self._async_note_clear(evaluation))
                else:
                    self._set_diagnostic("blocked", evaluation.reason)
                    results.append(DispatchResult("blocked", evaluation.reason))
                continue
            results.append(await self._async_dispatch(evaluation.snapshot))
        return tuple(results)
