"""Crash-durable worker-only scheduler foundation.

The scheduler reads meteorological configuration exclusively from
``DesiredStateStore``.  A caller injects a pure plan builder which translates
one immutable desired-state into generic :class:`ScheduleIntent` records.  No
provider is selected here and no job is executed here: this module only owns
deterministic cadence slots, a bounded SQLite outbox and single-consumer
leases.

Stable job IDs plus durable lease recovery provide at-least-once dispatch.
Consumers must use the stable job ID as their idempotency key.  Home Assistant
does not initiate ticks or provider cadences; later worker service wiring will
call this module from the add-on runtime.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import sqlite3
import time
from typing import Any
import unicodedata

from .desired_state import (
    MAX_CLOCK_FUTURE_SKEW_SECONDS,
    MAX_DYNAMIC_POSITION_AGE_SECONDS,
    DesiredState,
    DesiredStateStore,
)


CONTRACT = "stv3-worker-autonomous-scheduler-v1"
SCHEMA_VERSION = 1

MAX_PLAN_INTENTS = 128
MAX_PLAN_BYTES = 512 * 1024
MAX_INTENT_PAYLOAD_BYTES = 32 * 1024
MAX_PAYLOAD_ITEMS = 512
MAX_PAYLOAD_DEPTH = 12
MAX_OUTBOX_ROWS = 100_000
MAX_DATABASE_BYTES = 128 * 1024 * 1024
MAX_DIAGNOSTIC_BYTES = 4 * 1024

MIN_CADENCE_SECONDS = 1
MAX_CADENCE_SECONDS = 7 * 24 * 60 * 60
MAX_DEADLINE_SECONDS = 7 * 24 * 60 * 60
MAX_PRIORITY = 1_000
MAX_ATTEMPTS = 1_000_000

DEFAULT_LEASE_SECONDS = 90.0
DEFAULT_RETRY_BASE_SECONDS = 2.0
DEFAULT_RETRY_MAX_SECONDS = 15 * 60.0
CLOCK_BACKWARD_TOLERANCE_SECONDS = 1.0

DEFAULT_STORE_PATH = Path(
    os.getenv(
        "STV3_AUTONOMOUS_SCHEDULER_DB",
        "/share/storm_tracker_worker/autonomous_scheduler.sqlite3",
    )
)

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$")
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SECRET_KEY = re.compile(
    r"(^|[_-])(password|passwd|secret|api[_-]?key|access[_-]?token|"
    r"refresh[_-]?token|authorization|cookie)([_-]|$)",
    re.IGNORECASE,
)
_OUTBOX_STATES = (
    "pending",
    "leased",
    "retry",
    "completed",
    "expired",
    "superseded",
)
_DYNAMIC_TARGET_KINDS = frozenset({"person", "device_tracker", "test"})


class AutonomousSchedulerError(RuntimeError):
    """Base error for scheduler validation or durable transitions."""


class ScheduleIntentError(AutonomousSchedulerError):
    """A plan builder returned an invalid or unbounded intent."""


class SchedulerConflict(AutonomousSchedulerError):
    """The same stable identity was reused for non-identical work."""


class SchedulerCapacityError(AutonomousSchedulerError):
    """The bounded outbox cannot admit another durable record."""


class SchedulerClockError(AutonomousSchedulerError):
    """Wall-clock evidence is unsafe for creating or leasing slots."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class SchedulerDesiredStateError(AutonomousSchedulerError):
    """The active desired-state cannot safely drive autonomous work."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class ScheduleIntent:
    """Generic deterministic work requested by an injected plan builder.

    Lower numeric priorities are claimed first by default. A serial runtime may
    explicitly request first-attempt fairness while claiming so one failing
    intent cannot starve first capture for the rest of its queue. ``payload`` is
    the eventual worker-job payload; it may contain locations needed by
    execution, but it is
    never exposed by scheduler diagnostics.
    """

    intent_id: str
    job_type: str
    cadence_seconds: int
    deadline_seconds: int
    priority: int
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    status: str
    desired_state_sha256: str | None
    intent_count: int
    inserted_count: int
    idempotent_count: int
    missed_deadline_count: int
    recovered_lease_count: int
    superseded_count: int


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    job_id: str
    job_type: str
    payload: dict[str, Any]
    priority: int
    scheduled_for: float
    deadline_at: float
    desired_state_sha256: str
    attempt_count: int
    lease_token: str
    lease_expires_at: float


@dataclass(frozen=True, slots=True)
class TransitionResult:
    job_id: str
    status: str
    attempt_count: int
    next_attempt_at: float | None


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    """Payload-free record metadata for tests and bounded introspection."""

    job_id: str
    intent_id: str
    state: str
    desired_state_sha256: str
    slot_index: int
    scheduled_for: float
    deadline_at: float
    priority: int
    attempt_count: int
    next_attempt_at: float
    lease_expires_at: float | None


@dataclass(frozen=True, slots=True)
class SchedulerTerminalProof:
    """Payload-free immutable proof for one terminal scheduler row."""

    job_id: str
    slot_id: str
    record_sha256: str
    desired_state_sha256: str
    intent_id: str
    job_type: str
    payload_sha256: str
    state: str
    slot_index: int
    scheduled_for: float
    deadline_at: float
    attempt_count: int
    last_transition: str
    last_error_code: str | None
    terminal_at: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "attempt_count": self.attempt_count,
            "deadline_at": self.deadline_at,
            "desired_state_sha256": self.desired_state_sha256,
            "intent_id": self.intent_id,
            "job_id": self.job_id,
            "job_type": self.job_type,
            "last_error_code": self.last_error_code,
            "last_transition": self.last_transition,
            "payload_sha256": self.payload_sha256,
            "record_sha256": self.record_sha256,
            "scheduled_for": self.scheduled_for,
            "slot_id": self.slot_id,
            "slot_index": self.slot_index,
            "state": self.state,
            "terminal_at": self.terminal_at,
        }

    @property
    def proof_sha256(self) -> str:
        return hashlib.sha256(_canonical_json(self.as_dict())).hexdigest()


@dataclass(frozen=True, slots=True)
class _ActiveDesired:
    state: DesiredState
    activated_at: float
    last_received_at: float


@dataclass(frozen=True, slots=True)
class _ValidatedIntent:
    intent_id: str
    job_type: str
    cadence_seconds: int
    deadline_seconds: int
    priority: int
    payload: dict[str, Any]
    payload_bytes: bytes
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class _SlotCandidate:
    job_id: str
    slot_id: str
    record_sha256: str
    desired_state_sha256: str
    intent: _ValidatedIntent
    slot_index: int
    scheduled_for: float
    deadline_at: float


PlanBuilder = Callable[[DesiredState], Iterable[ScheduleIntent]]


def _finite_number(
    value: Any,
    name: str,
    *,
    minimum: float = 0.0,
    maximum: float = 2**53,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduleIntentError(f"{name} moet numeriek zijn")
    number = float(value)
    if not math.isfinite(number) or not minimum <= number <= maximum:
        raise ScheduleIntentError(f"{name} ligt buiten de harde grens")
    return number


def _bounded_integer(
    value: Any,
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ScheduleIntentError(f"{name} moet een integer zijn")
    if not minimum <= value <= maximum:
        raise ScheduleIntentError(f"{name} ligt buiten de harde grens")
    return value


def _identifier(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ScheduleIntentError(f"{name} moet tekst zijn")
    normalized = unicodedata.normalize("NFC", value)
    if normalized != value or _IDENTIFIER.fullmatch(value) is None:
        raise ScheduleIntentError(f"{name} is geen canonieke identifier")
    return value


def _validated_allowed_workload_ids(
    value: frozenset[str] | None,
) -> frozenset[str] | None:
    if value is None:
        return None
    if type(value) is not frozenset:
        raise TypeError("allowed_workload_ids must be an immutable frozenset")
    for workload_id in value:
        _identifier(workload_id, "allowed workload_id")
    return value


def _normalise_payload(
    value: Any,
    *,
    depth: int,
    counter: list[int],
) -> Any:
    if depth > MAX_PAYLOAD_DEPTH:
        raise ScheduleIntentError("intentpayload overschrijdt de dieptelimiet")
    counter[0] += 1
    if counter[0] > MAX_PAYLOAD_ITEMS:
        raise ScheduleIntentError("intentpayload overschrijdt de itemlimiet")
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        if not -(2**53) <= value <= 2**53:
            raise ScheduleIntentError("intentpayloadinteger ligt buiten de grens")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ScheduleIntentError("intentpayload bevat een niet-eindig getal")
        return value
    if isinstance(value, str):
        normalized = unicodedata.normalize("NFC", value)
        if normalized != value or len(value) > 4_096:
            raise ScheduleIntentError("intentpayloadtekst is niet canoniek of te lang")
        return value
    if isinstance(value, Mapping):
        if len(value) > MAX_PAYLOAD_ITEMS:
            raise ScheduleIntentError("intentpayloadobject is te groot")
        result: dict[str, Any] = {}
        keys = tuple(value)
        if not all(isinstance(key, str) for key in keys):
            raise ScheduleIntentError("intentpayloadsleutels moeten tekst zijn")
        for key in sorted(keys):
            normalized = unicodedata.normalize("NFC", key)
            if normalized != key or not 1 <= len(key) <= 160:
                raise ScheduleIntentError("intentpayloadsleutel is ongeldig")
            if _SECRET_KEY.search(key) is not None:
                raise ScheduleIntentError("intentpayload bevat een geheime sleutel")
            result[key] = _normalise_payload(
                value[key], depth=depth + 1, counter=counter
            )
        return result
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_PAYLOAD_ITEMS:
            raise ScheduleIntentError("intentpayloadlijst is te groot")
        return [
            _normalise_payload(item, depth=depth + 1, counter=counter)
            for item in value
        ]
    raise ScheduleIntentError("intentpayload bevat een niet-JSON datatype")


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _validate_intent(value: Any) -> _ValidatedIntent:
    if not isinstance(value, ScheduleIntent):
        raise ScheduleIntentError("plan-builder moet ScheduleIntent leveren")
    intent_id = _identifier(value.intent_id, "intent_id")
    job_type = _identifier(value.job_type, "job_type")
    cadence = _bounded_integer(
        value.cadence_seconds,
        "cadence_seconds",
        minimum=MIN_CADENCE_SECONDS,
        maximum=MAX_CADENCE_SECONDS,
    )
    deadline = _bounded_integer(
        value.deadline_seconds,
        "deadline_seconds",
        minimum=1,
        maximum=MAX_DEADLINE_SECONDS,
    )
    priority = _bounded_integer(
        value.priority,
        "priority",
        minimum=0,
        maximum=MAX_PRIORITY,
    )
    if not isinstance(value.payload, Mapping):
        raise ScheduleIntentError("intentpayload moet een object zijn")
    payload = _normalise_payload(value.payload, depth=0, counter=[0])
    payload_bytes = _canonical_json(payload)
    if len(payload_bytes) > MAX_INTENT_PAYLOAD_BYTES:
        raise ScheduleIntentError("intentpayload overschrijdt het bytebudget")
    return _ValidatedIntent(
        intent_id=intent_id,
        job_type=job_type,
        cadence_seconds=cadence,
        deadline_seconds=deadline,
        priority=priority,
        payload=payload,
        payload_bytes=payload_bytes,
        payload_sha256=hashlib.sha256(payload_bytes).hexdigest(),
    )


def _error_code(value: Any) -> str:
    if not isinstance(value, str) or _ERROR_CODE.fullmatch(value) is None:
        raise ScheduleIntentError("retry vereist een compacte error_code")
    return value


def _stable_slot_identity(
    desired_state_sha256: str,
    intent_id: str,
    cadence_seconds: int,
    slot_index: int,
) -> tuple[str, str]:
    identity = _canonical_json({
        "cadence_seconds": cadence_seconds,
        "contract": CONTRACT,
        "desired_state_sha256": desired_state_sha256,
        "intent_id": intent_id,
        "slot_index": slot_index,
    })
    digest = hashlib.sha256(identity).hexdigest()
    return f"autosched:{digest}", f"slot:{digest}"


def _slot_candidate(
    desired_state_sha256: str,
    intent: _ValidatedIntent,
    now: float,
) -> _SlotCandidate:
    slot_index = int(math.floor(now / intent.cadence_seconds))
    scheduled_for = float(slot_index * intent.cadence_seconds)
    deadline_at = scheduled_for + float(intent.deadline_seconds)
    job_id, slot_id = _stable_slot_identity(
        desired_state_sha256,
        intent.intent_id,
        intent.cadence_seconds,
        slot_index,
    )
    immutable = {
        "cadence_seconds": intent.cadence_seconds,
        "deadline_at": deadline_at,
        "desired_state_sha256": desired_state_sha256,
        "intent_id": intent.intent_id,
        "job_id": job_id,
        "job_type": intent.job_type,
        "payload": intent.payload,
        "payload_sha256": intent.payload_sha256,
        "priority": intent.priority,
        "scheduled_for": scheduled_for,
        "slot_id": slot_id,
        "slot_index": slot_index,
    }
    return _SlotCandidate(
        job_id=job_id,
        slot_id=slot_id,
        record_sha256=hashlib.sha256(_canonical_json(immutable)).hexdigest(),
        desired_state_sha256=desired_state_sha256,
        intent=intent,
        slot_index=slot_index,
        scheduled_for=scheduled_for,
        deadline_at=deadline_at,
    )


class AutonomousScheduler:
    """Durable planner/outbox with exactly one active serial lease."""

    _EXPECTED_TABLES = frozenset({
        "scheduler_schema",
        "scheduler_runtime",
        "scheduler_outbox",
    })
    _EXPECTED_COLUMNS = {
        "scheduler_schema": frozenset({"singleton", "schema_version"}),
        "scheduler_runtime": frozenset({
            "singleton",
            "last_wall_time",
            "active_desired_state_sha256",
            "active_plan_sha256",
            "active_intent_count",
        }),
        "scheduler_outbox": frozenset({
            "job_id",
            "slot_id",
            "record_sha256",
            "desired_state_sha256",
            "intent_id",
            "job_type",
            "cadence_seconds",
            "slot_index",
            "scheduled_for",
            "deadline_at",
            "priority",
            "payload_json",
            "payload_sha256",
            "state",
            "attempt_count",
            "next_attempt_at",
            "lease_owner",
            "lease_token",
            "lease_expires_at",
            "last_attempt_token",
            "last_transition",
            "last_error_code",
            "created_at",
            "updated_at",
            "terminal_at",
        }),
    }

    def __init__(
        self,
        path: str | Path = DEFAULT_STORE_PATH,
        *,
        desired_state_store: DesiredStateStore,
        plan_builder: PlanBuilder,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
        max_outbox_rows: int = MAX_OUTBOX_ROWS,
        max_database_bytes: int = MAX_DATABASE_BYTES,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        retry_base_seconds: float = DEFAULT_RETRY_BASE_SECONDS,
        retry_max_seconds: float = DEFAULT_RETRY_MAX_SECONDS,
        clock_backward_tolerance_seconds: float = (
            CLOCK_BACKWARD_TOLERANCE_SECONDS
        ),
        max_desired_state_age_seconds: float | None = None,
    ) -> None:
        if not isinstance(desired_state_store, DesiredStateStore):
            raise TypeError("desired_state_store moet DesiredStateStore zijn")
        if not callable(plan_builder):
            raise TypeError("plan_builder moet callable zijn")
        self.path = Path(path)
        self._desired_state_store = desired_state_store
        self._plan_builder = plan_builder
        self._clock = clock
        self._fault_injector = fault_injector
        self._max_outbox_rows = _bounded_integer(
            max_outbox_rows,
            "max_outbox_rows",
            minimum=1,
            maximum=MAX_OUTBOX_ROWS,
        )
        self._max_database_bytes = _bounded_integer(
            max_database_bytes,
            "max_database_bytes",
            minimum=1024 * 1024,
            maximum=MAX_DATABASE_BYTES,
        )
        self._lease_seconds = _finite_number(
            lease_seconds,
            "lease_seconds",
            minimum=0.1,
            maximum=24 * 60 * 60,
        )
        self._retry_base_seconds = _finite_number(
            retry_base_seconds,
            "retry_base_seconds",
            minimum=0.1,
            maximum=24 * 60 * 60,
        )
        self._retry_max_seconds = _finite_number(
            retry_max_seconds,
            "retry_max_seconds",
            minimum=self._retry_base_seconds,
            maximum=7 * 24 * 60 * 60,
        )
        self._clock_backward_tolerance_seconds = _finite_number(
            clock_backward_tolerance_seconds,
            "clock_backward_tolerance_seconds",
            minimum=0.0,
            maximum=60.0,
        )
        if max_desired_state_age_seconds is None:
            self._max_desired_state_age_seconds = None
        else:
            self._max_desired_state_age_seconds = _finite_number(
                max_desired_state_age_seconds,
                "max_desired_state_age_seconds",
                minimum=1.0,
                maximum=365 * 24 * 60 * 60,
            )

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _now(self, provided: float | None) -> float:
        value = self._clock() if provided is None else provided
        return _finite_number(value, "scheduler_now")

    def _connect(self) -> sqlite3.Connection:
        existed = self.path.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=30.0)
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA wal_autocheckpoint=128")
            db.execute("PRAGMA journal_size_limit=8388608")
            page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
            maximum_pages = max(1, self._max_database_bytes // page_size)
            if int(db.execute("PRAGMA page_count").fetchone()[0]) > maximum_pages:
                raise SchedulerCapacityError("scheduler_database_capacity")
            db.execute(f"PRAGMA max_page_count={maximum_pages}")
            self._ensure_schema(db)
            self._validate_schema(db, full=not existed)
            if not existed and os.name == "posix":
                directory_fd = os.open(
                    self.path.parent,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            return db
        except Exception:
            db.close()
            raise

    @staticmethod
    def _ensure_schema(db: sqlite3.Connection) -> None:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS scheduler_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                schema_version INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS scheduler_runtime (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                last_wall_time REAL,
                active_desired_state_sha256 TEXT,
                active_plan_sha256 TEXT,
                active_intent_count INTEGER NOT NULL DEFAULT 0
                    CHECK(active_intent_count >= 0)
            );
            CREATE TABLE IF NOT EXISTS scheduler_outbox (
                job_id TEXT PRIMARY KEY,
                slot_id TEXT NOT NULL UNIQUE,
                record_sha256 TEXT NOT NULL,
                desired_state_sha256 TEXT NOT NULL,
                intent_id TEXT NOT NULL,
                job_type TEXT NOT NULL,
                cadence_seconds INTEGER NOT NULL CHECK(cadence_seconds >= 1),
                slot_index INTEGER NOT NULL CHECK(slot_index >= 0),
                scheduled_for REAL NOT NULL CHECK(scheduled_for >= 0),
                deadline_at REAL NOT NULL CHECK(deadline_at > scheduled_for),
                priority INTEGER NOT NULL CHECK(priority >= 0),
                payload_json BLOB NOT NULL,
                payload_sha256 TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN (
                    'pending','leased','retry','completed','expired','superseded'
                )),
                attempt_count INTEGER NOT NULL DEFAULT 0
                    CHECK(attempt_count >= 0),
                next_attempt_at REAL NOT NULL CHECK(next_attempt_at >= 0),
                lease_owner TEXT,
                lease_token TEXT,
                lease_expires_at REAL,
                last_attempt_token TEXT,
                last_transition TEXT,
                last_error_code TEXT,
                created_at REAL NOT NULL CHECK(created_at >= 0),
                updated_at REAL NOT NULL CHECK(updated_at >= 0),
                terminal_at REAL,
                CHECK(
                    (state = 'leased' AND lease_owner IS NOT NULL
                        AND lease_token IS NOT NULL
                        AND lease_expires_at IS NOT NULL)
                    OR
                    (state != 'leased' AND lease_owner IS NULL
                        AND lease_token IS NULL
                        AND lease_expires_at IS NULL)
                )
            );
            CREATE UNIQUE INDEX IF NOT EXISTS scheduler_one_active_lease
                ON scheduler_outbox((1)) WHERE state = 'leased';
            CREATE INDEX IF NOT EXISTS scheduler_ready_order
                ON scheduler_outbox(
                    state, next_attempt_at, priority, scheduled_for, job_id
                );
            CREATE INDEX IF NOT EXISTS scheduler_intent_attempt_history
                ON scheduler_outbox(
                    intent_id, attempt_count, terminal_at, updated_at
                );
            """
        )
        schema = db.execute(
            "SELECT singleton,schema_version FROM scheduler_schema"
        ).fetchall()
        if not schema:
            db.execute(
                "INSERT INTO scheduler_schema(singleton,schema_version) "
                "VALUES(1,?)",
                (SCHEMA_VERSION,),
            )
        elif len(schema) != 1 or (
            int(schema[0]["singleton"]) != 1
            or int(schema[0]["schema_version"]) != SCHEMA_VERSION
        ):
            raise AutonomousSchedulerError("scheduler_schema_version")
        db.execute(
            "INSERT OR IGNORE INTO scheduler_runtime("
            "singleton,last_wall_time,active_desired_state_sha256,"
            "active_plan_sha256,active_intent_count) VALUES(1,NULL,NULL,NULL,0)"
        )
        db.commit()

    @classmethod
    def _validate_schema(
        cls,
        db: sqlite3.Connection,
        *,
        full: bool,
    ) -> None:
        tables = {
            str(row[0])
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        if tables != cls._EXPECTED_TABLES:
            # The positive reclamation participant is an explicit, inert
            # additive extension in this same database.  Accept only its
            # complete, independently fingerprinted table set; an unknown or
            # partial extension remains fail-closed.
            from .acquisition_control_reclamation import (
                CONTROL_RECLAMATION_TABLES,
                AcquisitionControlReclamationError,
                validate_control_reclamation_extension_schema,
            )

            if tables != cls._EXPECTED_TABLES | CONTROL_RECLAMATION_TABLES:
                raise AutonomousSchedulerError("scheduler_schema_tables")
            try:
                validate_control_reclamation_extension_schema(
                    db,
                    expected_role="scheduler",
                )
            except AcquisitionControlReclamationError as exc:
                raise AutonomousSchedulerError(
                    "scheduler_reclamation_extension"
                ) from exc
        for table, expected in cls._EXPECTED_COLUMNS.items():
            columns = {
                str(row[1])
                for row in db.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if columns != expected:
                raise AutonomousSchedulerError("scheduler_schema_columns")
        if full:
            if db.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                raise AutonomousSchedulerError("scheduler_quick_check")
            if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise AutonomousSchedulerError("scheduler_foreign_key_check")

    def _load_active_desired(self, now: float) -> _ActiveDesired | None:
        metadata = self._desired_state_store.current()
        if metadata.get("status") == "unconfigured":
            return None
        if metadata.get("status") != "configured":
            raise SchedulerDesiredStateError("desired_state_unavailable")
        state = self._desired_state_store.load_active()
        if state is None:
            raise SchedulerDesiredStateError("desired_state_inconsistent")
        exact = {
            "state_sha256": state.state_sha256,
            "producer_id": state.producer_id,
            "authority_epoch": state.authority_epoch,
            "generation": state.generation,
        }
        if any(metadata.get(key) != value for key, value in exact.items()):
            raise SchedulerDesiredStateError("desired_state_inconsistent")
        activated_at = _finite_number(metadata.get("activated_at"), "activated_at")
        last_received_at = _finite_number(
            metadata.get("last_received_at"), "last_received_at"
        )
        future_limit = now + MAX_CLOCK_FUTURE_SKEW_SECONDS
        if (
            state.issued_at > future_limit
            or activated_at > future_limit
            or last_received_at > future_limit
        ):
            raise SchedulerDesiredStateError("desired_state_future")
        freshness_reference = max(
            state.issued_at,
            activated_at,
            last_received_at,
        )
        if (
            self._max_desired_state_age_seconds is not None
            and now - freshness_reference > self._max_desired_state_age_seconds
        ):
            raise SchedulerDesiredStateError("desired_state_stale")
        for target in state.targets:
            observed_at = target.position_observed_at
            if observed_at is not None and observed_at > future_limit:
                raise SchedulerDesiredStateError("desired_state_future")
            if (
                target.available
                and target.kind in _DYNAMIC_TARGET_KINDS
                and (
                    observed_at is None
                    or now - observed_at > MAX_DYNAMIC_POSITION_AGE_SECONDS
                )
            ):
                raise SchedulerDesiredStateError("desired_state_stale")
        return _ActiveDesired(
            state=state,
            activated_at=activated_at,
            last_received_at=last_received_at,
        )

    def _assert_desired_unchanged(self, expected: DesiredState) -> None:
        metadata = self._desired_state_store.current()
        if (
            metadata.get("status") != "configured"
            or metadata.get("state_sha256") != expected.state_sha256
            or metadata.get("authority_epoch") != expected.authority_epoch
            or metadata.get("generation") != expected.generation
        ):
            raise SchedulerDesiredStateError("desired_state_changed_during_plan")

    def _build_plan(
        self,
        state: DesiredState,
    ) -> tuple[tuple[_ValidatedIntent, ...], str]:
        try:
            values = self._plan_builder(state)
            iterator = iter(values)
        except Exception as err:
            raise ScheduleIntentError("plan_builder_failed") from err
        intents: list[_ValidatedIntent] = []
        seen: set[str] = set()
        total_bytes = 0
        try:
            for index, value in enumerate(iterator):
                if index >= MAX_PLAN_INTENTS:
                    raise ScheduleIntentError("plan overschrijdt de intentlimiet")
                intent = _validate_intent(value)
                if intent.intent_id in seen:
                    raise ScheduleIntentError("plan bevat dubbel intent_id")
                seen.add(intent.intent_id)
                total_bytes += len(intent.payload_bytes) + 256
                if total_bytes > MAX_PLAN_BYTES:
                    raise ScheduleIntentError("plan overschrijdt het bytebudget")
                intents.append(intent)
        except ScheduleIntentError:
            raise
        except Exception as err:
            raise ScheduleIntentError("plan_builder_iteration_failed") from err
        intents.sort(key=lambda item: item.intent_id)
        plan_view = [
            {
                "cadence_seconds": item.cadence_seconds,
                "deadline_seconds": item.deadline_seconds,
                "intent_id": item.intent_id,
                "job_type": item.job_type,
                "payload": item.payload,
                "priority": item.priority,
            }
            for item in intents
        ]
        plan_bytes = _canonical_json(plan_view)
        if len(plan_bytes) > MAX_PLAN_BYTES:
            raise ScheduleIntentError("plan overschrijdt het bytebudget")
        return tuple(intents), hashlib.sha256(plan_bytes).hexdigest()

    def _guard_clock(self, db: sqlite3.Connection, now: float) -> float:
        row = db.execute(
            "SELECT last_wall_time FROM scheduler_runtime WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise AutonomousSchedulerError("scheduler_runtime_missing")
        previous = row["last_wall_time"]
        if previous is not None:
            previous_value = float(previous)
            if now + self._clock_backward_tolerance_seconds < previous_value:
                raise SchedulerClockError("clock_regressed")
            now = max(now, previous_value)
        db.execute(
            "UPDATE scheduler_runtime SET last_wall_time=? WHERE singleton=1",
            (now,),
        )
        return now

    def _retry_delay(self, attempt_count: int) -> float:
        exponent = min(30, max(0, attempt_count - 1))
        return min(
            self._retry_max_seconds,
            self._retry_base_seconds * (2**exponent),
        )

    def _expire_and_recover(
        self,
        db: sqlite3.Connection,
        now: float,
        allowed_workload_ids: frozenset[str] | None = None,
    ) -> int:
        if allowed_workload_ids is None:
            db.execute(
                "UPDATE scheduler_outbox SET state='expired',lease_owner=NULL,"
                "lease_token=NULL,lease_expires_at=NULL,last_transition='expired',"
                "last_error_code='deadline_expired',updated_at=?,terminal_at=? "
                "WHERE state IN ('pending','retry','leased') AND deadline_at<=?",
                (now, now, now),
            )
        else:
            deadline_rows = db.execute(
                "SELECT * FROM scheduler_outbox "
                "WHERE state IN ('pending','retry','leased') AND deadline_at<=? "
                "ORDER BY job_id",
                (now,),
            ).fetchall()
            for row in deadline_rows:
                payload = self._validate_static_row(row)
                if payload.get("workload_id") not in allowed_workload_ids:
                    continue
                db.execute(
                    "UPDATE scheduler_outbox SET state='expired',lease_owner=NULL,"
                    "lease_token=NULL,lease_expires_at=NULL,last_transition='expired',"
                    "last_error_code='deadline_expired',updated_at=?,terminal_at=? "
                    "WHERE job_id=? AND state IN ('pending','retry','leased')",
                    (now, now, row["job_id"]),
                )
        if allowed_workload_ids is None:
            rows = db.execute(
                "SELECT job_id,attempt_count FROM scheduler_outbox "
                "WHERE state='leased' AND lease_expires_at<=? ORDER BY job_id",
                (now,),
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM scheduler_outbox "
                "WHERE state='leased' AND lease_expires_at<=? ORDER BY job_id",
                (now,),
            ).fetchall()
        recovered = 0
        for row in rows:
            if allowed_workload_ids is not None:
                payload = self._validate_static_row(row)
                if payload.get("workload_id") not in allowed_workload_ids:
                    continue
            next_attempt = now + self._retry_delay(int(row["attempt_count"]))
            cursor = db.execute(
                "UPDATE scheduler_outbox SET state='retry',next_attempt_at=?,"
                "lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,"
                "last_transition='retry',last_error_code='lease_expired',"
                "updated_at=? WHERE job_id=? AND state='leased'",
                (next_attempt, now, row["job_id"]),
            )
            recovered += int(cursor.rowcount)
        return recovered

    def _supersede_inactive(
        self,
        db: sqlite3.Connection,
        desired_state_sha256: str,
        now: float,
        allowed_workload_ids: frozenset[str] | None = None,
    ) -> int:
        if allowed_workload_ids is None:
            cursor = db.execute(
                "UPDATE scheduler_outbox SET state='superseded',"
                "last_transition='superseded',last_error_code=NULL,updated_at=?,"
                "terminal_at=? WHERE state IN ('pending','retry') "
                "AND desired_state_sha256<>?",
                (now, now, desired_state_sha256),
            )
            return int(cursor.rowcount)
        rows = db.execute(
            "SELECT * FROM scheduler_outbox WHERE state IN ('pending','retry') "
            "AND desired_state_sha256<>? ORDER BY job_id",
            (desired_state_sha256,),
        ).fetchall()
        superseded = 0
        for row in rows:
            payload = self._validate_static_row(row)
            if payload.get("workload_id") not in allowed_workload_ids:
                continue
            cursor = db.execute(
                "UPDATE scheduler_outbox SET state='superseded',"
                "last_transition='superseded',last_error_code=NULL,updated_at=?,"
                "terminal_at=? WHERE job_id=? AND state IN ('pending','retry') "
                "AND desired_state_sha256<>?",
                (now, now, row["job_id"], desired_state_sha256),
            )
            superseded += int(cursor.rowcount)
        return superseded

    @staticmethod
    def _plan_head(
        db: sqlite3.Connection,
        desired_state_sha256: str,
        plan_sha256: str,
        intent_count: int,
    ) -> None:
        row = db.execute(
            "SELECT active_desired_state_sha256,active_plan_sha256 "
            "FROM scheduler_runtime WHERE singleton=1"
        ).fetchone()
        if (
            row["active_desired_state_sha256"] == desired_state_sha256
            and row["active_plan_sha256"] not in {None, plan_sha256}
        ):
            raise SchedulerConflict("plan_builder_nondeterministic")
        db.execute(
            "UPDATE scheduler_runtime SET active_desired_state_sha256=?,"
            "active_plan_sha256=?,active_intent_count=? WHERE singleton=1",
            (desired_state_sha256, plan_sha256, intent_count),
        )

    def reconcile(
        self,
        *,
        now: float | None = None,
        allowed_workload_ids: frozenset[str] | None = None,
    ) -> ReconcileResult:
        """Materialize only the current deterministic slot for each intent."""

        allowed = _validated_allowed_workload_ids(allowed_workload_ids)
        wall_now = self._now(now)
        active = self._load_active_desired(wall_now)
        if active is None:
            return ReconcileResult(
                status="idle_unconfigured",
                desired_state_sha256=None,
                intent_count=0,
                inserted_count=0,
                idempotent_count=0,
                missed_deadline_count=0,
                recovered_lease_count=0,
                superseded_count=0,
            )
        all_intents, plan_sha256 = self._build_plan(active.state)
        intents = (
            all_intents
            if allowed is None
            else tuple(
                intent
                for intent in all_intents
                if intent.payload.get("workload_id") in allowed
            )
        )
        self._assert_desired_unchanged(active.state)

        with closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                effective_now = self._guard_clock(db, wall_now)
                recovered = self._expire_and_recover(
                    db, effective_now, allowed
                )
                superseded = self._supersede_inactive(
                    db,
                    active.state.state_sha256,
                    effective_now,
                    allowed,
                )
                self._plan_head(
                    db,
                    active.state.state_sha256,
                    plan_sha256,
                    len(intents),
                )
                candidates = [
                    _slot_candidate(
                        active.state.state_sha256,
                        intent,
                        effective_now,
                    )
                    for intent in intents
                ]
                due = [
                    candidate
                    for candidate in candidates
                    if candidate.deadline_at > effective_now
                ]
                missed = len(candidates) - len(due)
                new_candidates: list[_SlotCandidate] = []
                idempotent = 0
                for candidate in due:
                    row = db.execute(
                        "SELECT record_sha256 FROM scheduler_outbox "
                        "WHERE job_id=?",
                        (candidate.job_id,),
                    ).fetchone()
                    if row is None:
                        new_candidates.append(candidate)
                    elif row["record_sha256"] == candidate.record_sha256:
                        idempotent += 1
                    else:
                        raise SchedulerConflict("stable_job_id_conflict")
                row_count = int(
                    db.execute(
                        "SELECT count(*) FROM scheduler_outbox"
                    ).fetchone()[0]
                )
                if row_count + len(new_candidates) > self._max_outbox_rows:
                    raise SchedulerCapacityError("scheduler_outbox_capacity")
                for candidate in new_candidates:
                    db.execute(
                        "INSERT INTO scheduler_outbox("
                        "job_id,slot_id,record_sha256,desired_state_sha256,"
                        "intent_id,job_type,cadence_seconds,slot_index,"
                        "scheduled_for,deadline_at,priority,payload_json,"
                        "payload_sha256,state,attempt_count,next_attempt_at,"
                        "lease_owner,lease_token,lease_expires_at,"
                        "last_attempt_token,last_transition,last_error_code,"
                        "created_at,updated_at,terminal_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',0,?,"
                        "NULL,NULL,NULL,NULL,NULL,NULL,?,?,NULL)",
                        (
                            candidate.job_id,
                            candidate.slot_id,
                            candidate.record_sha256,
                            candidate.desired_state_sha256,
                            candidate.intent.intent_id,
                            candidate.intent.job_type,
                            candidate.intent.cadence_seconds,
                            candidate.slot_index,
                            candidate.scheduled_for,
                            candidate.deadline_at,
                            candidate.intent.priority,
                            candidate.intent.payload_bytes,
                            candidate.intent.payload_sha256,
                            candidate.scheduled_for,
                            effective_now,
                            effective_now,
                        ),
                    )
                    self._fault("reconcile.after_insert")
                self._fault("reconcile.before_commit")
                db.commit()
                self._fault("reconcile.after_commit")
            except Exception:
                if db.in_transaction:
                    db.rollback()
                raise
        return ReconcileResult(
            status="planned" if intents else "idle_no_intents",
            desired_state_sha256=active.state.state_sha256,
            intent_count=len(intents),
            inserted_count=len(new_candidates),
            idempotent_count=idempotent,
            missed_deadline_count=missed,
            recovered_lease_count=recovered,
            superseded_count=superseded,
        )

    @staticmethod
    def _decode_payload(row: sqlite3.Row) -> dict[str, Any]:
        try:
            raw = bytes(row["payload_json"])
            decoded = json.loads(raw.decode("utf-8"))
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as err:
            raise AutonomousSchedulerError("scheduler_payload_corrupt") from err
        if not isinstance(decoded, dict):
            raise AutonomousSchedulerError("scheduler_payload_corrupt")
        normalised = _normalise_payload(decoded, depth=0, counter=[0])
        canonical = _canonical_json(normalised)
        if (
            raw != canonical
            or len(raw) > MAX_INTENT_PAYLOAD_BYTES
            or hashlib.sha256(raw).hexdigest() != row["payload_sha256"]
        ):
            raise AutonomousSchedulerError("scheduler_payload_corrupt")
        return normalised

    @classmethod
    def _validate_static_row(cls, row: sqlite3.Row) -> dict[str, Any]:
        payload = cls._decode_payload(row)
        job_id, slot_id = _stable_slot_identity(
            str(row["desired_state_sha256"]),
            str(row["intent_id"]),
            int(row["cadence_seconds"]),
            int(row["slot_index"]),
        )
        immutable = {
            "cadence_seconds": int(row["cadence_seconds"]),
            "deadline_at": float(row["deadline_at"]),
            "desired_state_sha256": str(row["desired_state_sha256"]),
            "intent_id": str(row["intent_id"]),
            "job_id": job_id,
            "job_type": str(row["job_type"]),
            "payload": payload,
            "payload_sha256": str(row["payload_sha256"]),
            "priority": int(row["priority"]),
            "scheduled_for": float(row["scheduled_for"]),
            "slot_id": slot_id,
            "slot_index": int(row["slot_index"]),
        }
        digest = hashlib.sha256(_canonical_json(immutable)).hexdigest()
        if (
            row["job_id"] != job_id
            or row["slot_id"] != slot_id
            or row["record_sha256"] != digest
        ):
            raise AutonomousSchedulerError("scheduler_record_corrupt")
        return payload

    def claim_next(
        self,
        lease_owner: str,
        *,
        now: float | None = None,
        allowed_workload_ids: frozenset[str] | None = None,
        prefer_unattempted: bool = False,
    ) -> ClaimedJob | None:
        """Lease at most one ready job for the worker's serial queue."""

        if type(prefer_unattempted) is not bool:
            raise TypeError("prefer_unattempted must be a bool")
        owner = _identifier(lease_owner, "lease_owner")
        allowed = _validated_allowed_workload_ids(allowed_workload_ids)
        # A desired-state refresh creates new content-addressed slot rows even
        # when the acquisition workload itself is unchanged. Moving Life360
        # targets also change region-bound intent IDs, so intent-only history
        # would restart the general provider queue every few minutes. Raw
        # acquisition payloads carry one stable, validated workload_id; use it
        # as the fairness identity across generations and positions. Generic
        # scheduler clients without that field retain the legacy intent_id
        # identity. Select the least-recently attempted stable workload first,
        # then use priority inside an equal-age cohort. This also prevents a
        # daily slot's old absolute scheduled_for from monopolising every new
        # desired-state generation.
        stable_candidate_identity = (
            "CASE WHEN json_type(scheduler_outbox.payload_json,"
            "'$.workload_id')='text' THEN 'workload:' || "
            "json_extract(scheduler_outbox.payload_json,'$.workload_id') "
            "ELSE 'intent:' || scheduler_outbox.intent_id END"
        )
        fair_select = (
            "WITH stable_history AS (SELECT CASE WHEN json_type("
            "history.payload_json,'$.workload_id')='text' THEN "
            "'workload:' || json_extract(history.payload_json,"
            "'$.workload_id') ELSE 'intent:' || history.intent_id END "
            "AS stable_identity,MAX(COALESCE(history.terminal_at,"
            "history.updated_at)) AS last_attempted FROM scheduler_outbox "
            "AS history WHERE history.attempt_count>0 GROUP BY "
            "stable_identity) SELECT scheduler_outbox.* FROM "
            "scheduler_outbox LEFT JOIN stable_history ON "
            "stable_history.stable_identity=("
            + stable_candidate_identity
            + ") "
            if prefer_unattempted
            else "SELECT * FROM scheduler_outbox "
        )
        claim_order = (
            "COALESCE(stable_history.last_attempted,-1.0) ASC,"
            "priority ASC,attempt_count ASC,scheduled_for ASC,job_id ASC"
            if prefer_unattempted
            else "priority ASC,scheduled_for ASC,job_id ASC"
        )
        wall_now = self._now(now)
        active = self._load_active_desired(wall_now)
        if active is None:
            return None
        with closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                effective_now = self._guard_clock(db, wall_now)
                self._expire_and_recover(db, effective_now, allowed)
                self._supersede_inactive(
                    db,
                    active.state.state_sha256,
                    effective_now,
                    allowed,
                )
                active_lease = db.execute(
                    "SELECT * FROM scheduler_outbox WHERE state='leased'"
                ).fetchone()
                if active_lease is not None:
                    self._validate_static_row(active_lease)
                    db.commit()
                    return None
                if allowed is None:
                    row = db.execute(
                        fair_select + "WHERE state IN ('pending','retry') "
                        "AND desired_state_sha256=? AND scheduled_for<=? "
                        "AND next_attempt_at<=? AND deadline_at>? "
                        "ORDER BY " + claim_order + " LIMIT 1",
                        (
                            active.state.state_sha256,
                            effective_now,
                            effective_now,
                            effective_now,
                        ),
                    ).fetchone()
                    payload = (
                        None if row is None else self._validate_static_row(row)
                    )
                else:
                    ready_rows = db.execute(
                        fair_select + "WHERE state IN ('pending','retry') "
                        "AND desired_state_sha256=? AND scheduled_for<=? "
                        "AND next_attempt_at<=? AND deadline_at>? "
                        "ORDER BY " + claim_order,
                        (
                            active.state.state_sha256,
                            effective_now,
                            effective_now,
                            effective_now,
                        ),
                    ).fetchall()
                    row = None
                    payload = None
                    for candidate in ready_rows:
                        candidate_payload = self._validate_static_row(candidate)
                        if candidate_payload.get("workload_id") in allowed:
                            row = candidate
                            payload = candidate_payload
                            break
                if row is None:
                    db.commit()
                    return None
                assert payload is not None
                attempt = int(row["attempt_count"]) + 1
                if attempt > MAX_ATTEMPTS:
                    raise SchedulerCapacityError("scheduler_attempt_capacity")
                token_seed = (
                    f"{row['job_id']}\0{attempt}\0{owner}\0{effective_now}\0"
                    f"{secrets.token_hex(16)}"
                ).encode("utf-8")
                token = hashlib.sha256(token_seed).hexdigest()
                lease_expires = min(
                    effective_now + self._lease_seconds,
                    float(row["deadline_at"]),
                )
                cursor = db.execute(
                    "UPDATE scheduler_outbox SET state='leased',attempt_count=?,"
                    "lease_owner=?,lease_token=?,lease_expires_at=?,"
                    "last_attempt_token=?,last_transition=NULL,"
                    "last_error_code=NULL,updated_at=? WHERE job_id=? "
                    "AND state IN ('pending','retry')",
                    (
                        attempt,
                        owner,
                        token,
                        lease_expires,
                        token,
                        effective_now,
                        row["job_id"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise SchedulerConflict("scheduler_claim_conflict")
                self._fault("claim.after_update")
                self._fault("claim.before_commit")
                db.commit()
                self._fault("claim.after_commit")
            except Exception:
                if db.in_transaction:
                    db.rollback()
                raise
        return ClaimedJob(
            job_id=str(row["job_id"]),
            job_type=str(row["job_type"]),
            payload=payload,
            priority=int(row["priority"]),
            scheduled_for=float(row["scheduled_for"]),
            deadline_at=float(row["deadline_at"]),
            desired_state_sha256=str(row["desired_state_sha256"]),
            attempt_count=attempt,
            lease_token=token,
            lease_expires_at=lease_expires,
        )

    def retry(
        self,
        job_id: str,
        lease_token: str,
        error_code: str,
        *,
        now: float | None = None,
    ) -> TransitionResult:
        """Durably return an exact leased attempt with exponential backoff."""

        job = _identifier(job_id, "job_id")
        token = _identifier(lease_token, "lease_token")
        code = _error_code(error_code)
        wall_now = self._now(now)
        with closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                effective_now = self._guard_clock(db, wall_now)
                row = db.execute(
                    "SELECT * FROM scheduler_outbox WHERE job_id=?",
                    (job,),
                ).fetchone()
                if row is None:
                    raise SchedulerConflict("scheduler_job_unknown")
                self._validate_static_row(row)
                attempt = int(row["attempt_count"])
                if row["state"] == "retry":
                    if (
                        row["last_attempt_token"] == token
                        and row["last_error_code"] == code
                        and row["last_transition"] == "retry"
                    ):
                        db.commit()
                        return TransitionResult(
                            job_id=job,
                            status="retry_idempotent",
                            attempt_count=attempt,
                            next_attempt_at=float(row["next_attempt_at"]),
                        )
                    raise SchedulerConflict("scheduler_retry_conflict")
                if (
                    row["state"] != "leased"
                    or row["lease_token"] != token
                    or row["last_attempt_token"] != token
                ):
                    raise SchedulerConflict("scheduler_retry_conflict")
                if float(row["deadline_at"]) <= effective_now:
                    state = "expired"
                    next_attempt = None
                    terminal_at = effective_now
                    transition = "expired"
                    stored_code = "deadline_expired"
                else:
                    state = "retry"
                    next_attempt = effective_now + self._retry_delay(attempt)
                    terminal_at = None
                    transition = "retry"
                    stored_code = code
                db.execute(
                    "UPDATE scheduler_outbox SET state=?,next_attempt_at=?,"
                    "lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,"
                    "last_transition=?,last_error_code=?,updated_at=?,"
                    "terminal_at=? WHERE job_id=? AND state='leased'",
                    (
                        state,
                        effective_now if next_attempt is None else next_attempt,
                        transition,
                        stored_code,
                        effective_now,
                        terminal_at,
                        job,
                    ),
                )
                self._fault("retry.after_update")
                self._fault("retry.before_commit")
                db.commit()
                self._fault("retry.after_commit")
            except Exception:
                if db.in_transaction:
                    db.rollback()
                raise
        return TransitionResult(
            job_id=job,
            status=state,
            attempt_count=attempt,
            next_attempt_at=next_attempt,
        )

    def complete(
        self,
        job_id: str,
        lease_token: str,
        *,
        now: float | None = None,
    ) -> TransitionResult:
        """Commit one exact lease terminally; identical retries are harmless."""

        job = _identifier(job_id, "job_id")
        token = _identifier(lease_token, "lease_token")
        wall_now = self._now(now)
        with closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                effective_now = self._guard_clock(db, wall_now)
                row = db.execute(
                    "SELECT * FROM scheduler_outbox WHERE job_id=?",
                    (job,),
                ).fetchone()
                if row is None:
                    raise SchedulerConflict("scheduler_job_unknown")
                self._validate_static_row(row)
                attempt = int(row["attempt_count"])
                if row["state"] == "completed":
                    if (
                        row["last_attempt_token"] == token
                        and row["last_transition"] == "completed"
                    ):
                        db.commit()
                        return TransitionResult(
                            job_id=job,
                            status="completed_idempotent",
                            attempt_count=attempt,
                            next_attempt_at=None,
                        )
                    raise SchedulerConflict("scheduler_complete_conflict")
                if (
                    row["state"] != "leased"
                    or row["lease_token"] != token
                    or row["last_attempt_token"] != token
                ):
                    raise SchedulerConflict("scheduler_complete_conflict")
                db.execute(
                    "UPDATE scheduler_outbox SET state='completed',"
                    "lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,"
                    "last_transition='completed',last_error_code=NULL,"
                    "updated_at=?,terminal_at=? WHERE job_id=? AND state='leased'",
                    (effective_now, effective_now, job),
                )
                self._fault("complete.after_update")
                self._fault("complete.before_commit")
                db.commit()
                self._fault("complete.after_commit")
            except Exception:
                if db.in_transaction:
                    db.rollback()
                raise
        return TransitionResult(
            job_id=job,
            status="completed",
            attempt_count=attempt,
            next_attempt_at=None,
        )

    def get_record(self, job_id: str) -> OutboxRecord | None:
        """Return payload-free metadata for one known stable job ID."""

        job = _identifier(job_id, "job_id")
        if not self.path.exists():
            return None
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT * FROM scheduler_outbox WHERE job_id=?",
                (job,),
            ).fetchone()
            if row is None:
                return None
            self._validate_static_row(row)
        return OutboxRecord(
            job_id=str(row["job_id"]),
            intent_id=str(row["intent_id"]),
            state=str(row["state"]),
            desired_state_sha256=str(row["desired_state_sha256"]),
            slot_index=int(row["slot_index"]),
            scheduled_for=float(row["scheduled_for"]),
            deadline_at=float(row["deadline_at"]),
            priority=int(row["priority"]),
            attempt_count=int(row["attempt_count"]),
            next_attempt_at=float(row["next_attempt_at"]),
            lease_expires_at=(
                None
                if row["lease_expires_at"] is None
                else float(row["lease_expires_at"])
            ),
        )

    def workload_id_for_job(self, job_id: str) -> str | None:
        """Return only the validated workload identity for an existing job.

        Scheduler payloads can contain target locations.  Restart recovery
        needs only the workload identifier, so this deliberately does not
        expose the rest of the payload outside the scheduler boundary.
        """

        job = _identifier(job_id, "job_id")
        if not self.path.exists():
            return None
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT * FROM scheduler_outbox WHERE job_id=?",
                (job,),
            ).fetchone()
            if row is None:
                return None
            payload = self._validate_static_row(row)
        workload_id = payload.get("workload_id")
        if not isinstance(workload_id, str):
            raise AutonomousSchedulerError("scheduler_payload_corrupt")
        return _identifier(workload_id, "workload_id")

    def workload_ids_for_jobs(
        self,
        job_ids: tuple[str, ...],
    ) -> dict[str, str]:
        """Resolve a bounded set of jobs with one read-only DB connection."""

        if (
            not isinstance(job_ids, tuple)
            or len(job_ids) > self._max_outbox_rows
            or any(not isinstance(job_id, str) for job_id in job_ids)
        ):
            raise TypeError("job_ids must be a bounded tuple of strings")
        validated = tuple(_identifier(job_id, "job_id") for job_id in job_ids)
        if not self.path.exists():
            return {}
        result: dict[str, str] = {}
        with closing(self._connect()) as db:
            for job_id in validated:
                row = db.execute(
                    "SELECT * FROM scheduler_outbox WHERE job_id=?",
                    (job_id,),
                ).fetchone()
                if row is None:
                    continue
                payload = self._validate_static_row(row)
                workload_id = payload.get("workload_id")
                if not isinstance(workload_id, str):
                    raise AutonomousSchedulerError(
                        "scheduler_payload_corrupt"
                    )
                result[job_id] = _identifier(
                    workload_id,
                    "workload_id",
                )
        return result

    def terminal_proof(self, job_id: str) -> SchedulerTerminalProof | None:
        """Return a checksum-bound proof only for an immutable terminal row."""

        job = _identifier(job_id, "job_id")
        if not self.path.exists():
            return None
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT * FROM scheduler_outbox WHERE job_id=?",
                (job,),
            ).fetchone()
            if row is None:
                return None
            self._validate_static_row(row)
            if (
                str(row["state"]) not in {"completed", "expired", "superseded"}
                or row["terminal_at"] is None
                or row["lease_owner"] is not None
                or row["lease_token"] is not None
                or row["lease_expires_at"] is not None
                or row["last_transition"] is None
            ):
                raise SchedulerConflict("scheduler_terminal_proof_not_terminal")
            proof = SchedulerTerminalProof(
                job_id=str(row["job_id"]),
                slot_id=str(row["slot_id"]),
                record_sha256=str(row["record_sha256"]),
                desired_state_sha256=str(row["desired_state_sha256"]),
                intent_id=str(row["intent_id"]),
                job_type=str(row["job_type"]),
                payload_sha256=str(row["payload_sha256"]),
                state=str(row["state"]),
                slot_index=int(row["slot_index"]),
                scheduled_for=float(row["scheduled_for"]),
                deadline_at=float(row["deadline_at"]),
                attempt_count=int(row["attempt_count"]),
                last_transition=str(row["last_transition"]),
                last_error_code=(
                    None
                    if row["last_error_code"] is None
                    else str(row["last_error_code"])
                ),
                terminal_at=float(row["terminal_at"]),
            )
        # Force the bounded canonical mapping and hash before authority escapes.
        if len(_canonical_json(proof.as_dict())) > 8 * 1024:
            raise SchedulerCapacityError("scheduler_terminal_proof_too_large")
        if not re.fullmatch(r"[0-9a-f]{64}", proof.proof_sha256):
            raise SchedulerConflict("scheduler_terminal_proof_hash_invalid")
        return proof

    def diagnostics(self) -> dict[str, Any]:
        """Return fixed aggregate diagnostics without payloads, IDs or coords."""

        if not self.path.exists():
            return {
                "contract": CONTRACT,
                "status": "idle",
                "outbox_rows": 0,
                "outbox_capacity": self._max_outbox_rows,
                "database_bytes": 0,
                "state_counts": {state: 0 for state in _OUTBOX_STATES},
                "active_lease": False,
                "last_wall_time": None,
                "active_desired_state_sha256": None,
                "active_plan_sha256": None,
                "active_intent_count": 0,
            }
        with closing(self._connect()) as db:
            page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
            counts = {state: 0 for state in _OUTBOX_STATES}
            for row in db.execute(
                "SELECT state,count(*) AS amount FROM scheduler_outbox "
                "GROUP BY state"
            ).fetchall():
                counts[str(row["state"])] = int(row["amount"])
            runtime = db.execute(
                "SELECT * FROM scheduler_runtime WHERE singleton=1"
            ).fetchone()
        result = {
            "contract": CONTRACT,
            "status": "operational",
            "outbox_rows": sum(counts.values()),
            "outbox_capacity": self._max_outbox_rows,
            "database_bytes": page_size * page_count,
            "state_counts": counts,
            "active_lease": counts["leased"] == 1,
            "last_wall_time": (
                None
                if runtime["last_wall_time"] is None
                else float(runtime["last_wall_time"])
            ),
            "active_desired_state_sha256": runtime[
                "active_desired_state_sha256"
            ],
            "active_plan_sha256": runtime["active_plan_sha256"],
            "active_intent_count": int(runtime["active_intent_count"]),
        }
        if len(_canonical_json(result)) > MAX_DIAGNOSTIC_BYTES:
            raise AutonomousSchedulerError("scheduler_diagnostics_unbounded")
        return result


__all__ = [
    "CONTRACT",
    "MAX_PLAN_INTENTS",
    "MAX_PLAN_BYTES",
    "MAX_INTENT_PAYLOAD_BYTES",
    "MAX_PAYLOAD_ITEMS",
    "MAX_OUTBOX_ROWS",
    "AutonomousSchedulerError",
    "ScheduleIntentError",
    "SchedulerConflict",
    "SchedulerCapacityError",
    "SchedulerClockError",
    "SchedulerDesiredStateError",
    "ScheduleIntent",
    "ReconcileResult",
    "ClaimedJob",
    "TransitionResult",
    "OutboxRecord",
    "SchedulerTerminalProof",
    "AutonomousScheduler",
]
