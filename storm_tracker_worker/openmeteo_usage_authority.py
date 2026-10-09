"""Durable, inert Open-Meteo free-tier usage and terms authority.

The sidecar charges a fixed conservative usage cost in four UTC windows before
any fetch can start.  A separately persisted one-shot fetch permit prevents a
crash or retry from authorizing a second network call.  Both successful and
failed terminal states remain charged.  This module performs no network I/O
and is intentionally not wired into acquisition or service code.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
from types import SimpleNamespace
from typing import Any, Callable, Final, Literal, Mapping

from .acquisition_query_authority import (
    AcquisitionQueryAuthority,
    validate_acquisition_query_authority,
)
from .openmeteo_native import (
    CURRENT,
    HOURLY,
    MINUTELY_15,
    OPENMETEO_ATTRIBUTION_POLICY_CANONICAL,
    OPENMETEO_ATTRIBUTION_POLICY_ID,
    OPENMETEO_ATTRIBUTION_POLICY_SHA256,
    _request_identity as _native_request_identity,
    _request_parameters as _native_request_parameters,
    _validate_job as _validate_native_job,
)
from .worker_acquisition_plan import (
    CADENCE_PROFILE_ID as ACQUISITION_CADENCE_PROFILE_ID,
    CADENCE_PROFILE_SHA256 as ACQUISITION_CADENCE_PROFILE_SHA256,
    CATALOG_SHA256 as ACQUISITION_CATALOG_SHA256,
    acquisition_catalog_snapshot,
)


CONTRACT: Final = "stv3-openmeteo-usage-terms-authority-v1"
PROFILE_SCHEMA: Final = "stv3-openmeteo-usage-profile-v1"
RESERVATION_SCHEMA: Final = "stv3-openmeteo-usage-reservation-v1"
PERMIT_SCHEMA: Final = "stv3-openmeteo-one-shot-fetch-permit-v1"
TERMINAL_SCHEMA: Final = "stv3-openmeteo-usage-terminal-v1"
PRUNE_CHAIN_SCHEMA: Final = "stv3-openmeteo-usage-prune-chain-v1"
DIAGNOSTICS_CONTRACT: Final = "stv3-openmeteo-usage-diagnostics-v1"
OWNER_LOCK_CONTRACT: Final = "stv3-openmeteo-usage-owner-lock-v1"
SCHEMA_VERSION: Final = 1
JOURNAL_MODE: Final = "delete"
ADMISSION_STATUS: Final = "inert_usage_authority_not_activated"

# The policy was reviewed on 2026-08-09.  This fixed deadline deliberately
# blocks fresh reservations and permits from 2026-09-08T00:00:00Z onward.
TERMS_REVIEWED_AT: Final = 1_786_233_600
TERMS_REVIEW_DUE_AT: Final = 1_788_825_600
FETCH_AUTHORIZATION_SECONDS: Final = 30
RETENTION_SECONDS: Final = 62 * 86_400
PRUNE_BATCH_ROWS: Final = 1_000
OPENMETEO_VARIABLE_COUNT: Final = len(CURRENT) + len(MINUTELY_15) + len(HOURLY)
USAGE_UNITS_PER_LOCATION: Final = math.ceil(OPENMETEO_VARIABLE_COUNT / 10)
OPENMETEO_CATALOG_CADENCE_SECONDS: Final = 900
OPENMETEO_CATALOG_FETCH_SLOTS_PER_UTC_DAY: Final = (
    86_400 // OPENMETEO_CATALOG_CADENCE_SECONDS
)

OPENMETEO_PRICING_POLICY_CANONICAL: Final = (
    b'{"contract":"stv3-openmeteo-pricing-metering-policy-v1",'
    b'"cost_formula":"ceil(variable_count/10)*query_count",'
    b'"provider_metering_rules_apply":true,"query_count_maximum":64,'
    b'"reviewed_on":"2026-08-09",'
    b'"source_url":"https://open-meteo.com/en/pricing",'
    b'"terms_policy_id":"'
    + OPENMETEO_ATTRIBUTION_POLICY_ID.encode("ascii")
    + b'","variable_count":19,"variable_group_size":10}'
)
OPENMETEO_PRICING_POLICY_SHA256: Final = hashlib.sha256(
    OPENMETEO_PRICING_POLICY_CANONICAL
).hexdigest()
OPENMETEO_PRICING_POLICY_ID: Final = (
    "omp1:" + OPENMETEO_PRICING_POLICY_SHA256
)

PUBLISHED_LIMITS: Final[dict[str, int]] = {
    "minute": 600,
    "hour": 5_000,
    "day": 10_000,
    "month": 300_000,
}
DEFAULT_INTERNAL_LIMITS: Final[dict[str, int]] = {
    "minute": 30,
    "hour": 240,
    "day": 1_000,
    "month": 20_000,
}
MAX_INTERNAL_LIMITS: Final[dict[str, int]] = {
    key: value // 5 for key, value in PUBLISHED_LIMITS.items()
}
WINDOW_KINDS: Final = ("minute", "hour", "day", "month")
FAILURE_REASON_CODES: Final = frozenset({
    "authorization_expired",
    "cancelled",
    "contract_rejected",
    "deadline_expired",
    "durability_failed",
    "recovery_unresolved",
    "transport_failed",
})
MAX_CATALOG_BYTES_HARD: Final = 64 * 1024 * 1024
MIN_CATALOG_BYTES: Final = 1024 * 1024
MAX_RESERVATIONS_HARD: Final = 250_000
DEFAULT_MAX_RESERVATIONS: Final = 100_000
PROFILE_CAPACITY_BASE_BYTES: Final = 4 * 1024 * 1024
PROFILE_BYTES_PER_RESERVATION_BOUND: Final = 2048
CATALOG_MAIN_FRACTION_NUMERATOR: Final = 3
CATALOG_MAIN_FRACTION_DENOMINATOR: Final = 4
TERMS_POLICY_DAYS: Final = (
    TERMS_REVIEW_DUE_AT - TERMS_REVIEWED_AT
) // 86_400
MAXIMUM_ROWS_BEFORE_TERMS_DUE: Final = (
    TERMS_POLICY_DAYS
    * (DEFAULT_INTERNAL_LIMITS["day"] // USAGE_UNITS_PER_LOCATION)
)
MINIMUM_MAIN_CATALOG_BYTES: Final = (
    PROFILE_CAPACITY_BASE_BYTES
    + MAXIMUM_ROWS_BEFORE_TERMS_DUE
    * PROFILE_BYTES_PER_RESERVATION_BOUND
)
MINIMUM_PHYSICAL_ENVELOPE_BYTES: Final = math.ceil(
    MINIMUM_MAIN_CATALOG_BYTES
    * CATALOG_MAIN_FRACTION_DENOMINATOR
    / CATALOG_MAIN_FRACTION_NUMERATOR
)
MAX_DIAGNOSTIC_BYTES: Final = 4096
MAX_PATH_CHARS: Final = 4096
MAX_TIMESTAMP: Final = 4_102_444_799

_SAFE_JOB_RE = re.compile(
    r"^acq:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RESERVATION_ID_RE = re.compile(r"^omur:[0-9a-f]{64}$")
_TERMINAL_STATUSES = frozenset({"COMMITTED", "FAILED"})


class OpenMeteoUsageError(RuntimeError):
    """Base class for detail-free, fail-closed usage-authority errors."""


class OpenMeteoUsageConflict(OpenMeteoUsageError):
    """A durable identity or CAS token was reused inconsistently."""


class OpenMeteoUsageLimitExceeded(OpenMeteoUsageError):
    """An internal usage, row or physical catalog bound was exceeded."""


class OpenMeteoUsageTermsExpired(OpenMeteoUsageError):
    """The fixed terms review deadline blocks a fresh fetch."""


class OpenMeteoUsageClockRollback(OpenMeteoUsageError):
    """The wall clock predates the last durable observation."""


class OpenMeteoUsageIntegrityError(OpenMeteoUsageError):
    """The schema, policy, row hashes or aggregates failed validation."""


class OpenMeteoUsageDurabilityError(OpenMeteoUsageError):
    """SQLite or a durability barrier could not prove persistence."""


class OpenMeteoUsageOwnerBusy(OpenMeteoUsageError):
    """Another process or in-process instance owns the sidecar."""


class OpenMeteoUsageOwnerLost(OpenMeteoUsageError):
    """A lease no longer owns the exact lock anchor."""


@dataclass(frozen=True, slots=True)
class OpenMeteoUsageProfile:
    """Immutable limits; callers may only tighten the compiled hard profile."""

    minute_units: int = DEFAULT_INTERNAL_LIMITS["minute"]
    hour_units: int = DEFAULT_INTERNAL_LIMITS["hour"]
    day_units: int = DEFAULT_INTERNAL_LIMITS["day"]
    month_units: int = DEFAULT_INTERNAL_LIMITS["month"]
    max_reservations: int = DEFAULT_MAX_RESERVATIONS
    max_catalog_bytes: int = MAX_CATALOG_BYTES_HARD
    sqlite_busy_timeout_seconds: float = 2.0

    def __post_init__(self) -> None:
        values = self.limits
        for kind in WINDOW_KINDS:
            value = values[kind]
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not USAGE_UNITS_PER_LOCATION
                <= value <= DEFAULT_INTERNAL_LIMITS[kind]
            ):
                raise ValueError(f"{kind}_units is outside the hard profile")
        if not (
            values["minute"] <= values["hour"]
            <= values["day"] <= values["month"]
        ):
            raise ValueError("usage window limits are not monotonic")
        if (
            isinstance(self.max_reservations, bool)
            or not isinstance(self.max_reservations, int)
            or not self.maximum_rows_before_terms_due
            <= self.max_reservations
            <= DEFAULT_MAX_RESERVATIONS
        ):
            raise ValueError("max_reservations is outside the hard bound")
        if (
            isinstance(self.max_catalog_bytes, bool)
            or not isinstance(self.max_catalog_bytes, int)
            or not self.minimum_physical_envelope_bytes
            <= self.max_catalog_bytes <= MAX_CATALOG_BYTES_HARD
        ):
            raise ValueError("max_catalog_bytes is outside the hard bound")
        timeout = self.sqlite_busy_timeout_seconds
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or not 0.001 <= float(timeout) <= 30.0
        ):
            raise ValueError("sqlite_busy_timeout_seconds is invalid")

    @property
    def limits(self) -> dict[str, int]:
        return {
            "minute": self.minute_units,
            "hour": self.hour_units,
            "day": self.day_units,
            "month": self.month_units,
        }

    @property
    def sustainable_locations_at_catalog_cadence(self) -> int:
        """Whole locations sustainable for every catalog slot for one day."""

        return self.day_units // (
            OPENMETEO_CATALOG_FETCH_SLOTS_PER_UTC_DAY
            * USAGE_UNITS_PER_LOCATION
        )

    @property
    def maximum_rows_before_terms_due(self) -> int:
        return TERMS_POLICY_DAYS * (
            self.day_units // USAGE_UNITS_PER_LOCATION
        )

    @property
    def minimum_main_catalog_bytes(self) -> int:
        return (
            PROFILE_CAPACITY_BASE_BYTES
            + self.maximum_rows_before_terms_due
            * PROFILE_BYTES_PER_RESERVATION_BOUND
        )

    @property
    def minimum_physical_envelope_bytes(self) -> int:
        return math.ceil(
            self.minimum_main_catalog_bytes
            * CATALOG_MAIN_FRACTION_DENOMINATOR
            / CATALOG_MAIN_FRACTION_NUMERATOR
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "admission_status": ADMISSION_STATUS,
            "acquisition_catalog_sha256": ACQUISITION_CATALOG_SHA256,
            "acquisition_cadence_profile_id": ACQUISITION_CADENCE_PROFILE_ID,
            "acquisition_cadence_profile_sha256": (
                ACQUISITION_CADENCE_PROFILE_SHA256
            ),
            "catalog_journal_mode": JOURNAL_MODE,
            "catalog_main_fraction": {
                "denominator": CATALOG_MAIN_FRACTION_DENOMINATOR,
                "numerator": CATALOG_MAIN_FRACTION_NUMERATOR,
            },
            "catalog_cadence_seconds": OPENMETEO_CATALOG_CADENCE_SECONDS,
            "catalog_fetch_slots_per_utc_day": (
                OPENMETEO_CATALOG_FETCH_SLOTS_PER_UTC_DAY
            ),
            "contract": PROFILE_SCHEMA,
            "fetch_authorization_seconds": FETCH_AUTHORIZATION_SECONDS,
            "cost_formula": "ceil(variable_count/10)*query_count",
            "pricing_policy_id": OPENMETEO_PRICING_POLICY_ID,
            "pricing_policy_sha256": OPENMETEO_PRICING_POLICY_SHA256,
            "internal_limits": self.limits,
            "max_catalog_bytes": self.max_catalog_bytes,
            "max_reservations": self.max_reservations,
            "maximum_rows_before_terms_due": (
                self.maximum_rows_before_terms_due
            ),
            "minimum_physical_envelope_bytes": (
                self.minimum_physical_envelope_bytes
            ),
            "minimum_main_catalog_bytes": self.minimum_main_catalog_bytes,
            "operational_admission_eligible": False,
            "policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
            "policy_sha256": OPENMETEO_ATTRIBUTION_POLICY_SHA256,
            "published_limits": PUBLISHED_LIMITS,
            "retention_seconds": RETENTION_SECONDS,
            "sqlite_busy_timeout_seconds": float(
                self.sqlite_busy_timeout_seconds
            ),
            "sustainable_locations_at_catalog_cadence": (
                self.sustainable_locations_at_catalog_cadence
            ),
            "terms_review_due_at": TERMS_REVIEW_DUE_AT,
            "terms_reviewed_at": TERMS_REVIEWED_AT,
            "usage_released_on_failure": False,
            "usage_units_per_location": USAGE_UNITS_PER_LOCATION,
            "variable_count": OPENMETEO_VARIABLE_COUNT,
        }


DEFAULT_USAGE_PROFILE: Final = OpenMeteoUsageProfile()


@dataclass(frozen=True, slots=True)
class OpenMeteoUsageReservation:
    reservation_id: str
    job_identity: str
    policy_id: str
    profile_sha256: str
    request_sha256: str
    reservation_sha256: str
    authority_sha256: str
    query_batch_sha256: str
    native_request_sha256: str
    pricing_policy_id: str
    status: Literal["RESERVED", "COMMITTED", "FAILED"]
    created_at: int
    authorization_expires_at: int
    minute_start: int
    hour_start: int
    day_start: int
    month_start: int
    query_count: int
    variable_count: int
    cost_units: int
    fetch_started_at: int | None
    fetch_permit_sha256: str | None
    state_sha256: str
    terminal_at: int | None
    terminal_reason_code: str | None
    capture_receipt_sha256: str | None
    terminal_sha256: str | None

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    def to_mapping(self) -> dict[str, object]:
        return {
            "admission_status": ADMISSION_STATUS,
            "authorization_expires_at": self.authorization_expires_at,
            "authority_sha256": self.authority_sha256,
            "capture_receipt_sha256": self.capture_receipt_sha256,
            "contract": RESERVATION_SCHEMA,
            "cost_units": self.cost_units,
            "created_at": self.created_at,
            "day_start": self.day_start,
            "fetch_permit_sha256": self.fetch_permit_sha256,
            "fetch_started_at": self.fetch_started_at,
            "hour_start": self.hour_start,
            "job_identity": self.job_identity,
            "minute_start": self.minute_start,
            "month_start": self.month_start,
            "native_request_sha256": self.native_request_sha256,
            "operational_admission_eligible": False,
            "policy_id": self.policy_id,
            "pricing_policy_id": self.pricing_policy_id,
            "profile_sha256": self.profile_sha256,
            "query_batch_sha256": self.query_batch_sha256,
            "query_count": self.query_count,
            "request_sha256": self.request_sha256,
            "reservation_id": self.reservation_id,
            "reservation_sha256": self.reservation_sha256,
            "state_sha256": self.state_sha256,
            "status": self.status,
            "terminal_at": self.terminal_at,
            "terminal_reason_code": self.terminal_reason_code,
            "terminal_sha256": self.terminal_sha256,
            "variable_count": self.variable_count,
        }


@dataclass(frozen=True, slots=True)
class OpenMeteoFetchPermit:
    reservation_id: str
    reservation_sha256: str
    policy_id: str
    pricing_policy_id: str
    profile_sha256: str
    authority_sha256: str
    query_batch_sha256: str
    native_request_sha256: str
    query_count: int
    variable_count: int
    charged_units: int
    started_at: int
    expires_at: int
    permit_sha256: str
    new_fetch_authorized: bool

    def to_mapping(self) -> dict[str, object]:
        return {
            "contract": PERMIT_SCHEMA,
            "authority_sha256": self.authority_sha256,
            "charged_units": self.charged_units,
            "expires_at": self.expires_at,
            "new_fetch_authorized": self.new_fetch_authorized,
            "operational_admission_eligible": False,
            "permit_sha256": self.permit_sha256,
            "policy_id": self.policy_id,
            "pricing_policy_id": self.pricing_policy_id,
            "profile_sha256": self.profile_sha256,
            "query_batch_sha256": self.query_batch_sha256,
            "query_count": self.query_count,
            "reservation_id": self.reservation_id,
            "reservation_sha256": self.reservation_sha256,
            "started_at": self.started_at,
            "native_request_sha256": self.native_request_sha256,
            "variable_count": self.variable_count,
        }


@dataclass(frozen=True, slots=True)
class _UsageBinding:
    job_identity: str
    authority_sha256: str
    query_batch_sha256: str
    native_request_sha256: str
    query_count: int
    variable_count: int
    charged_units: int


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha256(value: object, code: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise OpenMeteoUsageConflict(code)
    return value


def _job_identity(value: object) -> str:
    if not isinstance(value, str) or _SAFE_JOB_RE.fullmatch(value) is None:
        raise OpenMeteoUsageConflict("job_identity_invalid")
    return value


def _reservation_id(value: object) -> str:
    if not isinstance(value, str) or _RESERVATION_ID_RE.fullmatch(value) is None:
        raise OpenMeteoUsageConflict("reservation_id_invalid")
    return value


def _timestamp(value: object, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OpenMeteoUsageClockRollback(code)
    try:
        numeric = float(value)
    except (OverflowError, TypeError, ValueError):
        raise OpenMeteoUsageClockRollback(code) from None
    if not math.isfinite(numeric) or not 0 <= numeric <= MAX_TIMESTAMP:
        raise OpenMeteoUsageClockRollback(code)
    return math.floor(numeric)


def _window_bounds(timestamp: int) -> dict[str, tuple[int, int]]:
    minute = timestamp - timestamp % 60
    hour = timestamp - timestamp % 3_600
    day = timestamp - timestamp % 86_400
    current = datetime.fromtimestamp(timestamp, timezone.utc)
    month = int(datetime(
        current.year, current.month, 1, tzinfo=timezone.utc
    ).timestamp())
    if current.month == 12:
        following = datetime(current.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        following = datetime(
            current.year, current.month + 1, 1, tzinfo=timezone.utc
        )
    return {
        "minute": (minute, minute + 60),
        "hour": (hour, hour + 3_600),
        "day": (day, day + 86_400),
        "month": (month, int(following.timestamp())),
    }


def _normalize_sql(value: str) -> str:
    return " ".join(value.replace("\n", " ").split()).strip().lower()


_TABLE_DDL = (
    (
        "manifest",
        """
        CREATE TABLE manifest(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID
        """,
    ),
    (
        "ledger_state",
        """
        CREATE TABLE ledger_state(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            revision INTEGER NOT NULL CHECK(revision>=0),
            last_observed_at INTEGER NOT NULL CHECK(last_observed_at>=0),
            terms_review_required INTEGER NOT NULL
                CHECK(terms_review_required IN (0,1)),
            reservation_rows INTEGER NOT NULL CHECK(reservation_rows>=0),
            reserved_count INTEGER NOT NULL CHECK(reserved_count>=0),
            committed_count INTEGER NOT NULL CHECK(committed_count>=0),
            failed_count INTEGER NOT NULL CHECK(failed_count>=0),
            pruned_count INTEGER NOT NULL CHECK(pruned_count>=0),
            clock_rollback_terminalizations INTEGER NOT NULL
                CHECK(clock_rollback_terminalizations>=0),
            prune_chain_sha256 TEXT NOT NULL,
            state_sha256 TEXT NOT NULL
        )
        """,
    ),
    (
        "window_usage",
        """
        CREATE TABLE window_usage(
            window_kind TEXT NOT NULL
                CHECK(window_kind IN ('minute','hour','day','month')),
            window_start INTEGER NOT NULL CHECK(window_start>=0),
            window_end INTEGER NOT NULL CHECK(window_end>window_start),
            charged_units INTEGER NOT NULL CHECK(charged_units>0),
            reservation_count INTEGER NOT NULL CHECK(reservation_count>0),
            row_sha256 TEXT NOT NULL,
            PRIMARY KEY(window_kind,window_start)
        ) WITHOUT ROWID
        """,
    ),
    (
        "reservations",
        """
        CREATE TABLE reservations(
            reservation_id TEXT PRIMARY KEY,
            job_identity TEXT NOT NULL UNIQUE,
            policy_id TEXT NOT NULL,
            profile_sha256 TEXT NOT NULL,
            request_sha256 TEXT NOT NULL,
            reservation_sha256 TEXT NOT NULL,
            authority_sha256 TEXT NOT NULL,
            query_batch_sha256 TEXT NOT NULL,
            native_request_sha256 TEXT NOT NULL,
            pricing_policy_id TEXT NOT NULL,
            status TEXT NOT NULL
                CHECK(status IN ('RESERVED','COMMITTED','FAILED')),
            created_at INTEGER NOT NULL CHECK(created_at>=0),
            authorization_expires_at INTEGER NOT NULL
                CHECK(authorization_expires_at>=created_at),
            minute_start INTEGER NOT NULL CHECK(minute_start>=0),
            minute_end INTEGER NOT NULL CHECK(minute_end>minute_start),
            hour_start INTEGER NOT NULL CHECK(hour_start>=0),
            hour_end INTEGER NOT NULL CHECK(hour_end>hour_start),
            day_start INTEGER NOT NULL CHECK(day_start>=0),
            day_end INTEGER NOT NULL CHECK(day_end>day_start),
            month_start INTEGER NOT NULL CHECK(month_start>=0),
            month_end INTEGER NOT NULL CHECK(month_end>month_start),
            query_count INTEGER NOT NULL CHECK(query_count>0),
            variable_count INTEGER NOT NULL CHECK(variable_count>0),
            cost_units INTEGER NOT NULL CHECK(cost_units>0),
            fetch_started_at INTEGER,
            fetch_permit_sha256 TEXT,
            state_sha256 TEXT NOT NULL,
            terminal_previous_state_sha256 TEXT,
            terminal_at INTEGER,
            terminal_reason_code TEXT,
            capture_receipt_sha256 TEXT,
            terminal_sha256 TEXT,
            CHECK(
                (fetch_started_at IS NULL AND fetch_permit_sha256 IS NULL)
                OR
                (fetch_started_at>=created_at AND
                 fetch_started_at<=authorization_expires_at AND
                 fetch_permit_sha256 IS NOT NULL)
            ),
            CHECK(
                (status='RESERVED' AND
                 terminal_previous_state_sha256 IS NULL AND
                 terminal_at IS NULL AND terminal_reason_code IS NULL AND
                 capture_receipt_sha256 IS NULL AND terminal_sha256 IS NULL)
                OR
                (status='COMMITTED' AND fetch_started_at IS NOT NULL AND
                 terminal_previous_state_sha256 IS NOT NULL AND
                 terminal_at IS NOT NULL AND terminal_reason_code IS NULL AND
                 capture_receipt_sha256 IS NOT NULL AND
                 terminal_sha256 IS NOT NULL)
                OR
                (status='FAILED' AND
                 terminal_previous_state_sha256 IS NOT NULL AND
                 terminal_at IS NOT NULL AND terminal_reason_code IS NOT NULL AND
                 capture_receipt_sha256 IS NULL AND terminal_sha256 IS NOT NULL)
            )
        ) WITHOUT ROWID
        """,
    ),
)

_INDEX_DDL = (
    (
        "reservations_prune_order",
        "CREATE INDEX reservations_prune_order ON reservations("
        "status,month_end,reservation_id)",
    ),
)

_TRIGGER_DDL = (
    (
        "manifest_no_update",
        "CREATE TRIGGER manifest_no_update BEFORE UPDATE ON manifest "
        "BEGIN SELECT RAISE(ABORT,'manifest is immutable'); END",
        "manifest",
    ),
    (
        "manifest_no_delete",
        "CREATE TRIGGER manifest_no_delete BEFORE DELETE ON manifest "
        "BEGIN SELECT RAISE(ABORT,'manifest is immutable'); END",
        "manifest",
    ),
    (
        "reservation_delete_guard",
        "CREATE TRIGGER reservation_delete_guard BEFORE DELETE ON reservations "
        "WHEN usage_prune_authorized()<>1 "
        "BEGIN SELECT RAISE(ABORT,'reservation deletion is not authorized'); END",
        "reservations",
    ),
    (
        "window_delete_guard",
        "CREATE TRIGGER window_delete_guard BEFORE DELETE ON window_usage "
        "WHEN usage_prune_authorized()<>1 "
        "BEGIN SELECT RAISE(ABORT,'window deletion is not authorized'); END",
        "window_usage",
    ),
    (
        "window_increment_only",
        "CREATE TRIGGER window_increment_only BEFORE UPDATE ON window_usage "
        "WHEN NOT(NEW.window_kind=OLD.window_kind AND "
        "NEW.window_start=OLD.window_start AND NEW.window_end=OLD.window_end AND "
        "((usage_prune_authorized()<>1 AND "
        "NEW.charged_units>OLD.charged_units AND "
        "NEW.reservation_count=OLD.reservation_count+1) OR "
        "(usage_prune_authorized()=1 AND "
        "NEW.charged_units>0 AND NEW.charged_units<OLD.charged_units AND "
        "NEW.reservation_count>0 AND "
        "NEW.reservation_count<OLD.reservation_count))) "
        "BEGIN SELECT RAISE(ABORT,'window update is not an exact charge'); END",
        "window_usage",
    ),
    (
        "reservation_transition_only",
        """
        CREATE TRIGGER reservation_transition_only BEFORE UPDATE ON reservations
        WHEN NOT(
            NEW.reservation_id=OLD.reservation_id AND
            NEW.job_identity=OLD.job_identity AND
            NEW.policy_id=OLD.policy_id AND
            NEW.profile_sha256=OLD.profile_sha256 AND
            NEW.request_sha256=OLD.request_sha256 AND
            NEW.reservation_sha256=OLD.reservation_sha256 AND
            NEW.authority_sha256=OLD.authority_sha256 AND
            NEW.query_batch_sha256=OLD.query_batch_sha256 AND
            NEW.native_request_sha256=OLD.native_request_sha256 AND
            NEW.pricing_policy_id=OLD.pricing_policy_id AND
            NEW.created_at=OLD.created_at AND
            NEW.authorization_expires_at=OLD.authorization_expires_at AND
            NEW.minute_start=OLD.minute_start AND NEW.minute_end=OLD.minute_end AND
            NEW.hour_start=OLD.hour_start AND NEW.hour_end=OLD.hour_end AND
            NEW.day_start=OLD.day_start AND NEW.day_end=OLD.day_end AND
            NEW.month_start=OLD.month_start AND NEW.month_end=OLD.month_end AND
            NEW.query_count=OLD.query_count AND
            NEW.variable_count=OLD.variable_count AND
            NEW.cost_units=OLD.cost_units AND
            (
                (OLD.status='RESERVED' AND NEW.status='RESERVED' AND
                 OLD.fetch_started_at IS NULL AND
                 OLD.fetch_permit_sha256 IS NULL AND
                 NEW.fetch_started_at IS NOT NULL AND
                 NEW.fetch_permit_sha256 IS NOT NULL AND
                 NEW.terminal_previous_state_sha256 IS NULL AND
                 NEW.terminal_at IS NULL AND NEW.terminal_reason_code IS NULL AND
                 NEW.capture_receipt_sha256 IS NULL AND NEW.terminal_sha256 IS NULL)
                OR
                (OLD.status='RESERVED' AND
                 NEW.status IN ('COMMITTED','FAILED') AND
                 NEW.fetch_started_at IS OLD.fetch_started_at AND
                 NEW.fetch_permit_sha256 IS OLD.fetch_permit_sha256 AND
                 NEW.terminal_previous_state_sha256=OLD.state_sha256 AND
                 NEW.terminal_at IS NOT NULL AND NEW.terminal_sha256 IS NOT NULL)
            )
        )
        BEGIN SELECT RAISE(ABORT,'reservation transition is invalid'); END
        """,
        "reservations",
    ),
)


def _expected_schema_objects() -> tuple[tuple[str, str, str, str], ...]:
    values: list[tuple[str, str, str, str]] = []
    for name, sql in _TABLE_DDL:
        values.append(("table", name, name, _normalize_sql(sql)))
    for name, sql in _INDEX_DDL:
        values.append(("index", name, "reservations", _normalize_sql(sql)))
    for name, sql, table in _TRIGGER_DDL:
        values.append(("trigger", name, table, _normalize_sql(sql)))
    return tuple(sorted(values))


_EXPECTED_SCHEMA_OBJECTS = _expected_schema_objects()
_EXPECTED_SCHEMA_SHA256 = hashlib.sha256(
    _canonical(_EXPECTED_SCHEMA_OBJECTS)
).hexdigest()
_PRUNE_GENESIS_SHA256 = _digest({"contract": PRUNE_CHAIN_SCHEMA, "genesis": True})

_PROCESS_OWNER_LOCK = threading.Lock()
_PROCESS_OWNED_PATHS: set[str] = set()


def _claim_process_path(path: Path) -> bool:
    key = os.path.normcase(str(path))
    with _PROCESS_OWNER_LOCK:
        if key in _PROCESS_OWNED_PATHS:
            return False
        _PROCESS_OWNED_PATHS.add(key)
        return True


def _release_process_path(path: Path) -> None:
    with _PROCESS_OWNER_LOCK:
        _PROCESS_OWNED_PATHS.discard(os.path.normcase(str(path)))


def _lock_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_UN)


class OpenMeteoUsageAuthority:
    """One-owner SQLite authority; only a held lease exposes mutations."""

    def __init__(
        self,
        path: str | Path,
        *,
        profile: OpenMeteoUsageProfile = DEFAULT_USAGE_PROFILE,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        try:
            _validate_native_policy()
        except RuntimeError:
            raise OpenMeteoUsageIntegrityError(
                "openmeteo_policy_binding_drifted"
            ) from None
        if not isinstance(profile, OpenMeteoUsageProfile):
            raise TypeError("profile must be OpenMeteoUsageProfile")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("fault_injector must be callable")
        supplied = Path(path)
        if not supplied.is_absolute() or len(str(supplied)) > MAX_PATH_CHARS:
            raise ValueError("usage database path must be absolute and bounded")
        supplied.parent.mkdir(parents=True, exist_ok=True)
        resolved_parent = supplied.parent.resolve(strict=True)
        if os.path.normcase(str(resolved_parent)) != os.path.normcase(
            str(supplied.parent)
        ):
            raise OpenMeteoUsageIntegrityError("usage_path_parent_is_not_canonical")
        self.path = resolved_parent / supplied.name
        self.lock_path = resolved_parent / (supplied.name + ".owner.lock")
        self.profile = profile
        self.profile_bytes = _canonical(profile.to_mapping())
        self.profile_sha256 = hashlib.sha256(self.profile_bytes).hexdigest()
        self._clock = clock
        self._fault_injector = fault_injector
        self._database_lock = threading.RLock()
        self._owner_lock = threading.Lock()
        self._descriptor: int | None = None
        self._owner_token: object | None = None
        self._prune_authorized = False
        self._lock_anchor_bytes = _canonical({
            "contract": OWNER_LOCK_CONTRACT,
            "policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
            "profile_sha256": self.profile_sha256,
        })
        self._initialize_lock_anchor()
        self._initialize_database()
        self._database_witness = self._path_witness(self.path)
        self._lock_witness = self._path_witness(self.lock_path)

    def acquire(self) -> "OpenMeteoUsageLease":
        """Acquire the exact OS/process owner immediately or fail closed."""

        with self._owner_lock:
            if self._descriptor is not None or self._owner_token is not None:
                raise OpenMeteoUsageOwnerBusy("usage_owner_busy")
            if not _claim_process_path(self.lock_path):
                raise OpenMeteoUsageOwnerBusy("usage_owner_busy")
            flags = os.O_RDWR | getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor: int | None = None
            try:
                self._verify_paths()
                descriptor = os.open(self.lock_path, flags)
                os.set_inheritable(descriptor, False)
                if self._witness(os.fstat(descriptor)) != self._lock_witness:
                    raise OpenMeteoUsageIntegrityError(
                        "usage_owner_anchor_identity_changed"
                    )
                _lock_descriptor(descriptor)
                if self._read_descriptor(descriptor) != self._lock_anchor_bytes:
                    raise OpenMeteoUsageIntegrityError(
                        "usage_owner_anchor_content_changed"
                    )
                self._verify_database()
            except OpenMeteoUsageError:
                if descriptor is not None:
                    try:
                        _unlock_descriptor(descriptor)
                    except (ImportError, OSError):
                        pass
                    os.close(descriptor)
                _release_process_path(self.lock_path)
                raise
            except (BlockingIOError, PermissionError):
                if descriptor is not None:
                    os.close(descriptor)
                _release_process_path(self.lock_path)
                raise OpenMeteoUsageOwnerBusy("usage_owner_busy") from None
            except (ImportError, OSError) as exc:
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                _release_process_path(self.lock_path)
                raise OpenMeteoUsageDurabilityError(
                    "usage_owner_boundary_failed"
                ) from exc
            token = object()
            self._descriptor = descriptor
            self._owner_token = token
            return OpenMeteoUsageLease(self, token)

    def close(self) -> None:
        with self._owner_lock:
            token = self._owner_token
        if token is not None:
            self._release(token)

    def _release(self, token: object) -> None:
        with self._owner_lock:
            if self._owner_token is None and self._descriptor is None:
                return
            if token is not self._owner_token:
                raise OpenMeteoUsageOwnerLost("usage_owner_not_held")
            descriptor = self._descriptor
            self._descriptor = None
            self._owner_token = None
            try:
                if descriptor is not None:
                    _unlock_descriptor(descriptor)
            except (ImportError, OSError):
                pass
            finally:
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                _release_process_path(self.lock_path)

    def _assert_owner(self, token: object) -> None:
        with self._owner_lock:
            if (
                token is not self._owner_token
                or self._descriptor is None
            ):
                raise OpenMeteoUsageOwnerLost("usage_owner_not_held")
            try:
                if (
                    self._witness(os.fstat(self._descriptor))
                    != self._lock_witness
                    or self._read_descriptor(self._descriptor)
                    != self._lock_anchor_bytes
                ):
                    raise OpenMeteoUsageOwnerLost(
                        "usage_owner_identity_lost"
                    )
                self._verify_paths(read_lock=False)
            except OpenMeteoUsageOwnerLost:
                raise
            except (OSError, OpenMeteoUsageError) as exc:
                raise OpenMeteoUsageOwnerLost(
                    "usage_owner_identity_lost"
                ) from exc

    def _authority_binding(
        self,
        *,
        authority: AcquisitionQueryAuthority,
        expected_authority_sha256: str,
        query_batch: Mapping[str, Any],
    ) -> _UsageBinding:
        """Validate the exact authority projection without persisting locations."""

        try:
            expected = _sha256(
                expected_authority_sha256, "expected_authority_sha256_invalid"
            )
            if not isinstance(authority, AcquisitionQueryAuthority):
                raise ValueError("wrong authority type")
            validate_acquisition_query_authority(authority)
            if authority.authority_sha256 != expected:
                raise ValueError("authority checksum mismatch")
            projected = authority.openmeteo_shadow_query_batch()
            if type(query_batch) is not dict:
                raise ValueError("query batch is not an exact object")
            supplied_bytes = _canonical(query_batch)
            if supplied_bytes != _canonical(projected):
                raise ValueError("query batch differs from authority")
            isolated = json.loads(supplied_bytes)
            native_job = SimpleNamespace(
                job_id=authority.native_job_id,
                payload=isolated,
            )
            job_identity, queries = _validate_native_job(native_job)
            if job_identity != authority.native_job_id:
                raise ValueError("native job identity drift")
            params = _native_request_parameters(queries)
            native_request_sha, _query_string = _native_request_identity(
                queries, params
            )
            query_count = len(queries)
            variable_count = OPENMETEO_VARIABLE_COUNT
            charged_units = (
                math.ceil(variable_count / 10) * query_count
            )
            if (
                variable_count != 19
                or not 1 <= query_count <= 64
                or charged_units
                != USAGE_UNITS_PER_LOCATION * query_count
            ):
                raise ValueError("pricing formula drift")
            return _UsageBinding(
                job_identity=job_identity,
                authority_sha256=expected,
                query_batch_sha256=hashlib.sha256(supplied_bytes).hexdigest(),
                native_request_sha256=native_request_sha,
                query_count=query_count,
                variable_count=variable_count,
                charged_units=charged_units,
            )
        except OpenMeteoUsageError:
            raise
        except Exception:
            raise OpenMeteoUsageConflict(
                "authority_query_binding_invalid"
            ) from None

    def _reserve(
        self,
        token: object,
        *,
        authority: AcquisitionQueryAuthority,
        expected_authority_sha256: str,
        query_batch: Mapping[str, Any],
    ) -> OpenMeteoUsageReservation:
        self._assert_owner(token)
        binding = self._authority_binding(
            authority=authority,
            expected_authority_sha256=expected_authority_sha256,
            query_batch=query_batch,
        )
        job = binding.job_identity
        request_sha = self._request_sha(binding)
        reservation = "omur:" + request_sha
        now = self._now()
        with self._database_lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                state = self._state(database)
                self._require_fresh_fetch_clock(database, state, now)
                self._fault("reserve.after_begin")
                row = database.execute(
                    "SELECT * FROM reservations WHERE reservation_id=?",
                    (reservation,),
                ).fetchone()
                if row is not None:
                    existing = self._validated_reservation(row)
                    if (
                        existing.job_identity != job
                        or existing.request_sha256 != request_sha
                        or existing.authority_sha256
                        != binding.authority_sha256
                        or existing.query_batch_sha256
                        != binding.query_batch_sha256
                        or existing.native_request_sha256
                        != binding.native_request_sha256
                        or existing.query_count != binding.query_count
                        or existing.variable_count != binding.variable_count
                        or existing.cost_units != binding.charged_units
                    ):
                        raise OpenMeteoUsageConflict(
                            "reservation_identity_reused"
                        )
                    self._advance_state_clock(database, state, now)
                    database.commit()
                    result = existing
                else:
                    if state["reservation_rows"] >= self.profile.max_reservations:
                        raise OpenMeteoUsageLimitExceeded(
                            "usage_reservation_row_limit"
                        )
                    windows = _window_bounds(now)
                    for kind in WINDOW_KINDS:
                        self._charge_window(
                            database,
                            kind,
                            windows[kind],
                            charged_units=binding.charged_units,
                        )
                    expires = min(
                        now + FETCH_AUTHORIZATION_SECONDS,
                        TERMS_REVIEW_DUE_AT,
                    )
                    reservation_sha = self._reservation_sha(
                        reservation_id=reservation,
                        job_identity=job,
                        request_sha256=request_sha,
                        binding=binding,
                        created_at=now,
                        expires_at=expires,
                        windows=windows,
                    )
                    initial_state_sha = self._reservation_state_sha({
                        "capture_receipt_sha256": None,
                        "fetch_permit_sha256": None,
                        "fetch_started_at": None,
                        "reservation_sha256": reservation_sha,
                        "status": "RESERVED",
                        "terminal_at": None,
                        "terminal_previous_state_sha256": None,
                        "terminal_reason_code": None,
                        "terminal_sha256": None,
                    })
                    database.execute(
                        "INSERT INTO reservations("
                        "reservation_id,job_identity,policy_id,profile_sha256,"
                        "request_sha256,reservation_sha256,authority_sha256,"
                        "query_batch_sha256,native_request_sha256,"
                        "pricing_policy_id,status,created_at,"
                        "authorization_expires_at,minute_start,minute_end,"
                        "hour_start,hour_end,day_start,day_end,month_start,"
                        "month_end,query_count,variable_count,cost_units,"
                        "fetch_started_at,"
                        "fetch_permit_sha256,state_sha256,"
                        "terminal_previous_state_sha256,terminal_at,"
                        "terminal_reason_code,capture_receipt_sha256,"
                        "terminal_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,'RESERVED',"
                        "?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,?,NULL,NULL,NULL,NULL,"
                        "NULL)",
                        (
                            reservation,
                            job,
                            OPENMETEO_ATTRIBUTION_POLICY_ID,
                            self.profile_sha256,
                            request_sha,
                            reservation_sha,
                            binding.authority_sha256,
                            binding.query_batch_sha256,
                            binding.native_request_sha256,
                            OPENMETEO_PRICING_POLICY_ID,
                            now,
                            expires,
                            windows["minute"][0],
                            windows["minute"][1],
                            windows["hour"][0],
                            windows["hour"][1],
                            windows["day"][0],
                            windows["day"][1],
                            windows["month"][0],
                            windows["month"][1],
                            binding.query_count,
                            binding.variable_count,
                            binding.charged_units,
                            initial_state_sha,
                        ),
                    )
                    self._fault("reserve.after_insert")
                    updated = dict(state)
                    updated["revision"] += 1
                    updated["last_observed_at"] = now
                    updated["reservation_rows"] += 1
                    updated["reserved_count"] += 1
                    self._write_state(database, updated)
                    self._audit_database(database, full_integrity=False)
                    self._fault("reserve.before_commit")
                    database.commit()
                    self._fault("reserve.after_commit")
                    result = self._load_from_database(database, reservation)
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                self._raise_sqlite(exc)
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._durability_barrier()
        return result

    def _begin_fetch(
        self,
        token: object,
        *,
        reservation_id: str,
        expected_state_sha256: str,
    ) -> OpenMeteoFetchPermit:
        self._assert_owner(token)
        reservation = _reservation_id(reservation_id)
        expected = _sha256(expected_state_sha256, "expected_state_invalid")
        now = self._now()
        with self._database_lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                state = self._state(database)
                self._require_fresh_fetch_clock(database, state, now)
                row = database.execute(
                    "SELECT * FROM reservations WHERE reservation_id=?",
                    (reservation,),
                ).fetchone()
                if row is None:
                    raise OpenMeteoUsageConflict("reservation_missing")
                existing = self._validated_reservation(row)
                if existing.status != "RESERVED":
                    raise OpenMeteoUsageConflict(
                        "terminal_reservation_cannot_begin_fetch"
                    )
                if existing.fetch_started_at is not None:
                    if expected != self._initial_state_sha(existing):
                        raise OpenMeteoUsageConflict("fetch_permit_cas_mismatch")
                    self._advance_state_clock(database, state, now)
                    database.commit()
                    permit = self._permit(existing, newly_authorized=False)
                else:
                    if expected != existing.state_sha256:
                        raise OpenMeteoUsageConflict("fetch_permit_cas_mismatch")
                    if now >= existing.authorization_expires_at:
                        raise OpenMeteoUsageTermsExpired(
                            "fetch_authorization_expired"
                        )
                    permit_sha = self._permit_sha(existing, started_at=now)
                    started_state = self._reservation_state_sha({
                        "capture_receipt_sha256": None,
                        "fetch_permit_sha256": permit_sha,
                        "fetch_started_at": now,
                        "reservation_sha256": existing.reservation_sha256,
                        "status": "RESERVED",
                        "terminal_at": None,
                        "terminal_previous_state_sha256": None,
                        "terminal_reason_code": None,
                        "terminal_sha256": None,
                    })
                    cursor = database.execute(
                        "UPDATE reservations SET fetch_started_at=?,"
                        "fetch_permit_sha256=?,state_sha256=? "
                        "WHERE reservation_id=? AND status='RESERVED' "
                        "AND fetch_started_at IS NULL AND state_sha256=?",
                        (now, permit_sha, started_state, reservation, expected),
                    )
                    if cursor.rowcount != 1:
                        raise OpenMeteoUsageConflict("fetch_permit_cas_failed")
                    self._fault("begin_fetch.after_update")
                    updated = dict(state)
                    updated["revision"] += 1
                    updated["last_observed_at"] = now
                    self._write_state(database, updated)
                    self._audit_database(database, full_integrity=False)
                    self._fault("begin_fetch.before_commit")
                    database.commit()
                    self._fault("begin_fetch.after_commit")
                    loaded = self._load_from_database(database, reservation)
                    permit = self._permit(loaded, newly_authorized=True)
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                self._raise_sqlite(exc)
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._durability_barrier()
        return permit

    def _terminalize(
        self,
        token: object,
        *,
        reservation_id: str,
        expected_state_sha256: str,
        status: Literal["COMMITTED", "FAILED"],
        capture_receipt_sha256: str | None,
        reason_code: str | None,
    ) -> OpenMeteoUsageReservation:
        self._assert_owner(token)
        reservation = _reservation_id(reservation_id)
        expected = _sha256(expected_state_sha256, "expected_state_invalid")
        if status == "COMMITTED":
            capture = _sha256(
                capture_receipt_sha256, "capture_receipt_sha256_invalid"
            )
            reason = None
        else:
            capture = None
            if reason_code not in FAILURE_REASON_CODES:
                raise OpenMeteoUsageConflict("failure_reason_invalid")
            reason = reason_code
        raw_now = self._now()
        with self._database_lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                state = self._state(database)
                row = database.execute(
                    "SELECT * FROM reservations WHERE reservation_id=?",
                    (reservation,),
                ).fetchone()
                if row is None:
                    raise OpenMeteoUsageConflict("reservation_missing")
                existing = self._validated_reservation(row)
                if existing.status in _TERMINAL_STATUSES:
                    if (
                        existing.status != status
                        or existing.capture_receipt_sha256 != capture
                        or existing.terminal_reason_code != reason
                        or row["terminal_previous_state_sha256"] != expected
                    ):
                        raise OpenMeteoUsageConflict(
                            "terminal_retry_conflicts"
                        )
                    database.commit()
                    result = existing
                else:
                    if existing.state_sha256 != expected:
                        raise OpenMeteoUsageConflict("terminal_cas_mismatch")
                    if status == "COMMITTED" and existing.fetch_started_at is None:
                        raise OpenMeteoUsageConflict(
                            "commit_requires_started_fetch"
                        )
                    rollback = raw_now < state["last_observed_at"]
                    terminal_at = max(raw_now, state["last_observed_at"])
                    terminal_sha = self._terminal_sha(
                        existing,
                        previous_state_sha256=expected,
                        status=status,
                        terminal_at=terminal_at,
                        capture_receipt_sha256=capture,
                        reason_code=reason,
                    )
                    terminal_state = self._reservation_state_sha({
                        "capture_receipt_sha256": capture,
                        "fetch_permit_sha256": existing.fetch_permit_sha256,
                        "fetch_started_at": existing.fetch_started_at,
                        "reservation_sha256": existing.reservation_sha256,
                        "status": status,
                        "terminal_at": terminal_at,
                        "terminal_previous_state_sha256": expected,
                        "terminal_reason_code": reason,
                        "terminal_sha256": terminal_sha,
                    })
                    cursor = database.execute(
                        "UPDATE reservations SET status=?,state_sha256=?,"
                        "terminal_previous_state_sha256=?,terminal_at=?,"
                        "terminal_reason_code=?,capture_receipt_sha256=?,"
                        "terminal_sha256=? WHERE reservation_id=? "
                        "AND status='RESERVED' AND state_sha256=?",
                        (
                            status,
                            terminal_state,
                            expected,
                            terminal_at,
                            reason,
                            capture,
                            terminal_sha,
                            reservation,
                            expected,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise OpenMeteoUsageConflict("terminal_cas_failed")
                    self._fault("terminal.after_update")
                    updated = dict(state)
                    updated["revision"] += 1
                    updated["last_observed_at"] = terminal_at
                    updated["reserved_count"] -= 1
                    updated[
                        "committed_count" if status == "COMMITTED"
                        else "failed_count"
                    ] += 1
                    updated["clock_rollback_terminalizations"] += int(rollback)
                    self._write_state(database, updated)
                    self._audit_database(database, full_integrity=False)
                    self._fault("terminal.before_commit")
                    database.commit()
                    self._fault("terminal.after_commit")
                    result = self._load_from_database(database, reservation)
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                self._raise_sqlite(exc)
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._durability_barrier()
        return result

    def _load(
        self, token: object, reservation_id: str
    ) -> OpenMeteoUsageReservation | None:
        self._assert_owner(token)
        reservation = _reservation_id(reservation_id)
        with self._database_lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM reservations WHERE reservation_id=?",
                (reservation,),
            ).fetchone()
            return None if row is None else self._validated_reservation(row)

    def _prune(self, token: object) -> dict[str, int | str]:
        self._assert_owner(token)
        now = self._now()
        cutoff = max(0, now - RETENTION_SECONDS)
        with self._database_lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                state = self._state(database)
                if now < state["last_observed_at"]:
                    raise OpenMeteoUsageClockRollback("usage_clock_rollback")
                self._audit_database(database, full_integrity=False)
                rows = database.execute(
                    "SELECT * FROM reservations WHERE status IN "
                    "('COMMITTED','FAILED') AND minute_end<=? AND hour_end<=? "
                    "AND day_end<=? AND month_end<=? "
                    "ORDER BY month_end,reservation_id LIMIT ?",
                    (cutoff, cutoff, cutoff, cutoff, PRUNE_BATCH_ROWS),
                ).fetchall()
                receipts = [self._validated_reservation(row) for row in rows]
                if receipts:
                    chain_sha = _digest({
                        "contract": PRUNE_CHAIN_SCHEMA,
                        "cutoff": cutoff,
                        "previous_chain_sha256": state["prune_chain_sha256"],
                        "pruned_at": now,
                        "row_count": len(receipts),
                        "state_sha256s": sorted(
                            item.state_sha256 for item in receipts
                        ),
                    })
                    committed = sum(
                        item.status == "COMMITTED" for item in receipts
                    )
                    failed = len(receipts) - committed
                    self._prune_authorized = True
                    try:
                        database.executemany(
                            "DELETE FROM reservations WHERE reservation_id=?",
                            [(item.reservation_id,) for item in receipts],
                        )
                        self._reconcile_pruned_windows(database, receipts)
                    finally:
                        self._prune_authorized = False
                    updated = dict(state)
                    updated["revision"] += 1
                    updated["last_observed_at"] = now
                    updated["reservation_rows"] -= len(receipts)
                    updated["committed_count"] -= committed
                    updated["failed_count"] -= failed
                    updated["pruned_count"] += len(receipts)
                    updated["prune_chain_sha256"] = chain_sha
                    self._write_state(database, updated)
                    self._fault("prune.after_delete")
                else:
                    self._advance_state_clock(database, state, now)
                    chain_sha = state["prune_chain_sha256"]
                self._audit_database(database, full_integrity=False)
                self._fault("prune.before_commit")
                database.commit()
                self._fault("prune.after_commit")
                result = {
                    "prune_chain_sha256": chain_sha,
                    "pruned_rows": len(receipts),
                    "retention_seconds": RETENTION_SECONDS,
                }
            except sqlite3.DatabaseError as exc:
                self._prune_authorized = False
                if database.in_transaction:
                    database.rollback()
                self._raise_sqlite(exc)
            except BaseException:
                self._prune_authorized = False
                if database.in_transaction:
                    database.rollback()
                raise
        self._durability_barrier()
        return result

    def _audit(self, token: object) -> dict[str, int | str]:
        self._assert_owner(token)
        with self._database_lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database, full_integrity=True)
            result = self._audit_database(database, full_integrity=True)
        return result

    def _diagnostics(self, token: object) -> dict[str, Any]:
        self._assert_owner(token)
        now = self._now()
        with self._database_lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database)
            state = self._state(database)
            windows = _window_bounds(now)
            usage: dict[str, dict[str, int]] = {}
            for kind, (start, _end) in windows.items():
                row = database.execute(
                    "SELECT * FROM window_usage WHERE window_kind=? "
                    "AND window_start=?",
                    (kind, start),
                ).fetchone()
                charged = 0 if row is None else self._validated_window(row)[0]
                usage[kind] = {
                    "charged_units": charged,
                    "internal_limit_units": self.profile.limits[kind],
                }
            result = {
                "admission_status": ADMISSION_STATUS,
                "acquisition_catalog_sha256": ACQUISITION_CATALOG_SHA256,
                "acquisition_cadence_profile_id": (
                    ACQUISITION_CADENCE_PROFILE_ID
                ),
                "acquisition_cadence_profile_sha256": (
                    ACQUISITION_CADENCE_PROFILE_SHA256
                ),
                "catalog_cadence_seconds": (
                    OPENMETEO_CATALOG_CADENCE_SECONDS
                ),
                "catalog_fetch_slots_per_utc_day": (
                    OPENMETEO_CATALOG_FETCH_SLOTS_PER_UTC_DAY
                ),
                "catalog_bytes": self._catalog_bytes(),
                "clock_rollback_detected": now < state["last_observed_at"],
                "clock_rollback_terminalizations": state[
                    "clock_rollback_terminalizations"
                ],
                "committed_count": state["committed_count"],
                "contract": DIAGNOSTICS_CONTRACT,
                "failed_count": state["failed_count"],
                "maximum_rows_before_terms_due": (
                    self.profile.maximum_rows_before_terms_due
                ),
                "minimum_physical_envelope_bytes": (
                    self.profile.minimum_physical_envelope_bytes
                ),
                "minimum_main_catalog_bytes": (
                    self.profile.minimum_main_catalog_bytes
                ),
                "openmeteo_variable_count": OPENMETEO_VARIABLE_COUNT,
                "pricing_policy_id": OPENMETEO_PRICING_POLICY_ID,
                "operational_admission_eligible": False,
                "owner_backend": (
                    "windows_msvcrt" if os.name == "nt" else "posix_flock"
                ),
                "policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
                "profile_sha256": self.profile_sha256,
                "pruned_count": state["pruned_count"],
                "reservation_rows": state["reservation_rows"],
                "reserved_count": state["reserved_count"],
                "retention_seconds": RETENTION_SECONDS,
                "sustainable_locations_at_catalog_cadence": (
                    self.profile.sustainable_locations_at_catalog_cadence
                ),
                "terms_review_due_at": TERMS_REVIEW_DUE_AT,
                "terms_review_required": bool(
                    state["terms_review_required"]
                    or now < TERMS_REVIEWED_AT
                    or now >= TERMS_REVIEW_DUE_AT
                ),
                "terms_reviewed_at": TERMS_REVIEWED_AT,
                "usage_units_per_location": USAGE_UNITS_PER_LOCATION,
                "window_usage": usage,
            }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise OpenMeteoUsageLimitExceeded("usage_diagnostics_too_large")
        return result

    def _request_sha(self, binding: _UsageBinding) -> str:
        return _digest({
            "acquisition_cadence_profile_sha256": (
                ACQUISITION_CADENCE_PROFILE_SHA256
            ),
            "authority_sha256": binding.authority_sha256,
            "catalog_cadence_seconds": OPENMETEO_CATALOG_CADENCE_SECONDS,
            "charged_units": binding.charged_units,
            "contract": RESERVATION_SCHEMA,
            "job_identity": binding.job_identity,
            "native_request_sha256": binding.native_request_sha256,
            "policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
            "pricing_policy_id": OPENMETEO_PRICING_POLICY_ID,
            "profile_sha256": self.profile_sha256,
            "query_batch_sha256": binding.query_batch_sha256,
            "query_count": binding.query_count,
            "variable_count": binding.variable_count,
        })

    def _reservation_sha(
        self,
        *,
        reservation_id: str,
        job_identity: str,
        request_sha256: str,
        binding: _UsageBinding,
        created_at: int,
        expires_at: int,
        windows: dict[str, tuple[int, int]],
    ) -> str:
        return _digest({
            "acquisition_cadence_profile_sha256": (
                ACQUISITION_CADENCE_PROFILE_SHA256
            ),
            "authorization_expires_at": expires_at,
            "authority_sha256": binding.authority_sha256,
            "charged_units": binding.charged_units,
            "catalog_cadence_seconds": OPENMETEO_CATALOG_CADENCE_SECONDS,
            "contract": RESERVATION_SCHEMA,
            "created_at": created_at,
            "job_identity": job_identity,
            "native_request_sha256": binding.native_request_sha256,
            "policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
            "pricing_policy_id": OPENMETEO_PRICING_POLICY_ID,
            "profile_sha256": self.profile_sha256,
            "query_batch_sha256": binding.query_batch_sha256,
            "query_count": binding.query_count,
            "request_sha256": request_sha256,
            "reservation_id": reservation_id,
            "variable_count": binding.variable_count,
            "windows": {
                kind: {"end": windows[kind][1], "start": windows[kind][0]}
                for kind in WINDOW_KINDS
            },
        })

    @staticmethod
    def _reservation_state_sha(fields: dict[str, object]) -> str:
        return _digest({"contract": RESERVATION_SCHEMA, **fields})

    def _initial_state_sha(self, receipt: OpenMeteoUsageReservation) -> str:
        return self._reservation_state_sha({
            "capture_receipt_sha256": None,
            "fetch_permit_sha256": None,
            "fetch_started_at": None,
            "reservation_sha256": receipt.reservation_sha256,
            "status": "RESERVED",
            "terminal_at": None,
            "terminal_previous_state_sha256": None,
            "terminal_reason_code": None,
            "terminal_sha256": None,
        })

    @staticmethod
    def _permit_sha(
        receipt: OpenMeteoUsageReservation, *, started_at: int
    ) -> str:
        return _digest({
            "acquisition_cadence_profile_sha256": (
                ACQUISITION_CADENCE_PROFILE_SHA256
            ),
            "catalog_cadence_seconds": OPENMETEO_CATALOG_CADENCE_SECONDS,
            "contract": PERMIT_SCHEMA,
            "authority_sha256": receipt.authority_sha256,
            "charged_units": receipt.cost_units,
            "expires_at": receipt.authorization_expires_at,
            "native_request_sha256": receipt.native_request_sha256,
            "policy_id": receipt.policy_id,
            "pricing_policy_id": receipt.pricing_policy_id,
            "profile_sha256": receipt.profile_sha256,
            "query_batch_sha256": receipt.query_batch_sha256,
            "query_count": receipt.query_count,
            "reservation_id": receipt.reservation_id,
            "reservation_sha256": receipt.reservation_sha256,
            "started_at": started_at,
            "variable_count": receipt.variable_count,
        })

    def _permit(
        self, receipt: OpenMeteoUsageReservation, *, newly_authorized: bool
    ) -> OpenMeteoFetchPermit:
        if receipt.fetch_started_at is None or receipt.fetch_permit_sha256 is None:
            raise OpenMeteoUsageIntegrityError("fetch_permit_is_missing")
        expected = self._permit_sha(receipt, started_at=receipt.fetch_started_at)
        if expected != receipt.fetch_permit_sha256:
            raise OpenMeteoUsageIntegrityError("fetch_permit_hash_mismatch")
        return OpenMeteoFetchPermit(
            reservation_id=receipt.reservation_id,
            reservation_sha256=receipt.reservation_sha256,
            policy_id=receipt.policy_id,
            pricing_policy_id=receipt.pricing_policy_id,
            profile_sha256=receipt.profile_sha256,
            authority_sha256=receipt.authority_sha256,
            query_batch_sha256=receipt.query_batch_sha256,
            native_request_sha256=receipt.native_request_sha256,
            query_count=receipt.query_count,
            variable_count=receipt.variable_count,
            charged_units=receipt.cost_units,
            started_at=receipt.fetch_started_at,
            expires_at=receipt.authorization_expires_at,
            permit_sha256=expected,
            new_fetch_authorized=newly_authorized,
        )

    @staticmethod
    def _terminal_sha(
        receipt: OpenMeteoUsageReservation,
        *,
        previous_state_sha256: str,
        status: str,
        terminal_at: int,
        capture_receipt_sha256: str | None,
        reason_code: str | None,
    ) -> str:
        return _digest({
            "capture_receipt_sha256": capture_receipt_sha256,
            "contract": TERMINAL_SCHEMA,
            "previous_state_sha256": previous_state_sha256,
            "reason_code": reason_code,
            "reservation_sha256": receipt.reservation_sha256,
            "status": status,
            "terminal_at": terminal_at,
        })

    def _reconcile_pruned_windows(
        self,
        database: sqlite3.Connection,
        receipts: list[OpenMeteoUsageReservation],
    ) -> None:
        """Rehash only expired windows affected by the bounded prune batch."""

        for kind in WINDOW_KINDS:
            column = f"{kind}_start"
            starts = sorted({int(getattr(item, column)) for item in receipts})
            for start in starts:
                row = database.execute(
                    "SELECT * FROM window_usage WHERE window_kind=? "
                    "AND window_start=?",
                    (kind, start),
                ).fetchone()
                if row is None:
                    raise OpenMeteoUsageIntegrityError(
                        "pruned_window_row_missing"
                    )
                self._validated_window(row)
                end = int(row["window_end"])
                aggregate = database.execute(
                    f"SELECT COALESCE(SUM(cost_units),0),COUNT(*) "
                    f"FROM reservations WHERE {column}=?",
                    (start,),
                ).fetchone()
                if aggregate is None:
                    raise OpenMeteoUsageIntegrityError(
                        "pruned_window_aggregate_missing"
                    )
                charged = int(aggregate[0])
                count = int(aggregate[1])
                if count == 0:
                    database.execute(
                        "DELETE FROM window_usage WHERE window_kind=? "
                        "AND window_start=?",
                        (kind, start),
                    )
                    continue
                if charged <= 0:
                    raise OpenMeteoUsageIntegrityError(
                        "pruned_window_aggregate_invalid"
                    )
                database.execute(
                    "UPDATE window_usage SET charged_units=?,"
                    "reservation_count=?,row_sha256=? WHERE window_kind=? "
                    "AND window_start=?",
                    (
                        charged,
                        count,
                        self._window_sha(kind, start, end, charged, count),
                        kind,
                        start,
                    ),
                )

    def _charge_window(
        self,
        database: sqlite3.Connection,
        kind: str,
        bounds: tuple[int, int],
        *,
        charged_units: int,
    ) -> None:
        start, end = bounds
        row = database.execute(
            "SELECT * FROM window_usage WHERE window_kind=? AND window_start=?",
            (kind, start),
        ).fetchone()
        if row is None:
            charged = charged_units
            count = 1
            if charged > self.profile.limits[kind]:
                raise OpenMeteoUsageLimitExceeded(f"{kind}_usage_limit")
            row_sha = self._window_sha(kind, start, end, charged, count)
            database.execute(
                "INSERT INTO window_usage(window_kind,window_start,window_end,"
                "charged_units,reservation_count,row_sha256) VALUES(?,?,?,?,?,?)",
                (kind, start, end, charged, count, row_sha),
            )
            return
        old_charged, old_count = self._validated_window(row)
        if int(row["window_end"]) != end:
            raise OpenMeteoUsageIntegrityError("usage_window_end_mismatch")
        charged = old_charged + charged_units
        count = old_count + 1
        if charged > self.profile.limits[kind]:
            raise OpenMeteoUsageLimitExceeded(f"{kind}_usage_limit")
        database.execute(
            "UPDATE window_usage SET charged_units=?,reservation_count=?,"
            "row_sha256=? WHERE window_kind=? AND window_start=?",
            (
                charged,
                count,
                self._window_sha(kind, start, end, charged, count),
                kind,
                start,
            ),
        )

    def _window_sha(
        self, kind: str, start: int, end: int, charged: int, count: int
    ) -> str:
        return _digest({
            "charged_units": charged,
            "contract": CONTRACT,
            "policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
            "profile_sha256": self.profile_sha256,
            "reservation_count": count,
            "window_end": end,
            "window_kind": kind,
            "window_start": start,
        })

    def _validated_window(self, row: sqlite3.Row) -> tuple[int, int]:
        kind = str(row["window_kind"])
        start = int(row["window_start"])
        end = int(row["window_end"])
        charged = int(row["charged_units"])
        count = int(row["reservation_count"])
        if (
            kind not in WINDOW_KINDS
            or start < 0
            or end <= start
            or charged < count * USAGE_UNITS_PER_LOCATION
            or charged > self.profile.limits[kind]
            or row["row_sha256"]
            != self._window_sha(kind, start, end, charged, count)
        ):
            raise OpenMeteoUsageIntegrityError("usage_window_row_invalid")
        return charged, count

    def _validated_reservation(
        self, row: sqlite3.Row
    ) -> OpenMeteoUsageReservation:
        try:
            receipt = OpenMeteoUsageReservation(
                reservation_id=_reservation_id(row["reservation_id"]),
                job_identity=_job_identity(row["job_identity"]),
                policy_id=str(row["policy_id"]),
                profile_sha256=_sha256(
                    row["profile_sha256"], "profile_sha256_invalid"
                ),
                request_sha256=_sha256(
                    row["request_sha256"], "request_sha256_invalid"
                ),
                reservation_sha256=_sha256(
                    row["reservation_sha256"], "reservation_sha256_invalid"
                ),
                authority_sha256=_sha256(
                    row["authority_sha256"], "authority_sha256_invalid"
                ),
                query_batch_sha256=_sha256(
                    row["query_batch_sha256"], "query_batch_sha256_invalid"
                ),
                native_request_sha256=_sha256(
                    row["native_request_sha256"],
                    "native_request_sha256_invalid",
                ),
                pricing_policy_id=str(row["pricing_policy_id"]),
                status=str(row["status"]),
                created_at=int(row["created_at"]),
                authorization_expires_at=int(row["authorization_expires_at"]),
                minute_start=int(row["minute_start"]),
                hour_start=int(row["hour_start"]),
                day_start=int(row["day_start"]),
                month_start=int(row["month_start"]),
                query_count=int(row["query_count"]),
                variable_count=int(row["variable_count"]),
                cost_units=int(row["cost_units"]),
                fetch_started_at=(
                    None if row["fetch_started_at"] is None
                    else int(row["fetch_started_at"])
                ),
                fetch_permit_sha256=(
                    None if row["fetch_permit_sha256"] is None
                    else _sha256(
                        row["fetch_permit_sha256"],
                        "fetch_permit_sha256_invalid",
                    )
                ),
                state_sha256=_sha256(
                    row["state_sha256"], "state_sha256_invalid"
                ),
                terminal_at=(
                    None if row["terminal_at"] is None
                    else int(row["terminal_at"])
                ),
                terminal_reason_code=(
                    None if row["terminal_reason_code"] is None
                    else str(row["terminal_reason_code"])
                ),
                capture_receipt_sha256=(
                    None if row["capture_receipt_sha256"] is None
                    else _sha256(
                        row["capture_receipt_sha256"],
                        "capture_receipt_sha256_invalid",
                    )
                ),
                terminal_sha256=(
                    None if row["terminal_sha256"] is None
                    else _sha256(
                        row["terminal_sha256"], "terminal_sha256_invalid"
                    )
                ),
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise OpenMeteoUsageIntegrityError(
                "usage_reservation_row_invalid"
            ) from exc
        if (
            receipt.policy_id != OPENMETEO_ATTRIBUTION_POLICY_ID
            or receipt.pricing_policy_id != OPENMETEO_PRICING_POLICY_ID
            or receipt.profile_sha256 != self.profile_sha256
            or receipt.variable_count != OPENMETEO_VARIABLE_COUNT
            or not 1 <= receipt.query_count <= 64
            or receipt.cost_units
            != USAGE_UNITS_PER_LOCATION * receipt.query_count
            or receipt.reservation_id != "omur:" + receipt.request_sha256
        ):
            raise OpenMeteoUsageIntegrityError("usage_reservation_identity_drift")
        binding = _UsageBinding(
            job_identity=receipt.job_identity,
            authority_sha256=receipt.authority_sha256,
            query_batch_sha256=receipt.query_batch_sha256,
            native_request_sha256=receipt.native_request_sha256,
            query_count=receipt.query_count,
            variable_count=receipt.variable_count,
            charged_units=receipt.cost_units,
        )
        if receipt.request_sha256 != self._request_sha(binding):
            raise OpenMeteoUsageIntegrityError("usage_request_hash_mismatch")
        windows = {
            "minute": (receipt.minute_start, int(row["minute_end"])),
            "hour": (receipt.hour_start, int(row["hour_end"])),
            "day": (receipt.day_start, int(row["day_end"])),
            "month": (receipt.month_start, int(row["month_end"])),
        }
        if windows != _window_bounds(receipt.created_at):
            raise OpenMeteoUsageIntegrityError("usage_reservation_window_drift")
        if receipt.authorization_expires_at != min(
            receipt.created_at + FETCH_AUTHORIZATION_SECONDS,
            TERMS_REVIEW_DUE_AT,
        ):
            raise OpenMeteoUsageIntegrityError(
                "usage_authorization_expiry_drift"
            )
        expected_reservation = self._reservation_sha(
            reservation_id=receipt.reservation_id,
            job_identity=receipt.job_identity,
            request_sha256=receipt.request_sha256,
            binding=binding,
            created_at=receipt.created_at,
            expires_at=receipt.authorization_expires_at,
            windows=windows,
        )
        if expected_reservation != receipt.reservation_sha256:
            raise OpenMeteoUsageIntegrityError("reservation_hash_mismatch")
        previous = row["terminal_previous_state_sha256"]
        if previous is not None:
            previous = _sha256(previous, "terminal_previous_state_invalid")
        if receipt.status == "RESERVED":
            terminal_sha = None
        else:
            if previous is None or receipt.terminal_at is None:
                raise OpenMeteoUsageIntegrityError("terminal_fields_missing")
            terminal_sha = self._terminal_sha(
                receipt,
                previous_state_sha256=previous,
                status=receipt.status,
                terminal_at=receipt.terminal_at,
                capture_receipt_sha256=receipt.capture_receipt_sha256,
                reason_code=receipt.terminal_reason_code,
            )
            if terminal_sha != receipt.terminal_sha256:
                raise OpenMeteoUsageIntegrityError("terminal_hash_mismatch")
        expected_state = self._reservation_state_sha({
            "capture_receipt_sha256": receipt.capture_receipt_sha256,
            "fetch_permit_sha256": receipt.fetch_permit_sha256,
            "fetch_started_at": receipt.fetch_started_at,
            "reservation_sha256": receipt.reservation_sha256,
            "status": receipt.status,
            "terminal_at": receipt.terminal_at,
            "terminal_previous_state_sha256": previous,
            "terminal_reason_code": receipt.terminal_reason_code,
            "terminal_sha256": terminal_sha,
        })
        if expected_state != receipt.state_sha256:
            raise OpenMeteoUsageIntegrityError("reservation_state_hash_mismatch")
        if receipt.fetch_started_at is not None:
            self._permit(receipt, newly_authorized=False)
        if (
            receipt.status == "FAILED"
            and receipt.terminal_reason_code not in FAILURE_REASON_CODES
        ):
            raise OpenMeteoUsageIntegrityError("failure_reason_drift")
        return receipt

    def _state_sha(self, state: dict[str, Any]) -> str:
        return _digest({
            "clock_rollback_terminalizations": state[
                "clock_rollback_terminalizations"
            ],
            "committed_count": state["committed_count"],
            "contract": CONTRACT,
            "failed_count": state["failed_count"],
            "last_observed_at": state["last_observed_at"],
            "policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
            "profile_sha256": self.profile_sha256,
            "prune_chain_sha256": state["prune_chain_sha256"],
            "pruned_count": state["pruned_count"],
            "reservation_rows": state["reservation_rows"],
            "reserved_count": state["reserved_count"],
            "revision": state["revision"],
            "terms_review_required": state["terms_review_required"],
        })

    def _state(self, database: sqlite3.Connection) -> dict[str, Any]:
        row = database.execute(
            "SELECT * FROM ledger_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise OpenMeteoUsageIntegrityError("usage_ledger_state_missing")
        try:
            state = {
                "revision": int(row["revision"]),
                "last_observed_at": int(row["last_observed_at"]),
                "terms_review_required": int(row["terms_review_required"]),
                "reservation_rows": int(row["reservation_rows"]),
                "reserved_count": int(row["reserved_count"]),
                "committed_count": int(row["committed_count"]),
                "failed_count": int(row["failed_count"]),
                "pruned_count": int(row["pruned_count"]),
                "clock_rollback_terminalizations": int(
                    row["clock_rollback_terminalizations"]
                ),
                "prune_chain_sha256": _sha256(
                    row["prune_chain_sha256"], "prune_chain_sha256_invalid"
                ),
            }
        except (TypeError, ValueError, OverflowError) as exc:
            raise OpenMeteoUsageIntegrityError(
                "usage_ledger_state_invalid"
            ) from exc
        if any(
            not isinstance(value, int) or value < 0
            for key, value in state.items()
            if key not in {"prune_chain_sha256"}
        ):
            raise OpenMeteoUsageIntegrityError("usage_ledger_state_invalid")
        if state["terms_review_required"] not in {0, 1}:
            raise OpenMeteoUsageIntegrityError("usage_ledger_state_invalid")
        if (
            state["reservation_rows"]
            != state["reserved_count"]
            + state["committed_count"] + state["failed_count"]
            or row["state_sha256"] != self._state_sha(state)
        ):
            raise OpenMeteoUsageIntegrityError("usage_ledger_state_hash_mismatch")
        return state

    def _write_state(
        self, database: sqlite3.Connection, state: dict[str, Any]
    ) -> None:
        state_sha = self._state_sha(state)
        database.execute(
            "UPDATE ledger_state SET revision=?,last_observed_at=?,"
            "terms_review_required=?,"
            "reservation_rows=?,reserved_count=?,committed_count=?,"
            "failed_count=?,pruned_count=?,"
            "clock_rollback_terminalizations=?,prune_chain_sha256=?,"
            "state_sha256=? WHERE singleton=1",
            (
                state["revision"],
                state["last_observed_at"],
                state["terms_review_required"],
                state["reservation_rows"],
                state["reserved_count"],
                state["committed_count"],
                state["failed_count"],
                state["pruned_count"],
                state["clock_rollback_terminalizations"],
                state["prune_chain_sha256"],
                state_sha,
            ),
        )

    def _advance_state_clock(
        self,
        database: sqlite3.Connection,
        state: dict[str, Any],
        now: int,
    ) -> None:
        if now <= state["last_observed_at"]:
            return
        updated = dict(state)
        updated["revision"] += 1
        updated["last_observed_at"] = now
        self._write_state(database, updated)

    def _require_fresh_fetch_clock(
        self,
        database: sqlite3.Connection,
        state: dict[str, Any],
        now: int,
    ) -> None:
        if state["terms_review_required"]:
            raise OpenMeteoUsageTermsExpired("terms_review_required")
        if now < state["last_observed_at"]:
            raise OpenMeteoUsageClockRollback("usage_clock_rollback")
        if now < TERMS_REVIEWED_AT:
            raise OpenMeteoUsageClockRollback(
                "usage_clock_before_terms_review"
            )
        if now >= TERMS_REVIEW_DUE_AT:
            updated = dict(state)
            updated["revision"] += 1
            updated["last_observed_at"] = now
            updated["terms_review_required"] = 1
            self._write_state(database, updated)
            self._audit_database(database, full_integrity=False)
            self._fault("terms_review.before_commit")
            database.commit()
            self._fault("terms_review.after_commit")
            self._durability_barrier()
            raise OpenMeteoUsageTermsExpired("terms_review_required")

    def _audit_database(
        self, database: sqlite3.Connection, *, full_integrity: bool
    ) -> dict[str, int | str]:
        state = self._state(database)
        rows = database.execute(
            "SELECT * FROM reservations ORDER BY reservation_id"
        ).fetchall()
        if len(rows) > self.profile.max_reservations:
            raise OpenMeteoUsageLimitExceeded("usage_reservation_row_limit")
        receipts = [self._validated_reservation(row) for row in rows]
        counts = {
            "RESERVED": sum(item.status == "RESERVED" for item in receipts),
            "COMMITTED": sum(item.status == "COMMITTED" for item in receipts),
            "FAILED": sum(item.status == "FAILED" for item in receipts),
        }
        if (
            len(receipts) != state["reservation_rows"]
            or counts["RESERVED"] != state["reserved_count"]
            or counts["COMMITTED"] != state["committed_count"]
            or counts["FAILED"] != state["failed_count"]
        ):
            raise OpenMeteoUsageIntegrityError("usage_counts_do_not_sum")
        expected_windows: dict[tuple[str, int], tuple[int, int, int]] = {}
        for row in rows:
            for kind in WINDOW_KINDS:
                key = (kind, int(row[f"{kind}_start"]))
                end = int(row[f"{kind}_end"])
                old = expected_windows.get(key, (end, 0, 0))
                if old[0] != end:
                    raise OpenMeteoUsageIntegrityError("usage_window_end_drift")
                expected_windows[key] = (
                    end,
                    old[1] + int(row["cost_units"]),
                    old[2] + 1,
                )
        actual_windows: dict[tuple[str, int], tuple[int, int, int]] = {}
        for row in database.execute(
            "SELECT * FROM window_usage ORDER BY window_kind,window_start"
        ):
            charged, count = self._validated_window(row)
            key = (str(row["window_kind"]), int(row["window_start"]))
            actual_windows[key] = (int(row["window_end"]), charged, count)
        if actual_windows != expected_windows:
            raise OpenMeteoUsageIntegrityError("usage_windows_do_not_sum")
        if full_integrity:
            quick = database.execute("PRAGMA quick_check(1)").fetchone()
            if quick is None or str(quick[0]).lower() != "ok":
                raise OpenMeteoUsageIntegrityError("usage_sqlite_quick_check_failed")
        return {
            "committed_count": counts["COMMITTED"],
            "failed_count": counts["FAILED"],
            "prune_chain_sha256": state["prune_chain_sha256"],
            "pruned_count": state["pruned_count"],
            "reservation_rows": len(receipts),
            "reserved_count": counts["RESERVED"],
            "window_rows": len(actual_windows),
        }

    def _load_from_database(
        self, database: sqlite3.Connection, reservation_id: str
    ) -> OpenMeteoUsageReservation:
        row = database.execute(
            "SELECT * FROM reservations WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        if row is None:
            raise OpenMeteoUsageIntegrityError("reservation_disappeared")
        return self._validated_reservation(row)

    def _now(self) -> int:
        try:
            value = self._clock()
        except Exception:
            raise OpenMeteoUsageClockRollback("usage_clock_unavailable") from None
        return _timestamp(value, "usage_clock_invalid")

    def _manifest(self) -> dict[str, str]:
        return {
            "admission_status": ADMISSION_STATUS,
            "contract": CONTRACT,
            "journal_mode": JOURNAL_MODE,
            "policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
            "policy_sha256": OPENMETEO_ATTRIBUTION_POLICY_SHA256,
            "pricing_policy_id": OPENMETEO_PRICING_POLICY_ID,
            "pricing_policy_sha256": OPENMETEO_PRICING_POLICY_SHA256,
            "profile_json": self.profile_bytes.decode("ascii"),
            "profile_sha256": self.profile_sha256,
            "schema_version": str(SCHEMA_VERSION),
            "sqlite_schema_sha256": _EXPECTED_SCHEMA_SHA256,
            "terms_review_due_at": str(TERMS_REVIEW_DUE_AT),
            "terms_reviewed_at": str(TERMS_REVIEWED_AT),
        }

    def _initialize_database(self) -> None:
        with self._database_lock:
            self._prepare_database_path()
            try:
                with closing(self._connect_unconfigured()) as database:
                    self._configure(database)
                    database.execute("BEGIN IMMEDIATE")
                    try:
                        if not self._schema_objects(database):
                            for _name, sql in _TABLE_DDL:
                                database.execute(sql)
                            for _name, sql in _INDEX_DDL:
                                database.execute(sql)
                            for _name, sql, _table in _TRIGGER_DDL:
                                database.execute(sql)
                            database.executemany(
                                "INSERT INTO manifest(key,value) VALUES(?,?)",
                                sorted(self._manifest().items()),
                            )
                            initial = {
                                "clock_rollback_terminalizations": 0,
                                "committed_count": 0,
                                "failed_count": 0,
                                "last_observed_at": 0,
                                "terms_review_required": 0,
                                "prune_chain_sha256": _PRUNE_GENESIS_SHA256,
                                "pruned_count": 0,
                                "reservation_rows": 0,
                                "reserved_count": 0,
                                "revision": 0,
                            }
                            database.execute(
                                "INSERT INTO ledger_state(singleton,revision,"
                                "last_observed_at,terms_review_required,"
                                "reservation_rows,reserved_count,"
                                "committed_count,failed_count,pruned_count,"
                                "clock_rollback_terminalizations,"
                                "prune_chain_sha256,state_sha256) "
                                "VALUES(1,?,?,?,?,?,?,?,?,?,?,?)",
                                (
                                    initial["revision"],
                                    initial["last_observed_at"],
                                    initial["terms_review_required"],
                                    initial["reservation_rows"],
                                    initial["reserved_count"],
                                    initial["committed_count"],
                                    initial["failed_count"],
                                    initial["pruned_count"],
                                    initial["clock_rollback_terminalizations"],
                                    initial["prune_chain_sha256"],
                                    self._state_sha(initial),
                                ),
                            )
                        self._validate_schema(database, full_integrity=True)
                        self._audit_database(database, full_integrity=True)
                        self._fault("initialize.before_commit")
                        database.commit()
                        self._fault("initialize.after_commit")
                    except BaseException:
                        if database.in_transaction:
                            database.rollback()
                        raise
                self._durability_barrier()
            except sqlite3.DatabaseError as exc:
                self._raise_sqlite(exc)

    def _validate_schema(
        self, database: sqlite3.Connection, *, full_integrity: bool = False
    ) -> None:
        if self._schema_objects(database) != _EXPECTED_SCHEMA_OBJECTS:
            raise OpenMeteoUsageIntegrityError("usage_schema_fingerprint_invalid")
        manifest = dict(database.execute("SELECT key,value FROM manifest"))
        if manifest != self._manifest():
            raise OpenMeteoUsageIntegrityError("usage_manifest_invalid")
        self._state(database)
        if full_integrity:
            quick = database.execute("PRAGMA quick_check(1)").fetchone()
            if quick is None or str(quick[0]).lower() != "ok":
                raise OpenMeteoUsageIntegrityError("usage_sqlite_quick_check_failed")

    def _verify_database(self) -> None:
        # Windows byte-range ownership denies overlapping reads of the locked
        # byte through a second descriptor.  The held descriptor is already
        # the exact anchor witness; only reverify its path identity here.
        self._verify_paths(read_lock=False)
        with self._database_lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database, full_integrity=True)
            self._audit_database(database, full_integrity=True)

    def _schema_objects(
        self, database: sqlite3.Connection
    ) -> tuple[tuple[str, str, str, str], ...]:
        rows = database.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL"
        ).fetchall()
        return tuple(sorted(
            (
                str(row[0]), str(row[1]), str(row[2]),
                _normalize_sql(str(row[3])),
            )
            for row in rows
        ))

    def _prepare_database_path(self) -> None:
        if self.path.exists():
            metadata = self.path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise OpenMeteoUsageIntegrityError("usage_database_path_unsafe")
        self._check_catalog_envelope()

    def _connect_unconfigured(self) -> sqlite3.Connection:
        self._check_catalog_envelope()
        database = sqlite3.connect(
            self.path,
            timeout=float(self.profile.sqlite_busy_timeout_seconds),
            isolation_level=None,
        )
        database.row_factory = sqlite3.Row
        database.create_function(
            "usage_prune_authorized", 0,
            lambda: int(self._prune_authorized),
        )
        try:
            database.execute(
                f"PRAGMA busy_timeout={max(1, int(self.profile.sqlite_busy_timeout_seconds * 1000))}"
            )
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            page_limit = max(
                1,
                (
                    self.profile.max_catalog_bytes
                    * CATALOG_MAIN_FRACTION_NUMERATOR
                    // CATALOG_MAIN_FRACTION_DENOMINATOR
                )
                // page_size,
            )
            if int(database.execute("PRAGMA page_count").fetchone()[0]) > page_limit:
                raise OpenMeteoUsageLimitExceeded("usage_catalog_envelope")
            applied = int(
                database.execute(f"PRAGMA max_page_count={page_limit}").fetchone()[0]
            )
            if applied > page_limit:
                raise OpenMeteoUsageLimitExceeded("usage_catalog_page_limit")
            return database
        except BaseException:
            database.close()
            raise

    def _configure(self, database: sqlite3.Connection) -> None:
        database.execute("PRAGMA foreign_keys=ON")
        mode = str(
            database.execute(f"PRAGMA journal_mode={JOURNAL_MODE}").fetchone()[0]
        ).lower()
        if mode != JOURNAL_MODE:
            raise OpenMeteoUsageIntegrityError("usage_journal_mode_invalid")
        database.execute("PRAGMA synchronous=FULL")
        database.execute("PRAGMA secure_delete=ON")
        database.execute("PRAGMA temp_store=MEMORY")

    def _connect(self) -> sqlite3.Connection:
        self._verify_database_path()
        database = self._connect_unconfigured()
        try:
            self._configure(database)
            self._verify_database_path()
            return database
        except BaseException:
            database.close()
            raise

    def _connect_read_only(self) -> sqlite3.Connection:
        self._check_catalog_envelope()
        self._verify_database_path()
        try:
            database = sqlite3.connect(
                self.path.resolve().as_uri() + "?mode=ro",
                uri=True,
                timeout=float(self.profile.sqlite_busy_timeout_seconds),
                isolation_level=None,
            )
        except sqlite3.DatabaseError as exc:
            self._raise_sqlite(exc)
        database.row_factory = sqlite3.Row
        database.create_function("usage_prune_authorized", 0, lambda: 0)
        try:
            database.execute("PRAGMA query_only=ON")
            database.execute("PRAGMA foreign_keys=ON")
            database.execute(
                f"PRAGMA busy_timeout={max(1, int(self.profile.sqlite_busy_timeout_seconds * 1000))}"
            )
            self._verify_database_path()
            return database
        except BaseException:
            database.close()
            raise

    def _initialize_lock_anchor(self) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor: int | None = None
        try:
            descriptor = os.open(self.lock_path, flags, 0o600)
            written = 0
            while written < len(self._lock_anchor_bytes):
                count = os.write(descriptor, self._lock_anchor_bytes[written:])
                if count <= 0:
                    raise OpenMeteoUsageDurabilityError(
                        "usage_owner_anchor_write_failed"
                    )
                written += count
            os.fsync(descriptor)
        except FileExistsError:
            pass
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                raise OpenMeteoUsageLimitExceeded(
                    "usage_owner_anchor_disk_full"
                ) from None
            raise OpenMeteoUsageDurabilityError(
                "usage_owner_anchor_write_failed"
            ) from exc
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
        try:
            metadata = self.lock_path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise OpenMeteoUsageIntegrityError("usage_owner_anchor_unsafe")
            if self.lock_path.read_bytes() != self._lock_anchor_bytes:
                raise OpenMeteoUsageIntegrityError("usage_owner_anchor_invalid")
        except OpenMeteoUsageError:
            raise
        except PermissionError:
            # Windows denies a second descriptor while the one-byte owner
            # lock is held.  That is an ownership conflict, not evidence of
            # a corrupt anchor.
            raise OpenMeteoUsageOwnerBusy("usage_owner_busy") from None
        except OSError as exc:
            raise OpenMeteoUsageDurabilityError(
                "usage_owner_anchor_unreadable"
            ) from exc
        self._fsync_directory(self.lock_path.parent)

    @staticmethod
    def _witness(metadata: os.stat_result) -> tuple[int, int, int]:
        return (
            int(metadata.st_dev),
            int(metadata.st_ino),
            int(metadata.st_mode),
        )

    def _path_witness(self, path: Path) -> tuple[int, int, int]:
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise OpenMeteoUsageIntegrityError("usage_path_identity_invalid")
        return self._witness(metadata)

    def _verify_database_path(self) -> None:
        if self._path_witness(self.path) != self._database_witness:
            raise OpenMeteoUsageIntegrityError("usage_database_identity_changed")

    def _verify_paths(self, *, read_lock: bool = True) -> None:
        self._verify_database_path()
        if self._path_witness(self.lock_path) != self._lock_witness:
            raise OpenMeteoUsageIntegrityError("usage_owner_anchor_identity_changed")
        if read_lock and self.lock_path.read_bytes() != self._lock_anchor_bytes:
            raise OpenMeteoUsageIntegrityError("usage_owner_anchor_content_changed")

    @staticmethod
    def _read_descriptor(descriptor: int) -> bytes:
        os.lseek(descriptor, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        remaining = 4097
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        if os.name != "posix":
            return
        descriptor = os.open(
            path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _durability_barrier(self) -> None:
        try:
            self._check_catalog_envelope()
            descriptor = os.open(
                self.path,
                os.O_RDWR if os.name == "nt" else os.O_RDONLY,
            )
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._fsync_directory(self.path.parent)
            self._check_catalog_envelope()
        except OpenMeteoUsageError:
            raise
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                raise OpenMeteoUsageLimitExceeded(
                    "usage_durability_disk_full"
                ) from None
            raise OpenMeteoUsageDurabilityError(
                "usage_durability_barrier_failed"
            ) from exc

    @staticmethod
    def _regular_file_size(path: Path, label: str) -> int:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return 0
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise OpenMeteoUsageIntegrityError(f"usage_{label}_file_unsafe")
        return int(metadata.st_size)

    def _catalog_bytes(self) -> int:
        wal = Path(str(self.path) + "-wal")
        shm = Path(str(self.path) + "-shm")
        if wal.exists() or shm.exists():
            raise OpenMeteoUsageIntegrityError("usage_wal_sidecars_forbidden")
        return self._regular_file_size(
            self.path, "main"
        ) + self._regular_file_size(Path(str(self.path) + "-journal"), "journal")

    def _check_catalog_envelope(self) -> None:
        if self._catalog_bytes() > self.profile.max_catalog_bytes:
            raise OpenMeteoUsageLimitExceeded("usage_catalog_envelope")

    def _raise_sqlite(self, exc: sqlite3.DatabaseError) -> None:
        message = str(exc).lower()
        if "full" in message or "disk is full" in message:
            raise OpenMeteoUsageLimitExceeded("usage_sqlite_catalog_full") from exc
        if "locked" in message or "busy" in message:
            raise OpenMeteoUsageOwnerBusy("usage_sqlite_busy") from exc
        raise OpenMeteoUsageIntegrityError("usage_sqlite_failed_closed") from exc

    def _fault(self, point: str) -> None:
        if self._fault_injector is None:
            return
        try:
            self._fault_injector(point)
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                raise OpenMeteoUsageLimitExceeded(
                    "usage_fault_disk_full"
                ) from None
            raise OpenMeteoUsageDurabilityError("usage_fault_io_failed") from exc

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


@dataclass(slots=True, repr=False)
class OpenMeteoUsageLease:
    """Only capability that can reserve, permit, terminalize or prune usage."""

    _authority: OpenMeteoUsageAuthority
    _token: object

    def assert_held(self) -> None:
        self._authority._assert_owner(self._token)

    def reserve(
        self,
        *,
        authority: AcquisitionQueryAuthority,
        expected_authority_sha256: str,
        query_batch: Mapping[str, Any],
    ) -> OpenMeteoUsageReservation:
        return self._authority._reserve(
            self._token,
            authority=authority,
            expected_authority_sha256=expected_authority_sha256,
            query_batch=query_batch,
        )

    def begin_fetch(
        self,
        *,
        reservation_id: str,
        expected_state_sha256: str,
    ) -> OpenMeteoFetchPermit:
        return self._authority._begin_fetch(
            self._token,
            reservation_id=reservation_id,
            expected_state_sha256=expected_state_sha256,
        )

    def commit(
        self,
        *,
        reservation_id: str,
        expected_state_sha256: str,
        capture_receipt_sha256: str,
    ) -> OpenMeteoUsageReservation:
        return self._authority._terminalize(
            self._token,
            reservation_id=reservation_id,
            expected_state_sha256=expected_state_sha256,
            status="COMMITTED",
            capture_receipt_sha256=capture_receipt_sha256,
            reason_code=None,
        )

    def fail(
        self,
        *,
        reservation_id: str,
        expected_state_sha256: str,
        reason_code: str,
    ) -> OpenMeteoUsageReservation:
        return self._authority._terminalize(
            self._token,
            reservation_id=reservation_id,
            expected_state_sha256=expected_state_sha256,
            status="FAILED",
            capture_receipt_sha256=None,
            reason_code=reason_code,
        )

    def load(
        self, reservation_id: str
    ) -> OpenMeteoUsageReservation | None:
        return self._authority._load(self._token, reservation_id)

    def prune(self) -> dict[str, int | str]:
        return self._authority._prune(self._token)

    def audit(self) -> dict[str, int | str]:
        return self._authority._audit(self._token)

    def diagnostics(self) -> dict[str, Any]:
        return self._authority._diagnostics(self._token)

    def release(self) -> None:
        self._authority._release(self._token)

    close = release

    def __enter__(self) -> "OpenMeteoUsageLease":
        self.assert_held()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()

    def __del__(self) -> None:
        try:
            self.release()
        except BaseException:
            pass


def _validate_native_policy() -> None:
    try:
        policy = json.loads(OPENMETEO_ATTRIBUTION_POLICY_CANONICAL)
        pricing = json.loads(OPENMETEO_PRICING_POLICY_CANONICAL)
        catalog = acquisition_catalog_snapshot()
        openmeteo_workloads = [
            item
            for item in catalog["workloads"]
            if item.get("workload_id") == "model.openmeteo.region"
        ]
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Open-Meteo policy canonical bytes are invalid") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Open-Meteo catalog binding is invalid") from exc
    if (
        hashlib.sha256(OPENMETEO_ATTRIBUTION_POLICY_CANONICAL).hexdigest()
        != OPENMETEO_ATTRIBUTION_POLICY_SHA256
        or OPENMETEO_ATTRIBUTION_POLICY_ID
        != "oma1:" + OPENMETEO_ATTRIBUTION_POLICY_SHA256
        or policy.get("published_call_limits")
        != {
            "day": PUBLISHED_LIMITS["day"],
            "hour": PUBLISHED_LIMITS["hour"],
            "minute": PUBLISHED_LIMITS["minute"],
            "month": PUBLISHED_LIMITS["month"],
            "provider_metering_rules_apply": True,
        }
        or policy.get("terms_reviewed_on") != "2026-08-09"
        or _canonical(pricing) != OPENMETEO_PRICING_POLICY_CANONICAL
        or hashlib.sha256(OPENMETEO_PRICING_POLICY_CANONICAL).hexdigest()
        != OPENMETEO_PRICING_POLICY_SHA256
        or OPENMETEO_PRICING_POLICY_ID
        != "omp1:" + OPENMETEO_PRICING_POLICY_SHA256
        or pricing.get("terms_policy_id")
        != OPENMETEO_ATTRIBUTION_POLICY_ID
        or pricing.get("variable_count") != OPENMETEO_VARIABLE_COUNT
        or pricing.get("cost_formula")
        != "ceil(variable_count/10)*query_count"
        or USAGE_UNITS_PER_LOCATION != 2
        or OPENMETEO_CATALOG_CADENCE_SECONDS != 900
        or OPENMETEO_CATALOG_FETCH_SLOTS_PER_UTC_DAY != 96
        or TERMS_POLICY_DAYS != 30
        or MAXIMUM_ROWS_BEFORE_TERMS_DUE != 15_000
        or MINIMUM_MAIN_CATALOG_BYTES != 34_914_304
        or MINIMUM_PHYSICAL_ENVELOPE_BYTES != 46_552_406
        or DEFAULT_USAGE_PROFILE.max_catalog_bytes
        < MINIMUM_PHYSICAL_ENVELOPE_BYTES
        or len(openmeteo_workloads) != 1
        or openmeteo_workloads[0].get("cadence_seconds")
        != OPENMETEO_CATALOG_CADENCE_SECONDS
        or catalog.get("catalog_sha256") != ACQUISITION_CATALOG_SHA256
        or catalog.get("cadence_profile_id")
        != ACQUISITION_CADENCE_PROFILE_ID
        or catalog.get("cadence_profile_sha256")
        != ACQUISITION_CADENCE_PROFILE_SHA256
    ):
        raise RuntimeError("Open-Meteo policy binding drifted")


_validate_native_policy()


__all__ = [
    "ADMISSION_STATUS",
    "CONTRACT",
    "DEFAULT_INTERNAL_LIMITS",
    "DEFAULT_USAGE_PROFILE",
    "DIAGNOSTICS_CONTRACT",
    "FAILURE_REASON_CODES",
    "FETCH_AUTHORIZATION_SECONDS",
    "MAXIMUM_ROWS_BEFORE_TERMS_DUE",
    "MINIMUM_MAIN_CATALOG_BYTES",
    "MINIMUM_PHYSICAL_ENVELOPE_BYTES",
    "OPENMETEO_PRICING_POLICY_CANONICAL",
    "OPENMETEO_PRICING_POLICY_ID",
    "OPENMETEO_PRICING_POLICY_SHA256",
    "OPENMETEO_CATALOG_CADENCE_SECONDS",
    "OPENMETEO_CATALOG_FETCH_SLOTS_PER_UTC_DAY",
    "OPENMETEO_VARIABLE_COUNT",
    "OpenMeteoFetchPermit",
    "OpenMeteoUsageAuthority",
    "OpenMeteoUsageClockRollback",
    "OpenMeteoUsageConflict",
    "OpenMeteoUsageDurabilityError",
    "OpenMeteoUsageError",
    "OpenMeteoUsageIntegrityError",
    "OpenMeteoUsageLease",
    "OpenMeteoUsageLimitExceeded",
    "OpenMeteoUsageOwnerBusy",
    "OpenMeteoUsageOwnerLost",
    "OpenMeteoUsageProfile",
    "OpenMeteoUsageReservation",
    "OpenMeteoUsageTermsExpired",
    "PUBLISHED_LIMITS",
    "RETENTION_SECONDS",
    "TERMS_REVIEW_DUE_AT",
    "TERMS_REVIEWED_AT",
    "USAGE_UNITS_PER_LOCATION",
]
