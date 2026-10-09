"""Durable, serial and deliberately unwired terminal-completion dispatcher.

One lifetime OS/process owner scans the exact closed terminal raw catalogue in a
durable round robin.  A tick can invoke at most one exact adapter phase and
therefore at most one hard child.  NOAA and raw-manifest final reproofs are
continued on a later tick.  SQLite never stores a lifecycle lease token,
event ID, payload, pending Python object, path, or child diagnostic.
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import asdict, dataclass
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
from types import MappingProxyType
from typing import Any, Callable, Final, Literal, Mapping
from urllib.parse import quote

from .acquisition_evidence_lifecycle import (
    CONTRACT as LIFECYCLE_CONTRACT,
    SCHEMA_VERSION as LIFECYCLE_SCHEMA_VERSION,
    AcquisitionEvidenceLifecycleStore,
    CompletionLease,
    TerminalEventIdentity,
)
from .desired_state import (
    CONTRACT as DESIRED_STATE_CONTRACT,
    MAX_AUTHORITY_CLAIMS as DESIRED_MAX_AUTHORITY_CLAIMS,
    MAX_CONTRACT_BYTES as DESIRED_MAX_CONTRACT_BYTES,
    MAX_DATABASE_BYTES as DESIRED_MAX_DATABASE_BYTES,
    MAX_STATE_ROWS as DESIRED_MAX_STATE_ROWS,
    METHOD as DESIRED_STATE_METHOD,
    SCHEMA_VERSION as DESIRED_STATE_SCHEMA_VERSION,
    DesiredStateStore,
)
from .completion_route_adapters import (
    AdapterContinuation,
    AdapterPhaseResult,
    CompletionRouteAdapter,
    CompletionRouteAdapterIntegrityError,
    DEFAULT_ROUTE_LEASE_SECONDS,
    DwdCompletionRouteAdapter,
    HeavyChildPermit,
    MAX_ROUTES,
    NoaaCompletionRouteAdapter,
    PermanentEventFailure,
    ROUTE_CATALOG_SHA256,
    ROUTE_ORDER,
    RawManifestCompletionRouteAdapter,
    _SinglePhaseAdapter,
    is_retryable_exception,
    retryable_exception_reason,
    validate_completion_route_catalog,
)
from .terminal_raw_completion_router import TERMINAL_RAW_CONSUMER_CATALOG


CONTRACT: Final = "stv3-serial-terminal-completion-dispatcher-v1"
PROFILE_SCHEMA: Final = "stv3-serial-completion-dispatcher-profile-v1"
STATE_SCHEMA: Final = "stv3-serial-completion-dispatcher-state-v1"
RESULT_SCHEMA: Final = "stv3-serial-completion-dispatcher-tick-result-v1"
DIAGNOSTICS_SCHEMA: Final = "stv3-serial-completion-dispatcher-diagnostics-v1"
OWNER_LOCK_SCHEMA: Final = "stv3-serial-completion-dispatcher-owner-lock-v1"
GUARDED_CLOCK_SCHEMA: Final = "stv3-serial-completion-guarded-clock-v1"
COMPOSITION_AUTHORITY_SCHEMA: Final = "stv3-serial-completion-composition-authority-v1"
PREWIRING_BLOCKERS: Final = (
    "posix_completion_output_path_replace_os_policy_required",
)
SCHEMA_VERSION: Final = 2
JOURNAL_MODE: Final = "delete"
MAX_DATABASE_BYTES: Final = 8 * 1024 * 1024
MAX_DIAGNOSTIC_BYTES: Final = 4096
MAX_PATH_CHARS: Final = 4096
MAX_TIMESTAMP: Final = 2**53
SQLITE_BUSY_TIMEOUT_SECONDS: Final = 2.0
CLOSE_WAIT_SECONDS: Final = 5.0
MAX_HEAVY_CHILDREN_PER_TICK: Final = 1
MAX_IDLE_CLAIMS_PER_TICK: Final = MAX_ROUTES
LEGACY_ROUTE_LIMIT_V1: Final = 11
LEGACY_ROUTE_CATALOG_SHA256_V1: Final = (
    "78b6d12fa4642a05d4316698dcdef06e42a442fa28b6facdda803471dd10b4a5"
)
LEGACY_ROUTE_ORDER_V1: Final = (
    "ground.metar.global",
    "radar.aemet.composite",
    "radar.dpc.sri",
    "radar.dwd.radolan_rv",
    "radar.knmi.rtcor",
    "radar.meteofrance.dpradar",
    "radar.metoffice.uk_rate",
    "satellite.goes18.rrqpe",
    "satellite.goes19.rrqpe",
    "satellite.hsaf.h40b",
    "satellite.noaa.err",
)

_SHA = re.compile(r"^[0-9a-f]{64}$")
_SAFE_REASON = re.compile(r"^[a-z][a-z0-9_]{0,95}$")
_PENDING_PHASES = frozenset({"initial", "final_reproof"})

if (
    len(LEGACY_ROUTE_ORDER_V1) != LEGACY_ROUTE_LIMIT_V1
    or len(set(LEGACY_ROUTE_ORDER_V1)) != LEGACY_ROUTE_LIMIT_V1
    or not set(LEGACY_ROUTE_ORDER_V1).issubset(ROUTE_ORDER)
    or _SHA.fullmatch(LEGACY_ROUTE_CATALOG_SHA256_V1) is None
):  # pragma: no cover - fixed migration inventory
    raise RuntimeError("legacy completion route migration inventory drifted")


class SerialCompletionDispatcherError(RuntimeError):
    """Base detail-free dispatcher error."""


class SerialCompletionDispatcherIntegrityError(SerialCompletionDispatcherError):
    """The profile, state, schema, route, or owner witness differs."""


class SerialCompletionDispatcherDurabilityError(SerialCompletionDispatcherError):
    """SQLite or the owner anchor could not prove durable state."""


class SerialCompletionDispatcherOwnerBusy(SerialCompletionDispatcherError):
    """Another in-process or OS process owner already holds the dispatcher."""


class SerialCompletionDispatcherOwnerLost(SerialCompletionDispatcherError):
    """The lifetime owner fence changed or was released."""


class SerialCompletionDispatcherClockRollback(SerialCompletionDispatcherError):
    """Wall time moved below the process or durable high-water mark."""


def _classified_sqlite_error(
    error: sqlite3.DatabaseError, operation: str
) -> SerialCompletionDispatcherError:
    """Keep only explicit SQLite transient families on the retry path."""

    if is_retryable_exception(error):
        return SerialCompletionDispatcherDurabilityError(
            f"dispatcher {operation} unavailable"
        )
    return SerialCompletionDispatcherIntegrityError(
        f"dispatcher {operation} integrity failed"
    )


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise SerialCompletionDispatcherIntegrityError(
            "dispatcher value is not canonical JSON"
        ) from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _legacy_route_catalog_sha256_v1() -> str:
    raw_manifest = {
        "radar.aemet.composite",
        "radar.dpc.sri",
        "radar.meteofrance.dpradar",
        "satellite.goes18.rrqpe",
        "satellite.goes19.rrqpe",
        "satellite.hsaf.h40b",
    }
    adapter_kinds = {
        "ground.metar.global": "metar_single",
        "radar.dwd.radolan_rv": "dwd_verified_single",
        "radar.knmi.rtcor": "knmi_single",
        "radar.metoffice.uk_rate": "metoffice_single",
        "satellite.noaa.err": "noaa_split",
        **{item: "raw_manifest_split" for item in raw_manifest},
    }
    routes = []
    for workload_id in LEGACY_ROUTE_ORDER_V1:
        kind = adapter_kinds[workload_id]
        routes.append({
            "adapter_kind": kind,
            "binding": TERMINAL_RAW_CONSUMER_CATALOG[workload_id].as_dict(),
            "lease_seconds": DEFAULT_ROUTE_LEASE_SECONDS,
            "phase_contract": (
                "initial_then_final_reproof"
                if kind in {"noaa_split", "raw_manifest_split"}
                else "single"
            ),
            "workload_id": workload_id,
        })
    return _digest({
        "contract": "stv3-serial-completion-route-catalog-v1",
        "max_heavy_children_per_tick": 1,
        "max_idle_claims_per_tick": LEGACY_ROUTE_LIMIT_V1,
        "route_count": LEGACY_ROUTE_LIMIT_V1,
        "routes": routes,
        "runtime_wiring": "inert",
    })


if _legacy_route_catalog_sha256_v1() != LEGACY_ROUTE_CATALOG_SHA256_V1:
    raise RuntimeError("legacy completion route order/hash authority drifted")


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise SerialCompletionDispatcherIntegrityError(f"{label} is not SHA-256")
    return value


def _timestamp(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 <= float(value) <= MAX_TIMESTAMP
    ):
        raise SerialCompletionDispatcherIntegrityError(f"{label} is invalid")
    return float(value)


class GuardedCompletionClock:
    """One shared wall-clock high-water guard for dispatcher and consumers."""

    def __init__(self, source: Callable[[], float] = time.time) -> None:
        if not callable(source):
            raise TypeError("clock source must be callable")
        self._source = source
        self._lock = threading.Lock()
        self._high_water = 0.0

    def observe(self, *, durable_minimum: float | None = None) -> float:
        value = _timestamp(self._source(), "guarded clock")
        with self._lock:
            minimum = self._high_water
            if durable_minimum is not None:
                minimum = max(
                    minimum,
                    _timestamp(durable_minimum, "durable clock high-water"),
                )
            if value < minimum:
                raise SerialCompletionDispatcherClockRollback(
                    "completion_clock_rollback"
                )
            self._high_water = value
        return value

    def __call__(self) -> float:
        return self.observe()

    @property
    def high_water(self) -> float:
        with self._lock:
            return self._high_water


@dataclass(frozen=True, slots=True)
class SerialCompletionDispatcherProfile:
    max_database_bytes: int = MAX_DATABASE_BYTES
    sqlite_busy_timeout_seconds: float = SQLITE_BUSY_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        if self.max_database_bytes != MAX_DATABASE_BYTES:
            raise ValueError("dispatcher database cap is immutable")
        if float(self.sqlite_busy_timeout_seconds) != SQLITE_BUSY_TIMEOUT_SECONDS:
            raise ValueError("dispatcher SQLite timeout is immutable")

    def mapping(
        self,
        *,
        adapter_fingerprints: Mapping[str, str],
        composition_authority_sha256: str,
    ) -> dict[str, Any]:
        if tuple(adapter_fingerprints) != ROUTE_ORDER or any(
            _SHA.fullmatch(value) is None for value in adapter_fingerprints.values()
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "adapter fingerprints differ from route order"
            )
        return {
            "adapter_fingerprints": dict(adapter_fingerprints),
            "clock_contract": GUARDED_CLOCK_SCHEMA,
            "composition_authority_sha256": _sha(
                composition_authority_sha256,
                "composition authority SHA-256",
            ),
            "contract": PROFILE_SCHEMA,
            "journal_mode": JOURNAL_MODE,
            "max_database_bytes": self.max_database_bytes,
            "max_heavy_children_per_tick": MAX_HEAVY_CHILDREN_PER_TICK,
            "max_idle_claims_per_tick": MAX_IDLE_CLAIMS_PER_TICK,
            "route_catalog_sha256": ROUTE_CATALOG_SHA256,
            "route_order": list(ROUTE_ORDER),
            "prewiring_blockers": list(PREWIRING_BLOCKERS),
            "runtime_wiring": "inert",
            "sqlite_busy_timeout_seconds": float(
                self.sqlite_busy_timeout_seconds
            ),
        }


@dataclass(frozen=True, slots=True)
class SerialCompletionTickResult:
    """Capability-free tick receipt.

    ``child_invocations`` is a conservative upper bound on child-capable
    adapter calls.  Split adapters report the exact before-child callback;
    legacy single-phase adapters may report one when their internal preflight
    returned before spawning.  Actual heavy children are therefore never more
    than this value.
    """
    status: Literal[
        "idle",
        "busy",
        "stopped",
        "blocked",
        "continuation_pending",
        "retryable",
        "acknowledged",
        "rejected",
        "quarantined",
    ]
    reason_code: str
    dispatcher_profile_sha256: str
    dispatcher_state_sha256: str
    scanned_routes: int
    child_invocations: Literal[0, 1]
    workload_id: str | None = None
    event_sha256: str | None = None
    phase: Literal["none", "single", "initial", "final_reproof"] = "none"
    terminal_kind: Literal["acknowledged", "rejected", "quarantined"] | None = None
    output_sha256: str | None = None
    output_receipt_sha256: str | None = None
    process_receipt_sha256: str | None = None
    consumption_receipt_sha256: str | None = None
    quarantine_receipt_sha256: str | None = None

    def __post_init__(self) -> None:
        if (
            self.status not in {
                "idle", "busy", "stopped", "blocked",
                "continuation_pending", "retryable", "acknowledged",
                "rejected", "quarantined",
            }
            or
            _SAFE_REASON.fullmatch(self.reason_code) is None
            or _SHA.fullmatch(self.dispatcher_profile_sha256) is None
            or _SHA.fullmatch(self.dispatcher_state_sha256) is None
            or isinstance(self.scanned_routes, bool)
            or not 0 <= self.scanned_routes <= MAX_ROUTES
            or isinstance(self.child_invocations, bool)
            or self.child_invocations not in {0, 1}
            or self.phase not in {"none", "single", "initial", "final_reproof"}
            or (self.workload_id is not None and self.workload_id not in ROUTE_ORDER)
            or (self.event_sha256 is not None and _SHA.fullmatch(self.event_sha256) is None)
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher result identity differs"
            )
        hashes = (
            self.output_sha256,
            self.output_receipt_sha256,
            self.process_receipt_sha256,
            self.consumption_receipt_sha256,
            self.quarantine_receipt_sha256,
        )
        if any(value is not None and _SHA.fullmatch(value) is None for value in hashes):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher result receipt hash differs"
            )
        route_bound = self.workload_id is not None or self.event_sha256 is not None
        if (
            (self.workload_id is None) != (self.event_sha256 is None)
            or route_bound != (self.phase != "none")
            or self.status in {"idle", "busy"} and route_bound
            or self.status in {
                "continuation_pending", "acknowledged", "rejected", "quarantined"
            } and not route_bound
            or self.status == "continuation_pending" and self.phase != "initial"
            or self.status == "acknowledged" and self.phase == "initial"
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher result route/phase shape differs"
            )
        expected_terminal = {
            "acknowledged": "acknowledged",
            "rejected": "rejected",
            "quarantined": "quarantined",
        }.get(self.status)
        if self.terminal_kind != expected_terminal:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher terminal result shape differs"
            )
        if self.status == "acknowledged" and any(
            value is None for value in hashes[:4]
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher acknowledgement receipts are incomplete"
            )
        if self.status == "acknowledged" and self.quarantine_receipt_sha256 is not None:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher acknowledgement carries quarantine receipt"
            )
        if self.status == "rejected" and (
            self.consumption_receipt_sha256 is None
            or any(value is not None for value in hashes[:3])
            or self.quarantine_receipt_sha256 is not None
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher rejection receipt is incomplete"
            )
        if self.status == "quarantined" and (
            self.quarantine_receipt_sha256 is None
            or self.consumption_receipt_sha256 is None
            or any(value is not None for value in hashes[:3])
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher quarantine receipts are incomplete"
            )
        if self.status not in {"acknowledged", "rejected", "quarantined"} and any(
            value is not None for value in hashes
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher nonterminal result carries receipt hashes"
            )

    @property
    def result_sha256(self) -> str:
        return _digest(self.as_dict())

    def as_dict(self) -> dict[str, Any]:
        return {
            "child_invocations": self.child_invocations,
            "consumption_receipt_sha256": self.consumption_receipt_sha256,
            "contract": RESULT_SCHEMA,
            "dispatcher_profile_sha256": self.dispatcher_profile_sha256,
            "dispatcher_state_sha256": self.dispatcher_state_sha256,
            "event_sha256": self.event_sha256,
            "output_receipt_sha256": self.output_receipt_sha256,
            "output_sha256": self.output_sha256,
            "phase": self.phase,
            "process_receipt_sha256": self.process_receipt_sha256,
            "quarantine_receipt_sha256": self.quarantine_receipt_sha256,
            "reason_code": self.reason_code,
            "scanned_routes": self.scanned_routes,
            "status": self.status,
            "terminal_kind": self.terminal_kind,
            "workload_id": self.workload_id,
        }


def _normalize_sql(value: str) -> str:
    return " ".join(value.replace("\n", " ").split()).strip().casefold()


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
        "dispatcher_state",
        f"""
        CREATE TABLE dispatcher_state(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            revision INTEGER NOT NULL CHECK(revision>=0),
            last_observed_at REAL NOT NULL CHECK(last_observed_at>=0),
            next_route_index INTEGER NOT NULL CHECK(next_route_index>=0 AND next_route_index<{MAX_ROUTES}),
            owner_epoch TEXT NOT NULL,
            integrity_fenced INTEGER NOT NULL CHECK(integrity_fenced IN (0,1)),
            pending_workload_id TEXT,
            pending_event_sha256 TEXT,
            pending_stream_sequence INTEGER CHECK(
                pending_stream_sequence IS NULL OR pending_stream_sequence>=1
            ),
            pending_phase TEXT CHECK(pending_phase IS NULL OR pending_phase IN ('initial','final_reproof')),
            pending_lease_expires_at REAL,
            pending_owner_epoch TEXT,
            ticks INTEGER NOT NULL CHECK(ticks>=0),
            claims INTEGER NOT NULL CHECK(claims>=0),
            idle_scans INTEGER NOT NULL CHECK(idle_scans>=0),
            terminalizations INTEGER NOT NULL CHECK(terminalizations>=0),
            retryable_results INTEGER NOT NULL CHECK(retryable_results>=0),
            state_sha256 TEXT NOT NULL,
            CHECK(
                (pending_workload_id IS NULL AND pending_event_sha256 IS NULL AND
                 pending_stream_sequence IS NULL AND pending_phase IS NULL AND
                 pending_lease_expires_at IS NULL AND
                 pending_owner_epoch IS NULL)
                OR
                (pending_workload_id IS NOT NULL AND pending_event_sha256 IS NOT NULL AND
                 pending_stream_sequence IS NOT NULL AND pending_phase IS NOT NULL AND
                 pending_lease_expires_at IS NOT NULL AND
                 pending_owner_epoch IS NOT NULL)
            )
        )
        """,
    ),
)

_LEGACY_TABLE_DDL_V1 = tuple(
    (
        name,
        (
            sql.replace(
                f"next_route_index<{MAX_ROUTES}",
                f"next_route_index<{LEGACY_ROUTE_LIMIT_V1}",
            )
            if name == "dispatcher_state"
            else sql
        ),
    )
    for name, sql in _TABLE_DDL
)


def _expected_schema(
    table_ddl: tuple[tuple[str, str], ...] = _TABLE_DDL,
) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        sorted(("table", name, _normalize_sql(sql)) for name, sql in table_ddl)
    )


EXPECTED_SCHEMA: Final = _expected_schema()
EXPECTED_SCHEMA_SHA256: Final = _digest(EXPECTED_SCHEMA)
LEGACY_EXPECTED_SCHEMA_V1: Final = _expected_schema(_LEGACY_TABLE_DDL_V1)
LEGACY_EXPECTED_SCHEMA_SHA256_V1: Final = _digest(LEGACY_EXPECTED_SCHEMA_V1)


def _schema_inventory(
    database: sqlite3.Connection,
) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (
            str(row["type"]),
            str(row["name"]),
            _normalize_sql(str(row["sql"])),
        )
        for row in database.execute(
            "SELECT type,name,sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        )
    )

_PROCESS_OWNER_GUARD = threading.Lock()
_PROCESS_OWNED_PATHS: set[str] = set()


def _claim_process_path(path: Path) -> bool:
    key = os.path.normcase(str(path))
    with _PROCESS_OWNER_GUARD:
        if key in _PROCESS_OWNED_PATHS:
            return False
        _PROCESS_OWNED_PATHS.add(key)
        return True


def _release_process_path(path: Path) -> None:
    with _PROCESS_OWNER_GUARD:
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


class _CompositeCancel:
    __slots__ = ("_external", "_internal")

    def __init__(self, internal: threading.Event, external: object | None) -> None:
        if external is not None and not callable(getattr(external, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        self._internal = internal
        self._external = external

    def is_set(self) -> bool:
        return self._internal.is_set() or (
            self._external is not None and bool(self._external.is_set())
        )


class SerialCompletionDispatcher:
    """Lifetime-fenced one-owner round-robin dispatcher for exact adapters."""

    def __init__(
        self,
        path: str | Path,
        *,
        lifecycle_store: AcquisitionEvidenceLifecycleStore,
        adapters: Mapping[str, CompletionRouteAdapter],
        guarded_clock: GuardedCompletionClock,
        monotonic: Callable[[], float] = time.monotonic,
        profile: SerialCompletionDispatcherProfile = SerialCompletionDispatcherProfile(),
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        validate_completion_route_catalog()
        if not isinstance(lifecycle_store, AcquisitionEvidenceLifecycleStore):
            raise TypeError("lifecycle_store must be AcquisitionEvidenceLifecycleStore")
        if not isinstance(guarded_clock, GuardedCompletionClock):
            raise TypeError("guarded_clock must be GuardedCompletionClock")
        if not callable(monotonic):
            raise TypeError("monotonic must be callable")
        if not isinstance(profile, SerialCompletionDispatcherProfile):
            raise TypeError("profile must be SerialCompletionDispatcherProfile")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("fault_injector must be callable")
        supplied = Path(path)
        if (
            not supplied.is_absolute()
            or len(str(supplied)) > MAX_PATH_CHARS
            or supplied.name in {"", ".", ".."}
        ):
            raise ValueError("dispatcher database path must be absolute and bounded")
        # This authority never creates parent directories.  Every lexical
        # ancestor and both possible leaves are checked before the first open
        # or write, so a pre-existing link cannot be followed and mutated.
        resolved_parent = self._preflight_parent(supplied.parent)
        if os.path.normcase(str(resolved_parent)) != os.path.normcase(
            str(supplied.parent)
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher path parent is not canonical"
            )
        self.path = resolved_parent / supplied.name
        self.lock_path = resolved_parent / (supplied.name + ".owner.lock")
        if len(str(self.lock_path)) > MAX_PATH_CHARS:
            raise ValueError("dispatcher owner path exceeds bound")
        self._database_preflight_witness = self._preflight_leaf(self.path)
        self._lock_preflight_witness = self._preflight_leaf(self.lock_path)
        self.lifecycle_store = lifecycle_store
        self.guarded_clock = guarded_clock
        self._monotonic = monotonic
        self.profile = profile
        self._fault_injector = fault_injector
        self._tick_lock = threading.Lock()
        self._owner_lock = threading.RLock()
        self._stop = threading.Event()
        self._descriptor: int | None = None
        self._owner_token: object | None = None
        self._owner_epoch: str | None = None
        self._owner_id: str | None = None
        self._pending_memory: AdapterContinuation | None = None
        self.adapters = self._validate_adapters(adapters)
        self.composition_authority_sha256 = self._composition_authority()
        adapter_fingerprints = {
            workload_id: self.adapters[workload_id].fingerprint
            for workload_id in ROUTE_ORDER
        }
        self.profile_mapping = profile.mapping(
            adapter_fingerprints=adapter_fingerprints,
            composition_authority_sha256=self.composition_authority_sha256,
        )
        self.profile_sha256 = _digest(self.profile_mapping)
        self._lock_anchor = _canonical({
            "contract": OWNER_LOCK_SCHEMA,
            "dispatcher_profile_sha256": self.profile_sha256,
            "route_catalog_sha256": ROUTE_CATALOG_SHA256,
        })
        self._initialize_lock_anchor()
        self._lock_witness = self._path_witness(self.lock_path)
        self._initialize_database_leaf()
        self._database_witness = self._path_witness(self.path)
        self._initialize_database()
        self._composition_witnesses = self._capture_composition_witnesses()
        self._last_state_sha256 = "0" * 64

    def _validate_adapters(
        self, adapters: Mapping[str, CompletionRouteAdapter]
    ) -> Mapping[str, CompletionRouteAdapter]:
        if not isinstance(adapters, Mapping) or tuple(adapters) != ROUTE_ORDER:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher adapters differ from exact route order"
            )
        result: dict[str, CompletionRouteAdapter] = {}
        witnesses: dict[str, tuple[Any, ...]] = {}
        route_store_bindings: dict[str, tuple[dict[str, Any], ...]] = {}
        desired_store: DesiredStateStore | None = None
        for workload_id in ROUTE_ORDER:
            adapter = adapters[workload_id]
            if workload_id == "radar.dwd.radolan_rv":
                expected_type: type = DwdCompletionRouteAdapter
            elif workload_id == "satellite.noaa.err":
                expected_type = NoaaCompletionRouteAdapter
            elif TERMINAL_RAW_CONSUMER_CATALOG[workload_id].consumer_kind == "raw_manifest_audit":
                expected_type = RawManifestCompletionRouteAdapter
            else:
                expected_type = _SinglePhaseAdapter
            if type(adapter) is not expected_type:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher adapter type differs from fixed route"
                )
            if (
                adapter.workload_id != workload_id
                or adapter.consumer_id
                != TERMINAL_RAW_CONSUMER_CATALOG[workload_id].consumer_id
                or adapter.stream_id
                != TERMINAL_RAW_CONSUMER_CATALOG[workload_id].stream_id
                or float(adapter.lease_seconds) != DEFAULT_ROUTE_LEASE_SECONDS
                or _SHA.fullmatch(adapter.fingerprint) is None
                or getattr(adapter, "clock", None) is not self.guarded_clock
            ):
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher adapter binding or guarded clock differs"
                )
            bindings = self._validated_route_store_bindings(adapter)
            route_store_bindings[workload_id] = bindings
            consumer = getattr(adapter, "_consumer", None)
            adapter_lifecycle = getattr(adapter, "_lifecycle", None)
            if adapter_lifecycle is None:
                adapter_lifecycle = getattr(consumer, "lifecycle_store", None)
            if adapter_lifecycle is None:
                adapter_lifecycle = getattr(consumer, "lifecycle", None)
            if adapter_lifecycle is not self.lifecycle_store:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher adapter lifecycle store identity differs"
                )
            adapter_desired = getattr(adapter, "_desired", None)
            if adapter_desired is None:
                adapter_desired = getattr(consumer, "desired_state_store", None)
            if adapter_desired is None:
                adapter_desired = getattr(consumer, "desired", None)
            if not isinstance(adapter_desired, DesiredStateStore):
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher adapter desired store type differs"
                )
            if desired_store is None:
                desired_store = adapter_desired
            elif adapter_desired is not desired_store:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher adapters do not share one desired store"
                )
            result[workload_id] = adapter
            witnesses[workload_id] = self._adapter_witness(adapter)
        if desired_store is None:  # pragma: no cover - exact route set is nonempty
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher desired store is unavailable"
            )
        self.desired_state_store = desired_store
        self._route_store_bindings = MappingProxyType(route_store_bindings)
        self._adapter_witnesses = MappingProxyType(witnesses)
        return MappingProxyType(result)

    @staticmethod
    def _validated_route_store_bindings(
        adapter: CompletionRouteAdapter,
    ) -> tuple[dict[str, Any], ...]:
        raw = getattr(adapter, "composition_store_bindings", None)
        claimed_sha = getattr(
            adapter, "composition_store_bindings_sha256", None
        )
        if (
            not isinstance(raw, tuple)
            or not raw
            or not isinstance(claimed_sha, str)
            or _SHA.fullmatch(claimed_sha) is None
            or _digest(raw) != claimed_sha
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "adapter durable store authority differs"
            )
        normalized: list[dict[str, Any]] = []
        prior_path = ""
        persistent_output_authority = False
        for binding in raw:
            if not isinstance(binding, Mapping) or set(binding) != {
                "directory",
                "instance_authority_sha256",
                "path",
                "persistent_instance_authority",
                "roles",
                "store_class",
                "witness",
            }:
                raise SerialCompletionDispatcherIntegrityError(
                    "adapter durable store binding shape differs"
                )
            path = Path(binding["path"])
            witness = binding["witness"]
            instance_sha = binding["instance_authority_sha256"]
            roles = binding["roles"]
            if (
                not path.is_absolute()
                or str(path) <= prior_path
                or path.is_symlink()
                or not path.exists()
                or path.resolve(strict=True) != path
                or not isinstance(witness, Mapping)
                or set(witness) != {"device", "inode", "links", "mode"}
                or not isinstance(roles, list)
                or not roles
                or roles != sorted(set(roles))
                or not all(isinstance(role, str) and role for role in roles)
                or not isinstance(binding["store_class"], str)
                or not binding["store_class"]
                or not isinstance(binding["directory"], bool)
                or not isinstance(binding["persistent_instance_authority"], bool)
                or (
                    instance_sha is not None
                    and (not isinstance(instance_sha, str) or _SHA.fullmatch(instance_sha) is None)
                )
                or binding["persistent_instance_authority"] != (instance_sha is not None)
            ):
                raise SerialCompletionDispatcherIntegrityError(
                    "adapter durable store binding value differs"
                )
            info = path.lstat()
            actual = {
                "device": int(info.st_dev),
                "inode": int(info.st_ino),
                "links": int(info.st_nlink),
                "mode": int(info.st_mode),
            }
            if (
                dict(witness) != actual
                or binding["directory"] != stat.S_ISDIR(info.st_mode)
                or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))
                or (stat.S_ISREG(info.st_mode) and int(info.st_nlink) != 1)
            ):
                raise SerialCompletionDispatcherIntegrityError(
                    "adapter durable store witness differs"
                )
            normalized.append(json.loads(_canonical(dict(binding))))
            if binding["persistent_instance_authority"] and any(
                role.endswith(".output_store") for role in roles
            ):
                persistent_output_authority = True
            prior_path = str(path)
        if not persistent_output_authority:
            raise SerialCompletionDispatcherIntegrityError(
                "route completion output instance authority is unavailable"
            )
        normalized_tuple = tuple(normalized)
        if _digest(normalized_tuple) != claimed_sha:
            raise SerialCompletionDispatcherIntegrityError(
                "adapter durable store authority normalization differs"
            )
        return normalized_tuple

    @staticmethod
    def _runtime_binding_witness(
        adapter: CompletionRouteAdapter,
    ) -> tuple[Any, ...]:
        """Pin mutable consumer/runner/store references used after admission."""

        consumer = getattr(adapter, "_consumer", None)
        runner = getattr(consumer, "runner", None)
        if runner is None:
            runner = getattr(consumer, "process_runner", None)
        witness: list[Any] = [
            ("consumer", id(consumer), type(consumer)),
            ("runner", id(runner), type(runner)),
        ]
        for label, container in (("consumer", consumer), ("runner", runner)):
            if container is None:
                continue
            for attribute in (
                "_clock",
                "clock",
                "lifecycle_store",
                "lifecycle",
                "desired_state_store",
                "desired",
                "raw_store",
                "handoff_raw_store",
                "artifact_store",
                "evidence_store",
                "output_store",
            ):
                value = getattr(container, attribute, None)
                raw_path = getattr(value, "root", None)
                if raw_path is None:
                    raw_path = getattr(value, "path", None)
                witness.append((
                    f"{label}.{attribute}",
                    id(value),
                    type(value),
                    None if raw_path is None else str(raw_path),
                ))
        return tuple(witness)

    @staticmethod
    def _adapter_witness(adapter: CompletionRouteAdapter) -> tuple[Any, ...]:
        return (
            id(adapter),
            type(adapter),
            adapter.workload_id,
            adapter.consumer_id,
            adapter.stream_id,
            float(adapter.lease_seconds),
            adapter.fingerprint,
            id(getattr(adapter, "clock", None)),
            id(getattr(adapter, "_consumer", None)),
            id(getattr(adapter, "_lifecycle", None)),
            id(getattr(adapter, "_desired", None)),
            id(getattr(adapter, "_clock", None)),
            id(getattr(adapter, "_authority", None)),
            id(getattr(adapter, "_result_type", None)),
            getattr(adapter, "_output_store_identity_sha256", None),
            getattr(adapter, "composition_store_bindings_sha256", None),
            SerialCompletionDispatcher._runtime_binding_witness(adapter),
        )

    def _verify_adapter_bindings(
        self, workload_id: str | None = None
    ) -> None:
        workload_ids = ROUTE_ORDER if workload_id is None else (workload_id,)
        if tuple(self.adapters) != ROUTE_ORDER:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher adapter map changed"
            )
        for route in workload_ids:
            adapter = self.adapters[route]
            if (
                self._adapter_witness(adapter) != self._adapter_witnesses[route]
                or self._validated_route_store_bindings(adapter)
                != self._route_store_bindings[route]
            ):
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher adapter binding changed"
                )

    @staticmethod
    def _canonical_existing_path(path: Path, *, directory: bool) -> Path:
        if not path.is_absolute() or path.is_symlink() or not path.exists():
            raise SerialCompletionDispatcherIntegrityError(
                "composition store path is unsafe"
            )
        resolved = path.resolve(strict=True)
        info = path.lstat()
        if (
            resolved != path
            or directory != stat.S_ISDIR(info.st_mode)
            or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))
            or (stat.S_ISREG(info.st_mode) and int(info.st_nlink) != 1)
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "composition store path identity differs"
            )
        return resolved

    def _composition_authority(self) -> str:
        lifecycle_root = self._canonical_existing_path(
            Path(self.lifecycle_store.root), directory=True
        )
        lifecycle_path = self._canonical_existing_path(
            Path(self.lifecycle_store.path), directory=False
        )
        desired_path = self._canonical_existing_path(
            Path(self.desired_state_store.path), directory=False
        )
        lifecycle_sha = _digest({
            "contract": LIFECYCLE_CONTRACT,
            "database_path": str(lifecycle_path),
            "database_witness": self._witness_mapping(lifecycle_path),
            "identity_sha256": _sha(
                self.lifecycle_store.identity_sha256,
                "lifecycle store identity SHA-256",
            ),
            "limits": asdict(self.lifecycle_store._limits),
            "root_path": str(lifecycle_root),
            "root_witness": self._witness_mapping(lifecycle_root),
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
        })
        desired_sha = _digest({
            "contract": DESIRED_STATE_CONTRACT,
            "database_path": str(desired_path),
            "database_witness": self._witness_mapping(desired_path),
            "identity_sha256": _sha(
                self.desired_state_store.identity_sha256,
                "desired store identity SHA-256",
            ),
            "max_authority_claims": DESIRED_MAX_AUTHORITY_CLAIMS,
            "max_contract_bytes": DESIRED_MAX_CONTRACT_BYTES,
            "max_database_bytes": DESIRED_MAX_DATABASE_BYTES,
            "max_state_rows": DESIRED_MAX_STATE_ROWS,
            "method": DESIRED_STATE_METHOD,
            "schema_version": DESIRED_STATE_SCHEMA_VERSION,
        })
        return _digest({
            "desired_store_authority_sha256": desired_sha,
            "lifecycle_store_authority_sha256": lifecycle_sha,
            "route_store_bindings": {
                route: list(self._route_store_bindings[route])
                for route in ROUTE_ORDER
            },
            "schema": COMPOSITION_AUTHORITY_SCHEMA,
        })

    @staticmethod
    def _witness_mapping(path: Path) -> dict[str, int]:
        info = path.lstat()
        return {
            "device": int(info.st_dev),
            "inode": int(info.st_ino),
            "links": int(info.st_nlink),
            "mode": int(info.st_mode),
        }

    def _capture_composition_witnesses(
        self,
    ) -> tuple[tuple[str, int, int, int, int], ...]:
        paths = (
            (Path(self.lifecycle_store.root), True),
            (Path(self.lifecycle_store.path), False),
            (Path(self.desired_state_store.path), False),
            *(
                (Path(binding["path"]), bool(binding["directory"]))
                for route in ROUTE_ORDER
                for binding in self._route_store_bindings[route]
            ),
        )
        captured = []
        for path, directory in paths:
            canonical = self._canonical_existing_path(
                path, directory=directory
            )
            info = canonical.lstat()
            captured.append((
                str(canonical),
                int(info.st_dev),
                int(info.st_ino),
                int(info.st_mode),
                int(info.st_nlink),
            ))
        return tuple(captured)

    def _verify_composition_witnesses(self) -> None:
        if self._capture_composition_witnesses() != self._composition_witnesses:
            raise SerialCompletionDispatcherIntegrityError(
                "composition store identity changed"
            )

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    @staticmethod
    def _preflight_parent(parent: Path) -> Path:
        if not parent.exists() or parent.is_symlink():
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher parent is missing or linked"
            )
        for candidate in (parent, *parent.parents):
            info = candidate.lstat()
            if candidate.is_symlink() or not stat.S_ISDIR(info.st_mode):
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher parent chain is unsafe"
                )
        return parent.resolve(strict=True)

    @staticmethod
    def _preflight_leaf(path: Path) -> tuple[int, int, int] | None:
        if path.is_symlink():
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher leaf is linked"
            )
        if not path.exists():
            return None
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or int(info.st_nlink) != 1:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher leaf is not uniquely linked regular storage"
            )
        return (int(info.st_dev), int(info.st_ino), int(info.st_mode))

    @staticmethod
    def _path_witness(path: Path) -> tuple[int, int, int]:
        info = path.lstat()
        if (
            path.is_symlink()
            or not stat.S_ISREG(info.st_mode)
            or int(info.st_nlink) != 1
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher path is not a regular non-link file"
            )
        return (int(info.st_dev), int(info.st_ino), int(info.st_mode))

    def _verify_paths(self) -> None:
        if (
            self.path.resolve(strict=True) != self.path
            or self.lock_path.resolve(strict=True) != self.lock_path
            or self._path_witness(self.path) != self._database_witness
            or self._path_witness(self.lock_path) != self._lock_witness
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher path identity changed"
            )
        if hasattr(self, "_composition_witnesses"):
            self._verify_composition_witnesses()

    def _initialize_lock_anchor(self) -> None:
        expected = self._lock_preflight_witness
        flags = os.O_RDWR | getattr(os, "O_BINARY", 0)
        if expected is None:
            flags |= os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.lock_path, flags, 0o600)
            try:
                os.set_inheritable(descriptor, False)
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode):
                    raise SerialCompletionDispatcherIntegrityError(
                        "dispatcher owner anchor is not regular"
                    )
                opened = (int(info.st_dev), int(info.st_ino), int(info.st_mode))
                if expected is not None and opened != expected:
                    raise SerialCompletionDispatcherIntegrityError(
                        "dispatcher owner anchor changed before open"
                    )
                if self._path_witness(self.lock_path) != opened:
                    raise SerialCompletionDispatcherIntegrityError(
                        "dispatcher owner anchor changed during open"
                    )
                size = int(info.st_size)
                os.lseek(descriptor, 0, os.SEEK_SET)
                existing = os.read(descriptor, max(size, len(self._lock_anchor)) + 1)
                if size == 0:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    os.write(descriptor, self._lock_anchor)
                    os.ftruncate(descriptor, len(self._lock_anchor))
                    os.fsync(descriptor)
                elif existing != self._lock_anchor:
                    raise SerialCompletionDispatcherIntegrityError(
                        "dispatcher owner anchor content differs"
                    )
            finally:
                os.close(descriptor)
        except SerialCompletionDispatcherError:
            raise
        except PermissionError:
            # Windows may deny a second read handle while the first owner has
            # the anchor byte locked.  Defer content verification to acquire;
            # only a pre-existing regular, non-link anchor may take this path.
            if (
                self.lock_path.exists()
                and not self.lock_path.is_symlink()
                and stat.S_ISREG(self.lock_path.lstat().st_mode)
            ):
                return
            raise SerialCompletionDispatcherDurabilityError(
                "dispatcher owner anchor failed"
            ) from None
        except OSError as exc:
            raise SerialCompletionDispatcherDurabilityError(
                "dispatcher owner anchor failed"
            ) from exc

    def _initialize_database_leaf(self) -> None:
        expected = self._database_preflight_witness
        if expected is None:
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(self.path, flags, 0o600)
                try:
                    os.set_inheritable(descriptor, False)
                    info = os.fstat(descriptor)
                    if not stat.S_ISREG(info.st_mode):
                        raise SerialCompletionDispatcherIntegrityError(
                            "dispatcher database leaf is not regular"
                        )
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except SerialCompletionDispatcherError:
                raise
            except OSError as exc:
                raise SerialCompletionDispatcherDurabilityError(
                    "dispatcher database leaf creation failed"
                ) from exc
        elif self._path_witness(self.path) != expected:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher database changed after preflight"
            )

    def _connect(self) -> sqlite3.Connection:
        # ``mode=rw`` cannot create a surprise target and SQLite's nofollow URI
        # flag closes the leaf-link race on supported runtimes.  The exact file
        # witness is rechecked before any mutating PRAGMA.
        uri_path = quote(self.path.as_posix(), safe="/:")
        database = sqlite3.connect(
            f"file:{uri_path}?mode=rw&nofollow=1",
            timeout=SQLITE_BUSY_TIMEOUT_SECONDS,
            isolation_level=None,
            uri=True,
        )
        database.row_factory = sqlite3.Row
        try:
            if self._path_witness(self.path) != self._database_witness:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher database changed before SQLite open"
                )
        except BaseException:
            database.close()
            raise
        database.execute("PRAGMA foreign_keys=ON")
        database.execute("PRAGMA journal_mode=DELETE")
        database.execute("PRAGMA synchronous=FULL")
        database.execute("PRAGMA trusted_schema=OFF")
        database.execute(
            f"PRAGMA busy_timeout={int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000)}"
        )
        return database

    def _manifest(self) -> dict[str, str]:
        return {
            "contract": CONTRACT,
            "dispatcher_profile_sha256": self.profile_sha256,
            "composition_authority_sha256": self.composition_authority_sha256,
            "route_catalog_sha256": ROUTE_CATALOG_SHA256,
            "schema_version": str(SCHEMA_VERSION),
            "sqlite_schema_sha256": EXPECTED_SCHEMA_SHA256,
        }

    def _initial_state(self) -> dict[str, Any]:
        return {
            "claims": 0,
            "idle_scans": 0,
            "integrity_fenced": 0,
            "last_observed_at": 0.0,
            "next_route_index": 0,
            "owner_epoch": "0" * 64,
            "pending_event_sha256": None,
            "pending_lease_expires_at": None,
            "pending_owner_epoch": None,
            "pending_phase": None,
            "pending_stream_sequence": None,
            "pending_workload_id": None,
            "retryable_results": 0,
            "revision": 0,
            "terminalizations": 0,
            "ticks": 0,
        }

    def _state_hash(
        self,
        state: Mapping[str, Any],
        *,
        profile_sha256: str | None = None,
    ) -> str:
        return _digest({
            **state,
            "dispatcher_profile_sha256": (
                self.profile_sha256
                if profile_sha256 is None
                else _sha(profile_sha256, "legacy dispatcher profile SHA-256")
            ),
            "schema": STATE_SCHEMA,
        })

    def _migrate_legacy_database_v1(
        self,
        database: sqlite3.Connection,
    ) -> bool:
        """Atomically rebind one idle exact-11 cursor to the current catalogue."""

        if _schema_inventory(database) != LEGACY_EXPECTED_SCHEMA_V1:
            return False
        manifest = {
            str(row["key"]): str(row["value"])
            for row in database.execute("SELECT key,value FROM manifest")
        }
        if (
            set(manifest) != set(self._manifest())
            or manifest.get("contract") != CONTRACT
            or manifest.get("schema_version") != "1"
            or manifest.get("sqlite_schema_sha256")
            != LEGACY_EXPECTED_SCHEMA_SHA256_V1
            or manifest.get("route_catalog_sha256")
            != LEGACY_ROUTE_CATALOG_SHA256_V1
            or _SHA.fullmatch(manifest.get("dispatcher_profile_sha256", ""))
            is None
            or _SHA.fullmatch(manifest.get("composition_authority_sha256", ""))
            is None
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "legacy dispatcher migration authority differs"
            )
        state = self._read_state(
            database,
            profile_sha256=manifest["dispatcher_profile_sha256"],
            route_order=LEGACY_ROUTE_ORDER_V1,
            max_routes=LEGACY_ROUTE_LIMIT_V1,
        )
        if state["pending_workload_id"] is not None:
            raise SerialCompletionDispatcherIntegrityError(
                "legacy dispatcher pending claim cannot be rebound"
            )
        legacy_workload_id = LEGACY_ROUTE_ORDER_V1[state["next_route_index"]]
        try:
            state["next_route_index"] = ROUTE_ORDER.index(legacy_workload_id)
        except ValueError as err:  # pragma: no cover - import-time subset invariant
            raise SerialCompletionDispatcherIntegrityError(
                "legacy dispatcher route is absent from current catalogue"
            ) from err
        state.pop("state_sha256", None)
        database.execute(
            "ALTER TABLE dispatcher_state RENAME TO dispatcher_state_legacy_v1"
        )
        database.execute(dict(_TABLE_DDL)["dispatcher_state"])
        database.execute(
            "INSERT INTO dispatcher_state VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self._state_values(state),
        )
        database.execute("DROP TABLE dispatcher_state_legacy_v1")
        database.execute("DELETE FROM manifest")
        for key, value in self._manifest().items():
            database.execute(
                "INSERT INTO manifest(key,value) VALUES(?,?)",
                (key, value),
            )
        self._fault("migration.after_rebuild_before_commit")
        return True

    def _initialize_database(self) -> None:
        try:
            with closing(self._connect()) as database:
                database.execute("BEGIN EXCLUSIVE")
                objects = database.execute(
                    "SELECT type,name FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%'"
                ).fetchall()
                if not objects:
                    for _name, sql in _TABLE_DDL:
                        database.execute(sql)
                    for key, value in self._manifest().items():
                        database.execute(
                            "INSERT INTO manifest(key,value) VALUES(?,?)", (key, value)
                        )
                    state = self._initial_state()
                    database.execute(
                        "INSERT INTO dispatcher_state VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        self._state_values(state),
                    )
                else:
                    self._migrate_legacy_database_v1(database)
                self._verify_database_connection(database, full=True)
                database.commit()
        except SerialCompletionDispatcherError:
            raise
        except sqlite3.DatabaseError as exc:
            raise _classified_sqlite_error(exc, "database initialization") from exc
        self._verify_file_cap()

    def _state_values(self, state: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            1,
            state["revision"],
            state["last_observed_at"],
            state["next_route_index"],
            state["owner_epoch"],
            state["integrity_fenced"],
            state["pending_workload_id"],
            state["pending_event_sha256"],
            state["pending_stream_sequence"],
            state["pending_phase"],
            state["pending_lease_expires_at"],
            state["pending_owner_epoch"],
            state["ticks"],
            state["claims"],
            state["idle_scans"],
            state["terminalizations"],
            state["retryable_results"],
            self._state_hash(state),
        )

    def _read_state(
        self,
        database: sqlite3.Connection,
        *,
        profile_sha256: str | None = None,
        route_order: tuple[str, ...] = ROUTE_ORDER,
        max_routes: int = MAX_ROUTES,
    ) -> dict[str, Any]:
        row = database.execute(
            "SELECT * FROM dispatcher_state WHERE singleton=1"
        ).fetchone()
        if row is None or database.execute(
            "SELECT count(*) FROM dispatcher_state"
        ).fetchone()[0] != 1:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher state cardinality differs"
            )
        state = {
            "claims": int(row["claims"]),
            "idle_scans": int(row["idle_scans"]),
            "integrity_fenced": int(row["integrity_fenced"]),
            "last_observed_at": _timestamp(
                row["last_observed_at"], "dispatcher durable clock"
            ),
            "next_route_index": int(row["next_route_index"]),
            "owner_epoch": str(row["owner_epoch"]),
            "pending_event_sha256": row["pending_event_sha256"],
            "pending_lease_expires_at": row["pending_lease_expires_at"],
            "pending_owner_epoch": row["pending_owner_epoch"],
            "pending_phase": row["pending_phase"],
            "pending_stream_sequence": row["pending_stream_sequence"],
            "pending_workload_id": row["pending_workload_id"],
            "retryable_results": int(row["retryable_results"]),
            "revision": int(row["revision"]),
            "terminalizations": int(row["terminalizations"]),
            "ticks": int(row["ticks"]),
        }
        for name in (
            "claims",
            "idle_scans",
            "retryable_results",
            "revision",
            "terminalizations",
            "ticks",
        ):
            if state[name] < 0:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher counter is negative"
                )
        if not 0 <= state["next_route_index"] < max_routes:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher route cursor differs"
            )
        if state["integrity_fenced"] not in {0, 1}:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher integrity fence differs"
            )
        _sha(state["owner_epoch"], "dispatcher owner epoch")
        pending_values = (
            state["pending_workload_id"],
            state["pending_event_sha256"],
            state["pending_stream_sequence"],
            state["pending_phase"],
            state["pending_lease_expires_at"],
            state["pending_owner_epoch"],
        )
        if any(value is None for value in pending_values):
            if any(value is not None for value in pending_values):
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher pending state is partial"
                )
        else:
            if (
                state["pending_workload_id"] not in route_order
                or state["pending_phase"] not in _PENDING_PHASES
            ):
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher pending route differs"
                )
            _sha(state["pending_event_sha256"], "pending event SHA-256")
            _sha(state["pending_owner_epoch"], "pending owner epoch")
            if (
                isinstance(state["pending_stream_sequence"], bool)
                or not isinstance(state["pending_stream_sequence"], int)
                or state["pending_stream_sequence"] < 1
            ):
                raise SerialCompletionDispatcherIntegrityError(
                    "pending stream sequence differs"
                )
            state["pending_lease_expires_at"] = _timestamp(
                state["pending_lease_expires_at"], "pending lease expiry"
            )
        stored_hash = str(row["state_sha256"])
        if (
            _SHA.fullmatch(stored_hash) is None
            or self._state_hash(state, profile_sha256=profile_sha256)
            != stored_hash
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher durable state checksum differs"
            )
        state["state_sha256"] = stored_hash
        return state

    def _verify_database_connection(
        self, database: sqlite3.Connection, *, full: bool = False
    ) -> dict[str, Any]:
        if _schema_inventory(database) != EXPECTED_SCHEMA:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher SQLite schema differs"
            )
        manifest = {
            str(row["key"]): str(row["value"])
            for row in database.execute("SELECT key,value FROM manifest")
        }
        if manifest != self._manifest():
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher manifest differs"
            )
        if full and database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher SQLite quick_check failed"
            )
        page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
        if page_size <= 0 or page_count <= 0 or page_size * page_count > MAX_DATABASE_BYTES:
            raise SerialCompletionDispatcherDurabilityError(
                "dispatcher database physical cap exceeded"
            )
        return self._read_state(database)

    def _verify_file_cap(self) -> None:
        if not self.path.exists() or self.path.stat().st_size > MAX_DATABASE_BYTES:
            raise SerialCompletionDispatcherDurabilityError(
                "dispatcher database file cap exceeded"
            )

    def _read_verified_state(self) -> dict[str, Any]:
        self._verify_paths()
        self._verify_file_cap()
        try:
            with closing(self._connect()) as database:
                state = self._verify_database_connection(database, full=True)
        except SerialCompletionDispatcherError:
            raise
        except sqlite3.DatabaseError as exc:
            raise _classified_sqlite_error(exc, "state read") from exc
        self._last_state_sha256 = state["state_sha256"]
        return state

    def _mutate_state(
        self,
        mutate: Callable[[dict[str, Any]], None],
        *,
        fault_prefix: str,
    ) -> dict[str, Any]:
        self._assert_owner()
        self._verify_paths()
        try:
            with closing(self._connect()) as database:
                database.execute("BEGIN IMMEDIATE")
                state = self._verify_database_connection(database)
                previous_revision = state["revision"]
                state.pop("state_sha256")
                mutate(state)
                state["last_observed_at"] = max(
                    float(state["last_observed_at"]),
                    self.guarded_clock.high_water,
                )
                state["revision"] = previous_revision + 1
                values = self._state_values(state)
                self._fault(f"{fault_prefix}.before_commit")
                cursor = database.execute(
                    "UPDATE dispatcher_state SET "
                    "revision=?,last_observed_at=?,next_route_index=?,owner_epoch=?,"
                    "integrity_fenced=?,"
                    "pending_workload_id=?,pending_event_sha256=?,"
                    "pending_stream_sequence=?,pending_phase=?,"
                    "pending_lease_expires_at=?,pending_owner_epoch=?,ticks=?,claims=?,"
                    "idle_scans=?,terminalizations=?,retryable_results=?,state_sha256=? "
                    "WHERE singleton=1 AND revision=?",
                    (*values[1:], previous_revision),
                )
                if cursor.rowcount != 1:
                    raise SerialCompletionDispatcherIntegrityError(
                        "dispatcher state CAS failed"
                    )
                database.commit()
                self._fault(f"{fault_prefix}.after_commit")
        except SerialCompletionDispatcherError:
            raise
        except sqlite3.DatabaseError as exc:
            raise _classified_sqlite_error(exc, "state mutation") from exc
        self._verify_file_cap()
        verified = self._read_verified_state()
        if verified["revision"] != previous_revision + 1:
            raise SerialCompletionDispatcherDurabilityError(
                "dispatcher committed revision was not durable"
            )
        return verified

    def _refresh_after_clock(
        self, state: Mapping[str, Any]
    ) -> dict[str, Any]:
        # Observations only advance the in-memory guard.  The next existing
        # state transition persists max(state, high_water) atomically, avoiding
        # an fsync/revision burst for each idle route probe.
        return dict(state)

    def _restart_durable_clock_minimum(
        self, state: Mapping[str, Any]
    ) -> float:
        lifecycle_high_water = _timestamp(
            self.lifecycle_store.durable_clock_high_water(),
            "lifecycle durable clock high-water",
        )
        return max(float(state["last_observed_at"]), lifecycle_high_water)

    def acquire(self) -> "SerialCompletionDispatcher":
        with self._owner_lock:
            if self._descriptor is not None or self._owner_token is not None:
                raise SerialCompletionDispatcherOwnerBusy("dispatcher_owner_busy")
            if not _claim_process_path(self.lock_path):
                raise SerialCompletionDispatcherOwnerBusy("dispatcher_owner_busy")
            flags = os.O_RDWR | getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            descriptor: int | None = None
            try:
                self._verify_paths()
                descriptor = os.open(self.lock_path, flags)
                os.set_inheritable(descriptor, False)
                if self._path_witness(self.lock_path) != self._lock_witness:
                    raise SerialCompletionDispatcherIntegrityError(
                        "dispatcher owner anchor identity changed"
                    )
                _lock_descriptor(descriptor)
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.read(descriptor, len(self._lock_anchor) + 1) != self._lock_anchor:
                    raise SerialCompletionDispatcherIntegrityError(
                        "dispatcher owner anchor content changed"
                    )
                state = self._read_verified_state()
                now = self.guarded_clock.observe(
                    durable_minimum=self._restart_durable_clock_minimum(state)
                )
                epoch = hashlib.sha256(os.urandom(32)).hexdigest()
                token = object()
                self._descriptor = descriptor
                self._owner_token = token
                self._owner_epoch = epoch
                self._owner_id = "serial-completion-" + epoch[:32]

                def adopt(value: dict[str, Any]) -> None:
                    value["owner_epoch"] = epoch
                    value["last_observed_at"] = now

                self._mutate_state(adopt, fault_prefix="owner_adopt")
                self._pending_memory = None
                self._stop.clear()
                return self
            except (BlockingIOError, PermissionError):
                if descriptor is not None:
                    os.close(descriptor)
                _release_process_path(self.lock_path)
                raise SerialCompletionDispatcherOwnerBusy(
                    "dispatcher_owner_busy"
                ) from None
            except BaseException:
                self._descriptor = None
                self._owner_token = None
                self._owner_epoch = None
                self._owner_id = None
                if descriptor is not None:
                    try:
                        _unlock_descriptor(descriptor)
                    except (ImportError, OSError):
                        pass
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                _release_process_path(self.lock_path)
                raise

    start = acquire

    def _assert_owner(self) -> None:
        with self._owner_lock:
            descriptor = self._descriptor
            if (
                descriptor is None
                or self._owner_token is None
                or self._owner_epoch is None
                or self._owner_id is None
            ):
                raise SerialCompletionDispatcherOwnerLost(
                    "dispatcher_owner_not_held"
                )
            try:
                info = os.fstat(descriptor)
                os.lseek(descriptor, 0, os.SEEK_SET)
                content = os.read(descriptor, len(self._lock_anchor) + 1)
            except OSError as exc:
                raise SerialCompletionDispatcherOwnerLost(
                    "dispatcher_owner_witness_failed"
                ) from exc
            if (
                (int(info.st_dev), int(info.st_ino), int(info.st_mode))
                != self._lock_witness
                or content != self._lock_anchor
            ):
                raise SerialCompletionDispatcherOwnerLost(
                    "dispatcher_owner_witness_changed"
                )

    def request_stop(self) -> None:
        self._stop.set()

    def close(self, *, timeout_seconds: float = CLOSE_WAIT_SECONDS) -> bool:
        """Request stop and release ownership only after real tick quiescence.

        A non-cooperative child may outlive the bounded wait.  In that case the
        method returns ``False`` and deliberately retains both the OS lock and
        process-owner claim; the caller may retry close after the tick exits.
        """

        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or not 0.0 <= float(timeout_seconds) <= 3600.0
        ):
            raise ValueError("close timeout is invalid")
        self._stop.set()
        acquired = self._tick_lock.acquire(timeout=float(timeout_seconds))
        if not acquired:
            return False
        try:
            with self._owner_lock:
                descriptor = self._descriptor
                self._descriptor = None
                self._owner_token = None
                self._owner_epoch = None
                self._owner_id = None
                self._pending_memory = None
                if descriptor is not None:
                    try:
                        _unlock_descriptor(descriptor)
                    except (ImportError, OSError):
                        pass
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                    _release_process_path(self.lock_path)
            return True
        finally:
            self._tick_lock.release()

    def __enter__(self) -> "SerialCompletionDispatcher":
        return self.acquire()

    def __exit__(self, _kind: object, _value: object, _trace: object) -> None:
        if not self.close():
            raise SerialCompletionDispatcherOwnerBusy(
                "dispatcher_close_not_quiescent"
            )

    def _result(
        self,
        status: str,
        reason_code: str,
        *,
        state: Mapping[str, Any] | None = None,
        scanned_routes: int = 0,
        child_invocations: int = 0,
        workload_id: str | None = None,
        event_sha256: str | None = None,
        phase: str = "none",
        terminal_kind: str | None = None,
        output_sha256: str | None = None,
        output_receipt_sha256: str | None = None,
        process_receipt_sha256: str | None = None,
        consumption_receipt_sha256: str | None = None,
        quarantine_receipt_sha256: str | None = None,
    ) -> SerialCompletionTickResult:
        state_sha = (
            self._last_state_sha256
            if state is None
            else str(state["state_sha256"])
        )
        if _SHA.fullmatch(state_sha) is None:
            state_sha = "0" * 64
        return SerialCompletionTickResult(
            status=status,
            reason_code=reason_code,
            dispatcher_profile_sha256=self.profile_sha256,
            dispatcher_state_sha256=state_sha,
            scanned_routes=scanned_routes,
            child_invocations=child_invocations,
            workload_id=workload_id,
            event_sha256=event_sha256,
            phase=phase,
            terminal_kind=terminal_kind,
            output_sha256=output_sha256,
            output_receipt_sha256=output_receipt_sha256,
            process_receipt_sha256=process_receipt_sha256,
            consumption_receipt_sha256=consumption_receipt_sha256,
            quarantine_receipt_sha256=quarantine_receipt_sha256,
        )

    @staticmethod
    def _clear_pending_fields(state: dict[str, Any]) -> None:
        state["pending_workload_id"] = None
        state["pending_event_sha256"] = None
        state["pending_stream_sequence"] = None
        state["pending_phase"] = None
        state["pending_lease_expires_at"] = None
        state["pending_owner_epoch"] = None

    def _begin_tick(self) -> dict[str, Any]:
        state = self._read_verified_state()
        if state["integrity_fenced"]:
            return state
        now = self.guarded_clock.observe(
            durable_minimum=state["last_observed_at"]
        )

        def begin(value: dict[str, Any]) -> None:
            if value["owner_epoch"] != self._owner_epoch:
                raise SerialCompletionDispatcherOwnerLost(
                    "dispatcher owner epoch changed"
                )
            value["last_observed_at"] = now
            value["ticks"] += 1

        return self._mutate_state(begin, fault_prefix="tick_begin")

    def _fence_integrity(
        self, state: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Durably retain pending state and stop automatic event retries."""

        def fence(value: dict[str, Any]) -> None:
            if value["revision"] != state["revision"]:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher integrity fence CAS differs"
                )
            value["integrity_fenced"] = 1

        return self._mutate_state(fence, fault_prefix="integrity_fence")

    def _persist_claim(
        self, state: Mapping[str, Any], *, route_index: int, lease: CompletionLease
    ) -> dict[str, Any]:
        epoch = self._owner_epoch
        if epoch is None:
            raise SerialCompletionDispatcherOwnerLost("dispatcher owner not held")

        def claimed(value: dict[str, Any]) -> None:
            if value["revision"] != state["revision"]:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher claim state revision changed"
                )
            if value["pending_workload_id"] is not None:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher claim would overwrite pending accounting"
                )
            value["claims"] += 1
            value["next_route_index"] = (route_index + 1) % MAX_ROUTES
            value["pending_workload_id"] = ROUTE_ORDER[route_index]
            value["pending_event_sha256"] = lease.event_sha256
            value["pending_stream_sequence"] = lease.stream_sequence
            value["pending_phase"] = "initial"
            value["pending_lease_expires_at"] = lease.lease_expires_at
            value["pending_owner_epoch"] = epoch

        return self._mutate_state(claimed, fault_prefix="claim_persist")

    def _reconcile_pending_terminal(
        self, state: Mapping[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        """Account an exact lifecycle terminal left by a prior process crash."""

        workload_id = state["pending_workload_id"]
        if workload_id is None:
            return dict(state), False
        adapter = self.adapters[workload_id]
        self._verify_adapter_bindings(workload_id)
        identity = self.lifecycle_store.terminal_event_identity(
            adapter.stream_id,
            int(state["pending_stream_sequence"]),
        )
        if identity is None:
            return dict(state), False
        if (
            type(identity) is not TerminalEventIdentity
            or identity.status not in {"hot", "archived"}
            or identity.terminal_kind not in {
                "consumed", "rejected", "quarantined", "superseded"
            }
            or identity.consumer_id != adapter.consumer_id
            or identity.stream_id != adapter.stream_id
            or identity.stream_sequence != state["pending_stream_sequence"]
            or identity.event_sha256 != state["pending_event_sha256"]
            or _SHA.fullmatch(identity.identity_sha256) is None
        ):
            raise SerialCompletionDispatcherIntegrityError(
                "pending lifecycle terminal identity differs"
            )
        return self._finish_pending(state, terminal=True), True

    def _persist_continuation(
        self,
        state: Mapping[str, Any],
        continuation: AdapterContinuation,
    ) -> dict[str, Any]:
        epoch = self._owner_epoch
        if epoch is None:
            raise SerialCompletionDispatcherOwnerLost("dispatcher owner not held")

        def continued(value: dict[str, Any]) -> None:
            if (
                value["revision"] != state["revision"]
                or value["pending_workload_id"] != continuation.workload_id
                or value["pending_event_sha256"] != continuation.event_sha256
                or value["pending_owner_epoch"] != epoch
            ):
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher continuation CAS differs"
                )
            value["pending_phase"] = "final_reproof"
            value["pending_lease_expires_at"] = continuation.lease_expires_at

        return self._mutate_state(continued, fault_prefix="continuation_persist")

    def _finish_pending(
        self,
        state: Mapping[str, Any],
        *,
        terminal: bool = False,
        retryable: bool = False,
    ) -> dict[str, Any]:
        def finish(value: dict[str, Any]) -> None:
            if value["revision"] != state["revision"]:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher pending completion CAS differs"
                )
            self._clear_pending_fields(value)
            if terminal:
                value["terminalizations"] += 1
            if retryable:
                value["retryable_results"] += 1

        result = self._mutate_state(finish, fault_prefix="pending_finish")
        self._pending_memory = None
        return result

    def _rotate_idle(self, state: Mapping[str, Any]) -> dict[str, Any]:
        start = int(state["next_route_index"])

        def rotate(value: dict[str, Any]) -> None:
            if value["revision"] != state["revision"]:
                raise SerialCompletionDispatcherIntegrityError(
                    "dispatcher idle cursor CAS differs"
                )
            value["next_route_index"] = (start + 1) % MAX_ROUTES
            value["idle_scans"] += 1

        return self._mutate_state(rotate, fault_prefix="idle_rotate")

    def _invoke_phase(
        self,
        *,
        adapter: CompletionRouteAdapter,
        lease: CompletionLease,
        continuation: AdapterContinuation | None,
        cancellation: _CompositeCancel,
    ) -> tuple[AdapterPhaseResult, HeavyChildPermit]:
        epoch = self._owner_epoch
        if epoch is None:
            raise SerialCompletionDispatcherOwnerLost("dispatcher owner not held")
        self._verify_adapter_bindings(adapter.workload_id)
        nonce = object()
        requested_phase = (
            "final_reproof"
            if continuation is not None
            else (
                "initial"
                if type(adapter) in {
                    NoaaCompletionRouteAdapter,
                    RawManifestCompletionRouteAdapter,
                }
                else "single"
            )
        )
        tick_sha = _digest({
            "event_sha256": lease.event_sha256,
            "owner_epoch": epoch,
            "phase": requested_phase,
            "route_profile_sha256": adapter.fingerprint,
            "schema": "stv3-heavy-child-dispatch-tick-v1",
        })
        permit = HeavyChildPermit(
            tick_sha256=tick_sha,
            expected_workload_id=adapter.workload_id,
            expected_phase=requested_phase,
            nonce=nonce,
        )
        if continuation is None:
            result = adapter.begin(
                lease,
                permit=permit,
                permit_nonce=nonce,
                cancel_event=cancellation,
            )
        else:
            result = adapter.finalize(
                continuation,
                permit=permit,
                permit_nonce=nonce,
                cancel_event=cancellation,
            )
        self._verify_adapter_bindings(adapter.workload_id)
        if not isinstance(result, AdapterPhaseResult):
            raise CompletionRouteAdapterIntegrityError(
                "adapter returned wrong exact phase result"
            )
        if (
            result.workload_id != adapter.workload_id
            or result.event_sha256 != lease.event_sha256
            or result.phase != requested_phase
            or result.child_invocations > MAX_HEAVY_CHILDREN_PER_TICK
            or permit.consumed != (result.child_invocations == 1)
        ):
            raise CompletionRouteAdapterIntegrityError(
                "adapter result exceeds exact tick scope"
            )
        return result, permit

    def _result_from_adapter(
        self,
        adapter_result: AdapterPhaseResult,
        *,
        state: Mapping[str, Any],
        scanned_routes: int,
    ) -> SerialCompletionTickResult:
        status = adapter_result.status
        return self._result(
            status,
            adapter_result.reason_code,
            state=state,
            scanned_routes=scanned_routes,
            child_invocations=adapter_result.child_invocations,
            workload_id=adapter_result.workload_id,
            event_sha256=adapter_result.event_sha256,
            phase=adapter_result.phase,
            terminal_kind=adapter_result.terminal_kind,
            output_sha256=adapter_result.output_sha256,
            output_receipt_sha256=adapter_result.output_receipt_sha256,
            process_receipt_sha256=adapter_result.process_receipt_sha256,
            consumption_receipt_sha256=adapter_result.consumption_receipt_sha256,
        )

    def tick(self, *, cancel_event: object | None = None) -> SerialCompletionTickResult:
        """Run one bounded scan or one pending final phase, never both."""

        if not self._tick_lock.acquire(blocking=False):
            return self._result("busy", "dispatcher_tick_busy")
        state: dict[str, Any] | None = None
        last_workload_id: str | None = None
        last_event_sha256: str | None = None
        last_phase = "none"
        last_child_upper_bound = 0
        try:
            self._assert_owner()
            self._verify_adapter_bindings()
            cancellation = _CompositeCancel(self._stop, cancel_event)
            if cancellation.is_set():
                return self._result("stopped", "dispatcher_stopped")
            try:
                state = self._begin_tick()
            except SerialCompletionDispatcherClockRollback:
                return self._result("blocked", "clock_rollback_detected")
            if state["integrity_fenced"]:
                return self._result(
                    "blocked", "dispatcher_integrity_fenced", state=state
                )
            now = self.guarded_clock.observe(
                durable_minimum=state["last_observed_at"]
            )
            state = self._refresh_after_clock(state)

            pending_workload = state["pending_workload_id"]
            if pending_workload is not None:
                continuation = self._pending_memory
                if (
                    continuation is not None
                    and state["pending_owner_epoch"] == self._owner_epoch
                    and state["pending_phase"] == "final_reproof"
                    and continuation.workload_id == pending_workload
                    and continuation.event_sha256 == state["pending_event_sha256"]
                    and continuation.continuation_sha256
                ):
                    continuation.validate()
                    pending = continuation.opaque_pending
                    lease = getattr(pending, "lease", None)
                    if not isinstance(lease, CompletionLease):
                        raise SerialCompletionDispatcherIntegrityError(
                            "in-memory continuation lost parent-held lease"
                        )
                    adapter = self.adapters[pending_workload]
                    last_workload_id = pending_workload
                    last_event_sha256 = lease.event_sha256
                    last_phase = "final_reproof"
                    if cancellation.is_set():
                        return self._result(
                            "stopped", "dispatcher_stopped", state=state,
                            workload_id=pending_workload,
                            event_sha256=lease.event_sha256,
                            phase="final_reproof",
                        )
                    try:
                        last_child_upper_bound = 1
                        result, _permit = self._invoke_phase(
                            adapter=adapter,
                            lease=lease,
                            continuation=continuation,
                            cancellation=cancellation,
                        )
                        self.guarded_clock.observe(
                            durable_minimum=state["last_observed_at"]
                        )
                        state = self._refresh_after_clock(state)
                        last_child_upper_bound = result.child_invocations
                    except SerialCompletionDispatcherClockRollback:
                        raise
                    except PermanentEventFailure:
                        # No exact adapter currently has an independently
                        # durable failure receipt.  Caller-constructed typed
                        # failures are identity only and cannot quarantine.
                        state = self._refresh_after_clock(state)
                        state = self._fence_integrity(state)
                        return self._result(
                            "blocked", "permanent_failure_authority_unavailable",
                            state=state, workload_id=pending_workload,
                            event_sha256=lease.event_sha256,
                            phase="final_reproof",
                            child_invocations=last_child_upper_bound,
                        )
                    except Exception as exc:
                        state = self._refresh_after_clock(state)
                        if is_retryable_exception(exc):
                            state = self._finish_pending(state, retryable=True)
                            return self._result(
                                "retryable", retryable_exception_reason(exc),
                                state=state, workload_id=pending_workload,
                                event_sha256=lease.event_sha256,
                                phase="final_reproof",
                                child_invocations=last_child_upper_bound,
                            )
                        state = self._fence_integrity(state)
                        return self._result(
                            "blocked", "adapter_integrity_fenced",
                            state=state, workload_id=pending_workload,
                            event_sha256=lease.event_sha256,
                            phase="final_reproof",
                            child_invocations=last_child_upper_bound,
                        )
                    if result.status == "continuation_pending":
                        state = self._finish_pending(state, retryable=True)
                        return self._result(
                            "blocked", "nested_continuation_rejected", state=state,
                            child_invocations=result.child_invocations,
                            workload_id=pending_workload,
                            event_sha256=lease.event_sha256,
                            phase="final_reproof",
                        )
                    state = self._finish_pending(
                        state,
                        terminal=result.status in {"acknowledged", "rejected"},
                        retryable=result.status == "retryable",
                    )
                    return self._result_from_adapter(
                        result, state=state, scanned_routes=0
                    )

                # A prior process owned this lease/pending object.  Never
                # serialize/recreate its token.  claim_next remains the sole
                # lease authority.  The checksum-verified lifecycle terminal
                # identity accounts an ACK/reject/quarantine/supersession that
                # committed immediately before the old process crashed.
                state, terminal_reconciled = self._reconcile_pending_terminal(
                    state
                )
                if terminal_reconciled:
                    pending_workload = None
                elif now >= float(state["pending_lease_expires_at"]):
                    state = self._finish_pending(state)
                    pending_workload = None
                else:
                    # A new claim must never overwrite live crash accounting.
                    # The old owner cannot still be running while this owner
                    # holds the lifetime OS fence, so a later tick either sees
                    # terminal evidence or safely reaches lease expiry.
                    return self._result(
                        "idle", "pending_lease_active", state=state,
                        scanned_routes=0,
                    )

            start_index = int(state["next_route_index"])
            scanned = 0
            selected: tuple[int, CompletionLease] | None = None
            for offset in range(MAX_ROUTES):
                if cancellation.is_set():
                    return self._result(
                        "stopped", "dispatcher_stopped", state=state,
                        scanned_routes=scanned,
                    )
                route_index = (start_index + offset) % MAX_ROUTES
                workload_id = ROUTE_ORDER[route_index]
                scanned += 1
                adapter = self.adapters[workload_id]
                self._verify_adapter_bindings(workload_id)
                claim_now = self.guarded_clock.observe(
                    durable_minimum=state["last_observed_at"]
                )
                state = self._refresh_after_clock(state)
                lease = self.lifecycle_store.claim_next(
                    adapter.consumer_id,
                    adapter.stream_id,
                    self._owner_id,
                    lease_seconds=adapter.lease_seconds,
                    now=claim_now,
                )
                if lease is None:
                    continue
                if (
                    lease.consumer_id != adapter.consumer_id
                    or lease.stream_id != adapter.stream_id
                ):
                    raise SerialCompletionDispatcherIntegrityError(
                        "lifecycle claim differs from selected route"
                    )
                selected = (route_index, lease)
                break

            if selected is None:
                state = self._rotate_idle(state)
                return self._result(
                    "idle", "no_ready_completion", state=state,
                    scanned_routes=scanned,
                )

            route_index, lease = selected
            workload_id = ROUTE_ORDER[route_index]
            adapter = self.adapters[workload_id]
            last_workload_id = workload_id
            last_event_sha256 = lease.event_sha256
            last_phase = "initial" if type(adapter) in {
                NoaaCompletionRouteAdapter,
                RawManifestCompletionRouteAdapter,
            } else "single"
            try:
                state = self._persist_claim(
                    state, route_index=route_index, lease=lease
                )
            except SerialCompletionDispatcherError:
                # The lifecycle lease remains held until expiry.  No permit or
                # adapter call has occurred, so no child can have started.
                return self._result(
                    "blocked", "claim_state_not_durable",
                    state=state, scanned_routes=scanned,
                    workload_id=workload_id, event_sha256=lease.event_sha256,
                    phase=last_phase,
                )
            if cancellation.is_set():
                state = self._finish_pending(state, retryable=True)
                return self._result(
                    "stopped", "dispatcher_stopped", state=state,
                    scanned_routes=scanned, workload_id=workload_id,
                    event_sha256=lease.event_sha256, phase=last_phase,
                )
            try:
                last_child_upper_bound = 1
                result, _permit = self._invoke_phase(
                    adapter=adapter,
                    lease=lease,
                    continuation=None,
                    cancellation=cancellation,
                )
                self.guarded_clock.observe(
                    durable_minimum=state["last_observed_at"]
                )
                state = self._refresh_after_clock(state)
                last_child_upper_bound = result.child_invocations
            except SerialCompletionDispatcherClockRollback:
                raise
            except PermanentEventFailure:
                state = self._refresh_after_clock(state)
                state = self._fence_integrity(state)
                return self._result(
                    "blocked", "permanent_failure_authority_unavailable",
                    state=state, scanned_routes=scanned,
                    workload_id=workload_id, event_sha256=lease.event_sha256,
                    phase=last_phase,
                    child_invocations=last_child_upper_bound,
                )
            except Exception as exc:
                state = self._refresh_after_clock(state)
                if is_retryable_exception(exc):
                    state = self._finish_pending(state, retryable=True)
                    return self._result(
                        "retryable", retryable_exception_reason(exc), state=state,
                        scanned_routes=scanned, workload_id=workload_id,
                        event_sha256=lease.event_sha256, phase=last_phase,
                        child_invocations=last_child_upper_bound,
                    )
                state = self._fence_integrity(state)
                return self._result(
                    "blocked", "adapter_integrity_fenced", state=state,
                    scanned_routes=scanned, workload_id=workload_id,
                    event_sha256=lease.event_sha256, phase=last_phase,
                    child_invocations=last_child_upper_bound,
                )

            if result.status == "continuation_pending":
                continuation = result.continuation
                if continuation is None:
                    state = self._finish_pending(state, retryable=True)
                    return self._result(
                        "blocked", "continuation_identity_invalid", state=state,
                        scanned_routes=scanned,
                        child_invocations=result.child_invocations,
                        workload_id=workload_id,
                        event_sha256=lease.event_sha256,
                        phase="initial",
                    )
                try:
                    state = self._persist_continuation(state, continuation)
                except SerialCompletionDispatcherError:
                    # Durable output may exist, but the DB retains only the
                    # pre-child initial marker.  Never run final reproof in this
                    # tick and never persist the pending object/token.
                    return self._result(
                        "blocked", "continuation_state_not_durable",
                        state=state, scanned_routes=scanned,
                        child_invocations=result.child_invocations,
                        workload_id=workload_id,
                        event_sha256=lease.event_sha256,
                        phase="initial",
                    )
                self._pending_memory = continuation
                return self._result_from_adapter(
                    result, state=state, scanned_routes=scanned
                )

            state = self._finish_pending(
                state,
                terminal=result.status in {"acknowledged", "rejected"},
                retryable=result.status == "retryable",
            )
            return self._result_from_adapter(
                result, state=state, scanned_routes=scanned
            )
        except SerialCompletionDispatcherClockRollback:
            return self._result(
                "blocked", "clock_rollback_detected", state=state,
                child_invocations=last_child_upper_bound,
                workload_id=last_workload_id,
                event_sha256=last_event_sha256,
                phase=last_phase,
            )
        except SerialCompletionDispatcherDurabilityError:
            return self._result(
                "retryable", "system_store_unavailable", state=state,
                child_invocations=last_child_upper_bound,
                workload_id=last_workload_id,
                event_sha256=last_event_sha256,
                phase=last_phase,
            )
        except SerialCompletionDispatcherError:
            return self._result(
                "blocked", "dispatcher_integrity_blocked", state=state,
                child_invocations=last_child_upper_bound,
                workload_id=last_workload_id,
                event_sha256=last_event_sha256,
                phase=last_phase,
            )
        except Exception as exc:
            if is_retryable_exception(exc):
                return self._result(
                    "retryable", retryable_exception_reason(exc), state=state,
                    child_invocations=last_child_upper_bound,
                    workload_id=last_workload_id,
                    event_sha256=last_event_sha256,
                    phase=last_phase,
                )
            if state is not None:
                try:
                    state = self._fence_integrity(state)
                except SerialCompletionDispatcherError:
                    pass
            return self._result(
                "blocked", "dispatcher_integrity_fenced", state=state,
                child_invocations=last_child_upper_bound,
                workload_id=last_workload_id,
                event_sha256=last_event_sha256,
                phase=last_phase,
            )
        finally:
            self._tick_lock.release()

    def diagnostics(self) -> dict[str, Any]:
        """Return aggregate, bounded diagnostics with no route/event capability."""

        state = self._read_verified_state()
        result = {
            "claims": state["claims"],
            "composition_authority_sha256": self.composition_authority_sha256,
            "contract": DIAGNOSTICS_SCHEMA,
            "dispatcher_profile_sha256": self.profile_sha256,
            "dispatcher_state_sha256": state["state_sha256"],
            "healthy": not bool(state["integrity_fenced"]),
            "idle_scans": state["idle_scans"],
            "integrity_fenced": bool(state["integrity_fenced"]),
            "max_heavy_children_per_tick": MAX_HEAVY_CHILDREN_PER_TICK,
            "max_idle_claims_per_tick": MAX_IDLE_CLAIMS_PER_TICK,
            "owner_held": self._descriptor is not None,
            "pending_continuation": state["pending_workload_id"] is not None,
            "prewiring_blockers": list(PREWIRING_BLOCKERS),
            "retryable_results": state["retryable_results"],
            "route_catalog_sha256": ROUTE_CATALOG_SHA256,
            "route_count": MAX_ROUTES,
            "runtime_wiring": "inert",
            "terminalizations": state["terminalizations"],
            "ticks": state["ticks"],
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise SerialCompletionDispatcherIntegrityError(
                "dispatcher diagnostics exceed cap"
            )
        return result


__all__ = [
    "CONTRACT",
    "CLOSE_WAIT_SECONDS",
    "DIAGNOSTICS_SCHEMA",
    "GuardedCompletionClock",
    "MAX_DATABASE_BYTES",
    "MAX_HEAVY_CHILDREN_PER_TICK",
    "MAX_IDLE_CLAIMS_PER_TICK",
    "PREWIRING_BLOCKERS",
    "SerialCompletionDispatcher",
    "SerialCompletionDispatcherClockRollback",
    "SerialCompletionDispatcherDurabilityError",
    "SerialCompletionDispatcherError",
    "SerialCompletionDispatcherIntegrityError",
    "SerialCompletionDispatcherOwnerBusy",
    "SerialCompletionDispatcherOwnerLost",
    "SerialCompletionDispatcherProfile",
    "SerialCompletionTickResult",
]
