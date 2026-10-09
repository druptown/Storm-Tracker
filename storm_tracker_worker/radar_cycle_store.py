"""Crash-durable admission and compact state for worker-owned radar cycles.

Public API
==========

``RadarCycleStore.initialize()``
    Creates the bounded SQLite schema, verifies its version manifest, performs
    ``quick_check`` and foreign-key integrity checks, and configures WAL with
    FULL synchronous durability.  Normal hot-path snapshots only verify the
    small manifest when a store object is reopened.

``RadarCycleStore.admit(admission_id, request)``
    Uses the v2 operational parser as the only admission boundary, verifies
    every payload-free capture-set reference against ``RawArtifactStore`` and
    appends its exact canonical bindings.  Repeating the same admission is
    idempotent.  V1 replay is isolated in ``admit_v1_compat``.

``RadarCycleStore.commit_region(..., commit_id=...)``
    Appends one independently committed region revision, advances its head with
    compare-and-swap and writes an immutable operation receipt in the same
    transaction.  Exact retries return the original revision-specific response,
    even when the region head has advanced.  Other regions use separate SQLite
    transactions, so a failed region cannot roll back a successful sibling.

``RadarCycleStore.snapshot(cycle_id)``
    Reads only the cycle, region heads and targets.  Runtime is therefore
    O(regions + targets), independent of artifact, input and revision history.
     The result contains no observations, rasters, polygons, runs or cells.

``parse_region_evaluation(value)``
    Strictly admits the closed region status/evidence vocabulary.  Available
    weather must be fresh and explicit; stale, unknown and error states cannot
    publish a current wet/dry state.

The store never decodes radar.  Wet/dry evidence is a typed processor claim
referencing admitted opaque inputs.  ``dry`` is admitted only when every
referenced input declares complete coverage, the evidence is explicitly fresh
under a supplied age window, and a processor version is present.  Empty,
unknown or incomplete inputs cannot become synthetic dry weather.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Protocol

from .radar_cycle_contract import (
    CoverageStatus,
    MAX_REQUEST_BYTES,
    PrecipitationState,
    RadarCaptureInput,
    RadarCycleRequest,
    RadarCycleRequestV2,
    RadarInput,
    parse_operational_radar_cycle_request,
    parse_radar_cycle_request,
)


STORE_SCHEMA_VERSION = 1
STORE_ALGORITHM_VERSION = "worker-radar-cycle-store-v1"
# Read-only compatibility identity for the unreleased schema-2 set draft.
CAPTURE_SET_SCHEMA_VERSION = 1
# Canonical operational link to raw_artifacts.py schema 3.
NATIVE_CAPTURE_SCHEMA_VERSION = 1
DEFAULT_STORE_PATH = Path(
    os.getenv(
        "STV3_RADAR_CYCLE_STORE",
        "/share/storm_tracker_worker/radar_cycle_store.sqlite3",
    )
)
MAX_DATABASE_BYTES = 512 * 1024 * 1024
MAX_TOTAL_CYCLES = 100_000
MAX_TOTAL_ADMISSIONS = 200_000
MAX_ADMISSIONS_PER_CYCLE = 64
MAX_REVISIONS_PER_REGION = 4096
MAX_COMMIT_RECEIPTS_PER_REGION = 4096
MAX_TOTAL_REGION_COMMITS = 400_000
MAX_CANONICAL_INPUT_BYTES = 8 * 1024
MAX_EVIDENCE_BYTES = 16 * 1024
MAX_REGION_OPERATION_BYTES = 24 * 1024
MAX_SNAPSHOT_BYTES = 96 * 1024
REGION_COMMIT_CONTRACT = "stv3-radar-region-commit-v1"

_CYCLE_ID_RE = re.compile(r"^radar-cycle:[0-9a-f]{64}$")
_INPUT_ID_RE = re.compile(
    r"^(?:radar-input|radar-capture-input):[0-9a-f]{64}$"
)
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+/-]{0,255}$")


class AdmissionConflict(RuntimeError):
    """One admission ID was reused for non-identical cycle material."""


class CommitConflict(RuntimeError):
    """One commit ID was reused for non-identical region operation material."""


class RevisionConflict(RuntimeError):
    """The caller's expected region revision is no longer current."""


class RawCaptureSetMismatch(RuntimeError):
    """A v2 set reference did not match its immutable raw-store receipt."""


class RawCaptureMemberReceiptView(Protocol):
    member_id: str
    role: str
    receipt_id: str
    content_sha256: str
    receipt_sha256: str
    received_at: float


class RawCaptureReceiptView(Protocol):
    capture_id: str
    provider: str
    product: str
    fetch_id: str
    upstream_capture_id: str
    product_timestamp: float | None
    manifest_sha256: str
    raw_store_schema_version: int
    capture_schema: str
    capture_receipt_schema: str
    capture_receipt_sha256: str
    complete: bool
    members: tuple[RawCaptureMemberReceiptView, ...]


class RawArtifactCaptureReader(Protocol):
    def load_capture_receipt(
        self,
        capture_id: str,
    ) -> RawCaptureReceiptView | None: ...


class RegionStatus(str, Enum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    AVAILABLE = "available"
    STALE = "stale"
    ERROR = "error"


class FreshnessStatus(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"


class EvidenceOutcome(str, Enum):
    UNKNOWN = "unknown"
    WET = "wet"
    DRY = "dry"


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _finite(value: object, name: str, *, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric and not boolean")
    number = float(value)
    if not math.isfinite(number) or nonnegative and number < 0.0:
        raise ValueError(f"{name} must be finite" + (" and non-negative" if nonnegative else ""))
    return number


def _identifier(value: object, name: str, pattern: re.Pattern[str] = _ID_RE) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError(f"{name} has invalid canonical syntax")
    return value


def _optional_version(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _identifier(value, name, _VERSION_RE)


def _enum(value: object, enum_type: type[Enum], name: str):
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    try:
        return enum_type(value)
    except ValueError as err:
        raise ValueError(f"unsupported {name}: {value!r}") from err


@dataclass(frozen=True, slots=True)
class RegionWeatherEvidence:
    """Bounded processor evidence; never decoded or interpreted as a raster."""

    input_ids: tuple[str, ...]
    coverage_status: CoverageStatus
    freshness_status: FreshnessStatus
    freshness_max_age_seconds: float | None
    outcome: EvidenceOutcome

    def __post_init__(self) -> None:
        if not isinstance(self.input_ids, tuple):
            raise TypeError("evidence input_ids must be a tuple")
        input_ids = tuple(
            sorted(_identifier(value, "input_id", _INPUT_ID_RE) for value in self.input_ids)
        )
        if len(input_ids) > 48 or len(input_ids) != len(set(input_ids)):
            raise ValueError("evidence input_ids exceed bounds or contain duplicates")
        object.__setattr__(self, "input_ids", input_ids)
        if not isinstance(self.coverage_status, CoverageStatus):
            raise TypeError("coverage_status must be CoverageStatus")
        if not isinstance(self.freshness_status, FreshnessStatus):
            raise TypeError("freshness_status must be FreshnessStatus")
        if not isinstance(self.outcome, EvidenceOutcome):
            raise TypeError("outcome must be EvidenceOutcome")
        maximum_age = self.freshness_max_age_seconds
        if maximum_age is not None:
            maximum_age = _finite(
                maximum_age, "freshness_max_age_seconds", nonnegative=True
            )
        if self.freshness_status is FreshnessStatus.FRESH and maximum_age is None:
            raise ValueError("fresh evidence requires freshness_max_age_seconds")
        if self.freshness_status is not FreshnessStatus.FRESH and maximum_age is not None:
            raise ValueError("only fresh evidence may carry a freshness age window")
        if self.outcome is not EvidenceOutcome.UNKNOWN and not input_ids:
            raise ValueError("wet or dry evidence requires at least one input")
        object.__setattr__(self, "freshness_max_age_seconds", maximum_age)

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_ids": list(self.input_ids),
            "coverage_status": self.coverage_status.value,
            "freshness_status": self.freshness_status.value,
            "freshness_max_age_seconds": self.freshness_max_age_seconds,
            "outcome": self.outcome.value,
        }


@dataclass(frozen=True, slots=True)
class RegionEvaluation:
    status: RegionStatus
    precipitation_state: PrecipitationState
    evaluated_at: float
    processor_version: str | None
    reason_code: str
    evidence: RegionWeatherEvidence | None

    def __post_init__(self) -> None:
        if not isinstance(self.status, RegionStatus):
            raise TypeError("status must be RegionStatus")
        if not isinstance(self.precipitation_state, PrecipitationState):
            raise TypeError("precipitation_state must be PrecipitationState")
        object.__setattr__(
            self, "evaluated_at", _finite(self.evaluated_at, "evaluated_at", nonnegative=True)
        )
        object.__setattr__(
            self,
            "processor_version",
            _optional_version(self.processor_version, "processor_version"),
        )
        object.__setattr__(
            self, "reason_code", _identifier(self.reason_code, "reason_code")
        )
        if self.evidence is not None and not isinstance(
            self.evidence, RegionWeatherEvidence
        ):
            raise TypeError("evidence must be RegionWeatherEvidence or null")
        if self.status is RegionStatus.PENDING:
            if self.precipitation_state is not PrecipitationState.UNKNOWN:
                raise ValueError("pending status cannot claim wet or dry")
            if self.processor_version is not None or self.evidence is not None:
                raise ValueError("pending status cannot claim processor evidence")
        if self.status in {RegionStatus.STALE, RegionStatus.ERROR, RegionStatus.UNKNOWN}:
            if self.precipitation_state is not PrecipitationState.UNKNOWN:
                raise ValueError(f"{self.status.value} status cannot claim current wet or dry")
        if self.status in {RegionStatus.AVAILABLE, RegionStatus.STALE}:
            if self.processor_version is None or self.evidence is None:
                raise ValueError(f"{self.status.value} status requires processor evidence")
        if self.status is RegionStatus.AVAILABLE and self.evidence is not None:
            if self.evidence.freshness_status is not FreshnessStatus.FRESH:
                raise ValueError("available status requires fresh processor evidence")
            if self.precipitation_state is PrecipitationState.UNKNOWN:
                raise ValueError("available status requires an explicit wet or dry result")
        if self.status is RegionStatus.STALE and self.evidence is not None:
            if self.evidence.freshness_status is not FreshnessStatus.STALE:
                raise ValueError("stale status requires stale evidence")
        if self.status in {RegionStatus.UNKNOWN, RegionStatus.ERROR}:
            if self.evidence is not None and self.evidence.outcome is not EvidenceOutcome.UNKNOWN:
                raise ValueError(
                    f"{self.status.value} status cannot expose wet or dry evidence as current"
                )
        if self.status is RegionStatus.AVAILABLE and self.evidence is not None:
            expected = {
                EvidenceOutcome.UNKNOWN: PrecipitationState.UNKNOWN,
                EvidenceOutcome.WET: PrecipitationState.WET,
                EvidenceOutcome.DRY: PrecipitationState.DRY,
            }[self.evidence.outcome]
            if self.precipitation_state is not expected:
                raise ValueError("available precipitation state contradicts evidence outcome")
        if self.precipitation_state is PrecipitationState.DRY:
            if (
                self.status is not RegionStatus.AVAILABLE
                or self.processor_version is None
                or self.evidence is None
                or self.evidence.outcome is not EvidenceOutcome.DRY
                or self.evidence.coverage_status is not CoverageStatus.COMPLETE
                or self.evidence.freshness_status is not FreshnessStatus.FRESH
            ):
                raise ValueError(
                    "dry requires available, fresh, complete explicit processor evidence"
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "precipitation_state": self.precipitation_state.value,
            "evaluated_at": self.evaluated_at,
            "processor_version": self.processor_version,
            "reason_code": self.reason_code,
            "evidence": self.evidence.to_dict() if self.evidence is not None else None,
        }


_EVIDENCE_KEYS = frozenset({
    "input_ids", "coverage_status", "freshness_status",
    "freshness_max_age_seconds", "outcome",
})
_EVALUATION_KEYS = frozenset({
    "status", "precipitation_state", "evaluated_at", "processor_version",
    "reason_code", "evidence",
})


def _exact_mapping(value: object, keys: frozenset[str], name: str) -> dict:
    if type(value) is not dict:
        raise TypeError(f"{name} must be a JSON object")
    if any(not isinstance(key, str) for key in value):
        raise TypeError(f"{name} keys must be strings")
    actual = frozenset(value)
    if actual != keys:
        raise ValueError(
            f"{name} keys do not match; missing={sorted(keys-actual)}, "
            f"extra={sorted(actual-keys)}"
        )
    return value


def parse_region_evaluation(value: object) -> RegionEvaluation:
    if isinstance(value, RegionEvaluation):
        return value
    raw = _exact_mapping(value, _EVALUATION_KEYS, "region evaluation")
    evidence_value = raw["evidence"]
    evidence = None
    if evidence_value is not None:
        item = _exact_mapping(evidence_value, _EVIDENCE_KEYS, "region evidence")
        raw_ids = item["input_ids"]
        if type(raw_ids) is not list:
            raise TypeError("evidence input_ids must be a JSON array")
        evidence = RegionWeatherEvidence(
            input_ids=tuple(raw_ids),
            coverage_status=_enum(
                item["coverage_status"], CoverageStatus, "coverage status"
            ),
            freshness_status=_enum(
                item["freshness_status"], FreshnessStatus, "freshness status"
            ),
            freshness_max_age_seconds=item["freshness_max_age_seconds"],
            outcome=_enum(item["outcome"], EvidenceOutcome, "evidence outcome"),
        )
    return RegionEvaluation(
        status=_enum(raw["status"], RegionStatus, "region status"),
        precipitation_state=_enum(
            raw["precipitation_state"], PrecipitationState, "precipitation state"
        ),
        evaluated_at=raw["evaluated_at"],
        processor_version=raw["processor_version"],
        reason_code=raw["reason_code"],
        evidence=evidence,
    )


def _region_operation(
    cycle_id: str,
    region_id: str,
    expected_revision: int,
    evaluation: RegionEvaluation,
) -> tuple[bytes, str]:
    canonical = _canonical_json({
        "contract": REGION_COMMIT_CONTRACT,
        "cycle_id": cycle_id,
        "region_id": region_id,
        "expected_revision": expected_revision,
        "evaluation": evaluation.to_dict(),
    })
    if len(canonical) > MAX_REGION_OPERATION_BYTES:
        raise ValueError("canonical region operation exceeds byte budget")
    return canonical, hashlib.sha256(canonical).hexdigest()


class RadarCycleStore:
    """Append-only radar-cycle facts with mutable CAS head pointers only."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        raw_artifact_store: RawArtifactCaptureReader | None = None,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
        max_database_bytes: int = MAX_DATABASE_BYTES,
    ) -> None:
        self.path = Path(DEFAULT_STORE_PATH if path is None else path)
        self._raw_artifact_store = raw_artifact_store
        self._clock = clock
        self._fault_injector = fault_injector
        if (
            isinstance(max_database_bytes, bool)
            or not isinstance(max_database_bytes, int)
            or max_database_bytes < 1024 * 1024
        ):
            raise ValueError("max_database_bytes must be at least 1 MiB")
        self._max_database_bytes = max_database_bytes
        self._ready = False

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            self._create_schema(db)
            db.execute("BEGIN IMMEDIATE")
            try:
                expected = {
                    "schema_version": str(STORE_SCHEMA_VERSION),
                    "algorithm_version": STORE_ALGORITHM_VERSION,
                    "capture_set_schema_version": str(
                        CAPTURE_SET_SCHEMA_VERSION
                    ),
                    "native_capture_schema_version": str(
                        NATIVE_CAPTURE_SCHEMA_VERSION
                    ),
                }
                manifest = dict(db.execute("SELECT key,value FROM manifest"))
                if not manifest:
                    db.executemany(
                        "INSERT INTO manifest(key,value) VALUES(?,?)",
                        sorted(expected.items()),
                    )
                else:
                    core = {
                        key: value
                        for key, value in expected.items()
                        if key in {"schema_version", "algorithm_version"}
                    }
                    if any(
                        manifest.get(key) != value
                        for key, value in core.items()
                    ):
                        raise ValueError(
                            "radar cycle store has incompatible manifest"
                        )
                    for key in (
                        "capture_set_schema_version",
                        "native_capture_schema_version",
                    ):
                        actual = manifest.get(key)
                        if actual is None:
                            db.execute(
                                "INSERT INTO manifest(key,value) VALUES(?,?)",
                                (key, expected[key]),
                            )
                        elif actual != expected[key]:
                            raise ValueError(
                                "radar cycle store has incompatible manifest"
                            )
                if db.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                    raise ValueError("radar cycle store failed SQLite quick_check")
                if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                    raise ValueError("radar cycle store has foreign-key violations")
                db.commit()
            except BaseException:
                db.rollback()
                raise
        self._ready = True

    def _ensure_ready(self) -> None:
        """Cheap hot-path readiness check; full integrity audit is explicit."""
        if self._ready:
            return
        if not self.path.exists():
            self.initialize()
            return
        try:
            with closing(self._connect_read_only()) as db:
                manifest = dict(db.execute("SELECT key,value FROM manifest"))
        except sqlite3.OperationalError as err:
            if "no such table" not in str(err).lower():
                raise
            self.initialize()
            return
        expected = {
            "schema_version": str(STORE_SCHEMA_VERSION),
            "algorithm_version": STORE_ALGORITHM_VERSION,
            "capture_set_schema_version": str(CAPTURE_SET_SCHEMA_VERSION),
            "native_capture_schema_version": str(
                NATIVE_CAPTURE_SCHEMA_VERSION
            ),
        }
        if not manifest:
            self.initialize()
            return
        core = {
            key: value
            for key, value in expected.items()
            if key in {"schema_version", "algorithm_version"}
        }
        if any(manifest.get(key) != value for key, value in core.items()):
            raise ValueError("radar cycle store has incompatible manifest")
        for key in (
            "capture_set_schema_version",
            "native_capture_schema_version",
        ):
            actual = manifest.get(key)
            if actual is None:
                self.initialize()
                return
            if actual != expected[key]:
                raise ValueError("radar cycle store has incompatible manifest")
        self._ready = True

    def admit(self, admission_id: str, request: object) -> dict[str, Any]:
        """Operationally admit only a v2 request backed by complete raw sets."""
        parsed = parse_operational_radar_cycle_request(request)
        return self._admit_parsed(admission_id, parsed)

    def admit_v1_compat(
        self,
        admission_id: str,
        request: object,
    ) -> dict[str, Any]:
        """Explicit compatibility write path for replaying legacy v1 facts."""
        parsed = parse_radar_cycle_request(request)
        if not isinstance(parsed, RadarCycleRequest):
            raise ValueError("v1 compatibility admission requires contract v1")
        return self._admit_parsed(admission_id, parsed)

    def _admit_parsed(
        self,
        admission_id: str,
        parsed: RadarCycleRequest | RadarCycleRequestV2,
    ) -> dict[str, Any]:
        """Atomically append one already parsed admission and return snapshot."""
        admission_id = _identifier(admission_id, "admission_id")
        canonical_request = parsed.canonical_bytes
        if len(canonical_request) > MAX_REQUEST_BYTES:  # contract invariant
            raise ValueError("canonical request exceeds admission byte budget")
        now = _finite(self._clock(), "admitted_at", nonnegative=True)
        self._ensure_ready()
        with closing(self._connect_read_only()) as db:
            previous = db.execute(
                "SELECT cycle_id,request_sha256 FROM cycle_admissions "
                "WHERE admission_id=?",
                (admission_id,),
            ).fetchone()
        if previous is not None:
            if (
                previous[0] != parsed.cycle_id
                or previous[1] != parsed.request_sha256
            ):
                raise AdmissionConflict(
                    "admission_id already binds a different request or cycle_id"
                )
            result = self.snapshot(parsed.cycle_id)
            result.update({
                "admission_id": admission_id,
                "already_admitted": True,
                "admission_created": False,
            })
            return result
        if isinstance(parsed, RadarCycleRequestV2):
            self._verify_capture_sets(parsed)
        cycle_created = False
        receipt_created = False
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                previous = db.execute(
                    "SELECT cycle_id,request_sha256 FROM cycle_admissions "
                    "WHERE admission_id=?",
                    (admission_id,),
                ).fetchone()
                if previous is not None:
                    if (
                        previous[0] != parsed.cycle_id
                        or previous[1] != parsed.request_sha256
                    ):
                        raise AdmissionConflict(
                            "admission_id already binds a different request or cycle_id"
                        )
                    db.rollback()
                    result = self.snapshot(parsed.cycle_id)
                    result.update({
                        "admission_id": admission_id,
                        "already_admitted": True,
                        "admission_created": False,
                    })
                    return result

                cycle = db.execute(
                    "SELECT identity_sha256,regions_sha256,targets_sha256,"
                    "inputs_sha256 FROM cycles WHERE cycle_id=?",
                    (parsed.cycle_id,),
                ).fetchone()
                if cycle is None:
                    self._enforce_admission_bounds(db, parsed.cycle_id, new_cycle=True)
                    self._insert_cycle(db, parsed, now)
                    cycle_created = True
                    self._fault("admit.after_cycle")
                else:
                    first = db.execute(
                        "SELECT a.canonical_request FROM cycle_admissions a "
                        "JOIN cycles c ON c.cycle_id=a.cycle_id "
                        "AND c.first_request_sha256=a.request_sha256 "
                        "WHERE c.cycle_id=?",
                        (parsed.cycle_id,),
                    ).fetchone()
                    if first is None:
                        raise AdmissionConflict(
                            "cycle has no exact first admission receipt"
                        )
                    stored_identity = parse_radar_cycle_request(
                        bytes(first[0])
                    ).to_dict()
                    candidate_identity = parsed.to_dict()
                    stored_identity.pop("requested_at")
                    candidate_identity.pop("requested_at")
                    expected = (
                        parsed.identity_sha256,
                        parsed.regions_sha256,
                        parsed.targets_sha256,
                        parsed.inputs_sha256,
                    )
                    if (
                        tuple(cycle) != expected
                        or stored_identity != candidate_identity
                    ):
                        raise AdmissionConflict(
                            "cycle_id digest collision or changed cycle facts"
                        )
                    self._enforce_admission_bounds(db, parsed.cycle_id, new_cycle=False)

                db.execute(
                    "INSERT INTO cycle_admissions VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        admission_id, parsed.cycle_id, parsed.request_sha256,
                        parsed.requested_at, now, sqlite3.Binary(canonical_request),
                        len(canonical_request), parsed.contract,
                        STORE_ALGORITHM_VERSION,
                    ),
                )
                receipt_created = True
                self._fault("admit.after_admission")
                self._fault("admit.before_commit")
                db.commit()
            except BaseException:
                db.rollback()
                raise
        result = self.snapshot(parsed.cycle_id)
        result.update({
            "admission_id": admission_id,
            "already_admitted": not cycle_created,
            "admission_created": receipt_created,
        })
        return result

    def commit_region(
        self,
        cycle_id: str,
        region_id: str,
        *,
        commit_id: str,
        expected_revision: int,
        evaluation: object,
    ) -> dict[str, Any]:
        """Commit once and return the same immutable receipt on exact retries."""
        cycle_id = _identifier(cycle_id, "cycle_id", _CYCLE_ID_RE)
        region_id = _identifier(region_id, "region_id")
        commit_id = _identifier(commit_id, "commit_id")
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 0
        ):
            raise ValueError("expected_revision must be a non-negative integer")
        value = parse_region_evaluation(evaluation)
        canonical_operation, operation_sha256 = _region_operation(
            cycle_id, region_id, expected_revision, value
        )
        now = _finite(self._clock(), "revision_created_at", nonnegative=True)
        self._ensure_ready()
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                receipt = db.execute(
                    "SELECT operation_sha256,canonical_operation,cycle_id,"
                    "region_id,expected_revision FROM region_commit_receipts "
                    "WHERE commit_id=?",
                    (commit_id,),
                ).fetchone()
                if receipt is not None:
                    expected_receipt = (
                        operation_sha256,
                        canonical_operation,
                        cycle_id,
                        region_id,
                        expected_revision,
                    )
                    actual_receipt = (
                        str(receipt[0]),
                        bytes(receipt[1]),
                        str(receipt[2]),
                        str(receipt[3]),
                        int(receipt[4]),
                    )
                    if actual_receipt != expected_receipt:
                        raise CommitConflict(
                            "commit_id already binds a different region operation"
                        )
                    db.rollback()
                    return self._commit_response(
                        commit_id,
                        operation_sha256,
                        canonical_operation,
                    )

                head = db.execute(
                    "SELECT current_revision FROM region_heads "
                    "WHERE cycle_id=? AND region_id=?",
                    (cycle_id, region_id),
                ).fetchone()
                if head is None:
                    raise KeyError("unknown cycle_id or storage region_id")
                current_revision = int(head[0])
                if current_revision != expected_revision:
                    raise RevisionConflict(
                        f"expected revision {expected_revision}, current is {current_revision}"
                    )
                if current_revision >= MAX_REVISIONS_PER_REGION:
                    raise ValueError("region revision bound reached")
                self._enforce_commit_bounds(db, cycle_id, region_id)
                evidence_row = self._validate_evidence(
                    db, cycle_id, region_id, value
                )
                next_revision = current_revision + 1
                db.execute(
                    "INSERT INTO region_revisions VALUES("
                    "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        cycle_id, region_id, next_revision, current_revision,
                        value.status.value, value.precipitation_state.value,
                        value.evaluated_at, value.processor_version,
                        value.reason_code, evidence_row["evidence_json"],
                        evidence_row["evidence_sha256"],
                        evidence_row["input_count"],
                        evidence_row["product_valid_at_min"],
                        evidence_row["product_valid_at_max"],
                        evidence_row["received_at_min"],
                        evidence_row["received_at_max"],
                        now, STORE_ALGORITHM_VERSION,
                        evidence_row["freshness_max_age_seconds"],
                    ),
                )
                self._fault("region.after_revision")
                updated = db.execute(
                    "UPDATE region_heads SET current_revision=?,status=?,"
                    "precipitation_state=?,evaluated_at=?,processor_version=?,"
                    "reason_code=?,evidence_sha256=?,evidence_input_count=?,"
                    "product_valid_at_min=?,product_valid_at_max=?,"
                    "received_at_min=?,received_at_max=? "
                    "WHERE cycle_id=? AND region_id=? AND current_revision=?",
                    (
                        next_revision, value.status.value,
                        value.precipitation_state.value, value.evaluated_at,
                        value.processor_version, value.reason_code,
                        evidence_row["evidence_sha256"], evidence_row["input_count"],
                        evidence_row["product_valid_at_min"],
                        evidence_row["product_valid_at_max"],
                        evidence_row["received_at_min"],
                        evidence_row["received_at_max"],
                        cycle_id, region_id, current_revision,
                    ),
                ).rowcount
                if updated != 1:
                    raise RevisionConflict("region head changed during CAS commit")
                self._fault("region.after_head_cas")
                db.execute(
                    "INSERT INTO region_commit_receipts VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        commit_id,
                        operation_sha256,
                        sqlite3.Binary(canonical_operation),
                        len(canonical_operation),
                        cycle_id,
                        region_id,
                        expected_revision,
                        next_revision,
                        now,
                        STORE_ALGORITHM_VERSION,
                    ),
                )
                self._fault("region.after_receipt")
                self._fault("region.before_commit")
                db.commit()
                self._fault("region.after_commit")
            except BaseException:
                db.rollback()
                raise
        return self._commit_response(
            commit_id,
            operation_sha256,
            canonical_operation,
        )

    def snapshot(self, cycle_id: str) -> dict[str, Any]:
        """Return bounded MQTT-safe state in O(regions + targets)."""
        cycle_id = _identifier(cycle_id, "cycle_id", _CYCLE_ID_RE)
        self._ensure_ready()
        with closing(self._connect_read_only()) as db:
            db.execute("BEGIN")
            cycle = db.execute(
                "SELECT first_request_sha256,identity_sha256,regions_sha256,"
                "targets_sha256,inputs_sha256,first_requested_at,admitted_at,"
                "region_count,target_count,input_count,contract "
                "FROM cycles WHERE cycle_id=?",
                (cycle_id,),
            ).fetchone()
            if cycle is None:
                raise KeyError("unknown cycle_id")
            region_rows = db.execute(
                "SELECT r.region_id,r.center_lat,r.center_lon,r.radius_km,"
                "h.current_revision,h.status,h.precipitation_state,h.evaluated_at,"
                "h.processor_version,h.reason_code,h.evidence_sha256,"
                "h.evidence_input_count,h.product_valid_at_min,"
                "h.product_valid_at_max,h.received_at_min,h.received_at_max "
                "FROM cycle_regions r JOIN region_heads h USING(cycle_id,region_id) "
                "WHERE r.cycle_id=? ORDER BY r.region_id",
                (cycle_id,),
            ).fetchall()
            target_rows = db.execute(
                "SELECT target_id,region_id,latitude,longitude FROM cycle_targets "
                "WHERE cycle_id=? ORDER BY region_id,target_id",
                (cycle_id,),
            ).fetchall()
        targets_by_region: dict[str, list[dict[str, Any]]] = {}
        for target_id, target_region, latitude, longitude in target_rows:
            targets_by_region.setdefault(str(target_region), []).append({
                "target_id": str(target_id),
                "latitude": float(latitude),
                "longitude": float(longitude),
            })
        regions = []
        for row in region_rows:
            region_id = str(row[0])
            regions.append({
                "region_id": region_id,
                "center_lat": float(row[1]),
                "center_lon": float(row[2]),
                "radius_km": float(row[3]),
                "revision": int(row[4]),
                "status": str(row[5]),
                "precipitation_state": str(row[6]),
                "evaluated_at": None if row[7] is None else float(row[7]),
                "processor_version": row[8],
                "reason_code": str(row[9]),
                "evidence_sha256": row[10],
                "evidence_input_count": int(row[11]),
                "product_valid_at_min": row[12],
                "product_valid_at_max": row[13],
                "received_at_min": row[14],
                "received_at_max": row[15],
                "targets": targets_by_region.get(region_id, []),
            })
        result = {
            "method": "worker_radar_cycle_snapshot_v1",
            "cycle_id": cycle_id,
            "request_sha256": str(cycle[0]),
            "identity_sha256": str(cycle[1]),
            "regions_sha256": str(cycle[2]),
            "targets_sha256": str(cycle[3]),
            "inputs_sha256": str(cycle[4]),
            "requested_at": float(cycle[5]),
            "admitted_at": float(cycle[6]),
            "region_count": int(cycle[7]),
            "target_count": int(cycle[8]),
            "input_count": int(cycle[9]),
            "request_contract": str(cycle[10]),
            "capture_set_count": (
                int(cycle[9])
                if str(cycle[10]).endswith("request-v2")
                else 0
            ),
            "regions": regions,
        }
        if len(_canonical_json(result)) > MAX_SNAPSHOT_BYTES:
            raise ValueError("compact radar cycle snapshot exceeds byte budget")
        return result

    def region_snapshot(self, cycle_id: str, region_id: str) -> dict[str, Any]:
        snapshot = self.snapshot(cycle_id)
        for value in snapshot["regions"]:
            if value["region_id"] == region_id:
                return {
                    "method": "worker_radar_cycle_region_snapshot_v1",
                    "cycle_id": cycle_id,
                    **value,
                }
        raise KeyError("unknown storage region_id")

    def _commit_response(
        self,
        commit_id: str,
        operation_sha256: str,
        canonical_operation: bytes,
    ) -> dict[str, Any]:
        """Reconstruct the immutable revision named by an operation receipt."""
        self._ensure_ready()
        with closing(self._connect_read_only()) as db:
            db.execute("BEGIN")
            row = db.execute(
                "SELECT c.operation_sha256,c.canonical_operation,"
                "c.operation_bytes,c.cycle_id,c.region_id,c.expected_revision,"
                "c.committed_revision,c.committed_at,r.center_lat,r.center_lon,"
                "r.radius_km,v.revision,v.status,v.precipitation_state,"
                "v.evaluated_at,v.processor_version,v.reason_code,"
                "v.evidence_sha256,v.evidence_input_count,"
                "v.product_valid_at_min,v.product_valid_at_max,"
                "v.received_at_min,v.received_at_max "
                "FROM region_commit_receipts c "
                "JOIN cycle_regions r ON r.cycle_id=c.cycle_id "
                "AND r.region_id=c.region_id "
                "JOIN region_revisions v ON v.cycle_id=c.cycle_id "
                "AND v.region_id=c.region_id "
                "AND v.revision=c.committed_revision "
                "WHERE c.commit_id=?",
                (commit_id,),
            ).fetchone()
            if row is None:
                raise KeyError("unknown or corrupt region commit receipt")
            stored_canonical = bytes(row[1])
            if (
                str(row[0]) != operation_sha256
                or stored_canonical != canonical_operation
                or int(row[2]) != len(stored_canonical)
                or hashlib.sha256(stored_canonical).hexdigest() != str(row[0])
                or int(row[6]) != int(row[5]) + 1
            ):
                raise CommitConflict("region commit receipt operation checksum mismatch")
            targets = db.execute(
                "SELECT target_id,latitude,longitude FROM cycle_targets "
                "WHERE cycle_id=? AND region_id=? ORDER BY target_id",
                (str(row[3]), str(row[4])),
            ).fetchall()
        result = {
            "method": "worker_radar_cycle_region_commit_v1",
            "commit_id": commit_id,
            "operation_contract": REGION_COMMIT_CONTRACT,
            "operation_sha256": str(row[0]),
            "cycle_id": str(row[3]),
            "region_id": str(row[4]),
            "expected_revision": int(row[5]),
            "committed_revision": int(row[6]),
            "committed_at": float(row[7]),
            "center_lat": float(row[8]),
            "center_lon": float(row[9]),
            "radius_km": float(row[10]),
            "revision": int(row[11]),
            "status": str(row[12]),
            "precipitation_state": str(row[13]),
            "evaluated_at": None if row[14] is None else float(row[14]),
            "processor_version": row[15],
            "reason_code": str(row[16]),
            "evidence_sha256": str(row[17]),
            "evidence_input_count": int(row[18]),
            "product_valid_at_min": row[19],
            "product_valid_at_max": row[20],
            "received_at_min": row[21],
            "received_at_max": row[22],
            "targets": [
                {
                    "target_id": str(target[0]),
                    "latitude": float(target[1]),
                    "longitude": float(target[2]),
                }
                for target in targets
            ],
        }
        if result["revision"] != result["committed_revision"]:
            raise CommitConflict("region commit receipt revision mismatch")
        if len(_canonical_json(result)) > MAX_SNAPSHOT_BYTES:
            raise ValueError("region commit response exceeds byte budget")
        return result

    def _verify_capture_sets(self, request: RadarCycleRequestV2) -> None:
        """Cross-check every bounded v2 reference against a complete raw set."""
        if not request.inputs:
            return
        reader = self._raw_artifact_store
        if reader is None:
            raise RawCaptureSetMismatch(
                "v2 admission requires a schema-3 raw capture reader"
            )
        for radar_input in request.inputs:
            reference = radar_input.capture_set
            receipt = reader.load_capture_receipt(reference.capture_id)
            if receipt is None:
                raise RawCaptureSetMismatch(
                    f"raw capture {reference.capture_id!r} is missing"
                )
            expected_members = tuple(
                (
                    member.member_id,
                    member.role,
                    member.receipt_id,
                    member.content_sha256,
                    member.receipt_sha256,
                    member.received_at,
                )
                for member in reference.members
            )
            actual_members = tuple(
                (
                    member.member_id,
                    member.role,
                    member.receipt_id,
                    member.content_sha256,
                    member.receipt_sha256,
                    member.received_at,
                )
                for member in sorted(
                    receipt.members,
                    key=lambda value: (value.role, value.member_id),
                )
            )
            actual = (
                receipt.capture_id,
                receipt.provider,
                receipt.product,
                receipt.fetch_id,
                receipt.upstream_capture_id,
                receipt.manifest_sha256,
                receipt.product_timestamp,
                receipt.raw_store_schema_version,
                receipt.capture_schema,
                receipt.capture_receipt_schema,
                receipt.capture_receipt_sha256,
                receipt.complete,
                actual_members,
            )
            expected = (
                reference.capture_id,
                radar_input.provider,
                radar_input.product,
                reference.fetch_id,
                reference.upstream_capture_id,
                reference.manifest_sha256,
                reference.product_timestamp,
                reference.provenance.raw_store_schema_version,
                reference.provenance.capture_schema,
                reference.provenance.capture_receipt_schema,
                reference.provenance.capture_receipt_sha256,
                reference.provenance.complete,
                expected_members,
            )
            if actual != expected:
                raise RawCaptureSetMismatch(
                    "v2 capture reference does not exactly match its complete "
                    "RawArtifactStore receipt"
                )

    def _insert_cycle(
        self,
        db: sqlite3.Connection,
        request: RadarCycleRequest | RadarCycleRequestV2,
        now: float,
    ) -> None:
        db.execute(
            "INSERT INTO cycles VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                request.cycle_id, request.identity_sha256, request.contract,
                request.request_sha256, request.regions_sha256,
                request.targets_sha256, request.inputs_sha256,
                request.requested_at, now, len(request.regions),
                len(request.targets), len(request.inputs),
                STORE_ALGORITHM_VERSION, len(request.canonical_bytes),
            ),
        )
        for region in request.regions:
            db.execute(
                "INSERT INTO cycle_regions VALUES(?,?,?,?,?,?,?)",
                (
                    request.cycle_id, region.region_id, region.center_lat,
                    region.center_lon, region.radius_km, now,
                    STORE_ALGORITHM_VERSION,
                ),
            )
            evidence = _canonical_json(None)
            evidence_sha = hashlib.sha256(evidence).hexdigest()
            db.execute(
                "INSERT INTO region_revisions VALUES("
                "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    request.cycle_id, region.region_id, 0, None,
                    RegionStatus.PENDING.value, PrecipitationState.UNKNOWN.value,
                    None, None, "awaiting_region_processor",
                    sqlite3.Binary(evidence), evidence_sha, 0,
                    None, None, None, None, now, STORE_ALGORITHM_VERSION, None,
                ),
            )
            db.execute(
                "INSERT INTO region_heads VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    request.cycle_id, region.region_id, 0,
                    RegionStatus.PENDING.value, PrecipitationState.UNKNOWN.value,
                    None, None, "awaiting_region_processor", evidence_sha, 0,
                    None, None, None, None, STORE_ALGORITHM_VERSION,
                ),
            )
        for target in request.targets:
            db.execute(
                "INSERT INTO cycle_targets VALUES(?,?,?,?,?,?,?)",
                (
                    request.cycle_id, target.target_id, target.region_id,
                    target.latitude, target.longitude, now,
                    STORE_ALGORITHM_VERSION,
                ),
            )
        for radar_input in request.inputs:
            if isinstance(radar_input, RadarCaptureInput):
                self._insert_capture_input(
                    db,
                    request.cycle_id,
                    radar_input,
                    now,
                )
            else:
                self._insert_input(db, request.cycle_id, radar_input, now)
        self._fault("admit.after_inputs")

    def _insert_input(
        self,
        db: sqlite3.Connection,
        cycle_id: str,
        value: RadarInput,
        now: float,
    ) -> None:
        canonical = _canonical_json(value.to_dict())
        if len(canonical) > MAX_CANONICAL_INPUT_BYTES:
            raise ValueError("canonical radar input exceeds byte budget")
        input_sha = value.input_id.split(":", 1)[1]
        coverage = value.coverage.bbox or (None, None, None, None)
        previous = db.execute(
            "SELECT input_sha256,canonical_input FROM radar_inputs WHERE input_id=?",
            (value.input_id,),
        ).fetchone()
        if previous is None:
            db.execute(
                "INSERT INTO radar_inputs VALUES("
                "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    value.input_id, input_sha, sqlite3.Binary(canonical),
                    len(canonical), value.provider, value.product,
                    value.artifact.artifact_path,
                    value.artifact.artifact_sha256,
                    value.artifact.artifact_bytes,
                    value.artifact.content_type,
                    value.provenance.grade.value,
                    value.provenance.artifact_kind.value,
                    int(value.provenance.exact_raw_artifacts_complete),
                    value.provenance.parser_id,
                    value.provenance.upstream_record_id,
                    value.provenance.raw_artifact_receipt_id,
                    value.product_valid_at, value.received_at,
                    value.coverage.status.value, *coverage,
                    now, STORE_ALGORITHM_VERSION,
                ),
            )
        elif previous[0] != input_sha or bytes(previous[1]) != canonical:
            raise AdmissionConflict("input_id digest collision or changed facts")
        db.execute(
            "INSERT INTO cycle_inputs VALUES(?,?,?)",
            (cycle_id, value.input_id, STORE_ALGORITHM_VERSION),
        )
        db.executemany(
            "INSERT INTO cycle_input_regions VALUES(?,?,?,?)",
            (
                (cycle_id, value.input_id, region_id, STORE_ALGORITHM_VERSION)
                for region_id in value.region_ids
            ),
        )

    def _insert_capture_input(
        self,
        db: sqlite3.Connection,
        cycle_id: str,
        value: RadarCaptureInput,
        now: float,
    ) -> None:
        capture = value.capture_set
        canonical_capture = _canonical_json(capture.to_dict())
        canonical_input = _canonical_json(value.to_dict())
        if (
            len(canonical_capture) > MAX_CANONICAL_INPUT_BYTES
            or len(canonical_input) > MAX_CANONICAL_INPUT_BYTES
        ):
            raise ValueError("canonical radar capture input exceeds byte budget")
        capture_sha = capture.capture_ref_id.split(":", 1)[1]
        input_sha = value.input_id.split(":", 1)[1]
        prior_ref = db.execute(
            "SELECT capture_id,capture_ref_sha256,canonical_capture_ref "
            "FROM radar_native_capture_sets WHERE capture_ref_id=?",
            (capture.capture_ref_id,),
        ).fetchone()
        if prior_ref is not None and (
            str(prior_ref[0]) != capture.capture_id
            or str(prior_ref[1]) != capture_sha
            or bytes(prior_ref[2]) != canonical_capture
        ):
            raise AdmissionConflict(
                "capture_ref_id digest collision or changed complete-set facts"
            )
        prior_capture = db.execute(
            "SELECT capture_ref_id,capture_ref_sha256,canonical_capture_ref "
            "FROM radar_native_capture_sets WHERE capture_id=?",
            (capture.capture_id,),
        ).fetchone()
        if prior_capture is None and prior_ref is None:
            received_times = [member.received_at for member in capture.members]
            db.execute(
                "INSERT INTO radar_native_capture_sets "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    capture.capture_ref_id,
                    capture_sha,
                    sqlite3.Binary(canonical_capture),
                    len(canonical_capture),
                    capture.capture_id,
                    capture.fetch_id,
                    capture.upstream_capture_id,
                    capture.manifest_sha256,
                    capture.provenance.capture_receipt_sha256,
                    capture.product_timestamp,
                    min(received_times),
                    max(received_times),
                    len(capture.members),
                    now,
                    STORE_ALGORITHM_VERSION,
                ),
            )
            db.executemany(
                "INSERT INTO radar_native_capture_members "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    (
                        capture.capture_ref_id,
                        member.member_id,
                        member.role,
                        member.receipt_id,
                        member.content_sha256,
                        member.receipt_sha256,
                        member.received_at,
                        now,
                        STORE_ALGORITHM_VERSION,
                    )
                    for member in capture.members
                ),
            )
        elif prior_capture is None or (
            str(prior_capture[0]) != capture.capture_ref_id
            or str(prior_capture[1]) != capture_sha
            or bytes(prior_capture[2]) != canonical_capture
        ):
            raise AdmissionConflict(
                "capture_id digest collision or changed complete-set facts"
            )

        previous = db.execute(
            "SELECT input_sha256,canonical_input "
            "FROM radar_native_capture_inputs "
            "WHERE input_id=?",
            (value.input_id,),
        ).fetchone()
        coverage = value.coverage.bbox or (None, None, None, None)
        if previous is None:
            db.execute(
                "INSERT INTO radar_native_capture_inputs "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    value.input_id,
                    input_sha,
                    sqlite3.Binary(canonical_input),
                    len(canonical_input),
                    value.provider,
                    value.product,
                    capture.capture_ref_id,
                    value.product_valid_at,
                    value.received_at,
                    value.coverage.status.value,
                    *coverage,
                    now,
                    STORE_ALGORITHM_VERSION,
                ),
            )
        elif previous[0] != input_sha or bytes(previous[1]) != canonical_input:
            raise AdmissionConflict("input_id digest collision or changed facts")
        db.execute(
            "INSERT INTO cycle_native_capture_inputs VALUES(?,?,?)",
            (cycle_id, value.input_id, STORE_ALGORITHM_VERSION),
        )
        db.executemany(
            "INSERT INTO cycle_native_capture_input_regions VALUES(?,?,?,?)",
            (
                (cycle_id, value.input_id, region_id, STORE_ALGORITHM_VERSION)
                for region_id in value.region_ids
            ),
        )

    def _validate_evidence(
        self,
        db: sqlite3.Connection,
        cycle_id: str,
        region_id: str,
        evaluation: RegionEvaluation,
    ) -> dict[str, Any]:
        evidence = evaluation.evidence
        if evidence is None:
            encoded = _canonical_json(None)
            return {
                "evidence_json": sqlite3.Binary(encoded),
                "evidence_sha256": hashlib.sha256(encoded).hexdigest(),
                "input_count": 0,
                "product_valid_at_min": None,
                "product_valid_at_max": None,
                "received_at_min": None,
                "received_at_max": None,
                "freshness_max_age_seconds": None,
            }
        encoded = _canonical_json(evidence.to_dict())
        if len(encoded) > MAX_EVIDENCE_BYTES:
            raise ValueError("region weather evidence exceeds byte budget")
        rows = []
        for input_id in evidence.input_ids:
            row = db.execute(
                "SELECT i.product_valid_at,i.received_at,i.coverage_status "
                "FROM radar_inputs i JOIN cycle_input_regions r "
                "ON r.input_id=i.input_id WHERE r.cycle_id=? AND r.region_id=? "
                "AND r.input_id=?",
                (cycle_id, region_id, input_id),
            ).fetchone()
            if row is None:
                row = db.execute(
                    "SELECT i.product_valid_at,i.received_at,i.coverage_status "
                    "FROM radar_native_capture_inputs i "
                    "JOIN cycle_native_capture_input_regions r "
                    "ON r.input_id=i.input_id WHERE r.cycle_id=? "
                    "AND r.region_id=? AND r.input_id=?",
                    (cycle_id, region_id, input_id),
                ).fetchone()
            if row is None:
                raise ValueError("evidence references input outside this cycle region")
            rows.append(row)
        if evidence.coverage_status is CoverageStatus.COMPLETE:
            if not rows or any(row[2] != CoverageStatus.COMPLETE.value for row in rows):
                raise ValueError(
                    "complete evidence requires complete coverage on every input"
                )
        if evaluation.precipitation_state is PrecipitationState.DRY:
            if any(row[2] != CoverageStatus.COMPLETE.value for row in rows):
                raise ValueError("dry cannot use missing, incomplete or unknown coverage")
        if evidence.freshness_status is FreshnessStatus.FRESH:
            maximum_age = float(evidence.freshness_max_age_seconds)
            for product_valid_at, received_at, _status in rows:
                if evaluation.evaluated_at < float(received_at):
                    raise ValueError("fresh evidence cannot predate input receipt")
                if abs(evaluation.evaluated_at - float(product_valid_at)) > maximum_age:
                    raise ValueError("input product lies outside declared freshness window")
        product_times = [float(row[0]) for row in rows]
        received_times = [float(row[1]) for row in rows]
        return {
            "evidence_json": sqlite3.Binary(encoded),
            "evidence_sha256": hashlib.sha256(encoded).hexdigest(),
            "input_count": len(rows),
            "product_valid_at_min": min(product_times) if product_times else None,
            "product_valid_at_max": max(product_times) if product_times else None,
            "received_at_min": min(received_times) if received_times else None,
            "received_at_max": max(received_times) if received_times else None,
            "freshness_max_age_seconds": evidence.freshness_max_age_seconds,
        }

    def _enforce_admission_bounds(
        self,
        db: sqlite3.Connection,
        cycle_id: str,
        *,
        new_cycle: bool,
    ) -> None:
        cycle_count = db.execute("SELECT count(*) FROM cycles").fetchone()[0]
        if new_cycle and cycle_count >= MAX_TOTAL_CYCLES:
            raise ValueError("radar cycle count bound reached")
        admission_count = db.execute(
            "SELECT count(*) FROM cycle_admissions"
        ).fetchone()[0]
        if admission_count >= MAX_TOTAL_ADMISSIONS:
            raise ValueError("radar cycle admission count bound reached")
        if db.execute(
            "SELECT count(*) FROM cycle_admissions WHERE cycle_id=?",
            (cycle_id,),
        ).fetchone()[0] >= MAX_ADMISSIONS_PER_CYCLE:
            raise ValueError("per-cycle admission count bound reached")

    @staticmethod
    def _enforce_commit_bounds(
        db: sqlite3.Connection,
        cycle_id: str,
        region_id: str,
    ) -> None:
        total = db.execute(
            "SELECT count(*) FROM region_commit_receipts"
        ).fetchone()[0]
        if total >= MAX_TOTAL_REGION_COMMITS:
            raise ValueError("region commit receipt count bound reached")
        per_region = db.execute(
            "SELECT count(*) FROM region_commit_receipts "
            "WHERE cycle_id=? AND region_id=?",
            (cycle_id, region_id),
        ).fetchone()[0]
        if per_region >= MAX_COMMIT_RECEIPTS_PER_REGION:
            raise ValueError("per-region commit receipt count bound reached")

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30.0)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA wal_autocheckpoint=256")
            db.execute("PRAGMA journal_size_limit=16777216")
            page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
            max_pages = max(1, self._max_database_bytes // page_size)
            if int(db.execute("PRAGMA page_count").fetchone()[0]) > max_pages:
                raise ValueError("radar cycle store exceeds database byte bound")
            applied = int(db.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0])
            if applied > max_pages:
                raise ValueError("SQLite could not enforce database byte bound")
            return db
        except BaseException:
            db.close()
            raise

    def _connect_read_only(self) -> sqlite3.Connection:
        db = sqlite3.connect(
            self.path.resolve().as_uri() + "?mode=ro",
            uri=True,
            timeout=30.0,
        )
        try:
            db.execute("PRAGMA query_only=ON")
            db.execute("PRAGMA foreign_keys=ON")
            return db
        except BaseException:
            db.close()
            raise

    @staticmethod
    def _create_schema(db: sqlite3.Connection) -> None:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS manifest(
                key TEXT PRIMARY KEY,value TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS cycles(
                cycle_id TEXT PRIMARY KEY,identity_sha256 TEXT NOT NULL,
                contract TEXT NOT NULL,first_request_sha256 TEXT NOT NULL,
                regions_sha256 TEXT NOT NULL,targets_sha256 TEXT NOT NULL,
                inputs_sha256 TEXT NOT NULL,first_requested_at REAL NOT NULL,
                admitted_at REAL NOT NULL,region_count INTEGER NOT NULL,
                target_count INTEGER NOT NULL,input_count INTEGER NOT NULL,
                algorithm_version TEXT NOT NULL,canonical_request_bytes INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS cycle_admissions(
                admission_id TEXT PRIMARY KEY,cycle_id TEXT NOT NULL,
                request_sha256 TEXT NOT NULL,requested_at REAL NOT NULL,
                admitted_at REAL NOT NULL,canonical_request BLOB NOT NULL,
                request_bytes INTEGER NOT NULL,contract TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                FOREIGN KEY(cycle_id) REFERENCES cycles(cycle_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS admissions_cycle
                ON cycle_admissions(cycle_id,admitted_at);
            CREATE TABLE IF NOT EXISTS cycle_regions(
                cycle_id TEXT NOT NULL,region_id TEXT NOT NULL,
                center_lat REAL NOT NULL,center_lon REAL NOT NULL,
                radius_km REAL NOT NULL,created_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(cycle_id,region_id),
                FOREIGN KEY(cycle_id) REFERENCES cycles(cycle_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS cycle_targets(
                cycle_id TEXT NOT NULL,target_id TEXT NOT NULL,region_id TEXT NOT NULL,
                latitude REAL NOT NULL,longitude REAL NOT NULL,created_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(cycle_id,target_id),
                FOREIGN KEY(cycle_id,region_id)
                    REFERENCES cycle_regions(cycle_id,region_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS targets_region
                ON cycle_targets(cycle_id,region_id,target_id);
            CREATE TABLE IF NOT EXISTS radar_inputs(
                input_id TEXT PRIMARY KEY,input_sha256 TEXT NOT NULL,
                canonical_input BLOB NOT NULL,canonical_input_bytes INTEGER NOT NULL,
                provider TEXT NOT NULL,product TEXT NOT NULL,
                artifact_path TEXT NOT NULL,artifact_sha256 TEXT NOT NULL,
                artifact_bytes INTEGER NOT NULL,content_type TEXT NOT NULL,
                provenance_grade TEXT NOT NULL,artifact_kind TEXT NOT NULL,
                exact_raw_artifacts_complete INTEGER NOT NULL,
                parser_id TEXT,upstream_record_id TEXT,raw_artifact_receipt_id TEXT,
                product_valid_at REAL NOT NULL,received_at REAL NOT NULL,
                coverage_status TEXT NOT NULL,coverage_west REAL,coverage_south REAL,
                coverage_east REAL,coverage_north REAL,created_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS cycle_inputs(
                cycle_id TEXT NOT NULL,input_id TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(cycle_id,input_id),
                FOREIGN KEY(cycle_id) REFERENCES cycles(cycle_id),
                FOREIGN KEY(input_id) REFERENCES radar_inputs(input_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS cycle_input_regions(
                cycle_id TEXT NOT NULL,input_id TEXT NOT NULL,region_id TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(cycle_id,input_id,region_id),
                FOREIGN KEY(cycle_id,input_id)
                    REFERENCES cycle_inputs(cycle_id,input_id),
                FOREIGN KEY(cycle_id,region_id)
                    REFERENCES cycle_regions(cycle_id,region_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS radar_capture_sets(
                capture_ref_id TEXT PRIMARY KEY,capture_ref_sha256 TEXT NOT NULL,
                canonical_capture_ref BLOB NOT NULL,
                canonical_capture_ref_bytes INTEGER NOT NULL,
                capture_id TEXT NOT NULL UNIQUE,upstream_set_id TEXT NOT NULL,
                manifest_sha256 TEXT NOT NULL,provenance_sha256 TEXT NOT NULL,
                set_receipt_sha256 TEXT NOT NULL,product_valid_at REAL NOT NULL,
                received_at_min REAL NOT NULL,received_at_max REAL NOT NULL,
                member_count INTEGER NOT NULL,created_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS radar_capture_members(
                capture_ref_id TEXT NOT NULL,member_id TEXT NOT NULL,
                role TEXT NOT NULL,receipt_id TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,receipt_sha256 TEXT NOT NULL,
                received_at REAL NOT NULL,created_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(capture_ref_id,member_id),
                UNIQUE(capture_ref_id,role),
                UNIQUE(capture_ref_id,receipt_id),
                FOREIGN KEY(capture_ref_id)
                    REFERENCES radar_capture_sets(capture_ref_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS radar_capture_inputs(
                input_id TEXT PRIMARY KEY,input_sha256 TEXT NOT NULL,
                canonical_input BLOB NOT NULL,canonical_input_bytes INTEGER NOT NULL,
                provider TEXT NOT NULL,product TEXT NOT NULL,
                capture_ref_id TEXT NOT NULL,product_valid_at REAL NOT NULL,
                received_at REAL NOT NULL,coverage_status TEXT NOT NULL,
                coverage_west REAL,coverage_south REAL,coverage_east REAL,
                coverage_north REAL,created_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL,
                FOREIGN KEY(capture_ref_id)
                    REFERENCES radar_capture_sets(capture_ref_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS cycle_capture_inputs(
                cycle_id TEXT NOT NULL,input_id TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(cycle_id,input_id),
                FOREIGN KEY(cycle_id) REFERENCES cycles(cycle_id),
                FOREIGN KEY(input_id) REFERENCES radar_capture_inputs(input_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS cycle_capture_input_regions(
                cycle_id TEXT NOT NULL,input_id TEXT NOT NULL,region_id TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(cycle_id,input_id,region_id),
                FOREIGN KEY(cycle_id,input_id)
                    REFERENCES cycle_capture_inputs(cycle_id,input_id),
                FOREIGN KEY(cycle_id,region_id)
                    REFERENCES cycle_regions(cycle_id,region_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS radar_native_capture_sets(
                capture_ref_id TEXT PRIMARY KEY,capture_ref_sha256 TEXT NOT NULL,
                canonical_capture_ref BLOB NOT NULL,
                canonical_capture_ref_bytes INTEGER NOT NULL,
                capture_id TEXT NOT NULL UNIQUE,fetch_id TEXT NOT NULL,
                upstream_capture_id TEXT NOT NULL,manifest_sha256 TEXT NOT NULL,
                capture_receipt_sha256 TEXT NOT NULL,
                product_timestamp REAL NOT NULL,
                received_at_min REAL NOT NULL,received_at_max REAL NOT NULL,
                member_count INTEGER NOT NULL,created_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS radar_native_capture_members(
                capture_ref_id TEXT NOT NULL,member_id TEXT NOT NULL,
                role TEXT NOT NULL,receipt_id TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,receipt_sha256 TEXT NOT NULL,
                received_at REAL NOT NULL,created_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(capture_ref_id,member_id),
                UNIQUE(capture_ref_id,role),
                UNIQUE(capture_ref_id,receipt_id),
                FOREIGN KEY(capture_ref_id)
                    REFERENCES radar_native_capture_sets(capture_ref_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS radar_native_capture_inputs(
                input_id TEXT PRIMARY KEY,input_sha256 TEXT NOT NULL,
                canonical_input BLOB NOT NULL,canonical_input_bytes INTEGER NOT NULL,
                provider TEXT NOT NULL,product TEXT NOT NULL,
                capture_ref_id TEXT NOT NULL,product_valid_at REAL NOT NULL,
                received_at REAL NOT NULL,coverage_status TEXT NOT NULL,
                coverage_west REAL,coverage_south REAL,coverage_east REAL,
                coverage_north REAL,created_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL,
                FOREIGN KEY(capture_ref_id)
                    REFERENCES radar_native_capture_sets(capture_ref_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS cycle_native_capture_inputs(
                cycle_id TEXT NOT NULL,input_id TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(cycle_id,input_id),
                FOREIGN KEY(cycle_id) REFERENCES cycles(cycle_id),
                FOREIGN KEY(input_id)
                    REFERENCES radar_native_capture_inputs(input_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS cycle_native_capture_input_regions(
                cycle_id TEXT NOT NULL,input_id TEXT NOT NULL,region_id TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(cycle_id,input_id,region_id),
                FOREIGN KEY(cycle_id,input_id)
                    REFERENCES cycle_native_capture_inputs(cycle_id,input_id),
                FOREIGN KEY(cycle_id,region_id)
                    REFERENCES cycle_regions(cycle_id,region_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS region_revisions(
                cycle_id TEXT NOT NULL,region_id TEXT NOT NULL,revision INTEGER NOT NULL,
                previous_revision INTEGER,status TEXT NOT NULL,
                precipitation_state TEXT NOT NULL,evaluated_at REAL,
                processor_version TEXT,reason_code TEXT NOT NULL,
                evidence_json BLOB NOT NULL,evidence_sha256 TEXT NOT NULL,
                evidence_input_count INTEGER NOT NULL,product_valid_at_min REAL,
                product_valid_at_max REAL,received_at_min REAL,received_at_max REAL,
                created_at REAL NOT NULL,algorithm_version TEXT NOT NULL,
                freshness_max_age_seconds REAL,
                PRIMARY KEY(cycle_id,region_id,revision),
                FOREIGN KEY(cycle_id,region_id)
                    REFERENCES cycle_regions(cycle_id,region_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS region_commit_receipts(
                commit_id TEXT PRIMARY KEY,operation_sha256 TEXT NOT NULL,
                canonical_operation BLOB NOT NULL,operation_bytes INTEGER NOT NULL
                    CHECK(operation_bytes BETWEEN 1 AND 24576),
                cycle_id TEXT NOT NULL,region_id TEXT NOT NULL,
                expected_revision INTEGER NOT NULL CHECK(expected_revision >= 0),
                committed_revision INTEGER NOT NULL,
                committed_at REAL NOT NULL,algorithm_version TEXT NOT NULL,
                CHECK(length(operation_sha256) = 64),
                CHECK(committed_revision = expected_revision + 1),
                UNIQUE(cycle_id,region_id,committed_revision),
                FOREIGN KEY(cycle_id,region_id,committed_revision)
                    REFERENCES region_revisions(cycle_id,region_id,revision)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS region_commit_receipts_region
                ON region_commit_receipts(cycle_id,region_id,committed_revision);
            CREATE TABLE IF NOT EXISTS region_heads(
                cycle_id TEXT NOT NULL,region_id TEXT NOT NULL,
                current_revision INTEGER NOT NULL,status TEXT NOT NULL,
                precipitation_state TEXT NOT NULL,evaluated_at REAL,
                processor_version TEXT,reason_code TEXT NOT NULL,
                evidence_sha256 TEXT NOT NULL,evidence_input_count INTEGER NOT NULL,
                product_valid_at_min REAL,product_valid_at_max REAL,
                received_at_min REAL,received_at_max REAL,
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(cycle_id,region_id),
                FOREIGN KEY(cycle_id,region_id,current_revision)
                    REFERENCES region_revisions(cycle_id,region_id,revision)
            ) WITHOUT ROWID;
        """)
        append_only = (
            "cycles", "cycle_admissions", "cycle_regions", "cycle_targets",
            "radar_inputs", "cycle_inputs", "cycle_input_regions",
            "radar_capture_sets", "radar_capture_members",
            "radar_capture_inputs", "cycle_capture_inputs",
            "cycle_capture_input_regions",
            "radar_native_capture_sets", "radar_native_capture_members",
            "radar_native_capture_inputs", "cycle_native_capture_inputs",
            "cycle_native_capture_input_regions",
            "region_revisions", "region_commit_receipts",
        )
        for table in append_only:
            db.executescript(f"""
                CREATE TRIGGER IF NOT EXISTS {table}_no_update
                BEFORE UPDATE ON {table} BEGIN
                    SELECT RAISE(ABORT,'append-only table');
                END;
                CREATE TRIGGER IF NOT EXISTS {table}_no_delete
                BEFORE DELETE ON {table} BEGIN
                    SELECT RAISE(ABORT,'append-only table');
                END;
            """)


__all__ = [
    "AdmissionConflict",
    "CAPTURE_SET_SCHEMA_VERSION",
    "CommitConflict",
    "DEFAULT_STORE_PATH",
    "EvidenceOutcome",
    "FreshnessStatus",
    "MAX_ADMISSIONS_PER_CYCLE",
    "MAX_CANONICAL_INPUT_BYTES",
    "MAX_COMMIT_RECEIPTS_PER_REGION",
    "MAX_DATABASE_BYTES",
    "MAX_EVIDENCE_BYTES",
    "MAX_REGION_OPERATION_BYTES",
    "MAX_REVISIONS_PER_REGION",
    "MAX_SNAPSHOT_BYTES",
    "MAX_TOTAL_ADMISSIONS",
    "MAX_TOTAL_CYCLES",
    "MAX_TOTAL_REGION_COMMITS",
    "NATIVE_CAPTURE_SCHEMA_VERSION",
    "RadarCycleStore",
    "RawArtifactCaptureReader",
    "RawCaptureMemberReceiptView",
    "RawCaptureReceiptView",
    "RawCaptureSetMismatch",
    "RegionEvaluation",
    "RegionStatus",
    "RegionWeatherEvidence",
    "REGION_COMMIT_CONTRACT",
    "RevisionConflict",
    "STORE_ALGORITHM_VERSION",
    "STORE_SCHEMA_VERSION",
    "parse_region_evaluation",
]
