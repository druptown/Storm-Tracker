"""Bounded durable attempt and terminal receipts for shadow acquisition."""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable


CONTRACT = "stv3-worker-acquisition-execution-receipt-v2"
SCHEMA_VERSION = 1
MAX_RECEIPTS = 65_536
MAX_DATABASE_BYTES = 64 * 1024 * 1024
MAX_DIAGNOSTIC_BYTES = 4096
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_.:/-]{0,191}$")
_REASON = re.compile(r"^[a-z][a-z0-9_]{0,95}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_OUTCOMES = frozenset({
    "blocked_permanent",
    "completed_shadow_raw",
    "completion_replayed",
    "retryable_failure",
})


class ExecutionReceiptError(RuntimeError):
    pass


class ExecutionReceiptConflict(ExecutionReceiptError):
    pass


class ExecutionReceiptCapacityError(ExecutionReceiptError):
    pass


@dataclass(frozen=True, slots=True)
class ExecutionReceipt:
    receipt_sha256: str
    scheduler_job_id: str
    acquisition_job_id: str
    binding_sha256: str
    attempt_count: int
    outcome: str
    reason_code: str
    terminal: bool
    content_sha256: str | None
    raw_receipt_sha256: str | None
    capture_sha256: str | None
    capacity_commit_sha256: str | None
    created_at: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "attempt_count": self.attempt_count,
            "binding_sha256": self.binding_sha256,
            "capacity_commit_sha256": self.capacity_commit_sha256,
            "capture_sha256": self.capture_sha256,
            "content_sha256": self.content_sha256,
            "contract": CONTRACT,
            "created_at": self.created_at,
            "outcome": self.outcome,
            "raw_receipt_sha256": self.raw_receipt_sha256,
            "reason_code": self.reason_code,
            "receipt_sha256": self.receipt_sha256,
            "scheduler_job_id": self.scheduler_job_id,
            "terminal": self.terminal,
        }


@dataclass(frozen=True, slots=True)
class TerminalReceiptPage:
    """One bounded, insertion-ordered terminal inventory page."""

    after_cursor: int
    next_cursor: int
    cursors: tuple[int, ...]
    receipts: tuple[ExecutionReceipt, ...]
    exhausted: bool


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ExecutionReceiptError(f"invalid {name}")
    return value


def _sha(value: object, name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ExecutionReceiptError(f"invalid {name}")
    return value


def _timestamp(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ExecutionReceiptError("invalid receipt timestamp")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 2**53:
        raise ExecutionReceiptError("invalid receipt timestamp")
    return result


class ExecutionReceiptStore:
    """SQLite receipt log with one immutable terminal head per acquisition job."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], float] = time.time,
        max_receipts: int = MAX_RECEIPTS,
        max_database_bytes: int = MAX_DATABASE_BYTES,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError("execution receipt path must be absolute")
        if isinstance(max_receipts, bool) or not 1 <= max_receipts <= MAX_RECEIPTS:
            raise ValueError("invalid execution receipt capacity")
        if (
            isinstance(max_database_bytes, bool)
            or not 1024 * 1024 <= max_database_bytes <= MAX_DATABASE_BYTES
        ):
            raise ValueError("invalid execution receipt database bound")
        self._clock = clock
        self._max_receipts = max_receipts
        self._max_database_bytes = max_database_bytes
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=2.0)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        mode = str(db.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower()
        if mode != "delete":
            db.close()
            raise ExecutionReceiptError("execution receipt journal mode drift")
        page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
        maximum_pages = max(1, self._max_database_bytes // page_size)
        db.execute(f"PRAGMA max_page_count={maximum_pages}")
        return db

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def initialize(self) -> None:
        if self.path.exists() and self.path.is_symlink():
            raise ExecutionReceiptError("execution receipt path is a symlink")
        with self._lock, closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS execution_schema(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    schema_version INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS execution_receipts(
                    receipt_sha256 TEXT PRIMARY KEY,
                    scheduler_job_id TEXT NOT NULL,
                    acquisition_job_id TEXT NOT NULL,
                    binding_sha256 TEXT NOT NULL UNIQUE,
                    attempt_count INTEGER NOT NULL,
                    outcome TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    terminal INTEGER NOT NULL CHECK(terminal IN (0,1)),
                    content_sha256 TEXT,
                    raw_receipt_sha256 TEXT,
                    capture_sha256 TEXT,
                    capacity_commit_sha256 TEXT,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS terminal_heads(
                    acquisition_job_id TEXT PRIMARY KEY,
                    receipt_sha256 TEXT NOT NULL UNIQUE REFERENCES
                        execution_receipts(receipt_sha256)
                );
                """
            )
            db.execute(
                "INSERT OR IGNORE INTO execution_schema VALUES(1,?)",
                (SCHEMA_VERSION,),
            )
            row = db.execute(
                "SELECT schema_version FROM execution_schema WHERE singleton=1"
            ).fetchone()
            if row is None or int(row[0]) != SCHEMA_VERSION:
                raise ExecutionReceiptError("execution receipt schema mismatch")
            db.commit()

    @staticmethod
    def _from_row(row: sqlite3.Row) -> ExecutionReceipt:
        receipt = ExecutionReceipt(
            receipt_sha256=str(row["receipt_sha256"]),
            scheduler_job_id=str(row["scheduler_job_id"]),
            acquisition_job_id=str(row["acquisition_job_id"]),
            binding_sha256=str(row["binding_sha256"]),
            attempt_count=int(row["attempt_count"]),
            outcome=str(row["outcome"]),
            reason_code=str(row["reason_code"]),
            terminal=bool(row["terminal"]),
            content_sha256=row["content_sha256"],
            raw_receipt_sha256=row["raw_receipt_sha256"],
            capture_sha256=row["capture_sha256"],
            capacity_commit_sha256=row["capacity_commit_sha256"],
            created_at=float(row["created_at"]),
        )
        payload = receipt.as_dict()
        payload.pop("receipt_sha256")
        expected = hashlib.sha256(_canonical(payload)).hexdigest()
        if expected != receipt.receipt_sha256:
            raise ExecutionReceiptError("execution receipt checksum mismatch")
        return receipt

    def record(
        self,
        *,
        scheduler_job_id: str,
        acquisition_job_id: str,
        binding_sha256: str,
        attempt_count: int,
        outcome: str,
        reason_code: str,
        terminal: bool,
        content_sha256: str | None = None,
        raw_receipt_sha256: str | None = None,
        capture_sha256: str | None = None,
        capacity_commit_sha256: str | None = None,
    ) -> ExecutionReceipt:
        scheduler_job = _identifier(scheduler_job_id, "scheduler job ID")
        acquisition_job = _identifier(acquisition_job_id, "acquisition job ID")
        binding = _sha(binding_sha256, "binding SHA-256")
        # Zero is reserved for a scheduler slot that reached its deadline
        # before it ever obtained a provider-execution lease.
        if isinstance(attempt_count, bool) or not 0 <= attempt_count <= 1_000_000:
            raise ExecutionReceiptError("invalid attempt count")
        if outcome not in _OUTCOMES:
            raise ExecutionReceiptError("invalid execution outcome")
        if not isinstance(reason_code, str) or _REASON.fullmatch(reason_code) is None:
            raise ExecutionReceiptError("invalid execution reason")
        if type(terminal) is not bool:
            raise ExecutionReceiptError("terminal must be boolean")
        if terminal != (outcome in {"blocked_permanent", "completed_shadow_raw"}):
            raise ExecutionReceiptError("terminal flag conflicts with outcome")
        hashes = (
            _sha(content_sha256, "content SHA-256", optional=True),
            _sha(raw_receipt_sha256, "raw receipt SHA-256", optional=True),
            _sha(capture_sha256, "capture SHA-256", optional=True),
            _sha(capacity_commit_sha256, "capacity commit SHA-256", optional=True),
        )
        if outcome == "completed_shadow_raw" and any(value is None for value in hashes):
            raise ExecutionReceiptError("successful raw receipt lacks evidence hashes")
        if outcome != "completed_shadow_raw" and any(value is not None for value in hashes):
            raise ExecutionReceiptError("non-success receipt may not claim raw evidence")
        created_at = _timestamp(self._clock())
        payload = {
            "acquisition_job_id": acquisition_job,
            "attempt_count": attempt_count,
            "binding_sha256": binding,
            "capacity_commit_sha256": hashes[3],
            "capture_sha256": hashes[2],
            "content_sha256": hashes[0],
            "contract": CONTRACT,
            "created_at": created_at,
            "outcome": outcome,
            "raw_receipt_sha256": hashes[1],
            "reason_code": reason_code,
            "scheduler_job_id": scheduler_job,
            "terminal": terminal,
        }
        receipt_sha = hashlib.sha256(_canonical(payload)).hexdigest()
        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                existing = db.execute(
                    "SELECT * FROM execution_receipts WHERE binding_sha256=?",
                    (binding,),
                ).fetchone()
                if existing is not None:
                    receipt = self._from_row(existing)
                    # created_at belongs to the first durable publication, not
                    # to a later replay of the same immutable attempt input.
                    comparable = receipt.as_dict()
                    comparable.pop("receipt_sha256")
                    comparable.pop("created_at")
                    replay = dict(payload)
                    replay.pop("created_at")
                    if comparable != replay:
                        raise ExecutionReceiptConflict("binding receipt conflicts")
                    db.commit()
                    return receipt
                count = int(db.execute("SELECT count(*) FROM execution_receipts").fetchone()[0])
                if count >= self._max_receipts:
                    raise ExecutionReceiptCapacityError("execution receipt capacity exhausted")
                if terminal:
                    head = db.execute(
                        "SELECT receipt_sha256 FROM terminal_heads WHERE acquisition_job_id=?",
                        (acquisition_job,),
                    ).fetchone()
                    if head is not None:
                        prior = db.execute(
                            "SELECT * FROM execution_receipts WHERE receipt_sha256=?",
                            (head[0],),
                        ).fetchone()
                        prior_receipt = self._from_row(prior)
                        if (
                            prior_receipt.outcome != outcome
                            or prior_receipt.reason_code != reason_code
                            or prior_receipt.content_sha256 != hashes[0]
                            or prior_receipt.raw_receipt_sha256 != hashes[1]
                            or prior_receipt.capture_sha256 != hashes[2]
                            or prior_receipt.capacity_commit_sha256 != hashes[3]
                        ):
                            raise ExecutionReceiptConflict("terminal head conflicts")
                        db.commit()
                        return prior_receipt
                db.execute(
                    "INSERT INTO execution_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        receipt_sha, scheduler_job, acquisition_job, binding,
                        attempt_count, outcome, reason_code, int(terminal),
                        hashes[0], hashes[1], hashes[2], hashes[3], created_at,
                    ),
                )
                self._fault("record.after_receipt")
                if terminal:
                    db.execute(
                        "INSERT INTO terminal_heads VALUES(?,?)",
                        (acquisition_job, receipt_sha),
                    )
                    self._fault("record.after_terminal_head")
                self._fault("record.before_commit")
                db.commit()
            except sqlite3.OperationalError as err:
                if db.in_transaction:
                    db.rollback()
                if "full" in str(err).lower():
                    raise ExecutionReceiptCapacityError("execution receipt database full") from err
                raise
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        return ExecutionReceipt(receipt_sha, scheduler_job, acquisition_job, binding,
                                attempt_count, outcome, reason_code, terminal,
                                hashes[0], hashes[1], hashes[2], hashes[3], created_at)

    def load_terminal(self, acquisition_job_id: str) -> ExecutionReceipt | None:
        job = _identifier(acquisition_job_id, "acquisition job ID")
        with self._lock, closing(self._connect()) as db:
            row = db.execute(
                "SELECT r.* FROM terminal_heads h JOIN execution_receipts r "
                "ON r.receipt_sha256=h.receipt_sha256 WHERE h.acquisition_job_id=?",
                (job,),
            ).fetchone()
        return None if row is None else self._from_row(row)

    def terminal_receipts(self) -> tuple[ExecutionReceipt, ...]:
        """Return the complete bounded terminal inventory in stable order.

        The worker-first source-to-completion bridge uses this inventory to
        recover a crash after scheduler/execution terminalisation but before
        lifecycle publication.  Rows are checksum verified individually and
        every terminal head must point at exactly one returned terminal row.
        """

        with self._lock, closing(self._connect()) as db:
            rows = db.execute(
                "SELECT r.* FROM terminal_heads h "
                "JOIN execution_receipts r "
                "ON r.receipt_sha256=h.receipt_sha256 "
                "ORDER BY r.created_at,r.acquisition_job_id "
                "LIMIT ?",
                (self._max_receipts + 1,),
            ).fetchall()
            head_count = int(
                db.execute("SELECT count(*) FROM terminal_heads").fetchone()[0]
            )
        if len(rows) > self._max_receipts or len(rows) != head_count:
            raise ExecutionReceiptCapacityError(
                "terminal execution inventory exceeds its bound"
            )
        receipts = tuple(self._from_row(row) for row in rows)
        if (
            any(not item.terminal for item in receipts)
            or len({item.acquisition_job_id for item in receipts})
            != len(receipts)
            or len({item.receipt_sha256 for item in receipts}) != len(receipts)
        ):
            raise ExecutionReceiptError("terminal execution inventory is corrupt")
        return receipts

    def terminal_receipt_page(
        self,
        *,
        after_cursor: int = 0,
        max_rows: int = 64,
    ) -> TerminalReceiptPage:
        """Read a bounded page using the append-only terminal-head rowid.

        The cursor is an in-process scan aid, not evidence.  A restarted bridge
        deliberately begins again at zero and revalidates lifecycle presence,
        which closes the crash window without trusting a separate cursor store.
        """

        if (
            isinstance(after_cursor, bool)
            or not isinstance(after_cursor, int)
            or after_cursor < 0
        ):
            raise ValueError("terminal inventory cursor is invalid")
        if (
            isinstance(max_rows, bool)
            or not isinstance(max_rows, int)
            or not 1 <= max_rows <= 1024
        ):
            raise ValueError("terminal inventory page size is invalid")
        with self._lock, closing(self._connect()) as db:
            rows = db.execute(
                "SELECT h.rowid AS terminal_cursor,r.* "
                "FROM terminal_heads h JOIN execution_receipts r "
                "ON r.receipt_sha256=h.receipt_sha256 "
                "WHERE h.rowid>? ORDER BY h.rowid LIMIT ?",
                (after_cursor, max_rows + 1),
            ).fetchall()
        exhausted = len(rows) <= max_rows
        selected = rows[:max_rows]
        receipts = tuple(self._from_row(row) for row in selected)
        cursors = tuple(int(row["terminal_cursor"]) for row in selected)
        if (
            any(not item.terminal for item in receipts)
            or any(cursor <= after_cursor for cursor in cursors)
            or tuple(sorted(cursors)) != cursors
            or len(set(cursors)) != len(cursors)
        ):
            raise ExecutionReceiptError("terminal inventory page is corrupt")
        return TerminalReceiptPage(
            after_cursor=after_cursor,
            next_cursor=after_cursor if not cursors else cursors[-1],
            cursors=cursors,
            receipts=receipts,
            exhausted=exhausted,
        )

    def load_job_receipts(
        self, acquisition_job_id: str,
    ) -> tuple[ExecutionReceipt, ...]:
        """Return every checksum-verified attempt receipt for one job.

        Retention/outbox code needs the complete immutable attempt history, not
        merely the terminal head.  This read remains bounded by the store's
        hard receipt limit and cross-checks that a terminal head, when present,
        is both part of the returned history and the sole terminal receipt.
        """

        job = _identifier(acquisition_job_id, "acquisition job ID")
        with self._lock, closing(self._connect()) as db:
            rows = db.execute(
                "SELECT * FROM execution_receipts WHERE acquisition_job_id=? "
                "ORDER BY attempt_count,created_at,receipt_sha256 LIMIT ?",
                (job, self._max_receipts + 1),
            ).fetchall()
            if len(rows) > self._max_receipts:
                raise ExecutionReceiptCapacityError(
                    "job receipt history exceeds store bound"
                )
            head = db.execute(
                "SELECT receipt_sha256 FROM terminal_heads "
                "WHERE acquisition_job_id=?",
                (job,),
            ).fetchone()
        receipts = tuple(self._from_row(row) for row in rows)
        terminals = tuple(item for item in receipts if item.terminal)
        if head is None:
            if terminals:
                raise ExecutionReceiptError(
                    "terminal execution receipt has no terminal head"
                )
        elif (
            len(terminals) != 1
            or terminals[0].receipt_sha256 != str(head[0])
        ):
            raise ExecutionReceiptError(
                "terminal execution head differs from job receipt history"
            )
        return receipts

    def stats(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as db:
            counts = {
                str(row[0]): int(row[1])
                for row in db.execute(
                    "SELECT outcome,count(*) FROM execution_receipts GROUP BY outcome"
                )
            }
            total = int(db.execute("SELECT count(*) FROM execution_receipts").fetchone()[0])
            terminal = int(db.execute("SELECT count(*) FROM terminal_heads").fetchone()[0])
            page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
        result = {
            "contract": CONTRACT,
            "database_bytes": page_size * page_count,
            "max_receipts": self._max_receipts,
            "outcomes": {name: counts.get(name, 0) for name in sorted(_OUTCOMES)},
            "receipt_count": total,
            "terminal_count": terminal,
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise ExecutionReceiptError("execution receipt diagnostics exceed bound")
        return result


__all__ = [
    "CONTRACT",
    "ExecutionReceipt",
    "ExecutionReceiptCapacityError",
    "ExecutionReceiptConflict",
    "ExecutionReceiptError",
    "ExecutionReceiptStore",
    "TerminalReceiptPage",
]
