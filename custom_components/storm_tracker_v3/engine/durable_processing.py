"""Crashbestendig journal voor observatiebatches en trackingstate.

Deze opslag gebruikt bewust dezelfde afzonderlijke Storm Tracker-database als
de kalibratiecorpus, maar heeft een eigen schema en transactielock. Home
Assistant's algemene recorderdatabase wordt nooit benaderd.

Het journal is de autoriteit voor exact-once *businesssemantiek*:

* transport mag at-least-once zijn;
* een stabiele ``batch_id`` identificeert dezelfde meteorologische input;
* alleen ``COMMITTED`` betekent dat de trackingstate werd toegepast;
* trackingstate en het commitreceipt worden in één ``BEGIN IMMEDIATE``-
  transactie geschreven.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict, dataclass, fields, is_dataclass
from enum import Enum
from functools import wraps
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Iterable, Sequence
import uuid

from .observation import Observation, ObservationType


DURABLE_SCHEMA_VERSION = 3
DEFAULT_ENGINE_VERSION = "storm-tracker-v3-tracking-v1"

STATE_PREPARED = "PREPARED"
STATE_EXECUTING = "EXECUTING"
STATE_RESULT_STAGED = "RESULT_STAGED"
STATE_COMMITTED = "COMMITTED"
STATE_QUARANTINED = "QUARANTINED"
STATE_PARTITIONED = "PARTITIONED"

_ALL_STATES = {
    STATE_PREPARED,
    STATE_EXECUTING,
    STATE_RESULT_STAGED,
    STATE_COMMITTED,
    STATE_QUARANTINED,
    STATE_PARTITIONED,
}


class DurableProcessingError(RuntimeError):
    """Basisklasse voor expliciete journalfouten."""

    durable_no_fallback = True


class DurableBatchConflict(DurableProcessingError):
    """Dezelfde batch-ID werd met andere immutable inhoud aangeboden."""


class DurableRevisionConflict(DurableProcessingError):
    """De duurzame trackinghead staat niet op de verwachte revision."""


class DurableStateConflict(DurableProcessingError):
    """De gevraagde overgang past niet bij de actuele batchstate."""


def _durable_operation(method):
    """Never let a raw SQLite failure activate an unsafe caller fallback."""

    @wraps(method)
    def wrapped(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except DurableProcessingError:
            raise
        except sqlite3.Error as err:
            raise DurableProcessingError(
                f"duurzame SQLite-operatie {method.__name__} faalde: "
                f"{type(err).__name__}: {err}"
            ) from err

    return wrapped


@dataclass(frozen=True)
class DurableTrackingHead:
    """Volledig gevalideerde actuele trackingstate."""

    engine_version: str
    region_id: str
    revision: int
    snapshots: tuple[dict[str, Any], ...]
    snapshot_digest: str
    last_batch_id: str | None
    updated_at: float


@dataclass(frozen=True)
class DurableBatchRecord:
    """Immutable beeld van één journalbatch."""

    engine_version: str
    region_id: str
    batch_id: str
    root_batch_id: str
    parent_batch_id: str | None
    state: str
    start_revision: int
    input_digest: str
    start_state_digest: str
    observation_count: int
    result_digest: str | None
    attempt_count: int
    last_error: str | None
    lineage: dict[str, Any]
    created_at: float
    updated_at: float
    committed_at: float | None
    observations: tuple[Observation, ...] = ()
    result_snapshots: tuple[dict[str, Any], ...] = ()


def _json_safe(value: Any) -> Any:
    """Normaliseer willekeurige providerprovenance naar strikte JSON."""
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, dict):
        return {
            str(key): _json_safe(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if value is None or isinstance(value, (str, int, bool)):
        return value
    return str(value)


def canonical_json(value: Any) -> str:
    """Platformonafhankelijke JSON voor opslag en hashes."""
    return json.dumps(
        _json_safe(value),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _haversine_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    radius = 6371.0088
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return radius * 2 * math.atan2(
        math.sqrt(value), math.sqrt(max(0.0, 1 - value))
    )


def normalize_tracking_state(
    snapshots: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Sorteer snapshots canoniek zonder de invoer te muteren."""
    if not isinstance(snapshots, (list, tuple)):
        raise TypeError("tracking snapshots moeten een lijst of tuple zijn")
    normalized = [_json_safe(snapshot) for snapshot in snapshots]
    if any(not isinstance(snapshot, dict) for snapshot in normalized):
        raise TypeError("iedere tracking snapshot moet een object zijn")
    return sorted(
        normalized,
        key=lambda item: str(item.get("storm_id", "")),
    )


def tracking_state_digest(
    snapshots: Sequence[dict[str, Any]],
) -> str:
    return _digest({
        "schema": "stv3-tracking-state-v1",
        "snapshots": normalize_tracking_state(snapshots),
    })


def serialize_observation(observation: Observation) -> dict[str, Any]:
    """Serialiseer alle Observation-velden zonder type-informatie te verliezen."""
    if not isinstance(observation, Observation):
        raise TypeError("durable ingest vereist Observation-objecten")
    raw = _json_safe(asdict(observation))
    raw["obs_type"] = observation.obs_type.value
    return raw


def deserialize_observation(raw: dict[str, Any]) -> Observation:
    """Herstel een Observation uit het duurzame contract."""
    if not isinstance(raw, dict):
        raise TypeError("duurzame observation payload moet een object zijn")
    values = dict(raw)
    values["obs_type"] = ObservationType(str(values["obs_type"]))
    for name in ("footprint_points", "parent_footprint_points"):
        points = values.get(name) or ()
        values[name] = tuple(
            (float(point[0]), float(point[1]))
            for point in points
        )
    for name in (
        "footprint_polygons",
        "parent_footprint_polygons",
    ):
        values[name] = tuple(
            tuple(
                tuple(
                    tuple(float(coordinate) for coordinate in point)
                    for point in ring
                )
                for ring in polygon
            )
            for polygon in (values.get(name) or ())
        )
    values["quality_flags"] = tuple(
        str(item) for item in (values.get("quality_flags") or ())
    )
    allowed = {item.name for item in fields(Observation)}
    return Observation(**{
        key: value for key, value in values.items() if key in allowed
    })


def tracking_input_digest(
    observations: Sequence[Observation] | Sequence[dict[str, Any]],
) -> str:
    serialized = [
        serialize_observation(item)
        if isinstance(item, Observation)
        else _json_safe(item)
        for item in observations
    ]
    return _digest({
        "schema": "stv3-tracking-observations-v1",
        "observations": serialized,
    })


class DurableProcessingStore:
    """SQLite-journal met atomische state+receipt-commit."""

    def __init__(
        self,
        path: str | Path,
        *,
        engine_version: str = DEFAULT_ENGINE_VERSION,
        actor: str = "ha-integration",
        build_id: str = "unknown",
        fault_injector=None,
    ) -> None:
        self.path = Path(path)
        self.engine_version = str(engine_version)
        self.actor = str(actor)
        self.build_id = str(build_id)
        self._lock = threading.RLock()
        self._fault_injector = fault_injector

    @_durable_operation
    def initialize(self) -> dict[str, int]:
        """Maak uitsluitend additieve journaltabellen aan."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, closing(self._connect()) as db, db:
            metadata_exists = db.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='durable_processing_metadata'
                """
            ).fetchone()
            if metadata_exists is not None:
                version = db.execute(
                    """
                    SELECT value FROM durable_processing_metadata
                    WHERE key='schema_version'
                    """
                ).fetchone()
                if (
                    version is None
                    or str(version[0]) != str(DURABLE_SCHEMA_VERSION)
                ):
                    raise DurableProcessingError(
                        "onbekende durable schema-versie; automatische "
                        "overschrijving is geweigerd"
                    )
                self._validate_initialized_schema(db)
                return self.statistics(connection=db)
            schema_sql = """
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS durable_processing_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS durable_observations (
                    region_id TEXT NOT NULL,
                    observation_id TEXT NOT NULL,
                    observed_at REAL NOT NULL,
                    received_at REAL,
                    source TEXT NOT NULL,
                    observation_type TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    persisted_at REAL NOT NULL,
                    PRIMARY KEY(region_id, observation_id)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_durable_observation_time
                    ON durable_observations(region_id, observed_at);

                CREATE TABLE IF NOT EXISTS durable_ingest_queue (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    engine_version TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    observation_id TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN (
                        'RECEIVED', 'DEDUPED', 'BATCHED', 'COMMITTED'
                    )),
                    batch_id TEXT,
                    queued_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(engine_version, region_id, observation_id),
                    FOREIGN KEY(region_id, observation_id)
                        REFERENCES durable_observations(
                            region_id, observation_id
                        )
                );
                CREATE INDEX IF NOT EXISTS idx_durable_ingest_recovery
                    ON durable_ingest_queue(
                        engine_version, region_id, state, sequence
                    );

                CREATE TABLE IF NOT EXISTS durable_regions (
                    engine_version TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    center_lat REAL NOT NULL,
                    center_lon REAL NOT NULL,
                    observation_radius_km REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(engine_version, region_id)
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS durable_region_targets (
                    engine_version TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    last_lat REAL NOT NULL,
                    last_lon REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(engine_version, target_id),
                    FOREIGN KEY(engine_version, region_id)
                        REFERENCES durable_regions(
                            engine_version, region_id
                        )
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_durable_region_target_region
                    ON durable_region_targets(engine_version, region_id);

                CREATE TABLE IF NOT EXISTS observation_batches (
                    engine_version TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    root_batch_id TEXT NOT NULL,
                    parent_batch_id TEXT,
                    state TEXT NOT NULL CHECK(state IN (
                        'PREPARED', 'EXECUTING', 'RESULT_STAGED',
                        'COMMITTED', 'QUARANTINED', 'PARTITIONED'
                    )),
                    start_revision INTEGER NOT NULL,
                    input_digest TEXT NOT NULL,
                    start_state_digest TEXT NOT NULL,
                    observation_count INTEGER NOT NULL,
                    result_digest TEXT,
                    result_json TEXT,
                    active_attempt_id TEXT,
                    lease_expires_at REAL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    lineage_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    committed_at REAL,
                    PRIMARY KEY(engine_version, region_id, batch_id)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_observation_batches_recovery
                    ON observation_batches(
                        engine_version, state, created_at, region_id
                    );

                CREATE TABLE IF NOT EXISTS observation_batch_items (
                    engine_version TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    observation_id TEXT NOT NULL,
                    PRIMARY KEY(
                        engine_version, region_id, batch_id, ordinal
                    ),
                    UNIQUE(
                        engine_version, region_id, batch_id, observation_id
                    ),
                    FOREIGN KEY(engine_version, region_id, batch_id)
                        REFERENCES observation_batches(
                            engine_version, region_id, batch_id
                        ) ON DELETE CASCADE,
                    FOREIGN KEY(region_id, observation_id)
                        REFERENCES durable_observations(
                            region_id, observation_id
                        )
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS processing_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    engine_version TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    executor TEXT NOT NULL,
                    worker_id TEXT,
                    job_id TEXT,
                    start_revision INTEGER NOT NULL,
                    start_state_digest TEXT NOT NULL,
                    requested_at REAL NOT NULL,
                    started_at REAL NOT NULL,
                    deadline_at REAL NOT NULL,
                    terminal_at REAL,
                    terminal_state TEXT,
                    result_digest TEXT,
                    error_category TEXT,
                    error_detail TEXT,
                    retry_of TEXT,
                    FOREIGN KEY(engine_version, region_id, batch_id)
                        REFERENCES observation_batches(
                            engine_version, region_id, batch_id
                        ) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_processing_attempt_batch
                    ON processing_attempts(
                        engine_version, region_id, batch_id, started_at
                    );

                CREATE TABLE IF NOT EXISTS batch_state_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    engine_version TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    previous_state TEXT,
                    new_state TEXT NOT NULL,
                    transitioned_at REAL NOT NULL,
                    actor TEXT NOT NULL,
                    build_id TEXT NOT NULL,
                    error_category TEXT,
                    detail TEXT,
                    FOREIGN KEY(engine_version, region_id, batch_id)
                        REFERENCES observation_batches(
                            engine_version, region_id, batch_id
                        ) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_batch_transition_batch
                    ON batch_state_transitions(
                        engine_version, region_id, batch_id, id
                    );

                CREATE TABLE IF NOT EXISTS tracking_state_heads (
                    engine_version TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    current_revision INTEGER NOT NULL,
                    snapshot_digest TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    last_batch_id TEXT,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(engine_version, region_id)
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS tracking_state_versions (
                    engine_version TEXT NOT NULL,
                    region_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    batch_id TEXT,
                    snapshot_digest TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY(engine_version, region_id, revision),
                    UNIQUE(engine_version, region_id, batch_id)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_tracking_versions_batch
                    ON tracking_state_versions(
                        engine_version, region_id, batch_id
                    );
                INSERT OR IGNORE INTO durable_processing_metadata(key, value)
                VALUES('schema_version', '__DURABLE_SCHEMA_VERSION__');
                COMMIT;
            """.replace(
                "__DURABLE_SCHEMA_VERSION__",
                str(DURABLE_SCHEMA_VERSION),
            )
            db.executescript(schema_sql)
            self._validate_initialized_schema(db)
            return self.statistics(connection=db)

    @_durable_operation
    def resolve_region_id(
        self,
        *,
        center_lat: float,
        center_lon: float,
        observation_radius_km: float,
        sharing_distance_km: float,
        target_ids: Sequence[str],
    ) -> str:
        """Resolve a permanent region identity across GPS jitter/restarts."""
        center_lat = float(center_lat)
        center_lon = float(center_lon)
        observation_radius_km = float(observation_radius_km)
        sharing_distance_km = float(sharing_distance_km)
        if (
            not math.isfinite(center_lat)
            or not -90 <= center_lat <= 90
            or not math.isfinite(center_lon)
            or not -180 <= center_lon <= 180
            or not math.isfinite(observation_radius_km)
            or observation_radius_km <= 0
            or not math.isfinite(sharing_distance_km)
            or sharing_distance_km <= 0
        ):
            raise ValueError("ongeldige duurzame regiogeometrie")
        normalized_targets = tuple(sorted({
            str(target).strip()
            for target in target_ids
            if str(target).strip()
        }))
        now = time.time()
        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                candidates: dict[str, sqlite3.Row] = {}
                if normalized_targets:
                    placeholders = ",".join(
                        "?" for _ in normalized_targets
                    )
                    for row in db.execute(
                        f"""
                        SELECT r.*
                        FROM durable_region_targets t
                        JOIN durable_regions r
                          ON r.engine_version=t.engine_version
                         AND r.region_id=t.region_id
                        WHERE t.engine_version=?
                          AND t.target_id IN ({placeholders})
                        """,
                        (self.engine_version, *normalized_targets),
                    ):
                        candidates[str(row["region_id"])] = row
                for row in db.execute(
                    """
                    SELECT * FROM durable_regions
                    WHERE engine_version=?
                      AND abs(observation_radius_km - ?) < 0.001
                    """,
                    (self.engine_version, observation_radius_km),
                ):
                    candidates.setdefault(str(row["region_id"]), row)
                nearby = [
                    (
                        _haversine_km(
                            center_lat,
                            center_lon,
                            float(row["center_lat"]),
                            float(row["center_lon"]),
                        ),
                        region_id,
                    )
                    for region_id, row in candidates.items()
                    if abs(
                        float(row["observation_radius_km"])
                        - observation_radius_km
                    ) < 0.001
                ]
                distance, region_id = min(
                    nearby,
                    default=(float("inf"), ""),
                    key=lambda item: (item[0], item[1]),
                )
                if distance > sharing_distance_km:
                    region_id = uuid.uuid4().hex
                    db.execute(
                        """
                        INSERT INTO durable_regions(
                            engine_version, region_id, center_lat, center_lon,
                            observation_radius_km, created_at, updated_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            self.engine_version,
                            region_id,
                            center_lat,
                            center_lon,
                            observation_radius_km,
                            now,
                            now,
                        ),
                    )
                else:
                    db.execute(
                        """
                        UPDATE durable_regions
                        SET center_lat=?, center_lon=?,
                            observation_radius_km=?, updated_at=?
                        WHERE engine_version=? AND region_id=?
                        """,
                        (
                            center_lat,
                            center_lon,
                            observation_radius_km,
                            now,
                            self.engine_version,
                            region_id,
                        ),
                    )
                for target_id in normalized_targets:
                    db.execute(
                        """
                        INSERT INTO durable_region_targets(
                            engine_version, target_id, region_id,
                            last_lat, last_lon, updated_at
                        ) VALUES(?, ?, ?, ?, ?, ?)
                        ON CONFLICT(engine_version, target_id) DO UPDATE SET
                            region_id=excluded.region_id,
                            last_lat=excluded.last_lat,
                            last_lon=excluded.last_lon,
                            updated_at=excluded.updated_at
                        """,
                        (
                            self.engine_version,
                            target_id,
                            region_id,
                            center_lat,
                            center_lon,
                            now,
                        ),
                    )
                db.commit()
                return region_id
            except Exception:
                db.rollback()
                raise

    @_durable_operation
    def receive_observations(
        self,
        *,
        region_id: str,
        observations: Sequence[Observation],
        deduplicated: Sequence[bool] | None = None,
    ) -> tuple[bool, ...]:
        """Persist een ingestgroep atomair vóór opname in de OFE-cache.

        De booleans corresponderen met de invoervolgorde en zijn alleen waar
        voor nieuwe observation receipts. QoS-/providerredelivery van exact
        dezelfde canonical payload wordt daardoor ook na restart idempotent.
        """
        region_id = self._required_id(region_id, "region_id")
        prepared_rows = [
            self._observation_storage_row(observation)
            for observation in observations
        ]
        deduplicated = (
            tuple(False for _ in prepared_rows)
            if deduplicated is None
            else tuple(bool(value) for value in deduplicated)
        )
        if len(deduplicated) != len(prepared_rows):
            raise ValueError(
                "deduplicated moet één classificatie per observation bevatten"
            )
        now = time.time()
        inserted = []
        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                self._fault("receive_after_begin")
                for row, is_deduplicated in zip(
                    prepared_rows, deduplicated, strict=True
                ):
                    self._upsert_observation_row(
                        db, region_id=region_id, row=row, persisted_at=now
                    )
                    self._fault("receive_after_observation")
                    cursor = db.execute(
                        """
                        INSERT OR IGNORE INTO durable_ingest_queue(
                            engine_version, region_id, observation_id,
                            state, batch_id, queued_at, updated_at
                        ) VALUES(?, ?, ?, ?, NULL, ?, ?)
                        """,
                        (
                            self.engine_version,
                            region_id,
                            row[0],
                            "DEDUPED" if is_deduplicated else "RECEIVED",
                            now,
                            now,
                        ),
                    )
                    inserted.append(cursor.rowcount == 1)
                    self._fault("receive_after_receipt")
                self._fault("receive_before_commit")
                db.commit()
                self._fault("receive_after_commit")
            except Exception:
                db.rollback()
                raise
        return tuple(inserted)

    @_durable_operation
    def recover_received_observations(
        self,
        *,
        region_id: str,
    ) -> tuple[Observation, ...]:
        """Lees nog niet gebatchte input in exacte persistente FIFO-volgorde."""
        with self._lock, closing(self._connect()) as db, db:
            rows = db.execute(
                """
                SELECT o.payload_json
                FROM durable_ingest_queue q
                JOIN durable_observations o
                  ON o.region_id=q.region_id
                 AND o.observation_id=q.observation_id
                WHERE q.engine_version=? AND q.region_id=?
                  AND q.state='RECEIVED'
                ORDER BY q.sequence
                """,
                (self.engine_version, region_id),
            ).fetchall()
        return tuple(
            deserialize_observation(json.loads(row[0]))
            for row in rows
        )

    @_durable_operation
    def prepare_batch(
        self,
        *,
        region_id: str,
        batch_id: str,
        observations: Sequence[Observation],
        start_revision: int,
        start_state: Sequence[dict[str, Any]],
        root_batch_id: str | None = None,
        parent_batch_id: str | None = None,
        lineage: dict[str, Any] | None = None,
    ) -> DurableBatchRecord:
        """Persist immutable input vóór worker- of lokale berekening."""
        region_id = self._required_id(region_id, "region_id")
        batch_id = self._required_id(batch_id, "batch_id")
        root_batch_id = self._required_id(
            root_batch_id or batch_id, "root_batch_id"
        )
        if parent_batch_id is not None:
            parent_batch_id = self._required_id(
                parent_batch_id, "parent_batch_id"
            )
        if isinstance(start_revision, bool) or start_revision < 0:
            raise ValueError("start_revision moet een niet-negatieve integer zijn")
        if not isinstance(start_revision, int):
            raise TypeError("start_revision moet een integer zijn")
        serialized = [serialize_observation(item) for item in observations]
        observation_rows = [
            self._observation_storage_row(observation)
            for observation in observations
        ]
        input_digest = tracking_input_digest(serialized)
        normalized_start = normalize_tracking_state(start_state)
        start_json = canonical_json(normalized_start)
        start_digest = tracking_state_digest(normalized_start)
        lineage_json = canonical_json(lineage or {})
        now = time.time()

        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                existing = self._fetch_batch_row(
                    db, region_id=region_id, batch_id=batch_id
                )
                if existing is not None:
                    self._assert_same_batch_identity(
                        existing,
                        input_digest=input_digest,
                        observation_count=len(serialized),
                        root_batch_id=root_batch_id,
                        parent_batch_id=parent_batch_id,
                    )
                    if existing["state"] == STATE_COMMITTED:
                        db.commit()
                        return self._record_from_row(
                            db, existing, include_payload=True
                        )
                    same_base = (
                        int(existing["start_revision"]) == start_revision
                        and existing["start_state_digest"] == start_digest
                    )
                    if not same_base:
                        if existing["state"] != STATE_PREPARED:
                            raise DurableRevisionConflict(
                                "alleen een PREPARED batch kan op de "
                                "actuele trackinghead worden gerebased"
                            )
                        head = self._required_head_row(db, region_id)
                        self._assert_head_matches(
                            head,
                            start_revision=start_revision,
                            start_state_digest=start_digest,
                        )
                        db.execute(
                            """
                            UPDATE observation_batches
                            SET start_revision=?, start_state_digest=?,
                                last_error=NULL, updated_at=?
                            WHERE engine_version=? AND region_id=?
                              AND batch_id=? AND state=?
                            """,
                            (
                                start_revision,
                                start_digest,
                                now,
                                self.engine_version,
                                region_id,
                                batch_id,
                                STATE_PREPARED,
                            ),
                        )
                        self._insert_transition(
                            db,
                            region_id=region_id,
                            batch_id=batch_id,
                            previous_state=STATE_PREPARED,
                            new_state=STATE_PREPARED,
                            detail=(
                                "rebase naar actuele revision "
                                f"{start_revision}"
                            ),
                        )
                    db.commit()
                    existing = self._required_batch_row(
                        db, region_id, batch_id
                    )
                    return self._record_from_row(
                        db, existing, include_payload=True
                    )

                head = self._fetch_head_row(db, region_id)
                if head is None:
                    db.execute(
                        """
                        INSERT INTO tracking_state_heads(
                            engine_version, region_id, current_revision,
                            snapshot_digest, snapshot_json, last_batch_id,
                            updated_at
                        ) VALUES(?, ?, ?, ?, ?, NULL, ?)
                        """,
                        (
                            self.engine_version,
                            region_id,
                            start_revision,
                            start_digest,
                            start_json,
                            now,
                        ),
                    )
                    db.execute(
                        """
                        INSERT INTO tracking_state_versions(
                            engine_version, region_id, revision, batch_id,
                            snapshot_digest, snapshot_json, created_at
                        ) VALUES(?, ?, ?, NULL, ?, ?, ?)
                        """,
                        (
                            self.engine_version,
                            region_id,
                            start_revision,
                            start_digest,
                            start_json,
                            now,
                        ),
                    )
                else:
                    self._assert_head_matches(
                        head,
                        start_revision=start_revision,
                        start_state_digest=start_digest,
                    )

                if parent_batch_id is not None:
                    parent = self._required_batch_row(
                        db, region_id, parent_batch_id
                    )
                    if parent["root_batch_id"] != root_batch_id:
                        raise DurableBatchConflict(
                            "splitkind heeft een andere rootbatch"
                        )
                    if parent["state"] == STATE_COMMITTED:
                        raise DurableBatchConflict(
                            "gecommitteerde batch kan niet worden gesplitst"
                        )
                    if parent["state"] != STATE_PARTITIONED:
                        previous_parent_state = parent["state"]
                        db.execute(
                            """
                            UPDATE observation_batches
                            SET state=?, active_attempt_id=NULL,
                                lease_expires_at=NULL, updated_at=?
                            WHERE engine_version=? AND region_id=?
                              AND batch_id=?
                            """,
                            (
                                STATE_PARTITIONED,
                                now,
                                self.engine_version,
                                region_id,
                                parent_batch_id,
                            ),
                        )
                        self._insert_transition(
                            db,
                            region_id=region_id,
                            batch_id=parent_batch_id,
                            previous_state=previous_parent_state,
                            new_state=STATE_PARTITIONED,
                            detail="immutable split children",
                        )

                for row in observation_rows:
                    self._upsert_observation_row(
                        db,
                        region_id=region_id,
                        row=row,
                        persisted_at=now,
                    )
                    db.execute(
                        """
                        INSERT OR IGNORE INTO durable_ingest_queue(
                            engine_version, region_id, observation_id,
                            state, batch_id, queued_at, updated_at
                        ) VALUES(?, ?, ?, 'RECEIVED', NULL, ?, ?)
                        """,
                        (
                            self.engine_version,
                            region_id,
                            row[0],
                            now,
                            now,
                        ),
                    )
                    ingest = db.execute(
                        """
                        SELECT state, batch_id
                        FROM durable_ingest_queue
                        WHERE engine_version=? AND region_id=?
                          AND observation_id=?
                        """,
                        (
                            self.engine_version,
                            region_id,
                            row[0],
                        ),
                    ).fetchone()
                    if (
                        ingest["state"] == "COMMITTED"
                        or (
                            ingest["state"] == "BATCHED"
                            and ingest["batch_id"] != batch_id
                            and ingest["batch_id"] != parent_batch_id
                        )
                    ):
                        raise DurableBatchConflict(
                            "observation receipt behoort al tot een andere "
                            "duurzame toepassing"
                        )

                db.execute(
                    """
                    INSERT INTO observation_batches(
                        engine_version, region_id, batch_id, root_batch_id,
                        parent_batch_id, state, start_revision, input_digest,
                        start_state_digest, observation_count, lineage_json,
                        created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.engine_version,
                        region_id,
                        batch_id,
                        root_batch_id,
                        parent_batch_id,
                        STATE_PREPARED,
                        start_revision,
                        input_digest,
                        start_digest,
                        len(serialized),
                        lineage_json,
                        now,
                        now,
                    ),
                )
                for ordinal, row in enumerate(observation_rows):
                    db.execute(
                        """
                        INSERT INTO observation_batch_items(
                            engine_version, region_id, batch_id, ordinal,
                            observation_id
                        ) VALUES(?, ?, ?, ?, ?)
                        """,
                        (
                            self.engine_version,
                            region_id,
                            batch_id,
                            ordinal,
                            row[0],
                        ),
                    )
                    db.execute(
                        """
                        UPDATE durable_ingest_queue
                        SET state='BATCHED', batch_id=?, updated_at=?
                        WHERE engine_version=? AND region_id=?
                          AND observation_id=? AND (
                              state='RECEIVED'
                              OR (
                                  state='BATCHED'
                                  AND batch_id=?
                              )
                          )
                        """,
                        (
                            batch_id,
                            now,
                            self.engine_version,
                            region_id,
                            row[0],
                            parent_batch_id,
                        ),
                    )
                self._insert_transition(
                    db,
                    region_id=region_id,
                    batch_id=batch_id,
                    previous_state=None,
                    new_state=STATE_PREPARED,
                )
                db.commit()
            except Exception:
                db.rollback()
                raise
            row = self._fetch_batch_row(
                db, region_id=region_id, batch_id=batch_id
            )
            return self._record_from_row(db, row, include_payload=True)

    @_durable_operation
    def begin_attempt(
        self,
        *,
        region_id: str,
        batch_id: str,
        executor: str,
        lease_seconds: float,
        worker_id: str | None = None,
        job_id: str | None = None,
        retry_of: str | None = None,
    ) -> str:
        """Claim één herstelbare execution lease."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds moet positief zijn")
        attempt_id = uuid.uuid4().hex
        now = time.time()
        deadline = now + float(lease_seconds)
        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                row = self._required_batch_row(db, region_id, batch_id)
                state = row["state"]
                if state == STATE_COMMITTED:
                    db.commit()
                    return ""
                if state == STATE_EXECUTING:
                    lease_expires_at = row["lease_expires_at"]
                    if lease_expires_at is None or float(lease_expires_at) > now:
                        raise DurableStateConflict(
                            f"batch {batch_id} heeft al een actieve lease"
                        )
                    self._abandon_active_attempt(
                        db,
                        row,
                        terminal_state="expired_lease",
                        error_detail="lease verliep vóór een terminale status",
                    )
                elif state not in {STATE_PREPARED, STATE_RESULT_STAGED}:
                    raise DurableStateConflict(
                        f"batch {batch_id} kan niet starten vanuit {state}"
                    )
                previous_state = state
                db.execute(
                    """
                    INSERT INTO processing_attempts(
                        attempt_id, engine_version, region_id, batch_id,
                        executor, worker_id, job_id, start_revision,
                        start_state_digest, requested_at, started_at,
                        deadline_at, retry_of
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        attempt_id,
                        self.engine_version,
                        region_id,
                        batch_id,
                        str(executor),
                        worker_id,
                        job_id,
                        int(row["start_revision"]),
                        str(row["start_state_digest"]),
                        now,
                        now,
                        deadline,
                        retry_of,
                    ),
                )
                db.execute(
                    """
                    UPDATE observation_batches
                    SET state=?, active_attempt_id=?, lease_expires_at=?,
                        attempt_count=attempt_count+1, last_error=NULL,
                        updated_at=?
                    WHERE engine_version=? AND region_id=? AND batch_id=?
                    """,
                    (
                        STATE_EXECUTING,
                        attempt_id,
                        deadline,
                        now,
                        self.engine_version,
                        region_id,
                        batch_id,
                    ),
                )
                self._insert_transition(
                    db,
                    region_id=region_id,
                    batch_id=batch_id,
                    previous_state=previous_state,
                    new_state=STATE_EXECUTING,
                    detail=f"executor={executor}",
                )
                db.commit()
            except Exception:
                db.rollback()
                raise
        return attempt_id

    @_durable_operation
    def fail_attempt(
        self,
        *,
        region_id: str,
        batch_id: str,
        attempt_id: str,
        error_category: str,
        error_detail: str,
        quarantine: bool = False,
        lineage: dict[str, Any] | None = None,
    ) -> DurableBatchRecord:
        """Terminaliseer een poging zonder de input te verwijderen."""
        target_state = STATE_QUARANTINED if quarantine else STATE_PREPARED
        now = time.time()
        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                row = self._required_batch_row(db, region_id, batch_id)
                if row["state"] == STATE_COMMITTED:
                    db.commit()
                    return self._record_from_row(db, row)
                if (
                    row["state"] != STATE_EXECUTING
                    or row["active_attempt_id"] != attempt_id
                ):
                    raise DurableStateConflict(
                        "alleen de actieve execution lease kan falen"
                    )
                db.execute(
                    """
                    UPDATE processing_attempts
                    SET terminal_at=?, terminal_state='failed',
                        error_category=?, error_detail=?
                    WHERE attempt_id=? AND terminal_at IS NULL
                    """,
                    (
                        now,
                        str(error_category),
                        str(error_detail),
                        attempt_id,
                    ),
                )
                db.execute(
                    """
                    UPDATE observation_batches
                    SET state=?, active_attempt_id=NULL,
                        lease_expires_at=NULL, last_error=?,
                        lineage_json=coalesce(?, lineage_json), updated_at=?
                    WHERE engine_version=? AND region_id=? AND batch_id=?
                    """,
                    (
                        target_state,
                        str(error_detail),
                        canonical_json(lineage) if lineage is not None else None,
                        now,
                        self.engine_version,
                        region_id,
                        batch_id,
                    ),
                )
                self._insert_transition(
                    db,
                    region_id=region_id,
                    batch_id=batch_id,
                    previous_state=STATE_EXECUTING,
                    new_state=target_state,
                    error_category=error_category,
                    detail=error_detail,
                )
                db.commit()
            except Exception:
                db.rollback()
                raise
            current = self._required_batch_row(db, region_id, batch_id)
            return self._record_from_row(db, current, include_payload=True)

    @_durable_operation
    def stage_result(
        self,
        *,
        region_id: str,
        batch_id: str,
        attempt_id: str,
        snapshots: Sequence[dict[str, Any]],
    ) -> DurableBatchRecord:
        """Bewaar een gevalideerd resultaat vóór de zichtbare statecommit."""
        normalized = normalize_tracking_state(snapshots)
        result_json = canonical_json(normalized)
        result_digest = tracking_state_digest(normalized)
        now = time.time()
        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                row = self._required_batch_row(db, region_id, batch_id)
                if row["state"] == STATE_COMMITTED:
                    if row["result_digest"] != result_digest:
                        raise DurableBatchConflict(
                            "gecommitteerde batch heeft ander resultaat"
                        )
                    db.commit()
                    return self._record_from_row(
                        db, row, include_payload=True
                    )
                if (
                    row["state"] != STATE_EXECUTING
                    or row["active_attempt_id"] != attempt_id
                ):
                    raise DurableStateConflict(
                        "resultaat hoort niet bij de actieve execution lease"
                    )
                db.execute(
                    """
                    UPDATE processing_attempts
                    SET terminal_at=?, terminal_state='result_staged',
                        result_digest=?
                    WHERE attempt_id=? AND terminal_at IS NULL
                    """,
                    (now, result_digest, attempt_id),
                )
                db.execute(
                    """
                    UPDATE observation_batches
                    SET state=?, result_digest=?, result_json=?,
                        active_attempt_id=NULL, lease_expires_at=NULL,
                        last_error=NULL, updated_at=?
                    WHERE engine_version=? AND region_id=? AND batch_id=?
                    """,
                    (
                        STATE_RESULT_STAGED,
                        result_digest,
                        result_json,
                        now,
                        self.engine_version,
                        region_id,
                        batch_id,
                    ),
                )
                self._insert_transition(
                    db,
                    region_id=region_id,
                    batch_id=batch_id,
                    previous_state=STATE_EXECUTING,
                    new_state=STATE_RESULT_STAGED,
                )
                db.commit()
            except Exception:
                db.rollback()
                raise
            current = self._required_batch_row(db, region_id, batch_id)
            return self._record_from_row(
                db, current, include_payload=True
            )

    @_durable_operation
    def commit_staged(
        self,
        *,
        region_id: str,
        batch_id: str,
    ) -> DurableTrackingHead:
        """Commit trackingstate en receipt atomair met revision-CAS."""
        now = time.time()
        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                self._fault("commit_after_begin")
                batch = self._required_batch_row(db, region_id, batch_id)
                if batch["state"] == STATE_COMMITTED:
                    version = self._required_batch_version(
                        db, region_id=region_id, batch_id=batch_id
                    )
                    db.commit()
                    return self._head_from_version_row(version)
                if batch["state"] != STATE_RESULT_STAGED:
                    raise DurableStateConflict(
                        f"batch {batch_id} is niet RESULT_STAGED"
                    )
                result_json = batch["result_json"]
                result_digest = batch["result_digest"]
                if not result_json or not result_digest:
                    raise DurableStateConflict(
                        "RESULT_STAGED mist payload of digest"
                    )
                decoded_result = json.loads(result_json)
                if tracking_state_digest(decoded_result) != result_digest:
                    raise DurableBatchConflict(
                        "opgeslagen result_digest komt niet overeen"
                    )
                head = self._required_head_row(db, region_id)
                self._assert_head_matches(
                    head,
                    start_revision=int(batch["start_revision"]),
                    start_state_digest=str(batch["start_state_digest"]),
                )
                result_revision = int(batch["start_revision"]) + 1
                db.execute(
                    """
                    INSERT INTO tracking_state_versions(
                        engine_version, region_id, revision, batch_id,
                        snapshot_digest, snapshot_json, created_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.engine_version,
                        region_id,
                        result_revision,
                        batch_id,
                        result_digest,
                        result_json,
                        now,
                    ),
                )
                self._fault("commit_after_version")
                updated = db.execute(
                    """
                    UPDATE tracking_state_heads
                    SET current_revision=?, snapshot_digest=?,
                        snapshot_json=?, last_batch_id=?, updated_at=?
                    WHERE engine_version=? AND region_id=?
                      AND current_revision=? AND snapshot_digest=?
                    """,
                    (
                        result_revision,
                        result_digest,
                        result_json,
                        batch_id,
                        now,
                        self.engine_version,
                        region_id,
                        int(batch["start_revision"]),
                        str(batch["start_state_digest"]),
                    ),
                )
                if updated.rowcount != 1:
                    raise DurableRevisionConflict(
                        "trackinghead wijzigde tijdens atomische commit"
                    )
                self._fault("commit_after_head")
                db.execute(
                    """
                    UPDATE observation_batches
                    SET state=?, committed_at=?, updated_at=?
                    WHERE engine_version=? AND region_id=? AND batch_id=?
                      AND state=?
                    """,
                    (
                        STATE_COMMITTED,
                        now,
                        now,
                        self.engine_version,
                        region_id,
                        batch_id,
                        STATE_RESULT_STAGED,
                    ),
                )
                self._fault("commit_after_receipt")
                db.execute(
                    """
                    UPDATE durable_ingest_queue
                    SET state='COMMITTED', updated_at=?
                    WHERE engine_version=? AND region_id=?
                      AND batch_id=? AND state='BATCHED'
                    """,
                    (
                        now,
                        self.engine_version,
                        region_id,
                        batch_id,
                    ),
                )
                self._fault("commit_after_ingest")
                self._insert_transition(
                    db,
                    region_id=region_id,
                    batch_id=batch_id,
                    previous_state=STATE_RESULT_STAGED,
                    new_state=STATE_COMMITTED,
                )
                self._fault("commit_before_commit")
                db.commit()
                self._fault("commit_after_commit")
            except Exception:
                db.rollback()
                raise
            return self._head_from_row(
                self._required_head_row(db, region_id)
            )

    @_durable_operation
    def recover_after_restart(
        self,
        *,
        region_id: str | None = None,
    ) -> tuple[DurableBatchRecord, ...]:
        """Maak leases van het vorige proces herstelbaar en geef backlog terug."""
        now = time.time()
        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                orphan_params: list[Any] = [self.engine_version]
                orphan_region = ""
                if region_id is not None:
                    orphan_region = " AND q.region_id=?"
                    orphan_params.append(region_id)
                db.execute(
                    f"""
                    UPDATE durable_ingest_queue AS q
                    SET state='RECEIVED', batch_id=NULL, updated_at=?
                    WHERE q.engine_version=?
                      {orphan_region}
                      AND q.state='BATCHED'
                      AND EXISTS(
                          SELECT 1 FROM observation_batches b
                          WHERE b.engine_version=q.engine_version
                            AND b.region_id=q.region_id
                            AND b.batch_id=q.batch_id
                            AND b.state='PARTITIONED'
                      )
                    """,
                    [now, *orphan_params],
                )
                params: list[Any] = [self.engine_version, STATE_EXECUTING]
                where = "engine_version=? AND state=?"
                if region_id is not None:
                    where += " AND region_id=?"
                    params.append(region_id)
                executing = db.execute(
                    f"""
                    SELECT * FROM observation_batches
                    WHERE {where}
                    ORDER BY created_at, region_id, batch_id
                    """,
                    params,
                ).fetchall()
                for row in executing:
                    self._abandon_active_attempt(
                        db,
                        row,
                        terminal_state="abandoned_restart",
                        error_detail="vorige runtime stopte zonder terminale status",
                    )
                    db.execute(
                        """
                        UPDATE observation_batches
                        SET state=?, active_attempt_id=NULL,
                            lease_expires_at=NULL, last_error=?,
                            updated_at=?
                        WHERE engine_version=? AND region_id=? AND batch_id=?
                        """,
                        (
                            STATE_PREPARED,
                            "runtime_restart",
                            now,
                            self.engine_version,
                            row["region_id"],
                            row["batch_id"],
                        ),
                    )
                    self._insert_transition(
                        db,
                        region_id=row["region_id"],
                        batch_id=row["batch_id"],
                        previous_state=STATE_EXECUTING,
                        new_state=STATE_PREPARED,
                        error_category="runtime_restart",
                    )
                db.commit()
            except Exception:
                db.rollback()
                raise
            query_params: list[Any] = [
                self.engine_version,
                STATE_PREPARED,
                STATE_RESULT_STAGED,
            ]
            region_clause = ""
            if region_id is not None:
                region_clause = " AND region_id=?"
                query_params.append(region_id)
            rows = db.execute(
                f"""
                SELECT * FROM observation_batches
                WHERE engine_version=? AND state IN (?, ?)
                {region_clause}
                ORDER BY created_at, region_id, batch_id
                """,
                query_params,
            ).fetchall()
            return tuple(
                self._record_from_row(db, row, include_payload=True)
                for row in rows
            )

    @_durable_operation
    def quarantine_batch(
        self,
        *,
        region_id: str,
        batch_id: str,
        reason: str,
        lineage: dict[str, Any] | None = None,
    ) -> DurableBatchRecord:
        """Parkeer input expliciet zonder ze te verwijderen."""
        now = time.time()
        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                row = self._required_batch_row(db, region_id, batch_id)
                if row["state"] == STATE_COMMITTED:
                    db.commit()
                    return self._record_from_row(db, row)
                previous = row["state"]
                if previous == STATE_EXECUTING:
                    self._abandon_active_attempt(
                        db,
                        row,
                        terminal_state="quarantined",
                        error_detail=reason,
                    )
                db.execute(
                    """
                    UPDATE observation_batches
                    SET state=?, active_attempt_id=NULL,
                        lease_expires_at=NULL, last_error=?,
                        lineage_json=coalesce(?, lineage_json), updated_at=?
                    WHERE engine_version=? AND region_id=? AND batch_id=?
                    """,
                    (
                        STATE_QUARANTINED,
                        str(reason),
                        canonical_json(lineage) if lineage is not None else None,
                        now,
                        self.engine_version,
                        region_id,
                        batch_id,
                    ),
                )
                self._insert_transition(
                    db,
                    region_id=region_id,
                    batch_id=batch_id,
                    previous_state=previous,
                    new_state=STATE_QUARANTINED,
                    error_category="quarantine",
                    detail=reason,
                )
                db.commit()
            except Exception:
                db.rollback()
                raise
            return self._record_from_row(
                db,
                self._required_batch_row(db, region_id, batch_id),
                include_payload=True,
            )

    @_durable_operation
    def retry_quarantined(
        self,
        *,
        region_id: str | None = None,
    ) -> tuple[DurableBatchRecord, ...]:
        """Zet duurzame quarantaine terug naar PREPARED met dezelfde IDs."""
        now = time.time()
        with self._lock, closing(self._connect()) as db, db:
            self._begin_immediate(db)
            try:
                params: list[Any] = [
                    self.engine_version,
                    STATE_QUARANTINED,
                ]
                region_clause = ""
                if region_id is not None:
                    region_clause = " AND region_id=?"
                    params.append(region_id)
                rows = db.execute(
                    f"""
                    SELECT * FROM observation_batches
                    WHERE engine_version=? AND state=? {region_clause}
                    ORDER BY created_at, region_id, batch_id
                    """,
                    params,
                ).fetchall()
                for row in rows:
                    db.execute(
                        """
                        UPDATE observation_batches
                        SET state=?, last_error=NULL, updated_at=?
                        WHERE engine_version=? AND region_id=? AND batch_id=?
                        """,
                        (
                            STATE_PREPARED,
                            now,
                            self.engine_version,
                            row["region_id"],
                            row["batch_id"],
                        ),
                    )
                    self._insert_transition(
                        db,
                        region_id=row["region_id"],
                        batch_id=row["batch_id"],
                        previous_state=STATE_QUARANTINED,
                        new_state=STATE_PREPARED,
                        detail="operator_retry",
                    )
                db.commit()
            except Exception:
                db.rollback()
                raise
            return tuple(
                self.load_batch(
                    region_id=row["region_id"],
                    batch_id=row["batch_id"],
                    connection=db,
                )
                for row in rows
            )

    @_durable_operation
    def load_batch(
        self,
        *,
        region_id: str,
        batch_id: str,
        connection: sqlite3.Connection | None = None,
    ) -> DurableBatchRecord | None:
        """Lees één batch inclusief reconstructeerbare input."""
        if connection is not None:
            row = self._fetch_batch_row(
                connection, region_id=region_id, batch_id=batch_id
            )
            return (
                self._record_from_row(
                    connection, row, include_payload=True
                )
                if row is not None
                else None
            )
        with self._lock, closing(self._connect()) as db, db:
            row = self._fetch_batch_row(
                db, region_id=region_id, batch_id=batch_id
            )
            return (
                self._record_from_row(db, row, include_payload=True)
                if row is not None
                else None
            )

    @_durable_operation
    def load_tracking_head(
        self,
        region_id: str,
    ) -> DurableTrackingHead | None:
        with self._lock, closing(self._connect()) as db, db:
            row = self._fetch_head_row(db, region_id)
            return self._head_from_row(row) if row is not None else None

    @_durable_operation
    def recent_committed_batch_ids(
        self,
        region_id: str,
        *,
        limit: int = 2048,
    ) -> tuple[str, ...]:
        if limit <= 0:
            return ()
        with self._lock, closing(self._connect()) as db, db:
            rows = db.execute(
                """
                SELECT batch_id
                FROM observation_batches
                WHERE engine_version=? AND region_id=? AND state=?
                ORDER BY committed_at DESC
                LIMIT ?
                """,
                (
                    self.engine_version,
                    region_id,
                    STATE_COMMITTED,
                    int(limit),
                ),
            ).fetchall()
        return tuple(reversed([str(row[0]) for row in rows]))

    @_durable_operation
    def integrity_check(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as db, db:
            integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
            foreign_keys = db.execute("PRAGMA foreign_key_check").fetchall()
            impossible = db.execute(
                """
                SELECT count(*)
                FROM observation_batches b
                LEFT JOIN tracking_state_versions v
                  ON v.engine_version=b.engine_version
                 AND v.region_id=b.region_id
                 AND v.batch_id=b.batch_id
                WHERE b.engine_version=? AND (
                    (b.state='COMMITTED' AND v.batch_id IS NULL)
                    OR (b.state<>'COMMITTED' AND v.batch_id IS NOT NULL)
                )
                """,
                (self.engine_version,),
            ).fetchone()[0]
        return {
            "integrity": str(integrity),
            "foreign_key_errors": len(foreign_keys),
            "receipt_state_errors": int(impossible),
            "healthy": (
                integrity == "ok"
                and not foreign_keys
                and not impossible
            ),
        }

    @_durable_operation
    def statistics(
        self,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> dict[str, int]:
        def _read(db: sqlite3.Connection) -> dict[str, int]:
            states = {
                str(state): int(count)
                for state, count in db.execute(
                    """
                    SELECT state, count(*)
                    FROM observation_batches
                    WHERE engine_version=?
                    GROUP BY state
                    """,
                    (self.engine_version,),
                )
            }
            return {
                "observations": int(db.execute(
                    "SELECT count(*) FROM durable_observations"
                ).fetchone()[0]),
                "received": int(db.execute(
                    """
                    SELECT count(*) FROM durable_ingest_queue
                    WHERE engine_version=? AND state='RECEIVED'
                    """,
                    (self.engine_version,),
                ).fetchone()[0]),
                "deduplicated": int(db.execute(
                    """
                    SELECT count(*) FROM durable_ingest_queue
                    WHERE engine_version=? AND state='DEDUPED'
                    """,
                    (self.engine_version,),
                ).fetchone()[0]),
                "batches": sum(states.values()),
                "prepared": states.get(STATE_PREPARED, 0),
                "executing": states.get(STATE_EXECUTING, 0),
                "result_staged": states.get(STATE_RESULT_STAGED, 0),
                "committed": states.get(STATE_COMMITTED, 0),
                "quarantined": states.get(STATE_QUARANTINED, 0),
                "partitioned": states.get(STATE_PARTITIONED, 0),
            }

        if connection is not None:
            return _read(connection)
        with self._lock, closing(self._connect()) as db, db:
            return _read(db)

    def _record_from_row(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        include_payload: bool = False,
    ) -> DurableBatchRecord:
        observations: tuple[Observation, ...] = ()
        result_snapshots: tuple[dict[str, Any], ...] = ()
        if include_payload:
            payload_rows = db.execute(
                """
                SELECT o.payload_json
                FROM observation_batch_items i
                JOIN durable_observations o
                  ON o.region_id=i.region_id
                 AND o.observation_id=i.observation_id
                WHERE i.engine_version=? AND i.region_id=? AND i.batch_id=?
                ORDER BY i.ordinal
                """,
                (
                    self.engine_version,
                    row["region_id"],
                    row["batch_id"],
                ),
            ).fetchall()
            observations = tuple(
                deserialize_observation(json.loads(payload[0]))
                for payload in payload_rows
            )
            if row["result_json"]:
                result_snapshots = tuple(json.loads(row["result_json"]))
        return DurableBatchRecord(
            engine_version=str(row["engine_version"]),
            region_id=str(row["region_id"]),
            batch_id=str(row["batch_id"]),
            root_batch_id=str(row["root_batch_id"]),
            parent_batch_id=row["parent_batch_id"],
            state=str(row["state"]),
            start_revision=int(row["start_revision"]),
            input_digest=str(row["input_digest"]),
            start_state_digest=str(row["start_state_digest"]),
            observation_count=int(row["observation_count"]),
            result_digest=row["result_digest"],
            attempt_count=int(row["attempt_count"]),
            last_error=row["last_error"],
            lineage=json.loads(row["lineage_json"] or "{}"),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            committed_at=(
                float(row["committed_at"])
                if row["committed_at"] is not None
                else None
            ),
            observations=observations,
            result_snapshots=result_snapshots,
        )

    def _head_from_row(self, row: sqlite3.Row) -> DurableTrackingHead:
        snapshots = json.loads(row["snapshot_json"])
        digest = tracking_state_digest(snapshots)
        if digest != row["snapshot_digest"]:
            raise DurableBatchConflict(
                "duurzame trackinghead heeft een ongeldige digest"
            )
        return DurableTrackingHead(
            engine_version=str(row["engine_version"]),
            region_id=str(row["region_id"]),
            revision=int(row["current_revision"]),
            snapshots=tuple(snapshots),
            snapshot_digest=str(row["snapshot_digest"]),
            last_batch_id=row["last_batch_id"],
            updated_at=float(row["updated_at"]),
        )

    def _head_from_version_row(
        self,
        row: sqlite3.Row,
    ) -> DurableTrackingHead:
        snapshots = json.loads(row["snapshot_json"])
        digest = tracking_state_digest(snapshots)
        if digest != row["snapshot_digest"]:
            raise DurableBatchConflict(
                "duurzame trackingversion heeft een ongeldige digest"
            )
        return DurableTrackingHead(
            engine_version=str(row["engine_version"]),
            region_id=str(row["region_id"]),
            revision=int(row["revision"]),
            snapshots=tuple(snapshots),
            snapshot_digest=str(row["snapshot_digest"]),
            last_batch_id=row["batch_id"],
            updated_at=float(row["created_at"]),
        )

    def _assert_same_batch_identity(
        self,
        row: sqlite3.Row,
        *,
        input_digest: str,
        observation_count: int,
        root_batch_id: str,
        parent_batch_id: str | None,
    ) -> None:
        immutable = (
            row["input_digest"] == input_digest,
            int(row["observation_count"]) == observation_count,
            row["root_batch_id"] == root_batch_id,
            row["parent_batch_id"] == parent_batch_id,
        )
        if not all(immutable):
            raise DurableBatchConflict(
                f"batch-ID {row['batch_id']} werd met andere inhoud hergebruikt"
            )

    @staticmethod
    def _observation_storage_row(
        observation: Observation,
    ) -> tuple[Any, ...]:
        payload = serialize_observation(observation)
        payload_json = canonical_json(payload)
        payload_digest = hashlib.sha256(
            payload_json.encode("utf-8")
        ).hexdigest()
        identity_payload = dict(payload)
        # Ontvangsttijd is transportprovenance, niet de meteorologische
        # identiteit. Een QoS-/providerredelivery mag daarom geen tweede
        # toepassing creëren alleen omdat ze later aankwam.
        identity_payload.pop("received_at", None)
        observation_id = _digest({
            "schema": "stv3-observation-identity-v1",
            "observation": identity_payload,
        })
        return (
            observation_id,
            float(observation.timestamp),
            (
                float(observation.received_at)
                if observation.received_at is not None
                else None
            ),
            str(observation.source),
            observation.obs_type.value,
            payload_digest,
            payload_json,
        )

    @staticmethod
    def _upsert_observation_row(
        db: sqlite3.Connection,
        *,
        region_id: str,
        row: tuple[Any, ...],
        persisted_at: float,
    ) -> None:
        db.execute(
            """
            INSERT OR IGNORE INTO durable_observations(
                region_id, observation_id, observed_at,
                received_at, source, observation_type,
                payload_digest, payload_json, persisted_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (region_id, *row, persisted_at),
        )
        persisted = db.execute(
            """
            SELECT observation_id
            FROM durable_observations
            WHERE region_id=? AND observation_id=?
            """,
            (region_id, row[0]),
        ).fetchone()
        if persisted is None or persisted[0] != row[0]:
            raise DurableBatchConflict(
                "observation_id verwijst naar andere payload"
            )

    @staticmethod
    def _assert_head_matches(
        row: sqlite3.Row,
        *,
        start_revision: int,
        start_state_digest: str,
    ) -> None:
        if int(row["current_revision"]) != start_revision:
            raise DurableRevisionConflict(
                "duurzame revision wijkt af van batchbeginsituatie"
            )
        if row["snapshot_digest"] != start_state_digest:
            raise DurableRevisionConflict(
                "duurzame startstate wijkt af bij gelijke revision"
            )

    def _required_batch_row(
        self,
        db: sqlite3.Connection,
        region_id: str,
        batch_id: str,
    ) -> sqlite3.Row:
        row = self._fetch_batch_row(
            db, region_id=region_id, batch_id=batch_id
        )
        if row is None:
            raise KeyError(f"onbekende durable batch {region_id}/{batch_id}")
        return row

    def _fetch_batch_row(
        self,
        db: sqlite3.Connection,
        *,
        region_id: str,
        batch_id: str,
    ) -> sqlite3.Row | None:
        return db.execute(
            """
            SELECT *
            FROM observation_batches
            WHERE engine_version=? AND region_id=? AND batch_id=?
            """,
            (self.engine_version, region_id, batch_id),
        ).fetchone()

    def _fetch_head_row(
        self,
        db: sqlite3.Connection,
        region_id: str,
    ) -> sqlite3.Row | None:
        return db.execute(
            """
            SELECT *
            FROM tracking_state_heads
            WHERE engine_version=? AND region_id=?
            """,
            (self.engine_version, region_id),
        ).fetchone()

    def _required_head_row(
        self,
        db: sqlite3.Connection,
        region_id: str,
    ) -> sqlite3.Row:
        row = self._fetch_head_row(db, region_id)
        if row is None:
            raise DurableStateConflict(
                f"trackinghead ontbreekt voor regio {region_id}"
            )
        return row

    def _required_batch_version(
        self,
        db: sqlite3.Connection,
        *,
        region_id: str,
        batch_id: str,
    ) -> sqlite3.Row:
        row = db.execute(
            """
            SELECT *
            FROM tracking_state_versions
            WHERE engine_version=? AND region_id=? AND batch_id=?
            """,
            (self.engine_version, region_id, batch_id),
        ).fetchone()
        if row is None:
            raise DurableStateConflict(
                "commitreceipt heeft geen stateversion"
            )
        return row

    def _insert_transition(
        self,
        db: sqlite3.Connection,
        *,
        region_id: str,
        batch_id: str,
        previous_state: str | None,
        new_state: str,
        error_category: str | None = None,
        detail: str | None = None,
    ) -> None:
        if new_state not in _ALL_STATES:
            raise ValueError(f"ongeldige durable state {new_state}")
        db.execute(
            """
            INSERT INTO batch_state_transitions(
                engine_version, region_id, batch_id, previous_state,
                new_state, transitioned_at, actor, build_id,
                error_category, detail
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.engine_version,
                region_id,
                batch_id,
                previous_state,
                new_state,
                time.time(),
                self.actor,
                self.build_id,
                error_category,
                detail,
            ),
        )

    @staticmethod
    def _abandon_active_attempt(
        db: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        terminal_state: str,
        error_detail: str,
    ) -> None:
        attempt_id = row["active_attempt_id"]
        if not attempt_id:
            return
        db.execute(
            """
            UPDATE processing_attempts
            SET terminal_at=?, terminal_state=?,
                error_category=?, error_detail=?
            WHERE attempt_id=? AND terminal_at IS NULL
            """,
            (
                time.time(),
                terminal_state,
                terminal_state,
                error_detail,
                attempt_id,
            ),
        )

    @staticmethod
    def _required_id(value: str, name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} moet een niet-lege string zijn")
        return value.strip()

    @staticmethod
    def _begin_immediate(db: sqlite3.Connection) -> None:
        db.execute("BEGIN IMMEDIATE")

    def _fault(self, checkpoint: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(checkpoint)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(
            self.path,
            timeout=20,
            isolation_level=None,
        )
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA journal_mode=WAL")
            # Receipts zijn correctnessdata, geen vervangbare analysecache.
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA busy_timeout=20000")
            return db
        except BaseException:
            db.close()
            raise

    @staticmethod
    def _validate_initialized_schema(db: sqlite3.Connection) -> None:
        """Fail closed when CREATE IF NOT EXISTS hid an incompatible table."""
        required_columns = {
            "durable_ingest_queue": {
                "sequence",
                "engine_version",
                "region_id",
                "observation_id",
                "state",
                "batch_id",
            },
            "durable_regions": {
                "engine_version",
                "region_id",
                "center_lat",
                "center_lon",
                "observation_radius_km",
            },
            "durable_region_targets": {
                "engine_version",
                "target_id",
                "region_id",
                "last_lat",
                "last_lon",
            },
            "observation_batches": {
                "engine_version",
                "region_id",
                "batch_id",
                "state",
                "start_revision",
                "start_state_digest",
                "result_json",
            },
            "processing_attempts": {
                "attempt_id",
                "batch_id",
                "start_revision",
                "start_state_digest",
                "terminal_state",
            },
            "tracking_state_heads": {
                "region_id",
                "current_revision",
                "snapshot_digest",
                "snapshot_json",
            },
        }
        for table, required in required_columns.items():
            actual = {
                str(row[1])
                for row in db.execute(f"PRAGMA table_info({table})")
            }
            missing = required - actual
            if missing:
                raise DurableProcessingError(
                    f"duurzame tabel {table} mist kolommen: "
                    f"{', '.join(sorted(missing))}"
                )
        schema_sql = {
            str(row[0]): str(row[1] or "")
            for row in db.execute(
                """
                SELECT name, sql FROM sqlite_master
                WHERE type='table' AND name IN (
                    'durable_ingest_queue', 'observation_batches'
                )
                """
            )
        }
        if "DEDUPED" not in schema_sql.get("durable_ingest_queue", ""):
            raise DurableProcessingError(
                "duurzame ingesttabel ondersteunt DEDUPED niet"
            )
        if "PARTITIONED" not in schema_sql.get("observation_batches", ""):
            raise DurableProcessingError(
                "duurzame batchtabel ondersteunt PARTITIONED niet"
            )
