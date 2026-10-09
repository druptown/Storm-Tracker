"""Append-only SQLite-opslag voor grootschalige providervergelijking."""
from __future__ import annotations

from contextlib import closing, contextmanager
from copy import deepcopy
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
import threading
import zlib

from .bounded_payload import decompress_limited
from .provider_bias import profile_confidence, scope_from_region_key


SCHEMA_VERSION = 12
MAX_ORIGINAL_FRAME_COMPRESSED_BYTES = 64 * 1024 * 1024
MAX_ORIGINAL_FRAME_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
WORKER_CALIBRATION_STAGE_ROOT = Path("/share/storm_tracker_worker")
MAX_WORKER_CALIBRATION_STAGE_BYTES = 64 * 1024 * 1024
WORKER_CALIBRATION_STAGE_CONTENT_TYPE = (
    "application/vnd.stv3.calibration-stage+sqlite3"
)
WORKER_CALIBRATION_STAGE_TABLES = {
    "manifest": ("key", "value"),
    "frames": (
        "region_key", "source", "nominal_minute", "product_timestamp",
        "collected_at", "grid_deg", "observation_count",
        "grid_point_count", "coverage_lon_min", "coverage_lat_min",
        "coverage_lon_max", "coverage_lat_max", "coverage_fraction",
    ),
    "frame_points": (
        "region_key", "source", "nominal_minute", "grid_lat", "grid_lon",
        "max_intensity", "max_quality", "observation_count",
    ),
    "comparisons": (
        "region_key", "source_a", "source_b", "nominal_minute",
        "compared_at", "primary_cells", "reference_cells",
        "overlap_cells", "false_positive_cells", "missed_cells",
        "precision", "recall", "f1_score", "shared_coverage_fraction",
    ),
    "original_radar_frames": (
        "region_id", "source", "nominal_timestamp", "saved_at",
        "encoding", "payload",
    ),
    "provider_bias_samples": (
        "region_key", "scope_key", "from_source", "to_source",
        "nominal_minute", "compared_at", "from_cells", "to_cells",
        "overlap_cells", "detection_ratio", "extra_fraction",
        "wet_area_ratio", "f1_score", "intensity_bias",
        "shift_lat_cells", "shift_lon_cells", "latency_seconds",
    ),
}

_CALIBRATION_RESET_TABLES = (
    "frame_points",
    "comparisons",
    "frames",
    "forecast_verification_samples",
    "pysteps_preflight_samples",
    "system_performance_samples",
    "pysteps_frame_cache",
    "original_radar_frames",
    "provider_bias_samples",
    "provider_bias_profiles",
    "lightning_aggregates",
    "provider_cycle_samples",
    "ground_quality_assessments",
    "ground_confirmation_decisions",
    "ground_observations",
)


class CalibrationDataStore:
    """Schrijf compacte frames, rasterpunten en vergelijkingen transactioneel."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._totals = {
            "frames": 0, "datapoints": 0, "comparisons": 0,
            "verification_samples": 0, "warning_samples": 0,
            "pysteps_preflight_samples": 0,
            "system_performance_samples": 0,
            "lightning_aggregate_cells": 0,
            "lightning_strikes": 0,
            "provider_cycle_samples": 0,
            "ground_observations": 0,
            "ground_quality_assessments": 0,
            "ground_confirmation_decisions": 0,
            "bias_samples": 0, "bias_profiles": 0,
        }
        self._sources: set[str] = set()
        self._regions: set[str] = set()
        self._oldest_timestamp: float | None = None
        self._newest_timestamp: float | None = None
        self._last_reset_at: str | None = None
        self._reset_reason: str | None = None
        self._analysis_summary: dict = {}
        self._analysis_batches_since_refresh = 0

    @contextmanager
    def _restore_runtime_state_on_rollback(self):
        """Keep cheap in-memory counters consistent with SQLite rollback."""
        snapshot = {
            "totals": dict(self._totals),
            "sources": set(self._sources),
            "regions": set(self._regions),
            "oldest": self._oldest_timestamp,
            "newest": self._newest_timestamp,
            "analysis": deepcopy(self._analysis_summary),
            "analysis_batches": self._analysis_batches_since_refresh,
        }
        try:
            yield
        except BaseException:
            self._totals = snapshot["totals"]
            self._sources = snapshot["sources"]
            self._regions = snapshot["regions"]
            self._oldest_timestamp = snapshot["oldest"]
            self._newest_timestamp = snapshot["newest"]
            self._analysis_summary = snapshot["analysis"]
            self._analysis_batches_since_refresh = snapshot[
                "analysis_batches"
            ]
            raise

    def initialize(self) -> dict:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db, db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS frames (
                    id INTEGER PRIMARY KEY,
                    region_key TEXT NOT NULL,
                    source TEXT NOT NULL,
                    nominal_minute INTEGER NOT NULL,
                    product_timestamp REAL NOT NULL,
                    collected_at REAL NOT NULL,
                    grid_deg REAL NOT NULL,
                    observation_count INTEGER NOT NULL,
                    grid_point_count INTEGER NOT NULL,
                    coverage_lon_min REAL,
                    coverage_lat_min REAL,
                    coverage_lon_max REAL,
                    coverage_lat_max REAL,
                    coverage_fraction REAL NOT NULL DEFAULT 1.0,
                    UNIQUE(region_key, source, nominal_minute)
                );
                CREATE TABLE IF NOT EXISTS frame_points (
                    frame_id INTEGER NOT NULL REFERENCES frames(id) ON DELETE CASCADE,
                    grid_lat INTEGER NOT NULL,
                    grid_lon INTEGER NOT NULL,
                    max_intensity REAL,
                    max_quality REAL,
                    observation_count INTEGER NOT NULL,
                    PRIMARY KEY(frame_id, grid_lat, grid_lon)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS comparisons (
                    id INTEGER PRIMARY KEY,
                    region_key TEXT NOT NULL,
                    source_a TEXT NOT NULL,
                    source_b TEXT NOT NULL,
                    nominal_minute INTEGER NOT NULL,
                    compared_at REAL NOT NULL,
                    primary_cells INTEGER NOT NULL,
                    reference_cells INTEGER NOT NULL,
                    overlap_cells INTEGER NOT NULL,
                    false_positive_cells INTEGER NOT NULL,
                    missed_cells INTEGER NOT NULL,
                    precision REAL,
                    recall REAL,
                    f1_score REAL,
                    shared_coverage_fraction REAL NOT NULL DEFAULT 1.0,
                    UNIQUE(region_key, source_a, source_b, nominal_minute)
                );
                CREATE INDEX IF NOT EXISTS idx_frames_source_time
                    ON frames(source, nominal_minute);
                CREATE INDEX IF NOT EXISTS idx_frames_region_time
                    ON frames(region_key, nominal_minute);
                CREATE INDEX IF NOT EXISTS idx_comparisons_pair_time
                    ON comparisons(source_a, source_b, nominal_minute);
                CREATE TABLE IF NOT EXISTS forecast_verification_samples (
                    id INTEGER PRIMARY KEY,
                    sample_key TEXT NOT NULL UNIQUE,
                    sample_type TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    nominal_minute INTEGER NOT NULL,
                    observed_at REAL NOT NULL,
                    latitude REAL,
                    longitude REAL,
                    buienradar_average_mm_h REAL,
                    buienradar_total_mm REAL,
                    own_status TEXT,
                    own_distance_km REAL,
                    own_eta_minutes REAL,
                    own_passage TEXT,
                    own_confidence TEXT,
                    own_forecast_available INTEGER NOT NULL,
                    warning_stage TEXT,
                    snapshot_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_verification_target_time
                    ON forecast_verification_samples(target_id, nominal_minute);
                CREATE INDEX IF NOT EXISTS idx_verification_type_time
                    ON forecast_verification_samples(sample_type, nominal_minute);
                CREATE TABLE IF NOT EXISTS pysteps_preflight_samples (
                    id INTEGER PRIMARY KEY,
                    region_key TEXT NOT NULL,
                    source TEXT NOT NULL,
                    nominal_minute INTEGER NOT NULL,
                    evaluated_at REAL NOT NULL,
                    status TEXT NOT NULL,
                    frames_used INTEGER NOT NULL,
                    translation_km_per_hour REAL,
                    heading_deg REAL,
                    verification_iou REAL,
                    method TEXT,
                    confidence TEXT,
                    motion_match_iou REAL,
                    vector_consistency_deg REAL,
                    wet_cell_ratio REAL,
                    centroid_translation_km_per_hour REAL,
                    centroid_heading_deg REAL,
                    reason TEXT,
                    UNIQUE(region_key, source, nominal_minute)
                );
                CREATE INDEX IF NOT EXISTS idx_pysteps_preflight_region_time
                    ON pysteps_preflight_samples(region_key, nominal_minute);
                CREATE INDEX IF NOT EXISTS idx_pysteps_preflight_source_time
                    ON pysteps_preflight_samples(source, nominal_minute);
                CREATE TABLE IF NOT EXISTS system_performance_samples (
                    id INTEGER PRIMARY KEY,
                    sample_key TEXT NOT NULL UNIQUE,
                    cycle_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    finished_at REAL NOT NULL,
                    duration_ms REAL NOT NULL,
                    outcome TEXT NOT NULL,
                    cpu_before REAL,
                    cpu_after REAL,
                    temperature_before_c REAL,
                    temperature_after_c REAL,
                    memory_before_percent REAL,
                    memory_after_percent REAL,
                    load_before REAL,
                    load_after REAL,
                    fan_before REAL,
                    fan_after REAL,
                    snapshot_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_system_performance_cycle
                    ON system_performance_samples(cycle_id, started_at);
                CREATE INDEX IF NOT EXISTS idx_system_performance_stage
                    ON system_performance_samples(stage, started_at);
                CREATE TABLE IF NOT EXISTS pysteps_frame_cache (
                    region_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    nominal_timestamp INTEGER NOT NULL,
                    saved_at REAL NOT NULL,
                    frame_json TEXT NOT NULL,
                    PRIMARY KEY(region_id, source, nominal_timestamp)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_pysteps_frame_cache_time
                    ON pysteps_frame_cache(nominal_timestamp);
                CREATE TABLE IF NOT EXISTS original_radar_frames (
                    region_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    nominal_timestamp INTEGER NOT NULL,
                    saved_at REAL NOT NULL,
                    encoding TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY(region_id, source, nominal_timestamp)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_original_radar_frames_time
                    ON original_radar_frames(nominal_timestamp);
                CREATE TABLE IF NOT EXISTS provider_bias_samples (
                    id INTEGER PRIMARY KEY,
                    region_key TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    from_source TEXT NOT NULL,
                    to_source TEXT NOT NULL,
                    nominal_minute INTEGER NOT NULL,
                    compared_at REAL NOT NULL,
                    from_cells INTEGER NOT NULL,
                    to_cells INTEGER NOT NULL,
                    overlap_cells INTEGER NOT NULL,
                    detection_ratio REAL,
                    extra_fraction REAL,
                    wet_area_ratio REAL,
                    f1_score REAL,
                    intensity_bias REAL,
                    shift_lat_cells REAL,
                    shift_lon_cells REAL,
                    latency_seconds REAL,
                    UNIQUE(
                        region_key, from_source, to_source, nominal_minute
                    )
                );
                CREATE INDEX IF NOT EXISTS idx_bias_samples_pair_scope
                    ON provider_bias_samples(
                        from_source, to_source, scope_key, nominal_minute
                    );
                CREATE TABLE IF NOT EXISTS provider_bias_profiles (
                    scope_key TEXT NOT NULL,
                    from_source TEXT NOT NULL,
                    to_source TEXT NOT NULL,
                    sample_count INTEGER NOT NULL,
                    mean_detection_ratio REAL,
                    mean_extra_fraction REAL,
                    mean_wet_area_ratio REAL,
                    mean_f1_score REAL,
                    mean_intensity_bias REAL,
                    mean_shift_lat_cells REAL,
                    mean_shift_lon_cells REAL,
                    mean_latency_seconds REAL,
                    confidence TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(scope_key, from_source, to_source)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS lightning_aggregates (
                    nominal_minute INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    grid_lat INTEGER NOT NULL,
                    grid_lon INTEGER NOT NULL,
                    grid_deg REAL NOT NULL,
                    strike_count INTEGER NOT NULL,
                    latitude_sum REAL NOT NULL,
                    longitude_sum REAL NOT NULL,
                    latitude_min REAL NOT NULL,
                    latitude_max REAL NOT NULL,
                    longitude_min REAL NOT NULL,
                    longitude_max REAL NOT NULL,
                    first_timestamp REAL NOT NULL,
                    last_timestamp REAL NOT NULL,
                    region_ids_json TEXT NOT NULL,
                    PRIMARY KEY(
                        nominal_minute, source, grid_lat, grid_lon, grid_deg
                    )
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_lightning_aggregates_time
                    ON lightning_aggregates(nominal_minute);
                CREATE TABLE IF NOT EXISTS provider_cycle_samples (
                    sample_key TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    nominal_minute INTEGER NOT NULL,
                    observed_at REAL NOT NULL,
                    latitude REAL,
                    longitude REAL,
                    metrics_json TEXT NOT NULL
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_provider_cycles_provider_time
                    ON provider_cycle_samples(provider, nominal_minute);
                CREATE INDEX IF NOT EXISTS idx_provider_cycles_scope_time
                    ON provider_cycle_samples(scope_key, nominal_minute);
                CREATE TABLE IF NOT EXISTS ground_observations (
                    record_key TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    region_key TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    observed_at REAL NOT NULL,
                    received_at REAL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    precipitation_present INTEGER,
                    quantitative_precipitation INTEGER NOT NULL,
                    rain_mm REAL,
                    rain_5min REAL,
                    rain_60min REAL,
                    measurement_kind TEXT,
                    measurement_unit TEXT,
                    interval_minutes REAL,
                    parser_version TEXT,
                    interpretation_reason TEXT,
                    quality_flags_json TEXT NOT NULL,
                    raw_text TEXT,
                    raw_payload_json TEXT
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_ground_obs_region_time
                    ON ground_observations(region_key, observed_at);
                CREATE INDEX IF NOT EXISTS idx_ground_obs_station_time
                    ON ground_observations(provider, station_id, observed_at);
                CREATE TABLE IF NOT EXISTS ground_quality_assessments (
                    decision_key TEXT NOT NULL,
                    observation_key TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    region_key TEXT NOT NULL,
                    evaluated_at REAL NOT NULL,
                    station_id TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    storm_id TEXT,
                    age_seconds REAL,
                    distance_target_km REAL,
                    distance_storm_km REAL,
                    wet INTEGER NOT NULL,
                    fresh INTEGER NOT NULL,
                    eligible INTEGER NOT NULL,
                    quality_score REAL NOT NULL,
                    reasons_json TEXT NOT NULL,
                    PRIMARY KEY(decision_key, observation_key)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_ground_quality_target_time
                    ON ground_quality_assessments(target_id, evaluated_at);
                CREATE TABLE IF NOT EXISTS ground_confirmation_decisions (
                    decision_key TEXT PRIMARY KEY,
                    target_id TEXT NOT NULL,
                    region_key TEXT NOT NULL,
                    evaluated_at REAL NOT NULL,
                    confirmed INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    confidence_percent INTEGER NOT NULL,
                    score REAL NOT NULL,
                    storm_ids_json TEXT NOT NULL,
                    sources_json TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_ground_decisions_target_time
                    ON ground_confirmation_decisions(target_id, evaluated_at);
            """)
            stored_schema = db.execute(
                "SELECT value FROM metadata WHERE key='schema_version'"
            ).fetchone()
            stored_schema_version = (
                int(stored_schema[0]) if stored_schema is not None else 0
            )
            if stored_schema_version < 4:
                self._reset_for_schema_v4(
                    db,
                    previous_version=stored_schema_version,
                    existing_database=(
                        stored_schema is not None
                        or bool(
                            db.execute(
                                "SELECT count(*) FROM frames"
                            ).fetchone()[0]
                        )
                        or bool(
                            db.execute(
                                "SELECT count(*) FROM comparisons"
                            ).fetchone()[0]
                        )
                    ),
                )
            if stored_schema_version < 11 and stored_schema is not None:
                self._reset_for_original_frames_v11(
                    db, previous_version=stored_schema_version
                )
            elif (
                db.execute(
                    "SELECT count(*) FROM provider_bias_samples"
                ).fetchone()[0]
                and not db.execute(
                    "SELECT count(*) FROM provider_bias_profiles"
                ).fetchone()[0]
            ):
                # Zelfherstel na een uitzonderlijke onderbroken profielopbouw.
                self._rebuild_all_bias_profiles(db)
            self._ensure_preflight_v6_columns(db)
            db.execute(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            reset_at = db.execute(
                "SELECT value FROM metadata WHERE key='last_reset_at'"
            ).fetchone()
            reset_reason = db.execute(
                "SELECT value FROM metadata WHERE key='last_reset_reason'"
            ).fetchone()
            self._last_reset_at = reset_at[0] if reset_at else None
            self._reset_reason = reset_reason[0] if reset_reason else None
            self._totals = {
                "frames": db.execute("SELECT count(*) FROM frames").fetchone()[0],
                "datapoints": db.execute(
                    "SELECT coalesce(sum(grid_point_count), 0) FROM frames"
                ).fetchone()[0],
                "comparisons": db.execute(
                    "SELECT count(*) FROM comparisons"
                ).fetchone()[0],
                "verification_samples": db.execute(
                    "SELECT count(*) FROM forecast_verification_samples"
                ).fetchone()[0],
                "warning_samples": db.execute(
                    "SELECT count(*) FROM forecast_verification_samples "
                    "WHERE sample_type='warning_sent'"
                ).fetchone()[0],
                "pysteps_preflight_samples": db.execute(
                    "SELECT count(*) FROM pysteps_preflight_samples"
                ).fetchone()[0],
                "system_performance_samples": db.execute(
                    "SELECT count(*) FROM system_performance_samples"
                ).fetchone()[0],
                "lightning_aggregate_cells": db.execute(
                    "SELECT count(*) FROM lightning_aggregates"
                ).fetchone()[0],
                "lightning_strikes": db.execute(
                    "SELECT coalesce(sum(strike_count), 0) "
                    "FROM lightning_aggregates"
                ).fetchone()[0],
                "provider_cycle_samples": db.execute(
                    "SELECT count(*) FROM provider_cycle_samples"
                ).fetchone()[0],
                "ground_observations": db.execute(
                    "SELECT count(*) FROM ground_observations"
                ).fetchone()[0],
                "ground_quality_assessments": db.execute(
                    "SELECT count(*) FROM ground_quality_assessments"
                ).fetchone()[0],
                "ground_confirmation_decisions": db.execute(
                    "SELECT count(*) FROM ground_confirmation_decisions"
                ).fetchone()[0],
                "bias_samples": db.execute(
                    "SELECT count(*) FROM provider_bias_samples"
                ).fetchone()[0],
                "bias_profiles": db.execute(
                    "SELECT count(*) FROM provider_bias_profiles"
                ).fetchone()[0],
            }
            self._sources = {
                row[0] for row in db.execute("SELECT DISTINCT source FROM frames")
            }
            self._regions = {
                row[0] for row in db.execute("SELECT DISTINCT region_key FROM frames")
            }
            bounds = db.execute(
                "SELECT min(product_timestamp), max(product_timestamp) FROM frames"
            ).fetchone()
            self._oldest_timestamp, self._newest_timestamp = bounds
            self._analysis_summary = self._load_analysis_summary(db)
        return self.statistics()

    def _reset_for_original_frames_v11(
        self, db: sqlite3.Connection, *, previous_version: int
    ) -> None:
        """Start the calibration corpus clean at the lossless-frame contract."""
        existing_tables = {
            row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        # Een kalibratiereset mag operationele durable receipts nooit wissen.
        # Gebruik daarom een expliciete allowlist in plaats van alle onbekende
        # tabellen in dezelfde Storm Tracker-database leeg te maken.
        for table in _CALIBRATION_RESET_TABLES:
            if table in existing_tables:
                db.execute(f'DELETE FROM "{table}"')
        reset_at = datetime.now(timezone.utc).isoformat()
        reason = (
            f"schema_v11_original_resolution_clean_reset_from_v"
            f"{previous_version}"
        )
        db.executemany(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES(?, ?)",
            (
                ("last_reset_at", reset_at),
                ("last_reset_reason", reason),
            ),
        )
        self._last_reset_at = reset_at
        self._reset_reason = reason

    def save_pysteps_frames(
        self, frames_by_region: dict[str, list[dict]]
    ) -> int:
        """Bewaar de begrensde operationele framebuffer over deploys heen."""
        saved_at = datetime.now(timezone.utc).timestamp()
        rows = []
        for region_id, frames in frames_by_region.items():
            for frame in frames:
                rows.append((
                    str(region_id),
                    str(frame["source"]),
                    int(frame["nominal_timestamp"]),
                    saved_at,
                    json.dumps(
                        frame, separators=(",", ":"), ensure_ascii=False
                    ),
                ))
        with self._lock, closing(self._connect()) as db, db:
            db.executemany("""
                INSERT INTO pysteps_frame_cache(
                    region_id, source, nominal_timestamp, saved_at, frame_json
                ) VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(region_id, source, nominal_timestamp)
                DO UPDATE SET saved_at=excluded.saved_at,
                    frame_json=excluded.frame_json
            """, rows)
            cutoff = int(saved_at) - 6 * 3600
            db.execute(
                "DELETE FROM pysteps_frame_cache WHERE nominal_timestamp < ?",
                (cutoff,),
            )
        return len(rows)

    def load_pysteps_frames(self) -> dict[str, list[dict]]:
        """Laad recente buffers; raster- en ouderdomscontrole gebeurt in history."""
        with self._lock, closing(self._connect()) as db, db:
            rows = db.execute(
                "SELECT region_id, frame_json FROM pysteps_frame_cache "
                "ORDER BY nominal_timestamp"
            ).fetchall()
        result: dict[str, list[dict]] = {}
        for region_id, payload in rows:
            try:
                frame = json.loads(payload)
            except (TypeError, ValueError):
                continue
            result.setdefault(str(region_id), []).append(frame)
        return result

    def save_original_radar_frames(self, frames: list[dict]) -> int:
        """Persist provider overlays losslessly, without regridding or expiry."""
        saved_at = datetime.now(timezone.utc).timestamp()
        rows = []
        for frame in frames:
            payload = json.dumps(
                frame["overlay"], separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
            rows.append((
                str(frame["region_id"]),
                str(frame["source"]),
                int(frame["nominal_timestamp"]),
                saved_at,
                "zlib-json-v1",
                sqlite3.Binary(zlib.compress(payload, level=6)),
            ))
        if not rows:
            return 0
        with self._lock, closing(self._connect()) as db, db:
            db.executemany("""
                INSERT INTO original_radar_frames(
                    region_id, source, nominal_timestamp, saved_at,
                    encoding, payload
                ) VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(region_id, source, nominal_timestamp)
                DO UPDATE SET saved_at=excluded.saved_at,
                    encoding=excluded.encoding, payload=excluded.payload
            """, rows)
        return len(rows)

    def load_original_radar_frames(
        self, *, region_id: str | None = None, source: str | None = None
    ) -> list[dict]:
        """Load the exact saved provider overlays for replay."""
        clauses = []
        parameters: list[str] = []
        if region_id is not None:
            clauses.append("region_id=?")
            parameters.append(str(region_id))
        if source is not None:
            clauses.append("source=?")
            parameters.append(str(source))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock, closing(self._connect()) as db, db:
            rows = db.execute(
                "SELECT region_id, source, nominal_timestamp, encoding, payload "
                f"FROM original_radar_frames{where} "
                "ORDER BY nominal_timestamp",
                parameters,
            ).fetchall()
        result = []
        for saved_region, saved_source, timestamp, encoding, payload in rows:
            if encoding != "zlib-json-v1":
                continue
            result.append({
                "region_id": saved_region,
                "source": saved_source,
                "nominal_timestamp": timestamp,
                "overlay": json.loads(decompress_limited(
                    payload,
                    codec="zlib",
                    maximum_compressed_bytes=(
                        MAX_ORIGINAL_FRAME_COMPRESSED_BYTES
                    ),
                    maximum_uncompressed_bytes=(
                        MAX_ORIGINAL_FRAME_UNCOMPRESSED_BYTES
                    ),
                )),
            })
        return result

    @staticmethod
    def _ensure_preflight_v6_columns(db: sqlite3.Connection) -> None:
        """Breid schema 5 uit zonder bestaande wetenschappelijke data te wissen."""
        existing = {
            row[1] for row in db.execute(
                "PRAGMA table_info(pysteps_preflight_samples)"
            )
        }
        for name, sql_type in (
            ("method", "TEXT"),
            ("confidence", "TEXT"),
            ("motion_match_iou", "REAL"),
            ("vector_consistency_deg", "REAL"),
            ("wet_cell_ratio", "REAL"),
            ("centroid_translation_km_per_hour", "REAL"),
            ("centroid_heading_deg", "REAL"),
        ):
            if name not in existing:
                db.execute(
                    f"ALTER TABLE pysteps_preflight_samples "
                    f"ADD COLUMN {name} {sql_type}"
                )

    def _reset_for_schema_v4(
        self,
        db: sqlite3.Connection,
        *,
        previous_version: int,
        existing_database: bool,
    ) -> None:
        """Start een wetenschappelijk schone dataset met dekking per frame.

        Schema v3 bevat vergelijkingen waarbij de activatiemarge van een
        provider ten onrechte als volledige radardekking kon gelden. Die data
        kan niet betrouwbaar achteraf worden hersteld en wordt daarom eenmalig
        transactioneel verwijderd.
        """
        db.executescript("""
            BEGIN IMMEDIATE;
            DROP TABLE IF EXISTS provider_bias_profiles;
            DROP TABLE IF EXISTS provider_bias_samples;
            DROP TABLE IF EXISTS forecast_verification_samples;
            DROP TABLE IF EXISTS comparisons;
            DROP TABLE IF EXISTS frame_points;
            DROP TABLE IF EXISTS frames;

            CREATE TABLE frames (
                id INTEGER PRIMARY KEY,
                region_key TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                product_timestamp REAL NOT NULL,
                collected_at REAL NOT NULL,
                grid_deg REAL NOT NULL,
                observation_count INTEGER NOT NULL,
                grid_point_count INTEGER NOT NULL,
                coverage_lon_min REAL,
                coverage_lat_min REAL,
                coverage_lon_max REAL,
                coverage_lat_max REAL,
                coverage_fraction REAL NOT NULL,
                UNIQUE(region_key, source, nominal_minute)
            );
            CREATE TABLE frame_points (
                frame_id INTEGER NOT NULL
                    REFERENCES frames(id) ON DELETE CASCADE,
                grid_lat INTEGER NOT NULL,
                grid_lon INTEGER NOT NULL,
                max_intensity REAL,
                max_quality REAL,
                observation_count INTEGER NOT NULL,
                PRIMARY KEY(frame_id, grid_lat, grid_lon)
            ) WITHOUT ROWID;
            CREATE TABLE comparisons (
                id INTEGER PRIMARY KEY,
                region_key TEXT NOT NULL,
                source_a TEXT NOT NULL,
                source_b TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                compared_at REAL NOT NULL,
                primary_cells INTEGER NOT NULL,
                reference_cells INTEGER NOT NULL,
                overlap_cells INTEGER NOT NULL,
                false_positive_cells INTEGER NOT NULL,
                missed_cells INTEGER NOT NULL,
                precision REAL,
                recall REAL,
                f1_score REAL,
                shared_coverage_fraction REAL NOT NULL,
                UNIQUE(region_key, source_a, source_b, nominal_minute)
            );
            CREATE INDEX idx_frames_source_time
                ON frames(source, nominal_minute);
            CREATE INDEX idx_frames_region_time
                ON frames(region_key, nominal_minute);
            CREATE INDEX idx_comparisons_pair_time
                ON comparisons(source_a, source_b, nominal_minute);
            CREATE TABLE forecast_verification_samples (
                id INTEGER PRIMARY KEY,
                sample_key TEXT NOT NULL UNIQUE,
                sample_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                observed_at REAL NOT NULL,
                latitude REAL,
                longitude REAL,
                buienradar_average_mm_h REAL,
                buienradar_total_mm REAL,
                own_status TEXT,
                own_distance_km REAL,
                own_eta_minutes REAL,
                own_passage TEXT,
                own_confidence TEXT,
                own_forecast_available INTEGER NOT NULL,
                warning_stage TEXT,
                snapshot_json TEXT NOT NULL
            );
            CREATE INDEX idx_verification_target_time
                ON forecast_verification_samples(target_id, nominal_minute);
            CREATE INDEX idx_verification_type_time
                ON forecast_verification_samples(sample_type, nominal_minute);
            CREATE TABLE provider_bias_samples (
                id INTEGER PRIMARY KEY,
                region_key TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                from_source TEXT NOT NULL,
                to_source TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                compared_at REAL NOT NULL,
                from_cells INTEGER NOT NULL,
                to_cells INTEGER NOT NULL,
                overlap_cells INTEGER NOT NULL,
                detection_ratio REAL,
                extra_fraction REAL,
                wet_area_ratio REAL,
                f1_score REAL,
                intensity_bias REAL,
                shift_lat_cells REAL,
                shift_lon_cells REAL,
                latency_seconds REAL,
                UNIQUE(
                    region_key, from_source, to_source, nominal_minute
                )
            );
            CREATE INDEX idx_bias_samples_pair_scope
                ON provider_bias_samples(
                    from_source, to_source, scope_key, nominal_minute
                );
            CREATE TABLE provider_bias_profiles (
                scope_key TEXT NOT NULL,
                from_source TEXT NOT NULL,
                to_source TEXT NOT NULL,
                sample_count INTEGER NOT NULL,
                mean_detection_ratio REAL,
                mean_extra_fraction REAL,
                mean_wet_area_ratio REAL,
                mean_f1_score REAL,
                mean_intensity_bias REAL,
                mean_shift_lat_cells REAL,
                mean_shift_lon_cells REAL,
                mean_latency_seconds REAL,
                confidence TEXT NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(scope_key, from_source, to_source)
            ) WITHOUT ROWID;
            COMMIT;
        """)
        if existing_database:
            reset_at = datetime.now(timezone.utc).isoformat()
            reason = (
                "schema_v4_coverage_contract_clean_reset"
                f"_from_v{previous_version}"
            )
            db.executemany(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES(?, ?)",
                (
                    ("last_reset_at", reset_at),
                    ("last_reset_reason", reason),
                ),
            )
            self._last_reset_at = reset_at
            self._reset_reason = reason

    @staticmethod
    def _stage_sha256(path: Path, expected_bytes: int) -> str:
        digest = hashlib.sha256()
        read_bytes = 0
        with path.open("rb") as stream:
            while True:
                block = stream.read(1024 * 1024)
                if not block:
                    break
                read_bytes += len(block)
                if read_bytes > MAX_WORKER_CALIBRATION_STAGE_BYTES:
                    raise ValueError(
                        "Workerkalibratiestage overschrijdt bytecontract"
                    )
                digest.update(block)
        if read_bytes != expected_bytes:
            raise ValueError("Workerkalibratiestage wijzigde tijdens validatie")
        return digest.hexdigest()

    @staticmethod
    def _validate_stage_schema(db: sqlite3.Connection) -> None:
        objects = db.execute(
            "SELECT type, name FROM stage.sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%'"
        ).fetchall()
        if set(objects) != {
            ("table", name) for name in WORKER_CALIBRATION_STAGE_TABLES
        }:
            raise ValueError("Workerkalibratiestage heeft onbekende objecten")
        for table, expected_columns in WORKER_CALIBRATION_STAGE_TABLES.items():
            columns = tuple(
                str(row[1])
                for row in db.execute(f'PRAGMA stage.table_info("{table}")')
            )
            if columns != expected_columns:
                raise ValueError(
                    f"Workerkalibratiestage heeft fout schema voor {table}"
                )
        manifest = dict(db.execute("SELECT key, value FROM stage.manifest"))
        if (
            manifest.get("stage_schema_version") != "1"
            or manifest.get("algorithm_version")
            != "worker-radar-calibration-v2"
            or not manifest.get("job_id")
        ):
            raise ValueError("Workerkalibratiestage heeft ongeldig manifest")
        integrity = db.execute("PRAGMA stage.quick_check").fetchone()
        if integrity != ("ok",):
            raise ValueError("Workerkalibratiestage faalt integrity_check")

    def merge_worker_calibration_stage(self, metadata: dict) -> dict:
        """Merge a worker-built stage with one short SQL-only transaction."""
        raw_path = metadata.get("stage_path")
        if not isinstance(raw_path, str):
            raise ValueError("Workerkalibratieresultaat mist stagepad")
        unresolved = Path(raw_path)
        if unresolved.is_symlink():
            raise ValueError("Workerkalibratiestage mag geen symlink zijn")
        root = WORKER_CALIBRATION_STAGE_ROOT.resolve()
        path = unresolved.resolve()
        if (
            path.parent != root
            or not path.name.endswith(".calibration.sqlite3")
        ):
            raise ValueError("Workerkalibratiestage staat buiten gedeelde map")
        try:
            expected_bytes = int(metadata.get("stage_bytes", -1))
        except (TypeError, ValueError) as err:
            raise ValueError("Workerkalibratiestage heeft ongeldige grootte") from err
        if (
            expected_bytes <= 0
            or expected_bytes > MAX_WORKER_CALIBRATION_STAGE_BYTES
            or metadata.get("stage_content_type")
            != WORKER_CALIBRATION_STAGE_CONTENT_TYPE
            or path.stat().st_size != expected_bytes
        ):
            raise ValueError("Workerkalibratiestage faalt metadata-envelop")
        expected_sha = str(metadata.get("stage_sha256") or "")
        if self._stage_sha256(path, expected_bytes) != expected_sha:
            raise ValueError("Workerkalibratiestage-controlesom klopt niet")

        with self._lock, closing(self._connect()) as db:
            attached = False
            try:
                db.execute("ATTACH DATABASE ? AS stage", (str(path),))
                attached = True
                self._validate_stage_schema(db)
                changed_frames = db.execute("""
                    SELECT count(*) FROM stage.frames AS s
                    LEFT JOIN frames AS t
                      ON t.region_key=s.region_key AND t.source=s.source
                     AND t.nominal_minute=s.nominal_minute
                    WHERE t.id IS NULL OR NOT (
                        t.product_timestamp IS s.product_timestamp
                        AND t.grid_deg IS s.grid_deg
                        AND t.observation_count IS s.observation_count
                        AND t.grid_point_count IS s.grid_point_count
                        AND t.coverage_lon_min IS s.coverage_lon_min
                        AND t.coverage_lat_min IS s.coverage_lat_min
                        AND t.coverage_lon_max IS s.coverage_lon_max
                        AND t.coverage_lat_max IS s.coverage_lat_max
                        AND t.coverage_fraction IS s.coverage_fraction
                    )
                """).fetchone()[0]
                changed_comparisons = db.execute("""
                    SELECT count(*) FROM stage.comparisons AS s
                    LEFT JOIN comparisons AS t
                      ON t.region_key=s.region_key AND t.source_a=s.source_a
                     AND t.source_b=s.source_b
                     AND t.nominal_minute=s.nominal_minute
                    WHERE t.id IS NULL OR NOT (
                        t.primary_cells IS s.primary_cells
                        AND t.reference_cells IS s.reference_cells
                        AND t.overlap_cells IS s.overlap_cells
                        AND t.false_positive_cells IS s.false_positive_cells
                        AND t.missed_cells IS s.missed_cells
                        AND t.precision IS s.precision AND t.recall IS s.recall
                        AND t.f1_score IS s.f1_score
                        AND t.shared_coverage_fraction
                            IS s.shared_coverage_fraction
                    )
                """).fetchone()[0]
                staged_frames = db.execute(
                    "SELECT count(*) FROM stage.frames"
                ).fetchone()[0]
                staged_original = db.execute(
                    "SELECT count(*) FROM stage.original_radar_frames"
                ).fetchone()[0]
                affected_profiles = {
                    (str(scope), str(source_from), str(source_to))
                    for scope, source_from, source_to in db.execute(
                        "SELECT DISTINCT scope_key, from_source, to_source "
                        "FROM stage.provider_bias_samples"
                    )
                }
                affected_profiles |= {
                    ("*", source_from, source_to)
                    for _scope_key, source_from, source_to
                    in affected_profiles
                }

                db.execute("BEGIN IMMEDIATE")
                db.execute("""
                    DELETE FROM frame_points
                    WHERE EXISTS (
                        SELECT 1 FROM stage.frames AS s
                        JOIN frames AS f
                          ON f.region_key=s.region_key AND f.source=s.source
                         AND f.nominal_minute=s.nominal_minute
                        WHERE f.id=frame_points.frame_id
                    )
                """)
                db.execute("""
                    INSERT INTO frames(
                        region_key, source, nominal_minute, product_timestamp,
                        collected_at, grid_deg, observation_count,
                        grid_point_count, coverage_lon_min, coverage_lat_min,
                        coverage_lon_max, coverage_lat_max, coverage_fraction
                    ) SELECT
                        region_key, source, nominal_minute, product_timestamp,
                        collected_at, grid_deg, observation_count,
                        grid_point_count, coverage_lon_min, coverage_lat_min,
                        coverage_lon_max, coverage_lat_max, coverage_fraction
                    FROM stage.frames WHERE true
                    ON CONFLICT(region_key, source, nominal_minute) DO UPDATE SET
                        product_timestamp=excluded.product_timestamp,
                        collected_at=excluded.collected_at,
                        grid_deg=excluded.grid_deg,
                        observation_count=excluded.observation_count,
                        grid_point_count=excluded.grid_point_count,
                        coverage_lon_min=excluded.coverage_lon_min,
                        coverage_lat_min=excluded.coverage_lat_min,
                        coverage_lon_max=excluded.coverage_lon_max,
                        coverage_lat_max=excluded.coverage_lat_max,
                        coverage_fraction=excluded.coverage_fraction
                """)
                db.execute("""
                    INSERT INTO frame_points(
                        frame_id, grid_lat, grid_lon, max_intensity,
                        max_quality, observation_count
                    ) SELECT
                        f.id, p.grid_lat, p.grid_lon, p.max_intensity,
                        p.max_quality, p.observation_count
                    FROM stage.frame_points AS p
                    JOIN frames AS f
                      ON f.region_key=p.region_key AND f.source=p.source
                     AND f.nominal_minute=p.nominal_minute
                """)
                db.execute("""
                    INSERT INTO comparisons(
                        region_key, source_a, source_b, nominal_minute,
                        compared_at, primary_cells, reference_cells,
                        overlap_cells, false_positive_cells, missed_cells,
                        precision, recall, f1_score,
                        shared_coverage_fraction
                    ) SELECT * FROM stage.comparisons WHERE true
                    ON CONFLICT(region_key, source_a, source_b, nominal_minute)
                    DO UPDATE SET
                        compared_at=excluded.compared_at,
                        primary_cells=excluded.primary_cells,
                        reference_cells=excluded.reference_cells,
                        overlap_cells=excluded.overlap_cells,
                        false_positive_cells=excluded.false_positive_cells,
                        missed_cells=excluded.missed_cells,
                        precision=excluded.precision, recall=excluded.recall,
                        f1_score=excluded.f1_score,
                        shared_coverage_fraction=excluded.shared_coverage_fraction
                """)
                db.execute("""
                    INSERT INTO original_radar_frames(
                        region_id, source, nominal_timestamp, saved_at,
                        encoding, payload
                    ) SELECT * FROM stage.original_radar_frames WHERE true
                    ON CONFLICT(region_id, source, nominal_timestamp)
                    DO UPDATE SET saved_at=excluded.saved_at,
                        encoding=excluded.encoding, payload=excluded.payload
                """)
                db.execute("""
                    INSERT INTO provider_bias_samples(
                        region_key, scope_key, from_source, to_source,
                        nominal_minute, compared_at, from_cells, to_cells,
                        overlap_cells, detection_ratio, extra_fraction,
                        wet_area_ratio, f1_score, intensity_bias,
                        shift_lat_cells, shift_lon_cells, latency_seconds
                    ) SELECT * FROM stage.provider_bias_samples WHERE true
                    ON CONFLICT(region_key, from_source, to_source,
                                nominal_minute) DO UPDATE SET
                        scope_key=excluded.scope_key,
                        compared_at=excluded.compared_at,
                        from_cells=excluded.from_cells,
                        to_cells=excluded.to_cells,
                        overlap_cells=excluded.overlap_cells,
                        detection_ratio=excluded.detection_ratio,
                        extra_fraction=excluded.extra_fraction,
                        wet_area_ratio=excluded.wet_area_ratio,
                        f1_score=excluded.f1_score,
                        intensity_bias=coalesce(
                            excluded.intensity_bias,
                            provider_bias_samples.intensity_bias
                        ),
                        shift_lat_cells=coalesce(
                            excluded.shift_lat_cells,
                            provider_bias_samples.shift_lat_cells
                        ),
                        shift_lon_cells=coalesce(
                            excluded.shift_lon_cells,
                            provider_bias_samples.shift_lon_cells
                        ),
                        latency_seconds=coalesce(
                            excluded.latency_seconds,
                            provider_bias_samples.latency_seconds
                        )
                """)
                self._refresh_bias_profiles(db, affected_profiles)
                if (
                    path.stat().st_size != expected_bytes
                    or self._stage_sha256(path, expected_bytes) != expected_sha
                ):
                    raise ValueError(
                        "Workerkalibratiestage wijzigde tijdens merge"
                    )
                db.commit()

                self._totals["frames"] = db.execute(
                    "SELECT count(*) FROM frames"
                ).fetchone()[0]
                self._totals["datapoints"] = db.execute(
                    "SELECT coalesce(sum(grid_point_count), 0) FROM frames"
                ).fetchone()[0]
                self._totals["comparisons"] = db.execute(
                    "SELECT count(*) FROM comparisons"
                ).fetchone()[0]
                self._totals["bias_samples"] = db.execute(
                    "SELECT count(*) FROM provider_bias_samples"
                ).fetchone()[0]
                self._totals["bias_profiles"] = db.execute(
                    "SELECT count(*) FROM provider_bias_profiles"
                ).fetchone()[0]
                self._sources = {
                    row[0] for row in db.execute(
                        "SELECT DISTINCT source FROM frames"
                    )
                }
                self._regions = {
                    row[0] for row in db.execute(
                        "SELECT DISTINCT region_key FROM frames"
                    )
                }
                bounds = db.execute(
                    "SELECT min(product_timestamp), max(product_timestamp) "
                    "FROM frames"
                ).fetchone()
                self._oldest_timestamp, self._newest_timestamp = bounds
                profiles = self._load_bias_profiles(db)
            except BaseException:
                db.rollback()
                raise
            finally:
                if attached:
                    try:
                        db.execute("DETACH DATABASE stage")
                    except sqlite3.Error:
                        pass
        return {
            "frames_written": int(changed_frames),
            "comparisons_written": int(changed_comparisons),
            "original_radar_frames_written": int(staged_original),
            "frames_staged": int(staged_frames),
            "_bias_profiles": profiles,
            **self.statistics(),
        }

    def write_batch(self, batch: dict) -> dict:
        frames = batch.get("frames", ())
        comparisons = batch.get("comparisons", ())
        verification_samples = batch.get("verification_samples", ())
        pysteps_preflight_samples = batch.get("pysteps_preflight_samples", ())
        system_performance_samples = batch.get(
            "system_performance_samples", ()
        )
        lightning_aggregates = batch.get("lightning_aggregates", ())
        provider_cycle_samples = batch.get("provider_cycle_samples", ())
        ground_observations = batch.get("ground_observations", ())
        ground_quality = batch.get("ground_quality_assessments", ())
        ground_decisions = batch.get("ground_confirmation_decisions", ())
        with (
            self._lock,
            self._restore_runtime_state_on_rollback(),
            closing(self._connect()) as db,
            db,
        ):
            affected_bias_profiles: set[tuple[str, str, str]] = set()
            frames_written = 0
            comparisons_written = 0
            for frame in frames:
                if len(frame) == 9:
                    # Compatibiliteit voor oude interne test-/toolcallers.
                    frame = (
                        *frame[:8],
                        None, None, None, None, 1.0,
                        frame[8],
                    )
                existing = db.execute(
                    "SELECT id, grid_point_count, product_timestamp, grid_deg, "
                    "observation_count, coverage_lon_min, coverage_lat_min, "
                    "coverage_lon_max, coverage_lat_max, coverage_fraction "
                    "FROM frames "
                    "WHERE region_key=? AND source=? AND nominal_minute=?",
                    frame[:3],
                ).fetchone()
                if existing is not None and existing[2:] == (
                    frame[3], frame[5], frame[6],
                    frame[8], frame[9], frame[10], frame[11], frame[12],
                ) and int(existing[1]) == int(frame[7]):
                    continue
                db.execute("""
                    INSERT INTO frames(
                        region_key, source, nominal_minute, product_timestamp,
                        collected_at, grid_deg, observation_count,
                        grid_point_count, coverage_lon_min, coverage_lat_min,
                        coverage_lon_max, coverage_lat_max, coverage_fraction
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(region_key, source, nominal_minute) DO UPDATE SET
                        product_timestamp=excluded.product_timestamp,
                        collected_at=excluded.collected_at,
                        observation_count=excluded.observation_count,
                        grid_point_count=excluded.grid_point_count,
                        coverage_lon_min=excluded.coverage_lon_min,
                        coverage_lat_min=excluded.coverage_lat_min,
                        coverage_lon_max=excluded.coverage_lon_max,
                        coverage_lat_max=excluded.coverage_lat_max,
                        coverage_fraction=excluded.coverage_fraction
                """, frame[:13])
                frame_id = db.execute(
                    "SELECT id FROM frames WHERE region_key=? AND source=? AND nominal_minute=?",
                    frame[:3],
                ).fetchone()[0]
                if existing is None:
                    self._totals["frames"] += 1
                else:
                    self._totals["datapoints"] -= int(existing[1])
                self._totals["datapoints"] += int(frame[7])
                self._sources.add(str(frame[1]))
                self._regions.add(str(frame[0]))
                product_timestamp = float(frame[3])
                self._oldest_timestamp = (
                    product_timestamp if self._oldest_timestamp is None
                    else min(self._oldest_timestamp, product_timestamp)
                )
                self._newest_timestamp = (
                    product_timestamp if self._newest_timestamp is None
                    else max(self._newest_timestamp, product_timestamp)
                )
                db.execute("DELETE FROM frame_points WHERE frame_id=?", (frame_id,))
                db.executemany("""
                    INSERT INTO frame_points(
                        frame_id, grid_lat, grid_lon, max_intensity,
                        max_quality, observation_count
                    ) VALUES(?, ?, ?, ?, ?, ?)
                """, ((frame_id, *point) for point in frame[13]))
                frames_written += 1
            for comparison in comparisons:
                if len(comparison) == 13:
                    comparison = (*comparison, 1.0)
                existing = db.execute(
                    "SELECT primary_cells, reference_cells, overlap_cells, "
                    "false_positive_cells, missed_cells, precision, recall, "
                    "f1_score, shared_coverage_fraction FROM comparisons "
                    "WHERE region_key=? AND source_a=? "
                    "AND source_b=? AND nominal_minute=?",
                    comparison[:4],
                ).fetchone()
                if existing is not None and existing == tuple(comparison[5:]):
                    continue
                if existing is None:
                    self._totals["comparisons"] += 1
                db.execute("""
                INSERT INTO comparisons(
                    region_key, source_a, source_b, nominal_minute, compared_at,
                    primary_cells, reference_cells, overlap_cells,
                    false_positive_cells, missed_cells, precision, recall, f1_score
                    , shared_coverage_fraction
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(region_key, source_a, source_b, nominal_minute)
                DO UPDATE SET
                    compared_at=excluded.compared_at,
                    primary_cells=excluded.primary_cells,
                    reference_cells=excluded.reference_cells,
                    overlap_cells=excluded.overlap_cells,
                    false_positive_cells=excluded.false_positive_cells,
                    missed_cells=excluded.missed_cells,
                    precision=excluded.precision,
                    recall=excluded.recall,
                    f1_score=excluded.f1_score,
                    shared_coverage_fraction=excluded.shared_coverage_fraction
                """, comparison)
                affected_bias_profiles.update(
                    self._upsert_bias_samples_for_comparison(db, comparison)
                )
                comparisons_written += 1
            for sample in verification_samples:
                existed = db.execute(
                    "SELECT sample_type FROM forecast_verification_samples "
                    "WHERE sample_key=?",
                    (sample["sample_key"],),
                ).fetchone()
                payload = json.dumps(
                    sample.get("snapshot", {}),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                db.execute("""
                    INSERT INTO forecast_verification_samples(
                        sample_key, sample_type, target_id, nominal_minute,
                        observed_at, latitude, longitude,
                        buienradar_average_mm_h, buienradar_total_mm,
                        own_status, own_distance_km, own_eta_minutes,
                        own_passage, own_confidence, own_forecast_available,
                        warning_stage, snapshot_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sample_key) DO UPDATE SET
                        observed_at=excluded.observed_at,
                        latitude=excluded.latitude,
                        longitude=excluded.longitude,
                        buienradar_average_mm_h=excluded.buienradar_average_mm_h,
                        buienradar_total_mm=excluded.buienradar_total_mm,
                        own_status=excluded.own_status,
                        own_distance_km=excluded.own_distance_km,
                        own_eta_minutes=excluded.own_eta_minutes,
                        own_passage=excluded.own_passage,
                        own_confidence=excluded.own_confidence,
                        own_forecast_available=excluded.own_forecast_available,
                        warning_stage=excluded.warning_stage,
                        snapshot_json=excluded.snapshot_json
                """, (
                    sample["sample_key"], sample["sample_type"],
                    sample.get("target_id", "home"), sample["nominal_minute"],
                    sample["observed_at"], sample.get("latitude"),
                    sample.get("longitude"), sample.get("buienradar_average_mm_h"),
                    sample.get("buienradar_total_mm"), sample.get("own_status"),
                    sample.get("own_distance_km"), sample.get("own_eta_minutes"),
                    sample.get("own_passage"), sample.get("own_confidence"),
                    int(bool(sample.get("own_forecast_available"))),
                    sample.get("warning_stage"), payload,
                ))
                if existed is None:
                    self._totals["verification_samples"] += 1
                    if sample["sample_type"] == "warning_sent":
                        self._totals["warning_samples"] += 1
                    sample_types = self._analysis_summary.setdefault(
                        "sample_types", {}
                    )
                    sample_type = str(sample["sample_type"])
                    sample_types[sample_type] = (
                        int(sample_types.get(sample_type, 0)) + 1
                    )
                    target_samples = self._analysis_summary.setdefault(
                        "target_samples", {}
                    )
                    target_id = str(sample.get("target_id", "home"))
                    target_samples[target_id] = (
                        int(target_samples.get(target_id, 0)) + 1
                    )
            for sample in pysteps_preflight_samples:
                exists = db.execute(
                    "SELECT 1 FROM pysteps_preflight_samples "
                    "WHERE region_key=? AND source=? AND nominal_minute=?",
                    (
                        sample["region_key"], sample["source"],
                        sample["nominal_minute"],
                    ),
                ).fetchone()
                db.execute("""
                    INSERT INTO pysteps_preflight_samples(
                        region_key, source, nominal_minute, evaluated_at,
                        status, frames_used, translation_km_per_hour,
                        heading_deg, verification_iou, method, confidence,
                        motion_match_iou, vector_consistency_deg,
                        wet_cell_ratio, centroid_translation_km_per_hour,
                        centroid_heading_deg, reason
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(region_key, source, nominal_minute)
                    DO UPDATE SET
                        evaluated_at=excluded.evaluated_at,
                        status=excluded.status,
                        frames_used=excluded.frames_used,
                        translation_km_per_hour=excluded.translation_km_per_hour,
                        heading_deg=excluded.heading_deg,
                        verification_iou=excluded.verification_iou,
                        method=excluded.method,
                        confidence=excluded.confidence,
                        motion_match_iou=excluded.motion_match_iou,
                        vector_consistency_deg=excluded.vector_consistency_deg,
                        wet_cell_ratio=excluded.wet_cell_ratio,
                        centroid_translation_km_per_hour=
                            excluded.centroid_translation_km_per_hour,
                        centroid_heading_deg=excluded.centroid_heading_deg,
                        reason=excluded.reason
                """, (
                    sample["region_key"], sample["source"],
                    sample["nominal_minute"], sample["evaluated_at"],
                    sample["status"], sample["frames_used"],
                    sample.get("translation_km_per_hour"),
                    sample.get("heading_deg"),
                    sample.get("verification_iou"), sample.get("method"),
                    sample.get("confidence"), sample.get("motion_match_iou"),
                    sample.get("vector_consistency_deg"),
                    sample.get("wet_cell_ratio"),
                    sample.get("centroid_translation_km_per_hour"),
                    sample.get("centroid_heading_deg"), sample.get("reason"),
                ))
                if exists is None:
                    self._totals["pysteps_preflight_samples"] += 1
            for sample in system_performance_samples:
                exists = db.execute(
                    "SELECT 1 FROM system_performance_samples "
                    "WHERE sample_key=?",
                    (sample["sample_key"],),
                ).fetchone()
                payload = json.dumps(
                    sample.get("snapshot", {}),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                db.execute("""
                    INSERT INTO system_performance_samples(
                        sample_key, cycle_id, stage, started_at, finished_at,
                        duration_ms, outcome, cpu_before, cpu_after,
                        temperature_before_c, temperature_after_c,
                        memory_before_percent, memory_after_percent,
                        load_before, load_after, fan_before, fan_after,
                        snapshot_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sample_key) DO UPDATE SET
                        finished_at=excluded.finished_at,
                        duration_ms=excluded.duration_ms,
                        outcome=excluded.outcome,
                        cpu_before=excluded.cpu_before,
                        cpu_after=excluded.cpu_after,
                        temperature_before_c=excluded.temperature_before_c,
                        temperature_after_c=excluded.temperature_after_c,
                        memory_before_percent=excluded.memory_before_percent,
                        memory_after_percent=excluded.memory_after_percent,
                        load_before=excluded.load_before,
                        load_after=excluded.load_after,
                        fan_before=excluded.fan_before,
                        fan_after=excluded.fan_after,
                        snapshot_json=excluded.snapshot_json
                """, (
                    sample["sample_key"], sample["cycle_id"], sample["stage"],
                    sample["started_at"], sample["finished_at"],
                    sample["duration_ms"], sample["outcome"],
                    sample.get("cpu_before"), sample.get("cpu_after"),
                    sample.get("temperature_before_c"),
                    sample.get("temperature_after_c"),
                    sample.get("memory_before_percent"),
                    sample.get("memory_after_percent"),
                    sample.get("load_before"), sample.get("load_after"),
                    sample.get("fan_before"), sample.get("fan_after"), payload,
                ))
                if exists is None:
                    self._totals["system_performance_samples"] += 1
            lightning_cells_written = 0
            for sample in lightning_aggregates:
                existing = db.execute(
                    "SELECT strike_count, region_ids_json "
                    "FROM lightning_aggregates WHERE nominal_minute=? "
                    "AND source=? AND grid_lat=? AND grid_lon=? AND grid_deg=?",
                    (
                        sample["nominal_minute"], sample["source"],
                        sample["grid_lat"], sample["grid_lon"],
                        sample["grid_deg"],
                    ),
                ).fetchone()
                regions = set(sample.get("region_ids", ()))
                if existing is not None:
                    regions.update(json.loads(existing[1]))
                db.execute("""
                    INSERT INTO lightning_aggregates(
                        nominal_minute, source, grid_lat, grid_lon, grid_deg,
                        strike_count, latitude_sum, longitude_sum,
                        latitude_min, latitude_max, longitude_min,
                        longitude_max, first_timestamp, last_timestamp,
                        region_ids_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(
                        nominal_minute, source, grid_lat, grid_lon, grid_deg
                    ) DO UPDATE SET
                        strike_count=lightning_aggregates.strike_count+
                            excluded.strike_count,
                        latitude_sum=lightning_aggregates.latitude_sum+
                            excluded.latitude_sum,
                        longitude_sum=lightning_aggregates.longitude_sum+
                            excluded.longitude_sum,
                        latitude_min=min(
                            lightning_aggregates.latitude_min,
                            excluded.latitude_min
                        ),
                        latitude_max=max(
                            lightning_aggregates.latitude_max,
                            excluded.latitude_max
                        ),
                        longitude_min=min(
                            lightning_aggregates.longitude_min,
                            excluded.longitude_min
                        ),
                        longitude_max=max(
                            lightning_aggregates.longitude_max,
                            excluded.longitude_max
                        ),
                        first_timestamp=min(
                            first_timestamp, excluded.first_timestamp
                        ),
                        last_timestamp=max(
                            last_timestamp, excluded.last_timestamp
                        ),
                        region_ids_json=excluded.region_ids_json
                """, (
                    sample["nominal_minute"], sample["source"],
                    sample["grid_lat"], sample["grid_lon"], sample["grid_deg"],
                    sample["strike_count"], sample["latitude_sum"],
                    sample["longitude_sum"], sample["latitude_min"],
                    sample["latitude_max"], sample["longitude_min"],
                    sample["longitude_max"], sample["first_timestamp"],
                    sample["last_timestamp"],
                    json.dumps(sorted(regions), separators=(",", ":")),
                ))
                if existing is None:
                    self._totals["lightning_aggregate_cells"] += 1
                self._totals["lightning_strikes"] += int(
                    sample["strike_count"]
                )
                lightning_cells_written += 1
            provider_cycle_samples_written = 0
            for sample in provider_cycle_samples:
                existed = db.execute(
                    "SELECT 1 FROM provider_cycle_samples WHERE sample_key=?",
                    (sample["sample_key"],),
                ).fetchone()
                db.execute("""
                    INSERT INTO provider_cycle_samples(
                        sample_key, provider, scope_key, nominal_minute,
                        observed_at, latitude, longitude, metrics_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sample_key) DO UPDATE SET
                        observed_at=excluded.observed_at,
                        latitude=excluded.latitude,
                        longitude=excluded.longitude,
                        metrics_json=excluded.metrics_json
                """, (
                    sample["sample_key"], sample["provider"],
                    sample["scope_key"], sample["nominal_minute"],
                    sample["observed_at"], sample.get("latitude"),
                    sample.get("longitude"), json.dumps(
                        sample.get("metrics", {}),
                        ensure_ascii=False,
                        separators=(",", ":"),
                        default=str,
                    ),
                ))
                if existed is None:
                    self._totals["provider_cycle_samples"] += 1
                provider_cycle_samples_written += 1
            ground_observations_written = 0
            for sample in ground_observations:
                existed = db.execute(
                    "SELECT 1 FROM ground_observations WHERE record_key=?",
                    (sample["record_key"],),
                ).fetchone()
                db.execute("""
                    INSERT OR REPLACE INTO ground_observations(
                        record_key, provider, region_key, station_id,
                        observed_at, received_at, latitude, longitude,
                        precipitation_present, quantitative_precipitation,
                        rain_mm, rain_5min, rain_60min, measurement_kind,
                        measurement_unit, interval_minutes, parser_version,
                        interpretation_reason, quality_flags_json, raw_text,
                        raw_payload_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    sample["record_key"], sample["provider"],
                    sample["region_key"], sample["station_id"],
                    sample["observed_at"], sample.get("received_at"),
                    sample["latitude"], sample["longitude"],
                    sample.get("precipitation_present"),
                    int(bool(sample.get("quantitative_precipitation"))),
                    sample.get("rain_mm"), sample.get("rain_5min"),
                    sample.get("rain_60min"), sample.get("measurement_kind"),
                    sample.get("measurement_unit"),
                    sample.get("interval_minutes"), sample.get("parser_version"),
                    sample.get("interpretation_reason"),
                    json.dumps(sample.get("quality_flags", ())),
                    sample.get("raw_text"),
                    json.dumps(sample.get("raw_payload"), default=str)
                    if sample.get("raw_payload") is not None else None,
                ))
                if existed is None:
                    self._totals["ground_observations"] += 1
                ground_observations_written += 1
            ground_quality_written = 0
            for sample in ground_quality:
                db.execute("""
                    INSERT OR REPLACE INTO ground_quality_assessments(
                        decision_key, observation_key, target_id, region_key,
                        evaluated_at, station_id, provider, storm_id,
                        age_seconds, distance_target_km, distance_storm_km,
                        wet, fresh, eligible, quality_score, reasons_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    sample["decision_key"], sample["observation_key"],
                    sample["target_id"], sample["region_key"],
                    sample["evaluated_at"], sample["station_id"],
                    sample["provider"], sample.get("storm_id"),
                    sample.get("age_seconds"), sample.get("distance_target_km"),
                    sample.get("distance_storm_km"), int(bool(sample["wet"])),
                    int(bool(sample["fresh"])), int(bool(sample["eligible"])),
                    sample["quality_score"],
                    json.dumps(sample.get("reasons", ())),
                ))
                ground_quality_written += 1
            if ground_quality:
                self._totals["ground_quality_assessments"] = db.execute(
                    "SELECT count(*) FROM ground_quality_assessments"
                ).fetchone()[0]
            ground_decisions_written = 0
            for sample in ground_decisions:
                db.execute("""
                    INSERT OR REPLACE INTO ground_confirmation_decisions(
                        decision_key, target_id, region_key, evaluated_at,
                        confirmed, status, confidence_percent, score,
                        storm_ids_json, sources_json, snapshot_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    sample["decision_key"], sample["target_id"],
                    sample["region_key"], sample["evaluated_at"],
                    int(bool(sample["confirmed"])), sample["status"],
                    int(sample["confidence_percent"]), sample["score"],
                    json.dumps(sample.get("storm_ids", ())),
                    json.dumps(sample.get("sources", ())),
                    json.dumps(sample.get("snapshot", {}), default=str),
                ))
                ground_decisions_written += 1
            if ground_decisions:
                self._totals["ground_confirmation_decisions"] = db.execute(
                    "SELECT count(*) FROM ground_confirmation_decisions"
                ).fetchone()[0]
            if affected_bias_profiles:
                self._refresh_bias_profiles(db, affected_bias_profiles)
                self._totals["bias_samples"] = db.execute(
                    "SELECT count(*) FROM provider_bias_samples"
                ).fetchone()[0]
                self._totals["bias_profiles"] = db.execute(
                    "SELECT count(*) FROM provider_bias_profiles"
                ).fetchone()[0]
                profile_snapshot = self._load_bias_profiles(db)
            else:
                # Radar evidence and its profiles now live in the worker
                # corpus. A telemetry-only HA flush must not rescan the legacy
                # radar tables, otherwise every twelfth provider cycle causes
                # the exact SQLite/CPU spike this split is meant to remove.
                profile_snapshot = []
        return {
            "frames_written": frames_written,
            "comparisons_written": comparisons_written,
            "verification_samples_written": len(verification_samples),
            "pysteps_preflight_samples_written": len(
                pysteps_preflight_samples
            ),
            "system_performance_samples_written": len(
                system_performance_samples
            ),
            "lightning_aggregate_cells_written": lightning_cells_written,
            "provider_cycle_samples_written": provider_cycle_samples_written,
            "ground_observations_written": ground_observations_written,
            "ground_quality_assessments_written": ground_quality_written,
            "ground_confirmation_decisions_written": ground_decisions_written,
            "_bias_profiles": profile_snapshot,
            **self.statistics(),
        }

    def statistics(self) -> dict:
        """Geef goedkope tellers zonder de groeiende tabellen opnieuw te scannen."""
        related = (
            self.path,
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
        )
        return {
            "bytes": sum(path.stat().st_size for path in related if path.exists()),
            "path": str(self.path),
            "total_frames": self._totals["frames"],
            "total_datapoints": self._totals["datapoints"],
            "total_comparisons": self._totals["comparisons"],
            "total_verification_samples": self._totals["verification_samples"],
            "total_warning_samples": self._totals["warning_samples"],
            "total_pysteps_preflight_samples": self._totals[
                "pysteps_preflight_samples"
            ],
            "total_system_performance_samples": self._totals[
                "system_performance_samples"
            ],
            "total_lightning_aggregate_cells": self._totals[
                "lightning_aggregate_cells"
            ],
            "total_lightning_strikes": self._totals["lightning_strikes"],
            "total_provider_cycle_samples": self._totals[
                "provider_cycle_samples"
            ],
            "total_ground_observations": self._totals["ground_observations"],
            "total_ground_quality_assessments": self._totals[
                "ground_quality_assessments"
            ],
            "total_ground_confirmation_decisions": self._totals[
                "ground_confirmation_decisions"
            ],
            "total_bias_samples": self._totals["bias_samples"],
            "total_bias_profiles": self._totals["bias_profiles"],
            "sources": len(self._sources),
            "regions": len(self._regions),
            "oldest_timestamp": self._oldest_timestamp,
            "newest_timestamp": self._newest_timestamp,
            "schema_version": SCHEMA_VERSION,
            "last_reset_at": self._last_reset_at,
            "last_reset_reason": self._reset_reason,
            "analysis": self._analysis_summary,
        }

    def load_bias_profiles(self) -> list[dict]:
        """Lees de compacte profielen voor de runtime zonder ruwe frames."""
        with self._lock, closing(self._connect()) as db, db:
            return self._load_bias_profiles(db)

    def load_pending_virtual_probes(self, *, now: float | None = None) -> list[dict]:
        """Herstel nog niet beoordeelde virtuele verkenners na een herstart."""
        reference = float(
            now if now is not None
            else datetime.now(timezone.utc).timestamp()
        )
        with self._lock, closing(self._connect()) as db, db:
            resolved = {
                str(sample_key).removeprefix("virtual_probe_outcome:")
                for (sample_key,) in db.execute(
                    "SELECT sample_key FROM forecast_verification_samples "
                    "WHERE sample_type='virtual_probe_outcome'"
                )
            }
            rows = db.execute(
                "SELECT sample_key, snapshot_json "
                "FROM forecast_verification_samples "
                "WHERE sample_type='virtual_probe_forecast' "
                "ORDER BY observed_at DESC LIMIT 200"
            ).fetchall()
        restored = []
        for sample_key, payload in rows:
            probe_id = str(sample_key).removeprefix(
                "virtual_probe_forecast:"
            )
            if probe_id in resolved:
                continue
            try:
                probe = json.loads(payload).get("probe", {})
                expected_at = float(probe["expected_at"])
            except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                continue
            # Herstel ook verkenners waarvan de verificatiegrens al verstreken
            # is, zodat VirtualProbeManager ze als niet_verifieerbaar kan
            # afsluiten en een expliciete outcome kan bewaren. Begrens dit tot
            # één dag om zeer oude, historische forecasts niet opnieuw te
            # activeren.
            if expected_at + 24 * 60 * 60 >= reference:
                restored.append(probe)
        return restored

    def load_virtual_probe_history(self, *, limit: int = 20) -> dict:
        """Herstel duurzame verkennertellers en recente uitkomsten.

        De actieve verkenners worden afzonderlijk hersteld. Deze samenvatting
        voorkomt dat de zichtbare totalen na iedere HA- of workerherstart weer
        op nul beginnen terwijl de uitkomsten wel in de secundaire database
        bewaard zijn.
        """
        with self._lock, closing(self._connect()) as db, db:
            rows = db.execute(
                "SELECT snapshot_json FROM forecast_verification_samples "
                "WHERE sample_type='virtual_probe_outcome' "
                "ORDER BY observed_at DESC LIMIT ?",
                (max(1, min(int(limit), 100)),),
            ).fetchall()
            total = int(db.execute(
                "SELECT count(*) FROM forecast_verification_samples "
                "WHERE sample_type='virtual_probe_outcome'"
            ).fetchone()[0])
        recent = []
        counts = {
            "hits": 0,
            "misses": 0,
            "unverifiable": 0,
            "cancelled": 0,
        }
        for (payload,) in rows:
            try:
                decoded = json.loads(payload)
            except (TypeError, json.JSONDecodeError):
                continue
            outcome_name = decoded.get("outcome")
            probe = decoded.get("probe", {})
            if not isinstance(outcome_name, str) or not isinstance(probe, dict):
                continue
            recent.append({
                "probe_id": probe.get("probe_id"),
                "storm_id": probe.get("storm_id"),
                "station_id": probe.get("station_id"),
                "latitude": probe.get("latitude"),
                "longitude": probe.get("longitude"),
                "outcome": outcome_name,
                "rain_live_mm_h": decoded.get("rain_live_mm_h"),
                "rain_60min_mm": decoded.get("rain_60min_mm"),
                "verified_at": decoded.get("verified_at"),
            })
        # Tellers moeten over de volledige historie lopen, niet alleen over
        # het begrensde dashboardvenster.
        with self._lock, closing(self._connect()) as db, db:
            all_rows = db.execute(
                "SELECT snapshot_json FROM forecast_verification_samples "
                "WHERE sample_type='virtual_probe_outcome'"
            ).fetchall()
        for (payload,) in all_rows:
            try:
                decoded = json.loads(payload)
            except (TypeError, json.JSONDecodeError):
                continue
            result = str(decoded.get("outcome", "")).lower()
            if result == "regen_bevestigd":
                counts["hits"] += 1
            elif result == "droog_op_verwacht_moment":
                counts["misses"] += 1
            elif result in {
                "stormobject_verdwenen",
                "traject_gewijzigd",
            }:
                counts["cancelled"] += 1
            else:
                counts["unverifiable"] += 1
        return {
            "completed": total,
            **counts,
            "recent_outcomes": recent,
        }

    @staticmethod
    def _load_analysis_summary(db: sqlite3.Connection) -> dict:
        """Geef begrensde, veilige analysetellers zonder ruwe data te lekken."""
        sample_types = {
            str(sample_type): int(count)
            for sample_type, count in db.execute(
                "SELECT sample_type, count(*) "
                "FROM forecast_verification_samples "
                "GROUP BY sample_type ORDER BY count(*) DESC"
            )
        }
        target_samples = {
            str(target_id): int(count)
            for target_id, count in db.execute(
                "SELECT target_id, count(*) "
                "FROM forecast_verification_samples "
                "GROUP BY target_id ORDER BY count(*) DESC LIMIT 30"
            )
        }
        source_frames = {
            str(source): int(count)
            for source, count in db.execute(
                "SELECT source, count(*) FROM frames "
                "GROUP BY source ORDER BY count(*) DESC"
            )
        }
        provider_pairs = [
            {
                "pair": f"{source_a}<->{source_b}",
                "samples": int(samples),
                "comparable": int(comparable),
                "mean_f1_score": (
                    round(float(mean_f1), 3)
                    if mean_f1 is not None else None
                ),
                "mean_shared_coverage_fraction": round(
                    float(mean_coverage), 3
                ),
            }
            for (
                source_a,
                source_b,
                samples,
                comparable,
                mean_f1,
                mean_coverage,
            ) in db.execute(
                "SELECT source_a, source_b, count(*), count(f1_score), "
                "avg(f1_score), avg(shared_coverage_fraction) "
                "FROM comparisons GROUP BY source_a, source_b "
                "ORDER BY count(*) DESC LIMIT 30"
            )
        ]
        return {
            "sample_types": sample_types,
            "target_samples": target_samples,
            "source_frames": source_frames,
            "provider_pairs": provider_pairs,
        }

    def _backfill_bias_samples(self, db: sqlite3.Connection) -> None:
        """Migreer bestaande v2-vergelijkingen zonder zware puntenscan."""
        rows = db.execute("""
            SELECT
                c.region_key, c.source_a, c.source_b, c.nominal_minute,
                c.compared_at, c.primary_cells, c.reference_cells,
                c.overlap_cells, c.f1_score,
                frame_a.product_timestamp, frame_b.product_timestamp
            FROM comparisons AS c
            LEFT JOIN frames AS frame_a
              ON frame_a.region_key=c.region_key
             AND frame_a.source=c.source_a
             AND frame_a.nominal_minute=c.nominal_minute
            LEFT JOIN frames AS frame_b
              ON frame_b.region_key=c.region_key
             AND frame_b.source=c.source_b
             AND frame_b.nominal_minute=c.nominal_minute
        """).fetchall()
        for row in rows:
            (
                region_key, source_a, source_b, minute, compared_at,
                cells_a, cells_b, overlap, f1_score, timestamp_a, timestamp_b,
            ) = row
            latency_ab = (
                float(timestamp_b) - float(timestamp_a)
                if timestamp_a is not None and timestamp_b is not None
                else None
            )
            self._insert_directional_bias_sample(
                db,
                region_key=region_key,
                from_source=source_a,
                to_source=source_b,
                nominal_minute=minute,
                compared_at=compared_at,
                from_cells=cells_a,
                to_cells=cells_b,
                overlap_cells=overlap,
                f1_score=f1_score,
                latency_seconds=latency_ab,
            )
            self._insert_directional_bias_sample(
                db,
                region_key=region_key,
                from_source=source_b,
                to_source=source_a,
                nominal_minute=minute,
                compared_at=compared_at,
                from_cells=cells_b,
                to_cells=cells_a,
                overlap_cells=overlap,
                f1_score=f1_score,
                latency_seconds=-latency_ab if latency_ab is not None else None,
            )

    def _upsert_bias_samples_for_comparison(
        self,
        db: sqlite3.Connection,
        comparison: tuple,
    ) -> set[tuple[str, str, str]]:
        """Maak uit één symmetrische vergelijking twee bronrichtingen."""
        (
            region_key, source_a, source_b, minute, compared_at,
            cells_a, cells_b, overlap, _false_positive, _missed,
            _precision, _recall, f1_score, _shared_coverage_fraction,
        ) = comparison
        frame_rows = db.execute("""
            SELECT id, source, product_timestamp
            FROM frames
            WHERE region_key=? AND nominal_minute=? AND source IN (?, ?)
        """, (region_key, minute, source_a, source_b)).fetchall()
        frames = {
            str(source): (int(frame_id), float(timestamp))
            for frame_id, source, timestamp in frame_rows
        }
        points: dict[str, dict[tuple[int, int], float | None]] = {}
        for source in (str(source_a), str(source_b)):
            frame = frames.get(source)
            if frame is None:
                points[source] = {}
                continue
            points[source] = {
                (int(lat), int(lon)): (
                    float(intensity) if intensity is not None else None
                )
                for lat, lon, intensity in db.execute(
                    "SELECT grid_lat, grid_lon, max_intensity "
                    "FROM frame_points WHERE frame_id=?",
                    (frame[0],),
                )
            }

        timestamp_a = frames.get(str(source_a), (None, None))[1]
        timestamp_b = frames.get(str(source_b), (None, None))[1]
        latency_ab = (
            timestamp_b - timestamp_a
            if timestamp_a is not None and timestamp_b is not None
            else None
        )
        affected: set[tuple[str, str, str]] = set()
        for (
            from_source, to_source, from_cells, to_cells,
            from_points, to_points, latency,
        ) in (
            (
                str(source_a), str(source_b), cells_a, cells_b,
                points[str(source_a)], points[str(source_b)], latency_ab,
            ),
            (
                str(source_b), str(source_a), cells_b, cells_a,
                points[str(source_b)], points[str(source_a)],
                -latency_ab if latency_ab is not None else None,
            ),
        ):
            shift = self._best_grid_shift(set(from_points), set(to_points))
            intensity_bias = self._aligned_intensity_bias(
                from_points, to_points, shift
            )
            inserted = self._insert_directional_bias_sample(
                db,
                region_key=region_key,
                from_source=from_source,
                to_source=to_source,
                nominal_minute=minute,
                compared_at=compared_at,
                from_cells=from_cells,
                to_cells=to_cells,
                overlap_cells=overlap,
                f1_score=f1_score,
                intensity_bias=intensity_bias,
                shift_lat_cells=shift[0] if shift is not None else None,
                shift_lon_cells=shift[1] if shift is not None else None,
                latency_seconds=latency,
            )
            if inserted:
                scope = scope_from_region_key(str(region_key))
                affected.add((scope, from_source, to_source))
                affected.add(("*", from_source, to_source))
        return affected

    @staticmethod
    def _best_grid_shift(
        from_cells: set[tuple[int, int]],
        to_cells: set[tuple[int, int]],
    ) -> tuple[int, int] | None:
        """Zoek een kleine correctie waarmee `to` het best op `from` past."""
        if not from_cells or not to_cells:
            return None
        best_shift = (0, 0)
        best_score = -1
        best_cost = 99
        for shift_lat in range(-2, 3):
            for shift_lon in range(-2, 3):
                score = sum(
                    (lat + shift_lat, lon + shift_lon) in from_cells
                    for lat, lon in to_cells
                )
                cost = abs(shift_lat) + abs(shift_lon)
                if score > best_score or (
                    score == best_score and cost < best_cost
                ):
                    best_score = score
                    best_cost = cost
                    best_shift = (shift_lat, shift_lon)
        return best_shift

    @staticmethod
    def _aligned_intensity_bias(
        from_points: dict[tuple[int, int], float | None],
        to_points: dict[tuple[int, int], float | None],
        shift: tuple[int, int] | None,
    ) -> float | None:
        """Geef gemiddelde `to - from` intensiteit op uitgelijnde natte cellen."""
        if shift is None:
            return None
        difference_total = 0.0
        difference_count = 0
        for (lat, lon), to_intensity in to_points.items():
            from_intensity = from_points.get(
                (lat + shift[0], lon + shift[1])
            )
            if from_intensity is None or to_intensity is None:
                continue
            difference_total += float(to_intensity) - float(from_intensity)
            difference_count += 1
        return (
            round(difference_total / difference_count, 4)
            if difference_count else None
        )

    def _insert_directional_bias_sample(
        self,
        db: sqlite3.Connection,
        *,
        region_key: str,
        from_source: str,
        to_source: str,
        nominal_minute: int,
        compared_at: float,
        from_cells: int,
        to_cells: int,
        overlap_cells: int,
        f1_score: float | None,
        intensity_bias: float | None = None,
        shift_lat_cells: float | None = None,
        shift_lon_cells: float | None = None,
        latency_seconds: float | None = None,
    ) -> bool:
        """Bewaar alleen frames waarop de vertrekkende bron regen zag."""
        from_count = int(from_cells)
        to_count = int(to_cells)
        overlap_count = int(overlap_cells)
        if from_count <= 0:
            return False
        detection = overlap_count / from_count
        extra = (
            max(0, to_count - overlap_count) / to_count
            if to_count > 0 else None
        )
        wet_area = to_count / from_count
        db.execute("""
            INSERT INTO provider_bias_samples(
                region_key, scope_key, from_source, to_source,
                nominal_minute, compared_at, from_cells, to_cells,
                overlap_cells, detection_ratio, extra_fraction,
                wet_area_ratio, f1_score, intensity_bias,
                shift_lat_cells, shift_lon_cells, latency_seconds
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(region_key, from_source, to_source, nominal_minute)
            DO UPDATE SET
                compared_at=excluded.compared_at,
                from_cells=excluded.from_cells,
                to_cells=excluded.to_cells,
                overlap_cells=excluded.overlap_cells,
                detection_ratio=excluded.detection_ratio,
                extra_fraction=excluded.extra_fraction,
                wet_area_ratio=excluded.wet_area_ratio,
                f1_score=excluded.f1_score,
                intensity_bias=coalesce(
                    excluded.intensity_bias,
                    provider_bias_samples.intensity_bias
                ),
                shift_lat_cells=coalesce(
                    excluded.shift_lat_cells,
                    provider_bias_samples.shift_lat_cells
                ),
                shift_lon_cells=coalesce(
                    excluded.shift_lon_cells,
                    provider_bias_samples.shift_lon_cells
                ),
                latency_seconds=coalesce(
                    excluded.latency_seconds,
                    provider_bias_samples.latency_seconds
                )
        """, (
            str(region_key), scope_from_region_key(str(region_key)),
            str(from_source), str(to_source), int(nominal_minute),
            float(compared_at), from_count, to_count, overlap_count,
            round(detection, 6), round(extra, 6) if extra is not None else None,
            round(wet_area, 6), f1_score, intensity_bias,
            shift_lat_cells, shift_lon_cells, latency_seconds,
        ))
        return True

    def _rebuild_all_bias_profiles(self, db: sqlite3.Connection) -> None:
        db.execute("DELETE FROM provider_bias_profiles")
        exact = {
            (str(scope), str(source_from), str(source_to))
            for scope, source_from, source_to in db.execute(
                "SELECT DISTINCT scope_key, from_source, to_source "
                "FROM provider_bias_samples"
            )
        }
        global_pairs = {
            ("*", str(source_from), str(source_to))
            for source_from, source_to in db.execute(
                "SELECT DISTINCT from_source, to_source "
                "FROM provider_bias_samples"
            )
        }
        self._refresh_bias_profiles(db, exact | global_pairs)

    def _refresh_bias_profiles(
        self,
        db: sqlite3.Connection,
        profiles: set[tuple[str, str, str]],
    ) -> None:
        for scope, from_source, to_source in profiles:
            where_scope = "" if scope == "*" else " AND scope_key=?"
            params: tuple = (
                (from_source, to_source)
                if scope == "*"
                else (from_source, to_source, scope)
            )
            row = db.execute(f"""
                SELECT
                    count(*),
                    avg(detection_ratio),
                    avg(extra_fraction),
                    avg(wet_area_ratio),
                    avg(f1_score),
                    avg(intensity_bias),
                    avg(shift_lat_cells),
                    avg(shift_lon_cells),
                    avg(latency_seconds),
                    max(compared_at)
                FROM provider_bias_samples
                WHERE from_source=? AND to_source=?{where_scope}
            """, params).fetchone()
            count = int(row[0] or 0)
            if count == 0:
                db.execute(
                    "DELETE FROM provider_bias_profiles "
                    "WHERE scope_key=? AND from_source=? AND to_source=?",
                    (scope, from_source, to_source),
                )
                continue
            values = [
                round(float(value), 6) if value is not None else None
                for value in row[1:9]
            ]
            db.execute("""
                INSERT INTO provider_bias_profiles(
                    scope_key, from_source, to_source, sample_count,
                    mean_detection_ratio, mean_extra_fraction,
                    mean_wet_area_ratio, mean_f1_score,
                    mean_intensity_bias, mean_shift_lat_cells,
                    mean_shift_lon_cells, mean_latency_seconds,
                    confidence, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope_key, from_source, to_source) DO UPDATE SET
                    sample_count=excluded.sample_count,
                    mean_detection_ratio=excluded.mean_detection_ratio,
                    mean_extra_fraction=excluded.mean_extra_fraction,
                    mean_wet_area_ratio=excluded.mean_wet_area_ratio,
                    mean_f1_score=excluded.mean_f1_score,
                    mean_intensity_bias=excluded.mean_intensity_bias,
                    mean_shift_lat_cells=excluded.mean_shift_lat_cells,
                    mean_shift_lon_cells=excluded.mean_shift_lon_cells,
                    mean_latency_seconds=excluded.mean_latency_seconds,
                    confidence=excluded.confidence,
                    updated_at=excluded.updated_at
            """, (
                scope, from_source, to_source, count, *values,
                profile_confidence(count), float(row[9]),
            ))

    @staticmethod
    def _load_bias_profiles(db: sqlite3.Connection) -> list[dict]:
        columns = (
            "scope_key", "from_source", "to_source", "sample_count",
            "mean_detection_ratio", "mean_extra_fraction",
            "mean_wet_area_ratio", "mean_f1_score", "mean_intensity_bias",
            "mean_shift_lat_cells", "mean_shift_lon_cells",
            "mean_latency_seconds", "confidence", "updated_at",
        )
        rows = db.execute(
            "SELECT " + ", ".join(columns) + " "
            "FROM provider_bias_profiles "
            "ORDER BY scope_key<>'*', sample_count DESC, from_source, to_source"
        ).fetchall()
        return [
            dict(zip(columns, row, strict=True))
            for row in rows
        ]

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=20)
        try:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            db.execute("PRAGMA foreign_keys=ON")
            return db
        except BaseException:
            db.close()
            raise
