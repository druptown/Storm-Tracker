"""Append-only SQLite-opslag voor grootschalige providervergelijking."""
from __future__ import annotations

from pathlib import Path
import json
import sqlite3
import threading


SCHEMA_VERSION = 2


class CalibrationDataStore:
    """Schrijf compacte frames, rasterpunten en vergelijkingen transactioneel."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._totals = {
            "frames": 0, "datapoints": 0, "comparisons": 0,
            "verification_samples": 0, "warning_samples": 0,
        }
        self._sources: set[str] = set()
        self._regions: set[str] = set()
        self._oldest_timestamp: float | None = None
        self._newest_timestamp: float | None = None

    def initialize(self) -> dict:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
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
            """)
            db.execute(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
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
        return self.statistics()

    def write_batch(self, batch: dict) -> dict:
        frames = batch.get("frames", ())
        comparisons = batch.get("comparisons", ())
        verification_samples = batch.get("verification_samples", ())
        with self._lock, self._connect() as db:
            for frame in frames:
                existing = db.execute(
                    "SELECT id, grid_point_count FROM frames "
                    "WHERE region_key=? AND source=? AND nominal_minute=?",
                    frame[:3],
                ).fetchone()
                db.execute("""
                    INSERT INTO frames(
                        region_key, source, nominal_minute, product_timestamp,
                        collected_at, grid_deg, observation_count, grid_point_count
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(region_key, source, nominal_minute) DO UPDATE SET
                        product_timestamp=excluded.product_timestamp,
                        collected_at=excluded.collected_at,
                        observation_count=excluded.observation_count,
                        grid_point_count=excluded.grid_point_count
                """, frame[:8])
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
                """, ((frame_id, *point) for point in frame[8]))
            for comparison in comparisons:
                exists = db.execute(
                    "SELECT 1 FROM comparisons WHERE region_key=? AND source_a=? "
                    "AND source_b=? AND nominal_minute=?",
                    comparison[:4],
                ).fetchone()
                if exists is None:
                    self._totals["comparisons"] += 1
                db.execute("""
                INSERT INTO comparisons(
                    region_key, source_a, source_b, nominal_minute, compared_at,
                    primary_cells, reference_cells, overlap_cells,
                    false_positive_cells, missed_cells, precision, recall, f1_score
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    f1_score=excluded.f1_score
                """, comparison)
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
        return {
            "frames_written": len(frames),
            "comparisons_written": len(comparisons),
            "verification_samples_written": len(verification_samples),
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
            "sources": len(self._sources),
            "regions": len(self._regions),
            "oldest_timestamp": self._oldest_timestamp,
            "newest_timestamp": self._newest_timestamp,
        }

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=20)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        db.execute("PRAGMA foreign_keys=ON")
        return db
