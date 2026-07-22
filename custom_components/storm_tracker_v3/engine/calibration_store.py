"""Append-only SQLite-opslag voor grootschalige providervergelijking."""
from __future__ import annotations

from pathlib import Path
import sqlite3


SCHEMA_VERSION = 1


class CalibrationDataStore:
    """Schrijf compacte frames, rasterpunten en vergelijkingen transactioneel."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
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
            """)
            db.execute(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def write_batch(self, batch: dict) -> dict:
        frames = batch.get("frames", ())
        comparisons = batch.get("comparisons", ())
        with self._connect() as db:
            for frame in frames:
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
                db.execute("DELETE FROM frame_points WHERE frame_id=?", (frame_id,))
                db.executemany("""
                    INSERT INTO frame_points(
                        frame_id, grid_lat, grid_lon, max_intensity,
                        max_quality, observation_count
                    ) VALUES(?, ?, ?, ?, ?, ?)
                """, ((frame_id, *point) for point in frame[8]))
            db.executemany("""
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
            """, comparisons)
        return {
            "frames_written": len(frames),
            "comparisons_written": len(comparisons),
            "bytes": self.path.stat().st_size if self.path.exists() else 0,
            "path": str(self.path),
        }

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=20)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        db.execute("PRAGMA foreign_keys=ON")
        return db
