"""Append-only SQLite-opslag voor grootschalige providervergelijking."""
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import json
import sqlite3
import threading

from .provider_bias import profile_confidence, scope_from_region_key


SCHEMA_VERSION = 4


class CalibrationDataStore:
    """Schrijf compacte frames, rasterpunten en vergelijkingen transactioneel."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._totals = {
            "frames": 0, "datapoints": 0, "comparisons": 0,
            "verification_samples": 0, "warning_samples": 0,
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
            """)
            stored_schema = db.execute(
                "SELECT value FROM metadata WHERE key='schema_version'"
            ).fetchone()
            stored_schema_version = (
                int(stored_schema[0]) if stored_schema is not None else 0
            )
            if stored_schema_version < SCHEMA_VERSION:
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

    def write_batch(self, batch: dict) -> dict:
        frames = batch.get("frames", ())
        comparisons = batch.get("comparisons", ())
        verification_samples = batch.get("verification_samples", ())
        with self._lock, self._connect() as db:
            affected_bias_profiles: set[tuple[str, str, str]] = set()
            for frame in frames:
                if len(frame) == 9:
                    # Compatibiliteit voor oude interne test-/toolcallers.
                    frame = (
                        *frame[:8],
                        None, None, None, None, 1.0,
                        frame[8],
                    )
                existing = db.execute(
                    "SELECT id, grid_point_count FROM frames "
                    "WHERE region_key=? AND source=? AND nominal_minute=?",
                    frame[:3],
                ).fetchone()
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
            for comparison in comparisons:
                if len(comparison) == 13:
                    comparison = (*comparison, 1.0)
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
            self._refresh_bias_profiles(db, affected_bias_profiles)
            self._totals["bias_samples"] = db.execute(
                "SELECT count(*) FROM provider_bias_samples"
            ).fetchone()[0]
            self._totals["bias_profiles"] = db.execute(
                "SELECT count(*) FROM provider_bias_profiles"
            ).fetchone()[0]
            profile_snapshot = self._load_bias_profiles(db)
            self._analysis_batches_since_refresh += 1
            analysis_is_empty = not (
                self._analysis_summary.get("sample_types")
                or self._analysis_summary.get("source_frames")
                or self._analysis_summary.get("provider_pairs")
            )
            if (
                analysis_is_empty
                or self._analysis_batches_since_refresh >= 12
            ):
                self._analysis_summary = self._load_analysis_summary(db)
                self._analysis_batches_since_refresh = 0
        return {
            "frames_written": len(frames),
            "comparisons_written": len(comparisons),
            "verification_samples_written": len(verification_samples),
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
        with self._lock, self._connect() as db:
            return self._load_bias_profiles(db)

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
                shifted = {
                    (lat + shift_lat, lon + shift_lon)
                    for lat, lon in to_cells
                }
                score = len(from_cells & shifted)
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
        differences: list[float] = []
        for (lat, lon), to_intensity in to_points.items():
            from_intensity = from_points.get(
                (lat + shift[0], lon + shift[1])
            )
            if from_intensity is None or to_intensity is None:
                continue
            differences.append(float(to_intensity) - float(from_intensity))
        return (
            round(sum(differences) / len(differences), 4)
            if differences else None
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
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        db.execute("PRAGMA foreign_keys=ON")
        return db
