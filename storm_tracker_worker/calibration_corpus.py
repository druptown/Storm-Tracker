"""Persistent, worker-owned corpus for radar-provider calibration.

The transient calibration stage is useful for transferring one result to Home
Assistant, but it cannot compare products that arrived in different worker
jobs.  This store owns the long-lived calibration evidence instead:

* the checksum-verified gzip bytes of each *decoded provider-result* artifact
  are retained once; these are explicitly not upstream-native/raw products;
* every immutable capture and its rasterised artifact frame are retained;
* the newest frame for each provider/region/minute is selected
  deterministically;
* comparisons, directional bias samples and aggregate profiles are recomputed
  in the same SQLite transaction as an ingest receipt.

No database is opened at import time.  The service layer can therefore create
one :class:`CalibrationCorpusStore` per low-priority job without adding work to
Home Assistant or to the worker's operational radar path.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from itertools import combinations
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Any

from . import artifacts
from .calibration_stage import (
    ALGORITHM_VERSION as FRAME_ALGORITHM_VERSION,
    MAX_CAPTURES as STAGE_MAX_CAPTURES,
    _bias_rows,
    _comparison,
    _descriptor,
    _frame,
    _scope,
)


CORPUS_SCHEMA_VERSION = 1
CORPUS_ALGORITHM_VERSION = "worker-radar-calibration-corpus-v1"
SNAPSHOT_METHOD = "worker_calibration_corpus_snapshot_v1"
ARTIFACT_KIND = "decoded_provider_result"
PROVENANCE_GRADE = "decoded_legacy"
EXACT_RAW_ARTIFACTS_COMPLETE = False
DEFAULT_CORPUS_PATH = (
    artifacts.ARTIFACT_ROOT / "calibration_corpus.sqlite3"
)

MAX_CAPTURES_PER_INGEST = STAGE_MAX_CAPTURES
MAX_UNIQUE_ARTIFACT_BYTES_PER_INGEST = 64 * 1024 * 1024
MAX_NEW_GRID_POINTS_PER_INGEST = 1_000_000
MAX_TOTAL_CAPTURES = 100_000
MAX_TOTAL_ARTIFACT_BYTES = 384 * 1024 * 1024
MAX_TOTAL_GRID_POINTS = 5_000_000
MAX_CORPUS_BYTES = 512 * 1024 * 1024
MAX_SOURCES_PER_REGION_MINUTE = 64
MAX_COMPARISONS_PER_INGEST = 10_000
MAX_PROFILES_RETURNED = 128
MAX_RECEIPT_PAYLOAD_BYTES = 4 * 1024 * 1024
MAX_DESCRIPTOR_BYTES = 32 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTENT_TYPE = "application/json+gzip"


@dataclass(frozen=True, slots=True)
class CorpusBounds:
    """Hard admission bounds; exceeding one rejects the whole transaction."""

    captures_per_ingest: int = MAX_CAPTURES_PER_INGEST
    artifact_bytes_per_ingest: int = MAX_UNIQUE_ARTIFACT_BYTES_PER_INGEST
    grid_points_per_ingest: int = MAX_NEW_GRID_POINTS_PER_INGEST
    total_captures: int = MAX_TOTAL_CAPTURES
    total_artifact_bytes: int = MAX_TOTAL_ARTIFACT_BYTES
    total_grid_points: int = MAX_TOTAL_GRID_POINTS
    corpus_bytes: int = MAX_CORPUS_BYTES
    sources_per_region_minute: int = MAX_SOURCES_PER_REGION_MINUTE
    comparisons_per_ingest: int = MAX_COMPARISONS_PER_INGEST
    profiles_returned: int = MAX_PROFILES_RETURNED

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"corpusgrens {field_name} moet positief geheel zijn")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _receipt_payload_sha256(raw_captures: list[dict]) -> str:
    if not isinstance(raw_captures, list) or not raw_captures:
        raise ValueError("kalibratiecorpus vereist captures")
    if any(not isinstance(value, dict) for value in raw_captures):
        raise ValueError("kalibratiecorpus bevat ongeldige capture")
    capture_ids = [str(value.get("capture_id") or "") for value in raw_captures]
    if any(not _SHA256_RE.fullmatch(value) for value in capture_ids):
        raise ValueError("kalibratiecorpus bevat ongeldige capture-ID")
    if len(capture_ids) != len(set(capture_ids)):
        raise ValueError("kalibratiecorpus bevat dubbele capture-ID")
    document = {
        "algorithm_version": CORPUS_ALGORITHM_VERSION,
        "frame_algorithm_version": FRAME_ALGORITHM_VERSION,
        "captures": sorted(raw_captures, key=lambda value: value["capture_id"]),
    }
    encoded = _canonical_json(document).encode("utf-8")
    if len(encoded) > MAX_RECEIPT_PAYLOAD_BYTES:
        raise ValueError("kalibratiecorpus overschrijdt descriptor-bytebudget")
    return hashlib.sha256(encoded).hexdigest()


def _identifier(value: object, name: str, *, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise ValueError(f"{name} is ongeldig")
    return value


def _read_artifact_blob(metadata: dict) -> bytes:
    """Read immutable bytes of the decoded result that produced the frame."""
    if metadata.get("content_type") != _CONTENT_TYPE:
        raise ValueError("kalibratieartefact heeft ongeldig content-type")
    raw_path = metadata.get("artifact_path")
    if not isinstance(raw_path, str):
        raise ValueError("kalibratieartefact mist pad")
    root = artifacts.ARTIFACT_ROOT.resolve()
    path = Path(raw_path).resolve()
    if path.parent != root or path.suffixes[-2:] != [".json", ".gz"]:
        raise ValueError("kalibratieartefact staat buiten de worker-artifactmap")
    expected_bytes = int(metadata.get("artifact_bytes", -1))
    expected_sha = str(metadata.get("artifact_sha256") or "")
    if (
        expected_bytes <= 0
        or expected_bytes > artifacts.MAX_ARTIFACT_BYTES
        or not _SHA256_RE.fullmatch(expected_sha)
        or path.stat().st_size != expected_bytes
    ):
        raise ValueError("kalibratieartefact heeft ongeldige metadata")
    encoded = path.read_bytes()
    if len(encoded) != expected_bytes:
        raise ValueError("kalibratieartefact veranderde tijdens inlezen")
    if hashlib.sha256(encoded).hexdigest() != expected_sha:
        raise ValueError("kalibratieartefact-controlesom klopt niet")
    return encoded


def _confidence(sample_count: int) -> str:
    if sample_count < 12:
        return "insufficient"
    if sample_count < 30:
        return "low"
    if sample_count < 100:
        return "medium"
    return "high"


class CalibrationCorpusStore:
    """Bounded persistent store for immutable provider calibration evidence."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        bounds: CorpusBounds | None = None,
    ) -> None:
        self.path = Path(DEFAULT_CORPUS_PATH if path is None else path)
        self.bounds = bounds or CorpusBounds()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            # Individual CREATE statements are crash-atomic and idempotent.
            # Acquire the write lock only afterwards because executescript()
            # may otherwise commit a caller-started transaction implicitly.
            self._create_schema(db)
            db.execute("BEGIN IMMEDIATE")
            try:
                manifest = dict(db.execute("SELECT key, value FROM manifest"))
                expected = {
                    "schema_version": str(CORPUS_SCHEMA_VERSION),
                    "algorithm_version": CORPUS_ALGORITHM_VERSION,
                    "frame_algorithm_version": FRAME_ALGORITHM_VERSION,
                }
                if not manifest:
                    db.executemany(
                        "INSERT INTO manifest(key, value) VALUES(?, ?)",
                        sorted(expected.items()),
                    )
                elif any(manifest.get(key) != value for key, value in expected.items()):
                    raise ValueError("kalibratiecorpus heeft onverenigbaar manifest")
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def ingest_job(self, job) -> dict[str, Any]:
        """Ingest ``job.payload['captures']`` using ``job.job_id`` as receipt."""
        payload = getattr(job, "payload", None)
        if not isinstance(payload, dict):
            raise ValueError("kalibratiecorpusjob mist payload")
        return self.ingest(
            payload.get("captures"),
            receipt_id=str(getattr(job, "job_id", "") or ""),
        )

    def ingest(
        self,
        raw_captures: object,
        *,
        receipt_id: str,
        ingested_at: float | None = None,
    ) -> dict[str, Any]:
        """Persist captures and atomically refresh their cross-job evidence."""
        receipt_id = _identifier(receipt_id, "receipt_id")
        if not isinstance(raw_captures, list):
            raise ValueError("kalibratiecorpus vereist een capturelijst")
        if not raw_captures or len(raw_captures) > self.bounds.captures_per_ingest:
            raise ValueError("kalibratiecorpus overschrijdt capturebudget")
        payload_sha = _receipt_payload_sha256(raw_captures)
        now = time.time() if ingested_at is None else float(ingested_at)
        if not math.isfinite(now) or now <= 0.0:
            raise ValueError("ingested_at moet een geldige epoch zijn")

        self.initialize()
        with closing(self._connect()) as db:
            existing = self._receipt(db, receipt_id)
            if existing is not None:
                return self._repeat_result(db, existing, payload_sha)

        captures = [_descriptor(value) for value in raw_captures]
        # Descriptor validation also recomputes each capture_id.  Sorting makes
        # frame selection and receipt behaviour independent from queue order.
        captures.sort(key=lambda value: value["capture_id"])
        for capture in captures:
            _identifier(capture["region_key"], "region_key", maximum=512)
            _identifier(capture["region_id"], "region_id", maximum=256)
            _identifier(capture["source"], "source", maximum=128)
            _identifier(
                capture["artifact"]["artifact_path"],
                "artifact_path",
                maximum=4096,
            )
            if len(
                _canonical_json(self._stored_descriptor(capture)).encode("utf-8")
            ) > MAX_DESCRIPTOR_BYTES:
                raise ValueError("kalibratiecapture overschrijdt descriptorbudget")
        with closing(self._connect()) as db:
            known_before_parse = self._known_descriptors(db, captures)
        for capture in captures:
            stored = known_before_parse.get(capture["capture_id"])
            if stored is not None and stored != _canonical_json(
                self._stored_descriptor(capture)
            ):
                raise ValueError("capture-ID botst met andere provenance")
        captures_to_parse = [
            capture for capture in captures
            if capture["capture_id"] not in known_before_parse
        ]
        artifact_cache: dict[tuple[str, str, int], tuple[dict, bytes]] = {}
        frames_by_capture: dict[str, dict] = {}
        for capture in captures_to_parse:
            metadata = capture["artifact"]
            cache_key = (
                str(metadata["artifact_path"]),
                str(metadata["artifact_sha256"]),
                int(metadata["artifact_bytes"]),
            )
            cached = artifact_cache.get(cache_key)
            if cached is None:
                # The parser and the retained blob independently verify the
                # same SHA contract.  A same-hash replacement is therefore
                # byte-identical for all practical integrity purposes.
                payload = artifacts.read_json_artifact(metadata)
                blob = _read_artifact_blob(metadata)
                cached = (payload, blob)
                artifact_cache[cache_key] = cached
            frame = _frame(capture, cached[0], now)
            frame["capture_id"] = capture["capture_id"]
            frame["artifact_sha256"] = metadata["artifact_sha256"]
            frames_by_capture[capture["capture_id"]] = frame

        unique_artifacts: dict[str, bytes] = {}
        for capture in captures_to_parse:
            metadata = capture["artifact"]
            cache_key = (
                str(metadata["artifact_path"]),
                str(metadata["artifact_sha256"]),
                int(metadata["artifact_bytes"]),
            )
            sha = str(metadata["artifact_sha256"])
            blob = artifact_cache[cache_key][1]
            previous = unique_artifacts.setdefault(sha, blob)
            if previous != blob:  # pragma: no cover - SHA collision guard
                raise ValueError("gelijke artifact-SHA verwijst naar andere bytes")
        artifact_batch_bytes = sum(len(value) for value in unique_artifacts.values())
        point_batch_count = sum(
            len(frame["points"]) for frame in frames_by_capture.values()
        )
        if artifact_batch_bytes > self.bounds.artifact_bytes_per_ingest:
            raise ValueError("kalibratiecorpus overschrijdt artifact-bytebudget")
        if point_batch_count > self.bounds.grid_points_per_ingest:
            raise ValueError("kalibratiecorpus overschrijdt rasterpuntbudget")

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = self._receipt(db, receipt_id)
                if existing is not None:
                    db.rollback()
                    return self._repeat_result(db, existing, payload_sha)

                known_capture_ids = self._known_descriptors(db, captures)
                new_pairs = [
                    (capture, frames_by_capture[capture["capture_id"]])
                    for capture in captures
                    if capture["capture_id"] not in known_capture_ids
                ]
                for capture in captures:
                    stored = known_capture_ids.get(capture["capture_id"])
                    if stored is not None and stored != _canonical_json(
                        self._stored_descriptor(capture)
                    ):
                        raise ValueError("capture-ID botst met andere provenance")

                new_artifacts = {}
                for sha, blob in unique_artifacts.items():
                    stored_artifact = db.execute(
                        "SELECT content_type, artifact_bytes, payload, "
                        "algorithm_version, artifact_kind, provenance_grade, "
                        "exact_raw_artifacts_complete, parser_id, "
                        "upstream_product_id, upstream_content_type "
                        "FROM artifacts WHERE artifact_sha256=?",
                        (sha,),
                    ).fetchone()
                    if stored_artifact is None:
                        new_artifacts[sha] = blob
                        continue
                    if (
                        stored_artifact[0] != _CONTENT_TYPE
                        or int(stored_artifact[1]) != len(blob)
                        or bytes(stored_artifact[2]) != blob
                        or stored_artifact[3] != CORPUS_ALGORITHM_VERSION
                        or stored_artifact[4] != ARTIFACT_KIND
                        or stored_artifact[5] != PROVENANCE_GRADE
                        or int(stored_artifact[6]) != 0
                        or any(value is not None for value in stored_artifact[7:10])
                    ):
                        raise ValueError("bestaand corpusartefact faalt provenancecontrole")
                self._enforce_total_bounds(
                    db,
                    new_capture_count=len(new_pairs),
                    new_artifact_bytes=sum(len(value) for value in new_artifacts.values()),
                    new_grid_points=sum(len(frame["points"]) for _, frame in new_pairs),
                )
                artifacts_inserted = len(new_artifacts)
                for capture, _frame_value in new_pairs:
                    metadata = capture["artifact"]
                    sha = str(metadata["artifact_sha256"])
                    if sha not in new_artifacts:
                        continue
                    db.execute(
                        "INSERT INTO artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            sha,
                            _CONTENT_TYPE,
                            len(new_artifacts[sha]),
                            sqlite3.Binary(new_artifacts[sha]),
                            capture["capture_id"],
                            now,
                            CORPUS_ALGORITHM_VERSION,
                            ARTIFACT_KIND,
                            PROVENANCE_GRADE,
                            int(EXACT_RAW_ARTIFACTS_COMPLETE),
                            None,
                            None,
                            None,
                        ),
                    )
                    del new_artifacts[sha]

                affected_groups: set[tuple[str, int]] = set()
                for capture, frame in new_pairs:
                    self._insert_capture(db, receipt_id, capture, frame, now)
                    affected_groups.add(
                        (str(frame["region_key"]), int(frame["nominal_minute"]))
                    )

                comparison_count, bias_count, affected_profiles = (
                    self._recompute_groups(db, affected_groups, now)
                )
                self._refresh_profiles(db, affected_profiles, now)
                db.execute(
                    "INSERT INTO ingest_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        receipt_id,
                        payload_sha,
                        now,
                        len(captures),
                        len(new_pairs),
                        artifacts_inserted,
                        sum(len(frame["points"]) for _, frame in new_pairs),
                        len(affected_groups),
                        comparison_count,
                        bias_count,
                        CORPUS_ALGORITHM_VERSION,
                    ),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
            receipt = self._receipt(db, receipt_id)
            if receipt is None:  # pragma: no cover - transaction invariant
                raise AssertionError("ingest receipt ontbreekt na commit")
            return self._result(db, receipt, already_applied=False)

    def statistics(self) -> dict[str, Any]:
        self.initialize()
        with closing(self._connect()) as db:
            return self._statistics(db)

    def profiles(self) -> dict[str, Any]:
        self.initialize()
        with closing(self._connect()) as db:
            return self._profiles(db)

    def snapshot(self) -> dict[str, Any]:
        """Return one transactionally consistent, compact read-only view."""
        # This is the sole permitted write in the snapshot path: it creates or
        # idempotently verifies the schema before reopening the file read-only.
        self.initialize()
        with closing(self._connect_read_only()) as db:
            db.execute("BEGIN")
            return {
                "statistics": self._statistics(db),
                **self._profiles(db),
            }

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30.0)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA synchronous=FULL")
            page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
            max_pages = max(1, self.bounds.corpus_bytes // page_size)
            page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
            if page_count > max_pages:
                raise ValueError("kalibratiecorpus overschrijdt SQLite-bytebudget")
            applied_max = int(
                db.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0]
            )
            if applied_max > max_pages:
                raise ValueError("SQLite kon corpus-bytebudget niet afdwingen")
            return db
        except BaseException:
            db.close()
            raise

    def _connect_read_only(self) -> sqlite3.Connection:
        uri = self.path.resolve().as_uri() + "?mode=ro"
        db = sqlite3.connect(uri, uri=True, timeout=30.0)
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
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS artifacts(
                artifact_sha256 TEXT PRIMARY KEY,
                content_type TEXT NOT NULL,
                artifact_bytes INTEGER NOT NULL,
                payload BLOB NOT NULL,
                first_capture_id TEXT NOT NULL,
                stored_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL,
                artifact_kind TEXT NOT NULL,
                provenance_grade TEXT NOT NULL,
                exact_raw_artifacts_complete INTEGER NOT NULL CHECK(
                    exact_raw_artifacts_complete=0
                ),
                parser_id TEXT,
                upstream_product_id TEXT,
                upstream_content_type TEXT
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS ingest_receipts(
                receipt_id TEXT PRIMARY KEY,
                payload_sha256 TEXT NOT NULL,
                applied_at REAL NOT NULL,
                captures_received INTEGER NOT NULL,
                captures_inserted INTEGER NOT NULL,
                artifacts_inserted INTEGER NOT NULL,
                grid_points_inserted INTEGER NOT NULL,
                groups_recomputed INTEGER NOT NULL,
                comparisons_recomputed INTEGER NOT NULL,
                bias_samples_recomputed INTEGER NOT NULL,
                algorithm_version TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS captures(
                capture_id TEXT PRIMARY KEY,
                first_receipt_id TEXT NOT NULL,
                artifact_sha256 TEXT NOT NULL,
                artifact_kind TEXT NOT NULL,
                provenance_grade TEXT NOT NULL,
                exact_raw_artifacts_complete INTEGER NOT NULL CHECK(
                    exact_raw_artifacts_complete=0
                ),
                region_key TEXT NOT NULL,
                region_id TEXT NOT NULL,
                source TEXT NOT NULL,
                descriptor_product_timestamp REAL NOT NULL,
                artifact_frame_timestamp REAL NOT NULL,
                descriptor_json TEXT NOT NULL,
                ingested_at REAL NOT NULL,
                algorithm_version TEXT NOT NULL,
                FOREIGN KEY(artifact_sha256) REFERENCES artifacts(artifact_sha256),
                FOREIGN KEY(first_receipt_id) REFERENCES ingest_receipts(receipt_id)
                    DEFERRABLE INITIALLY DEFERRED
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS frames(
                capture_id TEXT PRIMARY KEY,
                artifact_sha256 TEXT NOT NULL,
                artifact_kind TEXT NOT NULL,
                provenance_grade TEXT NOT NULL,
                exact_raw_artifacts_complete INTEGER NOT NULL CHECK(
                    exact_raw_artifacts_complete=0
                ),
                region_key TEXT NOT NULL,
                region_id TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                product_timestamp REAL NOT NULL,
                collected_at REAL NOT NULL,
                grid_deg REAL NOT NULL,
                observation_count INTEGER NOT NULL,
                grid_point_count INTEGER NOT NULL,
                coverage_lon_min REAL NOT NULL,
                coverage_lat_min REAL NOT NULL,
                coverage_lon_max REAL NOT NULL,
                coverage_lat_max REAL NOT NULL,
                coverage_fraction REAL NOT NULL,
                evaluation_lon_min REAL NOT NULL,
                evaluation_lat_min REAL NOT NULL,
                evaluation_lon_max REAL NOT NULL,
                evaluation_lat_max REAL NOT NULL,
                algorithm_version TEXT NOT NULL,
                FOREIGN KEY(capture_id) REFERENCES captures(capture_id) ON DELETE CASCADE,
                FOREIGN KEY(artifact_sha256) REFERENCES artifacts(artifact_sha256)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS frames_region_minute_source
                ON frames(region_key, nominal_minute, source, product_timestamp);
            CREATE TABLE IF NOT EXISTS frame_points(
                capture_id TEXT NOT NULL,
                grid_lat INTEGER NOT NULL,
                grid_lon INTEGER NOT NULL,
                max_intensity REAL,
                max_quality REAL,
                observation_count INTEGER NOT NULL,
                PRIMARY KEY(capture_id, grid_lat, grid_lon),
                FOREIGN KEY(capture_id) REFERENCES frames(capture_id) ON DELETE CASCADE
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS comparisons(
                region_key TEXT NOT NULL,
                source_a TEXT NOT NULL,
                source_b TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                capture_id_a TEXT NOT NULL,
                capture_id_b TEXT NOT NULL,
                artifact_sha256_a TEXT NOT NULL,
                artifact_sha256_b TEXT NOT NULL,
                product_timestamp_a REAL NOT NULL,
                product_timestamp_b REAL NOT NULL,
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
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(region_key, source_a, source_b, nominal_minute)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS bias_samples(
                region_key TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                from_source TEXT NOT NULL,
                to_source TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                from_capture_id TEXT NOT NULL,
                to_capture_id TEXT NOT NULL,
                from_artifact_sha256 TEXT NOT NULL,
                to_artifact_sha256 TEXT NOT NULL,
                from_product_timestamp REAL NOT NULL,
                to_product_timestamp REAL NOT NULL,
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
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(region_key, from_source, to_source, nominal_minute)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS bias_profiles(
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
                algorithm_version TEXT NOT NULL,
                PRIMARY KEY(scope_key, from_source, to_source)
            ) WITHOUT ROWID;
        """)

    @staticmethod
    def _stored_descriptor(capture: dict) -> dict:
        return {
            key: value
            for key, value in capture.items()
            if key not in {
                "center_lat", "center_lon", "radius_km",
                "artifact_frame_timestamp", "evaluation_bbox",
            }
        }

    def _insert_capture(
        self,
        db: sqlite3.Connection,
        receipt_id: str,
        capture: dict,
        frame: dict,
        now: float,
    ) -> None:
        descriptor_json = _canonical_json(self._stored_descriptor(capture))
        metadata = capture["artifact"]
        db.execute(
            "INSERT INTO captures VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                capture["capture_id"], receipt_id,
                metadata["artifact_sha256"], ARTIFACT_KIND,
                PROVENANCE_GRADE, int(EXACT_RAW_ARTIFACTS_COMPLETE),
                capture["region_key"],
                capture["region_id"], capture["source"],
                capture["product_timestamp"], capture["artifact_frame_timestamp"],
                descriptor_json, now, CORPUS_ALGORITHM_VERSION,
            ),
        )
        db.execute(
            "INSERT INTO frames VALUES("
            "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                capture["capture_id"], metadata["artifact_sha256"],
                ARTIFACT_KIND, PROVENANCE_GRADE,
                int(EXACT_RAW_ARTIFACTS_COMPLETE),
                frame["region_key"], frame["region_id"], frame["source"],
                frame["nominal_minute"], frame["product_timestamp"],
                frame["collected_at"], frame["grid_deg"],
                frame["observation_count"], len(frame["points"]),
                *frame["coverage_bbox"], frame["coverage_fraction"],
                *frame["evaluation_bbox"], FRAME_ALGORITHM_VERSION,
            ),
        )
        db.executemany(
            "INSERT INTO frame_points VALUES(?,?,?,?,?,?)",
            (
                (
                    capture["capture_id"], grid_lat, grid_lon,
                    values[0], values[1], values[2],
                )
                for (grid_lat, grid_lon), values in frame["points"].items()
            ),
        )

    def _enforce_total_bounds(
        self,
        db: sqlite3.Connection,
        *,
        new_capture_count: int,
        new_artifact_bytes: int,
        new_grid_points: int,
    ) -> None:
        capture_count = int(db.execute("SELECT count(*) FROM captures").fetchone()[0])
        artifact_bytes = int(
            db.execute("SELECT coalesce(sum(artifact_bytes), 0) FROM artifacts").fetchone()[0]
        )
        point_count = int(db.execute("SELECT count(*) FROM frame_points").fetchone()[0])
        if capture_count + new_capture_count > self.bounds.total_captures:
            raise ValueError("kalibratiecorpus heeft maximale captureomvang bereikt")
        if artifact_bytes + new_artifact_bytes > self.bounds.total_artifact_bytes:
            raise ValueError("kalibratiecorpus heeft maximale artifactomvang bereikt")
        if point_count + new_grid_points > self.bounds.total_grid_points:
            raise ValueError("kalibratiecorpus heeft maximale rasteromvang bereikt")

    @staticmethod
    def _known_descriptors(
        db: sqlite3.Connection,
        captures: list[dict],
    ) -> dict[str, str]:
        if not captures:
            return {}
        return {
            str(row[0]): str(row[1])
            for row in db.execute(
                "SELECT capture_id, descriptor_json FROM captures "
                f"WHERE capture_id IN ({','.join('?' for _ in captures)})",
                tuple(value["capture_id"] for value in captures),
            )
        }

    def _recompute_groups(
        self,
        db: sqlite3.Connection,
        affected_groups: set[tuple[str, int]],
        now: float,
    ) -> tuple[int, int, set[tuple[str, str, str]]]:
        comparisons_written = 0
        bias_written = 0
        profile_keys: set[tuple[str, str, str]] = set()
        for region_key, nominal_minute in sorted(affected_groups):
            old_pairs = list(db.execute(
                "SELECT DISTINCT scope_key, from_source, to_source "
                "FROM bias_samples WHERE region_key=? AND nominal_minute=?",
                (region_key, nominal_minute),
            ))
            for scope_key, source_from, source_to in old_pairs:
                profile_keys.add((str(scope_key), str(source_from), str(source_to)))
                profile_keys.add(("*", str(source_from), str(source_to)))
            db.execute(
                "DELETE FROM bias_samples WHERE region_key=? AND nominal_minute=?",
                (region_key, nominal_minute),
            )
            db.execute(
                "DELETE FROM comparisons WHERE region_key=? AND nominal_minute=?",
                (region_key, nominal_minute),
            )
            source_count = int(db.execute(
                "SELECT count(DISTINCT source) FROM frames "
                "WHERE region_key=? AND nominal_minute=?",
                (region_key, nominal_minute),
            ).fetchone()[0])
            if source_count > self.bounds.sources_per_region_minute:
                raise ValueError("kalibratiecorpus overschrijdt bronbudget per frame")
            frames = self._active_frames(db, region_key, nominal_minute)
            possible = len(frames) * (len(frames) - 1) // 2
            comparisons_written += possible
            if comparisons_written > self.bounds.comparisons_per_ingest:
                raise ValueError("kalibratiecorpus overschrijdt vergelijkingsbudget")
            for first, second in combinations(frames, 2):
                comparison = _comparison(first, second, now)
                if comparison is None:
                    comparisons_written -= 1
                    continue
                db.execute(
                    "INSERT INTO comparisons VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        region_key, first["source"], second["source"],
                        nominal_minute, first["capture_id"], second["capture_id"],
                        first["artifact_sha256"], second["artifact_sha256"],
                        first["product_timestamp"], second["product_timestamp"],
                        now, comparison["primary_cells"],
                        comparison["reference_cells"], comparison["overlap_cells"],
                        comparison["false_positive_cells"],
                        comparison["missed_cells"], comparison["precision"],
                        comparison["recall"], comparison["f1_score"],
                        comparison["shared_coverage_fraction"],
                        CORPUS_ALGORITHM_VERSION,
                    ),
                )
                for row in _bias_rows(comparison, first, second):
                    source_frame = first if row[2] == first["source"] else second
                    target_frame = second if source_frame is first else first
                    db.execute(
                        "INSERT INTO bias_samples VALUES("
                        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            row[0], row[1], row[2], row[3], row[4],
                            source_frame["capture_id"], target_frame["capture_id"],
                            source_frame["artifact_sha256"],
                            target_frame["artifact_sha256"],
                            source_frame["product_timestamp"],
                            target_frame["product_timestamp"],
                            row[5], *row[6:], CORPUS_ALGORITHM_VERSION,
                        ),
                    )
                    bias_written += 1
                    profile_keys.add((str(row[1]), str(row[2]), str(row[3])))
                    profile_keys.add(("*", str(row[2]), str(row[3])))
        return comparisons_written, bias_written, profile_keys

    def _active_frames(
        self,
        db: sqlite3.Connection,
        region_key: str,
        nominal_minute: int,
    ) -> list[dict]:
        columns = (
            "capture_id", "artifact_sha256", "artifact_kind",
            "provenance_grade", "exact_raw_artifacts_complete",
            "region_key", "region_id", "source",
            "nominal_minute", "product_timestamp", "collected_at", "grid_deg",
            "observation_count", "grid_point_count", "coverage_lon_min",
            "coverage_lat_min", "coverage_lon_max", "coverage_lat_max",
            "coverage_fraction", "evaluation_lon_min", "evaluation_lat_min",
            "evaluation_lon_max", "evaluation_lat_max",
        )
        rows = db.execute(
            "SELECT " + ",".join(columns) + " FROM frames "
            "WHERE region_key=? AND nominal_minute=? "
            "ORDER BY source, product_timestamp DESC, capture_id DESC",
            (region_key, nominal_minute),
        ).fetchall()
        selected: dict[str, dict] = {}
        for row in rows:
            value = dict(zip(columns, row, strict=True))
            selected.setdefault(str(value["source"]), value)
        frames = []
        for value in selected.values():
            point_rows = db.execute(
                "SELECT grid_lat, grid_lon, max_intensity, max_quality, "
                "observation_count FROM frame_points WHERE capture_id=?",
                (value["capture_id"],),
            )
            value["coverage_bbox"] = (
                value.pop("coverage_lon_min"), value.pop("coverage_lat_min"),
                value.pop("coverage_lon_max"), value.pop("coverage_lat_max"),
            )
            value["evaluation_bbox"] = (
                value.pop("evaluation_lon_min"), value.pop("evaluation_lat_min"),
                value.pop("evaluation_lon_max"), value.pop("evaluation_lat_max"),
            )
            value["points"] = {
                (int(grid_lat), int(grid_lon)): [intensity, quality, int(count)]
                for grid_lat, grid_lon, intensity, quality, count in point_rows
            }
            if len(value["points"]) != int(value.pop("grid_point_count")):
                raise ValueError("kalibratiecorpus bevat onvolledig rasterframe")
            frames.append(value)
        return frames

    @staticmethod
    def _refresh_profiles(
        db: sqlite3.Connection,
        profile_keys: set[tuple[str, str, str]],
        now: float,
    ) -> None:
        for scope_key, source_from, source_to in sorted(profile_keys):
            scope_clause = "" if scope_key == "*" else " AND scope_key=?"
            params: tuple[Any, ...] = (
                (source_from, source_to)
                if scope_key == "*"
                else (source_from, source_to, scope_key)
            )
            row = db.execute(
                "SELECT count(*), avg(detection_ratio), avg(extra_fraction), "
                "avg(wet_area_ratio), avg(f1_score), avg(intensity_bias), "
                "avg(shift_lat_cells), avg(shift_lon_cells), avg(latency_seconds) "
                "FROM bias_samples WHERE from_source=? AND to_source=?"
                + scope_clause,
                params,
            ).fetchone()
            count = int(row[0] or 0)
            if count == 0:
                db.execute(
                    "DELETE FROM bias_profiles WHERE scope_key=? AND "
                    "from_source=? AND to_source=?",
                    (scope_key, source_from, source_to),
                )
                continue
            values = tuple(
                round(float(value), 6) if value is not None else None
                for value in row[1:]
            )
            db.execute(
                "INSERT INTO bias_profiles VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(scope_key,from_source,to_source) DO UPDATE SET "
                "sample_count=excluded.sample_count,"
                "mean_detection_ratio=excluded.mean_detection_ratio,"
                "mean_extra_fraction=excluded.mean_extra_fraction,"
                "mean_wet_area_ratio=excluded.mean_wet_area_ratio,"
                "mean_f1_score=excluded.mean_f1_score,"
                "mean_intensity_bias=excluded.mean_intensity_bias,"
                "mean_shift_lat_cells=excluded.mean_shift_lat_cells,"
                "mean_shift_lon_cells=excluded.mean_shift_lon_cells,"
                "mean_latency_seconds=excluded.mean_latency_seconds,"
                "confidence=excluded.confidence,updated_at=excluded.updated_at,"
                "algorithm_version=excluded.algorithm_version",
                (
                    scope_key, source_from, source_to, count, *values,
                    _confidence(count), now, CORPUS_ALGORITHM_VERSION,
                ),
            )

    @staticmethod
    def _receipt(db: sqlite3.Connection, receipt_id: str) -> dict | None:
        columns = (
            "receipt_id", "payload_sha256", "applied_at", "captures_received",
            "captures_inserted", "artifacts_inserted", "grid_points_inserted",
            "groups_recomputed", "comparisons_recomputed",
            "bias_samples_recomputed", "algorithm_version",
        )
        row = db.execute(
            "SELECT " + ",".join(columns) + " FROM ingest_receipts WHERE receipt_id=?",
            (receipt_id,),
        ).fetchone()
        return dict(zip(columns, row, strict=True)) if row is not None else None

    def _repeat_result(
        self,
        db: sqlite3.Connection,
        receipt: dict,
        payload_sha: str,
    ) -> dict[str, Any]:
        if receipt["payload_sha256"] != payload_sha:
            raise ValueError("receipt-ID werd al gebruikt voor andere captures")
        return self._result(db, receipt, already_applied=True)

    def _result(
        self,
        db: sqlite3.Connection,
        receipt: dict,
        *,
        already_applied: bool,
    ) -> dict[str, Any]:
        public_receipt = {
            key: value for key, value in receipt.items()
            if key != "payload_sha256"
        }
        public_receipt["capture_set_sha256"] = receipt["payload_sha256"]
        return {
            "method": "worker_calibration_corpus_v1",
            "receipt": public_receipt,
            "already_applied": bool(already_applied),
            "statistics": self._statistics(db),
            **self._profiles(db),
        }

    def _statistics(self, db: sqlite3.Connection) -> dict[str, Any]:
        counts = {
            table: int(db.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in (
                "ingest_receipts", "artifacts", "captures", "frames",
                "frame_points", "comparisons", "bias_samples", "bias_profiles",
            )
        }
        artifact_bytes = int(
            db.execute("SELECT coalesce(sum(artifact_bytes),0) FROM artifacts").fetchone()[0]
        )
        bounds = db.execute(
            "SELECT min(product_timestamp), max(product_timestamp), "
            "count(DISTINCT source), count(DISTINCT region_key) FROM frames"
        ).fetchone()
        return {
            "schema_version": CORPUS_SCHEMA_VERSION,
            "algorithm_version": CORPUS_ALGORITHM_VERSION,
            "frame_algorithm_version": FRAME_ALGORITHM_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "provenance_grade": PROVENANCE_GRADE,
            "exact_raw_artifacts_complete": EXACT_RAW_ARTIFACTS_COMPLETE,
            "parser_id": None,
            "upstream_product_id": None,
            "upstream_content_type": None,
            "receipts": counts["ingest_receipts"],
            "artifacts": counts["artifacts"],
            "artifact_bytes": artifact_bytes,
            "captures": counts["captures"],
            "frames": counts["frames"],
            "grid_points": counts["frame_points"],
            "comparisons": counts["comparisons"],
            "bias_samples": counts["bias_samples"],
            "bias_profiles": counts["bias_profiles"],
            "sources": int(bounds[2]),
            "regions": int(bounds[3]),
            "oldest_product_timestamp": bounds[0],
            "newest_product_timestamp": bounds[1],
            "database_bytes": self.path.stat().st_size if self.path.exists() else 0,
        }

    def _profiles(self, db: sqlite3.Connection) -> dict[str, Any]:
        columns = (
            "scope_key", "from_source", "to_source", "sample_count",
            "mean_detection_ratio", "mean_extra_fraction", "mean_wet_area_ratio",
            "mean_f1_score", "mean_intensity_bias", "mean_shift_lat_cells",
            "mean_shift_lon_cells", "mean_latency_seconds", "confidence",
            "updated_at", "algorithm_version",
        )
        total = int(db.execute("SELECT count(*) FROM bias_profiles").fetchone()[0])
        rows = db.execute(
            "SELECT " + ",".join(columns) + " FROM bias_profiles "
            "ORDER BY scope_key<>'*', sample_count DESC, from_source, to_source "
            "LIMIT ?",
            (self.bounds.profiles_returned,),
        ).fetchall()
        return {
            "profiles": [dict(zip(columns, row, strict=True)) for row in rows],
            "profiles_total": total,
            "profiles_truncated": total > len(rows),
        }


def calibration_corpus_ingest(job) -> dict[str, Any]:
    """Service-compatible entrypoint using the default persistent corpus."""
    return CalibrationCorpusStore().ingest_job(job)


def calibration_corpus_snapshot(job) -> dict[str, Any]:
    """Return the compact corpus view for an exactly empty worker payload."""
    payload = getattr(job, "payload", None)
    if type(payload) is not dict or payload:
        raise ValueError("kalibratiecorpussnapshot vereist exact een leeg object")
    return {
        "method": SNAPSHOT_METHOD,
        **CalibrationCorpusStore().snapshot(),
    }


__all__ = [
    "ARTIFACT_KIND",
    "CORPUS_ALGORITHM_VERSION",
    "CORPUS_SCHEMA_VERSION",
    "EXACT_RAW_ARTIFACTS_COMPLETE",
    "PROVENANCE_GRADE",
    "SNAPSHOT_METHOD",
    "CalibrationCorpusStore",
    "CorpusBounds",
    "calibration_corpus_ingest",
    "calibration_corpus_snapshot",
]
