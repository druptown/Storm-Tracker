from __future__ import annotations

import hashlib
import sqlite3

from storm_tracker_worker.postgis_projection import (
    CONTRACT,
    PostGISProjectionSettings,
    PostGISRawCalibrationProjector,
)
from storm_tracker_worker.raw_artifacts import (
    RawArtifactCaptureMember,
    RawArtifactCaptureReceipt,
)
from storm_tracker_worker.raw_calibration_index import RawCalibrationIndex


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _capture(label: str) -> RawArtifactCaptureReceipt:
    return RawArtifactCaptureReceipt(
        capture_id=_sha("capture:" + label),
        provider="dwd",
        product="radolan-rv",
        fetch_id="fetch-" + label,
        upstream_capture_id="upstream-" + label,
        product_timestamp=1_000.0,
        manifest_sha256=_sha("manifest:" + label),
        capture_receipt_sha256=_sha("receipt:" + label),
        created_at=1_002.0,
        members=(
            RawArtifactCaptureMember(
                role="payload",
                receipt_id="member-" + label,
                content_sha256=_sha("content:" + label),
                receipt_sha256=_sha("member-receipt:" + label),
                received_at=1_001.0,
            ),
        ),
    )


def _settings() -> PostGISProjectionSettings:
    return PostGISProjectionSettings(
        host="192.0.2.10",
        port=5433,
        database="storm_tracker",
        user="storm_ingest",
        password="not-printed",
    )


class _FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.selected = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, parameters):
        normalized = " ".join(sql.split())
        if normalized.startswith("INSERT INTO stv3_ingest.raw_captures"):
            assert "%s::double precision IS NULL" in normalized
            assert normalized.count("to_timestamp(%s::double precision)") == 4
            capture_id = parameters[0]
            self.connection.rows.setdefault(
                capture_id,
                (
                    *parameters[:6],
                    parameters[6],
                    parameters[8],
                    parameters[9],
                    parameters[10],
                    parameters[11],
                    parameters[12],
                    parameters[13],
                ),
            )
        elif normalized.startswith("SELECT capture_id"):
            self.selected = self.connection.rows.get(parameters[0])
        else:
            raise AssertionError(normalized)

    def fetchone(self):
        return self.selected


class _FakeConnection:
    def __init__(self, rows=None):
        self.rows = {} if rows is None else dict(rows)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_projection_settings_are_opt_in_and_hide_password():
    assert PostGISProjectionSettings.from_environment({}) is None
    settings = PostGISProjectionSettings.from_environment(
        {
            "STV3_POSTGIS_ENABLED": "true",
            "STV3_POSTGIS_HOST": "192.0.2.10",
            "STV3_POSTGIS_PORT": "5433",
            "STV3_POSTGIS_DATABASE": "storm_tracker",
            "STV3_POSTGIS_USER": "storm_ingest",
            "STV3_POSTGIS_PASSWORD": "secret-value",
            "STV3_POSTGIS_SSLMODE": "disable",
        }
    )
    assert settings is not None
    assert settings.port == 5433
    assert "secret-value" not in repr(settings)


def test_raw_index_projection_outbox_is_durable_and_leased(tmp_path):
    index = RawCalibrationIndex(tmp_path / "raw-calibration.sqlite3")
    index.record(
        "radar.dwd.radolan_rv", _capture("one"), indexed_at=1_003.0
    )
    index.record(
        "radar.dwd.radolan_rv", _capture("two"), indexed_at=1_004.0
    )
    assert index.statistics()["postgis_projection"] == {
        "ready": 2,
        "leased": 0,
        "projected": 0,
    }

    first = index.claim_projection_batch(
        lease_owner="projector-one", now=2_000.0, limit=1
    )
    assert len(first) == 1
    assert first[0]["attempts"] == 1
    index.retry_projection_batch(
        [first[0]["capture_id"]],
        lease_owner="projector-one",
        now=2_001.0,
        delay_seconds=5.0,
        error_code="postgis_unavailable",
    )
    assert index.claim_projection_batch(
        lease_owner="projector-one", now=2_005.0, limit=2
    )[0]["capture_id"] != first[0]["capture_id"]

    retried = index.claim_projection_batch(
        lease_owner="projector-two", now=2_007.0, limit=2
    )
    assert [row["capture_id"] for row in retried] == [first[0]["capture_id"]]
    index.complete_projection_batch(
        [retried[0]["capture_id"]],
        lease_owner="projector-two",
        projected_at=2_008.0,
    )
    projection = index.statistics()["postgis_projection"]
    assert projection["projected"] == 1
    assert projection["leased"] == 1


def test_raw_index_can_expedite_only_ready_projection_retries(tmp_path):
    index = RawCalibrationIndex(tmp_path / "raw-calibration.sqlite3")
    index.record("radar.dwd.radolan_rv", _capture("ready"), indexed_at=1_003.0)
    index.record("radar.dwd.radolan_rv", _capture("leased"), indexed_at=1_004.0)
    claimed = index.claim_projection_batch(
        lease_owner="projector-one", now=2_000.0, limit=2
    )
    index.retry_projection_batch(
        [claimed[0]["capture_id"]],
        lease_owner="projector-one",
        now=2_001.0,
        delay_seconds=3_600.0,
        error_code="postgis_unavailable",
    )

    assert index.expedite_projection_retries(now=2_002.0) == 1
    retried = index.claim_projection_batch(
        lease_owner="projector-two", now=2_002.0, limit=2
    )
    assert [row["capture_id"] for row in retried] == [claimed[0]["capture_id"]]
    assert index.statistics()["postgis_projection"]["leased"] == 2


def test_raw_index_additively_backfills_pre_projection_database(tmp_path):
    path = tmp_path / "raw-calibration.sqlite3"
    index = RawCalibrationIndex(path)
    capture = _capture("legacy")
    index.record("radar.dwd.radolan_rv", capture, indexed_at=1_003.0)
    with sqlite3.connect(path) as database:
        database.execute("DROP TABLE raw_calibration_projection_outbox")
        database.commit()

    restored = RawCalibrationIndex(path)
    assert restored.initialize()["postgis_projection"]["ready"] == 1
    assert restored.claim_projection_batch(
        lease_owner="backfill", now=2_000.0
    )[0]["capture_id"] == capture.capture_id


def test_projector_commits_postgres_before_local_ack(tmp_path):
    index = RawCalibrationIndex(tmp_path / "raw-calibration.sqlite3")
    capture = _capture("success")
    index.record("radar.dwd.radolan_rv", capture, indexed_at=1_003.0)
    connection = _FakeConnection()
    projector = PostGISRawCalibrationProjector(
        index,
        _settings(),
        clock=lambda: 2_000.0,
        connect_factory=lambda **kwargs: connection,
    )

    result = projector.flush_once()
    assert result["status"] == "current"
    assert result["batch"] == 1
    assert result["projected"] == 1
    assert connection.committed is True
    assert connection.closed is True
    assert connection.rows[capture.capture_id][11] == CONTRACT


def test_projector_types_nullable_product_timestamp_for_extended_protocol(tmp_path):
    index = RawCalibrationIndex(tmp_path / "raw-calibration.sqlite3")
    source = _capture("nullable-product-time")
    capture = RawArtifactCaptureReceipt(
        capture_id=source.capture_id,
        provider=source.provider,
        product=source.product,
        fetch_id=source.fetch_id,
        upstream_capture_id=source.upstream_capture_id,
        product_timestamp=None,
        manifest_sha256=source.manifest_sha256,
        capture_receipt_sha256=source.capture_receipt_sha256,
        created_at=source.created_at,
        members=source.members,
    )
    index.record("ground.metar.global", capture, indexed_at=1_003.0)
    connection = _FakeConnection()
    projector = PostGISRawCalibrationProjector(
        index,
        _settings(),
        clock=lambda: 2_000.0,
        connect_factory=lambda **kwargs: connection,
    )

    result = projector.flush_once()

    assert result["status"] == "current"
    assert result["projected"] == 1
    assert connection.rows[capture.capture_id][6] is None


def test_projector_releases_batch_when_postgres_is_unavailable(tmp_path):
    index = RawCalibrationIndex(tmp_path / "raw-calibration.sqlite3")
    index.record(
        "radar.dwd.radolan_rv", _capture("retry"), indexed_at=1_003.0
    )

    def unavailable(**kwargs):
        raise OSError("database unavailable")

    projector = PostGISRawCalibrationProjector(
        index,
        _settings(),
        clock=lambda: 2_000.0,
        connect_factory=unavailable,
    )
    result = projector.flush_once()
    assert result["status"] == "degraded"
    assert result["last_error_code"] == "postgis_projection_failed"
    assert result["ready"] == 1
    assert result["leased"] == 0
    assert result["projected"] == 0
