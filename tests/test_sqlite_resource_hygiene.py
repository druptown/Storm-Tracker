"""Regression checks for deterministic SQLite connection cleanup."""
from __future__ import annotations

import gc
import warnings

import pytest

from storm_tracker_worker import service as worker_service_module
from storm_tracker_worker.service import _JobReceiptStore
from storm_tracker_worker.tracking_core.engine import (
    meteorological_evidence_store as evidence_store_module,
)
from storm_tracker_worker.tracking_core.engine import (
    raw_artifact_store as raw_store_module,
)
from storm_tracker_worker.tracking_core.engine.meteorological_evidence_store import (
    MeteorologicalEvidenceStore,
)
from storm_tracker_worker.tracking_core.engine.raw_artifact_store import (
    RawArtifactStore,
)


def test_short_lived_store_connections_never_reach_gc_unclosed(
    tmp_path,
    calibration_store_module,
    durable_processing_module,
):
    """Repeated store operations must not defer connection cleanup to GC."""
    durable = durable_processing_module.DurableProcessingStore(
        tmp_path / "durable.sqlite3"
    )
    calibration = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    raw = RawArtifactStore(tmp_path / "raw.sqlite3")
    evidence = MeteorologicalEvidenceStore(tmp_path / "evidence.sqlite3")
    receipts = _JobReceiptStore(
        tmp_path / "receipts.sqlite3",
        instance_id="resource-hygiene",
        build_id="test",
    )

    # Clear warnings from objects created by earlier tests before this test owns
    # the warning window.
    gc.collect()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        for _ in range(8):
            durable.initialize()
            durable.integrity_check()
            calibration.initialize()
            calibration.load_bias_profiles()
            raw.initialize()
            raw.stats()
            raw.integrity_check()
            evidence.initialize()
            evidence.stats()
            evidence.integrity_check()
            receipts.initialize()
            receipts.load("missing-job")
        gc.collect()
        gc.collect()

    resource_warnings = [
        warning
        for warning in caught
        if issubclass(warning.category, ResourceWarning)
    ]
    assert resource_warnings == []


class _FailingPragmaConnection:
    """Minimal connection double that fails before `_connect()` can return."""

    def __init__(self) -> None:
        self.closed = False
        self.row_factory = None

    def execute(self, _statement):
        raise RuntimeError("synthetic PRAGMA failure")

    def close(self) -> None:
        self.closed = True


def test_connection_setup_failure_closes_half_configured_connection(
    tmp_path,
    monkeypatch,
    calibration_store_module,
    durable_processing_module,
):
    """Every factory must close when connection configuration aborts."""
    cases = (
        (
            durable_processing_module,
            durable_processing_module.DurableProcessingStore(
                tmp_path / "durable-failure.sqlite3"
            ),
        ),
        (
            calibration_store_module,
            calibration_store_module.CalibrationDataStore(
                str(tmp_path / "calibration-failure.sqlite3")
            ),
        ),
        (
            worker_service_module,
            _JobReceiptStore(
                tmp_path / "receipt-failure.sqlite3",
                instance_id="resource-hygiene",
                build_id="test",
            ),
        ),
        (
            raw_store_module,
            RawArtifactStore(tmp_path / "raw-failure.sqlite3"),
        ),
        (
            evidence_store_module,
            MeteorologicalEvidenceStore(tmp_path / "evidence-failure.sqlite3"),
        ),
    )

    for module, store in cases:
        connection = _FailingPragmaConnection()
        with monkeypatch.context() as patch:
            patch.setattr(
                module.sqlite3,
                "connect",
                lambda *_args, **_kwargs: connection,
            )
            with pytest.raises(RuntimeError, match="synthetic PRAGMA failure"):
                store._connect()
        assert connection.closed
