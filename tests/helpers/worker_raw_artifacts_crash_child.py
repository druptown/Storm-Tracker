"""Subprocess helper for abrupt raw-artifact store boundary tests."""
from __future__ import annotations

import os
from pathlib import Path
import sys

from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactStore,
)


root = Path(sys.argv[1])
checkpoint = sys.argv[2]


def crash(name: str) -> None:
    if name == checkpoint:
        os._exit(73)


store = RawArtifactStore(root, fault_injector=crash)
store.initialize()
if checkpoint == "between_capture_members":
    store.store(
        provider="kmi",
        product="kmi_appv4_forecast_manifest",
        fetch_id="job-crash:forecast-json",
        content_type="application/json",
        received_at=1_800_000_000.25,
        upstream_id="forecast/frame-content-fallback",
        provenance={
            "representation": NATIVE_REPRESENTATION,
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "provider-http-adapter",
            "collector_version": "2026.08.01",
            "transport": {"method": "GET", "content_encoding": "identity"},
        },
        payload=b"exact-forecast-json",
    )
    os._exit(73)
store.store(
    provider="test-provider",
    product="native-crash-product",
    fetch_id="crash-job:native-object",
    content_type="application/octet-stream",
    received_at=1_800_000_000.0,
    upstream_id="frame/2027-01-15T12:00:00Z",
    provenance={
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": "crash-test",
        "collector_version": "1",
    },
    payload=b"\x00native-crash-evidence",
)
raise SystemExit("checkpoint was not reached")
