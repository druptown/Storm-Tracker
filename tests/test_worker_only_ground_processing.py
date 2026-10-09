from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path

from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceCapacityError,
)
from storm_tracker_worker import netatmo_native
from storm_tracker_worker.desired_state import DesiredStateStore
from storm_tracker_worker.raw_artifacts import RawArtifactStore
from storm_tracker_worker.raw_capture_service_composition import (
    RawCaptureServiceRuntime,
)
from storm_tracker_worker.tracking_core.operational_snapshot import (
    decode_control_envelope,
)
from storm_tracker_worker.worker_only_ground_processing import (
    decode_worker_only_ground_receipts,
)
from storm_tracker_worker.worker_only_service_runtime import (
    WorkerOnlyServiceRuntime,
)


NOW = 1_800_300_200.0
GOLDEN = (
    Path(__file__).parent
    / "fixtures"
    / "netatmo_getpublicdata_ground_golden.json"
)


def _capture(raw: RawArtifactStore):
    payload = GOLDEN.read_bytes()
    for timestamp in (
        1_721_999_800,
        1_721_999_900,
        1_722_000_000,
        1_722_000_001,
        1_722_000_102,
    ):
        payload = payload.replace(
            str(timestamp).encode("ascii"),
            str(timestamp + 78_300_000).encode("ascii"),
        )
    context = {
        "schema": netatmo_native.NETATMO_QUERY_CONTEXT_SCHEMA,
        "method": "GET",
        "origin": netatmo_native.NETATMO_ORIGIN,
        "path": netatmo_native.NETATMO_PUBLIC_DATA_PATH,
        "query_identity": "worker-only-ground-test-query",
        "query_kind": "local",
        "requested_center": {"latitude": 51.047, "longitude": 4.418},
        "requested_radius_km": 10.0,
        "wire_geometry": "bounding_box_not_geodesic_circle",
        "parameters": [],
        "coverage_status": "unknown_not_proven_by_station_count",
    }
    context_sha = netatmo_native._query_context_sha256(context)
    content_sha = hashlib.sha256(payload).hexdigest()
    receipt = raw.store(
        provider="netatmo",
        product=netatmo_native.NETATMO_JSON_PRODUCT,
        fetch_id="worker-only-ground-test:response_json",
        content_type="application/json",
        received_at=NOW - 20.0,
        upstream_id=(
            "netatmo/getpublicdata"
            f"|query_sha256={context_sha}|content_sha256={content_sha}"
        ),
        provenance=netatmo_native._native_provenance(
            context,
            transport_attempt=1,
        ),
        payload=payload,
    )
    return raw.commit_capture(
        provider="netatmo",
        product=netatmo_native.NETATMO_CAPTURE_PRODUCT,
        fetch_id="worker-only-ground-test:capture",
        upstream_capture_id=(
            f"{netatmo_native.NETATMO_CAPTURE_PRODUCT}"
            f"|query_sha256={context_sha}|content_sha256={content_sha}"
        ),
        product_timestamp=None,
        members={"response_json": receipt.receipt_id},
    )


def _desired(path: Path) -> DesiredStateStore:
    store = DesiredStateStore(path)
    claim = store.claim_producer(
        "ha:worker-only-ground-test",
        "worker-only-ground-test-claim",
        claimed_at=NOW - 2.0,
    )
    store.apply({
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:worker-only-ground-test",
        "authority_epoch": claim["authority_epoch"],
        "generation": 1,
        "issued_at": NOW - 1.0,
        "previous_state_sha256": None,
        "targets": [{
            "target_id": "home",
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": 51.047,
            "longitude": 4.418,
            "position_observed_at": NOW - 1.0,
            "location_accuracy_m": 10.0,
            "observation_radius_km": 20.0,
            "country_code": "BE",
        }],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    }, received_at=NOW - 0.5)
    return store


class _RetainedGroundRuntime(RawCaptureServiceRuntime):
    def __init__(self, desired, raw, capture):
        self._desired = desired
        self._raw = raw
        self._capture = capture
        self._stopped = False

    @property
    def desired_state_store(self):
        return self._desired

    @property
    def raw_artifact_store(self):
        return self._raw

    @property
    def latest_tick_result(self):
        return type("Result", (), {
            "terminal": False,
            "workload_id": None,
            "receipt_sha256": None,
            "source_capture_id": None,
            "status": "idle",
            "reason_code": "not_started",
        })()

    def retained_successful_captures(self):
        return (("ground.netatmo.region", self._capture),)

    def latest_terminal_results(self):
        return ()

    async def async_start(self):
        return self.diagnostics()

    async def async_tick(self):
        return self.diagnostics()

    async def async_stop(self):
        self._stopped = True
        return self.diagnostics()

    def diagnostics(self):
        return {
            "state": "stopped" if self._stopped else "running",
            "heavy_lane_active": False,
            "ownership_fence_held": not self._stopped,
            "shutdown_complete": self._stopped,
            "last_tick": {"status": "idle", "reason_code": "not_started"},
        }


def test_worker_only_start_decodes_retained_netatmo_into_live_ground_cycle(tmp_path):
    raw = RawArtifactStore((tmp_path / "raw").resolve())
    raw.initialize()
    capture = _capture(raw)
    desired = _desired(tmp_path / "desired.sqlite3")
    published = []
    match_calls = []
    processor_scopes = []

    def scoped_processor(**kwargs):
        processor_scopes.append(kwargs["target_scopes"])
        return decode_worker_only_ground_receipts(**kwargs)

    class FakeCellTracker:
        def process_ground_receipts(self, **kwargs):
            match_calls.append(kwargs)
            return 0

        @staticmethod
        def diagnostics():
            return {
                "active_tracks": 0,
                "tracked_tracks": 0,
                "motion_ready_tracks": 0,
                "region_states": 0,
                "restart_persistent": True,
                "store_error": None,
                "ground_track_match_candidate_count": 0,
                "ground_track_match_admission_status": (
                    "shadow_candidate_unadmitted"
                ),
            }

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_RetainedGroundRuntime(desired, raw, capture),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        ground_evidence_path=tmp_path / "ground.sqlite3",
        ground_processor=scoped_processor,
        cell_tracker=FakeCellTracker(),
        clock=lambda: NOW,
    )

    async def start_and_first_tick():
        await runtime.async_start()
        return await runtime.async_tick()

    diagnostics = asyncio.run(start_and_first_tick())

    assert diagnostics["ground_source_count"] == 1
    assert diagnostics["ground_fact_count"] == 7
    assert diagnostics["ground_errors"] == {}
    assert diagnostics["counts"]["ground_successes"] == 1
    assert diagnostics["counts"]["ground_track_match_successes"] == 1
    assert len(match_calls) == 1
    assert len(processor_scopes) == 1
    assert len(processor_scopes[0]) == 1
    assert processor_scopes[0][0].target.target_id == "home"
    assert processor_scopes[0][0].target.available is True
    assert processor_scopes[0][0].radius_km == 20.0
    assert match_calls[0]["workload_id"] == "ground.netatmo.region"
    assert len(match_calls[0]["receipts"]) == 7
    snapshot = decode_control_envelope(published[-1])["snapshot"]
    providers = {
        item["provider_id"]: item for item in snapshot["providers"]["items"]
    }
    assert providers["ground.netatmo.region"]["state"] == "operational"
    assert providers["ground.metar.global"]["state"] == "degraded"
    assert providers["ground.kmi.aws_10min"]["state"] == "degraded"
    assert providers["ground.kmi.aws_10min"]["reason_codes"] == [
        "ground_target_candidate_unavailable"
    ]


def test_worker_only_logs_fixed_canonical_capacity_reason(tmp_path, caplog):
    raw = RawArtifactStore((tmp_path / "raw").resolve())
    raw.initialize()
    capture = _capture(raw)
    desired = _desired(tmp_path / "desired.sqlite3")

    def capacity_failure(**_kwargs):
        raise CanonicalGroundEvidenceCapacityError(
            "ground fact row bound reached"
        )

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_RetainedGroundRuntime(desired, raw, capture),
        publisher=lambda _payload: None,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        ground_evidence_path=tmp_path / "ground.sqlite3",
        ground_processor=capacity_failure,
        clock=lambda: NOW,
    )

    async def start_and_first_tick():
        await runtime.async_start()
        return await runtime.async_tick()

    with caplog.at_level(logging.ERROR):
        diagnostics = asyncio.run(start_and_first_tick())

    assert diagnostics["ground_errors"] == {
        "ground.netatmo.region": "ground_processing_failed"
    }
    assert (
        "class=CanonicalGroundEvidenceCapacityError "
        "reason=ground fact row bound reached"
    ) in caplog.text
