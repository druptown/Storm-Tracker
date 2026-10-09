from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
import hashlib
import threading
from types import SimpleNamespace

import pytest

from storm_tracker_worker.acquisition_runtime import ShadowTickResult
from storm_tracker_worker.desired_state import DesiredStateStore
from storm_tracker_worker.raw_artifacts import (
    RawArtifactCaptureMember,
    RawArtifactCaptureReceipt,
)
from storm_tracker_worker.raw_capture_service_composition import (
    RawCaptureServiceRuntime,
)
from storm_tracker_worker.multi_source_radar_facts import VerifiedRadarFieldSource
from storm_tracker_worker.multi_source_radar_snapshot import (
    PositiveRadarTargetObservation,
    WithheldDryRadarTargetObservation,
    WithheldUndetectRadarTargetObservation,
    UNDETECT_OBSERVATION_REASON,
    ZERO_OBSERVATION_REASON,
)
from storm_tracker_worker.tracking_core.operational_snapshot import (
    decode_control_envelope,
)
from storm_tracker_worker.operational_snapshot_store import OperationalSnapshotStore
from storm_tracker_worker.operational_fact_store import OperationalFactStoreConflict
from storm_tracker_worker.worker_only_service_runtime import (
    MAX_SNAPSHOT_SETTLE_TICKS,
    SOURCE_COUNT,
    WorkerOnlyServiceRuntime,
    WorkerOnlyServiceRuntimeError,
    _safe_failure_reason,
)
from storm_tracker_worker.worker_only_radar_processing import (
    WorkerOnlyRadarProcessingResult,
)
from storm_tracker_worker.worker_only_cell_tracking import (
    ADMISSION_STATUS,
    SHADOW_ARRIVAL_HORIZON_SECONDS,
    WorkerOnlyShadowArrivalCandidate,
)
from storm_tracker_worker import service
from storm_tracker_worker import worker_only_service_runtime as runtime_module
from storm_tracker_worker.multi_source_cycle_assembler import (
    CapturedSourceEvidence,
)
from storm_tracker_worker.openmeteo_operational_guidance import (
    OpenMeteoTargetGuidance,
    SOURCE_ID as OPENMETEO_SOURCE_ID,
)


NOW = 1_800_300_000.0


def test_safe_failure_reason_never_returns_free_exception_text():
    assert _safe_failure_reason(
        RuntimeError("raw_capture_retention_guard_blocked")
    ) == "raw_capture_retention_guard_blocked"
    assert _safe_failure_reason(
        RuntimeError("SUPERS3CRETTOKEN-DO-NOT-LEAK")
    ) == "unclassified"
    assert _safe_failure_reason(
        RuntimeError("desired_state_inconsistent")
    ) == "desired_state_inconsistent"
    assert _safe_failure_reason(
        RuntimeError("existing_raw_capture_accounting_integrity")
    ) == "existing_raw_capture_accounting_integrity"
    assert _safe_failure_reason(
        RuntimeError("existing_raw_capture_committed_missing")
    ) == "existing_raw_capture_committed_missing"


def test_live_blitzortung_diagnostics_drive_direct_provider_status():
    runtime = object.__new__(WorkerOnlyServiceRuntime)
    runtime._raw = SimpleNamespace(diagnostics=lambda: {
        "event_ingress": {
            "blitzortung": {
                "readiness": {
                    "ready": True,
                    "reason_code": "regional_qos0_raw_capture_active",
                    "status": "running_exact_raw",
                },
            },
        },
    })
    assert runtime._provider_status_overrides() == {
        "lightning.blitzortung.event_ingress": (
            "operational",
            "regional_qos0_raw_capture_active",
        ),
    }


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _desired_store(path, *, model_guidance_enabled=True, target_kind="home"):
    store = DesiredStateStore(path)
    claim = store.claim_producer(
        "ha:worker-only-test",
        "worker-only-test-claim",
        claimed_at=NOW - 2,
    )
    store.apply(
        {
            "contract": "stv3-worker-desired-state-v1",
            "producer_id": "ha:worker-only-test",
            "authority_epoch": claim["authority_epoch"],
            "generation": 1,
            "issued_at": NOW - 1,
            "previous_state_sha256": None,
            "targets": [{
                "target_id": "home",
                "display_name": "Thuis",
                "kind": target_kind,
                "available": True,
                "latitude": 50.8,
                "longitude": 4.2,
                "position_observed_at": NOW - 1,
                "location_accuracy_m": 10.0,
                "observation_radius_km": 250.0,
                "country_code": "BE",
            }],
            "settings": {
                "ground_radius_km": 20.0,
                "lightning_source_mode": "auto",
                "model_guidance_enabled": model_guidance_enabled,
                "region_sharing_distance_km": 100.0,
            },
        },
        received_at=NOW - 0.5,
    )
    return store


def _apply_next_desired_state(
    store,
    previous_state_sha256,
    *,
    generation=2,
    latitude=50.81,
    longitude=4.21,
    target_kind="home",
):
    store.apply(
        {
            "contract": "stv3-worker-desired-state-v1",
            "producer_id": "ha:worker-only-test",
            "authority_epoch": 1,
            "generation": generation,
            "issued_at": NOW - 0.25,
            "previous_state_sha256": previous_state_sha256,
            "targets": [{
                "target_id": "home",
                "display_name": "Thuis",
                "kind": target_kind,
                "available": True,
                "latitude": latitude,
                "longitude": longitude,
                "position_observed_at": NOW - 0.25,
                "location_accuracy_m": 10.0,
                "observation_radius_km": 250.0,
                "country_code": "BE",
            }],
            "settings": {
                "ground_radius_km": 20.0,
                "lightning_source_mode": "auto",
                "model_guidance_enabled": True,
                "region_sharing_distance_km": 100.0,
            },
        },
        received_at=NOW - 0.1,
    )


def _capture():
    return RawArtifactCaptureReceipt(
        capture_id="capture:dwd:test",
        provider="dwd",
        product="radolan_rv",
        fetch_id="fetch:dwd:test",
        upstream_capture_id="upstream:dwd:test",
        product_timestamp=NOW - 60,
        manifest_sha256=_sha("manifest"),
        capture_receipt_sha256=_sha("capture-receipt"),
        created_at=NOW - 30,
        members=(RawArtifactCaptureMember(
            role="radolan_rv_bin",
            receipt_id="receipt:dwd:test",
            content_sha256=_sha("content"),
            receipt_sha256=_sha("member-receipt"),
            received_at=NOW - 30,
        ),),
    )


class _RawStore:
    def __init__(self):
        self.capture = _capture()

    def load_capture(self, capture_id):
        assert capture_id == self.capture.capture_id
        return self.capture


class _FakeRawRuntime(RawCaptureServiceRuntime):
    def __init__(self, desired):
        self.desired = desired
        self.store = _RawStore()
        self.result = ShadowTickResult(
            "idle", "not_started", None, None, None, None, False
        )
        self.stopped = False
        self.tick_calls = 0
        self.retained = ()
        self.terminal_results = ()

    @property
    def desired_state_store(self):
        return self.desired

    @property
    def raw_artifact_store(self):
        return self.store

    @property
    def latest_tick_result(self):
        return self.result

    def retained_successful_captures(self):
        return self.retained

    def latest_terminal_results(self):
        return self.terminal_results

    async def async_start(self):
        return self.diagnostics()

    async def async_tick(self):
        self.tick_calls += 1
        self.result = ShadowTickResult(
            "completed_shadow_raw",
            "shadow_raw_capture_complete",
            "completed",
            _sha("terminal-receipt"),
            1,
            "radar.dwd.radolan_rv",
            True,
            acquisition_job_id="job:dwd:test",
            desired_state_sha256=self.desired.load_active().state_sha256,
            source_capture_id=self.store.capture.capture_id,
        )
        return self.diagnostics()

    async def async_stop(self):
        self.stopped = True
        return self.diagnostics()

    def diagnostics(self):
        return {
            "state": "stopped" if self.stopped else "running",
            "heavy_lane_active": False,
            "ownership_fence_held": not self.stopped,
            "shutdown_complete": self.stopped,
            "last_tick": self.result.as_dict(),
        }

    def handle_control_job(self, job_type, payload):
        return {"job_type": job_type, "payload": dict(payload)}


class _SlowRawRuntime(_FakeRawRuntime):
    def __init__(self, desired):
        super().__init__(desired)
        self.tick_completed = False

    async def async_tick(self):
        await asyncio.sleep(0.03)
        result = await super().async_tick()
        self.tick_completed = True
        return result


class _DesiredStateRaceRawRuntime(_FakeRawRuntime):
    """Expose the startup race where authority arrives during the first tick."""

    def __init__(self, unavailable, ready):
        super().__init__(unavailable)
        self._ready = ready

    async def async_tick(self):
        if self.tick_calls == 0:
            self.tick_calls += 1
            self.result = ShadowTickResult(
                "idle",
                "desired_state_unconfigured",
                None,
                None,
                None,
                None,
                False,
            )
            self.desired = self._ready
            return self.diagnostics()
        return await super().async_tick()


class _BatchRawRuntime(_FakeRawRuntime):
    def __init__(self, desired, captures, results):
        super().__init__(desired)
        self._captures_by_id = {
            capture.capture_id: capture for capture in captures
        }
        self.batch_results = tuple(results)

    @property
    def latest_tick_results(self):
        return self.batch_results

    @property
    def raw_artifact_store(self):
        captures = self._captures_by_id
        return SimpleNamespace(load_capture=lambda capture_id: captures.get(capture_id))


def test_parallel_batch_consumes_every_terminal_capture(tmp_path):
    desired = _desired_store(tmp_path / "desired.sqlite3")
    dwd = _capture()
    kmi = replace(
        dwd,
        capture_id="capture:kmi:test",
        provider="kmi",
        product="kmi_public_radar_wcs_exact_capture",
        fetch_id="fetch:kmi:test",
        upstream_capture_id="upstream:kmi:test",
        product_timestamp=None,
        manifest_sha256=_sha("kmi-manifest"),
        capture_receipt_sha256=_sha("kmi-capture-receipt"),
        members=(replace(
            dwd.members[0],
            role="geoserver_response",
            receipt_id="receipt:kmi:test",
            content_sha256=_sha("kmi-content"),
            receipt_sha256=_sha("kmi-member-receipt"),
        ),),
    )
    state_sha = desired.load_active().state_sha256
    results = (
        ShadowTickResult(
            "completed_shadow_raw",
            "shadow_raw_capture_complete",
            "completed",
            _sha("dwd-terminal"),
            1,
            "radar.dwd.radolan_rv",
            True,
            acquisition_job_id="job:dwd:test",
            desired_state_sha256=state_sha,
            source_capture_id=dwd.capture_id,
        ),
        ShadowTickResult(
            "completed_shadow_raw",
            "shadow_raw_capture_complete",
            "completed",
            _sha("kmi-terminal"),
            1,
            "radar.kmi.public_wcs_unknown_quantity",
            True,
            acquisition_job_id="job:kmi:test",
            desired_state_sha256=state_sha,
            source_capture_id=kmi.capture_id,
        ),
    )
    raw = _BatchRawRuntime(desired, (dwd, kmi), results)
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=lambda _payload: None,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    consumed = runtime._consume_terminal_results()

    assert consumed == results
    assert runtime._capture_ids == {
        "radar.dwd.radolan_rv": dwd.capture_id,
        "radar.kmi.public_wcs_unknown_quantity": kmi.capture_id,
    }
    assert runtime.diagnostics()["counts"]["terminal_updates"] == 2


def test_quantitative_decode_uses_two_lanes_and_never_duplicates_a_source(
    tmp_path,
    monkeypatch,
):
    """CPU-heavy projection is bounded at two unique providers at a time."""

    desired_store = _desired_store(tmp_path / "desired.sqlite3")
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired_store),
        publisher=lambda _payload: None,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )
    active = 0
    maximum_active = 0
    active_sources = set()
    maximum_per_source = {}
    processed = []

    async def fake_decode(*, workload_id, capture_id, desired):
        nonlocal active, maximum_active
        active += 1
        active_sources.add(workload_id)
        maximum_active = max(maximum_active, active)
        maximum_per_source[workload_id] = max(
            maximum_per_source.get(workload_id, 0),
            sum(item == workload_id for item in active_sources),
        )
        processed.append((workload_id, capture_id, desired.state_sha256))
        await asyncio.sleep(0.01)
        active_sources.remove(workload_id)
        active -= 1

    monkeypatch.setattr(runtime, "_process_quantitative_capture", fake_decode)
    results = tuple(
        SimpleNamespace(
            status="completed_shadow_raw",
            workload_id=source_id,
            source_capture_id=capture_id,
        )
        for source_id, capture_id in (
            ("radar.dwd.radolan_rv", "capture:dwd:old"),
            ("radar.dwd.radolan_rv", "capture:dwd:new"),
            ("radar.knmi.rtcor", "capture:knmi"),
            ("radar.opera.dbzh", "capture:opera"),
            ("satellite.noaa.err", "capture:noaa"),
        )
    )

    asyncio.run(
        runtime._process_quantitative_results(
            results,
            desired_store.load_active(),
        )
    )

    assert maximum_active == 2
    assert maximum_per_source == {
        "radar.dwd.radolan_rv": 1,
        "radar.knmi.rtcor": 1,
        "radar.opera.dbzh": 1,
        "satellite.noaa.err": 1,
    }
    assert ("radar.dwd.radolan_rv", "capture:dwd:old") not in {
        item[:2] for item in processed
    }
    assert ("radar.dwd.radolan_rv", "capture:dwd:new") in {
        item[:2] for item in processed
    }


def test_worker_only_runtime_refreshes_snapshot_during_slow_provider_tick(
    tmp_path,
    monkeypatch,
):
    desired = _desired_store(tmp_path / "desired.sqlite3")
    raw = _SlowRawRuntime(desired)
    now = [NOW]
    publications = []
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=lambda payload: publications.append(
            (payload, raw.tick_completed)
        ),
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: now[0],
    )
    monkeypatch.setattr(
        runtime_module,
        "SNAPSHOT_MAINTENANCE_POLL_SECONDS",
        0.001,
    )

    async def scenario():
        await runtime.async_start()
        now[0] = NOW + 271.0
        return await runtime.async_tick()

    ticked = asyncio.run(scenario())

    assert ticked["state"] == "running"
    assert raw.tick_completed is True
    assert len(publications) >= 2
    assert publications[0][1] is False
    assert any(completed is False for _, completed in publications[1:])
    decoded = [
        decode_control_envelope(payload)["snapshot"]
        for payload, _completed in publications
    ]
    assert decoded[1]["evaluated_at"] == NOW + 271.0
    assert decoded[1]["valid_until"] > decoded[1]["evaluated_at"]
    assert len(decoded[1]["providers"]["items"]) == SOURCE_COUNT


def test_worker_only_runtime_ttl_refresh_keeps_all_source_statuses(tmp_path):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    now = [NOW]
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: now[0],
    )

    async def scenario():
        await runtime.async_start()
        for _ in range(6):
            now[0] += 60.0
            await runtime.async_tick()
        return runtime.diagnostics()

    diagnostics = asyncio.run(scenario())
    latest = decode_control_envelope(published[-1])["snapshot"]

    assert len(latest["providers"]["items"]) == SOURCE_COUNT
    assert diagnostics["snapshot_rich_built"] >= 3
    assert diagnostics["snapshot_unknown_built"] == 0


def test_worker_only_runtime_defers_slow_reprojection_until_after_start(
    tmp_path,
    monkeypatch,
):
    desired = _desired_store(tmp_path / "desired.sqlite3")
    raw = _FakeRawRuntime(desired)
    raw.retained = (("radar.dwd.radolan_rv", raw.store.capture),)
    now = [NOW]
    processor_completed = [False]
    maintenance_published = threading.Event()
    publications = []

    def slow_processor(**_kwargs):
        now[0] = NOW + 271.0
        if not maintenance_published.wait(timeout=2.0):
            raise RuntimeError("maintenance_snapshot_not_published")
        processor_completed[0] = True
        return ()

    def publisher(payload):
        publications.append((payload, processor_completed[0]))
        if len(publications) > 1:
            maintenance_published.set()

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=publisher,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=slow_processor,
        clock=lambda: now[0],
    )
    monkeypatch.setattr(
        runtime_module,
        "SNAPSHOT_MAINTENANCE_POLL_SECONDS",
        0.001,
    )

    async def scenario():
        started = await runtime.async_start()
        assert processor_completed[0] is False
        ticked = await runtime.async_tick()
        return started, ticked

    started, ticked = asyncio.run(scenario())

    assert started["state"] == "running"
    assert ticked["state"] == "running"
    assert processor_completed[0] is True
    assert maintenance_published.is_set()
    assert len(publications) >= 2
    assert publications[0][1] is False
    assert publications[1][1] is False
    refresh = decode_control_envelope(publications[1][0])["snapshot"]
    assert refresh["evaluated_at"] == NOW + 271.0
    assert refresh["valid_until"] > refresh["evaluated_at"]
    assert refresh["targets"]["items"][0]["current_precipitation"][
        "state"
    ] == "unknown"


def test_startup_authority_race_refetches_before_heavy_reprojection(
    tmp_path,
    monkeypatch,
):
    unavailable = DesiredStateStore(tmp_path / "unavailable.sqlite3")
    ready = _desired_store(tmp_path / "ready.sqlite3")
    raw = _DesiredStateRaceRawRuntime(unavailable, ready)
    processor_calls = []

    def processor(**kwargs):
        processor_calls.append(kwargs)
        assert raw.tick_calls == 2
        return ()

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=lambda _payload: None,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=processor,
        clock=lambda: NOW,
    )

    async def projection_already_complete(_self):
        return False

    monkeypatch.setattr(
        WorkerOnlyServiceRuntime,
        "_restore_provider_projection_if_missing",
        projection_already_complete,
    )

    async def scenario():
        await runtime.async_start()
        return await runtime.async_tick()

    ticked = asyncio.run(scenario())

    assert ticked["state"] == "running"
    assert raw.tick_calls == 2
    assert len(processor_calls) == 1
    assert processor_calls[0]["capture_id"] == "capture:dwd:test"


def test_worker_only_runtime_publishes_all_sources_then_real_capture(tmp_path):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    raw = _FakeRawRuntime(desired)
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        raw_calibration_path=tmp_path / "raw-calibration.sqlite3",
        clock=lambda: NOW,
    )

    async def scenario():
        started = await runtime.async_start()
        raw.terminal_results = (
            (
                "radar.kmi.appv4_visual",
                "blocked",
                "process_decoder_failed",
            ),
        )
        ticked = await runtime.async_tick()
        stopped = await runtime.async_stop()
        return started, ticked, stopped

    started, ticked, stopped = asyncio.run(scenario())

    assert len(published) == 2
    initial = decode_control_envelope(published[0])["snapshot"]
    updated = decode_control_envelope(published[1])["snapshot"]
    assert len(initial["providers"]["items"]) == SOURCE_COUNT == 28
    initial_states = {
        item["provider_id"]: item["state"]
        for item in initial["providers"]["items"]
    }
    assert initial_states["radar.dwd.radolan_rv"] == "degraded"
    updated_states = {
        item["provider_id"]: item["state"]
        for item in updated["providers"]["items"]
    }
    assert updated_states["radar.dwd.radolan_rv"] == "operational"
    assert updated["targets"]["items"][0]["current_precipitation"][
        "state"
    ] == "unknown"
    assert started["snapshot_revision"] == 1
    assert ticked["snapshot_revision"] == 2
    assert ticked["captured_provider_count"] == 1
    assert ticked["last_tick_workload_id"] == "radar.dwd.radolan_rv"
    assert ticked["last_tick_status"] == "completed_shadow_raw"
    assert ticked["last_tick_reason_code"] == "shadow_raw_capture_complete"
    assert ticked["raw_calibration"]["status"] == "ready"
    assert ticked["raw_calibration"]["captures"] == 1
    assert ticked["raw_calibration"]["sources"] == 1
    assert ticked["raw_calibration"]["exact_raw"] is True
    assert ticked["raw_calibration"]["source_statistics"] == [{
        "source_id": "radar.dwd.radolan_rv",
        "provider": "dwd",
        "captures": 1,
        "latest_received_at": NOW - 30.0,
    }]
    assert ticked["counts"]["raw_calibration_indexed"] == 1
    assert ticked["acquisition_terminal_results"] == [{
        "workload_id": "radar.kmi.appv4_visual",
        "status": "blocked",
        "reason_code": "process_decoder_failed",
    }]
    assert stopped["state"] == "stopped"
    assert stopped["shutdown_complete"] is True

    selected = object.__new__(service.WorkerService)
    selected._worker_first_runtime_mode = "worker_only"
    selected._worker_first_allowed_workload_ids = ()
    assert selected._worker_first_start_is_ready(started, operation="start")


    assert selected._worker_first_start_is_ready(ticked, operation="tick")
    assert not selected._worker_first_start_is_ready(ticked, operation="start")

    opera_withheld = deepcopy(ticked)
    opera_withheld["quantitative_errors"] = {
        "radar.opera.dbzh": "opera_reflectivity_decode_withheld"
    }
    assert selected._worker_first_start_is_ready(
        opera_withheld,
        operation="tick",
    )

    # A retry-delayed PostGIS backlog is non-critical. The projector exposes
    # its complete diagnostics contract while it waits for the next eligible
    # retry; worker admission must remain open during that normal state.
    waiting_for_postgis_retry = deepcopy(ticked)
    waiting_for_postgis_retry["postgis_projection"] = {
        "batch": 0,
        "contract": "stv3-postgis-raw-calibration-projection-v1",
        "counts": {
            "conflicts": 0,
            "flushes": 2,
            "projected": 0,
            "retries": 7,
        },
        "enabled": True,
        "last_error_code": "postgis_projection_failed",
        "last_success_at": None,
        "leased": 0,
        "projected": 817,
        "ready": 7,
        "status": "waiting",
    }
    assert selected._worker_first_start_is_ready(
        waiting_for_postgis_retry,
        operation="tick",
    )


def test_worker_only_runtime_wires_openmeteo_guidance_into_snapshot(
    tmp_path,
    monkeypatch,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )
    guidance = OpenMeteoTargetGuidance(
        target_id="home",
        product_valid_at=NOW,
        received_at=NOW,
        current_precipitation_mm_15m=0.2,
        current_rate_mm_h=0.8,
        first_wet_at=NOW + 15 * 60,
        forecast_total_90m_mm=0.6,
        source_receipt_sha256=_sha("model-source"),
        certificate_sha256=_sha("model-certificate"),
        process_receipt_sha256=_sha("model-process"),
    )
    monkeypatch.setattr(
        runtime_module,
        "decode_openmeteo_guidance",
        lambda **_kwargs: (guidance,),
    )

    async def inline_to_thread(function, *args, **kwargs):
        return function(*args, **kwargs)

    # Keep this focused wiring test deterministic: background executor teardown
    # is exercised elsewhere and can outlive pytest on the bundled Python runtime.
    monkeypatch.setattr(runtime_module.asyncio, "to_thread", inline_to_thread)

    async def scenario():
        await runtime.async_start()
        state_sha = desired.load_active().state_sha256
        runtime._captures[OPENMETEO_SOURCE_ID] = CapturedSourceEvidence(
            source_id=OPENMETEO_SOURCE_ID,
            product_at_ms=int(NOW * 1000),
            received_at_ms=int(NOW * 1000),
            content_sha256=_sha("model-content"),
            receipt_sha256=_sha("model-receipt"),
            size_bytes=100,
        )
        runtime._capture_ids[OPENMETEO_SOURCE_ID] = "capture:model:test"
        runtime._capture_desired_state_sha256[OPENMETEO_SOURCE_ID] = state_sha
        await runtime._process_model_guidance_capture(
            capture_id="capture:model:test",
            desired=desired.load_active(),
        )
        assert await runtime._stage_cycle()
        await runtime._output.async_publish_ready_fact()
        await runtime.async_stop()

    asyncio.run(scenario())

    snapshot = decode_control_envelope(published[-1])["snapshot"]
    target = snapshot["targets"]["items"][0]
    assert target["current_precipitation"]["state"] == "raining"
    assert target["current_precipitation"]["rain_rate_mm_h"] == 0.8
    assert target["next_threat"]["state"] == "available"
    assert target["next_threat"]["eta_minutes"] == 15.0
    assert runtime.diagnostics()["model_guidance_target_count"] == 1
    assert runtime.diagnostics()["counts"]["model_guidance_successes"] == 1


def test_worker_only_runtime_does_not_admit_disabled_openmeteo_guidance(
    tmp_path,
    monkeypatch,
):
    desired = _desired_store(
        tmp_path / "desired.sqlite3",
        model_guidance_enabled=False,
    )
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=lambda _payload: None,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )
    state = desired.load_active()
    runtime._capture_desired_state_sha256[OPENMETEO_SOURCE_ID] = (
        state.state_sha256
    )

    def should_not_decode(**_kwargs):
        raise AssertionError("disabled model guidance must not be decoded")

    monkeypatch.setattr(
        runtime_module,
        "decode_openmeteo_guidance",
        should_not_decode,
    )

    asyncio.run(runtime._process_model_guidance_capture(
        capture_id="capture:model:test",
        desired=state,
    ))

    assert runtime.diagnostics()["model_guidance_target_count"] == 0
    assert runtime.diagnostics()["counts"]["model_guidance_successes"] == 0
    assert runtime.diagnostics()["counts"]["model_guidance_failures"] == 0


def test_worker_only_runtime_restores_retained_capture_before_first_tick(tmp_path):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    raw = _FakeRawRuntime(desired)
    raw.retained = (("radar.dwd.radolan_rv", raw.store.capture),)
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    started = asyncio.run(runtime.async_start())

    assert raw.tick_calls == 0
    assert started["captured_provider_count"] == 1
    assert started["counts"]["restored_captures"] == 1
    snapshot = decode_control_envelope(published[-1])["snapshot"]
    provider = next(
        item
        for item in snapshot["providers"]["items"]
        if item["provider_id"] == "radar.dwd.radolan_rv"
    )
    assert provider["state"] == "operational"


def test_worker_only_runtime_restores_provider_error_before_first_tick(tmp_path):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    raw = _FakeRawRuntime(desired)
    raw.terminal_results = ((
        "model.openmeteo.region",
        "blocked",
        "provider_contract_rejected",
    ),)
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    asyncio.run(runtime.async_start())

    assert raw.tick_calls == 0
    assert runtime._source_errors == {  # noqa: SLF001
        "model.openmeteo.region": "provider_contract_rejected"
    }
    snapshot = decode_control_envelope(published[-1])["snapshot"]
    provider = next(
        item
        for item in snapshot["providers"]["items"]
        if item["provider_id"] == "model.openmeteo.region"
    )
    assert provider["state"] == "degraded"

def test_worker_only_runtime_deduplicates_replayed_terminal(tmp_path):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        await runtime.async_tick()
        await runtime.async_tick()
        return await runtime.async_stop()

    asyncio.run(scenario())
    assert len(published) == 2


def test_target_update_rebinds_retained_capture_without_provider_refetch(tmp_path):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    raw = _FakeRawRuntime(desired)
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        await runtime.async_tick()
        first = desired.load_active()
        _apply_next_desired_state(
            desired,
            first.state_sha256,
            latitude=50.1109,
        )
        ticked = await runtime.async_tick()
        await runtime.async_stop()
        return ticked

    ticked = asyncio.run(scenario())

    assert raw.tick_calls == 2
    assert len(published) == 3
    rebound = decode_control_envelope(published[-1])["snapshot"]
    provider = next(
        item for item in rebound["providers"]["items"]
        if item["provider_id"] == "radar.dwd.radolan_rv"
    )
    assert provider["state"] == "operational"
    assert rebound["desired_state_sha256"] == desired.load_active().state_sha256
    assert ticked["captured_provider_count"] == 1


def test_worker_only_runtime_supersedes_obsolete_ready_authority(tmp_path):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        first = desired.load_active()
        _apply_next_desired_state(desired, first.state_sha256)
        assert await runtime._stage_cycle()
        second = desired.load_active()
        _apply_next_desired_state(
            desired,
            second.state_sha256,
            generation=3,
            latitude=50.82,
        )
        assert await runtime._stage_cycle()
        diagnostics = runtime._fact_store.diagnostics()
        await runtime.async_stop()
        return diagnostics

    diagnostics = asyncio.run(scenario())

    assert diagnostics["ready"] == 1
    assert diagnostics["superseded"] == 1
    assert diagnostics["snapshot_committed"] == 1


def test_worker_only_start_recovers_expired_fact_occupying_next_snapshot_slot(
    tmp_path,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    now = [NOW]
    runtime_paths = {
        "operational_fact_path": tmp_path / "facts.sqlite3",
        "operational_snapshot_path": tmp_path / "snapshots.sqlite3",
        "snapshot_outbox_path": tmp_path / "outbox.sqlite3",
    }

    first = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        clock=lambda: now[0],
        **runtime_paths,
    )

    async def leave_terminal_fact_in_next_slot():
        await first.async_start()
        assert await first._stage_cycle()
        record = first._fact_store.select_for_publication(
            desired.load_active(),
            expected_snapshot_revision=2,
        )
        assert record is not None
        first._fact_store.mark_superseded(
            record.pipeline_item_id,
            revision_sha256=record.revision_sha256,
            reason="fact_revision_expired",
        )
        await first.async_stop()

    asyncio.run(leave_terminal_fact_in_next_slot())
    now[0] = NOW + 301.0

    recovered = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        clock=lambda: now[0],
        **runtime_paths,
    )
    started = asyncio.run(recovered.async_start())

    assert started["state"] == "running"
    assert started["last_error_code"] is None
    assert started["snapshot_revision"] == 3
    assert [decode_control_envelope(item)["revision"] for item in published] == [
        1,
        2,
        3,
    ]
    diagnostics = recovered._fact_store.diagnostics()
    assert diagnostics["snapshot_committed"] == 2
    assert diagnostics["superseded"] == 1


def test_worker_only_start_recovers_terminalized_committed_head_and_next_slot(
    tmp_path,
    monkeypatch,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    now = [NOW]
    runtime_paths = {
        "operational_fact_path": tmp_path / "facts.sqlite3",
        "operational_snapshot_path": tmp_path / "snapshots.sqlite3",
        "snapshot_outbox_path": tmp_path / "outbox.sqlite3",
    }
    interrupted = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        clock=lambda: now[0],
        **runtime_paths,
    )

    async def leave_live_failure_shape():
        await interrupted._raw.async_start()
        interrupted._ensure_output()
        assert await interrupted._stage_cycle()

        original_reconcile = interrupted._fact_store.reconcile_snapshot_head

        def interrupt_after_snapshot_commit(head):
            if head is not None:
                raise OperationalFactStoreConflict("simulated crash boundary")
            return original_reconcile(head)

        monkeypatch.setattr(
            interrupted._fact_store,
            "reconcile_snapshot_head",
            interrupt_after_snapshot_commit,
        )
        output = await interrupted._output.async_start()
        assert output["state"] == "degraded"
        assert interrupted._snapshot_store.head()["revision"] == 1

        # Reproduce the historical startup ordering: it selected the next
        # slot before crash reconciliation, terminalizing both the committed
        # head fact and the already expired next fact.
        assert await interrupted._stage_cycle()
        record = interrupted._fact_store.select_for_publication(
            desired.load_active(),
            expected_snapshot_revision=2,
        )
        assert record is not None
        interrupted._fact_store.mark_superseded(
            record.pipeline_item_id,
            revision_sha256=record.revision_sha256,
            reason="fact_revision_expired",
        )
        await interrupted._output.async_stop()
        await interrupted._raw.async_stop()

    asyncio.run(leave_live_failure_shape())
    now[0] = NOW + 301.0

    recovered = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        clock=lambda: now[0],
        **runtime_paths,
    )
    started = asyncio.run(recovered.async_start())

    assert started["state"] == "running"
    assert started["last_error_code"] is None
    assert started["snapshot_revision"] == 3
    assert [decode_control_envelope(item)["revision"] for item in published] == [
        1,
        2,
        3,
    ]
    diagnostics = recovered._fact_store.diagnostics()
    assert diagnostics["snapshot_committed"] == 1
    assert diagnostics["superseded"] == 2


def test_worker_only_runtime_restages_when_target_authority_changes_after_commit(
    tmp_path,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    initial_authority = desired.load_active().state_sha256
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        original_commit = runtime._snapshot_store.commit
        authority_replaced = False

        def replace_authority_after_commit(*args, **kwargs):
            nonlocal authority_replaced
            result = original_commit(*args, **kwargs)
            if not authority_replaced:
                authority_replaced = True
                _apply_next_desired_state(desired, initial_authority)
            return result

        runtime._snapshot_store.commit = replace_authority_after_commit
        return await runtime.async_tick()

    ticked = asyncio.run(scenario())

    assert len(published) == 2
    assert [
        decode_control_envelope(payload)["revision"] for payload in published
    ] == [1, 3]
    assert ticked["snapshot_revision"] == 3
    assert ticked["snapshot_transport_state"] == "published"
    assert ticked["state"] == "running"
    assert ticked["last_error_code"] is None
    assert ticked["counts"]["cycles_staged"] == 3
    assert ticked["counts"]["snapshots_published"] == 1
    assert runtime._raw.tick_calls == 1
    final_snapshot = decode_control_envelope(published[-1])["snapshot"]
    assert final_snapshot["desired_state_sha256"] == (
        desired.load_active().state_sha256
    )
    assert len(final_snapshot["providers"]["items"]) == SOURCE_COUNT

    selected = object.__new__(service.WorkerService)
    selected._worker_first_runtime_mode = "worker_only"
    selected._worker_first_allowed_workload_ids = ()
    assert selected._worker_first_start_is_ready(ticked, operation="tick")
    superseded = deepcopy(ticked)
    superseded["snapshot_transport_state"] = "superseded"
    assert not selected._worker_first_start_is_ready(
        superseded,
        operation="tick",
    )


def test_worker_only_runtime_settles_start_authority_race(tmp_path, monkeypatch):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    initial_authority = desired.load_active().state_sha256
    original_commit = OperationalSnapshotStore.commit
    authority_replaced = False

    def replace_authority_after_commit(snapshot_store, *args, **kwargs):
        nonlocal authority_replaced
        result = original_commit(snapshot_store, *args, **kwargs)
        if not authority_replaced:
            authority_replaced = True
            _apply_next_desired_state(desired, initial_authority)
        return result

    monkeypatch.setattr(
        OperationalSnapshotStore,
        "commit",
        replace_authority_after_commit,
    )
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    started = asyncio.run(runtime.async_start())

    assert [
        decode_control_envelope(payload)["revision"] for payload in published
    ] == [2]
    assert runtime._raw.tick_calls == 0
    assert started["snapshot_revision"] == 2
    assert started["snapshot_transport_state"] == "published"
    assert started["state"] == "running"
    final_snapshot = decode_control_envelope(published[-1])["snapshot"]
    assert len(final_snapshot["providers"]["items"]) == SOURCE_COUNT

    selected = object.__new__(service.WorkerService)
    selected._worker_first_runtime_mode = "worker_only"
    selected._worker_first_allowed_workload_ids = ()
    assert selected._worker_first_start_is_ready(started, operation="start")


def test_worker_only_runtime_settles_multiple_life360_authority_races(
    tmp_path,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    initial_authority = desired.load_active().state_sha256
    raw = _FakeRawRuntime(desired)
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        original_commit = runtime._snapshot_store.commit

        def replace_authority_after_each_commit(*args, **kwargs):
            result = original_commit(*args, **kwargs)
            revision = result["revision"]
            if revision == 2:
                _apply_next_desired_state(desired, initial_authority)
            elif revision == 3:
                current = desired.load_active()
                _apply_next_desired_state(
                    desired,
                    current.state_sha256,
                    generation=3,
                    latitude=50.82,
                )
            return result

        runtime._snapshot_store.commit = replace_authority_after_each_commit
        await runtime.async_tick()

    asyncio.run(scenario())

    diagnostics = runtime.diagnostics()
    assert raw.tick_calls == 1
    assert [
        decode_control_envelope(payload)["revision"] for payload in published
    ] == [1, 4]
    assert diagnostics["state"] == "running"
    assert diagnostics["last_error_code"] is None
    assert diagnostics["counts"]["failures"] == 0
    assert diagnostics["snapshot_transport_state"] == "published"
    final_snapshot = decode_control_envelope(published[-1])["snapshot"]
    assert len(final_snapshot["providers"]["items"]) == SOURCE_COUNT
    assert any(
        item["provider_id"] == "radar.dwd.radolan_rv"
        and item["freshness"] == "current"
        for item in final_snapshot["providers"]["items"]
    )


def test_snapshot_settle_restages_fact_for_current_authority():
    superseded = {
        "state": "degraded",
        "started": True,
        "closed": False,
        "in_flight": False,
        "last_error_code": "publication_incomplete",
        "publication": {
            "dirty": False,
            "last_error_code": None,
            "snapshot_revision": 7,
            "transport_state": "superseded",
        },
    }
    ready = {
        "state": "running",
        "started": True,
        "closed": False,
        "in_flight": False,
        "last_error_code": None,
        "publication": {
            "dirty": False,
            "last_error_code": None,
            "snapshot_revision": 8,
            "transport_state": "published",
        },
    }
    calls = {"stage": 0, "publish": 0, "tick": 0}

    class Output:
        async def async_publish_ready_fact(self):
            calls["publish"] += 1
            return ready

        async def async_tick(self):
            calls["tick"] += 1
            raise AssertionError("current authority must be restaged first")

    runtime = object.__new__(WorkerOnlyServiceRuntime)
    runtime._output = Output()

    async def stage_cycle():
        calls["stage"] += 1
        return True

    runtime._stage_cycle = stage_cycle
    result = asyncio.run(runtime._settle_snapshot_output(superseded))

    assert result is ready
    assert calls == {"stage": 1, "publish": 1, "tick": 0}


def test_worker_only_runtime_fails_closed_after_bounded_authority_churn(
    tmp_path,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    raw = _FakeRawRuntime(desired)
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        original_commit = runtime._snapshot_store.commit

        def replace_authority_after_every_commit(*args, **kwargs):
            result = original_commit(*args, **kwargs)
            revision = result["revision"]
            current = desired.load_active()
            _apply_next_desired_state(
                desired,
                current.state_sha256,
                generation=revision,
                latitude=50.81 + revision / 10_000,
            )
            return result

        runtime._snapshot_store.commit = replace_authority_after_every_commit
        await runtime.async_tick()

    with pytest.raises(
        WorkerOnlyServiceRuntimeError,
        match="worker_only_snapshot_output_not_ready",
    ):
        asyncio.run(scenario())

    diagnostics = runtime.diagnostics()
    assert raw.tick_calls == 1
    assert len(published) == 1
    assert diagnostics["state"] == "degraded"
    assert diagnostics["last_error_code"] == "worker_only_tick_failed"
    assert diagnostics["counts"]["failures"] == 1
    assert diagnostics["snapshot_transport_state"] == "superseded"
    assert runtime._snapshot_store.head()["revision"] == (
        1 + MAX_SNAPSHOT_SETTLE_TICKS + 1
    )


def test_worker_only_runtime_publishes_positive_rain_from_worker_processor(
    tmp_path,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    processor_calls = []
    tracker_calls = []

    # This test exercises only the typed hand-off between the quantitative
    # processor and the tracker.  Provider-field integrity is covered by the
    # quantitative decoder and cell-tracking suites, so a deliberately opaque
    # instance keeps this runtime test independent of one provider fixture.
    verified_source = object.__new__(VerifiedRadarFieldSource)

    class FakeCellTracker:
        def process_source(self, **kwargs):
            tracker_calls.append(kwargs)
            return SimpleNamespace(duplicate_regions=1, reset_regions=1)

        @staticmethod
        def diagnostics():
            return {
                "active_tracks": 1,
                "tracked_tracks": 1,
                "motion_ready_tracks": 1,
                "region_states": 1,
                "restart_persistent": True,
                "store_error": None,
            }

        @staticmethod
        def shadow_arrivals():
            return (WorkerOnlyShadowArrivalCandidate(
                candidate_id="shadow-arrival:runtime-test",
                workload_id="radar.dwd.radolan_rv",
                region_lineage_id="tracking-region:runtime-test",
                target_id="home",
                track_id="track:runtime-test",
                detection_id="detection:runtime-test",
                source_candidate_id="candidate:runtime-test",
                source_candidate_content_sha256=_sha("source-candidate"),
                analysis_grid_id="grid:runtime-test",
                reference_valid_at=NOW - 60,
                horizon_seconds=SHADOW_ARRIVAL_HORIZON_SECONDS,
                entry_seconds=12.0 * 60.0,
                exit_seconds=22.0 * 60.0,
                vx_m_s=7.0,
                vy_m_s=1.0,
                track_sample_count=3,
                footprint_pixel_count=20,
                area_km2=20.0,
                mean_rain_rate_mm_h=3.0,
                maximum_rain_rate_mm_h=6.0,
                target_location_accuracy_m=10.0,
                low_confidence_fragment=False,
                passage_state="future_intersection",
                admission_status=ADMISSION_STATUS,
            ),)

    def processor(**kwargs):
        processor_calls.append(kwargs)
        return WorkerOnlyRadarProcessingResult(
            observations=(PositiveRadarTargetObservation(
                source_id="radar.dwd.radolan_rv",
                target_id="home",
                rain_rate_mm_h=3.75,
                product_valid_at=NOW - 60,
                received_at=NOW - 30,
                source_receipt_sha256=_sha("canonical-source-receipt"),
                certificate_sha256=_sha("dwd-grid-certificate"),
                process_receipt_sha256=_sha("target-sampling-process"),
            ),),
            verified_source=verified_source,
        )

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=processor,
        cell_tracker=FakeCellTracker(),
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        ticked = await runtime.async_tick()
        await runtime.async_stop()
        return ticked

    ticked = asyncio.run(scenario())

    assert len(processor_calls) == 1
    assert processor_calls[0]["capture_id"] == "capture:dwd:test"
    assert len(tracker_calls) == 1
    assert tracker_calls[0]["workload_id"] == "radar.dwd.radolan_rv"
    assert tracker_calls[0]["source"] is verified_source
    assert len(published) == 2
    snapshot = decode_control_envelope(published[-1])["snapshot"]
    current = snapshot["targets"]["items"][0]["current_precipitation"]
    assert current["freshness"] == "current"
    assert current["product_valid_at"] == NOW - 60
    assert current["provider_ids"] == ["radar.dwd.radolan_rv"]
    assert current["rain_rate_mm_h"] == 3.75
    assert current["reason_codes"] == ["positive_radar_candidates_fused"]
    assert current["state"] == "raining"
    threat = snapshot["targets"]["items"][0]["next_threat"]
    assert threat["state"] == "available"
    assert threat["eta_minutes"] == 12.0
    assert threat["passage_status"] == "possible"
    assert snapshot["targets"]["items"][0]["status_text"] == (
        "Regen: 3.8 mm/u"
    )
    assert ticked["counts"]["quantitative_successes"] == 1
    assert ticked["counts"]["quantitative_failures"] == 0
    assert ticked["counts"]["tracking_successes"] == 1
    assert ticked["counts"]["tracking_duplicates"] == 1
    assert ticked["counts"]["tracking_resets"] == 1


def test_motion_enrichment_failure_keeps_certified_radar_fact(
    tmp_path,
    monkeypatch,
):
    """A stale optional motion candidate must not erase valid radar truth."""

    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")

    class FakeCellTracker:
        @staticmethod
        def diagnostics():
            return {
                "active_tracks": 1,
                "tracked_tracks": 1,
                "motion_ready_tracks": 1,
                "region_states": 1,
                "restart_persistent": True,
                "store_error": None,
            }

        @staticmethod
        def shadow_arrivals():
            return (object(),)

    def processor(**_kwargs):
        return WorkerOnlyRadarProcessingResult(
            observations=(PositiveRadarTargetObservation(
                source_id="radar.dwd.radolan_rv",
                target_id="home",
                rain_rate_mm_h=2.5,
                product_valid_at=NOW - 60,
                received_at=NOW - 30,
                source_receipt_sha256=_sha("motion-fallback-source"),
                certificate_sha256=_sha("motion-fallback-certificate"),
                process_receipt_sha256=_sha("motion-fallback-process"),
            ),),
            verified_source=None,
        )

    def reject_motion(*_args, **_kwargs):
        raise runtime_module.WorkerOnlyMotionSnapshotError(
            "motion_arrival_target_not_available"
        )

    monkeypatch.setattr(
        runtime_module,
        "build_radar_motion_nowcast_revision",
        reject_motion,
    )
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=processor,
        cell_tracker=FakeCellTracker(),
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    diagnostics = asyncio.run(scenario())
    snapshot = decode_control_envelope(published[-1])["snapshot"]
    current = snapshot["targets"]["items"][0]["current_precipitation"]

    assert current["state"] == "raining"
    assert current["rain_rate_mm_h"] == 2.5
    assert current["provider_ids"] == ["radar.dwd.radolan_rv"]
    assert diagnostics["quantitative_errors"] == {}
    assert diagnostics["counts"]["quantitative_failures"] == 0
    assert diagnostics["counts"]["motion_enrichment_failures"] == 1


def test_liveness_only_desired_state_refresh_reuses_quantitative_fusion(
    tmp_path,
):
    """A fresh tracker timestamp must not trigger the heavy radar replay."""

    published = []
    desired = _desired_store(
        tmp_path / "desired.sqlite3",
        target_kind="person",
    )
    processor_calls = []

    def processor(**kwargs):
        processor_calls.append(kwargs)
        return (PositiveRadarTargetObservation(
            source_id="radar.dwd.radolan_rv",
            target_id="home",
            rain_rate_mm_h=3.75,
            product_valid_at=NOW - 60,
            received_at=NOW - 30,
            source_receipt_sha256=_sha("lease-source-receipt"),
            certificate_sha256=_sha("lease-grid-certificate"),
            process_receipt_sha256=_sha("lease-target-sampling"),
        ),)

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=processor,
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        await runtime.async_tick()
        first = desired.load_active()
        _apply_next_desired_state(
            desired,
            first.state_sha256,
            latitude=first.targets[0].latitude,
            longitude=first.targets[0].longitude,
            target_kind="person",
        )
        lease_ticked = await runtime.async_tick()
        lease_snapshot = decode_control_envelope(published[-1])["snapshot"]
        lease_processor_calls = len(processor_calls)
        second = desired.load_active()
        _apply_next_desired_state(
            desired,
            second.state_sha256,
            generation=3,
            latitude=second.targets[0].latitude + 0.02,
            longitude=second.targets[0].longitude,
            target_kind="person",
        )
        moved_ticked = await runtime.async_tick()
        await runtime.async_stop()
        return (
            lease_ticked,
            lease_snapshot,
            lease_processor_calls,
            moved_ticked,
        )

    ticked, rebound, lease_processor_calls, moved_ticked = asyncio.run(
        scenario()
    )

    assert lease_processor_calls == 1
    assert rebound["targets"]["items"][0]["current_precipitation"]["state"] == (
        "raining"
    )
    assert len(processor_calls) == 2


def test_liveness_authority_is_published_before_slow_provider_tick(tmp_path):
    """A lease refresh must not wait behind one blocking provider batch."""

    published = []
    desired = _desired_store(
        tmp_path / "desired.sqlite3",
        target_kind="person",
    )
    raw = _SlowRawRuntime(desired)
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=lambda payload: published.append(
            (payload, raw.tick_completed)
        ),
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        await runtime.async_tick()
        previous_state = decode_control_envelope(published[-1][0])["snapshot"][
            "targets"
        ]["items"][0]["current_precipitation"]["state"]
        first = desired.load_active()
        _apply_next_desired_state(
            desired,
            first.state_sha256,
            latitude=first.targets[0].latitude,
            longitude=first.targets[0].longitude,
            target_kind="person",
        )
        before = len(published)
        raw.tick_completed = False
        tick = asyncio.create_task(runtime.async_tick())
        await asyncio.sleep(0.005)
        early = tuple(published[before:])
        await tick
        await runtime.async_stop()
        return early, previous_state

    early, previous_state = asyncio.run(scenario())

    assert len(early) == 1
    payload, provider_completed = early[0]
    assert provider_completed is False
    rebound = decode_control_envelope(payload)["snapshot"]
    assert rebound["desired_state_sha256"] == desired.load_active().state_sha256
    assert rebound["targets"]["items"][0]["current_precipitation"][
        "state"
    ] == previous_state


def test_radar_fusion_inconsistency_withholds_enrichment_without_worker_exit(
    tmp_path,
    monkeypatch,
    caplog,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")

    def processor(**_kwargs):
        return (PositiveRadarTargetObservation(
            source_id="radar.dwd.radolan_rv",
            target_id="home",
            rain_rate_mm_h=3.75,
            product_valid_at=NOW - 60,
            received_at=NOW - 30,
            source_receipt_sha256=_sha("withheld-source-receipt"),
            certificate_sha256=_sha("withheld-grid-certificate"),
            process_receipt_sha256=_sha("withheld-target-sampling"),
        ),)

    def reject_rich_revision(*_args, **_kwargs):
        from storm_tracker_worker.multi_source_radar_snapshot import (
            MultiSourceRadarSnapshotError,
        )

        raise MultiSourceRadarSnapshotError("radarproduct ligt in de toekomst")

    monkeypatch.setattr(
        runtime_module,
        "build_positive_radar_fact_revision",
        reject_rich_revision,
    )
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=processor,
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        ticked = await runtime.async_tick()
        await runtime.async_stop()
        return ticked

    ticked = asyncio.run(scenario())

    assert ticked["state"] == "running"
    assert ticked["counts"]["failures"] == 0
    assert ticked["counts"]["quantitative_failures"] == 1
    assert ticked["quantitative_errors"] == {
        "radar.dwd.radolan_rv": "quantitative_processing_failed"
    }
    assert "reason=radar_product_future" in caplog.text
    snapshot = decode_control_envelope(published[-1])["snapshot"]
    assert snapshot["targets"]["items"][0]["current_precipitation"][
        "state"
    ] == "unknown"


def test_worker_only_runtime_publishes_qualified_zero_without_fake_dry(
    tmp_path,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")

    def processor(**_kwargs):
        return (WithheldDryRadarTargetObservation(
            source_id="radar.dwd.radolan_rv",
            target_id="home",
            rain_rate_mm_h=0.0,
            product_valid_at=NOW - 60,
            received_at=NOW - 30,
            source_receipt_sha256=_sha("canonical-zero-source-receipt"),
            certificate_sha256=_sha("dwd-zero-grid-certificate"),
            process_receipt_sha256=_sha("zero-target-sampling-process"),
        ),)

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=processor,
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        ticked = await runtime.async_tick()
        await runtime.async_stop()
        return ticked

    ticked = asyncio.run(scenario())
    snapshot = decode_control_envelope(published[-1])["snapshot"]
    target = snapshot["targets"]["items"][0]
    current = target["current_precipitation"]

    assert current["state"] == "unknown"
    assert current["freshness"] == "current"
    assert current["rain_rate_mm_h"] is None
    assert current["provider_ids"] == ["radar.dwd.radolan_rv"]
    assert current["reason_codes"] == [ZERO_OBSERVATION_REASON]
    assert "droogte niet onafhankelijk bevestigd" in target["status_text"]
    assert ticked["counts"]["quantitative_successes"] == 1
    assert ticked["counts"]["quantitative_failures"] == 0


def test_worker_only_runtime_publishes_qualified_undetect_without_fake_dry(
    tmp_path,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")

    def processor(**_kwargs):
        return (WithheldUndetectRadarTargetObservation(
            source_id="radar.dwd.radolan_rv",
            target_id="home",
            rain_rate_mm_h=None,
            product_valid_at=NOW - 60,
            received_at=NOW - 30,
            source_receipt_sha256=_sha("undetect-source-receipt"),
            certificate_sha256=_sha("undetect-grid-certificate"),
            process_receipt_sha256=_sha("undetect-target-sampling"),
        ),)

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=processor,
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        ticked = await runtime.async_tick()
        await runtime.async_stop()
        return ticked

    ticked = asyncio.run(scenario())
    snapshot = decode_control_envelope(published[-1])["snapshot"]
    target = snapshot["targets"]["items"][0]
    current = target["current_precipitation"]

    assert current["state"] == "unknown"
    assert current["freshness"] == "current"
    assert current["rain_rate_mm_h"] is None
    assert current["provider_ids"] == ["radar.dwd.radolan_rv"]
    assert current["reason_codes"] == [UNDETECT_OBSERVATION_REASON]
    assert "onder detectiedrempel" in target["status_text"]
    assert ticked["counts"]["quantitative_successes"] == 1
    assert ticked["counts"]["quantitative_failures"] == 0


def test_worker_only_runtime_withholds_positive_rain_inside_publication_margin(
    tmp_path,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    raw = _FakeRawRuntime(desired)
    # DWD remains acquisition-current for 27 seconds, but the snapshot needs
    # 30 seconds of publication lead. This exact boundary crashed the live
    # 1.17.22 worker when the fictional tracker was placed in Germany.
    raw.store.capture = replace(
        raw.store.capture,
        product_timestamp=NOW - 873,
    )
    processor_calls = []

    def processor(**kwargs):
        processor_calls.append(kwargs)
        return (PositiveRadarTargetObservation(
            source_id="radar.dwd.radolan_rv",
            target_id="home",
            rain_rate_mm_h=3.75,
            product_valid_at=NOW - 873,
            received_at=NOW - 30,
            source_receipt_sha256=_sha("near-deadline-source-receipt"),
            certificate_sha256=_sha("near-deadline-grid-certificate"),
            process_receipt_sha256=_sha("near-deadline-target-sampling"),
        ),)

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=raw,
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=processor,
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        ticked = await runtime.async_tick()
        await runtime.async_stop()
        return ticked

    ticked = asyncio.run(scenario())

    assert len(processor_calls) == 1
    assert len(published) == 2
    snapshot = decode_control_envelope(published[-1])["snapshot"]
    dwd = next(
        item for item in snapshot["providers"]["items"]
        if item["provider_id"] == "radar.dwd.radolan_rv"
    )
    assert dwd["state"] == "stale"
    assert dwd["reason_codes"] == [
        "freshness_deadline_not_publication_safe"
    ]
    assert snapshot["targets"]["items"][0]["current_precipitation"][
        "state"
    ] == "unknown"
    assert ticked["state"] == "running"
    assert ticked["counts"]["failures"] == 0
    assert ticked["counts"]["quantitative_successes"] == 1


def test_worker_only_runtime_publishes_current_fallback_before_reprojection(
    tmp_path,
):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    processor_calls = []

    def processor(**kwargs):
        processor_calls.append(kwargs["desired_state"].state_sha256)
        return (PositiveRadarTargetObservation(
            source_id="radar.dwd.radolan_rv",
            target_id="home",
            rain_rate_mm_h=3.75,
            product_valid_at=NOW - 60,
            received_at=NOW - 30,
            source_receipt_sha256=_sha("canonical-source-receipt"),
            certificate_sha256=_sha("dwd-grid-certificate"),
            process_receipt_sha256=_sha(
                "target-sampling-process-" + str(len(processor_calls))
            ),
        ),)

    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=processor,
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        await runtime.async_tick()
        previous = desired.load_active().state_sha256
        _apply_next_desired_state(
            desired,
            previous,
            latitude=50.8,
            longitude=4.2,
        )
        return await runtime.async_tick()

    ticked = asyncio.run(scenario())

    assert len(processor_calls) == 2
    assert len(published) == 4
    fallback = decode_control_envelope(published[-2])["snapshot"]
    rebound = decode_control_envelope(published[-1])["snapshot"]
    current_sha = desired.load_active().state_sha256
    assert fallback["desired_state_sha256"] == current_sha
    assert len(fallback["regions"]["items"]) == 1
    assert fallback["targets"]["items"][0]["current_precipitation"][
        "state"
    ] == "unknown"
    assert rebound["desired_state_sha256"] == current_sha
    assert rebound["targets"]["items"][0]["current_precipitation"][
        "state"
    ] == "raining"
    assert ticked["state"] == "running"
    assert ticked["counts"]["failures"] == 0


def test_authority_fallback_does_not_wait_for_ground_replay(tmp_path):
    published = []
    desired = _desired_store(tmp_path / "desired.sqlite3")
    runtime = WorkerOnlyServiceRuntime(
        raw_runtime=_FakeRawRuntime(desired),
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        operational_fact_path=tmp_path / "facts.sqlite3",
        operational_snapshot_path=tmp_path / "snapshots.sqlite3",
        snapshot_outbox_path=tmp_path / "outbox.sqlite3",
        radar_artifact_root=tmp_path / "quantitative",
        radar_evidence_path=tmp_path / "quantitative.sqlite3",
        radar_processor=lambda **_kwargs: (),
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        await runtime.async_tick()
        previous = desired.load_active().state_sha256
        _apply_next_desired_state(
            desired,
            previous,
            latitude=50.8,
            longitude=4.2,
        )
        current_sha = desired.load_active().state_sha256
        replay_entered = asyncio.Event()
        release_replay = asyncio.Event()

        async def blocked_ground_refresh():
            replay_entered.set()
            await release_replay.wait()

        runtime._ground_processor = object()
        runtime._ground_evidence_path = tmp_path / "ground.sqlite3"
        runtime._refresh_ground_captures = blocked_ground_refresh
        before = len(published)
        tick_task = asyncio.create_task(runtime.async_tick())
        await asyncio.wait_for(replay_entered.wait(), timeout=1.0)
        assert len(published) == before + 1
        fallback = decode_control_envelope(published[-1])["snapshot"]
        assert fallback["desired_state_sha256"] == current_sha
        assert fallback["targets"]["items"][0]["current_precipitation"][
            "state"
        ] == "unknown"
        release_replay.set()
        await tick_task

    asyncio.run(scenario())
