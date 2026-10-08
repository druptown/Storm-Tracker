from __future__ import annotations

import threading
import time
from dataclasses import replace

import numpy as np

from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_provider_adapters import ThinRadarField
from storm_tracker_worker.thin_provider_runtime import ProviderResult
from storm_tracker_worker.thin_region_planner import (
    ThinAcquisitionRegion,
    ThinRegionPlan,
)
from storm_tracker_worker.thin_regional_radar_runtime import (
    ThinRegionalRadarFusionCoordinator,
)


def _plan() -> ThinRegionPlan:
    target = Target(
        "home", "Home", "home", True, 0.0, 0.0,
        1_000.0, 5.0, 2.0, "BE",
    )
    region = ThinAcquisitionRegion(
        region_id="region:home",
        kind="home",
        center_latitude=0.0,
        center_longitude=0.0,
        observation_radius_km=2.0,
        targets=(target,),
        stale_target_ids=(),
    )
    return ThinRegionPlan(
        regions=(region,),
        targets=(target,),
        target_region_ids={"home": "region:home"},
        location_statuses={"home": "current"},
    )


def _providers() -> dict[str, ProviderResult]:
    field = ThinRadarField(
        "radar", "rain-rate", 1_000.0, "EPSG:3857",
        -2_000.0, 2_000.0, 1_000.0, 1_000.0,
        np.ones((4, 4), dtype=float),
        np.ones((4, 4), dtype=bool),
    )
    return {
        "radar": ProviderResult(
            "radar", True, "operational", None, (),
            1_000.0, 1_000.0, (field,),
        ),
    }


def test_poll_never_waits_for_pending_fusion():
    gate = threading.Event()
    coordinator = ThinRegionalRadarFusionCoordinator(
        max_workers=2,
        fuse=lambda *args, **kwargs: gate.wait(5.0),
    )
    coordinator.submit(_plan(), _providers(), evaluated_at=1_000.0)

    started = time.monotonic()
    assert coordinator.poll() == {}
    assert time.monotonic() - started < 0.05

    gate.set()
    coordinator.close()


def test_coordinator_uses_at_most_two_compute_threads():
    coordinator = ThinRegionalRadarFusionCoordinator(max_workers=2)

    assert coordinator.max_workers == 2

    coordinator.close()


def test_fusion_exposes_native_source_clocks_and_explicit_bit_mapping():
    from storm_tracker_worker.thin_regional_radar_fusion import fuse_regional_radar_fields
    source = _providers()["radar"]
    older = replace(source.radar_fields[0], provider_id="a", observed_at=900., identity=None)
    result = fuse_regional_radar_fields(_plan().regions[0], {
        "radar": source, "a": replace(source, provider_id="a", radar_fields=(older,))},
        evaluated_at=1000.)
    assert {f.provider_id: f.observed_at for f,l in result.native_layers} == {"a":900., "radar":1000.}
    assert result.summary.source_index == ("radar", "a")
    assert result.summary.source_product_times == {"radar":1000., "a":900.}


def test_coordinator_fingerprint_changes_on_corrected_content_and_region_revision():
    from storm_tracker_worker.thin_regional_radar_runtime import _input_fingerprint
    plan = _plan(); providers = _providers()
    original = _input_fingerprint(plan.regions[0], providers)
    field = providers["radar"].radar_fields[0]
    changed = replace(field, rain_rate_mm_h=np.full((4,4), .2), identity=None)
    assert _input_fingerprint(plan.regions[0], {"radar":replace(providers["radar"], radar_fields=(changed,))}) != original
    revision = replace(plan.regions[0], region_revision_id="moved")
    assert _input_fingerprint(revision, providers) != original


def test_identical_completed_fingerprint_is_not_scheduled_again():
    calls: list[str] = []
    completed = threading.Event()

    def fuse(region, provider_results, *, evaluated_at):
        calls.append(region.region_id)
        completed.set()
        return object()

    coordinator = ThinRegionalRadarFusionCoordinator(max_workers=2, fuse=fuse)
    coordinator.submit(_plan(), _providers(), evaluated_at=1_000.0)
    assert completed.wait(1.0)
    coordinator.poll()

    coordinator.submit(_plan(), _providers(), evaluated_at=1_001.0)
    time.sleep(0.02)

    assert calls == ["region:home"]
    coordinator.close()


def test_source_engine_reuses_products_and_is_reset_for_region_revision():
    from storm_tracker_worker.thin_source_radar_forecast import SourceRadarForecastEngine
    engines = []
    calls = []
    def engine_factory():
        engine = SourceRadarForecastEngine(estimate=lambda a,b: calls.append(
            (a.field.observed_at,b.field.observed_at)) or ())
        engines.append(engine)
        return engine
    coordinator = ThinRegionalRadarFusionCoordinator(source_engine_factory=engine_factory)
    plan = _plan()
    def complete(providers, at):
        coordinator.submit(plan, providers, evaluated_at=at)
        # Bounded synchronization of the existing job, not a production poll.
        coordinator._jobs["region:home"].future.result(timeout=3.)
        return coordinator.poll()["region:home"]
    first = complete(_providers(),1000.)
    assert first.source_motions[0].reason_code == "source_pair_missing"
    source = _providers()["radar"]
    newer = replace(source.radar_fields[0], observed_at=1300., identity=None)
    providers = {"radar":replace(source, radar_fields=(newer,))}
    second = complete(providers,1300.)
    assert second.source_motions[0].delta_seconds == 300.
    assert calls == [(1000.,1300.)]
    coordinator.submit(plan,providers,evaluated_at=1310.)
    assert coordinator.pending_region_ids() == frozenset()
    plan = replace(plan, regions=(replace(plan.regions[0],region_revision_id="moved"),))
    moved = complete(providers,1320.)
    assert moved.source_motions[0].reason_code == "source_pair_missing"
    assert len(engines) == 2
    coordinator.close()


def test_completed_old_region_revision_is_not_published_after_move():
    gate = threading.Event()
    def fuse(*args, **kwargs):
        gate.wait(2.)
        return object()
    coordinator = ThinRegionalRadarFusionCoordinator(fuse=fuse)
    plan = _plan()
    coordinator.submit(plan, _providers(), evaluated_at=1000.)
    moved = replace(plan,regions=(replace(plan.regions[0],region_revision_id="moved"),))
    coordinator.submit(moved, _providers(), evaluated_at=1001.)
    job = coordinator._jobs["region:home"]
    gate.set(); job.future.result(timeout=2.)
    assert coordinator.poll() == {}
    coordinator.close()
