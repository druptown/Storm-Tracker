from __future__ import annotations

import threading
import time

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
