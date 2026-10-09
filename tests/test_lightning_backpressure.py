from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "custom_components"
    / "storm_tracker_v3"
    / "engine"
    / "lightning_backpressure.py"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("lightning_backpressure", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Handle:
    def __init__(self, callback):
        self.callback = callback
        self.cancelled = False

    def cancel(self):
        self.cancelled = True


class _Scheduler:
    def __init__(self):
        self.calls = []

    def call_later(self, delay, callback):
        handle = _Handle(callback)
        self.calls.append((delay, handle))
        return handle


def test_strikes_are_coalesced_into_separate_sensor_and_map_updates():
    module = _load_module()
    scheduler = _Scheduler()
    sensor_updates = []
    map_updates = []
    log_batches = []
    coalescer = module.LightningUpdateCoalescer(
        scheduler.call_later,
        sensor_updates.append,
        map_updates.append,
        log_batches.append,
    )

    for index in range(25):
        coalescer.submit({
            "lat": 51.0,
            "lon": 4.0,
            "timestamp": index,
            "source": "blitzortung",
        }, log=True)

    assert [delay for delay, _ in scheduler.calls] == [2.0, 10.0]
    scheduler.calls[0][1].callback()
    assert sensor_updates == [{
        "lat": 51.0,
        "lon": 4.0,
        "timestamp": 24,
        "source": "blitzortung",
        "batch_count": 25,
        "update_interval_s": 5.0,
    }]
    assert len(log_batches) == 1
    assert len(log_batches[0]) == 1
    assert log_batches[0][0]["strike_count"] == 25
    assert map_updates == []

    scheduler.calls[1][1].callback()
    assert map_updates[0]["batch_count"] == 25
    assert map_updates[0]["update_interval_s"] == 10.0


def test_high_pressure_slows_next_sensor_window_and_close_cancels_timers():
    module = _load_module()
    scheduler = _Scheduler()
    coalescer = module.LightningUpdateCoalescer(
        scheduler.call_later, lambda payload: None, lambda payload: None,
        lambda rows: None,
    )

    for index in range(51):
        coalescer.submit({"timestamp": index})
    scheduler.calls[0][1].callback()
    coalescer.submit({"timestamp": 52})

    assert scheduler.calls[2][0] == 10.0
    pending_handles = [handle for _, handle in scheduler.calls[1:]]
    coalescer.close()
    assert all(handle.cancelled for handle in pending_handles)


def test_raw_strikes_are_aggregated_per_minute_and_grid_cell():
    module = _load_module()
    rows = module.aggregate_lightning_rows([
        {
            "lat": 51.001,
            "lon": 4.001,
            "timestamp": 120.0,
            "source": "blitzortung",
            "engine_ids": ["region-1"],
        },
        {
            "lat": 51.011,
            "lon": 4.011,
            "timestamp": 135.0,
            "source": "blitzortung",
            "engine_ids": ["region-1", "region-2"],
        },
        {
            "lat": 51.001,
            "lon": 4.001,
            "timestamp": 181.0,
            "source": "blitzortung",
            "engine_ids": ["region-1"],
        },
    ])

    assert len(rows) == 2
    first = next(row for row in rows if row["nominal_minute"] == 2)
    assert first["strike_count"] == 2
    assert first["latitude_sum"] == 102.012
    assert first["first_timestamp"] == 120.0
    assert first["last_timestamp"] == 135.0
    assert first["region_ids"] == ["region-1", "region-2"]
