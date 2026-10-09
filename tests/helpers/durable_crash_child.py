"""Abrupt-process helper for the durable SQLite fault matrix."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys
import types


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def main() -> None:
    database_path = Path(sys.argv[1])
    mode = sys.argv[2]
    crash_checkpoint = sys.argv[3]
    root = Path(__file__).resolve().parents[2]
    package = types.ModuleType("stv3_crash")
    package.__path__ = []
    engine = types.ModuleType("stv3_crash.engine")
    engine.__path__ = []
    sys.modules["stv3_crash"] = package
    sys.modules["stv3_crash.engine"] = engine
    observation = _load(
        "stv3_crash.engine.observation",
        root
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "observation.py",
    )
    durable = _load(
        "stv3_crash.engine.durable_processing",
        root
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "durable_processing.py",
    )

    def fault(checkpoint: str) -> None:
        if checkpoint == crash_checkpoint:
            os._exit(73)

    store = durable.DurableProcessingStore(
        database_path,
        fault_injector=fault,
    )
    store.initialize()
    if mode == "receive":
        item = observation.Observation(
            obs_type=observation.ObservationType.RAIN,
            lat=40.6413,
            lon=-73.7781,
            timestamp=1_700_000_000.0,
            received_at=1_700_000_003.0,
            rain_mm=2.4,
            station_id="KJFK",
            rain_capable=True,
            precipitation_present=True,
            source="metar",
        )
        store.receive_observations(
            region_id="new-york",
            observations=[item],
        )
        return
    if mode == "commit":
        store.commit_staged(
            region_id="40.64,-73.78,200",
            batch_id="a" * 32,
        )
        return
    raise ValueError(f"onbekende crashmodus: {mode}")


if __name__ == "__main__":
    main()
