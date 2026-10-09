from pathlib import Path

import pytest

from storm_tracker_worker.virtual_probe import run_virtual_probes


def _probe(expected_at=1_500.0):
    return {
        "probe_id": "p1",
        "storm_id": "storm-a",
        "region_id": "r1",
        "station_id": "s1",
        "latitude": 51.0,
        "longitude": 4.0,
        "created_at": 1_000.0,
        "expected_at": expected_at,
        "eta_minutes": 8.3,
        "passage": "raak",
        "pass_distance_km": 0.0,
        "motion_confidence": "Matig",
        "radar_source": "rainviewer",
    }


def _payload(observed_at=1_600.0):
    return {
        "observed_at": observed_at,
        "active_probes": [_probe()],
        "history": {},
        "states_by_region": {},
        "observations_by_region": {
            "r1": [{
                "obs_type": "rain",
                "lat": 51.0,
                "lon": 4.0,
                "timestamp": 1_590.0,
                "station_id": "s1",
                "rain_capable": True,
                "rain_mm": 0.4,
                "rain_60min": 1.2,
                "source": "netatmo",
            }]
        },
        "radar_sources_by_region": {},
    }


def test_worker_virtual_probe_owns_confirmed_rain_evaluation():
    result = run_virtual_probes(_payload())

    assert result["method"] == "virtual_probe_operational_v1"
    assert result["samples"][0]["own_status"] == "regen_bevestigd"
    assert result["diagnostics"]["active_count"] == 0
    assert result["diagnostics"]["confirmed_count"] == 1


def test_worker_virtual_probe_keeps_pending_probe_active():
    payload = _payload(observed_at=1_000.0)
    payload["active_probes"] = [_probe(expected_at=2_000.0)]
    payload["observations_by_region"] = {}

    result = run_virtual_probes(payload)

    assert result["samples"] == []
    assert result["diagnostics"]["active_count"] == 1


def test_worker_accepts_dense_20km_netatmo_query():
    payload = _payload(observed_at=1_000.0)
    payload["active_probes"] = []
    payload["observations_by_region"]["r1"] *= 750

    result = run_virtual_probes(payload)

    assert result["method"] == "virtual_probe_operational_v1"


def test_worker_accepts_rolling_restore_above_operational_probe_target():
    payload = _payload(observed_at=1_000.0)
    payload["observations_by_region"] = {}
    payload["active_probes"] = [
        {
            **_probe(expected_at=2_000.0 + index),
            "probe_id": f"restored-{index}",
            "station_id": f"station-{index}",
        }
        for index in range(9)
    ]

    result = run_virtual_probes(payload)

    assert result["method"] == "virtual_probe_operational_v1"
    assert result["diagnostics"]["active_count"] == 9
    assert len(result["diagnostics"]["probes"]) == 9
    assert {
        probe["probe_id"] for probe in result["diagnostics"]["probes"]
    } == {f"restored-{index}" for index in range(9)}
    assert result["processing_counts"] == {
        "regions": 0,
        "observation_regions": 0,
        "observations": 0,
        "active_probes_received": 9,
        "active_probe_operational_target": 8,
        "active_probe_restore_ceiling": 64,
    }


def test_worker_still_rejects_unbounded_observation_payload():
    payload = _payload(observed_at=1_000.0)
    payload["observations_by_region"]["r1"] *= 2_001

    with pytest.raises(ValueError, match="te grote verkennerpayload"):
        run_virtual_probes(payload)


def test_worker_rejects_restore_above_explicit_migration_ceiling():
    payload = _payload(observed_at=1_000.0)
    payload["observations_by_region"] = {}
    payload["active_probes"] = [
        {
            **_probe(expected_at=2_000.0 + index),
            "probe_id": f"restored-{index}",
            "station_id": f"station-{index}",
        }
        for index in range(65)
    ]

    with pytest.raises(
        ValueError,
        match="actieve verkenners 65 overschrijden herstelgrens 64",
    ):
        run_virtual_probes(payload)


def test_worker_probe_core_is_exact_integration_mirror():
    root = Path(__file__).resolve().parents[1]
    assert (
        root / "storm_tracker_worker/probe_core.py"
    ).read_bytes() == (
        root / "custom_components/storm_tracker_v3/engine/virtual_probe.py"
    ).read_bytes()
