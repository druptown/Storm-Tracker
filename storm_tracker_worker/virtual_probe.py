"""Worker-owned planning, path control and evaluation of virtual probes."""
from __future__ import annotations

from .probe_core import MAX_ACTIVE_PROBES, VirtualProbeManager
from .tracking import _observation
from .tracking_core.engine.storm_engine import StormEngine

MAX_REGIONS = 8
# Een dichtbevolkte 20-km Netatmo-query kan meer dan 500 stations bevatten.
# Het algemene MQTT-contract begrenst de bytes al; deze limiet beschermt alleen
# de lineaire verwerking in de worker.
MAX_OBSERVATIONS = 2_000
# MAX_ACTIVE_PROBES is het normale planningsdoel, geen veilige herstelgrens.
# Tijdens een rolling upgrade of databaseherstel kan HA tijdelijk meer actieve
# verkenners aanleveren.
MAX_RESTORED_ACTIVE_PROBES = MAX_REGIONS * MAX_ACTIVE_PROBES


def _trim_active_probes(
    active_probes: list[dict],
    observed_at: float,
    max_active_probes: int,
) -> tuple[list[dict], bool]:
    if len(active_probes) <= max_active_probes:
        return active_probes, False

    def _distance_to_expected_at(probe: dict) -> float:
        expected_at = probe.get("expected_at")
        if not isinstance(expected_at, (int, float)):
            return float("inf")
        return abs(float(expected_at) - float(observed_at))

    trimmed = sorted(
        active_probes,
        key=_distance_to_expected_at,
    )[:max_active_probes]
    return trimmed, True


def _trim_observations_by_region(
    observations_by_region: dict,
    max_observations: int,
) -> tuple[dict, bool]:
    total_observations = sum(len(items) for items in observations_by_region.values())
    if total_observations <= max_observations:
        return observations_by_region, False

    ranked = []
    for region_id, items in observations_by_region.items():
        for item in items:
            observed_at = item.get("timestamp")
            if isinstance(observed_at, (int, float)):
                observed_at_ts = float(observed_at)
            else:
                observed_at_ts = float("-inf")
            ranked.append((observed_at_ts, str(region_id), item))

    ranked.sort(key=lambda entry: entry[0], reverse=True)
    selected: dict[str, list[dict]] = {}
    for _, region_id, item in ranked[:max_observations]:
        selected.setdefault(region_id, []).append(item)
    return selected, True


def run_virtual_probes(payload: dict) -> dict:
    states = payload.get("states_by_region", {})
    raw_observations = payload.get("observations_by_region", {})
    active = payload.get("active_probes", [])
    history = payload.get("history", {})
    allow_trimmed_payload = bool(payload.get("allow_trimmed_payload", False))
    observed_at_raw = payload.get("observed_at")

    if not isinstance(states, dict) or len(states) > MAX_REGIONS:
        raise ValueError("te grote verkennerpayload")
    if not isinstance(raw_observations, dict) or not isinstance(active, list):
        raise ValueError("te grote verkennerpayload")
    if not isinstance(observed_at_raw, (int, float)):
        raise ValueError("te grote verkennerpayload")

    observed_at = float(observed_at_raw)
    raw_active_count = len(active)
    raw_observation_count = sum(len(items) for items in raw_observations.values())

    if raw_observation_count > MAX_OBSERVATIONS:
        if not allow_trimmed_payload:
            raise ValueError("te grote verkennerpayload")
        raw_observations, _ = _trim_observations_by_region(
            raw_observations,
            MAX_OBSERVATIONS,
        )

    if raw_active_count > MAX_RESTORED_ACTIVE_PROBES:
        if allow_trimmed_payload:
            active, _ = _trim_active_probes(
                active,
                observed_at=observed_at,
                max_active_probes=MAX_RESTORED_ACTIVE_PROBES,
            )
        else:
            raise ValueError(
                "te grote verkennerpayload: actieve verkenners "
                f"{raw_active_count} overschrijden herstelgrens "
                f"{MAX_RESTORED_ACTIVE_PROBES}"
            )

    observation_count = sum(len(items) for items in raw_observations.values())
    storms_by_engine = {}
    for region_id, snapshots in states.items():
        engine = StormEngine()
        engine.restore_mcs_history(list(snapshots))
        storms_by_engine[str(region_id)] = engine.get_active_storms()
    observations_by_engine = {
        str(region_id): [_observation(raw) for raw in items]
        for region_id, items in raw_observations.items()
    }
    manager = VirtualProbeManager(
        restored=list(active),
        history=dict(history or {}),
    )
    samples, diagnostics = manager.update(
        storms_by_engine=storms_by_engine,
        observations_by_engine=observations_by_engine,
        radar_sources_by_engine=dict(
            payload.get("radar_sources_by_region", {})
        ),
        now=observed_at,
    )
    roaming_target = manager.choose_roaming_target(
        storms_by_engine=storms_by_engine,
        observations_by_engine=observations_by_engine,
        now=observed_at,
    )
    return {
        "method": "virtual_probe_operational_v1",
        "samples": samples,
        "diagnostics": diagnostics,
        "roaming_target": roaming_target,
        "processing_counts": {
            "regions": len(states),
            "observation_regions": len(raw_observations),
            "observations": observation_count,
            "active_probes_received": len(active),
            "active_probe_operational_target": MAX_ACTIVE_PROBES,
            "active_probe_restore_ceiling": MAX_RESTORED_ACTIVE_PROBES,
        },
    }


def virtual_probe_operational(job) -> dict:
    return run_virtual_probes(job.payload)
