"""Meteorological contracts for shadow ground-observation adapters."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from storm_tracker_worker.netatmo import _parse_observations
from storm_tracker_worker.tracking_core.engine.ground_fusion import (
    CellTrackFootprint,
    GroundConfirmationState,
    GroundFusionPolicy,
    MeasurementSemantics,
    PhysicalSiteRegistry,
    PrecipitationState,
    fuse_ground_observations,
)
from storm_tracker_worker.tracking_core.engine.ground_observation_adapters import (
    GroundAdapterIssueState,
    adapt_worker_observation,
    adapt_worker_observations,
)


OBSERVED = 1_800_000_000.0
RECEIVED = OBSERVED + 30.0


def _weather_group(
    raw: str,
    *,
    vicinity: bool = False,
) -> dict:
    return {
        "raw": raw,
        "intensity": None,
        "proximity": "vicinity" if vicinity else "at_station",
        "descriptor": None,
        "phenomena": ["RA"],
        "precipitation": True,
        "local_precipitation": not vicinity,
    }


def _metar(
    station: str = "KNYC",
    *,
    groups: list[dict] | None = None,
    flags: list[str] | None = None,
    accumulation: float | None = None,
) -> dict:
    groups = [] if groups is None else groups
    flags = [] if flags is None else flags
    return {
        "source": "metar",
        "station_id": f"metar:{station}",
        "lat": 40.75,
        "lon": -73.98,
        "timestamp": OBSERVED,
        "received_at": RECEIVED,
        "parser_version": "metar_wx_v3",
        "precipitation_present": any(
            group["local_precipitation"] for group in groups
        ),
        "quality_flags": [
            flag for flag in flags if flag != "maintenance_required"
        ],
        "raw_text": f"METAR {station} {' '.join(flags)}",
        "raw_artifact_receipt_id": f"receipt:metar:{station}",
        "raw_content_sha256": "b" * 64,
        "raw_payload": {
            "station_id": station,
            "weather_groups": groups,
            "sensor_status_flags": [
                flag
                for flag in flags
                if flag in {"PWINO", "PNO", "FZRANO", "TSNO"}
            ],
            "maintenance_required": "maintenance_required" in flags,
            "hourly_precipitation_mm": accumulation,
            "hourly_precipitation_interval":
                "since_previous_metar_report",
            "raw_text": f"METAR {station} {' '.join(flags)}",
        },
    }


def _synoptic(
    station: str = "KNYC",
    *,
    intervals: list[dict],
    qc_flagged=None,
) -> dict:
    return {
        "source": "synoptic",
        "station_id": f"synoptic:{station}",
        "lat": 40.75,
        "lon": -73.98,
        "timestamp": OBSERVED,
        "received_at": RECEIVED,
        "parser_version": "synoptic_interval_v3",
        "precipitation_present": any(
            float(item.get("value_mm") or 0.0) > 0.0
            for item in intervals
        ),
        "quality_flags": [],
        "raw_payload": {
            "stid": station,
            "precipitation_intervals": intervals,
            "qc_flagged": qc_flagged,
            "qc_summary": {"QC_CHECKS_APPLIED": ["synopticlabs"]},
            "api_summary": {"RESPONSE_CODE": 1},
        },
    }


def _interval(
    field: str,
    sensor_set: str,
    amount: float,
    *,
    start: float | None = OBSERVED - 300.0,
    end: float | None = OBSERVED,
    complete: bool = True,
    sample_index: int = 1,
) -> dict:
    return {
        "field": field,
        "sensor_set": sensor_set,
        "sample_index": sample_index,
        "value_mm": amount,
        "interval_start": start,
        "interval_end": end,
        "interval_seconds": (
            end - start
            if start is not None and end is not None else None
        ),
        "interval_complete": complete,
    }


def test_metar_vicinity_groups_are_point_dry_not_local_wet():
    batch = adapt_worker_observation(_metar(
        "KVC",
        groups=[
            _weather_group("VCSH", vicinity=True),
            _weather_group("VCRA", vicinity=True),
        ],
    ))

    assert len(batch.facts) == 1
    fact = batch.facts[0]
    observation = fact.observation
    assert fact.fact_kind == "metar_present_weather"
    assert observation.station_id == "KVC"
    assert observation.precipitation_state is PrecipitationState.ABSENT
    assert (
        observation.measurement_semantics
        is MeasurementSemantics.INSTANTANEOUS_STATE
    )
    assert observation.interval_start_at == observation.interval_end_at
    assert "precipitation_in_vicinity" in observation.quality_flags
    assert observation.provenance.raw_content_sha256 == "b" * 64
    assert observation.provenance.raw_artifact_receipt_id == (
        "receipt:metar:KVC"
    )
    assert json.loads(fact.raw_payload_json)[
        "worker_observation"
    ]["raw_payload"]["weather_groups"][1]["raw"] == "VCRA"


@pytest.mark.parametrize(
    ("flag", "expected"),
    (
        ("PWINO", PrecipitationState.UNAVAILABLE),
        ("PNO", PrecipitationState.UNKNOWN),
        ("FZRANO", PrecipitationState.UNKNOWN),
        ("TSNO", PrecipitationState.UNKNOWN),
        ("maintenance_required", PrecipitationState.UNKNOWN),
    ),
)
def test_metar_sensor_status_never_becomes_false_dry(flag, expected):
    observation = adapt_worker_observation(
        _metar("KFAIL", flags=[flag])
    ).observations[0]

    assert observation.precipitation_state is expected
    assert flag in observation.quality_flags


def test_metar_prrrr_without_previous_report_time_is_not_current_rain():
    batch = adapt_worker_observation(_metar(
        "KACC",
        groups=[],
        accumulation=3.048,
    ))

    assert [fact.fact_kind for fact in batch.facts] == [
        "metar_present_weather",
    ]
    assert batch.observations[0].precipitation_state is (
        PrecipitationState.ABSENT
    )
    assert batch.observations[0].amount is None
    issue = next(
        item
        for item in batch.issues
        if item.code == "metar_accumulation_interval_start_unknown"
    )
    assert issue.state is GroundAdapterIssueState.UNAVAILABLE


def test_metar_prrrr_is_separate_when_real_interval_is_explicit():
    record = _metar("KACC", groups=[], accumulation=1.27)
    record["raw_payload"]["hourly_precipitation_interval_start"] = (
        OBSERVED - 600.0
    )
    record["raw_payload"]["hourly_precipitation_interval_end"] = OBSERVED

    batch = adapt_worker_observation(record)
    by_kind = {fact.fact_kind: fact.observation for fact in batch.facts}

    assert by_kind["metar_present_weather"].precipitation_state is (
        PrecipitationState.ABSENT
    )
    accumulation = by_kind["metar_interval_accumulation"]
    assert accumulation.measurement_semantics is (
        MeasurementSemantics.INTERVAL_ACCUMULATION
    )
    assert accumulation.interval_start_at == OBSERVED - 600.0
    assert accumulation.interval_end_at == OBSERVED
    assert accumulation.amount == pytest.approx(1.27)
    assert accumulation.measurement_unit == "mm"


def test_synoptic_sensor_sets_stay_separate_and_are_never_summed():
    record = _synoptic(intervals=[
        _interval("precip_intervals_set_1", "1", 1.0),
        _interval("precip_intervals_set_2", "2", 2.0),
    ])

    batch = adapt_worker_observation(record)

    assert len(batch.facts) == 2
    assert {fact.sensor_id for fact in batch.facts} == {"1", "2"}
    assert sorted(
        fact.observation.amount for fact in batch.facts
    ) == [1.0, 2.0]
    assert all(
        fact.observation.measurement_semantics
        is MeasurementSemantics.INTERVAL_ACCUMULATION
        for fact in batch.facts
    )
    assert {
        fact.observation.provenance.provider_record_id
        for fact in batch.facts
    }.__len__() == 2
    assert batch.site_candidates[0].site.aliases == (
        batch.site_candidates[0].station_alias,
    )


def test_synoptic_incomplete_interval_is_unavailable_not_dry():
    record = _synoptic(intervals=[
        _interval(
            "precip_intervals_set_1",
            "1",
            0.0,
            start=None,
            complete=False,
        ),
    ])

    batch = adapt_worker_observation(record)

    assert len(batch.observations) == 1
    assert batch.observations[0].precipitation_state is (
        PrecipitationState.UNAVAILABLE
    )
    assert batch.observations[0].amount is None
    assert "incomplete_precip_interval" in (
        batch.observations[0].quality_flags
    )
    assert batch.issues[0].code == "synoptic_interval_incomplete"


def test_synoptic_qc_flagged_value_is_retained_as_unavailable_provenance():
    field = "precip_intervals_set_1d"
    record = _synoptic(
        intervals=[_interval(field, "1d", 4.2)],
        qc_flagged={field: [False, True]},
    )

    batch = adapt_worker_observation(record)
    observation = batch.observations[0]

    assert observation.precipitation_state is (
        PrecipitationState.UNAVAILABLE
    )
    assert observation.amount is None
    assert observation.measurement_semantics is (
        MeasurementSemantics.INTERVAL_ACCUMULATION
    )
    assert "provider_qc_failed" in observation.quality_flags
    snapshot = json.loads(batch.facts[0].raw_payload_json)
    assert snapshot["worker_observation"]["raw_payload"]["qc_flagged"] == {
        field: [False, True],
    }


def test_netatmo_parser_and_adapter_keep_each_module_and_temporal_semantics():
    data = {
        "body": [{
            "_id": "station-1",
            "place": {"location": [-73.98, 40.75]},
            "measures": {
                "module-a": {
                    "sensor_id": "rain-a",
                    "rain_live": 0.2,
                    "rain_60min": 1.0,
                    "rain_timeutc": OBSERVED,
                },
                "module-b": {
                    "sensor_id": "rain-b",
                    "rain_live": 0.0,
                    "rain_60min": 2.0,
                    "rain_timeutc": OBSERVED - 60.0,
                },
            },
        }],
    }
    worker_record = _parse_observations(data, RECEIVED)[0]

    assert worker_record["parser_version"] == "netatmo_pws_v4"
    raw_measurements = worker_record["raw_payload"]["rain_measurements"]
    assert [item["module_id"] for item in raw_measurements] == [
        "module-a",
        "module-b",
    ]
    assert raw_measurements[0]["rain_60min_interval_start"] == (
        OBSERVED - 3600.0
    )
    assert raw_measurements[0]["measurement_timestamp_field"] == (
        "rain_timeutc"
    )

    batch = adapt_worker_observation(worker_record)
    assert len(batch.facts) == 4
    live = [
        fact for fact in batch.facts
        if fact.fact_kind == "netatmo_rain_live"
    ]
    hourly = [
        fact for fact in batch.facts
        if fact.fact_kind == "netatmo_rain_60min"
    ]
    assert {fact.sensor_id for fact in live} == {"rain-a", "rain-b"}
    assert sorted(fact.observation.amount for fact in live) == [0.0, 0.2]
    assert sorted(fact.observation.amount for fact in hourly) == [1.0, 2.0]
    assert all(
        fact.observation.measurement_semantics
        is MeasurementSemantics.INSTANTANEOUS_RATE
        for fact in live
    )
    assert all(
        fact.observation.measurement_semantics
        is MeasurementSemantics.INTERVAL_ACCUMULATION
        and fact.observation.interval_seconds == 3600.0
        for fact in hourly
    )
    assert {
        fact.observation.provenance.provider_record_id
        for fact in batch.facts
    }.__len__() == 4


def test_legacy_netatmo_aggregate_cannot_become_false_sensor_fact():
    batch = adapt_worker_observation({
        "source": "netatmo",
        "station_id": "station-old",
        "lat": 40.75,
        "lon": -73.98,
        "timestamp": OBSERVED,
        "received_at": RECEIVED,
        "parser_version": "netatmo_pws_v2",
        "rain_mm": 9.0,
        "rain_60min": 20.0,
        "precipitation_present": True,
        "raw_payload": {
            "station_id": "station-old",
            "module_count": 4,
        },
    })

    assert len(batch.facts) == 1
    assert batch.observations[0].precipitation_state is (
        PrecipitationState.UNAVAILABLE
    )
    assert batch.observations[0].amount is None
    assert batch.issues[0].code == "netatmo_module_provenance_missing"


def test_netatmo_missing_rain_time_never_uses_fetch_time_as_dry_fact():
    worker_record = _parse_observations({
        "body": [{
            "_id": "station-no-time",
            "place": {"location": [-73.98, 40.75]},
            "measures": {
                "rain-module": {
                    "rain_live": 0.0,
                    "rain_60min": 0.0,
                },
            },
        }],
    }, RECEIVED)[0]

    assert worker_record["timestamp"] == RECEIVED
    assert worker_record["raw_payload"]["rain_measurements"][0][
        "measurement_timestamp"
    ] is None
    batch = adapt_worker_observation(worker_record)
    assert batch.observations == ()
    assert {
        issue.code for issue in batch.issues
    } == {
        "netatmo_live_timestamp_or_value_invalid",
        "netatmo_60min_interval_incomplete",
        "time_quality_quarantined",
    }


def test_explicit_time_quarantine_never_becomes_ground_vote():
    record = _parse_observations({
        "body": [{
            "_id": "station-future",
            "place": {"location": [-73.98, 40.75]},
            "measures": {
                "rain-module": {
                    "rain_live": 1.2,
                    "rain_timeutc": RECEIVED + 60.0,
                },
            },
        }],
    }, RECEIVED)[0]

    assert record["usable_for_current_decision"] is False
    assert record["time_quality"] == "future_clock_skew"
    batch = adapt_worker_observation(record)
    assert batch.facts == ()
    assert {issue.code for issue in batch.issues} == {
        "time_quality_quarantined",
    }


def test_exact_shared_icao_is_correlated_but_proximity_alone_is_not():
    metar = adapt_worker_observation(_metar(
        "KNYC",
        groups=[_weather_group("RA")],
    ))
    synoptic = adapt_worker_observation(_synoptic(
        "KNYC",
        intervals=[_interval("precip_intervals_set_1", "1", 1.0)],
    ))
    same_location_other_id = adapt_worker_observation(_synoptic(
        "KOTHER",
        intervals=[_interval("precip_intervals_set_1", "1", 1.0)],
    ))

    metar_site = metar.site_candidates[0].site
    synoptic_site = synoptic.site_candidates[0].site
    other_site = same_location_other_id.site_candidates[0].site
    assert metar_site.physical_site_id != synoptic_site.physical_site_id
    assert metar_site.independence_group == synoptic_site.independence_group
    assert other_site.independence_group != metar_site.independence_group

    registry = PhysicalSiteRegistry(
        sites=(metar_site, synoptic_site),
        registry_version="adapter-candidates-v1",
    )
    footprint = CellTrackFootprint(
        footprint_id="footprint-1",
        cell_track_id="track-1",
        cell_detection_id="detection-1",
        valid_from=OBSERVED - 400.0,
        valid_until=OBSERVED + 60.0,
        representative_at=OBSERVED,
        rings=((
            (40.70, -74.05),
            (40.80, -74.05),
            (40.80, -73.90),
            (40.70, -73.90),
        ),),
        radar_source="test-radar",
    )
    decision = fuse_ground_observations(
        metar.observations + synoptic.observations,
        registry,
        footprint,
        decision_key="candidate-dedup",
        target_id="tracker",
        region_key="region",
        evaluated_at=RECEIVED,
        policy=GroundFusionPolicy(
            freshness_seconds_by_provider=(
                ("metar", 3600.0),
                ("synoptic", 3600.0),
            ),
            minimum_independent_wet_groups=2,
            minimum_interval_overlap_fraction=0.5,
        ),
    )
    assert decision.state is GroundConfirmationState.NOT_CONFIRMED
    assert decision.independent_wet_groups == (
        metar_site.independence_group,
    )


def test_batch_deduplicates_exact_provider_alias_candidate():
    first = _metar("KDUP")
    second = dict(first)
    second["received_at"] = RECEIVED + 1.0

    batch = adapt_worker_observations((first, second))

    assert len(batch.site_candidates) == 1
    assert batch.station_aliases[0].provider_id == "metar"
    assert batch.station_aliases[0].station_id == "KDUP"


def test_batch_deduplicates_exact_fact_and_flags_coordinate_conflict():
    first = _metar("KMOVED")
    moved = _metar("KMOVED")
    moved["lat"] = 41.25

    exact = adapt_worker_observations((first, first))
    conflict = adapt_worker_observations((first, moved))

    assert len(exact.facts) == 1
    assert len(exact.site_candidates) == 1
    assert len(conflict.site_candidates) == 2
    assert "provider_station_coordinate_conflict" in {
        issue.code for issue in conflict.issues
    }


def test_ha_worker_adapter_mirrors_are_exact_and_not_live_wired():
    root = Path(__file__).resolve().parents[1]
    ha_module = (
        root
        / "custom_components/storm_tracker_v3/engine/"
        "ground_observation_adapters.py"
    )
    worker_module = (
        root
        / "storm_tracker_worker/tracking_core/engine/"
        "ground_observation_adapters.py"
    )

    assert ha_module.read_bytes() == worker_module.read_bytes()
    assert "ground_observation_adapters" not in (
        root / "custom_components/storm_tracker_v3/__init__.py"
    ).read_text(encoding="utf-8")
    assert "adapt_worker_observation" not in (
        root / "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")
