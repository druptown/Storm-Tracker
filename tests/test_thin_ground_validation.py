from __future__ import annotations

from dataclasses import replace

import pytest

from storm_tracker_worker.thin_footprint_forecast import TargetPassage
from storm_tracker_worker.thin_ground_validation import (
    GroundValidationRuntime,
    validate_passages,
)
from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)


NOW = 20_000.0


def _passage(*, entry=NOW - 300.0, exit=NOW + 300.0):
    return TargetPassage(
        target_id="home",
        evaluated_at=NOW - 600.0,
        cell_id="cell-1",
        provider_id="dwd_radolan",
        entry_at=entry,
        exit_at=exit,
        method="translated_footprint",
        predicted_intensity_mm_h=4.0,
        footprint={"type": "Polygon", "coordinates": [[
            [4.4, 51.0], [4.5, 51.0], [4.5, 51.1],
            [4.4, 51.1], [4.4, 51.0],
        ]]},
    )


def _rain(
    provider: str,
    station: str,
    observed_at: float,
    value: float,
):
    return NormalizedObservation.create(
        provider_id=provider,
        product_id="ground_observation",
        target_id="home",
        kind=ObservationKind.RAIN_RATE,
        value=value,
        unit="mm/h",
        observed_at=observed_at,
        received_at=observed_at + 1.0,
        latitude=51.05,
        longitude=4.42,
        quality=ObservationQuality.GOOD,
        metadata={"station_id": station},
        maximum_age_seconds=10_000.0,
    )


def test_hit_aggregates_mixed_providers_and_compares_intensity():
    outcomes = validate_passages(
        (_passage(),),
        (
            _rain("netatmo", "net-1", NOW - 60.0, 3.0),
            _rain("metar", "EBBR", NOW, 2.0),
            _rain("wunderground", "IMECHE17", NOW + 30.0, 5.0),
        ),
        evaluated_at=NOW + 30.0,
    )

    assert len(outcomes) == 1
    assert outcomes[0].outcome == "hit"
    assert outcomes[0].observed_intensity_mm_h == 5.0
    assert outcomes[0].intensity_error_mm_h == 1.0
    assert outcomes[0].contributing_stations == (
        "metar:ebbr", "netatmo:net-1", "wunderground:imeche17",
    )


def test_early_and_late_observations_are_distinguished():
    passage = _passage(entry=NOW, exit=NOW + 300.0)

    early = validate_passages(
        (passage,), (_rain("netatmo", "one", NOW - 400.0, 1.0),),
        evaluated_at=NOW,
        tolerance_seconds=120.0,
    )
    late = validate_passages(
        (passage,), (_rain("netatmo", "one", NOW + 500.0, 1.0),),
        evaluated_at=NOW + 500.0,
        tolerance_seconds=120.0,
    )

    assert early[0].outcome == "early"
    assert early[0].entry_error_seconds == -400.0
    assert late[0].outcome == "late"
    assert late[0].entry_error_seconds == 500.0


def test_expired_passage_with_dry_confirmation_is_false_alarm():
    passage = _passage(entry=NOW - 900.0, exit=NOW - 600.0)

    outcomes = validate_passages(
        (passage,), (_rain("wunderground", "dry", NOW - 700.0, 0.0),),
        evaluated_at=NOW,
        tolerance_seconds=120.0,
    )

    assert outcomes[0].outcome == "false_alarm"
    assert outcomes[0].observed_intensity_mm_h == 0.0


def test_unpredicted_wet_station_is_a_miss():
    outcomes = validate_passages(
        (), (_rain("netatmo", "wet", NOW, 2.0),),
        evaluated_at=NOW,
    )

    assert len(outcomes) == 1
    assert outcomes[0].outcome == "miss"
    assert outcomes[0].cell_id is None


def test_stale_ground_data_is_rejected_and_physical_station_is_deduplicated():
    passage = _passage()
    duplicate = _rain("netatmo", "same", NOW, 2.0)

    outcomes = validate_passages(
        (passage,),
        (
            duplicate,
            duplicate,
            _rain("wunderground", "old", NOW - 8_000.0, 5.0),
        ),
        evaluated_at=NOW,
        maximum_ground_age_seconds=7_200.0,
    )

    assert outcomes[0].outcome == "hit"
    assert outcomes[0].contributing_stations == ("netatmo:same",)


def test_first_dry_measurement_after_wet_estimates_exit_error():
    passage = _passage(entry=NOW - 300.0, exit=NOW + 60.0)

    outcomes = validate_passages(
        (passage,),
        (
            _rain("netatmo", "same", NOW, 2.0),
            _rain("netatmo", "same", NOW + 180.0, 0.0),
        ),
        evaluated_at=NOW + 180.0,
    )

    assert outcomes[0].outcome == "hit"
    assert outcomes[0].exit_error_seconds == 120.0


def test_runtime_emits_the_same_evidence_only_once_across_cycles():
    runtime = GroundValidationRuntime()
    passage = _passage()
    rain = _rain("netatmo", "same", NOW, 2.0)

    first = runtime.advance((passage,), (rain,), evaluated_at=NOW)
    second = runtime.advance((passage,), (rain,), evaluated_at=NOW + 10.0)

    assert len(first) == 1
    assert second == ()


def test_open_ended_passage_validates_arrival_without_inventing_exit_error():
    passage = replace(_passage(exit=None), right_censored=True)

    outcomes = GroundValidationRuntime().advance(
        (passage,),
        (_rain("netatmo", "same", NOW, 2.0),
         _rain("netatmo", "same", NOW + 180.0, 0.0)),
        evaluated_at=NOW + 180.0,
    )

    assert len(outcomes) == 1
    assert outcomes[0].outcome == "hit"
    assert outcomes[0].entry_error_seconds == 300.0
    assert outcomes[0].exit_error_seconds is None


def test_open_ended_passage_with_dry_evidence_is_not_a_false_alarm():
    outcomes = validate_passages(
        (_passage(exit=None),),
        (_rain("netatmo", "same", NOW + 900.0, 0.0),),
        evaluated_at=NOW + 900.0,
    )

    assert outcomes == ()


def test_open_ended_passage_is_retained_but_eventually_expires():
    runtime = GroundValidationRuntime()
    passage = _passage(entry=NOW + 600.0, exit=None)
    assert runtime.advance((passage,), (), evaluated_at=NOW) == ()
    hit = runtime.advance(
        (), (_rain("netatmo", "same", NOW + 700.0, 2.0),),
        evaluated_at=NOW + 700.0,
    )
    assert len(hit) == 1
    assert hit[0].outcome == "hit"

    # Two hours after the known entry: the open forecast cannot suppress
    # genuinely unpredicted rain indefinitely.
    expired = runtime.advance(
        (), (_rain("netatmo", "same", NOW + 8_501.0, 2.0),),
        evaluated_at=NOW + 8_501.0,
    )
    assert len(expired) == 1
    assert expired[0].outcome == "miss"


def test_left_censored_arrival_has_no_artificial_entry_error():
    passage = replace(_passage(entry=NOW, exit=None), left_censored=True)
    outcomes = validate_passages(
        (passage,), (_rain("netatmo", "same", NOW - 400.0, 2.0),),
        evaluated_at=NOW,
    )

    assert len(outcomes) == 1
    assert outcomes[0].outcome == "hit"
    assert outcomes[0].entry_error_seconds is None
    assert outcomes[0].exit_error_seconds is None


@pytest.mark.parametrize("censor", ["horizon_clipped", "right_censored", "coverage_censored"])
def test_censored_numeric_boundary_is_not_scored_as_a_known_end(censor):
    passage = replace(
        _passage(entry=NOW - 300.0, exit=NOW + 60.0), **{censor: True},
    )
    outcomes = validate_passages(
        (passage,),
        (_rain("netatmo", "same", NOW, 2.0),
         _rain("netatmo", "same", NOW + 180.0, 0.0)),
        evaluated_at=NOW + 180.0,
    )
    assert len(outcomes) == 1
    assert outcomes[0].outcome == "hit"
    assert outcomes[0].exit_error_seconds is None

    assert validate_passages(
        (passage,), (_rain("netatmo", "same", NOW + 900.0, 0.0),),
        evaluated_at=NOW + 900.0,
    ) == ()
