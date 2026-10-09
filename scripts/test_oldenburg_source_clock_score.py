from oldenburg_source_clock_score import reference_events, recorded_passage_expectation


def row(at, rate):
    return {'metadata': {'observed_at': at}, 'target_rate_mm_h': rate}


def test_weak_and_policy_onset_references_stay_distinct():
    rows = [row(0, 0.), row(300, .02), row(600, .2), row(900, 0.)]
    assert reference_events(rows, 0)[0]['entry_bracket'] == [0, 300]
    assert reference_events(rows, .1)[0]['entry_bracket'] == [300, 600]


def test_unknown_reference_does_not_prove_continuous_rain_or_an_exit():
    events = reference_events([row(0, .2), row(300, None), row(600, .2), row(900, 0.)], .1)
    assert len(events) == 2
    assert events[0]['exit_bracket'] is None
    assert events[0]['end_status'] == 'coverage_censored'
    assert events[1]['entry_bracket'] is None


def test_source_product_gap_is_explicit_not_interpolated():
    events = reference_events([row(0, 0.), row(1200, .2), row(1500, 0.)], .1)
    assert events[0]['entry_bracket'] == [0, 1200]
    assert events[0]['maximum_adjacent_product_gap_seconds'] == 1200


def test_old_unknown_end_is_not_invented_and_no_warning_is_not_dry_truth():
    assert recorded_passage_expectation([], 1300) == 'no_issued_wet_interval'
    assert recorded_passage_expectation([{'entry_at': 1000, 'exit_at': None}], 1300) == 'unknown_censored_end'
    assert recorded_passage_expectation([{'entry_at': 1000, 'exit_at': 1500}], 1300) == 'issued_wet_interval'
