from oldenburg_target_score import field_expectation, original_expectation


def summary(state='complete', end=180):
    return dict(issued_at=0, support_state=state, known_sample_count=121,
                total_sample_count=121, intervals=[dict(entry_at=60,
                first_supported_at=90, last_supported_at=150, exit_at=end)])


def test_complete_issued_interval_dry_before_and_wet_inside():
    assert field_expectation(summary(), 30) == 'dry'
    assert field_expectation(summary(), 120) == 'wet'


def test_partial_does_not_claim_dry_outside_supported_wet():
    assert field_expectation(summary('partial'), 30) == 'unknown'
    assert field_expectation(summary('partial'), 120) == 'wet'


def test_censored_end_is_unknown_after_last_support_not_dry_or_infinite_wet():
    assert field_expectation(summary(end=None), 120) == 'wet'
    assert field_expectation(summary(end=None), 170) == 'unknown'


def test_held_complete_summary_uses_original_absolute_times_and_horizon():
    assert field_expectation(summary('held'), 120) == 'wet'
    assert field_expectation(summary('held'), 7300) == 'unknown'


def test_original_censored_end_is_unknown_and_absence_is_not_dry():
    assert original_expectation([dict(entry_at=60, exit_at=None)], 120) == 'unknown_censored_end'
    assert original_expectation([], 120) == 'no_issued_interval'
    assert original_expectation([dict(entry_at=60, exit_at=180)], 120) == 'issued_wet_interval'
