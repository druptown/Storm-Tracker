"""Focused verification semantics for the offline field benchmark."""
from oldenburg_field_score import sample_prediction, classification, bracket_error_minutes


def test_unknown_is_an_abstention_not_dry():
    samples = [{'at': 60., 'rate_mm_h': None, 'reason': 'motion_support_missing'}]
    rate, reason = sample_prediction(samples, 60.)
    assert rate is None and reason == 'motion_support_missing'
    assert classification(rate, .2, .1) == 'unknown_on_wet'
    assert classification(rate, 0., .1) == 'unknown_on_dry'


def test_no_interpolation_across_unknown_prediction():
    samples = [{'at': 0., 'rate_mm_h': .2}, {'at': 60., 'rate_mm_h': None},
               {'at': 120., 'rate_mm_h': .2}]
    assert sample_prediction(samples, 70.)[0] is None
    assert sample_prediction(samples, 160.)[0] is None


def test_weak_echo_and_policy_threshold_are_distinct():
    assert classification(.06, .03, 0.) == 'hit'
    assert classification(.06, .03, .1) == 'correct_dry'
    assert classification(.2, 0., .1) == 'false_wet'
    assert classification(0., .2, .1) == 'miss'


def test_arrival_error_keeps_observed_time_bracket():
    assert bracket_error_minutes(120., [60., 180.]) == [-1., 1.]
    assert bracket_error_minutes(None, [60., 180.]) is None
    assert bracket_error_minutes(120., [60., None]) is None
