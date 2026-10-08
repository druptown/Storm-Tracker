from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
from oldenburg_full_chain_score import issued_expectation, compare


def record():
    return {'issued_at':1000.,'forecast':{'radar_field_forecast':{
        'issued_at':1000.,'support_state':'partial','known_sample_count':7,'total_sample_count':121,
        'intervals':[{'entry_at':1100.,'exit_at':None,'first_supported_at':1100.,'last_supported_at':1360.}],
        'intensity_intervals':[]}},'passages':[]}


def test_weak_episode_censored_tail_and_absent_kernel_are_not_dry():
    r = record()
    assert issued_expectation(r,1360.) == 'wet'
    assert issued_expectation(r,1420.) == 'unknown'
    assert issued_expectation(r,1360.,'intensity') == 'unknown'
    assert issued_expectation(r,1360.,'kernel_windows') == 'unknown'


def test_reference_thresholds_and_missing_original_warning_remain_distinct():
    obs = [{'metadata':{'observed_at':1360.},'target_valid':True,'target_rate_mm_h':.03}]
    out = compare([record()],obs,{1000.:{'forecast_issue_id':'old'}},{'old':[]})
    assert out['0.0']['5-30min']['episode'] == {'wet_on_wet':1}
    assert out['0.1']['5-30min']['episode'] == {'wet_on_dry':1}
    assert out['0.0']['5-30min']['original'] == {'no_issued_interval_on_wet':1}
