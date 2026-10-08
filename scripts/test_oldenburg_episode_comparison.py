from oldenburg_episode_comparison import score


def test_score_separates_weak_episode_from_intensity_and_does_not_score_unknown_dry():
    interval = dict(entry_at=1060.,first_supported_at=1060.,last_supported_at=1360.,
                    exit_at=None)
    summary = dict(issued_at=1000.,intervals=[interval],intensity_intervals=[],
                   known_sample_count=7,total_sample_count=121,support_state='partial')
    record = dict(issued_at=1000.,forecast={'radar_field_forecast':summary})
    observations = [dict(metadata={'observed_at':1360.},target_valid=True,target_rate_mm_h=.03),
                    dict(metadata={'observed_at':1420.},target_valid=True,target_rate_mm_h=0.)]
    result = score([record],observations)
    assert result['0.0']['episode'] == {'wet_on_wet':1,'unknown_on_dry':1}
    assert result['0.0']['intensity'] == {'unknown_on_wet':1,'unknown_on_dry':1}
    assert result['0.1']['episode'] == {'wet_on_dry':1,'unknown_on_dry':1}
