def test_profile_selection_prefers_exact_region(provider_bias_module):
    profiles = (
        {
            "scope_key": "*", "from_source": "kmi", "to_source": "opera",
            "sample_count": 100, "mean_f1_score": 0.5,
        },
        {
            "scope_key": "51.05,4.42,350",
            "from_source": "kmi", "to_source": "opera",
            "sample_count": 20, "mean_f1_score": 0.9,
        },
    )
    index = provider_bias_module.build_profile_index(profiles)
    selected = provider_bias_module.select_transition_profile(
        index,
        region_key="region-1@51.05,4.42,350",
        from_source="kmi",
        to_source="opera",
    )
    assert selected["scope_key"] == "51.05,4.42,350"


def test_profile_selection_falls_back_to_global(provider_bias_module):
    profile = {
        "scope_key": "*", "from_source": "kmi", "to_source": "opera",
        "sample_count": 100, "mean_f1_score": 0.7,
    }
    selected = provider_bias_module.select_transition_profile(
        provider_bias_module.build_profile_index((profile,)),
        region_key="region-9@50.00,5.00,350",
        from_source="kmi",
        to_source="opera",
    )
    assert selected == profile


def test_transition_without_history_preserves_safe_default(provider_bias_module):
    adjustment = provider_bias_module.transition_adjustment(None)
    assert adjustment["profile_available"] is False
    assert adjustment["confidence_penalty_percent"] == 10
    assert adjustment["transition_window_seconds"] == 600
    assert adjustment["application"] == "confidence_only"


def test_good_mature_profile_reduces_but_never_removes_margin(
    provider_bias_module,
):
    adjustment = provider_bias_module.transition_adjustment({
        "scope_key": "51.05,4.42,350",
        "sample_count": 120,
        "confidence": "high",
        "mean_f1_score": 0.9,
        "mean_detection_ratio": 0.95,
    })
    assert adjustment["profile_available"] is True
    assert adjustment["confidence_penalty_percent"] == 4
    assert adjustment["transition_window_seconds"] == 300


def test_poor_profile_increases_transition_penalty(provider_bias_module):
    adjustment = provider_bias_module.transition_adjustment({
        "scope_key": "*",
        "sample_count": 40,
        "confidence": "medium",
        "mean_f1_score": 0.1,
    })
    assert adjustment["confidence_penalty_percent"] == 12
    assert adjustment["transition_window_seconds"] == 600
