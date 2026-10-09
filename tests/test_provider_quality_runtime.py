from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.provider_quality_runtime import (
    ProviderFrameQualityInput,
    ProviderFreshnessPolicy,
    ProviderQualityPolicy,
    ProviderSelectionState,
    assess_provider_frame,
    select_current_provider,
)


NOW = 2_000.0


def _weights(**changes: float) -> tuple[tuple[str, float], ...]:
    values = {
        "freshness": 2.0,
        "completeness": 2.0,
        "upstream_quality": 1.0,
        "geolocation_consistency": 1.0,
        "echo_continuity": 1.0,
        "operational_health": 2.0,
        "empirical_replay_performance": 1.0,
        "handoff_readiness": 1.0,
    }
    values.update(changes)
    return tuple(values.items())


def _policy(**changes) -> ProviderQualityPolicy:
    values = {
        "policy_id": "provider-quality-test-v1",
        "component_weights": _weights(),
        "freshness_by_provider": (
            ProviderFreshnessPolicy("kmi", 300.0, 900.0),
            ProviderFreshnessPolicy("rainviewer", 180.0, 600.0),
            ProviderFreshnessPolicy("noaa", 120.0, 480.0),
        ),
        "maximum_future_skew_seconds": 30.0,
        "tie_score_tolerance": 0.0,
    }
    values.update(changes)
    return ProviderQualityPolicy(**values)


def _frame(
    frame_id: str,
    provider_id: str,
    *,
    age_seconds: float = 100.0,
    valid_pixel_count: int = 90,
    upstream_quality: float | None = 0.8,
    geolocation_consistency: float | None = 0.9,
    echo_continuity: float | None = 0.7,
    parser_error: str | None = None,
    fetch_error: str | None = None,
    empirical_replay_performance: float | None = 0.6,
    handoff_readiness: float | None = 0.8,
) -> ProviderFrameQualityInput:
    observed_at = NOW - age_seconds
    return ProviderFrameQualityInput(
        provider_id=provider_id,
        frame_id=frame_id,
        product_id=f"{provider_id}-rain-rate",
        observed_at=observed_at,
        received_at=min(NOW, observed_at + 10.0),
        evaluated_at=NOW,
        total_pixel_count=100,
        valid_pixel_count=valid_pixel_count,
        upstream_quality=upstream_quality,
        geolocation_consistency=geolocation_consistency,
        echo_continuity=echo_continuity,
        parser_error=parser_error,
        fetch_error=fetch_error,
        empirical_replay_performance=empirical_replay_performance,
        empirical_replay_metric_id=(
            "heldout-provider-performance-index-v1"
            if empirical_replay_performance is not None
            else None
        ),
        empirical_replay_event_count=(
            20 if empirical_replay_performance is not None else None
        ),
        empirical_replay_corpus_sha256=(
            "c" * 64 if empirical_replay_performance is not None else None
        ),
        empirical_replay_split_sha256=(
            "d" * 64 if empirical_replay_performance is not None else None
        ),
        empirical_replay_evidence_sha256=(
            "b" * 64 if empirical_replay_performance is not None else None
        ),
        handoff_readiness=handoff_readiness,
        input_artifact_sha256="a" * 64,
        parser_version=f"{provider_id}-parser-v3",
    )


def test_quality_score_publishes_every_component_and_exact_contribution():
    frame = _frame("kmi-current", "kmi")

    assessment = assess_provider_frame(frame, policy=_policy())

    assert assessment.eligible_for_current_selection
    assert assessment.frame_age_seconds == 100.0
    assert assessment.is_stale is False
    assert assessment.is_future is False
    assert tuple(item.component_id for item in assessment.components) == (
        "freshness",
        "completeness",
        "upstream_quality",
        "geolocation_consistency",
        "echo_continuity",
        "operational_health",
        "empirical_replay_performance",
        "handoff_readiness",
    )
    values = {item.component_id: item.value for item in assessment.components}
    assert values["freshness"] == 1.0
    assert values["completeness"] == 0.9
    assert values["operational_health"] == 1.0
    assert (
        assessment.empirical_replay_metric_id
        == "heldout-provider-performance-index-v1"
    )
    assert assessment.empirical_replay_event_count == 20
    assert assessment.empirical_replay_corpus_sha256 == "c" * 64
    assert assessment.empirical_replay_split_sha256 == "d" * 64
    expected = (
        2.0 * 1.0
        + 2.0 * 0.9
        + 0.8
        + 0.9
        + 0.7
        + 2.0
        + 0.6
        + 0.8
    ) / 11.0
    assert assessment.score == pytest.approx(expected)


def test_stale_healthy_frame_is_preserved_but_never_selected_as_current():
    stale = _frame(
        "rainviewer-stale",
        "rainviewer",
        age_seconds=900.0,
        valid_pixel_count=100,
        upstream_quality=1.0,
        geolocation_consistency=1.0,
        echo_continuity=1.0,
        empirical_replay_performance=1.0,
        handoff_readiness=1.0,
    )
    current = _frame(
        "kmi-current",
        "kmi",
        valid_pixel_count=70,
        upstream_quality=0.5,
        geolocation_consistency=0.5,
        echo_continuity=0.5,
        empirical_replay_performance=0.5,
        handoff_readiness=0.5,
    )

    selection = select_current_provider(
        (stale, current),
        policy=_policy(),
    )

    assert selection.state is ProviderSelectionState.SELECTED
    assert selection.selected_frame_id == "kmi-current"
    stale_assessment = next(
        item
        for item in selection.assessments
        if item.frame_id == "rainviewer-stale"
    )
    assert stale_assessment.score is not None
    assert stale_assessment.is_stale
    assert not stale_assessment.eligible_for_current_selection
    assert "frame_stale_for_current_selection" in (
        stale_assessment.reason_codes
    )


def test_missing_weighted_evidence_fails_closed_but_zero_weight_is_explicit():
    missing = _frame(
        "missing-continuity",
        "kmi",
        echo_continuity=None,
    )

    assessment = assess_provider_frame(missing, policy=_policy())
    assert assessment.score is None
    assert not assessment.eligible_for_current_selection
    assert "weighted_component_missing:echo_continuity" in (
        assessment.reason_codes
    )

    zero_weight_policy = _policy(
        component_weights=_weights(echo_continuity=0.0)
    )
    explicitly_ignored = assess_provider_frame(
        missing,
        policy=zero_weight_policy,
    )
    assert explicitly_ignored.score is not None
    assert explicitly_ignored.eligible_for_current_selection
    component = next(
        item
        for item in explicitly_ignored.components
        if item.component_id == "echo_continuity"
    )
    assert component.value is None
    assert component.weight == 0.0
    assert component.evidence_state == "missing"


def test_parser_fetch_future_and_unknown_freshness_are_not_weather_rejections():
    parser_error = _frame(
        "parser-error",
        "kmi",
        parser_error="malformed-grid",
    )
    future = _frame("future", "noaa", age_seconds=-60.0)
    unknown_provider = _frame("unknown", "new-provider")

    selection = select_current_provider(
        (parser_error, future, unknown_provider),
        policy=_policy(),
    )

    assert selection.state is ProviderSelectionState.NO_ELIGIBLE_FRAME
    assert len(selection.assessments) == 3
    reasons = {
        item.frame_id: item.reason_codes for item in selection.assessments
    }
    assert "parser_error_present" in reasons["parser-error"]
    assert "frame_time_beyond_future_tolerance" in reasons["future"]
    assert "freshness_policy_missing" in reasons["unknown"]


def test_true_quality_and_time_tie_is_reported_instead_of_provider_priority():
    left = _frame("frame-a", "kmi")
    right = _frame("frame-b", "rainviewer")
    policy = _policy(
        freshness_by_provider=(
            ProviderFreshnessPolicy("kmi", 300.0, 900.0),
            ProviderFreshnessPolicy("rainviewer", 300.0, 900.0),
        )
    )

    tied = select_current_provider((right, left), policy=policy)

    assert tied.state is ProviderSelectionState.TIED
    assert tied.selected_provider_id is None
    assert tied.tied_frame_ids == ("frame-a", "frame-b")

    newer = replace(
        right,
        frame_id="frame-newer",
        observed_at=right.observed_at + 50.0,
        received_at=right.received_at + 50.0,
    )
    selected = select_current_provider((left, newer), policy=policy)
    assert selected.state is ProviderSelectionState.SELECTED
    assert selected.selected_frame_id == "frame-newer"


def test_selection_is_input_order_independent_and_policy_has_no_defaults():
    frames = (
        _frame("kmi-a", "kmi", upstream_quality=0.7),
        _frame("rv-b", "rainviewer", upstream_quality=0.9),
    )

    first = select_current_provider(frames, policy=_policy())
    second = select_current_provider(reversed(frames), policy=_policy())

    assert first == second
    with pytest.raises(TypeError):
        ProviderQualityPolicy(policy_id="incomplete")  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="canonical component"):
        _policy(component_weights=(("freshness", 1.0),))
    with pytest.raises(ValueError, match="typed provider frames"):
        select_current_provider(("not-a-frame",), policy=_policy())


def test_frame_contract_rejects_partial_replay_evidence_and_invalid_pixels():
    valid = _frame("valid", "kmi")
    with pytest.raises(ValueError, match="present together"):
        replace(valid, empirical_replay_evidence_sha256=None)
    with pytest.raises(ValueError, match="cannot exceed"):
        replace(valid, valid_pixel_count=101)
    with pytest.raises(ValueError, match="cannot precede received_at"):
        replace(valid, received_at=NOW + 1.0)
    with pytest.raises(ValueError, match=r"lie in \[0, 1\]"):
        replace(valid, handoff_readiness=1.1)


def test_provider_quality_mirrors_are_exact_and_shadow_only():
    root = Path(__file__).resolve().parents[1]
    integration = (
        root
        / "custom_components/storm_tracker_v3/engine/provider_quality_runtime.py"
    )
    worker = (
        root
        / "storm_tracker_worker/tracking_core/engine/provider_quality_runtime.py"
    )
    assert integration.read_bytes() == worker.read_bytes()
    for live_path in (
        root / "custom_components/storm_tracker_v3/__init__.py",
        root / "custom_components/storm_tracker_v3/engine/nowcast.py",
        root / "storm_tracker_worker/service.py",
        root / "storm_tracker_worker/tracking_core/__init__.py",
    ):
        assert "provider_quality_runtime" not in live_path.read_text(
            encoding="utf-8"
        )
