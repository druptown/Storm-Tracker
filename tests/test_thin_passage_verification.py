from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from storm_tracker_worker.thin_passage_verification import (
    PassageForecastEvidence,
    PassageReferenceEvidence,
    RadarForecastIssue,
    build_temporal_radar_reference,
    verify_passages_by_event,
)


def _field(
    fingerprint: str,
    valid_at: float,
    *,
    received_at: float,
    available_at: float,
    complete: bool = True,
):
    return SimpleNamespace(
        provider_id="fusion:home",
        valid_mask=np.array([[complete]], dtype=bool),
        identity=SimpleNamespace(
            content_fingerprint=fingerprint,
            canonical_valid_at=valid_at,
            received_at=received_at,
            available_at=available_at,
            grid_id="grid-a",
            quantity="precipitation_rate",
            unit="mm/h",
            quality_kind="source_valid_mask",
        ),
    )


def test_temporal_reference_rejects_issue_time_leakage_and_selects_exact_revision():
    leaked = RadarForecastIssue(
        forecast_issue_id="issue-a", issued_at=100.0,
        input_fingerprints=("selected",),
        input_fields=(_field(
            "selected", 90.0, received_at=101.0, available_at=99.0,
        ),),
    )
    with pytest.raises(ValueError, match="issue_time_input_leakage"):
        build_temporal_radar_reference(leaked, ())

    issue = RadarForecastIssue(
        forecast_issue_id="issue-a", issued_at=100.0,
        input_fingerprints=("selected",),
        input_fields=(
            _field("older-revision", 90.0, received_at=92.0, available_at=92.0),
            _field("selected", 90.0, received_at=95.0, available_at=95.0),
        ),
    )
    reference = build_temporal_radar_reference(issue, (
        _field("same-time", 90.0, received_at=110.0, available_at=110.0),
        _field("incomplete", 110.0, received_at=111.0, available_at=111.0, complete=False),
        _field("later-complete", 120.0, received_at=121.0, available_at=121.0),
    ))

    assert [item.identity.content_fingerprint for item in reference.input_fields] == [
        "selected"
    ]
    assert [item.identity.content_fingerprint for item in reference.later_fields] == [
        "later-complete"
    ]
    assert reference.provenance["grid_ids"] == ("grid-a",)
    assert reference.provenance["quantities"] == ("precipitation_rate:mm/h",)


def test_bracket_verification_is_event_weighted_and_retains_misses_and_censoring():
    forecasts = (
        PassageForecastEvidence(
            event_id="event-hit", passage_id="p1", status="confirmed",
            entry_bracket=(100.0, 120.0), exit_bracket=(180.0, 200.0),
        ),
        PassageForecastEvidence(
            event_id="event-false", passage_id="p2", status="candidate",
            entry_bracket=(300.0, 320.0), exit_bracket=None,
            right_censored=True,
        ),
    )
    references = (
        PassageReferenceEvidence(
            event_id="event-hit", occurred=True,
            entry_bracket=(110.0, 115.0), exit_bracket=(185.0, 195.0),
        ),
        PassageReferenceEvidence(event_id="event-false", occurred=False),
        PassageReferenceEvidence(
            event_id="event-miss", occurred=True,
            entry_bracket=(500.0, 510.0), exit_bracket=(550.0, 560.0),
        ),
    )

    report = verify_passages_by_event(forecasts, references)

    assert report.event_count == 3
    assert report.hit_count == 1
    assert report.false_alarm_count == 1
    assert report.miss_count == 1
    assert report.censored_forecast_count == 1
    hit = next(item for item in report.events if item.event_id == "event-hit")
    assert hit.entry_intersects is True
    assert hit.entry_contains_reference is True
    assert hit.duration_error_seconds == pytest.approx(2.5)
