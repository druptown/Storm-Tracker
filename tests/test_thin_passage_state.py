from __future__ import annotations

from storm_tracker_worker.thin_passage_state import (
    PassageEvidence,
    PassageStatePolicy,
    PassageStateRegistry,
)


POLICY = PassageStatePolicy(
    confirmation_scans=2,
    cancellation_scans=2,
    maximum_coasting_seconds=900.0,
)


def _evidence(
    outcome: str,
    valid_at: float,
    *,
    alias: str = "passage:storm-a",
    evaluated_at: float | None = None,
    complete_corridor: bool = False,
    passage_ids: tuple[str, ...] = ("interval:0",),
) -> PassageEvidence:
    return PassageEvidence(
        target_id="home",
        passage_system_id=alias,
        radar_valid_at=valid_at,
        evaluated_at=valid_at if evaluated_at is None else evaluated_at,
        field_fingerprint=f"{int(valid_at):064x}"[-64:],
        outcome=outcome,
        complete_corridor=complete_corridor,
        passage_ids=passage_ids,
        lineage_status="tracked",
    )


def test_two_unique_support_scans_confirm_but_same_time_revision_does_not() -> None:
    registry = PassageStateRegistry(POLICY)

    first = registry.advance(_evidence("support", 100.0))
    repeated = registry.advance(_evidence("support", 100.0))
    second = registry.advance(_evidence("support", 200.0))

    assert first.status == "candidate"
    assert repeated.status == "candidate"
    assert repeated.support_scan_count == 1
    assert second.status == "confirmed"
    assert second.support_scan_count == 2


def test_unusable_or_target_dry_evidence_coasts_without_cancelling() -> None:
    registry = PassageStateRegistry(POLICY)
    registry.advance(_evidence("support", 100.0))
    registry.advance(_evidence("support", 200.0))

    unusable = registry.advance(_evidence("unusable", 300.0))
    dry = registry.advance(_evidence("ground_dry", 301.0))

    assert unusable.status == "coasting"
    assert dry.status == "coasting"
    assert dry.counterevidence_scan_count == 0


def test_only_two_complete_lineage_corridor_scans_cancel() -> None:
    registry = PassageStateRegistry(POLICY)
    registry.advance(_evidence("support", 100.0))
    registry.advance(_evidence("support", 200.0))

    incomplete = registry.advance(_evidence("counter", 300.0))
    first = registry.advance(_evidence("counter", 400.0, complete_corridor=True))
    second = registry.advance(_evidence("counter", 500.0, complete_corridor=True))

    assert incomplete.status == "coasting"
    assert incomplete.counterevidence_scan_count == 0
    assert first.status == "coasting"
    assert second.status == "cancelled"


def test_ground_wet_changes_current_state_without_radar_support() -> None:
    registry = PassageStateRegistry(POLICY)
    candidate = registry.advance(_evidence("support", 100.0))
    current = registry.advance(_evidence("ground_wet", 101.0))

    assert candidate.support_scan_count == 1
    assert current.status == "current"
    assert current.support_scan_count == 1


def test_restart_and_expired_coasting_fail_indeterminate() -> None:
    registry = PassageStateRegistry(POLICY)
    recovering = registry.recover("home", "passage:storm-a", evaluated_at=100.0)
    registry.advance(_evidence("support", 200.0))
    registry.advance(_evidence("support", 300.0))
    expired = registry.advance(
        _evidence("unusable", 400.0, evaluated_at=1_301.0),
    )

    assert recovering.status == "indeterminate"
    assert recovering.reason_code == "recovering_after_restart"
    assert expired.status == "indeterminate"
    assert expired.reason_code == "coasting_freshness_exceeded"


def test_unrelated_system_cannot_cancel_existing_passage() -> None:
    registry = PassageStateRegistry(POLICY)
    registry.advance(_evidence("support", 100.0, alias="passage:storm-a"))
    registry.advance(_evidence("support", 200.0, alias="passage:storm-a"))

    registry.advance(_evidence("counter", 300.0, alias="passage:storm-b", complete_corridor=True))
    original = registry.snapshot("home", "passage:storm-a")

    assert original.status == "confirmed"
    assert original.counterevidence_scan_count == 0


def test_split_merge_and_reentry_remain_distinct() -> None:
    registry = PassageStateRegistry(POLICY)
    left = registry.advance(_evidence(
        "support", 100.0, alias="passage:split-left",
        passage_ids=("left:0", "left:1"),
    ))
    right = registry.advance(_evidence(
        "support", 100.0, alias="passage:split-right",
        passage_ids=("right:0",),
    ))

    assert left.passage_system_id != right.passage_system_id
    assert left.passage_ids == ("left:0", "left:1")
    assert right.passage_ids == ("right:0",)
