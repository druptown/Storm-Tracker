"""Tests for isolated, interval-aware, cell-specific ground fusion."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.ground_fusion import (
    CellTrackFootprint,
    GroundConfirmationState,
    GroundEvidenceRole,
    GroundFusionPolicy,
    GroundMatchOutcome,
    GroundObservation,
    MeasurementSemantics,
    PhysicalSite,
    PhysicalSiteRegistry,
    PrecipitationState,
    RawGroundProvenance,
    StationAlias,
    assess_ground_observation,
    decide_ground_confirmation,
    fuse_ground_observations,
)


NOW = 1_800_000_100.0
OBSERVED = 1_800_000_000.0


def _site(
    site_id: str,
    group_id: str,
    latitude: float,
    longitude: float,
    *aliases: tuple[str, str],
) -> PhysicalSite:
    return PhysicalSite(
        physical_site_id=site_id,
        latitude=latitude,
        longitude=longitude,
        independence_group=group_id,
        aliases=tuple(
            StationAlias(provider, station)
            for provider, station in aliases
        ),
    )


def _registry() -> PhysicalSiteRegistry:
    return PhysicalSiteRegistry(
        sites=(
            _site(
                "physical-knyc",
                "independent-knyc",
                40.7500,
                -73.9800,
                ("metar", "KNYC"),
                ("synoptic", "KNYC"),
            ),
            _site(
                "physical-gauge-b",
                "independent-gauge-b",
                40.7600,
                -73.9700,
                ("netatmo", "gauge-b"),
            ),
        ),
        registry_version="registry-test-v1",
    )


def _policy(**changes) -> GroundFusionPolicy:
    values = {
        "freshness_seconds_by_provider": (
            ("metar", 35 * 60.0),
            ("synoptic", 20 * 60.0),
            ("netatmo", 10 * 60.0),
        ),
        "minimum_independent_wet_groups": 2,
        "spatial_tolerance_km": 0.0,
        "temporal_tolerance_seconds": 0.0,
        "minimum_interval_overlap_fraction": 0.5,
        "maximum_site_coordinate_disagreement_km": 1.0,
        "maximum_future_skew_seconds": 60.0,
    }
    values.update(changes)
    return GroundFusionPolicy(**values)


def _footprint(
    *,
    track_id: str = "cell-track-7",
    detection_id: str = "frame-20:cell-3",
    valid_from: float = OBSERVED - 150.0,
    valid_until: float = OBSERVED + 150.0,
    far_away: bool = False,
) -> CellTrackFootprint:
    if far_away:
        ring = (
            (41.50, -75.10),
            (41.60, -75.10),
            (41.60, -74.90),
            (41.50, -74.90),
        )
    else:
        ring = (
            (40.70, -74.05),
            (40.80, -74.05),
            (40.80, -73.90),
            (40.70, -73.90),
        )
    return CellTrackFootprint(
        footprint_id=f"{detection_id}:footprint",
        cell_track_id=track_id,
        cell_detection_id=detection_id,
        valid_from=valid_from,
        valid_until=valid_until,
        representative_at=(valid_from + valid_until) / 2.0,
        rings=(ring,),
        radar_source="noaa-mrms",
        raw_artifact_receipt_id="radar-receipt-20",
    )


def _provenance(provider: str, station: str) -> RawGroundProvenance:
    return RawGroundProvenance(
        provider_record_id=f"{provider}:{station}:{int(OBSERVED)}",
        parser_version=f"{provider}-parser-v2",
        raw_artifact_receipt_id=f"receipt:{provider}:{station}",
        raw_content_sha256="a" * 64,
        raw_text="-RA" if provider == "metar" else None,
    )


def _observation(
    observation_id: str,
    provider: str,
    station: str,
    *,
    state: PrecipitationState = PrecipitationState.PRESENT,
    latitude: float = 40.7500,
    longitude: float = -73.9800,
    start: float = OBSERVED,
    end: float | None = None,
    representative_at: float | None = None,
    semantics: MeasurementSemantics = (
        MeasurementSemantics.INSTANTANEOUS_STATE
    ),
    amount: float | None = None,
    unit: str | None = None,
) -> GroundObservation:
    end = start if end is None else end
    representative_at = (
        start if representative_at is None else representative_at
    )
    return GroundObservation(
        observation_id=observation_id,
        provider_id=provider,
        station_id=station,
        latitude=latitude,
        longitude=longitude,
        interval_start_at=start,
        interval_end_at=end,
        representative_at=representative_at,
        received_at=end + 15.0,
        precipitation_state=state,
        measurement_semantics=semantics,
        provenance=_provenance(provider, station),
        amount=amount,
        measurement_unit=unit,
        quality_flags=("provider-qc-passed",),
    )


def _fuse(observations, *, registry=None, footprint=None, policy=None):
    return fuse_ground_observations(
        observations,
        registry or _registry(),
        footprint or _footprint(),
        decision_key="target:cell-track-7:1800000100",
        target_id="tracker-new-york",
        region_key="region-new-york",
        evaluated_at=NOW,
        policy=policy or _policy(),
    )


def test_physical_sites_and_registry_are_immutable_and_alias_aware():
    registry = _registry()

    assert (
        registry.resolve("METAR", "knyc").physical_site_id
        == "physical-knyc"
    )
    assert (
        registry.resolve("synoptic", "KNYC").independence_group
        == "independent-knyc"
    )
    assert len(registry.registry_fingerprint) == 64
    assert registry.registry_fingerprint == _registry().registry_fingerprint
    with pytest.raises(FrozenInstanceError):
        registry.registry_version = "changed"
    with pytest.raises(FrozenInstanceError):
        registry.sites[0].latitude = 0.0


def test_alias_cannot_belong_to_two_physical_sites():
    first = _site(
        "first",
        "group-first",
        40.0,
        -74.0,
        ("metar", "KXYZ"),
    )
    second = _site(
        "second",
        "group-second",
        41.0,
        -75.0,
        ("METAR", "kxyz"),
    )

    with pytest.raises(ValueError, match="meerdere fysieke sites"):
        PhysicalSiteRegistry(
            sites=(first, second),
            registry_version="bad-registry",
        )


def test_instantaneous_wet_observation_matches_one_specific_cell():
    observation = _observation("metar-wet", "metar", "KNYC")

    assessment = assess_ground_observation(
        observation,
        _registry(),
        _footprint(),
        evaluated_at=NOW,
        policy=_policy(),
    )

    assert assessment.outcome is GroundMatchOutcome.MATCHED_WET
    assert assessment.usable is True
    assert assessment.supports_precipitation is True
    assert assessment.physical_site_id == "physical-knyc"
    assert assessment.independence_group == "independent-knyc"
    assert assessment.registry_version == "registry-test-v1"
    assert assessment.registry_fingerprint == _registry().registry_fingerprint
    assert assessment.catalog_fingerprint is None
    assert assessment.mapping_fingerprint is None
    assert assessment.distance_to_footprint_km == 0.0
    assert assessment.temporal_overlap_fraction == 1.0
    assert assessment.provenance is observation.provenance
    assert assessment.provenance.raw_text == "-RA"


def test_interval_measurement_requires_explicit_temporal_overlap_fraction():
    footprint = _footprint(
        valid_from=OBSERVED,
        valid_until=OBSERVED + 60.0,
    )
    weak_overlap = _observation(
        "one-hour-accumulation",
        "synoptic",
        "KNYC",
        start=OBSERVED - 100.0,
        end=OBSERVED + 100.0,
        representative_at=OBSERVED,
        semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
        amount=1.2,
        unit="mm",
    )
    strong_overlap = replace(
        weak_overlap,
        observation_id="short-accumulation",
        interval_start_at=OBSERVED - 10.0,
        interval_end_at=OBSERVED + 70.0,
    )

    weak = assess_ground_observation(
        weak_overlap,
        _registry(),
        footprint,
        evaluated_at=NOW,
        policy=_policy(),
    )
    strong = assess_ground_observation(
        strong_overlap,
        _registry(),
        footprint,
        evaluated_at=NOW,
        policy=_policy(),
    )

    assert weak.temporal_overlap_seconds == 60.0
    assert weak.temporal_overlap_fraction == pytest.approx(0.3)
    assert (
        weak.outcome
        is GroundMatchOutcome.INSUFFICIENT_INTERVAL_OVERLAP
    )
    assert strong.temporal_overlap_fraction == pytest.approx(0.75)
    assert strong.outcome is GroundMatchOutcome.MATCHED_WET


@pytest.mark.parametrize(
    ("observation", "footprint", "policy", "outcome"),
    (
        (
            _observation(
                "unavailable",
                "metar",
                "KNYC",
                state=PrecipitationState.UNAVAILABLE,
            ),
            _footprint(),
            _policy(),
            GroundMatchOutcome.OBSERVATION_UNAVAILABLE,
        ),
        (
            _observation(
                "unknown",
                "metar",
                "KNYC",
                state=PrecipitationState.UNKNOWN,
            ),
            _footprint(),
            _policy(),
            GroundMatchOutcome.PRECIPITATION_UNKNOWN,
        ),
        (
            _observation(
                "stale",
                "netatmo",
                "gauge-b",
                latitude=40.76,
                longitude=-73.97,
                start=OBSERVED - 1_000.0,
            ),
            _footprint(
                valid_from=OBSERVED - 1_100.0,
                valid_until=OBSERVED - 900.0,
            ),
            _policy(),
            GroundMatchOutcome.OBSERVATION_STALE,
        ),
        (
            _observation(
                "wrong-time",
                "metar",
                "KNYC",
                start=OBSERVED - 500.0,
            ),
            _footprint(),
            _policy(),
            GroundMatchOutcome.OUTSIDE_CELL_TIME,
        ),
        (
            _observation("wrong-cell", "metar", "KNYC"),
            _footprint(far_away=True),
            _policy(),
            GroundMatchOutcome.OUTSIDE_CELL_FOOTPRINT,
        ),
        (
            _observation("no-policy", "metar", "KNYC"),
            _footprint(),
            _policy(
                freshness_seconds_by_provider=(("synoptic", 1_200.0),)
            ),
            GroundMatchOutcome.FRESHNESS_POLICY_MISSING,
        ),
        (
            _observation("unknown-alias", "metar", "KZZZ"),
            _footprint(),
            _policy(),
            GroundMatchOutcome.UNKNOWN_STATION_ALIAS,
        ),
        (
            _observation(
                "future",
                "metar",
                "KNYC",
                start=NOW + 120.0,
            ),
            _footprint(
                valid_from=NOW + 100.0,
                valid_until=NOW + 140.0,
            ),
            _policy(),
            GroundMatchOutcome.OBSERVATION_FROM_FUTURE,
        ),
        (
            _observation(
                "moved-station",
                "metar",
                "KNYC",
                latitude=41.0,
                longitude=-75.0,
            ),
            _footprint(),
            _policy(),
            GroundMatchOutcome.SITE_LOCATION_CONFLICT,
        ),
    ),
)
def test_exclusion_outcomes_are_explicit_and_never_become_dry_evidence(
    observation,
    footprint,
    policy,
    outcome,
):
    assessment = assess_ground_observation(
        observation,
        _registry(),
        footprint,
        evaluated_at=NOW,
        policy=policy,
    )

    assert assessment.outcome is outcome
    assert assessment.usable is False
    assert assessment.supports_precipitation is None
    assert outcome.value in assessment.reason_codes


def test_only_excluded_rows_yield_indeterminate_not_a_negative_weather_claim():
    observations = (
        _observation(
            "unavailable",
            "metar",
            "KNYC",
            state=PrecipitationState.UNAVAILABLE,
        ),
        _observation(
            "outside",
            "netatmo",
            "gauge-b",
            latitude=40.76,
            longitude=-73.97,
        ),
    )

    decision = _fuse(observations, footprint=_footprint(far_away=True))

    assert decision.state is GroundConfirmationState.INDETERMINATE
    assert decision.confirmed is False
    assert decision.independent_dry_groups == ()
    assert "no_usable_spatiotemporal_ground_evidence" in (
        decision.reason_codes
    )
    assert all(
        contribution.role is GroundEvidenceRole.INELIGIBLE
        for contribution in decision.contributions
    )


def test_metar_and_synoptic_aliases_at_one_site_never_double_vote():
    observations = (
        _observation("metar-row", "metar", "KNYC"),
        _observation("synoptic-row", "synoptic", "KNYC"),
    )

    decision = _fuse(observations)

    assert decision.state is GroundConfirmationState.NOT_CONFIRMED
    assert decision.independent_wet_groups == ("independent-knyc",)
    roles = {item.role for item in decision.contributions}
    assert roles == {
        GroundEvidenceRole.INDEPENDENT_WET,
        GroundEvidenceRole.DUPLICATE_INDEPENDENCE_GROUP,
    }


def test_two_physical_sites_in_one_independence_group_still_count_once():
    registry = PhysicalSiteRegistry(
        sites=(
            _site(
                "site-a",
                "correlated-campus",
                40.75,
                -73.98,
                ("metar", "KAAA"),
            ),
            _site(
                "site-b",
                "correlated-campus",
                40.76,
                -73.97,
                ("synoptic", "SBBB"),
            ),
        ),
        registry_version="correlation-test",
    )
    observations = (
        _observation("a", "metar", "KAAA"),
        _observation(
            "b",
            "synoptic",
            "SBBB",
            latitude=40.76,
            longitude=-73.97,
        ),
    )

    decision = _fuse(observations, registry=registry)

    assert decision.confirmed is False
    assert decision.independent_wet_groups == ("correlated-campus",)


def test_two_independent_sites_confirm_only_the_selected_track():
    observations = (
        _observation("airport", "metar", "KNYC"),
        _observation(
            "gauge",
            "netatmo",
            "gauge-b",
            latitude=40.76,
            longitude=-73.97,
        ),
    )

    selected = _fuse(observations)
    other_cell = _fuse(
        observations,
        footprint=_footprint(
            track_id="other-cell-track",
            detection_id="frame-20:cell-99",
            far_away=True,
        ),
    )

    assert selected.state is GroundConfirmationState.CONFIRMED
    assert selected.confirmed is True
    assert selected.independent_wet_groups == (
        "independent-gauge-b",
        "independent-knyc",
    )
    assert selected.cell_track_id == "cell-track-7"
    assert other_cell.state is GroundConfirmationState.INDETERMINATE
    assert other_cell.confirmed is False
    assert all(
        assessment.outcome
        is GroundMatchOutcome.OUTSIDE_CELL_FOOTPRINT
        for assessment in other_cell.assessments
    )


def test_assessments_from_another_cell_cannot_enter_decision():
    assessment = assess_ground_observation(
        _observation("one", "metar", "KNYC"),
        _registry(),
        _footprint(track_id="cell-a", detection_id="detection-a"),
        evaluated_at=NOW,
        policy=_policy(),
    )

    with pytest.raises(ValueError, match="exact dezelfde cel"):
        decide_ground_confirmation(
            (assessment,),
            decision_key="wrong-cell",
            target_id="target",
            region_key="region",
            evaluated_at=NOW,
            footprint=_footprint(
                track_id="cell-b",
                detection_id="detection-b",
            ),
            registry=_registry(),
            policy=_policy(),
        )


def test_conflicting_wet_and_dry_aliases_are_not_arbitrarily_counted():
    observations = (
        _observation("wet", "metar", "KNYC"),
        _observation(
            "dry",
            "synoptic",
            "KNYC",
            state=PrecipitationState.ABSENT,
        ),
    )

    decision = _fuse(observations)

    assert decision.state is GroundConfirmationState.NOT_CONFIRMED
    assert decision.independent_wet_groups == ()
    assert decision.independent_dry_groups == ()
    assert decision.conflicted_groups == ("independent-knyc",)
    assert "conflicting_ground_evidence" in decision.reason_codes
    assert all(
        contribution.role
        is GroundEvidenceRole.CONFLICTED_INDEPENDENCE_GROUP
        for contribution in decision.contributions
    )


def test_interval_contract_rejects_invented_or_incoherent_semantics():
    with pytest.raises(ValueError, match="positieve duur"):
        _observation(
            "bad-interval",
            "synoptic",
            "KNYC",
            semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
        )
    with pytest.raises(ValueError, match="puntinterval"):
        _observation(
            "bad-instant",
            "metar",
            "KNYC",
            end=OBSERVED + 60.0,
            semantics=MeasurementSemantics.INSTANTANEOUS_STATE,
        )
    with pytest.raises(ValueError, match="geen hoeveelheid"):
        _observation(
            "invented-amount",
            "metar",
            "KNYC",
            state=PrecipitationState.UNKNOWN,
            amount=0.1,
            unit="mm/h",
        )


def test_structured_polygon_hole_does_not_confirm_station_inside_radar_gap():
    outer = (
        (40.70, -74.05),
        (40.80, -74.05),
        (40.80, -73.90),
        (40.70, -73.90),
    )
    hole = (
        (40.74, -73.99),
        (40.76, -73.99),
        (40.76, -73.97),
        (40.74, -73.97),
    )
    footprint = replace(
        _footprint(),
        rings=(),
        polygons=((outer, hole),),
    )

    assessment = assess_ground_observation(
        _observation("hole-site", "metar", "KNYC"),
        _registry(),
        footprint,
        evaluated_at=NOW,
        policy=_policy(),
    )

    assert assessment.outcome is GroundMatchOutcome.OUTSIDE_CELL_FOOTPRINT
    assert assessment.usable is False
    assert assessment.supports_precipitation is None
    assert assessment.distance_to_footprint_km > 0.0


def test_structured_and_legacy_multipolygons_keep_components_separate():
    far = (
        (41.50, -75.10),
        (41.60, -75.10),
        (41.60, -74.90),
        (41.50, -74.90),
    )
    near = (
        (40.70, -74.05),
        (40.80, -74.05),
        (40.80, -73.90),
        (40.70, -73.90),
    )
    structured = replace(
        _footprint(),
        rings=(),
        polygons=((far,), (near,)),
    )
    legacy = replace(_footprint(), rings=(far, near), polygons=None)
    observation = _observation("multi-site", "metar", "KNYC")

    for footprint in (structured, legacy):
        assessment = assess_ground_observation(
            observation,
            _registry(),
            footprint,
            evaluated_at=NOW,
            policy=_policy(),
        )
        assert assessment.outcome is GroundMatchOutcome.MATCHED_WET
        assert assessment.distance_to_footprint_km == 0.0


def test_structured_footprint_rejects_ambiguous_or_invalid_topology():
    outer = (
        (40.70, -74.05),
        (40.80, -74.05),
        (40.80, -73.90),
        (40.70, -73.90),
    )
    outside_hole = (
        (41.00, -75.00),
        (41.10, -75.00),
        (41.10, -74.90),
        (41.00, -74.90),
    )
    with pytest.raises(ValueError, match="topologisch ongeldig"):
        replace(
            _footprint(),
            rings=(),
            polygons=((outer, outside_hole),),
        )
    with pytest.raises(ValueError, match="exact één"):
        replace(_footprint(), polygons=((outer,),))
    with pytest.raises(ValueError, match="exact één"):
        replace(_footprint(), rings=(), polygons=())


def test_ha_and_worker_ground_fusion_mirrors_are_exact_and_shadow_only():
    root = Path(__file__).resolve().parents[1]
    ha_module = (
        root
        / "custom_components/storm_tracker_v3/engine/ground_fusion.py"
    )
    worker_module = (
        root
        / "storm_tracker_worker/tracking_core/engine/ground_fusion.py"
    )
    integration_runtime = (
        root / "custom_components/storm_tracker_v3/__init__.py"
    ).read_text(encoding="utf-8")
    worker_service = (
        root / "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")

    assert ha_module.read_bytes() == worker_module.read_bytes()
    assert "ground_fusion" not in integration_runtime
    assert "fuse_ground_observations" not in integration_runtime
    assert "ground_fusion" not in worker_service
    assert "fuse_ground_observations" not in worker_service
