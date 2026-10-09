from __future__ import annotations

import pytest

from storm_tracker_worker.desired_state import Target

from storm_tracker_worker.thin_fusion import fuse_observations
from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from storm_tracker_worker.thin_provider_runtime import ProviderResult
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest
from storm_tracker_worker.thin_satellite_adapters import GoesRrqpeAdapter


def _observation(
    provider_id: str,
    value: float,
    *,
    kind: ObservationKind = ObservationKind.RAIN_RATE,
    quality: ObservationQuality = ObservationQuality.GOOD,
    observed_at: float = 100.0,
    target_id: str = "home",
    metadata: dict | None = None,
) -> NormalizedObservation:
    units = {
        ObservationKind.RAIN_RATE: "mm/h",
        ObservationKind.REFLECTIVITY: "dBZ",
        ObservationKind.RAIN_PROBABILITY: "%",
        ObservationKind.PRESSURE: "hPa",
        ObservationKind.RADAR_LEVEL: "level",
    }
    return NormalizedObservation.create(
        provider_id=provider_id,
        product_id="weather",
        target_id=target_id,
        kind=kind,
        value=value,
        unit=units[kind],
        observed_at=observed_at,
        received_at=observed_at + 1.0,
        latitude=50.85,
        longitude=4.35,
        quality=quality,
        geometry={"type": "Point", "coordinates": [4.35, 50.85]},
        metadata=metadata,
    )


def _result(
    provider_id: str,
    *observations: NormalizedObservation,
    status: str = "operational",
    reason_code: str | None = None,
) -> ProviderResult:
    return ProviderResult(
        provider_id=provider_id,
        ready=True,
        status=status,
        reason_code=reason_code,
        observations=tuple(observations),
        attempted_at=100.0,
        completed_at=101.0,
    )


def test_one_bad_provider_does_not_erase_two_consistent_sources():
    cycle = fuse_observations({
        "kmi_radar": _result("kmi_radar", _observation("kmi_radar", 2.0)),
        "opera_radar": _result(
            "opera_radar", _observation("opera_radar", 2.2)
        ),
        "broken": _result("broken", status="error", reason_code="bad_payload"),
    }, evaluated_at=120.0)

    home = cycle.targets["home"]
    assert home.precipitation_rate_mm_h == pytest.approx(2.1)
    assert home.contributors == ("kmi_radar", "opera_radar")
    assert cycle.providers["broken"].status == "error"


def test_stale_source_is_rejected_without_erasing_current_source():
    cycle = fuse_observations({
        "old": _result("old", _observation("old", 9.0, observed_at=99.0)),
        "fresh": _result("fresh", _observation("fresh", 1.0, observed_at=990.0)),
    }, evaluated_at=1000.0)

    assert cycle.targets["home"].precipitation_rate_mm_h == 1.0
    assert cycle.targets["home"].contributors == ("fresh",)
    assert cycle.rejected["old"] == ("stale",)


def test_aemet_official_bundle_latency_remains_usable_for_45_minutes():
    cycle = fuse_observations({
        "aemet_radar": _result(
            "aemet_radar",
            _observation(
                "aemet_radar",
                4.0,
                kind=ObservationKind.RADAR_LEVEL,
                observed_at=100.0,
            ),
        ),
    }, evaluated_at=100.0 + 44 * 60.0, target_ids=("home",))

    assert cycle.targets["home"].radar_level == 4.0
    assert cycle.targets["home"].contributors == ("aemet_radar",)


def test_meteofrance_official_frame_remains_usable_for_25_minutes():
    cycle = fuse_observations({
        "meteofrance_radar": _result(
            "meteofrance_radar",
            _observation(
                "meteofrance_radar",
                3.0,
                observed_at=100.0,
            ),
        ),
    }, evaluated_at=100.0 + 24 * 60.0, target_ids=("home",))

    assert cycle.targets["home"].precipitation_rate_mm_h == 3.0
    assert cycle.targets["home"].contributors == ("meteofrance_radar",)


def test_quality_weight_changes_fused_value_deterministically():
    cycle = fuse_observations({
        "good": _result("good", _observation("good", 2.0)),
        "poor": _result(
            "poor",
            _observation("poor", 4.0, quality=ObservationQuality.POOR),
        ),
    }, evaluated_at=120.0)

    assert cycle.targets["home"].precipitation_rate_mm_h == pytest.approx(2.4)
    assert cycle.targets["home"].confidence == pytest.approx(0.625)


def test_extreme_disagreement_is_rejected_when_two_sources_agree():
    cycle = fuse_observations({
        "first": _result("first", _observation("first", 1.0)),
        "second": _result("second", _observation("second", 1.2)),
        "outlier": _result("outlier", _observation("outlier", 30.0)),
    }, evaluated_at=120.0)

    home = cycle.targets["home"]
    assert home.precipitation_rate_mm_h == pytest.approx(1.1)
    assert home.contributors == ("first", "second")
    assert cycle.rejected["outlier"] == ("disagreement_outlier",)


def test_no_usable_observation_is_explicitly_insufficient():
    cycle = fuse_observations({
        "broken": _result("broken", status="error", reason_code="timeout"),
    }, evaluated_at=120.0, target_ids=("home",))

    assert cycle.targets["home"].status == "onvoldoende_data"
    assert cycle.targets["home"].precipitation_rate_mm_h is None
    assert cycle.targets["home"].contributors == ()


def test_visualisation_only_radar_cannot_claim_current_rain_at_target():
    cycle = fuse_observations({
        "kmi_appv4": _result(
            "kmi_appv4",
            _observation(
                "kmi_appv4",
                3.0,
                kind=ObservationKind.RADAR_LEVEL,
                metadata={
                    "operational_use": "visualisation_only",
                    "quantitative_semantics_eligible": False,
                },
            ),
        ),
        "model": _result("model", _observation("model", 0.0)),
    }, evaluated_at=120.0, target_ids=("home",))

    home = cycle.targets["home"]
    assert home.precipitation_rate_mm_h == 0.0
    assert home.radar_level is None
    assert home.contributors == ("model",)
    assert cycle.rejected["kmi_appv4"] == ("visualisation_only",)


def test_model_guidance_rain_rate_does_not_claim_current_precipitation():
    cycle = fuse_observations({
        "open_meteo": _result(
            "open_meteo",
            _observation(
                "open_meteo",
                1.6,
                metadata={"role": "model_guidance"},
            ),
            _observation(
                "open_meteo",
                98.0,
                kind=ObservationKind.RAIN_PROBABILITY,
                metadata={"role": "model_guidance"},
            ),
        ),
    }, evaluated_at=120.0, target_ids=("home",))

    home = cycle.targets["home"]
    assert home.precipitation_rate_mm_h is None
    assert home.rain_probability_percent == 98.0
    assert home.contributors == ("open_meteo",)


def test_zero_quality_radar_echo_cannot_claim_current_precipitation():
    cycle = fuse_observations({
        "opera_radar": _result(
            "opera_radar",
            _observation(
                "opera_radar",
                37.0,
                kind=ObservationKind.REFLECTIVITY,
                quality=ObservationQuality.SUSPECT,
                metadata={"quality_index": 0.0},
            ),
        ),
    }, evaluated_at=120.0, target_ids=("home",))

    home = cycle.targets["home"]
    assert home.reflectivity_dbz is None
    assert home.contributors == ("opera_radar",)


def test_hourly_ground_pressure_remains_usable_for_two_hours():
    cycle = fuse_observations({
        "metar": _result(
            "metar",
            _observation(
                "metar",
                1008.2,
                kind=ObservationKind.PRESSURE,
                observed_at=100.0,
            ),
        ),
    }, evaluated_at=1900.0, target_ids=("home",))

    assert cycle.targets["home"].pressure_hpa == 1008.2
    assert cycle.targets["home"].contributors == ("metar",)


@pytest.mark.asyncio
async def test_quantitative_satellite_observation_enters_fusion():
    now = 1_800_000_000.0
    target = Target(
        target_id="home", display_name="Home", kind="test", available=True,
        latitude=40.0, longitude=-75.0, position_observed_at=now,
        location_accuracy_m=10.0, observation_radius_km=100.0,
        country_code="US",
    )
    adapter = GoesRrqpeAdapter(19, fetch_job=lambda job: {
        "frame_timestamp": now - 60.0,
        "rain_samples": [{
            "lat": 40.0, "lon": -75.0, "timestamp": now - 60.0,
            "rain_rate_mm_h": 3.2, "quality": 0.9,
            "source": "noaa_goes19_rrqpe",
        }],
    })
    observations = await adapter.fetch(
        ThinProviderRequest("goes19_rrqpe", "rrqpe", (target,), now),
        None,
        now=now,
    )

    cycle = fuse_observations({
        "goes19_rrqpe": ProviderResult(
            provider_id="goes19_rrqpe", ready=True, status="operational",
            reason_code=None, observations=observations,
            attempted_at=now - 1.0, completed_at=now,
        ),
    }, evaluated_at=now, target_ids=("home",))

    assert cycle.targets["home"].precipitation_rate_mm_h == pytest.approx(3.2)
    assert cycle.targets["home"].contributors == ("goes19_rrqpe",)
