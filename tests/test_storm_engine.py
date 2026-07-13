"""Storm Tracker V3 — tests/test_storm_engine.py v0.1.0

Tests voor engine/storm_engine.py — de kern van de architectuur
(clustering, merge, lifecycle, regressie) en tot nu toe volledig
ongetest, ondanks dat dit precies de laag is waar Blokker 1's zorg
("verre cellen vullen de lokale stormlimiet") zich zou manifesteren.
"""
from __future__ import annotations

import asyncio
import time

import pytest


def _obs(observation_module, obs_type, lat, lon, ts=None, **kw):
    return observation_module.Observation(
        obs_type=obs_type, lat=lat, lon=lon,
        timestamp=ts if ts is not None else time.time(),
        **kw,
    )


# ── Clustering: nieuwe storm vs. toewijzing aan bestaande ─────────────────

def test_first_lightning_observation_creates_a_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine()
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storms = engine.get_storms()
    assert len(storms) == 1
    assert storms[0].centroid_lat == pytest.approx(51.026)
    assert storms[0].strike_count == 1


def test_nearby_lightning_joins_existing_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.030, 4.480)  # ~0.5km verderop
    asyncio.run(engine.process_batch([o1]))
    asyncio.run(engine.process_batch([o2]))

    storms = engine.get_storms()
    assert len(storms) == 1, "een strike binnen de cluster-radius moet bij dezelfde storm horen"
    assert storms[0].strike_count == 2


def test_far_lightning_creates_a_second_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)  # Heffen
    o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 41.0, 2.0)       # Barcelona
    asyncio.run(engine.process_batch([o1]))
    asyncio.run(engine.process_batch([o2]))

    storms = engine.get_storms()
    assert len(storms) == 2, "twee ver uit elkaar liggende strikes horen twee storms te worden"


def test_retain_within_removes_systems_from_previous_region(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    brest = _obs(
        observation_module,
        observation_module.ObservationType.LIGHTNING,
        48.3904,
        -4.4861,
    )
    bordeaux = _obs(
        observation_module,
        observation_module.ObservationType.LIGHTNING,
        44.8378,
        -0.5792,
    )
    asyncio.run(engine.process_batch([brest, bordeaux]))

    removed = engine.retain_within(48.3904, -4.4861, 350.0)
    storms = engine.get_storms()

    assert removed == 1
    assert len(storms) == 1
    assert storms[0].centroid_lat == pytest.approx(48.3904)
    assert set(engine._history) == {storms[0].storm_id}


# ── MAX_STORMS: bescherming tegen ruis / verre cellen (Blokker 1) ──────────

def test_max_storms_limit_is_respected(storm_engine_module, observation_module):
    """
    Dit is precies het scenario uit Blokker 1: als er meer potentiële
    storms zijn dan MAX_STORMS, mogen er nooit méér dan max_storms
    actieve storms ontstaan —ongeacht hoeveel verre cellen er nog
    binnenkomen.
    """
    engine = storm_engine_module.StormEngine(cluster_radius_km=5.0, max_storms=3)
    # 5 ver uit elkaar liggende locaties (elk >5km van de vorige) -> zouden
    # zonder de limiet 5 aparte storms opleveren
    locations = [(51.0, 4.0), (52.0, 5.0), (53.0, 6.0), (54.0, 7.0), (55.0, 8.0)]
    for lat, lon in locations:
        obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, lat, lon)
        asyncio.run(engine.process_batch([obs]))

    storms = engine.get_storms()
    assert len(storms) <= 3, f"MAX_STORMS=3 overschreden: {len(storms)} storms aangemaakt"


def test_radar_cells_also_respect_max_storms(storm_engine_module, observation_module):
    """Zelfde beschermingsprincipe, maar dan voor RADAR-observaties (zoals OPERA)."""
    engine = storm_engine_module.StormEngine(cluster_radius_km=5.0, max_storms=2)
    locations = [(51.0, 4.0), (52.0, 5.0), (53.0, 6.0), (54.0, 7.0)]
    for lat, lon in locations:
        obs = _obs(observation_module, observation_module.ObservationType.RADAR, lat, lon, intensity=5)
        asyncio.run(engine.process_batch([obs]))

    assert len(engine.get_storms()) <= 2


def test_radar_children_share_parent_weather_system(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    timestamp = time.time()
    children = [
        _obs(
            observation_module, observation_module.ObservationType.RADAR,
            48.4, -4.4, ts=timestamp, intensity=7,
            radar_cell_id="opera:frame:p0:c0",
            parent_system_id="opera:frame:p0", area_km2=400.0,
        ),
        _obs(
            observation_module, observation_module.ObservationType.RADAR,
            48.8, -1.4, ts=timestamp, intensity=8,
            radar_cell_id="opera:frame:p0:c1",
            parent_system_id="opera:frame:p0", area_km2=600.0,
        ),
    ]

    asyncio.run(engine.process_batch(children))

    storms = engine.get_storms()
    assert len(storms) == 1
    assert len(storms[0].radar_cells) == 2
    assert storms[0].source_system_ids == {"opera:frame:p0"}


def test_closest_radar_point_uses_child_footprint(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    radar = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        48.8, -3.2, intensity=8, radar_cell_id="cell-1",
        parent_system_id="parent-1",
        footprint_points=((48.4, -4.45), (48.8, -3.2)),
    )
    asyncio.run(engine.process_batch([radar]))

    distance, lat, lon = engine.get_storms()[0].closest_radar_point(
        48.3904, -4.4861
    )
    assert distance < 5.0
    assert (lat, lon) == (48.4, -4.45)


def _record_mcs_frame(
    storm, observation_module, timestamp, frame_number, *, intense=True
):
    parent_id = f"opera:{timestamp}:p0"
    footprint = ((48.5, -4.5), (48.5, -2.5), (48.7, -1.0))
    dbz_values = (55.0 if intense else 45.0, 44.0)
    for child_number, (lon, dbz) in enumerate(
        zip((-4.4, -2.7), dbz_values)
    ):
        obs = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            48.6,
            lon,
            ts=timestamp,
            intensity=8 if dbz >= 50 else 6,
            max_dbz=dbz,
            radar_cell_id=f"frame-{frame_number}-cell-{child_number}",
            parent_system_id=parent_id,
            parent_area_km2=46_627.0,
            parent_footprint_points=footprint,
        )
        storm.record_radar_cell(obs)


def test_single_mcs_shaped_frame_is_only_candidate(
    storm_module, observation_module
):
    storm = storm_module.Storm()
    _record_mcs_frame(storm, observation_module, time.time(), 0)

    storm.update_radar_classification()

    assert storm.mcs_status == "candidate"
    assert storm.system_type == "mcs_candidate"
    assert storm.mcs_duration_minutes == 0.0
    assert storm.mcs_convective_span_km >= 100.0
    assert storm.mcs_intense_cells == 1


def test_three_hours_of_qualifying_frames_confirms_mcs(
    storm_module, observation_module
):
    storm = storm_module.Storm()
    start = time.time() - 180 * 60
    for frame_number in range(37):
        _record_mcs_frame(
            storm,
            observation_module,
            start + frame_number * 5 * 60,
            frame_number,
        )

    storm.update_radar_classification()

    assert storm.mcs_status == "confirmed"
    assert storm.system_type == "mcs"
    assert storm.mcs_duration_minutes == pytest.approx(180.0)


def test_mcs_history_survives_engine_restart(
    storm_engine_module, storm_module, observation_module
):
    source = storm_engine_module.StormEngine()
    storm = storm_module.Storm(centroid_lat=48.6, centroid_lon=-3.0)
    start = time.time() - 180 * 60
    for frame_number in range(37):
        _record_mcs_frame(
            storm, observation_module, start + frame_number * 300, frame_number
        )
    storm.update_radar_classification()
    source._storms[storm.storm_id] = storm

    snapshots = source.export_mcs_history()
    restored_engine = storm_engine_module.StormEngine()
    assert restored_engine.restore_mcs_history(snapshots) == 1

    restored = restored_engine.get_storm(storm.storm_id)
    assert restored is not None
    assert restored.mcs_status == "confirmed"
    assert restored.mcs_duration_minutes == pytest.approx(180.0)
    assert len(restored.radar_system_frames) == 37


def test_large_rain_area_without_intense_convection_is_not_mcs(
    storm_module, observation_module
):
    storm = storm_module.Storm()
    _record_mcs_frame(
        storm, observation_module, time.time(), 0, intense=False
    )

    storm.update_radar_classification()

    assert storm.mcs_status == "not_mcs"
    assert storm.system_type == "convective_cluster"
    assert storm.mcs_parent_area_km2 == 46_627.0


# ── RAIN: verifieert, creëert NOOIT een nieuwe storm ────────────────────────

def test_rain_alone_never_creates_a_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine()
    rain = _obs(observation_module, observation_module.ObservationType.RAIN,
                51.026, 4.478, station_id="netatmo-1", rain_mm=2.0)
    asyncio.run(engine.process_batch([rain]))

    assert engine.get_storms() == [], "een RAIN-observatie mag NOOIT zelfstandig een storm aanmaken"


def test_rain_confirms_nearby_existing_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    lightning = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([lightning]))

    rain = _obs(observation_module, observation_module.ObservationType.RAIN,
                51.030, 4.480, station_id="netatmo-1", rain_mm=1.0)
    asyncio.run(engine.process_batch([rain]))

    storm = engine.get_storms()[0]
    assert storm.netatmo_confirmations == 1
    assert storm.netatmo_no_rain_count == 0


def test_rain_without_precipitation_increments_no_rain_count(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    lightning = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([lightning]))

    dry_station = _obs(observation_module, observation_module.ObservationType.RAIN,
                        51.030, 4.480, station_id="netatmo-2", rain_mm=0.0)
    asyncio.run(engine.process_batch([dry_station]))

    storm = engine.get_storms()[0]
    assert storm.netatmo_no_rain_count == 1
    assert storm.netatmo_confirmations == 0


# ── Merge ────────────────────────────────────────────────────────────────

def test_nearby_storms_are_merged(storm_engine_module, observation_module):
    """
    Twee storms die binnen MERGE_RADIUS_KM van elkaar komen, moeten
    samengevoegd worden zodra de merge-throttle dat toelaat.
    """
    engine = storm_engine_module.StormEngine(cluster_radius_km=5.0)
    # Twee punten >5km uit elkaar (aparte storms) maar <60km (binnen MERGE_RADIUS_KM)
    o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.00, 4.00)
    o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.30, 4.30)  # ~35km
    asyncio.run(engine.process_batch([o1]))
    asyncio.run(engine.process_batch([o2]))
    assert len(engine.get_storms()) == 2, "voorwaarde: moeten eerst 2 aparte storms zijn"

    # Forceer de merge-throttle open (normaal max 1x/15s)
    engine._last_merge_check = 0.0
    asyncio.run(engine.process_batch([
        _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.00, 4.00)
    ]))

    assert len(engine.get_storms()) == 1, "storms binnen MERGE_RADIUS_KM moeten samengevoegd worden"


def test_merge_combines_strike_counts(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=5.0)
    o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.00, 4.00)
    o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.30, 4.30)
    asyncio.run(engine.process_batch([o1]))
    asyncio.run(engine.process_batch([o2]))

    engine._last_merge_check = 0.0
    asyncio.run(engine.process_batch([
        _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.00, 4.00)
    ]))

    storm = engine.get_storms()[0]
    assert storm.strike_count == 3  # 1 + 1 + 1 (de laatste observatie telt ook mee)


# ── Lifecycle: dormant / expired ──────────────────────────────────────────

def test_storm_becomes_dormant_after_expire_minutes(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(expire_minutes=5.0, remove_minutes=15.0)
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    # Simuleer dat de storm al 10 minuten niets meer deed
    storm.last_update = time.time() - 10 * 60

    # Triggert _expire_storms() via een batch met een observatie op een andere plek
    other = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 60.0, 10.0)
    asyncio.run(engine.process_batch([other]))

    original = engine.get_storm(storm.storm_id)
    assert original is not None
    assert original.is_dormant is True


def test_storm_is_removed_after_remove_minutes(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(expire_minutes=5.0, remove_minutes=15.0)
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    storm.last_update = time.time() - 20 * 60  # ruim voorbij remove_minutes

    other = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 60.0, 10.0)
    asyncio.run(engine.process_batch([other]))

    assert engine.get_storm(storm.storm_id) is None, "storm had verwijderd moeten worden"


def test_dormant_storm_does_not_receive_new_lightning_assignment(storm_engine_module, observation_module):
    """Een sluimerende storm mag geen nieuwe strikes toegewezen krijgen (_assign_observation skipt dormant storms)."""
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0, expire_minutes=5.0)
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    storm.is_dormant = True
    storm.last_update = time.time()  # niet oud genoeg om verwijderd te worden, maar wel dormant

    # Nieuwe strike op dezelfde plek zou normaal bij deze storm horen
    new_obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.027, 4.479)
    asyncio.run(engine.process_batch([new_obs]))

    storms = engine.get_storms()
    # Een NIEUWE storm moet ontstaan zijn, de dormant storm blijft ongewijzigd op 1 strike
    assert len(storms) == 2
    dormant = engine.get_storm(storm.storm_id)
    assert dormant.strike_count == 1


# ── Regressie / beweging ────────────────────────────────────────────────

def test_movement_regression_detects_eastward_motion(storm_engine_module, observation_module):
    """
    Een storm die stap voor stap naar het oosten verschuift, moet na
    genoeg history-punten een heading rond 90 graden krijgen.
    """
    engine = storm_engine_module.StormEngine(cluster_radius_km=50.0)
    base_ts = time.time() - 300
    lon = 4.0
    for i in range(6):
        obs = _obs(
            observation_module, observation_module.ObservationType.LIGHTNING,
            51.0, lon, ts=base_ts + i * 60,
        )
        asyncio.run(engine.process_batch([obs]))
        lon += 0.05  # ~3.5km oostwaarts per stap

    storm = engine.get_storms()[0]
    assert storm.heading_deg is not None
    assert 60 < storm.heading_deg < 120, f"verwacht ~oostwaarts (90°), kreeg {storm.heading_deg}"
    assert storm.speed_kmh is not None and storm.speed_kmh > 0


def test_confidence_is_insufficient_with_too_little_history(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine()
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    assert storm.confidence == "Onvoldoende data"
    assert storm.heading_deg is None


# ── Geocoding (lazy, alleen met places-database) ──────────────────────────

def test_geocoding_skipped_without_places_database(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(places=None)
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    assert storm.location_name == ""


def test_geocoding_finds_nearest_place_when_database_provided(storm_engine_module, observation_module, geocode_module):
    places = [geocode_module.PlaceEntry("Mechelen", "BE", 51.0259, 4.4776)]
    engine = storm_engine_module.StormEngine(places=places)
    obs = _obs(observation_module, observation_module.ObservationType.RADAR, 51.026, 4.478, intensity=5)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    assert storm.location_name == "Mechelen"
