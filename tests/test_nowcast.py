"""Tests voor de compacte operationele neerslagstatus."""


def _add_frame(storm_module, storm, key, timestamp, *, lat=51.0, lon=4.0, dbz=35.0):
    storm.radar_cells[key] = storm_module.RadarCellSnapshot(
        cell_id=key,
        timestamp=timestamp,
        lat=lat,
        lon=lon,
        intensity=3,
        area_km2=100.0,
        max_dbz=dbz,
    )


def test_status_is_dry_without_active_systems(nowcast_module):
    result = nowcast_module.build_precipitation_status(
        [], 51.0, 5.0,
        radar_source="opera",
        pressure_trend={"trend": "stabiel", "delta_60m_hpa": -0.2},
    )

    assert result["status"] == "droog"
    assert result["active_system_count"] == 0
    assert result["radar_source"] == "opera"
    assert result["pressure_trend"] == "stabiel"


def test_single_radar_frame_is_only_observed(nowcast_module, storm_module):
    storm = storm_module.Storm(storm_id="echo", centroid_lat=51.0, centroid_lon=4.0)
    _add_frame(storm_module, storm, "one", 1_000.0, dbz=32.0)

    result = nowcast_module.build_precipitation_status([storm], 51.0, 5.0)

    assert result["status"] == "waargenomen"
    assert result["tracking_status"] == "waargenomen"
    assert result["consecutive_radar_frames"] == 1
    assert result["eta_minutes"] is None


def test_confirmed_approaching_system_gets_eta(nowcast_module, storm_module):
    storm = storm_module.Storm(
        storm_id="approaching",
        centroid_lat=51.0,
        centroid_lon=4.0,
        heading_deg=90.0,
        speed_kmh=60.0,
        confidence="Matig",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, dbz=34.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lat=51.0, lon=4.1, dbz=38.0)

    result = nowcast_module.build_precipitation_status([storm], 51.0, 5.0)

    assert result["status"] == "naderend"
    assert result["tracking_status"] == "bevestigd"
    assert result["moving_towards"] is True
    assert result["eta_minutes"] is not None
    assert result["max_dbz"] == 38.0


def test_confirmed_system_preferred_over_closer_single_echo(nowcast_module, storm_module):
    close_echo = storm_module.Storm(storm_id="close", centroid_lat=51.0, centroid_lon=4.9)
    _add_frame(storm_module, close_echo, "close-one", 1_000.0, lat=51.0, lon=4.9)

    confirmed = storm_module.Storm(storm_id="confirmed", centroid_lat=51.0, centroid_lon=4.0)
    _add_frame(storm_module, confirmed, "far-one", 1_000.0)
    _add_frame(storm_module, confirmed, "far-two", 1_300.0, lon=4.1)

    result = nowcast_module.build_precipitation_status(
        [close_echo, confirmed], 51.0, 5.0
    )

    assert result["storm_id"] == "confirmed"
    assert result["status"] == "bevestigd"
    assert result["active_system_count"] == 2
