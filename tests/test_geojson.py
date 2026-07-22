from types import SimpleNamespace
import time


def _storm(
    cells=None, *, hull=None, heading=90.0, speed=60.0,
    confidence="Hoog", tracking_status="bevestigd", motion_samples=6,
    motion_history=20.0, motion_fit=0.9,
):
    return SimpleNamespace(
        storm_id="storm-a",
        centroid_lat=51.0,
        centroid_lon=4.0,
        hull=hull or [(50.9, 3.9), (51.1, 3.9), (51.0, 4.1)],
        heading_deg=heading,
        speed_kmh=speed,
        confidence=confidence,
        tracking_status=tracking_status,
        motion_sample_count=motion_samples,
        motion_history_minutes=motion_history,
        motion_fit_quality=motion_fit,
        radar_cells=cells or {},
        system_type="rain_area",
        mcs_status="not_mcs",
    )


def _region(storm):
    return SimpleNamespace(
        engine_id="region-1",
        center_lat=51.0,
        center_lon=4.0,
        observation_radius_km=300.0,
        projection_targets={"zone.home"},
        storm_engine=SimpleNamespace(get_storms=lambda: [storm]),
    )


def test_collection_contains_target_region_storm_and_motion(geojson_module):
    result = geojson_module.build_feature_collection({
        "home": {
            "name": "Thuis", "entity_id": "zone.home",
            "latitude": 51.0, "longitude": 4.0,
            "available": True, "primary": True, "radar_covered": True,
            "region_engine_id": "region-1",
        }
    }, [_region(_storm())])
    assert result["type"] == "FeatureCollection"
    layers = {feature["properties"]["layer"] for feature in result["features"]}
    assert layers == {"target", "region", "storm", "motion"}
    polygon = next(f for f in result["features"] if f["properties"]["layer"] == "storm")
    assert polygon["geometry"]["type"] == "Polygon"
    assert polygon["geometry"]["coordinates"][0][0] == [3.9, 50.9]
    assert polygon["geometry"]["coordinates"][0][-1] == [3.9, 50.9]


def test_radar_cells_are_capped_and_report_truncation(geojson_module):
    cells = {
        str(index): SimpleNamespace(
            cell_id=str(index), timestamp=1_000.0, lat=51.0, lon=4.0,
            footprint_points=(), intensity=3, max_dbz=35.0, area_km2=10.0,
        )
        for index in range(geojson_module.MAX_RADAR_CELLS + 5)
    }
    result = geojson_module.build_feature_collection({}, [_region(_storm(cells))])
    assert result["metadata"]["radar_cells_total"] == 155
    assert result["metadata"]["radar_cells_included"] == 150
    assert result["metadata"]["truncated"] is True


def test_only_latest_radar_frame_is_published(geojson_module):
    old = SimpleNamespace(
        cell_id="old", timestamp=1_000.0, lat=51.0, lon=4.0,
        footprint_points=(), intensity=3, max_dbz=35.0, area_km2=10.0,
    )
    current = SimpleNamespace(
        cell_id="current", timestamp=1_600.0, lat=51.1, lon=4.1,
        footprint_points=(), intensity=3, max_dbz=35.0, area_km2=10.0,
    )
    result = geojson_module.build_feature_collection(
        {}, [_region(_storm({"old": old, "current": current}))]
    )
    cell_ids = {
        feature["id"] for feature in result["features"]
        if feature["properties"]["layer"] == "radar_cell"
    }
    assert cell_ids == {"cell:region-1:storm-a:current"}
    assert result["metadata"]["historical_radar_cells_excluded"] == 1


def test_inactive_radar_source_cells_are_not_published(geojson_module):
    rainviewer = SimpleNamespace(
        cell_id="rainviewer:1600:51.0000:4.0000", timestamp=1_600.0,
        lat=51.0, lon=4.0, footprint_points=(), intensity=2,
        max_dbz=None, area_km2=10.0,
    )
    opera = SimpleNamespace(
        cell_id="opera:cell-a", timestamp=1_590.0,
        lat=51.1, lon=4.1, footprint_points=(), intensity=6,
        max_dbz=45.0, area_km2=20.0,
    )

    result = geojson_module.build_feature_collection(
        {},
        [_region(_storm({"rv": rainviewer, "opera": opera}))],
        active_radar_source="opera",
    )

    cell_ids = {
        feature["id"] for feature in result["features"]
        if feature["properties"]["layer"] == "radar_cell"
    }
    assert cell_ids == {"cell:region-1:storm-a:opera:cell-a"}
    assert result["metadata"]["radar_cells_total"] == 1


def test_each_region_filters_with_its_own_selected_source(geojson_module):
    def cell(source, timestamp, lat):
        return SimpleNamespace(
            cell_id=f"{source}:{timestamp}:{lat}:4.0", timestamp=timestamp,
            lat=lat, lon=4.0, footprint_points=(), intensity=3,
            max_dbz=35.0, area_km2=10.0,
        )

    italy = _region(_storm({
        "dpc": cell("dpc_radar", 1_600.0, 42.0),
        "opera": cell("opera", 1_600.0, 42.1),
    }))
    italy.engine_id = "region-it"
    belgium = _region(_storm({
        "kmi": cell("kmi", 1_600.0, 51.0),
        "opera": cell("opera", 1_600.0, 51.1),
    }))
    belgium.engine_id = "region-be"

    result = geojson_module.build_feature_collection(
        {}, [italy, belgium], active_radar_source="per_engine",
        radar_sources_by_engine={
            "region-it": {"source": "dpc_radar"},
            "region-be": {"source": "kmi"},
        },
    )
    cell_ids = {
        feature["id"] for feature in result["features"]
        if feature["properties"]["layer"] == "radar_cell"
    }
    assert cell_ids == {
        "cell:region-it:storm-a:dpc_radar:1600.0:42.0:4.0",
        "cell:region-be:storm-a:kmi:1600.0:51.0:4.0",
    }


def test_storm_and_motion_without_current_active_source_cell_are_hidden(
    geojson_module,
):
    rainviewer = SimpleNamespace(
        cell_id="rainviewer:1600:51.0000:4.0000", timestamp=1_600.0,
        lat=51.0, lon=4.0, footprint_points=(), intensity=2,
        max_dbz=None, area_km2=10.0,
    )

    result = geojson_module.build_feature_collection(
        {},
        [_region(_storm({"rv": rainviewer}))],
        active_radar_source="opera",
    )

    layers = {feature["properties"]["layer"] for feature in result["features"]}
    assert layers == {"region"}
    assert result["metadata"]["radar_cells_total"] == 0


def test_target_exposes_goes_fallback_diagnostics(geojson_module):
    target = {
        "home": {
            "latitude": 25.7617, "longitude": -80.1918,
            "region_engine_id": "region-us", "name": "Miami",
            "available": True, "radar_covered": True,
        }
    }
    result = geojson_module.build_feature_collection(
        target, [],
        radar_sources_by_engine={
            "region-us": {
                "source": "rainviewer",
                "goes_rrqpe": {
                    "supported": True, "status": "active",
                    "observations": 0, "satellites": [19],
                },
            }
        },
    )
    feature = next(item for item in result["features"] if item["id"] == "target:home")
    assert feature["properties"]["goes_rrqpe"] == {
        "supported": True, "status": "active",
        "observations": 0, "satellites": [19],
    }


def test_missing_target_coordinates_are_not_published(geojson_module):
    result = geojson_module.build_feature_collection({
        "away": {"latitude": None, "longitude": None}
    }, [])
    assert result["features"] == []


def test_unreliable_motion_vector_is_not_published(geojson_module):
    storm = _storm(
        confidence="Laag", motion_samples=3, motion_history=5.0, motion_fit=0.2
    )

    result = geojson_module.build_feature_collection({}, [_region(storm)])

    layers = [feature["properties"]["layer"] for feature in result["features"]]
    assert "storm" in layers
    assert "motion" not in layers


def test_unordered_radar_footprint_is_published_as_convex_ring(geojson_module):
    cell = SimpleNamespace(
        cell_id="cell", timestamp=1.0, lat=51.0, lon=4.0,
        footprint_points=(
            (51.1, 4.1), (50.9, 3.9), (51.1, 3.9),
            (51.0, 4.0), (50.9, 4.1),
        ),
        intensity=4, max_dbz=40.0, area_km2=20.0,
    )
    result = geojson_module.build_feature_collection(
        {}, [_region(_storm({"cell": cell}))]
    )
    feature = next(
        item for item in result["features"]
        if item["properties"]["layer"] == "radar_cell"
    )
    ring = feature["geometry"]["coordinates"][0]
    assert len(ring) == 5
    assert [4.0, 51.0] not in ring
    assert ring[0] == ring[-1]


def test_closed_source_rings_form_multipolygon_without_48_point_chords(
    geojson_module,
):
    ring = tuple(
        [(51.0, 4.0 + index * 0.001) for index in range(80)]
        + [(51.01, 4.079), (51.01, 4.0), (51.0, 4.0)]
    )
    cell = SimpleNamespace(
        cell_id="dpc_radar:1600:c1", timestamp=1_600.0,
        lat=51.005, lon=4.04, footprint_points=ring,
        intensity=4, max_dbz=None, area_km2=8.0,
    )

    result = geojson_module.build_feature_collection(
        {}, [_region(_storm({"cell": cell}))],
        active_radar_source="dpc_radar",
    )
    storm = next(
        feature for feature in result["features"]
        if feature["properties"]["layer"] == "storm"
    )
    radar = next(
        feature for feature in result["features"]
        if feature["properties"]["layer"] == "radar_cell"
    )

    assert storm["geometry"]["type"] == "MultiPolygon"
    assert len(radar["geometry"]["coordinates"][0]) == len(ring)


def test_lightning_is_a_separate_recent_point_layer(geojson_module):
    now = time.time()
    result = geojson_module.build_feature_collection(
        {}, [_region(_storm())],
        lightning_events=[{
            "lat": 51.05,
            "lon": 4.05,
            "timestamp": now - 90,
            "source": "eumetsat_li",
            "engine_ids": ["region-1"],
        }],
    )

    lightning = next(
        feature for feature in result["features"]
        if feature["properties"]["layer"] == "lightning"
    )
    assert lightning["geometry"] == {
        "type": "Point", "coordinates": [4.05, 51.05]
    }
    assert lightning["properties"]["source"] == "eumetsat_li"
    assert lightning["properties"]["engine_id"] == "region-1"
    assert 80 <= lightning["properties"]["age_seconds"] <= 100
    assert result["metadata"]["lightning_events_included"] == 1


def test_old_or_unrelated_lightning_is_not_published(geojson_module):
    now = time.time()
    result = geojson_module.build_feature_collection(
        {}, [_region(_storm())],
        lightning_events=[
            {
                "lat": 51.0, "lon": 4.0,
                "timestamp": now - geojson_module.LIGHTNING_MAX_AGE_S - 1,
                "source": "blitzortung", "engine_ids": ["region-1"],
            },
            {
                "lat": 51.0, "lon": 4.0, "timestamp": now,
                "source": "blitzortung", "engine_ids": ["removed-region"],
            },
        ],
    )

    assert not any(
        feature["properties"]["layer"] == "lightning"
        for feature in result["features"]
    )
    assert result["metadata"]["lightning_events_included"] == 0
