from types import SimpleNamespace


def _storm(cells=None, *, hull=None, heading=90.0, speed=60.0):
    return SimpleNamespace(
        storm_id="storm-a",
        centroid_lat=51.0,
        centroid_lon=4.0,
        hull=hull or [(50.9, 3.9), (51.1, 3.9), (51.0, 4.1)],
        heading_deg=heading,
        speed_kmh=speed,
        confidence="Hoog",
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
            cell_id=str(index), timestamp=float(index), lat=51.0, lon=4.0,
            footprint_points=(), intensity=3, max_dbz=35.0, area_km2=10.0,
        )
        for index in range(geojson_module.MAX_RADAR_CELLS + 5)
    }
    result = geojson_module.build_feature_collection({}, [_region(_storm(cells))])
    assert result["metadata"]["radar_cells_total"] == 155
    assert result["metadata"]["radar_cells_included"] == 150
    assert result["metadata"]["truncated"] is True


def test_missing_target_coordinates_are_not_published(geojson_module):
    result = geojson_module.build_feature_collection({
        "away": {"latitude": None, "longitude": None}
    }, [])
    assert result["features"] == []


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
