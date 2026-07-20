import json


def test_load_places_json_accepts_list_and_skips_invalid(location_resolver_module, tmp_path):
    path = tmp_path / "places.json"
    path.write_text(json.dumps([
        ["Mechelen", "be", 51.0259, 4.4776],
        ["onvolledig"],
        ["fout", "BE", "geen-lat", 4.0],
    ]), encoding="utf-8")

    assert location_resolver_module.load_places_json(path) == (
        ("Mechelen", "BE", 51.0259, 4.4776),
    )


def test_resolve_location_uses_nearest_place_and_country(location_resolver_module):
    places = (
        ("Mechelen", "BE", 51.0259, 4.4776),
        ("Antwerpen", "BE", 51.2194, 4.4025),
    )

    result = location_resolver_module.resolve_location(51.03, 4.48, places)

    assert result.place == "Mechelen"
    assert result.country_code == "BE"
    assert result.distance_km < 1


def test_resolve_location_preserves_tracker_place_label(location_resolver_module):
    places = (("Mechelen", "BE", 51.0259, 4.4776),)

    result = location_resolver_module.resolve_location(
        51.03, 4.48, places, preferred_place="Thuis"
    )

    assert result.place == "Thuis"
    assert result.country_code == "BE"


def test_resolve_location_returns_no_country_outside_coverage(location_resolver_module):
    places = (("Mechelen", "BE", 51.0259, 4.4776),)

    result = location_resolver_module.resolve_location(25.76, -80.19, places)

    assert result.place is None
    assert result.country_code is None
