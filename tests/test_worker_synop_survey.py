from datetime import datetime, timezone

from storm_tracker_worker.synop_survey_cli import (
    _land_station_ids,
    _recent_file_names,
    _station_metadata,
    run,
)


def test_recent_file_names_honours_window():
    html = """
    gda01-synop-202607281100.txt
    gda01-synop-202607281300.txt
    gda01-synop-202607281408.txt
    """
    names = _recent_file_names(
        html,
        datetime(2026, 7, 28, 14, 15, tzinfo=timezone.utc),
    )
    assert "gda01-synop-202607281100.txt" not in names
    assert "gda01-synop-202607281408.txt" in names


def test_only_aaxx_land_station_ids_are_kept():
    text = """AAXX 28121
80001 02470 81203 10268=
80002 22470 80802 10254=
BBXX
42357 28121 99301 70882=
"""
    assert _land_station_ids(text) == {"80001", "80002"}


def test_station_metadata_maps_wmo_prefix():
    history = '''"USAF","WBAN","STATION NAME","CTRY","STATE","ICAO","LAT","LON","ELEV(M)","BEGIN","END"
"800010","99999","ONE","CO","","","12.5","-81.7","5","2000","2025"
'''
    stations = _station_metadata(history, {"80001"})
    assert stations[0]["station_id"] == "80001"


def test_run_marks_two_current_synop_stations_as_usable():
    baseline = {
        "countries": [{
            "country_code": "CO",
            "selection": "niet_verifieerbaar",
            "cities": [{
                "name": "Test",
                "latitude": 12.5,
                "longitude": -81.7,
            }],
        }],
    }
    history = '''"USAF","WBAN","STATION NAME","CTRY","STATE","ICAO","LAT","LON","ELEV(M)","BEGIN","END"
"800010","99999","ONE","CO","","","12.5","-81.7","5","2000","2025"
"800020","99999","TWO","CO","","","12.6","-81.8","5","2000","2025"
'''
    result = run(
        baseline,
        index_html=(
            "gda01-synop-202607281408.txt"
        ),
        bulletins=[(
            "AAXX 28141\n"
            "80001 02470 81203=\n"
            "80002 22470 80802=\n"
        )],
        history_csv=history,
        now=datetime(2026, 7, 28, 14, 15, tzinfo=timezone.utc),
    )
    assert result["countries"][0]["selection"] == "synop"
