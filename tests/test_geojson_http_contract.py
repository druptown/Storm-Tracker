import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"


def test_geojson_endpoint_requires_authentication():
    tree = ast.parse((COMPONENT / "http.py").read_text(encoding="utf-8"))
    view = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "StormTrackerGeoJsonView"
    )
    assignments = {
        target.id: ast.literal_eval(node.value)
        for node in view.body if isinstance(node, ast.Assign)
        for target in node.targets if isinstance(target, ast.Name)
        and isinstance(node.value, ast.Constant)
    }
    assert assignments["url"] == "/api/storm_tracker_v3/geojson"
    assert assignments["requires_auth"] is True


def test_sensor_does_not_record_full_geojson_payload():
    source = (COMPONENT / "sensor.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    sensor = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "StormMapGeoJsonSensor"
    )
    segment = ast.get_source_segment(source, sensor)
    assert '"endpoint": "/api/storm_tracker_v3/geojson"' in segment
    assert '"geojson": collection' not in segment
