import ast
import importlib.util
from pathlib import Path
import subprocess
import pytest


ROOT = Path(__file__).resolve().parent.parent
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"


@pytest.mark.parametrize("empty_frame", [True, False])
@pytest.mark.parametrize("coverage", ["inside", "outside", "unknown"])
def test_dashboard_distinguishes_received_empty_radar_from_missing_frame(empty_frame, coverage):
    """Zero visible rain pixels must not be reported as no selected radar."""
    node = "/home/ikke/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node"
    script = r"""
const fs = require('fs');
const registry = new Map();
global.HTMLElement = class {};
global.customElements = {get: n => registry.get(n), define: (n,c) => registry.set(n,c)};
global.window = {customCards: []};
global.document = {createElement: () => ({style: {}})};
eval(fs.readFileSync(process.argv[1], 'utf8'));
const card = new (registry.get('stv3-multi-target-map'))();
const selected = {geometry: {coordinates: [18.4241,-33.9249]},
  properties: {layer:'target',target_id:'test_tracker',name:'Test',region_engine:'region:test'}};
selected.properties.rainviewer_nominal_coverage = {state:process.argv[3],live_operation_confirmed:false};
const emptyFrame = process.argv[2] === 'true';
card._data = {features: [selected],radar_overlays: emptyFrame ? {
  'region:test': {provider_id:'rainviewer',quantity:'visual_class',observed_at:1791365400,source_shape:[400,400],runs:[]}
} : {}};
const meta = {textContent:''};
const canvas = {clientWidth:256,clientHeight:256,replaceChildren(){},appendChild(){},setAttribute(){}};
card.shadowRoot = {querySelector: s => s === '.meta' ? meta : canvas};
card._zoom = 6;
card._distanceRings = card._targetGroups = card._lightningClusters = () => {};
card._draw(selected);
const labels = {inside:'binnen gepubliceerde dekking',outside:'buiten gepubliceerde dekking',unknown:'dekking onbekend'};
if (!meta.textContent.includes('RainViewer op doel: '+labels[process.argv[3]]))
  throw new Error('selected target nominal coverage missing: '+meta.textContent);
if (!meta.textContent.includes('geen bewijs van actuele radarwerking'))
  throw new Error('nominal coverage falsely equated to live operation');
if (emptyFrame) {
  if (meta.textContent.includes('radar: geen') || meta.textContent.includes('nog niet geselecteerd'))
    throw new Error('received empty frame incorrectly reported as absent radar: '+meta.textContent);
  if (!meta.textContent.includes('rainviewer') || !meta.textContent.includes('0 pixelruns'))
    throw new Error('empty frame provider and zero run count not exposed: '+meta.textContent);
  if (!meta.textContent.includes('geen zichtbare neerslagecho') || !meta.textContent.includes('dekking niet bevestigd'))
    throw new Error('empty frame does not distinguish echo absence from unverified coverage');
} else if (!meta.textContent.includes('radar: geen') || !meta.textContent.includes('nog niet geselecteerd')) {
  throw new Error('missing frame falsely presented as received');
}
"""
    completed = subprocess.run(
        [node, "-e", script, str(ROOT / "dashboard" / "stv3-multi-target-map.js"), str(empty_frame).lower(), coverage],
        check=False, capture_output=True, text=True,
    )
    assert completed.returncode == 0, completed.stderr


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


def test_geojson_feed_exposes_engine_raster_overlays():
    source = (COMPONENT / "http.py").read_text(encoding="utf-8")
    assert "with_radar_overlays" in source
    assert 'data.get("radar_overlays_by_engine", {})' in source


def test_geojson_feed_preserves_worker_raster_overlays_over_legacy_state():
    module_path = COMPONENT / "geojson_http_payload.py"
    spec = importlib.util.spec_from_file_location("geojson_http_payload", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    collection = {
        "type": "FeatureCollection",
        "features": [],
        "radar_overlays": {
            "region:external:test": {"runs": [{"level": 2}]},
        },
    }
    merged = module.with_radar_overlays(
        collection,
        {"region:home": {"runs": [{"level": 1}]}},
    )

    assert merged["radar_overlays"] == {
        "region:home": {"runs": [{"level": 1}]},
        "region:external:test": {"runs": [{"level": 2}]},
    }
    assert merged is not collection


def test_worker_only_geojson_endpoint_uses_retained_mqtt_collection():
    source = (COMPONENT / "http.py").read_text(encoding="utf-8")
    assert "async_refresh_worker_geojson" not in source
    assert 'data.get("geojson_worker_collection")' in source


def test_worker_only_entry_registers_authenticated_geojson_view():
    source = (COMPONENT / "__init__.py").read_text(encoding="utf-8")
    start = source.index("if mode == RUNTIME_MODE_WORKER_ONLY:")
    worker_only = source[
        start:source.index("        return True", start) + len("        return True")
    ]

    assert "from .http import StormTrackerGeoJsonView" in worker_only
    assert "hass.http.register_view(StormTrackerGeoJsonView(hass))" in worker_only


def test_worker_bridge_subscribes_to_retained_thin_geojson():
    source = (COMPONENT / "worker_bridge.py").read_text(encoding="utf-8")
    assert 'OPERATIONAL_GEOJSON_TOPIC = f"{TOPIC_ROOT}/operational/geojson"' in source
    assert 'data["geojson_worker_collection"] = collection' in source
    assert '"mode": "thin_mqtt"' in source


def test_dashboard_prefers_intensity_overlay_over_technical_contours():
    source = (ROOT / "dashboard" / "stv3-multi-target-map.js").read_text(
        encoding="utf-8"
    )
    assert "_radarOverlay(svg,overlay,center,w,h)" in source
    assert "pulsePaths" not in source
    assert "hasRadarPixels&&!this._showTechnical" in source
    assert "radarOverlay&&!this._showTechnical" not in source
    assert "radarOverlay?.runs)&&radarOverlay.runs.length>0" in source
    assert "const isClosed=ring.length===5" in source
    assert "const coordinates=isClosed?ring.slice(0,-1):ring" in source
    assert "storm-pulse" in source
    assert "_lightningClusters" in source
    assert "availableCells=visible.filter" in source
    assert "analysecellen" in source
    assert "geen echo gedetecteerd" in source
    assert "goes_rrqpe" in source
    assert "type==='MultiPolygon'" in source
    assert "Kalibratiedatabase:" not in source
    assert "total_datapoints" not in source
    assert "lightning_zone" in source
    assert "layer==='lightning_zone'" in source
    assert "f.properties.layer === 'virtual_probe'" in source
    assert "'Verkenner: '" in source
    assert "this._centerKey(f)===this._selected" in source
    assert "selected.properties.region_engine||selected.properties.engine_id" in source
    assert "img.referrerPolicy='origin'" in source
    assert "OpenStreetMap-bijdragers" in source
    assert "referrerPolicy='no-referrer'" not in source
    assert "entityId.startsWith('device_tracker.life360_')" in source
    assert "device_tracker.storm_tracker_fictieve_tracker" in source
    assert "worker_only_tracker_fallback" in source
    assert "this._trackerFallbackCollection()" in source


def test_dashboard_clears_canvas_before_each_deferred_draw():
    """Concurrent renders must not append the same live layers twice."""
    source = (ROOT / "dashboard" / "stv3-multi-target-map.js").read_text(
        encoding="utf-8"
    )

    draw_start = source.index("  _draw(selected) {")
    draw_end = source.index("\n  _radarOverlay(", draw_start)
    draw = source[draw_start:draw_end]

    assert "tiles.replaceChildren();" in draw
    assert "svg.replaceChildren();" in draw


def test_dashboard_overlays_live_test_tracker_position_without_mutating_snapshot():
    """A fresh HA tracker event must move the marker before the next worker snapshot."""
    node = (
        "/home/ikke/.cache/codex-runtimes/codex-primary-runtime/"
        "dependencies/node/bin/node"
    )
    script = r"""
const fs = require('fs');
const registry = new Map();
global.HTMLElement = class {};
global.customElements = {
  get: name => registry.get(name),
  define: (name, klass) => registry.set(name, klass),
};
global.window = {customCards: []};
eval(fs.readFileSync(process.argv[1], 'utf8'));

const Card = registry.get('stv3-multi-target-map');
const card = new Card();
card.config = {entities: ['device_tracker.storm_tracker_fictieve_tracker']};
card._hass = {states: {
  'device_tracker.storm_tracker_fictieve_tracker': {
    state: 'not_home',
    attributes: {
      latitude: 55.798049,
      longitude: 7.773256,
      friendly_name: 'Fictieve Tracker',
    },
  },
}};
const radar = {
  type: 'Feature',
  geometry: {type: 'Point', coordinates: [8.0, 56.0]},
  properties: {layer: 'radar_cell', engine_id: 'region:test'},
};
const snapshot = {
  type: 'FeatureCollection',
  features: [{
    type: 'Feature',
    geometry: {type: 'Point', coordinates: [4.936927, 54.656116]},
    properties: {
      layer: 'target',
      target_id: 'test_tracker',
      name: 'Fictieve Tracker',
      region_engine: 'region:test',
    },
  }, radar],
  radar_overlays: {'region:test': {runs: []}},
};

const result = card._overlayLiveTrackerPositions(snapshot);
const target = result.features[0];
if (JSON.stringify(target.geometry.coordinates) !== JSON.stringify([7.773256, 55.798049])) {
  throw new Error('live tracker coordinates did not replace the snapshot coordinates');
}
if (result.features[1] !== radar) {
  throw new Error('non-target weather features must remain untouched');
}
if (JSON.stringify(snapshot.features[0].geometry.coordinates) !== JSON.stringify([4.936927, 54.656116])) {
  throw new Error('the retained worker snapshot was mutated');
}
"""
    completed = subprocess.run(
        [node, "-e", script, str(ROOT / "dashboard" / "stv3-multi-target-map.js")],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_dashboard_filters_ground_stations_by_selected_target_and_zooms_to_12():
    """Station markers follow the selected target; the close view reaches zoom 12."""
    node = (
        "/home/ikke/.cache/codex-runtimes/codex-primary-runtime/"
        "dependencies/node/bin/node"
    )
    script = r"""
const fs = require('fs');
const registry = new Map();
global.HTMLElement = class {};
global.customElements = {
  get: name => registry.get(name),
  define: (name, klass) => registry.set(name, klass),
};
global.window = {customCards: []};
eval(fs.readFileSync(process.argv[1], 'utf8'));

const Card = registry.get('stv3-multi-target-map');
const card = new Card();
const station = {
  properties: {
    layer: 'ground_station',
    target_ids: ['home', 'person.family'],
  },
};
const home = {properties: {layer: 'target', target_id: 'home'}};
const other = {properties: {layer: 'target', target_id: 'person.other'}};
if (!card._stationAppliesToSelectedTarget(station, home)) {
  throw new Error('shared station was hidden from a linked target');
}
if (card._stationAppliesToSelectedTarget(station, other)) {
  throw new Error('station leaked into an unrelated target view');
}
card._zoom = 11;
card._zoomIn();
card._zoomIn();
if (card._zoom !== 12) {
  throw new Error('map did not expose exactly one extra zoom level');
}
"""
    completed = subprocess.run(
        [node, "-e", script, str(ROOT / "dashboard" / "stv3-multi-target-map.js")],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
