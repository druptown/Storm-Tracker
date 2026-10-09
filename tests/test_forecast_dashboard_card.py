from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parent.parent


def test_null_eta_does_not_render_product_time_as_arrival():
    """A missing ETA must stay unknown instead of becoming zero minutes."""
    node = (
        "/home/ikke/.cache/codex-runtimes/codex-primary-runtime/"
        "dependencies/node/bin/node"
    )
    script = r"""
const fs = require('fs');
const registry = new Map();
global.HTMLElement = class {
  constructor() { this.shadowRoot = null; this.isConnected = false; }
  attachShadow() { this.shadowRoot = {innerHTML: ''}; return this.shadowRoot; }
};
global.customElements = {
  get: name => registry.get(name),
  define: (name, klass) => registry.set(name, klass),
};
global.window = {customCards: []};
eval(fs.readFileSync(process.argv[1], 'utf8'));

const Card = registry.get('storm-tracker-forecast-card');
const card = new Card();
card.setConfig({entity: 'sensor.test', target_id: 'test_tracker'});
card.hass = {states: {'sensor.test': {
  state: 'droog',
  attributes: {
    status_text: 'Geen actuele regen',
    next_threat_eta_minutes: null,
    next_threat_product_valid_at: 43200,
    next_threat_reason_codes: ['current_precipitation'],
  },
}}};
    if (!card.shadowRoot.innerHTML.includes('Aankomst</span><strong>nog niet berekend')) {
      throw new Error('missing ETA was not rendered as unknown');
    }
"""
    completed = subprocess.run(
        [node, "-e", script, str(ROOT / "dashboard" / "stv3-forecast-card.js")],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_current_rain_keeps_future_passages_as_separate_upcoming_events():
    """Current rain and later tracked cells must not share one arrival label."""
    node = (
        "/home/ikke/.cache/codex-runtimes/codex-primary-runtime/"
        "dependencies/node/bin/node"
    )
    script = r"""
const fs = require('fs');
const registry = new Map();
global.HTMLElement = class {
  constructor() { this.shadowRoot = null; this.isConnected = false; }
  attachShadow() { this.shadowRoot = {innerHTML: ''}; return this.shadowRoot; }
};
global.customElements = {
  get: name => registry.get(name),
  define: (name, klass) => registry.set(name, klass),
};
global.window = {customCards: []};
eval(fs.readFileSync(process.argv[1], 'utf8'));

const Card = registry.get('storm-tracker-forecast-card');
const card = new Card();
card.setConfig({entity: 'sensor.test', target_id: 'test_tracker'});
card._passages = {
  schema_version: 1,
  evaluated_at: 43200,
  items: [
    {target_id: 'test_tracker', cell_id: 'later-a', entry_at: 46800, exit_at: 47400,
     predicted_intensity_mm_h: 1.6, method: 'translated_footprint'},
    {target_id: 'test_tracker', cell_id: 'later-b', entry_at: 48600, exit_at: 49200,
     predicted_intensity_mm_h: 2.4, method: 'translated_footprint'},
  ],
};
card.hass = {states: {'sensor.test': {
  state: 'regen',
  attributes: {
    status_text: 'Regen gedetecteerd',
    current_precipitation_state: 'raining',
    rain_rate_mm_h: 0.8,
    next_threat_reason_codes: ['current_precipitation'],
  },
}}};
const html = card.shadowRoot.innerHTML;
if (!html.includes('Aankomst</span><strong>bezig')) {
  throw new Error('current rain was rendered as a future arrival');
}
if (!html.includes('Einde</span><strong>nog niet berekend')) {
  throw new Error('unknown end of current rain was not preserved');
}
if (!html.includes('Aankomende passages')) {
  throw new Error('future passages were discarded');
}
if (!html.includes('later-a') || !html.includes('later-b')) {
  throw new Error('multiple future passages were not rendered separately');
}
"""
    completed = subprocess.run(
        [node, "-e", script, str(ROOT / "dashboard" / "stv3-forecast-card.js")],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_schema_two_candidate_keeps_unknown_exit_and_shows_bracket_status():
    node = (
        "/home/ikke/.cache/codex-runtimes/codex-primary-runtime/"
        "dependencies/node/bin/node"
    )
    script = r"""
const fs = require('fs');
const registry = new Map();
global.HTMLElement = class { constructor(){this.shadowRoot=null;this.isConnected=false;} attachShadow(){this.shadowRoot={innerHTML:''};return this.shadowRoot;} };
global.customElements = {get:n=>registry.get(n),define:(n,k)=>registry.set(n,k)};
global.window = {customCards: []};
eval(fs.readFileSync(process.argv[1], 'utf8'));
const Card = registry.get('storm-tracker-forecast-card');
const card = new Card();
card.setConfig({entity:'sensor.test',target_id:'home'});
card._passages={schema_version:2,evaluated_at:1000,overflow_count:0,items:[{
  forecast_issue_id:'forecast-issue:a',passage_id:'passage:a',target_id:'home',
  passage_system_id:'system:a',cell_id:'cell-a',provider_id:'radar',interval_ordinal:0,
  status:'candidate',entry_at:1120,exit_at:null,
  entry_bracket:{lower_at:1100,upper_at:1140},exit_bracket:null,
  left_censored:false,right_censored:true,coverage_censored:false,
  method:'advected_labelled_qpe',predicted_mean_intensity_mm_h:2.4,
  predicted_peak_intensity_mm_h:4.8,quantity:'precipitation_rate',unit:'mm/h',
  policy_label:'provisional_unvalidated',validation_label:'unvalidated',
  scenario_outcomes:[],provenance:{algorithm_version:'scientific-passage-v1',policy_version:'candidate_policy_v1',field_fingerprint:'e'.repeat(64)}
}]};
card.hass={states:{'sensor.test':{state:'droog',attributes:{status_text:'Voorlopige passage',next_threat_reason_codes:[]}}}};
const html=card.shadowRoot.innerHTML;
if(!html.includes('voorlopig')) throw new Error('candidate status missing');
if(!html.includes('Einde</span><strong>onbekend')) throw new Error('null exit was invented');
if(!html.includes('Aankomstvenster')) throw new Error('entry bracket missing');
"""
    completed = subprocess.run(
        [node, "-e", script, str(ROOT / "dashboard" / "stv3-forecast-card.js")],
        check=False, capture_output=True, text=True,
    )
    assert completed.returncode == 0, completed.stderr
