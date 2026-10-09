from __future__ import annotations

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parent.parent
CARD_PATH = ROOT / "dashboard" / "stv3-worker-technical-card.js"


def _source() -> str:
    return CARD_PATH.read_text(encoding="utf-8")


def test_card_is_standalone_and_requires_an_explicit_entity():
    source = _source()

    assert "class Stv3WorkerTechnicalCard extends HTMLElement" in source
    assert 'customElements.define("stv3-worker-technical-card"' in source
    assert 'card.type === "stv3-worker-technical-card"' in source
    assert "setConfig(config)" in source
    assert 'typeof config.entity !== "string"' in source
    assert "config.entity.trim()" in source
    assert "getStubConfig" not in source
    assert "sensor.storm_tracker_worker_technical_diagnostics" not in source
    assert "this._hass.states?.[this._config.entity]" in source


def test_card_accepts_only_the_exact_projector_contract():
    source = _source()

    assert "stv3-ha-worker-technical-diagnostics-v1" in source
    for fixed_group in (
        "attributes.worker",
        "attributes.freshness",
        "attributes.jobs",
        "attributes.storage",
        "attributes.transition",
        "attributes.replay",
        "attributes.projection_reason_codes",
    ):
        assert fixed_group in source
    for required_field in (
        "build_sha256",
        "config_sha256",
        "desired_state_sha256",
        "provider",
        "frame",
        "cache",
        "queue_depth",
        "terminal_jobs",
        "failed_jobs",
        "retention_alarm",
        "capacity_alarm",
        "headroom_bytes",
        "headroom_percent",
        "current_version",
        "next_version",
        "contract_version",
        "algorithm_version",
    ):
        assert required_field in source


def test_dynamic_values_only_enter_the_dom_through_text_content():
    source = _source()
    template_assignments = re.findall(
        r"innerHTML\s*=\s*(`(?:[^`]|\\`)*`)",
        source,
        flags=re.DOTALL,
    )

    assert len(template_assignments) == 1
    assert "${" not in template_assignments[0]
    assert ".textContent = value" in source
    assert "message.textContent" in source
    assert ".outerHTML" not in source
    assert "insertAdjacentHTML" not in source
    assert "document.write" not in source
    assert "eval(" not in source
    assert "new Function" not in source


def test_card_has_no_transport_or_secret_data_capability():
    source = _source().lower()

    forbidden_capabilities = (
        "fetch(",
        "callapi",
        "callservice",
        "callws",
        "subscribeevents",
        "websocket",
        "xmlhttprequest",
        "mqtt",
    )
    forbidden_secret_shapes = (
        "credential",
        "password",
        "token",
        "raw_payload",
        "provider_payload",
        "filesystem_path",
        "mqtt_topic",
    )
    for forbidden in forbidden_capabilities + forbidden_secret_shapes:
        assert forbidden not in source


def test_card_bounds_and_validates_every_open_collection_or_identifier():
    source = _source()

    assert ".slice(0, 16)" in source
    assert "value.length > 64" in source
    assert "[a-z][a-z0-9_.:-]{0,63}" in source
    assert "sha256:([0-9a-f]{64})" in source
    assert "value <= 1000000000" in source
    assert "value <= 31536000" in source
    assert "value <= 100" in source
    assert "Object.keys(" not in source
    assert "Object.entries(" not in source
    assert "JSON.stringify(" not in source


def test_card_makes_no_weather_admission_or_alert_claim():
    source = _source().lower()

    assert "alleen technische control-plane-diagnose" in source
    assert "geen meteorologische" in source
    assert "operationele toelating" in source
    assert "of waarschuwing" in source
    for meteorological_field in (
        "rain_rate",
        "precipitation",
        "eta_minutes",
        "distance_km",
        "probability_percent",
        "next_threat",
        "storm_id",
        "track_id",
    ):
        assert meteorological_field not in source
