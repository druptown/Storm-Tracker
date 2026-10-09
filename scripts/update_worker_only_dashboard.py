"""Create or replace a Storm Tracker dashboard through Home Assistant's WS API.

The script runs inside a Home Assistant add-on network namespace and uses only
the Supervisor token already supplied to that add-on.  It never reads or
writes Home Assistant's ``.storage`` files directly.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import struct
from typing import Any


_WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

PROVIDER_ENTITIES = [
    "sensor.storm_tracker_bron_ground_kmi_aws_10min",
    "sensor.storm_tracker_bron_ground_metar_global",
    "sensor.storm_tracker_bron_ground_netatmo_region",
    "sensor.storm_tracker_bron_ground_synoptic_region",
    "sensor.storm_tracker_bron_lightning_blitzortung_event_ingress",
    "sensor.storm_tracker_bron_lightning_eumetsat_li",
    "sensor.storm_tracker_bron_lightning_goes18_glm",
    "sensor.storm_tracker_bron_lightning_goes19_glm",
    "sensor.storm_tracker_bron_model_openmeteo_italiameteo_icon2i_region",
    "sensor.storm_tracker_bron_model_openmeteo_region",
    "sensor.storm_tracker_bron_radar_aemet_composite",
    "sensor.storm_tracker_bron_radar_dpc_sri",
    "sensor.storm_tracker_bron_radar_dwd_radolan_rv",
    "sensor.storm_tracker_bron_radar_kmi_appv4_visual",
    "sensor.storm_tracker_bron_radar_kmi_radqpe_native",
    "sensor.storm_tracker_bron_radar_knmi_rtcor",
    "sensor.storm_tracker_bron_radar_meteofrance_dpradar",
    "sensor.storm_tracker_bron_radar_metoffice_uk_rate",
    "sensor.storm_tracker_bron_radar_opera_dbzh",
    "sensor.storm_tracker_bron_radar_rainviewer_visual",
    "sensor.storm_tracker_bron_satellite_goes18_rrqpe",
    "sensor.storm_tracker_bron_satellite_goes19_rrqpe",
    "sensor.storm_tracker_bron_satellite_hsaf_h40b",
    "sensor.storm_tracker_bron_satellite_noaa_err",
    "sensor.storm_tracker_bron_validation_geosphere_location",
    "sensor.storm_tracker_bron_validation_italiameteo_dpc_sri_historical_catalog",
    "sensor.storm_tracker_bron_validation_meteolux_location",
]

PROVIDER_GROUPS = {
    "Radar": PROVIDER_ENTITIES[10:20],
    "Grondmetingen en modellen": PROVIDER_ENTITIES[0:4]
    + PROVIDER_ENTITIES[8:10],
    "Bliksem en satelliet": PROVIDER_ENTITIES[4:8]
    + PROVIDER_ENTITIES[20:24],
    "Validatie": PROVIDER_ENTITIES[24:27],
}

TARGET_ENTITIES = [
    "sensor.storm_tracker_thuis_neerslag",
    "sensor.storm_tracker_life360_wim_van_san_neerslag",
    "sensor.storm_tracker_life360_jochem_lavrysen_neerslag",
    "sensor.storm_tracker_life360_elke_lavrysen_neerslag",
    "sensor.storm_tracker_life360_manuel_van_san_neerslag",
    "sensor.storm_tracker_life360_marjolein_van_san_neerslag",
    "sensor.storm_tracker_life360_nathan_lavrysen_neerslag",
    "sensor.storm_tracker_fictieve_tracker_neerslag",
]

MAP_ENTITIES = [
    "zone.home",
    "person.wim_van_san",
    "person.jochem",
    "person.elke",
    "person.manuel",
    "person.marjolein",
    "person.nathan",
    "device_tracker.fictieve_tracker",
]


class WebSocketError(RuntimeError):
    """Raised when the bounded Home Assistant WebSocket exchange fails."""


class _WebSocket:
    def __init__(self, host: str, port: int, resource: str) -> None:
        self._socket = socket.create_connection((host, port), timeout=15)
        self._socket.settimeout(30)
        self._recv_buffer = bytearray()
        key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
        request = (
            f"GET {resource} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        ).encode("ascii")
        self._socket.sendall(request)
        response = self._read_headers()
        first_line, *header_lines = response.split("\r\n")
        if " 101 " not in first_line:
            raise WebSocketError(f"websocket_upgrade_failed:{first_line}")
        headers: dict[str, str] = {}
        for line in header_lines:
            if ":" in line:
                name, value = line.split(":", 1)
                headers[name.strip().lower()] = value.strip()
        expected = base64.b64encode(
            hashlib.sha1((key + _WEBSOCKET_GUID).encode("ascii")).digest()
        ).decode("ascii")
        if headers.get("sec-websocket-accept") != expected:
            raise WebSocketError("websocket_accept_mismatch")

    def _read_headers(self) -> str:
        raw = bytearray()
        while b"\r\n\r\n" not in raw:
            chunk = self._socket.recv(4096)
            if not chunk:
                raise WebSocketError("websocket_upgrade_eof")
            raw.extend(chunk)
            if len(raw) > 65536:
                raise WebSocketError("websocket_upgrade_headers_too_large")
        headers, remainder = bytes(raw).split(b"\r\n\r\n", 1)
        self._recv_buffer.extend(remainder)
        return headers.decode("iso-8859-1")

    def _recv_exact(self, size: int) -> bytes:
        result = bytearray()
        if self._recv_buffer:
            take = min(size, len(self._recv_buffer))
            result.extend(self._recv_buffer[:take])
            del self._recv_buffer[:take]
        while len(result) < size:
            chunk = self._socket.recv(size - len(result))
            if not chunk:
                raise WebSocketError("websocket_eof")
            result.extend(chunk)
        return bytes(result)

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        mask = secrets.token_bytes(4)
        length = len(payload)
        header = bytearray([0x80 | opcode])
        if length < 126:
            header.append(0x80 | length)
        elif length <= 0xFFFF:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        header.extend(mask)
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        self._socket.sendall(bytes(header) + masked)

    def send_json(self, value: object) -> None:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        self._send_frame(0x1, encoded)

    def recv_json(self) -> dict[str, Any]:
        fragments = bytearray()
        started = False
        while True:
            first, second = self._recv_exact(2)
            final = bool(first & 0x80)
            opcode = first & 0x0F
            masked = bool(second & 0x80)
            length = second & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._recv_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._recv_exact(8))[0]
            if length > 8 * 1024 * 1024:
                raise WebSocketError("websocket_message_too_large")
            mask = self._recv_exact(4) if masked else None
            payload = self._recv_exact(length)
            if mask is not None:
                payload = bytes(
                    value ^ mask[index % 4] for index, value in enumerate(payload)
                )
            if opcode == 0x8:
                raise WebSocketError("websocket_closed")
            if opcode == 0x9:
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode == 0x1:
                fragments = bytearray(payload)
                started = True
            elif opcode == 0x0 and started:
                fragments.extend(payload)
            else:
                raise WebSocketError(f"websocket_unexpected_opcode:{opcode}")
            if final:
                decoded = json.loads(fragments.decode("utf-8"))
                if not isinstance(decoded, dict):
                    raise WebSocketError("websocket_json_not_object")
                return decoded

    def close(self) -> None:
        try:
            self._send_frame(0x8, b"")
        except OSError:
            pass
        self._socket.close()


class _HomeAssistant:
    def __init__(self, token: str) -> None:
        self._websocket = _WebSocket("supervisor", 80, "/core/websocket")
        required = self._websocket.recv_json()
        if required.get("type") != "auth_required":
            raise WebSocketError("home_assistant_auth_not_requested")
        self._websocket.send_json({"type": "auth", "access_token": token})
        authenticated = self._websocket.recv_json()
        if authenticated.get("type") != "auth_ok":
            raise WebSocketError("home_assistant_auth_failed")
        self._next_id = 1

    def request(self, message_type: str, **values: object) -> Any:
        request_id = self._next_id
        self._next_id += 1
        self._websocket.send_json(
            {"id": request_id, "type": message_type, **values}
        )
        while True:
            response = self._websocket.recv_json()
            if response.get("id") != request_id:
                continue
            if response.get("type") != "result" or response.get("success") is not True:
                error = response.get("error")
                code = error.get("code") if isinstance(error, dict) else "unknown"
                raise WebSocketError(f"home_assistant_request_failed:{message_type}:{code}")
            return response.get("result")

    def close(self) -> None:
        self._websocket.close()


def _heading(text: str, icon: str) -> dict[str, object]:
    return {"type": "heading", "heading": text, "icon": icon}


def _tile(entity: str, name: str, icon: str) -> dict[str, object]:
    return {"type": "tile", "entity": entity, "name": name, "icon": icon}


def _provider_overview_markdown() -> str:
    providers = json.dumps(PROVIDER_ENTITIES, ensure_ascii=False)
    template = """{% set worker = 'sensor.stv3_worker_status' %}
{% set diagnostic = 'sensor.stv3_worker_technical_diagnostics' %}
{% set tracker = 'device_tracker.storm_tracker_fictieve_tracker' %}
{% set live_worker = states(worker) %}
{% set ns = namespace(ready=0, not_ready=0, actueel=0, wacht=0, geblokkeerd=0, api=0, onbekend=0) %}
{% for entity_id in __PROVIDER_ENTITIES__ %}
  {% set value = states(entity_id) %}
  {% if state_attr(entity_id, 'readiness') == 'ready' %}{% set ns.ready = ns.ready + 1 %}
  {% else %}{% set ns.not_ready = ns.not_ready + 1 %}{% endif %}
  {% if value in ['operational', 'current'] %}{% set ns.actueel = ns.actueel + 1 %}
  {% elif value == 'degraded' %}{% set ns.wacht = ns.wacht + 1 %}
  {% elif value == 'offline' %}{% set ns.geblokkeerd = ns.geblokkeerd + 1 %}
  {% elif value == 'api_required' %}{% set ns.api = ns.api + 1 %}
  {% else %}{% set ns.onbekend = ns.onbekend + 1 %}{% endif %}
{% endfor %}
### {{ '✅ Worker online' if live_worker == 'online' and state_attr(worker, 'snapshot_availability') == 'current' else '⚠️ Worker niet actueel' }}
**Workerversie:** {{ state_attr(worker, 'worker_version') or 'onbekend' }} · **Integratieversie:** {{ state_attr(diagnostic, 'integration_version') or 'onbekend' }} · **modus:** {{ state_attr(worker, 'runtime_mode') or 'worker_only' }} · **snapshot:** {{ state_attr(worker, 'snapshot_revision') or 0 }} ({{ state_attr(worker, 'snapshot_availability') or 'onbekend' }})  
**Bronnen:** {{ ns.actueel }} actueel · {{ ns.wacht }} wacht/fout · {{ ns.geblokkeerd }} geblokkeerd/offline · {{ ns.api }} API vereist · {{ ns.onbekend }} buiten bereik/onbekend  
**Gereedheid:** {{ ns.ready }} Ready · {{ ns.not_ready }} nog niet ready  
**Fictieve tracker:** {{ state_attr(tracker, 'stad') or states(tracker) }} · {{ state_attr(tracker, 'latitude') or 'onbekend' }}, {{ state_attr(tracker, 'longitude') or 'onbekend' }}  
**Celtracking:** {{ state_attr(worker, 'tracking_active_tracks') or 0 }} actief · {{ state_attr(worker, 'tracking_tracked_tracks') or 0 }} gevolgd · {{ state_attr(worker, 'tracking_motion_ready_tracks') or 0 }} bewegingsklaar · {{ state_attr(worker, 'tracking_shadow_arrival_candidates') or 0 }} doelkandidaten · {{ state_attr(worker, 'tracking_shadow_future_arrivals') or 0 }} toekomstige aankomsten  
"""
    return template.replace("__PROVIDER_ENTITIES__", providers)


def _preserve_split_dashboard(current: dict[str, Any]) -> dict[str, Any] | None:
    """Update the live split dashboard without replacing either view."""

    candidate = json.loads(json.dumps(current))
    views = candidate.get("views")
    if not isinstance(views, list):
        return None
    paths = {view.get("path") for view in views if isinstance(view, dict)}
    if not {"resultaten", "monitoring"}.issubset(paths):
        return None
    system_updated = False
    providers_updated = False
    provider_groups_updated: set[str] = set()
    legacy_group_headings = {
        "Radar": "Radar",
        "Grond, modellen en validatie": "Grondmetingen en modellen",
        "Bliksem en satelliet": "Bliksem en satelliet",
        "Validatie": "Validatie",
    }
    for view in views:
        if not isinstance(view, dict):
            continue
        if view.get("path") == "resultaten":
            for section in view.get("sections", []):
                cards = section.get("cards") if isinstance(section, dict) else None
                if not isinstance(cards, list):
                    continue
                for index, card in enumerate(cards[:-1]):
                    if (
                        isinstance(card, dict)
                        and card.get("type") == "heading"
                        and card.get("heading") == "Neerslag en waarschuwingen"
                    ):
                        precipitation = cards[index + 1]
                        if (
                            not isinstance(precipitation, dict)
                            or precipitation.get("type") != "markdown"
                        ):
                            raise WebSocketError(
                                "lovelace_split_precipitation_card_missing"
                            )
                        precipitation["content"] = _precipitation_markdown()
                        precipitation["entity_id"] = [
                            "sensor.storm_tracker_thuis_neerslag",
                            "sensor.stv3_worker_status",
                        ]
            continue
        if view.get("path") != "monitoring":
            continue
        for section in view.get("sections", []):
            cards = section.get("cards") if isinstance(section, dict) else None
            if not isinstance(cards, list):
                continue
            for index, card in enumerate(cards[:-1]):
                if not isinstance(card, dict):
                    continue
                if card.get("type") != "heading":
                    continue
                if card.get("heading") != "Systeem en verwerking":
                    continue
                status = cards[index + 1]
                if not isinstance(status, dict) or status.get("type") != "markdown":
                    raise WebSocketError("lovelace_split_status_card_missing")
                content = status.get("content")
                if not isinstance(content, str):
                    raise WebSocketError("lovelace_split_status_content_invalid")
                status["content"] = _provider_overview_markdown()
                system_updated = True
            if any(
                isinstance(card, dict)
                and card.get("type") == "heading"
                and card.get("heading") == "Alle databronnen"
                for card in cards
            ):
                section["cards"] = _provider_status_cards()
                providers_updated = True
                continue
            for index, card in enumerate(cards[:-1]):
                if not isinstance(card, dict) or card.get("type") != "heading":
                    continue
                group_name = legacy_group_headings.get(card.get("heading"))
                if group_name is None:
                    continue
                status = cards[index + 1]
                if not isinstance(status, dict) or status.get("type") != "markdown":
                    raise WebSocketError(
                        f"lovelace_split_provider_status_card_missing:{group_name}"
                    )
                entities = PROVIDER_GROUPS[group_name]
                status["content"] = _provider_group_markdown(entities)
                status["entity_id"] = entities
                provider_groups_updated.add(group_name)
    if provider_groups_updated == set(PROVIDER_GROUPS):
        providers_updated = True
    if not system_updated:
        raise WebSocketError("lovelace_split_system_section_missing")
    if not providers_updated:
        raise WebSocketError("lovelace_split_provider_section_missing")
    return candidate


def _precipitation_markdown() -> str:
    return """{% set e = 'sensor.storm_tracker_thuis_neerslag' %}
{% set worker = 'sensor.stv3_worker_status' %}
{% set live_worker = states(worker) %}
{% set reasons = state_attr(e, 'current_reason_codes') or [] %}
{% set threat_reasons = state_attr(e, 'next_threat_reason_codes') or [] %}
### Neerslag voor Thuis
**Status:** {{ states(e) | replace('_', ' ') }} · **actuele worker:** {{ live_worker | replace('_', ' ') }} · **snapshot:** {{ state_attr(e, 'snapshot_revision') or 0 }} ({{ state_attr(e, 'snapshot_availability') or 'onbekend' }})  
**Huidige neerslag:** {{ state_attr(e, 'current_precipitation_state') or 'onbekend' }}{% if state_attr(e, 'rain_rate_mm_h') is not none %} · {{ state_attr(e, 'rain_rate_mm_h') }} mm/u{% endif %}  
**Volgende dreiging:** {{ state_attr(e, 'next_threat') or 'onbekend' }}{% if state_attr(e, 'next_threat_eta_minutes') is not none %} · ETA {{ state_attr(e, 'next_threat_eta_minutes') | round(0) | int }} min{% endif %}{% if state_attr(e, 'next_threat_probability_percent') is not none %} · kans {{ state_attr(e, 'next_threat_probability_percent') }}%{% endif %}  
{% if not state_attr(e, 'current_presentation_valid') %}**Waarom nog geen waarde:** {{ reasons | join(', ') if reasons else 'nog geen gevalideerde multi-bronwaarde' }}{% endif %}  
{% if not state_attr(e, 'next_threat_presentation_valid') %}**Waarom nog geen waarschuwing:** {{ threat_reasons | join(', ') if threat_reasons else 'nog geen gevalideerde dreiging' }}{% endif %}
"""


def _entities_card(title: str, entities: list[str]) -> dict[str, object]:
    return {
        "type": "entities",
        "title": title,
        "show_header_toggle": False,
        "state_color": True,
        "entities": entities,
    }


def _provider_group_markdown(entities: list[str]) -> str:
    encoded = json.dumps(entities, ensure_ascii=False)
    template = """{% set providers = __PROVIDERS__ %}
<table>
<thead><tr><th>Actuele status</th><th>Gereedheid</th><th>Bron</th></tr></thead>
<tbody>
{% for e in providers %}
{% set readiness = state_attr(e, 'readiness') or 'not_ready' %}
{% set effective = state_attr(e, 'effective_status') or states(e) %}
{% set reasons = state_attr(e, 'reason_codes') or [] %}
{% set outside = effective == 'unknown' and 'target_outside_catalog_scope' in reasons %}
{% if effective in ['operational', 'current'] %}{% set marker = '🟢' %}{% set label = 'Operationeel' %}
{% elif effective == 'stale' %}{% set marker = '🟡' %}{% set label = 'Verouderd' %}
{% elif effective in ['degraded', 'api_required'] %}{% set marker = '🟠' %}{% set label = 'API vereist' if effective == 'api_required' else 'Herstel/fout' %}
{% elif effective == 'offline' %}{% set marker = '🔴' %}{% set label = 'Offline' %}
{% else %}{% set marker = '🔵' %}{% set label = 'Buiten bereik' if outside else 'Onbekend' %}{% endif %}
<tr><td>{{ marker }} {{ label }}</td><td><strong>{{ 'Ready' if readiness == 'ready' else 'Nog niet ready' }}</strong></td><td><strong>{{ (state_attr(e, 'friendly_name') or e) | replace('Storm Tracker bron ', '') }}</strong></td></tr>
{% endfor %}
</tbody>
</table>"""
    return template.replace("__PROVIDERS__", encoded)


def _provider_status_cards() -> list[dict[str, object]]:
    cards: list[dict[str, object]] = [
        _heading("Alle databronnen", "mdi:database-search"),
        {
            "type": "markdown",
            "content": """### Dubbele status
**Ready** betekent dat de volledige providerketen al met een geldige capture is bewezen, onafhankelijk van de huidige trackerlocatie. De gekleurde status erachter is de effectieve toestand voor de huidige locatie: 🟢 operationeel · 🟡 verouderd · 🟠 herstel/fout of API vereist · 🔴 offline · 🔵 buiten bereik/onbekend.
""",
            "grid_options": {"columns": 12},
        },
    ]
    for title, entities in PROVIDER_GROUPS.items():
        cards.append({
            "type": "markdown",
            "title": title,
            "content": _provider_group_markdown(entities),
            "entity_id": entities,
            "grid_options": {"columns": 12},
        })
    return cards


def _location_section() -> dict[str, object]:
    return {
        "type": "grid",
        "cards": [
            _heading("Doellocaties en testtracker", "mdi:map-marker-multiple"),
            {
                "type": "custom:fictieve-tracker-card",
                "title": "📍 Fictieve Tracker",
            },
            {
                "type": "map",
                "entities": MAP_ENTITIES,
                "hours_to_show": 0,
                "grid_options": {"columns": 12},
            },
        ],
    }


def build_dashboard(title: str = "Storm Tracker") -> dict[str, object]:
    target_names = [
        "Thuis",
        "Wim",
        "Jochem",
        "Elke",
        "Manuel",
        "Marjolein",
        "Nathan",
        "Fictieve tracker",
    ]
    target_tiles = [
        _tile(entity, name, "mdi:weather-rainy")
        for entity, name in zip(TARGET_ENTITIES, target_names, strict=True)
    ]
    return {
        "title": title,
        "views": [
            {
                "title": "Storm Tracker",
                "path": "storm-tracker",
                "icon": "mdi:weather-lightning-rainy",
                "type": "sections",
                "max_columns": 4,
                "sections": [
                    _location_section(),
                    {
                        "type": "grid",
                        "cards": [
                            _heading("Systeem en verwerking", "mdi:server-network"),
                            {
                                "type": "markdown",
                                "content": _provider_overview_markdown(),
                                "entity_id": [
                                    "sensor.storm_tracker_workerstatus",
                                    "sensor.stv3_worker_technical_diagnostics",
                                    *PROVIDER_ENTITIES,
                                ],
                                "grid_options": {"columns": 12},
                            },
                            _tile(
                                "sensor.storm_tracker_workerstatus",
                                "Meteorologische snapshot",
                                "mdi:database-clock",
                            ),
                            _tile(
                                "sensor.stv3_worker_technical_diagnostics",
                                "Worker en verwerking",
                                "mdi:cog-sync",
                            ),
                            _tile(
                                "sensor.storm_tracker_stormen",
                                "Stormen",
                                "mdi:weather-lightning",
                            ),
                            _tile(
                                "sensor.storm_tracker_regio_s",
                                "Regio's",
                                "mdi:map-marker-radius",
                            ),
                        ],
                    },
                    {
                        "type": "grid",
                        "cards": [
                            _heading(
                                "Neerslag en waarschuwingen",
                                "mdi:weather-pouring",
                            ),
                            {
                                "type": "markdown",
                                "content": _precipitation_markdown(),
                                "entity_id": [
                                    "sensor.storm_tracker_thuis_neerslag",
                                    "sensor.stv3_worker_technical_diagnostics",
                                ],
                                "grid_options": {"columns": 12},
                            },
                            *target_tiles,
                        ],
                    },
                    {
                        "type": "grid",
                        "cards": _provider_status_cards(),
                    },
                ],
            }
        ],
    }


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_exclusive(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            path.unlink()
        except OSError:
            pass
        raise


def _dashboard_list(api: _HomeAssistant) -> list[dict[str, Any]]:
    dashboards = api.request("lovelace/dashboards/list")
    if not isinstance(dashboards, list) or any(
        not isinstance(dashboard, dict) for dashboard in dashboards
    ):
        raise WebSocketError("lovelace_dashboard_list_invalid")
    return dashboards


def _dashboard_by_url(
    dashboards: list[dict[str, Any]], url_path: str
) -> dict[str, Any] | None:
    matches = [
        dashboard
        for dashboard in dashboards
        if dashboard.get("url_path") == url_path
    ]
    if len(matches) > 1:
        raise WebSocketError("lovelace_dashboard_url_not_unique")
    return matches[0] if matches else None


def _read_dashboard_config(api: _HomeAssistant, url_path: str) -> dict[str, Any]:
    config = api.request("lovelace/config", url_path=url_path)
    if not isinstance(config, dict):
        raise WebSocketError("lovelace_config_not_object")
    return config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-path", default="dashboard-test")
    parser.add_argument("--title", default="Storm Tracker")
    parser.add_argument("--create", action="store_true")
    parser.add_argument("--preserve-url-path")
    parser.add_argument("--backup")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.apply and not args.backup:
        parser.error("--apply vereist --backup")
    if args.preserve_url_path == args.url_path:
        parser.error("--preserve-url-path moet verschillen van --url-path")
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        raise SystemExit("SUPERVISOR_TOKEN ontbreekt")

    api = _HomeAssistant(token)
    try:
        dashboards_before = _dashboard_list(api)
        target_before = _dashboard_by_url(dashboards_before, args.url_path)
        preserved_before = (
            _read_dashboard_config(api, args.preserve_url_path)
            if args.preserve_url_path
            else None
        )
        candidate = build_dashboard(args.title)

        if args.create:
            if target_before is not None:
                raise WebSocketError("lovelace_dashboard_already_exists")
            result: dict[str, object] = {
                "url_path": args.url_path,
                "title": args.title,
                "created": False,
                "candidate_sha256": _canonical_sha256(candidate),
                "candidate_view_count": len(candidate["views"]),
                "preserved_url_path": args.preserve_url_path,
                "preserved_sha256": (
                    _canonical_sha256(preserved_before)
                    if preserved_before is not None
                    else None
                ),
                "applied": False,
            }
            if not args.apply:
                print(json.dumps(result, sort_keys=True, separators=(",", ":")))
                return

            backup = Path(args.backup)
            _write_exclusive(
                backup,
                {
                    "contract": "stv3-dashboard-create-prestate-v1",
                    "target_url_path": args.url_path,
                    "target_existed": False,
                    "preserved_url_path": args.preserve_url_path,
                    "preserved_config": preserved_before,
                    "dashboards": dashboards_before,
                },
            )
            created_id: str | None = None
            create_acknowledged = False
            try:
                created = api.request(
                    "lovelace/dashboards/create",
                    url_path=args.url_path,
                    title=args.title,
                    icon="mdi:weather-lightning-rainy",
                    show_in_sidebar=True,
                    require_admin=False,
                    mode="storage",
                )
                create_acknowledged = True
                if isinstance(created, dict) and isinstance(created.get("id"), str):
                    created_id = created["id"]
                if created_id is None:
                    discovered = _dashboard_by_url(_dashboard_list(api), args.url_path)
                    if discovered is not None and isinstance(discovered.get("id"), str):
                        created_id = discovered["id"]
                if created_id is None:
                    raise WebSocketError("lovelace_created_dashboard_id_missing")

                api.request(
                    "lovelace/config/save",
                    url_path=args.url_path,
                    config=candidate,
                )
                observed = _read_dashboard_config(api, args.url_path)
                if observed != candidate:
                    raise WebSocketError("lovelace_save_reinspection_mismatch")

                dashboards_after = _dashboard_list(api)
                target_after = _dashboard_by_url(dashboards_after, args.url_path)
                expected_metadata = {
                    "id": created_id,
                    "url_path": args.url_path,
                    "title": args.title,
                    "icon": "mdi:weather-lightning-rainy",
                    "show_in_sidebar": True,
                    "require_admin": False,
                    "mode": "storage",
                }
                if target_after is None or any(
                    target_after.get(key) != value
                    for key, value in expected_metadata.items()
                ):
                    raise WebSocketError("lovelace_created_dashboard_metadata_mismatch")
                if args.preserve_url_path:
                    preserved_after = _read_dashboard_config(
                        api, args.preserve_url_path
                    )
                    if preserved_after != preserved_before:
                        raise WebSocketError("lovelace_preserved_dashboard_changed")
            except BaseException as original_error:
                if not create_acknowledged:
                    raise WebSocketError(
                        "lovelace_creation_state_ambiguous_no_rollback"
                    ) from original_error
                rollback_id = created_id
                if rollback_id is None:
                    try:
                        discovered = _dashboard_by_url(
                            _dashboard_list(api), args.url_path
                        )
                        if discovered is not None and isinstance(
                            discovered.get("id"), str
                        ):
                            rollback_id = discovered["id"]
                    except BaseException:
                        pass
                try:
                    if rollback_id is not None:
                        api.request(
                            "lovelace/dashboards/delete",
                            dashboard_id=rollback_id,
                        )
                    if _dashboard_by_url(
                        _dashboard_list(api), args.url_path
                    ) is not None:
                        raise WebSocketError("lovelace_create_rollback_target_present")
                    if args.preserve_url_path:
                        preserved_after_rollback = _read_dashboard_config(
                            api, args.preserve_url_path
                        )
                        if preserved_after_rollback != preserved_before:
                            raise WebSocketError(
                                "lovelace_create_rollback_preserved_dashboard_changed"
                            )
                except BaseException as rollback_error:
                    raise WebSocketError(
                        "lovelace_automatic_create_rollback_failed"
                    ) from rollback_error
                raise original_error

            result["created"] = True
            result["applied"] = True
            result["dashboard_id"] = created_id
            result["observed_sha256"] = _canonical_sha256(observed)
            result["backup"] = str(backup)
            print(json.dumps(result, sort_keys=True, separators=(",", ":")))
            return

        if target_before is None:
            raise WebSocketError("lovelace_dashboard_not_found")
        current = _read_dashboard_config(api, args.url_path)
        preserved_split = _preserve_split_dashboard(current)
        if preserved_split is not None:
            candidate = preserved_split
        result: dict[str, object] = {
            "url_path": args.url_path,
            "current_sha256": _canonical_sha256(current),
            "candidate_sha256": _canonical_sha256(candidate),
            "current_view_count": len(current.get("views", [])),
            "candidate_view_count": len(candidate["views"]),
            "split_dashboard_preserved": preserved_split is not None,
            "preserved_url_path": args.preserve_url_path,
            "preserved_sha256": (
                _canonical_sha256(preserved_before)
                if preserved_before is not None
                else None
            ),
            "applied": False,
        }
        if not args.apply:
            print(json.dumps(result, sort_keys=True, separators=(",", ":")))
            return

        backup = Path(args.backup)
        _write_exclusive(backup, current)
        try:
            api.request(
                "lovelace/config/save",
                url_path=args.url_path,
                config=candidate,
            )
            observed = _read_dashboard_config(api, args.url_path)
            if observed != candidate:
                raise WebSocketError("lovelace_save_reinspection_mismatch")
            if args.preserve_url_path:
                preserved_after = _read_dashboard_config(
                    api, args.preserve_url_path
                )
                if preserved_after != preserved_before:
                    raise WebSocketError("lovelace_preserved_dashboard_changed")
        except BaseException:
            api.request(
                "lovelace/config/save",
                url_path=args.url_path,
                config=current,
            )
            restored = _read_dashboard_config(api, args.url_path)
            if restored != current:
                raise WebSocketError("lovelace_automatic_rollback_failed")
            if args.preserve_url_path:
                preserved_after_rollback = _read_dashboard_config(
                    api, args.preserve_url_path
                )
                if preserved_after_rollback != preserved_before:
                    raise WebSocketError(
                        "lovelace_update_rollback_preserved_dashboard_changed"
                    )
            raise
        result["applied"] = True
        result["observed_sha256"] = _canonical_sha256(observed)
        result["backup"] = str(backup)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    finally:
        api.close()


if __name__ == "__main__":
    main()
