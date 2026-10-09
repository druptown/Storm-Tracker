"""Storm Tracker V3 — providers/blitzortung.py v0.6.0

Provider: Blitzortung (via paho-mqtt, exact zoals bestaande HA-integratie)

Versiegeschiedenis:
  v0.6.0 — paho-mqtt MQTTv311 zonder credentials, exact zoals
            custom_components/blitzortung/mqtt.py werkt
  v0.5.0 — aiomqtt (timeout problemen)
  v0.4.0 — directe WebSocket (SSL + beleidsproblemen)
  v0.3.0 — factory-patroon
  v0.2.0 — Observation model
  v0.1.0 — state listener op HA-integratie
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Callable, Optional

from ..engine.observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

MQTT_HOST         = "blitzortung.ha.sed.pl"
MQTT_PORT         = 1883
MQTT_TOPIC        = "blitzortung/1.1/#"
RECONNECT_DELAY_S = 10
DEFAULT_KEEPALIVE = 60


class BlitzortungProvider:
    """
    Verbindt via paho-mqtt (MQTTv311) met de publieke Blitzortung-broker.
    Exact dezelfde aanpak als de bestaande homeassistant-blitzortung integratie.
    Levert alle wereldwijde LIGHTNING Observations zonder filter.
    """

    def __init__(self, on_observation: Callable[[Observation], None]) -> None:
        self._on_obs  = on_observation
        self._task:   Optional[asyncio.Task] = None
        self._running = False

    def set_callback(self, on_observation: Callable) -> None:
        self._on_obs = on_observation

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.get_event_loop().create_task(
            self._run_forever(), name="blitzortung_mqtt"
        )
        _LOGGER.info("BlitzortungProvider gestart via paho-mqtt (%s:%d)", MQTT_HOST, MQTT_PORT)

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        _LOGGER.info("BlitzortungProvider gestopt")

    async def _run_forever(self) -> None:
        while self._running:
            try:
                await self._connect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                _LOGGER.warning(
                    "BlitzortungProvider: verbinding verbroken (%s) — herverbinden in %ds",
                    e, RECONNECT_DELAY_S
                )
            if self._running:
                await asyncio.sleep(RECONNECT_DELAY_S)

    async def _connect(self) -> None:
        """Verbind via paho-mqtt en verwerk berichten in een executor thread."""
        import paho.mqtt.client as mqtt

        loop = asyncio.get_event_loop()
        connected = asyncio.Event()
        messages  = asyncio.Queue()

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                _LOGGER.debug("BlitzortungProvider: verbonden met MQTT broker")
                client.subscribe(MQTT_TOPIC, qos=0)
                loop.call_soon_threadsafe(connected.set)
            else:
                _LOGGER.warning("BlitzortungProvider: verbinding geweigerd (rc=%d)", rc)

        def on_message(client, userdata, msg):
            try:
                loop.call_soon_threadsafe(messages.put_nowait, msg.payload)
            except Exception:
                pass

        def on_disconnect(client, userdata, rc):
            _LOGGER.debug("BlitzortungProvider: verbinding verbroken (rc=%d)", rc)
            loop.call_soon_threadsafe(connected.clear)

        mqttc = mqtt.Client(protocol=mqtt.MQTTv311)
        mqttc.on_connect    = on_connect
        mqttc.on_message    = on_message
        mqttc.on_disconnect = on_disconnect

        await loop.run_in_executor(
            None, lambda: mqttc.connect(MQTT_HOST, MQTT_PORT, DEFAULT_KEEPALIVE)
        )
        mqttc.loop_start()

        try:
            await asyncio.wait_for(connected.wait(), timeout=15)
            _LOGGER.info("BlitzortungProvider: verbonden, ontvangen inslagen...")

            while self._running:
                try:
                    payload = await asyncio.wait_for(messages.get(), timeout=60)
                    self._handle_message(payload)
                except asyncio.TimeoutError:
                    # Keepalive — geen probleem
                    pass
        finally:
            mqttc.loop_stop()
            mqttc.disconnect()

    def _handle_message(self, raw: bytes) -> None:
        try:
            data = json.loads(raw)
            lat = data.get("lat")
            lon = data.get("lon")
            ts  = data.get("time")

            if lat is None or lon is None:
                return

            lat = float(lat)
            lon = float(lon)
            timestamp = float(ts) / 1e9 if ts else time.time()

            self._on_obs(Observation(
                obs_type  = ObservationType.LIGHTNING,
                lat       = lat,
                lon       = lon,
                timestamp = timestamp,
                source    = "blitzortung",
            ))
            _LOGGER.debug("Blitzortung: inslag %.3f,%.3f", lat, lon)

        except (KeyError, ValueError, json.JSONDecodeError):
            pass


class BlitzortungProviderFactory:
    """Blitzortung is wereldwijd beschikbaar — supports() geeft altijd True."""

    def supports(self, center_lat: float, center_lon: float, radius_km: float) -> bool:
        return True

    def create(self, hass, center_lat: float, center_lon: float, radius_km: float):
        return BlitzortungProvider(on_observation=lambda obs: None)
