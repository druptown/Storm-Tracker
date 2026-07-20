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
import math
import random
import time
from typing import Callable, Iterable, Optional

from ..engine.observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

MQTT_HOST         = "blitzortung.ha.sed.pl"
MQTT_PORT         = 1883
DEFAULT_KEEPALIVE = 60
RECONNECT_MIN_S   = 60
RECONNECT_MAX_S   = 900
GEOHASH_BASE32    = "0123456789bcdefghjkmnpqrstuvwxyz"


def _encode_geohash(lat: float, lon: float, precision: int = 3) -> str:
    """Encodeer een WGS84-punt als standaard geohash."""
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    bits = (16, 8, 4, 2, 1)
    result: list[str] = []
    bit = value = 0
    even = True
    while len(result) < precision:
        bounds = lon_range if even else lat_range
        coordinate = lon if even else lat
        midpoint = (bounds[0] + bounds[1]) / 2
        if coordinate >= midpoint:
            value |= bits[bit]
            bounds[0] = midpoint
        else:
            bounds[1] = midpoint
        even = not even
        if bit < 4:
            bit += 1
        else:
            result.append(GEOHASH_BASE32[value])
            bit = value = 0
    return "".join(result)


def geohashes_for_region(lat: float, lon: float, radius_km: float) -> set[str]:
    """Geef een conservatieve geohashdekking voor een runtime-regio."""
    radius_km = max(1.0, float(radius_km))
    lat_margin = radius_km / 110.574
    cos_lat = max(0.05, abs(math.cos(math.radians(lat))))
    lon_margin = min(180.0, radius_km / (111.320 * cos_lat))
    # Een half precision-3-vak als rasterstap voorkomt gaten aan celgrenzen.
    sample_step = 1.40625 / 2
    hashes: set[str] = set()
    sample_lat = max(-90.0, lat - lat_margin - sample_step)
    max_lat = min(90.0, lat + lat_margin + sample_step)
    while sample_lat <= max_lat + 1e-9:
        sample_lon = lon - lon_margin - sample_step
        max_lon = lon + lon_margin + sample_step
        while sample_lon <= max_lon + 1e-9:
            wrapped_lon = ((sample_lon + 180.0) % 360.0) - 180.0
            hashes.add(_encode_geohash(sample_lat, wrapped_lon, precision=3))
            sample_lon += sample_step
        sample_lat += sample_step
    return hashes


def topics_for_regions(regions: Iterable[tuple[float, float, float]]) -> set[str]:
    """Maak een gededupliceerde unie van regionale MQTT-topics."""
    hashes: set[str] = set()
    for lat, lon, radius_km in regions:
        hashes.update(geohashes_for_region(lat, lon, radius_km))
    return {f"blitzortung/1.1/{'/'.join(code)}/#" for code in hashes}


class BlitzortungProvider:
    """
    Verbindt via paho-mqtt (MQTTv311) met de publieke Blitzortung-broker.
    Exact dezelfde aanpak als de bestaande homeassistant-blitzortung integratie.
    Levert LIGHTNING-observaties voor de actieve regionale geohash-topics.
    """

    def __init__(
        self,
        on_observation: Callable[[Observation], None],
        regions: Iterable[tuple[float, float, float]] = (),
    ) -> None:
        self._on_obs  = on_observation
        self._task:   Optional[asyncio.Task] = None
        self._running = False
        self._mqttc = None
        self._connected = False
        self._topics = topics_for_regions(regions)
        self._subscribed_topics: set[str] = set()

    def set_callback(self, on_observation: Callable) -> None:
        self._on_obs = on_observation

    @property
    def connected(self) -> bool:
        """Of de publieke MQTT-feed momenteel een geldige verbinding heeft."""
        return self._connected

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
        self._connected = False
        if self._task and not self._task.done():
            self._task.cancel()
        _LOGGER.info("BlitzortungProvider gestopt")

    def update_regions(self, regions: Iterable[tuple[float, float, float]]) -> None:
        """Ververs regionale subscriptions zonder een tweede MQTT-client."""
        new_topics = topics_for_regions(regions)
        old_topics = self._topics
        self._topics = new_topics
        mqttc = self._mqttc
        if not self._connected or mqttc is None:
            return
        for topic in old_topics - new_topics:
            mqttc.unsubscribe(topic)
            self._subscribed_topics.discard(topic)
        for topic in new_topics - old_topics:
            mqttc.subscribe(topic, qos=0)
            self._subscribed_topics.add(topic)
        _LOGGER.info("BlitzortungProvider: %d regionale geohash-topics actief", len(new_topics))

    async def _run_forever(self) -> None:
        failures = 0
        while self._running:
            try:
                await self._connect()
                failures = 0
            except asyncio.CancelledError:
                break
            except Exception as err:
                failures += 1
                base_delay = min(RECONNECT_MIN_S * (2 ** (failures - 1)), RECONNECT_MAX_S)
                delay = base_delay + random.uniform(0, base_delay * 0.2)
                _LOGGER.warning(
                    "BlitzortungProvider: %s: %s; nieuwe poging over %.0fs",
                    type(err).__name__, str(err) or "geen MQTT CONNACK", delay,
                )
                if self._running:
                    await asyncio.sleep(delay)

    async def _connect(self) -> None:
        """Verbind via paho-mqtt en verwerk berichten in een executor thread."""
        import paho.mqtt.client as mqtt

        loop = asyncio.get_event_loop()
        connected = asyncio.Event()
        disconnected = asyncio.Event()
        messages  = asyncio.Queue()

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                _LOGGER.debug("BlitzortungProvider: verbonden met MQTT broker")
                self._connected = True
                self._subscribed_topics.clear()
                for topic in self._topics:
                    client.subscribe(topic, qos=0)
                    self._subscribed_topics.add(topic)
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
            self._connected = False
            loop.call_soon_threadsafe(connected.clear)
            loop.call_soon_threadsafe(disconnected.set)

        mqttc = mqtt.Client(protocol=mqtt.MQTTv311)
        mqttc.on_connect    = on_connect
        mqttc.on_message    = on_message
        mqttc.on_disconnect = on_disconnect
        self._mqttc = mqttc

        await loop.run_in_executor(
            None, lambda: mqttc.connect(MQTT_HOST, MQTT_PORT, DEFAULT_KEEPALIVE)
        )
        mqttc.loop_start()

        try:
            await asyncio.wait_for(connected.wait(), timeout=15)
            _LOGGER.info(
                "BlitzortungProvider: verbonden met %d regionale geohash-topics",
                len(self._topics),
            )

            while self._running:
                message_task = asyncio.create_task(messages.get())
                disconnect_task = asyncio.create_task(disconnected.wait())
                done, pending = await asyncio.wait(
                    {message_task, disconnect_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                if disconnect_task in done:
                    raise ConnectionError("MQTT broker verbrak de verbinding")
                self._handle_message(message_task.result())
        finally:
            self._connected = False
            self._mqttc = None
            self._subscribed_topics.clear()
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
        return BlitzortungProvider(
            on_observation=lambda obs: None,
            regions=[(center_lat, center_lon, radius_km)],
        )
