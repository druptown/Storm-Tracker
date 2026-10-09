"""Small regional Blitzortung MQTT ingress for the thin runtime."""

from __future__ import annotations

import logging
import math
import time
from typing import Any, Callable, Iterable

from .desired_state import Target
from .thin_provider_adapters import BlitzortungAdapter


_LOGGER = logging.getLogger(__name__)
MQTT_HOST = "blitzortung.ha.sed.pl"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def _encode_geohash(latitude: float, longitude: float, *, precision: int = 3) -> str:
    latitude_range = [-90.0, 90.0]
    longitude_range = [-180.0, 180.0]
    bits = (16, 8, 4, 2, 1)
    result: list[str] = []
    bit = value = 0
    longitude_turn = True
    while len(result) < precision:
        bounds = longitude_range if longitude_turn else latitude_range
        coordinate = longitude if longitude_turn else latitude
        midpoint = (bounds[0] + bounds[1]) / 2.0
        if coordinate >= midpoint:
            value |= bits[bit]
            bounds[0] = midpoint
        else:
            bounds[1] = midpoint
        longitude_turn = not longitude_turn
        if bit < 4:
            bit += 1
        else:
            result.append(GEOHASH_BASE32[value])
            bit = value = 0
    return "".join(result)


def _region_geohashes(target: Target) -> set[str]:
    radius_km = max(1.0, float(target.observation_radius_km))
    latitude_margin = radius_km / 110.574
    cos_latitude = max(0.05, abs(math.cos(math.radians(target.latitude))))
    longitude_margin = min(180.0, radius_km / (111.320 * cos_latitude))
    sample_step = 1.40625 / 2.0
    hashes: set[str] = set()
    latitude = max(-90.0, target.latitude - latitude_margin - sample_step)
    maximum_latitude = min(90.0, target.latitude + latitude_margin + sample_step)
    while latitude <= maximum_latitude + 1e-9:
        longitude = target.longitude - longitude_margin - sample_step
        maximum_longitude = target.longitude + longitude_margin + sample_step
        while longitude <= maximum_longitude + 1e-9:
            wrapped_longitude = ((longitude + 180.0) % 360.0) - 180.0
            hashes.add(_encode_geohash(latitude, wrapped_longitude))
            longitude += sample_step
        latitude += sample_step
    return hashes


def topics_for_targets(targets: Iterable[Target]) -> set[str]:
    hashes: set[str] = set()
    for target in targets:
        if target.available:
            hashes.update(_region_geohashes(target))
    return {f"blitzortung/1.1/{'/'.join(code)}/#" for code in hashes}


class ThinBlitzortungFeed:
    """Own one public MQTT connection and only regional subscriptions."""

    def __init__(
        self,
        *,
        client: Any,
        adapter: BlitzortungAdapter,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._adapter = adapter
        self._clock = clock
        self._topics: set[str] = set()
        self._subscribed_topics: set[str] = set()
        self._connected = False
        self._running = False

    def update_targets(self, targets: Iterable[Target]) -> None:
        new_topics = topics_for_targets(targets)
        old_topics = self._topics
        self._topics = new_topics
        if not self._connected:
            return
        for mqtt_topic in sorted(old_topics - new_topics):
            self._client.unsubscribe(mqtt_topic)
            self._subscribed_topics.discard(mqtt_topic)
        for mqtt_topic in sorted(new_topics - old_topics):
            self._client.subscribe(mqtt_topic, qos=0)
            self._subscribed_topics.add(mqtt_topic)
        _LOGGER.info("Blitzortung: %d regionale topics actief", len(new_topics))

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
        self._client.loop_start()

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._connected = False
        self._adapter.set_transport_state(False)
        self._subscribed_topics.clear()
        self._client.loop_stop()
        self._client.disconnect()

    def _on_connect(
        self,
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        try:
            successful = int(reason_code) == 0
        except (TypeError, ValueError):
            successful = str(reason_code).lower() in {"0", "success"}
        self._connected = successful
        self._adapter.set_transport_state(successful)
        if not successful:
            return
        self._subscribed_topics.clear()
        for mqtt_topic in sorted(self._topics):
            client.subscribe(mqtt_topic, qos=0)
            self._subscribed_topics.add(mqtt_topic)
        _LOGGER.info("Blitzortung verbonden met %d regionale topics", len(self._topics))

    def _on_disconnect(
        self,
        client: Any,
        userdata: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        self._connected = False
        self._adapter.set_transport_state(False)

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        self._adapter.feed_message(bytes(message.payload), received_at=float(self._clock()))


__all__ = ["ThinBlitzortungFeed", "topics_for_targets"]
