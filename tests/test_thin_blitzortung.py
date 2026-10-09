from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_blitzortung import ThinBlitzortungFeed
from storm_tracker_worker.thin_provider_adapters import BlitzortungAdapter, ProviderBlocked
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest


NOW = 1_789_900_000.0


def _target() -> Target:
    return Target(
        target_id="home",
        display_name="Home",
        kind="home",
        available=True,
        latitude=50.85,
        longitude=4.35,
        position_observed_at=NOW,
        location_accuracy_m=10.0,
        observation_radius_km=300.0,
        country_code="BE",
    )


class FakeClient:
    def __init__(self):
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None
        self.connected = None
        self.subscribed = []
        self.unsubscribed = []
        self.loop_started = False

    def connect_async(self, host, port, keepalive=60):
        self.connected = (host, port, keepalive)

    def loop_start(self):
        self.loop_started = True

    def loop_stop(self):
        self.loop_started = False

    def disconnect(self):
        pass

    def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))

    def unsubscribe(self, topic):
        self.unsubscribed.append(topic)


@pytest.mark.asyncio
async def test_direct_feed_subscribes_only_region_topics_and_delivers_event():
    client = FakeClient()
    adapter = BlitzortungAdapter()
    feed = ThinBlitzortungFeed(
        client=client,
        adapter=adapter,
        clock=lambda: NOW,
    )
    feed.update_targets((_target(),))

    feed.start()
    client.on_connect(client, None, None, 0)

    assert client.connected == ("blitzortung.ha.sed.pl", 1883, 60)
    assert client.subscribed
    assert all(topic.startswith("blitzortung/1.1/") for topic, _ in client.subscribed)
    assert all(topic != "blitzortung/1.1/#" for topic, _ in client.subscribed)

    client.on_message(
        client,
        None,
        SimpleNamespace(payload=json.dumps({
            "lat": 50.86,
            "lon": 4.36,
            "time": int((NOW - 1.0) * 1_000_000_000),
        }).encode()),
    )
    observations = await adapter.fetch(
        ThinProviderRequest(
            "blitzortung",
            "lightning_events",
            (_target(),),
            NOW,
        ),
        None,
        now=NOW,
    )

    assert len(observations) == 1
    assert observations[0].provider_id == "blitzortung"
    feed.stop()


@pytest.mark.asyncio
async def test_disconnected_feed_is_not_reported_operational():
    adapter = BlitzortungAdapter()

    with pytest.raises(ProviderBlocked, match="transport_unavailable"):
        await adapter.fetch(
            ThinProviderRequest(
                "blitzortung",
                "lightning_events",
                (_target(),),
                NOW,
            ),
            None,
            now=NOW,
        )
