from datetime import datetime, timezone

import pytest

from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_provider_adapters import MeteoLuxValidationAdapter, ProviderStale
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest
from storm_tracker_worker.thin_station_measurement import deduplicate_station_measurements
from storm_tracker_worker.thin_fusion import fuse_observations
from storm_tracker_worker.thin_provider_runtime import ProviderResult


TIME = datetime(2026, 10, 7, 8, 30, tzinfo=timezone.utc).timestamp()
SPECS = {
    "srr": ("mm", 0.2), "spsl": ("hPa", 1011.1),
    "st": ("degC", 15.1), "su": ("%", 68),
    "s2ff_rwymd0": ("m/s", 2.1), "s2dd_rwymd0": ("degrees", 104),
    "s2ffgust_rwymd0": ("m/s", 3.1),
}


class HvdFetcher:
    def __init__(self):
        self.payloads = {
            "hvd/stations/LUX0000001": {"data": {"id": "LUX0000001", "location": [6.23301, 49.63258]}},
            "hvd/observations": {"timestamp": "2026-10-07T08:30:00Z", "data": [
                {"id": key, "value": value} for key, (_, value) in SPECS.items()
            ]},
            "hvd/observations/metadata": {"data": [
                {"id": key, "unit": unit, "timeResolution": "PT1M",
                 "dataType": "realtime", "qualityCode": 0} for key, (unit, _) in SPECS.items()
            ]},
        }

    async def get_json(self, url, **kwargs):
        return self.payloads[url]


def request(*targets):
    return ThinProviderRequest("meteolux", "location_weather_validation", tuple(targets), TIME)


def target(name, lat=49.6116, lon=6.1319):
    return Target(target_id=name, display_name=name, kind="test", available=True,
                  latitude=lat, longitude=lon, position_observed_at=TIME,
                  location_accuracy_m=5.0, observation_radius_km=250.0, country_code="LU")


@pytest.mark.asyncio
async def test_hvd_real_station_units_windows_and_shared_measurement_identity():
    observations = await MeteoLuxValidationAdapter(endpoint="hvd/observations").fetch(
        request(target("one"), target("two")), HvdFetcher(), now=TIME + 60,
    )
    assert len(observations) == 16
    values = {o.metadata["measurement"]: o for o in observations}
    assert values["rain_rate"].value == pytest.approx(12.0)
    assert values["rain_accumulation"].value == 0.2
    assert values["rain_accumulation"].metadata["accumulation_window_seconds"] == 60
    assert values["wind_speed"].value == 2.1
    assert values["wind_speed"].metadata["averaging_window_seconds"] == 120
    assert values["pressure"].value == 1011.1
    assert values["pressure"].metadata["pressure_reference"] == "mean_sea_level_QFF"
    assert all(o.observed_at == TIME and o.latitude == 49.63258 and o.longitude == 6.23301 for o in observations)
    assert len(deduplicate_station_measurements(observations)) == 8


@pytest.mark.asyncio
async def test_hvd_does_not_attach_findel_to_a_distant_target():
    observations = await MeteoLuxValidationAdapter(endpoint="hvd/observations").fetch(
        request(target("home", 51.05, 4.42)), HvdFetcher(), now=TIME + 60,
    )
    assert observations == ()


@pytest.mark.asyncio
async def test_hvd_stale_timestamp_is_not_replaced_by_fetch_time():
    with pytest.raises(ProviderStale):
        await MeteoLuxValidationAdapter(endpoint="hvd/observations").fetch(
            request(target("lux")), HvdFetcher(), now=TIME + 3601,
        )


@pytest.mark.asyncio
async def test_hvd_rejects_unknown_units_instead_of_assuming_rain_rate():
    fetcher = HvdFetcher()
    fetcher.payloads["hvd/observations/metadata"]["data"][0]["unit"] = "mm/h"
    with pytest.raises(ValueError, match="meteolux_metadata_invalid"):
        await MeteoLuxValidationAdapter(endpoint="hvd/observations").fetch(
            request(target("lux")), fetcher, now=TIME + 60,
        )


@pytest.mark.asyncio
async def test_hvd_retained_minute_measurements_expire_before_next_slow_poll():
    observations = await MeteoLuxValidationAdapter(endpoint="hvd/observations").fetch(
        request(target("lux")), HvdFetcher(), now=TIME + 60,
    )
    result = ProviderResult(provider_id="meteolux", ready=True, status="operational",
                            reason_code=None, observations=observations,
                            attempted_at=TIME + 60, completed_at=TIME + 60)
    fusion = fuse_observations({"meteolux": result}, evaluated_at=TIME + 601, target_ids=("lux",))
    assert fusion.observations == ()
