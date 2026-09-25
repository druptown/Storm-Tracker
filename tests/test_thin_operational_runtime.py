from __future__ import annotations

import asyncio
import json
from types import MappingProxyType

import numpy as np
import pytest
from pyproj import Transformer

from storm_tracker_worker.desired_state import METHOD, parse_desired_state
from storm_tracker_worker.thin_operational_runtime import (
    ThinOperationalRuntime,
    _storm_collection,
)
from storm_tracker_worker.thin_nowcast import StormCell
from storm_tracker_worker.thin_postgis import PostGISWriteResult
from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from storm_tracker_worker.thin_provider_runtime import ProviderResult
from storm_tracker_worker.thin_provider_adapters import ThinRadarField
from storm_tracker_worker.thin_radar_tracking import RadarTrack
from storm_tracker_worker.thin_regional_radar_fusion import (
    FusedRegionalRadarField,
    RegionalFusionSummary,
)
from storm_tracker_worker.tracking_core.operational_snapshot import validate_snapshot


def _state(
    *,
    epoch=1,
    generation=1,
    previous=None,
    issued_at=100.0,
    target_id="home",
    display_name="Thuis",
    location_accuracy_m=10.0,
):
    document = {
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha-nas",
        "authority_epoch": epoch,
        "generation": generation,
        "issued_at": issued_at,
        "previous_state_sha256": previous,
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": display_name,
            "kind": "person",
            "latitude": 50.85,
            "location_accuracy_m": location_accuracy_m,
            "longitude": 4.35,
            "observation_radius_km": 50.0,
            "position_observed_at": 100.0,
            "target_id": target_id,
        }],
        "settings": {
            "ground_radius_km": 50.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    }
    return parse_desired_state(document).as_dict()


def _shared_home_state():
    document = {
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha-nas",
        "authority_epoch": 1,
        "generation": 1,
        "issued_at": 100.0,
        "previous_state_sha256": None,
        "targets": [
            {
                "available": True,
                "country_code": "BE",
                "display_name": "Thuis",
                "kind": "home",
                "latitude": 51.05,
                "location_accuracy_m": 10.0,
                "longitude": 4.42,
                "observation_radius_km": 300.0,
                "position_observed_at": 100.0,
                "target_id": "home",
            },
            {
                "available": True,
                "country_code": "BE",
                "display_name": "Gezinslid",
                "kind": "person",
                "latitude": 51.10,
                "location_accuracy_m": 10.0,
                "longitude": 4.42,
                "observation_radius_km": 300.0,
                "position_observed_at": 100.0,
                "target_id": "person.family",
            },
        ],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
    }
    return parse_desired_state(document).as_dict()


def _home_state():
    document = _shared_home_state()
    document["targets"] = document["targets"][:1]
    return parse_desired_state(document).as_dict()


def _shared_home_state_with_unavailable_person(previous: str):
    document = _shared_home_state()
    document["generation"] = 2
    document["issued_at"] = 200.0
    document["previous_state_sha256"] = previous
    document["targets"][0]["position_observed_at"] = 200.0
    person = document["targets"][1]
    person["available"] = False
    person["latitude"] = 0.0
    person["longitude"] = 0.0
    person["position_observed_at"] = None
    return parse_desired_state(document).as_dict()


class EmptyProviders:
    def __init__(self):
        self.requests = []

    async def fetch(self, requests):
        self.requests.append(requests)
        return {
            request.provider_id: ProviderResult(
                provider_id=request.provider_id,
                ready=True,
                status="empty",
                reason_code="no_observations",
                observations=(),
                attempted_at=request.requested_at,
                completed_at=request.requested_at,
            )
            for request in requests
        }


class ReadyAemetProviders(EmptyProviders):
    ready_provider_ids = frozenset({"aemet_radar"})


class ReadyMeteoFranceProviders(EmptyProviders):
    ready_provider_ids = frozenset({"meteofrance_radar"})


class ReadyDpcProviders(EmptyProviders):
    ready_provider_ids = frozenset({"dpc_radar"})


class ReadyMetOfficeProviders(EmptyProviders):
    ready_provider_ids = frozenset({"metoffice_radar"})


class ReadyRainViewerProviders(EmptyProviders):
    ready_provider_ids = frozenset({"rainviewer"})


class ReadyKmiAwsProviders(EmptyProviders):
    ready_provider_ids = frozenset({"kmi_aws"})


class ReadyItaliaMeteoProviders(EmptyProviders):
    ready_provider_ids = frozenset({"italiameteo_icon2i"})


class ReadyGeoSphereProviders(EmptyProviders):
    ready_provider_ids = frozenset({"geosphere"})


class ReadyItaliaMeteoCatalogProviders(EmptyProviders):
    ready_provider_ids = frozenset({"italiameteo_dpc_catalog"})


class ReadyMeteoLuxProviders(EmptyProviders):
    ready_provider_ids = frozenset({"meteolux"})


class StaleOperaProviders(EmptyProviders):
    ready_provider_ids = frozenset({"opera_radar"})

    async def fetch(self, requests):
        return {
            "opera_radar": ProviderResult(
                provider_id="opera_radar",
                ready=True,
                status="stale",
                reason_code="product_stale",
                observations=(),
                attempted_at=100.0,
                completed_at=100.0,
            )
        }


@pytest.mark.asyncio
async def test_ready_provider_outside_current_geography_keeps_readiness():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyAemetProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    aemet = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"] == "radar.aemet.composite"
    )
    assert aemet["readiness"] == "ready"
    assert aemet["state"] == "unknown"
    assert aemet["reason_codes"] == ["target_outside_catalog_scope"]


@pytest.mark.asyncio
async def test_stale_opera_product_is_waiting_not_a_provider_failure():
    runtime = ThinOperationalRuntime(
        provider_runtime=StaleOperaProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    opera = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"] == "radar.opera.dbzh"
    )
    assert opera["readiness"] == "ready"
    assert opera["state"] == "stale"
    assert opera["freshness"] == "stale"
    assert opera["error"] is None
    assert opera["reason_codes"] == ["product_stale"]


@pytest.mark.asyncio
async def test_kmi_aws_runtime_id_maps_to_ground_dashboard_provider_id():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyKmiAwsProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    provider = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"] == "ground.kmi.aws_10min"
    )
    assert provider["readiness"] == "ready"
    assert provider["state"] == "operational"


@pytest.mark.asyncio
async def test_italiameteo_runtime_id_maps_to_model_dashboard_provider_id():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyItaliaMeteoProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    provider = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"]
        == "model.openmeteo.italiameteo_icon2i.region"
    )
    assert provider["readiness"] == "ready"
    assert provider["state"] == "unknown"
    assert provider["reason_codes"] == ["target_outside_catalog_scope"]


@pytest.mark.asyncio
async def test_geosphere_runtime_id_maps_to_validation_dashboard_provider_id():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyGeoSphereProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    provider = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"] == "validation.geosphere.location"
    )
    assert provider["readiness"] == "ready"
    assert provider["state"] == "unknown"
    assert provider["reason_codes"] == ["target_outside_catalog_scope"]


@pytest.mark.asyncio
async def test_italiameteo_catalog_runtime_id_maps_to_validation_dashboard_provider_id():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyItaliaMeteoCatalogProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    provider = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"]
        == "validation.italiameteo.dpc_sri_historical_catalog"
    )
    assert provider["readiness"] == "ready"
    assert provider["state"] == "unknown"
    assert provider["reason_codes"] == ["target_outside_catalog_scope"]


@pytest.mark.asyncio
async def test_meteolux_runtime_id_maps_to_validation_dashboard_provider_id():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyMeteoLuxProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    provider = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"] == "validation.meteolux.location"
    )
    assert provider["readiness"] == "ready"
    assert provider["state"] == "unknown"
    assert provider["reason_codes"] == ["target_outside_catalog_scope"]


@pytest.mark.asyncio
async def test_meteofrance_runtime_id_maps_to_dashboard_provider_id():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyMeteoFranceProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    provider = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"] == "radar.meteofrance.dpradar"
    )
    assert provider["readiness"] == "ready"
    assert provider["state"] == "operational"


@pytest.mark.asyncio
async def test_dpc_runtime_id_maps_to_dashboard_provider_id():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyDpcProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    provider = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"] == "radar.dpc.sri"
    )
    assert provider["readiness"] == "ready"
    assert provider["state"] == "unknown"
    assert provider["reason_codes"] == ["target_outside_catalog_scope"]


@pytest.mark.asyncio
async def test_metoffice_runtime_id_maps_to_dashboard_provider_id():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyMetOfficeProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    provider = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"] == "radar.metoffice.uk_rate"
    )
    assert provider["readiness"] == "ready"
    assert provider["state"] == "unknown"
    assert provider["reason_codes"] == ["target_outside_catalog_scope"]


@pytest.mark.asyncio
async def test_rainviewer_runtime_id_maps_to_dashboard_provider_id():
    runtime = ThinOperationalRuntime(
        provider_runtime=ReadyRainViewerProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    provider = next(
        item for item in result.snapshot["providers"]["items"]
        if item["provider_id"] == "radar.rainviewer.visual"
    )
    assert provider["readiness"] == "ready"
    assert provider["state"] == "operational"


@pytest.mark.asyncio
async def test_native_tracks_survive_cycles_without_a_new_provider_result():
    import numpy as np
    from pyproj import Transformer
    from storm_tracker_worker.thin_provider_adapters import ThinRadarField

    clock = [100.0]
    x, y = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform(4.35, 50.85)

    class RadarProviders:
        async def fetch(self, requests):
            if clock[0] not in (100.0, 400.0):
                return {}
            rates = np.zeros((16, 16), dtype=float)
            column = 4 if clock[0] == 100.0 else 6
            rates[6:10, column:column + 4] = 5.0
            field = ThinRadarField(
                "dwd_radolan", "radolan_rw", clock[0], "EPSG:3857",
                x - 8000.0, y + 8000.0, 1000.0, 1000.0, rates,
                np.ones((16, 16), dtype=bool),
            )
            observation = NormalizedObservation.create(
                provider_id="dwd_radolan", product_id="radolan_rw", target_id="home",
                kind=ObservationKind.RAIN_RATE, value=5.0, unit="mm/h",
                observed_at=clock[0], received_at=clock[0], latitude=50.85, longitude=4.35,
                quality=ObservationQuality.GOOD,
                geometry={"type": "Point", "coordinates": [4.35, 50.85]},
            )
            return {"dwd_radolan": ProviderResult(
                "dwd_radolan", True, "operational", None, (observation,), clock[0], clock[0], (field,),
            )}

    runtime = ThinOperationalRuntime(provider_runtime=RadarProviders(), clock=lambda: clock[0])
    runtime.apply_desired_state(_state())
    await runtime.run_cycle()
    clock[0] = 400.0
    moving = await runtime.run_cycle()
    assert moving.nowcast.storm_cells
    assert moving.nowcast.storm_cells[0].speed_kmh > 0.0
    for instant in (402.0, 404.0, 410.0):
        clock[0] = instant
        repeated = await runtime.run_cycle()
        assert repeated.nowcast.storm_cells == moving.nowcast.storm_cells
    clock[0] = 1301.0
    expired = await runtime.run_cycle()
    assert not expired.nowcast.storm_cells


def _runtime_radar_field(provider_id: str) -> ThinRadarField:
    x, y = Transformer.from_crs(
        "EPSG:4326", "EPSG:3857", always_xy=True,
    ).transform(4.42, 51.05)
    values = np.zeros((16, 16), dtype=float)
    values[6:10, 6:10] = 5.0
    return ThinRadarField(
        provider_id, "rain-rate", 100.0, "EPSG:3857",
        x - 8_000.0, y + 8_000.0, 1_000.0, 1_000.0,
        values, np.ones((16, 16), dtype=bool),
    )


def _regional_result(field: ThinRadarField) -> FusedRegionalRadarField:
    summary = RegionalFusionSummary(
        region_id="region:home",
        grid_id="grid:home",
        status="current",
        mode="fused",
        evaluated_at=100.0,
        field_observed_at=field.observed_at,
        contributors=("dwd_radolan", "opera_radar"),
        excluded={},
        standby={},
        reason_codes=(),
        coverage_fraction=1.0,
        mean_confidence=0.8,
        disagreement_fraction=0.0,
    )
    return FusedRegionalRadarField(
        region_id="region:home",
        field=field,
        confidence=np.ones(field.rain_rate_mm_h.shape, dtype=float),
        contributor_mask=np.ones(field.rain_rate_mm_h.shape, dtype=np.uint64),
        summary=summary,
    )


class _RegionalCoordinator:
    def __init__(self, completed=None, *, pending=False):
        self.completed = {} if completed is None else completed
        self.pending = pending
        self.closed = False

    def submit(self, region_plan, provider_results, *, evaluated_at):
        self.region_plan = region_plan

    def poll(self):
        return self.completed

    def pending_region_ids(self):
        return frozenset({"region:home"}) if self.pending else frozenset()

    def close(self):
        self.closed = True


class _RecordingRadarTracker:
    def __init__(self):
        self.fields = {}

    def advance(self, field, target):
        self.fields[target.target_id] = field
        return object()

    def tracks(self, field, target_id):
        if self.fields.get(target_id) is not field:
            return ()
        return (RadarTrack(
            track_id="track-1",
            provider_id=field.provider_id,
            observed_at=field.observed_at,
            latitude=51.05,
            longitude=4.42,
            rain_rate_mm_h=5.0,
            velocity_east_kmh=10.0,
            velocity_north_kmh=0.0,
            geometry={
                "type": "MultiPolygon",
                "coordinates": [[[[4.41, 51.04], [4.43, 51.04],
                                   [4.43, 51.06], [4.41, 51.06],
                                   [4.41, 51.04]]]],
            },
        ),)


class _NativeRadarProviders:
    async def fetch(self, requests):
        field = _runtime_radar_field("dwd_radolan")
        observation = NormalizedObservation.create(
            provider_id="dwd_radolan",
            product_id="rain-rate",
            target_id="home",
            kind=ObservationKind.RAIN_RATE,
            value=5.0,
            unit="mm/h",
            observed_at=100.0,
            received_at=100.0,
            latitude=51.05,
            longitude=4.42,
            quality=ObservationQuality.GOOD,
            geometry={"type": "Point", "coordinates": [4.42, 51.05]},
        )
        return {
            "dwd_radolan": ProviderResult(
                "dwd_radolan", True, "operational", None, (observation,),
                100.0, 100.0, (field,),
            ),
        }


@pytest.mark.asyncio
async def test_completed_fusion_is_primary_tracking_source():
    fused = _runtime_radar_field("fusion:region:home")
    coordinator = _RegionalCoordinator({"region:home": _regional_result(fused)})
    runtime = ThinOperationalRuntime(
        provider_runtime=_NativeRadarProviders(),
        regional_radar_coordinator=coordinator,
        clock=lambda: 100.0,
    )
    runtime._radar_tracker = _RecordingRadarTracker()
    runtime.apply_desired_state(_home_state())

    result = await runtime.run_cycle()

    assert result.nowcast.storm_cells
    assert {cell.provider_id for cell in result.nowcast.storm_cells} == {
        "fusion:region:home",
    }
    assert result.regional_radar[0].mode == "fused"
    assert result.snapshot["radar_fusion"]["items"][0]["mode"] == "fused"
    radar_metadata = runtime.geojson_collection()["metadata"]["radar_fusion"]
    assert radar_metadata[0]["region_id"] == "region:home"
    assert "rain_rate_mm_h" not in json.dumps(radar_metadata)
    assert "valid_mask" not in json.dumps(radar_metadata)


@pytest.mark.asyncio
async def test_regional_radar_rain_at_target_overrides_dry_model_point():
    fused = _runtime_radar_field("fusion:region:home")
    coordinator = _RegionalCoordinator({"region:home": _regional_result(fused)})
    runtime = ThinOperationalRuntime(
        provider_runtime=ZeroRainProviders(),
        regional_radar_coordinator=coordinator,
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_home_state())

    result = await runtime.run_cycle()

    target = result.nowcast.target_results["home"]
    assert target.current_state == "regen"
    assert target.precipitation_rate_mm_h == 5.0
    assert set(target.contributors) >= {"dwd_radolan", "opera_radar"}
    assert [item.provider_id for item in result.fusion.observations] == ["open_meteo"]
    assert result.snapshot["targets"]["items"][0]["current_precipitation"]["state"] == "raining"


@pytest.mark.asyncio
async def test_light_rain_is_mapped_without_becoming_a_tracked_cell():
    fused = _runtime_radar_field("fusion:region:home")
    fused.rain_rate_mm_h[:] = 0.0
    fused.rain_rate_mm_h[6:10, 6:10] = 0.2
    coordinator = _RegionalCoordinator({"region:home": _regional_result(fused)})
    runtime = ThinOperationalRuntime(
        provider_runtime=EmptyProviders(),
        regional_radar_coordinator=coordinator,
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_home_state())

    await runtime.run_cycle()

    collection = runtime.geojson_collection()
    assert not [
        feature
        for feature in collection["features"]
        if feature["properties"]["layer"] == "radar_cell"
    ]
    overlay = collection["radar_overlays"]["region:home"]
    assert overlay["provider_id"] == "fusion:region:home"
    assert overlay["observed_at"] == 100.0
    assert overlay["threshold_mm_h"] == 0.1
    assert overlay["runs"]
    assert {run["intensity"] for run in overlay["runs"]} == {1}


@pytest.mark.asyncio
async def test_pending_fusion_uses_marked_per_provider_fallback():
    coordinator = _RegionalCoordinator(pending=True)
    runtime = ThinOperationalRuntime(
        provider_runtime=_NativeRadarProviders(),
        regional_radar_coordinator=coordinator,
        clock=lambda: 100.0,
    )
    runtime._radar_tracker = _RecordingRadarTracker()
    runtime.apply_desired_state(_home_state())

    result = await runtime.run_cycle()

    assert any(
        cell.provider_id == "dwd_radolan"
        for cell in result.nowcast.storm_cells
    )
    assert result.regional_radar[0].mode == "fallback"
    assert result.regional_radar[0].reason_codes == ("fusion_pending",)


class BrokenPostGIS:
    async def write_cycle(
        self,
        cycle,
        nowcast,
        validations=(),
        *,
        regional_radar=(),
        region_plan=None,
    ):
        return PostGISWriteResult(
            status="degraded",
            last_error_code="postgis_unavailable",
            written_rows=MappingProxyType({}),
            queued_cycles=1,
            dropped_cycles=0,
        )


class AdvancingPostGIS:
    def __init__(self, clock_value):
        self._clock_value = clock_value

    async def write_cycle(
        self,
        cycle,
        nowcast,
        validations=(),
        *,
        regional_radar=(),
        region_plan=None,
    ):
        self._clock_value[0] = 400.0
        return PostGISWriteResult(
            status="current",
            last_error_code=None,
            written_rows=MappingProxyType({}),
            queued_cycles=0,
            dropped_cycles=0,
        )


class ZeroRainProviders:
    async def fetch(self, requests):
        request = next(item for item in requests if item.provider_id == "open_meteo")
        observations = tuple(
            NormalizedObservation.create(
                provider_id="open_meteo",
                product_id="forecast",
                target_id=target.target_id,
                kind=ObservationKind.RAIN_RATE,
                value=0.0,
                unit="mm/h",
                observed_at=100.0,
                received_at=100.0,
                latitude=target.latitude,
                longitude=target.longitude,
                quality=ObservationQuality.GOOD,
                geometry={
                    "type": "Point",
                    "coordinates": [target.longitude, target.latitude],
                },
            )
            for target in request.targets
        )
        return {
            "open_meteo": ProviderResult(
                provider_id="open_meteo",
                ready=True,
                status="operational",
                reason_code=None,
                observations=observations,
                attempted_at=100.0,
                completed_at=100.0,
            )
        }


class WetGroundProviders:
    async def fetch(self, requests):
        request = next(item for item in requests if item.provider_id == "netatmo")
        observations = tuple(
            NormalizedObservation.create(
                provider_id="netatmo",
                product_id="ground_observation",
                target_id=target.target_id,
                kind=ObservationKind.RAIN_RATE,
                value=0.6,
                unit="mm/h",
                observed_at=100.0,
                received_at=100.0,
                latitude=target.latitude,
                longitude=target.longitude,
                quality=ObservationQuality.GOOD,
                geometry={
                    "type": "Point",
                    "coordinates": [target.longitude, target.latitude],
                },
                metadata={
                    "station_id": "70:ee:50:29:59:56",
                    "measurement": "rain_rate",
                },
            )
            for target in request.targets
        )
        return {
            "netatmo": ProviderResult(
                provider_id="netatmo",
                ready=True,
                status="operational",
                reason_code=None,
                observations=observations,
                attempted_at=100.0,
                completed_at=100.0,
            )
        }


class BlockingZeroRainProviders(ZeroRainProviders):
    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def fetch(self, requests):
        self.started.set()
        await self.release.wait()
        return await super().fetch(requests)


class MovingRadarProviders:
    def __init__(self):
        self._frame = 0

    async def fetch(self, requests):
        self._frame += 1
        observed_at = 100.0 if self._frame == 1 else 700.0
        longitude = 4.00 if self._frame == 1 else 4.10
        observation = NormalizedObservation.create(
            provider_id="opera_radar",
            product_id="composite",
            target_id=None,
            kind=ObservationKind.RAIN_RATE,
            value=4.0,
            unit="mm/h",
            observed_at=observed_at,
            received_at=observed_at,
            latitude=50.85,
            longitude=longitude,
            quality=ObservationQuality.GOOD,
            geometry={"type": "Point", "coordinates": [longitude, 50.85]},
            metadata={"cell_id": "moving-cell"},
        )
        target_observation = NormalizedObservation.create(
            provider_id="opera_radar",
            product_id="composite",
            target_id="home",
            kind=ObservationKind.RAIN_RATE,
            value=0.0,
            unit="mm/h",
            observed_at=observed_at,
            received_at=observed_at,
            latitude=50.85,
            longitude=4.35,
            quality=ObservationQuality.GOOD,
            geometry={"type": "Point", "coordinates": [4.35, 50.85]},
        )
        return {
            "opera_radar": ProviderResult(
                provider_id="opera_radar",
                ready=True,
                status="operational",
                reason_code=None,
                observations=(observation, target_observation),
                attempted_at=observed_at,
                completed_at=observed_at,
            )
        }


class SpatialScopeProviders:
    async def fetch(self, requests):
        observations = tuple(
            NormalizedObservation.create(
                provider_id="opera_radar",
                product_id="composite",
                target_id=None,
                kind=ObservationKind.RAIN_RATE,
                value=4.0,
                unit="mm/h",
                observed_at=100.0,
                received_at=100.0,
                latitude=latitude,
                longitude=longitude,
                quality=ObservationQuality.GOOD,
                geometry={
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                metadata={"cell_id": cell_id},
            )
            for cell_id, latitude, longitude in (
                ("near-home", 51.20, 4.42),
                ("far-away", 48.86, 2.35),
            )
        )
        return {
            "opera_radar": ProviderResult(
                provider_id="opera_radar",
                ready=True,
                status="operational",
                reason_code=None,
                observations=observations,
                attempted_at=100.0,
                completed_at=100.0,
            )
        }


def test_desired_state_revisions_are_monotonic_and_idempotent():
    runtime = ThinOperationalRuntime(provider_runtime=EmptyProviders(), clock=lambda: 100.0)
    first = _state()

    activated = runtime.apply_desired_state(first)
    duplicate = runtime.apply_desired_state(first)

    assert activated["method"] == METHOD
    assert activated["status"] == "activated"
    assert duplicate["status"] == "idempotent"
    assert duplicate["receipt_count"] == 2

    with pytest.raises(ValueError, match="generation"):
        runtime.apply_desired_state(
            _state(generation=1, previous=activated["state_sha256"], issued_at=101.0)
        )


@pytest.mark.asyncio
async def test_first_cycle_is_immediate_and_postgis_failure_does_not_hide_snapshot():
    providers = EmptyProviders()
    runtime = ThinOperationalRuntime(
        provider_runtime=providers,
        postgis_writer=BrokenPostGIS(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    result = await runtime.run_cycle()

    assert providers.requests and providers.requests[0]
    assert result.postgis.status == "degraded"
    validated = validate_snapshot(result.snapshot)
    assert validated["revision"] == 1
    assert validated["worker"]["state"] == "degraded"
    assert validated["targets"]["items"][0]["target_id"] == "home"


@pytest.mark.asyncio
async def test_cycle_accepts_target_with_unknown_location_accuracy():
    runtime = ThinOperationalRuntime(
        provider_runtime=EmptyProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state(location_accuracy_m=None))

    result = await runtime.run_cycle()

    assert result.snapshot["revision"] >= 1
    assert result.snapshot["targets"]["items"][0]["target_id"] == "home"


@pytest.mark.asyncio
async def test_snapshot_ttl_starts_when_slow_postgis_cycle_is_published():
    now = [100.0]
    runtime = ThinOperationalRuntime(
        provider_runtime=ZeroRainProviders(),
        postgis_writer=AdvancingPostGIS(now),
        clock=lambda: now[0],
    )
    runtime.apply_desired_state(_state())

    snapshot = (await runtime.run_cycle()).snapshot

    assert snapshot["evaluated_at"] == 400.0
    assert snapshot["valid_until"] == 520.0
    assert snapshot["product_valid_at"] == 100.0
    assert snapshot["received_at"] == 100.0
    validate_snapshot(snapshot)


@pytest.mark.asyncio
async def test_runtime_reuses_one_region_for_provider_requests_snapshot_and_map():
    providers = EmptyProviders()
    runtime = ThinOperationalRuntime(
        provider_runtime=providers,
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_shared_home_state())

    result = await runtime.run_cycle()

    netatmo = next(
        request
        for request in providers.requests[0]
        if request.provider_id == "netatmo"
    )
    assert [region.region_id for region in netatmo.regions] == ["region:home"]
    assert {
        item["region_id"] for item in result.snapshot["targets"]["items"]
    } == {"region:home"}
    assert result.snapshot["regions"]["items"] == [{
        "region_id": "region:home",
        "display_name": "Thuis",
        "state": "unknown",
            "provider_ids": [
                    "ground.kmi.aws_10min",
                    "ground.metar.global",
                "ground.netatmo.region",
                "ground.wunderground.station",
                "lightning.blitzortung.event_ingress",
                "lightning.eumetsat.li",
                "lightning.goes19.glm",
                "model.openmeteo.region",
                "radar.dwd.radolan_rv",
                "radar.kmi.appv4_visual",
                "radar.kmi.radqpe_native",
            "radar.knmi.rtcor",
                    "radar.meteofrance.dpradar",
                    "radar.opera.dbzh",
                    "radar.rainviewer.visual",
                    "satellite.goes19.rrqpe",
                    "satellite.hsaf.h40b",
                    "satellite.noaa.err",
                ],
        "target_ids": ["home", "person.family"],
    }]
    target_features = [
        item
        for item in runtime.geojson_collection()["features"]
        if item["properties"]["layer"] == "target"
    ]
    assert {
        item["properties"]["region_engine"] for item in target_features
    } == {"region:home"}


@pytest.mark.asyncio
async def test_runtime_keeps_last_life360_region_during_grace_then_expires_it():
    now = [100.0]
    runtime = ThinOperationalRuntime(
        provider_runtime=EmptyProviders(),
        clock=lambda: now[0],
    )
    accepted = runtime.apply_desired_state(_shared_home_state())
    await runtime.run_cycle()

    now[0] = 200.0
    runtime.apply_desired_state(
        _shared_home_state_with_unavailable_person(
            accepted["state_sha256"],
        )
    )
    during_grace = (await runtime.run_cycle()).snapshot
    person = next(
        item
        for item in during_grace["targets"]["items"]
        if item["target_id"] == "person.family"
    )

    assert person["region_id"] == "region:home"
    assert "target_location_stale" in person["current_precipitation"]["reason_codes"]

    now[0] = 1001.0
    runtime._last_attempts.clear()
    expired = (await runtime.run_cycle()).snapshot

    assert [
        item["target_id"] for item in expired["targets"]["items"]
    ] == ["home"]


@pytest.mark.asyncio
async def test_runtime_discards_spatial_weather_outside_every_active_region():
    runtime = ThinOperationalRuntime(
        provider_runtime=SpatialScopeProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_shared_home_state())

    cycle = await runtime.run_cycle()

    assert {
        item.metadata["cell_id"] for item in cycle.fusion.observations
    } == {"near-home"}


@pytest.mark.asyncio
async def test_snapshot_uses_the_targets_captured_at_cycle_start():
    providers = BlockingZeroRainProviders()
    runtime = ThinOperationalRuntime(provider_runtime=providers, clock=lambda: 100.0)
    initial = _state()
    accepted = runtime.apply_desired_state(initial)

    cycle = asyncio.create_task(runtime.run_cycle())
    await providers.started.wait()
    runtime.apply_desired_state(_state(
        generation=2,
        previous=accepted["state_sha256"],
        issued_at=101.0,
        target_id="person.life360_manuel_van_san",
        display_name="Manuel",
    ))
    providers.release.set()

    snapshot = (await cycle).snapshot

    assert [item["target_id"] for item in snapshot["targets"]["items"]] == ["home"]


@pytest.mark.asyncio
async def test_snapshot_revision_remains_monotonic_across_runtime_restart():
    first = ThinOperationalRuntime(
        provider_runtime=EmptyProviders(),
        clock=lambda: 1_700_000_000.0,
    )
    first.apply_desired_state(_state())
    first_revision = (await first.run_cycle()).snapshot["revision"]

    restarted = ThinOperationalRuntime(
        provider_runtime=EmptyProviders(),
        clock=lambda: 1_700_000_002.0,
    )
    restarted.apply_desired_state(_state())
    restarted_revision = (await restarted.run_cycle()).snapshot["revision"]

    assert restarted_revision > first_revision


@pytest.mark.asyncio
async def test_zero_rain_observation_is_published_as_current_dry_result():
    runtime = ThinOperationalRuntime(
        provider_runtime=ZeroRainProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    snapshot = (await runtime.run_cycle()).snapshot
    target = snapshot["targets"]["items"][0]

    assert target["current_precipitation"]["state"] == "dry"
    assert target["current_precipitation"]["rain_rate_mm_h"] == 0.0
    assert target["current_precipitation"]["provider_ids"] == [
        "model.openmeteo.region"
    ]
    assert target["next_threat"]["state"] == "none"
    provider_ids = {
        item["provider_id"] for item in snapshot["providers"]["items"]
    }
    assert "model.openmeteo.region" in provider_ids
    assert "open_meteo" not in provider_ids


@pytest.mark.asyncio
async def test_wet_physical_station_is_published_as_current_ground_confirmation():
    runtime = ThinOperationalRuntime(
        provider_runtime=WetGroundProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())

    snapshot = (await runtime.run_cycle()).snapshot
    target = snapshot["targets"]["items"][0]
    ground = target["current_precipitation"]["ground_evidence"]

    assert ground == {
        "status": "wet",
        "freshness": "current",
        "confirmation": True,
        "same_track": None,
        "evidence_valid_at": 100.0,
        "observed_at_min": 100.0,
        "observed_at_max": 100.0,
        "received_at": 100.0,
        "physical_site_count": 1,
        "wet_site_count": 1,
        "source_ids": ["ground.netatmo.region"],
        "reason_codes": [],
    }
    validate_snapshot(snapshot)


@pytest.mark.asyncio
async def test_moving_threat_does_not_publish_uncalibrated_probability():
    now = [100.0]
    runtime = ThinOperationalRuntime(
        provider_runtime=MovingRadarProviders(),
        clock=lambda: now[0],
    )
    runtime.apply_desired_state(_state())
    await runtime.run_cycle()
    now[0] = 700.0
    runtime._last_attempts.clear()

    snapshot = (await runtime.run_cycle()).snapshot
    threat = snapshot["targets"]["items"][0]["next_threat"]

    assert threat["state"] == "available"
    assert threat["calibrated_probability_percent"] is None
    assert threat["probability_calibration_id"] is None
    validate_snapshot(snapshot)


def test_claim_and_snapshot_match_home_assistant_control_contract():
    runtime = ThinOperationalRuntime(provider_runtime=EmptyProviders(), clock=lambda: 100.0)
    claim = runtime.claim_authority("ha-nas", "claim-1")
    assert claim == {
        "method": METHOD,
        "status": "claimed",
        "producer_id": "ha-nas",
        "authority_epoch": 1,
        "claim_id": "claim-1",
        "claimed_at": 100.0,
    }
    assert runtime.desired_state_snapshot() == {
        "method": METHOD,
        "status": "unconfigured",
        "producer_id": "ha-nas",
        "authority_epoch": 1,
    }


def test_storm_snapshot_keeps_strongest_64_cells_and_reports_overflow():
    items = [
        {"storm_id": f"cell-{index:03d}", "max_dbz": float(index)}
        for index in range(80)
    ]

    collection = _storm_collection(items)

    assert collection["complete"] is False
    assert collection["overflow_count"] == 16
    assert len(collection["items"]) == 64
    assert {item["storm_id"] for item in collection["items"]} == {
        f"cell-{index:03d}" for index in range(16, 80)
    }
    assert [item["storm_id"] for item in collection["items"]] == sorted(
        item["storm_id"] for item in collection["items"]
    )


def _storm_cell(index: int, *, coordinates_per_ring: int = 0) -> StormCell:
    if coordinates_per_ring:
        ring = [
            [4.0 + point / 100_000.0, 50.0 + index / 10_000.0]
            for point in range(coordinates_per_ring)
        ]
        ring.append(ring[0])
        geometry = {"type": "Polygon", "coordinates": [ring]}
    else:
        geometry = {"type": "Point", "coordinates": [4.0, 50.0]}
    return StormCell(
        cell_id=f"cell-{index:03d}",
        provider_id="radar.opera.dbzh",
        observed_at=100.0,
        latitude=50.0,
        longitude=4.0,
        geometry=geometry,
        precipitation_rate_mm_h=1.0,
        reflectivity_dbz=float(index),
        radar_level=None,
        velocity_east_kmh=0.0,
        velocity_north_kmh=0.0,
        speed_kmh=0.0,
        heading_deg=None,
    )


def test_geojson_includes_all_storm_cells_when_payload_fits_transport_budget():
    runtime = ThinOperationalRuntime(
        provider_runtime=EmptyProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())
    fusion = type("Fusion", (), {"observations": (), "evaluated_at": 100.0})()
    nowcast = type(
        "Nowcast",
        (),
        {"storm_cells": tuple(_storm_cell(index) for index in range(80))},
    )()

    collection = runtime._build_geojson(fusion, nowcast)

    radar_features = [
        item for item in collection["features"]
        if item["properties"]["layer"] == "radar_cell"
    ]
    assert len(radar_features) == 80
    assert collection["metadata"]["radar_cells_included"] == 80
    assert collection["metadata"]["radar_cells_total"] == 80
    assert collection["metadata"]["truncated"] is False


def test_geojson_truncates_weakest_cells_only_at_actual_byte_budget():
    runtime = ThinOperationalRuntime(
        provider_runtime=EmptyProviders(),
        clock=lambda: 100.0,
    )
    runtime.apply_desired_state(_state())
    fusion = type("Fusion", (), {"observations": (), "evaluated_at": 100.0})()
    nowcast = type(
        "Nowcast",
        (),
        {
            "storm_cells": tuple(
                _storm_cell(index, coordinates_per_ring=1_000)
                for index in range(80)
            )
        },
    )()

    collection = runtime._build_geojson(fusion, nowcast)
    encoded = __import__("json").dumps(
        collection,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    radar_ids = {
        item["properties"]["storm_id"]
        for item in collection["features"]
        if item["properties"]["layer"] == "radar_cell"
    }

    assert len(encoded) <= 900_000
    assert collection["metadata"]["truncated"] is True
    assert collection["metadata"]["radar_cells_included"] < 80
    assert "cell-079" in radar_ids
    assert "cell-000" not in radar_ids
