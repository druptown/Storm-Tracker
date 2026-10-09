"""Geographic provider selection without jobs, receipts or RAW state."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping

from .desired_state import DesiredState, Target
from .thin_region_planner import ThinAcquisitionRegion, ThinRegionPlan


@dataclass(frozen=True, slots=True)
class CoverageBox:
    south: float
    north: float
    west: float
    east: float

    def contains(self, latitude: float, longitude: float) -> bool:
        longitude_matches = (
            self.west <= longitude <= self.east
            if self.west <= self.east
            else longitude >= self.west or longitude <= self.east
        )
        return self.south <= latitude <= self.north and longitude_matches


@dataclass(frozen=True, slots=True)
class ThinProviderSpec:
    provider_id: str
    product_id: str
    cadence_seconds: float
    coverage: CoverageBox | None

    def applies_to(self, target: Target) -> bool:
        return self.coverage is None or self.coverage.contains(
            target.latitude,
            target.longitude,
        )


@dataclass(frozen=True, slots=True)
class ThinProviderRequest:
    provider_id: str
    product_id: str
    targets: tuple[Target, ...]
    requested_at: float
    regions: tuple[ThinAcquisitionRegion, ...] = ()
    ground_radius_km: float = 20.0
    selected_station_ids: tuple[str, ...] = ()
    station_selection_applied: bool = False
    station_selection_reason: str | None = None
    station_discovery_target_ids: tuple[str, ...] = ()


EUROPE = CoverageBox(south=34.0, north=72.0, west=-25.0, east=45.0)
BELGIUM = CoverageBox(south=49.4, north=51.7, west=2.0, east=6.7)
NETHERLANDS_AND_BELGIUM = CoverageBox(
    south=49.4,
    north=54.8,
    west=2.0,
    east=8.0,
)
GERMANY_AND_BORDER = CoverageBox(
    south=46.0,
    north=56.0,
    west=3.0,
    east=16.0,
)
SPAIN = CoverageBox(south=35.0, north=44.5, west=-10.0, east=5.0)
FRANCE_METROPOLE = CoverageBox(
    south=41.0,
    north=51.5,
    west=-5.5,
    east=9.8,
)
ITALY = CoverageBox(south=35.0, north=48.0, west=4.5, east=20.5)
AUSTRIA = CoverageBox(south=45.5, north=49.5, west=8.1, east=17.75)
LUXEMBOURG_AND_BORDER = CoverageBox(
    south=48.5,
    north=51.0,
    west=4.5,
    east=7.5,
)
UNITED_KINGDOM_AND_IRELAND = CoverageBox(
    south=48.0,
    north=62.0,
    west=-12.0,
    east=4.0,
)
EUMETSAT_MTG_DISK = CoverageBox(
    south=-70.0,
    north=70.0,
    west=-70.0,
    east=70.0,
)
GOES18_DISK = CoverageBox(
    south=-65.0,
    north=65.0,
    west=155.0,
    east=-55.0,
)
GOES19_DISK = CoverageBox(
    south=-65.0,
    north=65.0,
    west=-140.0,
    east=20.0,
)


PROVIDERS: tuple[ThinProviderSpec, ...] = (
    ThinProviderSpec("kmi_aws", "aws_10min", 600.0, BELGIUM),
    ThinProviderSpec("kmi_appv4", "appv4_visual", 300.0, BELGIUM),
    ThinProviderSpec("kmi_radqpe", "radqpe_native", 300.0, BELGIUM),
    ThinProviderSpec("opera_radar", "odyssey_composite", 300.0, EUROPE),
    ThinProviderSpec(
        "knmi_radar",
        "radar_composite",
        300.0,
        NETHERLANDS_AND_BELGIUM,
    ),
    ThinProviderSpec("dwd_radolan", "radolan_rw", 300.0, GERMANY_AND_BORDER),
    ThinProviderSpec("open_meteo", "forecast", 600.0, None),
    ThinProviderSpec("metar", "ground_observation", 600.0, None),
    ThinProviderSpec("netatmo", "ground_observation", 300.0, None),
    ThinProviderSpec("wunderground", "ground_observation", 300.0, None),
    ThinProviderSpec("blitzortung", "lightning_events", 30.0, None),
    ThinProviderSpec("eumetsat_li", "mtg_li", 60.0, EUMETSAT_MTG_DISK),
    ThinProviderSpec("goes18_glm", "glm_l2", 120.0, GOES18_DISK),
    ThinProviderSpec("goes19_glm", "glm_l2", 120.0, GOES19_DISK),
    ThinProviderSpec("goes18_rrqpe", "rrqpe", 600.0, GOES18_DISK),
    ThinProviderSpec("goes19_rrqpe", "rrqpe", 600.0, GOES19_DISK),
    ThinProviderSpec("noaa_err", "enterprise_rain_rate", 600.0, None),
    ThinProviderSpec("hsaf_h40b", "h40b", 600.0, EUMETSAT_MTG_DISK),
    ThinProviderSpec("rainviewer", "visual", 300.0, None),
    ThinProviderSpec("aemet_radar", "radar_composite", 300.0, SPAIN),
    ThinProviderSpec(
        "meteofrance_radar",
        "radar_accumulation",
        300.0,
        FRANCE_METROPOLE,
    ),
    ThinProviderSpec(
        "dpc_radar",
        "surface_rainfall_intensity",
        300.0,
        ITALY,
    ),
    ThinProviderSpec(
        "italiameteo_icon2i",
        "forecast",
        900.0,
        ITALY,
    ),
    ThinProviderSpec(
        "italiameteo_dpc_catalog",
        "historical_daily_catalog",
        86_400.0,
        ITALY,
    ),
    ThinProviderSpec(
        "geosphere",
        "inca_nowcast",
        900.0,
        AUSTRIA,
    ),
    ThinProviderSpec(
        "meteolux",
        "location_weather_validation",
        900.0,
        LUXEMBOURG_AND_BORDER,
    ),
    ThinProviderSpec(
        "metoffice_radar",
        "uk_rate",
        300.0,
        UNITED_KINGDOM_AND_IRELAND,
    ),
)


def _target_is_usable(target: Target) -> bool:
    """Keep valid available coordinates usable while a person is stationary.

    Home Assistant trackers commonly retain an unchanged position timestamp for
    hours or days.  That timestamp describes the last movement, not the expiry
    of the coordinates, so provider acquisition must not be suppressed by it.
    """

    return target.available


def build_due_requests(
    state: DesiredState,
    *,
    now: float,
    last_attempts: Mapping[str, float],
    region_plan: ThinRegionPlan | None = None,
) -> tuple[ThinProviderRequest, ...]:
    """Return at most one request per due provider for applicable targets."""

    if not isinstance(state, DesiredState):
        raise TypeError("state must be DesiredState")
    now_value = float(now)
    if not math.isfinite(now_value) or now_value < 0.0:
        raise ValueError("now must be a finite non-negative timestamp")

    if region_plan is not None and not isinstance(region_plan, ThinRegionPlan):
        raise TypeError("region_plan must be ThinRegionPlan or None")
    usable_targets = (
        region_plan.targets
        if region_plan is not None
        else tuple(target for target in state.targets if _target_is_usable(target))
    )
    if not usable_targets:
        return ()

    requests: list[ThinProviderRequest] = []
    for spec in PROVIDERS:
        last_attempt = last_attempts.get(spec.provider_id)
        if last_attempt is not None:
            try:
                elapsed = now_value - float(last_attempt)
            except (TypeError, ValueError):
                elapsed = spec.cadence_seconds
            if elapsed < spec.cadence_seconds:
                continue
        targets = tuple(
            target for target in usable_targets if spec.applies_to(target)
        )
        if not targets:
            continue
        target_ids = {target.target_id for target in targets}
        regions = (
            tuple(
                region
                for region in region_plan.regions
                if target_ids.intersection(region.target_ids)
            )
            if region_plan is not None
            else ()
        )
        requests.append(
            ThinProviderRequest(
                provider_id=spec.provider_id,
                product_id=spec.product_id,
                targets=targets,
                requested_at=now_value,
                regions=regions,
                ground_radius_km=state.settings.ground_radius_km,
            )
        )
    return tuple(requests)


__all__ = [
    "PROVIDERS",
    "CoverageBox",
    "ThinProviderRequest",
    "ThinProviderSpec",
    "build_due_requests",
]
