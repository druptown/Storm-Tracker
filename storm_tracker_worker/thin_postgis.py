"""Direct PostGIS persistence for normalized observations and nowcasts."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import hashlib
import json
import os
from types import MappingProxyType
from typing import Any, Callable, Mapping

from .thin_fusion import FusionCycle
from .thin_ground_validation import ForecastValidation
from .thin_nowcast import ThinNowcast
from .thin_regional_radar_fusion import RegionalFusionSummary
from .thin_region_planner import ThinRegionPlan
from .thin_station_measurement import (
    StationMeasurement,
    deduplicate_station_measurements,
)


MAX_RETRY_CYCLES = 8
AUTO_DISCOVERY_GROUND_PROVIDERS = frozenset({
    "kmi_aws",
    "metar",
    "netatmo",
    "wunderground",
})


@dataclass(frozen=True, slots=True)
class ThinPostGISSettings:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)
    sslmode: str = "disable"
    connect_timeout: int = 3

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "ThinPostGISSettings | None":
        values = os.environ if environment is None else environment
        if str(values.get("STV3_POSTGIS_ENABLED", "false")).lower() not in {
            "1", "true", "yes", "on"
        }:
            return None
        host = str(values.get("STV3_POSTGIS_HOST", "")).strip()
        database = str(values.get("STV3_POSTGIS_DATABASE", "")).strip()
        user = str(values.get("STV3_POSTGIS_USER", "")).strip()
        password = str(values.get("STV3_POSTGIS_PASSWORD", ""))
        if not all((host, database, user, password)):
            raise ValueError("postgis_config_incomplete")
        return cls(
            host=host,
            port=int(values.get("STV3_POSTGIS_PORT", "5432")),
            database=database,
            user=user,
            password=password,
            sslmode=str(values.get("STV3_POSTGIS_SSLMODE", "disable")),
            connect_timeout=int(values.get("STV3_POSTGIS_CONNECT_TIMEOUT", "3")),
        )

    def connect_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
            "connect_timeout": self.connect_timeout,
            "application_name": "storm_tracker_thin_runtime",
        }


@dataclass(frozen=True, slots=True)
class PostGISWriteResult:
    status: str
    last_error_code: str | None
    written_rows: Mapping[str, int]
    queued_cycles: int
    dropped_cycles: int


_INSERT_OBSERVATION = """
WITH source AS (
    SELECT
        %s::text AS provider_id,
        %s::text AS product_id,
        %s::text AS kind,
        to_timestamp(%s) AS observed_at,
        ST_SetSRID(ST_GeomFromGeoJSON(%s),4326) AS geometry,
        %s::double precision AS value,
        %s::text AS unit,
        to_timestamp(%s) AS received_at,
        %s::text AS quality,
        %s::jsonb AS metadata
)
INSERT INTO stv3_observation(
    observation_key,provider_id,product_id,target_id,kind,value,unit,
    observed_at,received_at,quality,metadata,geometry
)
SELECT
    public.stv3_observation_source_key(
        provider_id,product_id,kind,observed_at,geometry
    ),
    provider_id,product_id,NULL,kind,value,unit,
    observed_at,received_at,quality,metadata,geometry
FROM source
ON CONFLICT(observation_key) DO UPDATE SET
    value=EXCLUDED.value, unit=EXCLUDED.unit,
    received_at=GREATEST(stv3_observation.received_at,EXCLUDED.received_at),
    quality=EXCLUDED.quality,
    metadata=EXCLUDED.metadata, geometry=EXCLUDED.geometry,
    updated_at=clock_timestamp()
"""

_INSERT_OBSERVATION_REGION = """
INSERT INTO stv3_observation_region(
    observation_key,region_revision_id,first_seen_at,last_seen_at
) VALUES(
    public.stv3_observation_source_key(
        %s,%s,%s,to_timestamp(%s),
        ST_SetSRID(ST_GeomFromGeoJSON(%s),4326)
    ),
    %s,to_timestamp(%s),to_timestamp(%s)
)
ON CONFLICT(observation_key,region_revision_id) DO UPDATE SET
    first_seen_at=LEAST(
        stv3_observation_region.first_seen_at,
        EXCLUDED.first_seen_at
    ),
    last_seen_at=GREATEST(
        stv3_observation_region.last_seen_at,
        EXCLUDED.last_seen_at
    )
"""

_ENFORCE_RETENTION = "SELECT public.stv3_enforce_retention()"

_CLOSE_REGION_TARGETS = """
UPDATE stv3_region_target
SET valid_until=to_timestamp(%s),active=false,updated_at=clock_timestamp()
WHERE valid_until IS NULL
  AND NOT EXISTS(
      SELECT 1
      FROM unnest(%s::text[],%s::text[])
          AS desired(target_id,region_revision_id)
      WHERE desired.target_id=stv3_region_target.target_id
        AND desired.region_revision_id=stv3_region_target.region_revision_id
  )
"""

_CLOSE_REGION_REVISIONS = """
UPDATE stv3_region_revision
SET valid_until=to_timestamp(%s),active=false
WHERE valid_until IS NULL
  AND NOT(region_revision_id=ANY(%s::text[]))
"""

_CLOSE_REGION_INSTANCES = """
UPDATE stv3_region_instance
SET valid_until=to_timestamp(%s),active=false
WHERE valid_until IS NULL
  AND NOT(region_instance_id=ANY(%s::text[]))
"""

_INSERT_REGION_INSTANCE = """
INSERT INTO stv3_region_instance(
    region_instance_id,region_id,kind,activated_at,last_seen_at,
    valid_until,active
) VALUES(
    %s,%s,%s,to_timestamp(%s),to_timestamp(%s),NULL,true
)
ON CONFLICT(region_instance_id) DO UPDATE SET
    region_id=EXCLUDED.region_id,
    kind=EXCLUDED.kind,
    activated_at=LEAST(
        stv3_region_instance.activated_at,EXCLUDED.activated_at
    ),
    last_seen_at=GREATEST(
        stv3_region_instance.last_seen_at,EXCLUDED.last_seen_at
    ),
    valid_until=NULL,active=true
"""

_INSERT_REGION_REVISION = """
INSERT INTO stv3_region_revision(
    region_revision_id,region_instance_id,center,observation_radius_km,
    radar_analysis_radius_km,valid_from,last_seen_at,valid_until,active
) VALUES(
    %s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),%s,%s,
    to_timestamp(%s),to_timestamp(%s),NULL,true
)
ON CONFLICT(region_revision_id) DO UPDATE SET
    region_instance_id=EXCLUDED.region_instance_id,
    center=EXCLUDED.center,
    observation_radius_km=EXCLUDED.observation_radius_km,
    radar_analysis_radius_km=EXCLUDED.radar_analysis_radius_km,
    valid_from=LEAST(stv3_region_revision.valid_from,EXCLUDED.valid_from),
    last_seen_at=GREATEST(
        stv3_region_revision.last_seen_at,EXCLUDED.last_seen_at
    ),
    valid_until=NULL,active=true
"""

_INSERT_REGION_TARGET = """
INSERT INTO stv3_region_target(
    target_id,region_revision_id,position,location_accuracy_m,
    position_observed_at,valid_from,valid_until,active
) VALUES(
    %s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),%s,
    to_timestamp(%s),to_timestamp(%s),NULL,true
)
ON CONFLICT(target_id) WHERE valid_until IS NULL DO UPDATE SET
    region_revision_id=EXCLUDED.region_revision_id,
    position=EXCLUDED.position,
    location_accuracy_m=EXCLUDED.location_accuracy_m,
    position_observed_at=EXCLUDED.position_observed_at,
    valid_from=LEAST(stv3_region_target.valid_from,EXCLUDED.valid_from),
    valid_until=NULL,active=true,
    updated_at=clock_timestamp()
"""

_INSERT_STATION_MEASUREMENT = """
INSERT INTO stv3_station_measurement(
    provider_id,station_id,observed_at,measurement_type,product_id,
    value,unit,received_at,quality,metadata
) VALUES(
    %s,%s,to_timestamp(%s),%s,%s,
    %s,%s,to_timestamp(%s),%s,%s::jsonb
)
ON CONFLICT(provider_id,station_id,observed_at,measurement_type) DO UPDATE SET
    product_id=CASE
        WHEN stv3_quality_rank(EXCLUDED.quality) >
             stv3_quality_rank(stv3_station_measurement.quality)
          OR (
             stv3_quality_rank(EXCLUDED.quality) =
             stv3_quality_rank(stv3_station_measurement.quality)
             AND EXCLUDED.received_at >= stv3_station_measurement.received_at
          )
        THEN EXCLUDED.product_id ELSE stv3_station_measurement.product_id END,
    value=CASE
        WHEN stv3_quality_rank(EXCLUDED.quality) >
             stv3_quality_rank(stv3_station_measurement.quality)
          OR (
             stv3_quality_rank(EXCLUDED.quality) =
             stv3_quality_rank(stv3_station_measurement.quality)
             AND EXCLUDED.received_at >= stv3_station_measurement.received_at
          )
        THEN EXCLUDED.value ELSE stv3_station_measurement.value END,
    unit=CASE
        WHEN stv3_quality_rank(EXCLUDED.quality) >
             stv3_quality_rank(stv3_station_measurement.quality)
          OR (
             stv3_quality_rank(EXCLUDED.quality) =
             stv3_quality_rank(stv3_station_measurement.quality)
             AND EXCLUDED.received_at >= stv3_station_measurement.received_at
          )
        THEN EXCLUDED.unit ELSE stv3_station_measurement.unit END,
    received_at=GREATEST(
        stv3_station_measurement.received_at,
        EXCLUDED.received_at
    ),
    quality=CASE
        WHEN stv3_quality_rank(EXCLUDED.quality) >
             stv3_quality_rank(stv3_station_measurement.quality)
          OR (
             stv3_quality_rank(EXCLUDED.quality) =
             stv3_quality_rank(stv3_station_measurement.quality)
             AND EXCLUDED.received_at >= stv3_station_measurement.received_at
          )
        THEN EXCLUDED.quality ELSE stv3_station_measurement.quality END,
    metadata=CASE
        WHEN stv3_quality_rank(EXCLUDED.quality) >
             stv3_quality_rank(stv3_station_measurement.quality)
          OR (
             stv3_quality_rank(EXCLUDED.quality) =
             stv3_quality_rank(stv3_station_measurement.quality)
             AND EXCLUDED.received_at >= stv3_station_measurement.received_at
          )
        THEN EXCLUDED.metadata ELSE stv3_station_measurement.metadata END,
    updated_at=clock_timestamp()
"""

_INSERT_STATION_MEASUREMENT_REGION = """
INSERT INTO stv3_station_measurement_region(
    provider_id,station_id,observed_at,measurement_type,region_revision_id,
    first_seen_at,last_seen_at
) VALUES(
    %s,%s,to_timestamp(%s),%s,%s,to_timestamp(%s),to_timestamp(%s)
)
ON CONFLICT(
    provider_id,station_id,observed_at,measurement_type,region_revision_id
) DO UPDATE SET
    first_seen_at=LEAST(
        stv3_station_measurement_region.first_seen_at,
        EXCLUDED.first_seen_at
    ),
    last_seen_at=GREATEST(
        stv3_station_measurement_region.last_seen_at,
        EXCLUDED.last_seen_at
    )
"""

_AUTO_UPSERT_GROUND_STATION = """
INSERT INTO stv3_ground_station(
    provider_id,station_id,position,discovered_at,last_seen_at,
    quality_score,operational
) VALUES(
    %s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),
    to_timestamp(%s),to_timestamp(%s),%s,true
)
ON CONFLICT(provider_id,station_id) DO UPDATE SET
    position=EXCLUDED.position,
    last_seen_at=GREATEST(
        stv3_ground_station.last_seen_at,
        EXCLUDED.last_seen_at
    ),
    quality_score=GREATEST(
        stv3_ground_station.quality_score,
        EXCLUDED.quality_score
    ),
    operational=true,
    updated_at=clock_timestamp()
"""

_AUTO_INSERT_STATION_CAPABILITY = """
INSERT INTO stv3_station_capability(provider_id,station_id,capability)
VALUES(%s,%s,%s)
ON CONFLICT(provider_id,station_id,capability) DO NOTHING
"""

_INSERT_FUSION = """
INSERT INTO stv3_fusion_cycle(
    cycle_key,evaluated_at,targets,providers,rejected,regional_radar
) VALUES(%s,to_timestamp(%s),%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb)
ON CONFLICT(cycle_key) DO UPDATE SET
    targets=EXCLUDED.targets,providers=EXCLUDED.providers,
    rejected=EXCLUDED.rejected,regional_radar=EXCLUDED.regional_radar,
    updated_at=clock_timestamp()
"""

_INSERT_CELL = """
INSERT INTO stv3_storm_cell(
    cell_id,observed_at,provider_id,precipitation_rate_mm_h,
    reflectivity_dbz,radar_level,geometry,region_revision_id
) VALUES(%s,to_timestamp(%s),%s,%s,%s,%s,
    ST_SetSRID(ST_GeomFromGeoJSON(%s),4326),%s)
ON CONFLICT(cell_id,observed_at) DO UPDATE SET
    precipitation_rate_mm_h=EXCLUDED.precipitation_rate_mm_h,
    reflectivity_dbz=EXCLUDED.reflectivity_dbz,
    radar_level=EXCLUDED.radar_level,geometry=EXCLUDED.geometry,
    region_revision_id=EXCLUDED.region_revision_id
"""

_INSERT_TRACK = """
INSERT INTO stv3_track_point(
    cell_id,observed_at,velocity_east_kmh,velocity_north_kmh,
    speed_kmh,heading_deg,position,region_revision_id,motion_metadata
) VALUES(%s,to_timestamp(%s),%s,%s,%s,%s,
    ST_SetSRID(ST_GeomFromGeoJSON(%s),4326),%s,%s::jsonb)
ON CONFLICT(cell_id,observed_at) DO UPDATE SET
    velocity_east_kmh=EXCLUDED.velocity_east_kmh,
    velocity_north_kmh=EXCLUDED.velocity_north_kmh,
    speed_kmh=EXCLUDED.speed_kmh,heading_deg=EXCLUDED.heading_deg,
    position=EXCLUDED.position,
    region_revision_id=EXCLUDED.region_revision_id,
    motion_metadata=EXCLUDED.motion_metadata
"""

_INSERT_FORECAST = """
INSERT INTO stv3_target_forecast(
    target_id,evaluated_at,current_state,forecast_state,
    precipitation_rate_mm_h,eta_minutes,confidence,reason_code,contributors,
    region_revision_id
) VALUES(%s,to_timestamp(%s),%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
ON CONFLICT(target_id,evaluated_at) DO UPDATE SET
    current_state=EXCLUDED.current_state,
    forecast_state=EXCLUDED.forecast_state,
    precipitation_rate_mm_h=EXCLUDED.precipitation_rate_mm_h,
    eta_minutes=EXCLUDED.eta_minutes,confidence=EXCLUDED.confidence,
    reason_code=EXCLUDED.reason_code,contributors=EXCLUDED.contributors,
    region_revision_id=EXCLUDED.region_revision_id
"""

_INSERT_TARGET_POSITION = """
INSERT INTO stv3_target_position(
    target_id,observed_at,position,accuracy_km
) VALUES(
    %s,to_timestamp(%s),
    ST_SetSRID(ST_MakePoint(%s,%s),4326),%s
)
ON CONFLICT(target_id,observed_at) DO UPDATE SET
    position=EXCLUDED.position,accuracy_km=EXCLUDED.accuracy_km
"""

_INSERT_PASSAGE = """
INSERT INTO stv3_target_passage(
    target_id,evaluated_at,cell_id,provider_id,entry_at,exit_at,
    method,predicted_intensity_mm_h,footprint,metadata,region_revision_id
) VALUES(
    %s,to_timestamp(%s),%s,%s,to_timestamp(%s),to_timestamp(%s),
    %s,%s,ST_SetSRID(ST_GeomFromGeoJSON(%s),4326),%s::jsonb,%s
)
ON CONFLICT(target_id,evaluated_at,cell_id) DO UPDATE SET
    entry_at=EXCLUDED.entry_at,exit_at=EXCLUDED.exit_at,
    method=EXCLUDED.method,
    predicted_intensity_mm_h=EXCLUDED.predicted_intensity_mm_h,
    footprint=EXCLUDED.footprint,metadata=EXCLUDED.metadata,
    region_revision_id=EXCLUDED.region_revision_id
"""

_INSERT_VALIDATION = """
INSERT INTO stv3_forecast_validation(
    validation_key,target_id,evaluated_at,cell_id,provider_id,
    station_provider_id,station_id,outcome,entry_error_seconds,
    exit_error_seconds,predicted_intensity_mm_h,observed_intensity_mm_h,
    details,region_revision_id
) VALUES(
    %s,%s,to_timestamp(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s
)
ON CONFLICT(validation_key) DO UPDATE SET
    outcome=EXCLUDED.outcome,
    entry_error_seconds=EXCLUDED.entry_error_seconds,
    exit_error_seconds=EXCLUDED.exit_error_seconds,
    predicted_intensity_mm_h=EXCLUDED.predicted_intensity_mm_h,
    observed_intensity_mm_h=EXCLUDED.observed_intensity_mm_h,
    details=EXCLUDED.details,
    region_revision_id=EXCLUDED.region_revision_id
"""


def _json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, tuple):
        return list(value)
    raise TypeError(f"not_json_compatible:{type(value).__name__}")


def _sha(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _track_motion_metadata(cell: Any) -> dict[str, Any]:
    """Return the auditable admission decision for every tracked cell."""

    return {
        "motion_ready": bool(getattr(cell, "motion_ready", False)),
        "motion_status": str(
            getattr(cell, "motion_status", "indeterminate")
        ),
        "motion_confidence": str(
            getattr(cell, "motion_confidence", "indeterminate")
        ),
        "motion_method": getattr(cell, "motion_method", None),
        "motion_reason_codes": [
            str(item)
            for item in getattr(cell, "motion_reason_codes", ())
        ],
        "motion_supporting_methods": [
            str(item)
            for item in getattr(cell, "motion_supporting_methods", ())
        ],
        "motion_agreement_angle_deg": getattr(
            cell, "motion_agreement_angle_deg", None
        ),
    }


def _passage_metadata(passage: Any) -> dict[str, Any]:
    """Keep a target passage tied to the exact admitted motion evidence."""

    return {
        "fallback_reason": passage.fallback_reason,
        "horizon_clipped": passage.horizon_clipped,
        "motion_status": passage.motion_status,
        "motion_confidence": passage.motion_confidence,
        "motion_method": passage.motion_method,
        "motion_reason_codes": list(passage.motion_reason_codes),
        "motion_supporting_methods": list(
            passage.motion_supporting_methods
        ),
        "motion_agreement_angle_deg": passage.motion_agreement_angle_deg,
    }


def _known_station_measurements(
    cursor: Any,
    measurements: tuple[StationMeasurement, ...],
) -> tuple[StationMeasurement, ...]:
    """Keep the canonical route limited to stations present in the catalog.

    Unknown stations deliberately stay on the legacy observation route.  This
    prevents one foreign-key miss from rolling back the complete weather
    cycle while station discovery catches up independently.
    """

    station_keys = tuple(sorted({
        (measurement.provider_id, measurement.station_id)
        for measurement in measurements
    }))
    if not station_keys:
        return ()
    values_sql = ",".join("(%s,%s)" for _item in station_keys)
    parameters = tuple(value for key in station_keys for value in key)
    cursor.execute(
        "SELECT station.provider_id,station.station_id "
        "FROM stv3_ground_station AS station "
        f"JOIN (VALUES {values_sql}) AS requested(provider_id,station_id) "
        "ON requested.provider_id=station.provider_id "
        "AND requested.station_id=station.station_id",
        parameters,
    )
    known = {(str(row[0]), str(row[1])) for row in cursor.fetchall()}
    return tuple(
        measurement for measurement in measurements
        if (measurement.provider_id, measurement.station_id) in known
    )


def _auto_discover_ground_stations(
    cursor: Any,
    observations: tuple[Any, ...],
    measurements: tuple[StationMeasurement, ...],
) -> int:
    """Upsert validated live ground stations without replacing the importer."""

    observations_by_identity: dict[tuple[str, str, float, str], Any] = {}
    for observation in observations:
        measurement = StationMeasurement.from_observation(observation)
        if measurement is not None:
            observations_by_identity.setdefault(measurement.identity, observation)

    stations: dict[tuple[str, str], dict[str, Any]] = {}
    quality_score = {
        "good": 1.0,
        "suspect": 0.75,
        "poor": 0.5,
        "unknown": 0.25,
    }
    for measurement in measurements:
        if measurement.provider_id not in AUTO_DISCOVERY_GROUND_PROVIDERS:
            continue
        observation = observations_by_identity.get(measurement.identity)
        if observation is None:
            continue
        key = (measurement.provider_id, measurement.station_id)
        item = stations.setdefault(key, {
            "longitude": float(observation.longitude),
            "latitude": float(observation.latitude),
            "discovered_at": float(measurement.received_at),
            "last_seen_at": float(measurement.observed_at),
            "quality_score": quality_score[measurement.quality.value],
            "capabilities": set(),
        })
        if measurement.observed_at >= item["last_seen_at"]:
            item["longitude"] = float(observation.longitude)
            item["latitude"] = float(observation.latitude)
            item["last_seen_at"] = float(measurement.observed_at)
        item["discovered_at"] = min(
            item["discovered_at"],
            float(measurement.received_at),
        )
        item["quality_score"] = max(
            item["quality_score"],
            quality_score[measurement.quality.value],
        )
        item["capabilities"].add(measurement.measurement_type)

    if not stations:
        return 0
    cursor.executemany(
        _AUTO_UPSERT_GROUND_STATION,
        (
            (
                provider_id,
                station_id,
                item["longitude"],
                item["latitude"],
                item["discovered_at"],
                item["last_seen_at"],
                item["quality_score"],
            )
            for (provider_id, station_id), item in sorted(stations.items())
        ),
    )
    cursor.executemany(
        _AUTO_INSERT_STATION_CAPABILITY,
        (
            (provider_id, station_id, capability)
            for (provider_id, station_id), item in sorted(stations.items())
            for capability in sorted(item["capabilities"])
        ),
    )
    return len(stations)


class ThinPostGISWriter:
    def __init__(
        self,
        settings: ThinPostGISSettings,
        *,
        connect_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._settings = settings
        self._connect_factory = connect_factory
        self._queue: deque[
            tuple[
                FusionCycle,
                ThinNowcast,
                tuple[ForecastValidation, ...],
                tuple[RegionalFusionSummary, ...],
                ThinRegionPlan | None,
            ]
        ] = deque()
        self._lock = asyncio.Lock()
        self._dropped_cycles = 0
        self._last_error_code: str | None = None
        self._last_written_rows: dict[str, int] = {}

    def _connect(self):
        factory = self._connect_factory
        if factory is None:
            import psycopg
            factory = psycopg.connect
        return factory(**self._settings.connect_kwargs())

    def _write_batch(
        self,
        batch: tuple[
            tuple[
                FusionCycle,
                ThinNowcast,
                tuple[ForecastValidation, ...],
                tuple[RegionalFusionSummary, ...],
                ThinRegionPlan | None,
            ], ...
        ],
    ) -> dict[str, int]:
        connection = self._connect()
        counts = {
            "regions": 0,
            "region_instances": 0,
            "region_revisions": 0,
            "region_targets": 0,
            "observations": 0,
            "observation_region_links": 0,
            "station_measurements": 0,
            "station_measurement_region_links": 0,
            "auto_discovered_stations": 0,
            "fusion_cycles": 0,
            "storm_cells": 0,
            "track_points": 0,
            "target_forecasts": 0,
            "target_positions": 0,
            "target_passages": 0,
            "forecast_validations": 0,
        }
        try:
            with connection.cursor() as cursor:
                seen_instance_ids: set[str] = set()
                seen_revision_ids: set[str] = set()
                seen_memberships: set[tuple[str, str]] = set()
                for (
                    cycle,
                    _nowcast,
                    _validations,
                    _regional_radar,
                    region_plan,
                ) in batch:
                    if region_plan is None:
                        continue
                    active_instances = tuple(
                        region.region_instance_id
                        for region in region_plan.regions
                    )
                    active_revisions = tuple(
                        region.region_revision_id
                        for region in region_plan.regions
                    )
                    active_memberships = tuple(
                        (target.target_id, region.region_revision_id)
                        for region in region_plan.regions
                        for target in region.targets
                    )
                    cursor.execute(
                        _CLOSE_REGION_TARGETS,
                        (
                            cycle.evaluated_at,
                            [item[0] for item in active_memberships],
                            [item[1] for item in active_memberships],
                        ),
                    )
                    cursor.execute(
                        _CLOSE_REGION_REVISIONS,
                        (cycle.evaluated_at, list(active_revisions)),
                    )
                    cursor.execute(
                        _CLOSE_REGION_INSTANCES,
                        (cycle.evaluated_at, list(active_instances)),
                    )
                    instance_rows: list[tuple[Any, ...]] = []
                    revision_rows: list[tuple[Any, ...]] = []
                    membership_rows: list[tuple[Any, ...]] = []
                    for region in region_plan.regions:
                        instance_rows.append((
                            region.region_instance_id,
                            region.region_id,
                            region.kind,
                            region.activated_at,
                            cycle.evaluated_at,
                        ))
                        revision_rows.append((
                            region.region_revision_id,
                            region.region_instance_id,
                            region.center_longitude,
                            region.center_latitude,
                            region.observation_radius_km,
                            region.radar_analysis_radius_km,
                            region.revision_valid_from,
                            cycle.evaluated_at,
                        ))
                        seen_instance_ids.add(region.region_instance_id)
                        seen_revision_ids.add(region.region_revision_id)
                        for target in region.targets:
                            membership_rows.append((
                                target.target_id,
                                region.region_revision_id,
                                target.longitude,
                                target.latitude,
                                target.location_accuracy_m,
                                target.position_observed_at,
                                cycle.evaluated_at,
                            ))
                            seen_memberships.add((
                                target.target_id,
                                region.region_revision_id,
                            ))
                    if instance_rows:
                        cursor.executemany(_INSERT_REGION_INSTANCE, instance_rows)
                    if revision_rows:
                        cursor.executemany(_INSERT_REGION_REVISION, revision_rows)
                    if membership_rows:
                        cursor.executemany(_INSERT_REGION_TARGET, membership_rows)
                counts["region_instances"] = len(seen_instance_ids)
                counts["region_revisions"] = len(seen_revision_ids)
                counts["regions"] = len(seen_revision_ids)
                counts["region_targets"] = len(seen_memberships)

                all_observations = tuple(
                    observation
                    for (
                        cycle,
                        _nowcast,
                        _validations,
                        _regional_radar,
                        _region_plan,
                    ) in batch
                    for observation in cycle.observations
                )
                station_candidates = deduplicate_station_measurements(
                    all_observations,
                )
                counts["auto_discovered_stations"] = (
                    _auto_discover_ground_stations(
                        cursor,
                        all_observations,
                        station_candidates,
                    )
                )
                station_measurements = _known_station_measurements(
                    cursor,
                    station_candidates,
                )
                station_identities = {
                    measurement.identity
                    for measurement in station_measurements
                }
                station_rows = [
                    (
                        measurement.provider_id,
                        measurement.station_id,
                        measurement.observed_at,
                        measurement.measurement_type,
                        measurement.product_id,
                        measurement.value,
                        measurement.unit,
                        measurement.received_at,
                        measurement.quality.value,
                        _json(measurement.metadata),
                    )
                    for measurement in station_measurements
                ]
                if station_rows:
                    cursor.executemany(
                        _INSERT_STATION_MEASUREMENT,
                        station_rows,
                    )
                    counts["station_measurements"] = len(station_rows)

                station_region_rows: dict[
                    tuple[tuple[str, str, float, str], str],
                    tuple[Any, ...],
                ] = {}
                for (
                    cycle,
                    _nowcast,
                    _validations,
                    _regional_radar,
                    region_plan,
                ) in batch:
                    if region_plan is None:
                        continue
                    for observation in cycle.observations:
                        measurement = StationMeasurement.from_observation(
                            observation
                        )
                        if (
                            measurement is None
                            or measurement.identity not in station_identities
                        ):
                            continue
                        region_revision_id = None
                        if observation.target_id is not None:
                            region_revision_id = region_plan.region_revision_id_for(
                                observation.target_id
                            )
                        if region_revision_id is None:
                            region_revision_id = (
                                region_plan.region_revision_id_for_position(
                                observation.latitude,
                                observation.longitude,
                            )
                            )
                        if region_revision_id is None:
                            continue
                        station_region_rows[
                            (measurement.identity, region_revision_id)
                        ] = (
                            measurement.provider_id,
                            measurement.station_id,
                            measurement.observed_at,
                            measurement.measurement_type,
                            region_revision_id,
                            measurement.received_at,
                            measurement.received_at,
                        )
                if station_region_rows:
                    cursor.executemany(
                        _INSERT_STATION_MEASUREMENT_REGION,
                        station_region_rows.values(),
                    )
                    counts["station_measurement_region_links"] = len(
                        station_region_rows
                    )

                observations_by_source: dict[
                    tuple[str, str, str, float, str], tuple[Any, ...]
                ] = {}
                observation_region_rows: dict[
                    tuple[tuple[str, str, str, float, str], str],
                    tuple[Any, ...],
                ] = {}
                for (
                    cycle,
                    nowcast,
                    _validations,
                    _regional_radar,
                    region_plan,
                ) in batch:
                    for observation in cycle.observations:
                        station_measurement = StationMeasurement.from_observation(
                            observation,
                        )
                        if (
                            station_measurement is not None
                            and station_measurement.identity in station_identities
                        ):
                            continue
                        payload = observation.as_dict()
                        geometry = payload["geometry"] or {
                            "type": "Point",
                            "coordinates": [observation.longitude, observation.latitude],
                        }
                        geometry_json = _json(geometry)
                        source_identity = (
                            observation.provider_id,
                            observation.product_id,
                            observation.kind.value,
                            observation.observed_at,
                            geometry_json,
                        )
                        observations_by_source[source_identity] = (
                            observation.provider_id,
                            observation.product_id,
                            observation.kind.value,
                            observation.observed_at,
                            geometry_json,
                            observation.value,
                            observation.unit,
                            observation.received_at,
                            observation.quality.value,
                            _json(payload["metadata"]),
                        )
                        region_revision_id = None
                        if region_plan is not None:
                            if observation.target_id is not None:
                                region_revision_id = (
                                    region_plan.region_revision_id_for(
                                    observation.target_id
                                )
                                )
                            if region_revision_id is None:
                                region_revision_id = (
                                    region_plan.region_revision_id_for_position(
                                    observation.latitude,
                                    observation.longitude,
                                )
                                )
                        if region_revision_id is not None:
                            observation_region_rows[
                                (source_identity, region_revision_id)
                            ] = (
                                observation.provider_id,
                                observation.product_id,
                                observation.kind.value,
                                observation.observed_at,
                                geometry_json,
                                region_revision_id,
                                observation.received_at,
                                observation.received_at,
                            )

                observation_rows = list(observations_by_source.values())
                if observation_rows:
                    cursor.executemany(_INSERT_OBSERVATION, observation_rows)
                    counts["observations"] = len(observation_rows)
                region_rows = list(observation_region_rows.values())
                if region_rows:
                    cursor.executemany(_INSERT_OBSERVATION_REGION, region_rows)
                    counts["observation_region_links"] = len(region_rows)

                for (
                    cycle,
                    nowcast,
                    validations,
                    regional_radar,
                    region_plan,
                ) in batch:
                    targets = {
                        key: value.as_dict() for key, value in cycle.targets.items()
                    }
                    providers = {
                        key: {
                            "ready": value.ready,
                            "status": value.status,
                            "reason_code": value.reason_code,
                            "observation_count": len(value.observations),
                        }
                        for key, value in cycle.providers.items()
                    }
                    cycle_key = _sha({
                        "evaluated_at": cycle.evaluated_at,
                        "targets": sorted(cycle.targets),
                    })
                    cursor.execute(_INSERT_FUSION, (
                        cycle_key,
                        cycle.evaluated_at,
                        _json(targets),
                        _json(providers),
                        _json(dict(cycle.rejected)),
                        _json([
                            summary.as_dict()
                            for summary in sorted(
                                regional_radar,
                                key=lambda item: item.region_id,
                            )
                        ]),
                    ))
                    counts["fusion_cycles"] += 1

                    for cell in nowcast.storm_cells:
                        cell_payload = cell.as_dict()
                        geometry_json = _json(cell_payload["geometry"])
                        cell_region_revision_id = None
                        if region_plan is not None:
                            if cell.region_id is not None:
                                cell_region_revision_id = (
                                    region_plan.region_revision_id_for_region(
                                        cell.region_id,
                                    )
                                )
                            else:
                                cell_region_revision_id = (
                                    region_plan.region_revision_id_for_position(
                                        cell.latitude,
                                        cell.longitude,
                                    )
                                )
                        cursor.execute(_INSERT_CELL, (
                            cell.cell_id,
                            cell.observed_at,
                            cell.provider_id,
                            cell.precipitation_rate_mm_h,
                            cell.reflectivity_dbz,
                            cell.radar_level,
                            geometry_json,
                            cell_region_revision_id,
                        ))
                        counts["storm_cells"] += 1
                        cursor.execute(_INSERT_TRACK, (
                            cell.cell_id,
                            cell.observed_at,
                            cell.velocity_east_kmh,
                            cell.velocity_north_kmh,
                            cell.speed_kmh,
                            cell.heading_deg,
                            _json({
                                "type": "Point",
                                "coordinates": [cell.longitude, cell.latitude],
                            }),
                            cell_region_revision_id,
                            _json(_track_motion_metadata(cell)),
                        ))
                        counts["track_points"] += 1

                    for target_id, forecast in nowcast.target_results.items():
                        target_region_revision_id = (
                            None
                            if region_plan is None
                            else region_plan.region_revision_id_for(target_id)
                        )
                        cursor.execute(_INSERT_FORECAST, (
                            target_id,
                            nowcast.evaluated_at,
                            forecast.current_state,
                            forecast.forecast_state,
                            forecast.precipitation_rate_mm_h,
                            forecast.eta_minutes,
                            forecast.confidence,
                            forecast.reason_code,
                            _json(list(forecast.contributors)),
                            target_region_revision_id,
                        ))
                        counts["target_forecasts"] += 1
                        if (
                            forecast.target_latitude is not None
                            and forecast.target_longitude is not None
                        ):
                            cursor.execute(_INSERT_TARGET_POSITION, (
                                target_id,
                                nowcast.evaluated_at,
                                forecast.target_longitude,
                                forecast.target_latitude,
                                forecast.target_accuracy_km,
                            ))
                            counts["target_positions"] += 1

                    for passage in nowcast.target_passages:
                        passage_region_revision_id = (
                            None
                            if region_plan is None
                            else region_plan.region_revision_id_for(
                                passage.target_id
                            )
                        )
                        cursor.execute(_INSERT_PASSAGE, (
                            passage.target_id,
                            passage.evaluated_at,
                            passage.cell_id,
                            passage.provider_id,
                            passage.entry_at,
                            passage.exit_at,
                            passage.method,
                            passage.predicted_intensity_mm_h,
                            _json(dict(passage.footprint)),
                            _json(_passage_metadata(passage)),
                            passage_region_revision_id,
                        ))
                        counts["target_passages"] += 1

                    for validation in validations:
                        validation_region_revision_id = (
                            None
                            if region_plan is None
                            else region_plan.region_revision_id_for(
                                validation.target_id
                            )
                        )
                        cursor.execute(_INSERT_VALIDATION, (
                            validation.validation_key,
                            validation.target_id,
                            validation.evaluated_at,
                            validation.cell_id,
                            validation.provider_id,
                            validation.station_provider_id,
                            validation.station_id,
                            validation.outcome,
                            validation.entry_error_seconds,
                            validation.exit_error_seconds,
                            validation.predicted_intensity_mm_h,
                            validation.observed_intensity_mm_h,
                            _json({
                                "intensity_error_mm_h": (
                                    validation.intensity_error_mm_h
                                ),
                                "contributing_stations": list(
                                    validation.contributing_stations
                                ),
                            }),
                            validation_region_revision_id,
                        ))
                        counts["forecast_validations"] += 1

                cursor.execute(_ENFORCE_RETENTION)
            connection.commit()
            return counts
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    async def write_cycle(
        self,
        cycle: FusionCycle,
        nowcast: ThinNowcast,
        validations: tuple[ForecastValidation, ...] = (),
        *,
        regional_radar: tuple[RegionalFusionSummary, ...] = (),
        region_plan: ThinRegionPlan | None = None,
    ) -> PostGISWriteResult:
        async with self._lock:
            if len(self._queue) >= MAX_RETRY_CYCLES:
                self._queue.popleft()
                self._dropped_cycles += 1
            self._queue.append((
                cycle,
                nowcast,
                tuple(validations),
                tuple(regional_radar),
                region_plan,
            ))
            batch = tuple(self._queue)
            try:
                counts = await asyncio.to_thread(self._write_batch, batch)
            except Exception:
                self._last_error_code = "postgis_unavailable"
                return PostGISWriteResult(
                    status="degraded",
                    last_error_code=self._last_error_code,
                    written_rows=MappingProxyType({}),
                    queued_cycles=len(self._queue),
                    dropped_cycles=self._dropped_cycles,
                )
            self._queue.clear()
            self._last_error_code = None
            self._last_written_rows = counts
            return PostGISWriteResult(
                status="current",
                last_error_code=None,
                written_rows=MappingProxyType(dict(counts)),
                queued_cycles=0,
                dropped_cycles=self._dropped_cycles,
            )

    def diagnostics(self) -> dict[str, Any]:
        return {
            "status": "degraded" if self._last_error_code else "current",
            "last_error_code": self._last_error_code,
            "queued_cycles": len(self._queue),
            "dropped_cycles": self._dropped_cycles,
            "last_written_rows": dict(self._last_written_rows),
        }

__all__ = [
    "MAX_RETRY_CYCLES",
    "PostGISWriteResult",
    "ThinPostGISSettings",
    "ThinPostGISWriter",
]
