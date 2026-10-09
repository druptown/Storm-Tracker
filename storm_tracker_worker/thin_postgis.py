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


MAX_RETRY_CYCLES = 8


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
INSERT INTO stv3_observation(
    observation_key,provider_id,product_id,target_id,kind,value,unit,
    observed_at,received_at,quality,metadata,geometry
) VALUES(
    %s,%s,%s,%s,%s,%s,%s,
    to_timestamp(%s),to_timestamp(%s),%s,%s::jsonb,
    ST_SetSRID(ST_GeomFromGeoJSON(%s),4326)
)
ON CONFLICT(observation_key) DO UPDATE SET
    value=EXCLUDED.value, quality=EXCLUDED.quality,
    metadata=EXCLUDED.metadata, geometry=EXCLUDED.geometry,
    updated_at=clock_timestamp()
"""

_INSERT_FUSION = """
INSERT INTO stv3_fusion_cycle(
    cycle_key,evaluated_at,targets,providers,rejected
) VALUES(%s,to_timestamp(%s),%s::jsonb,%s::jsonb,%s::jsonb)
ON CONFLICT(cycle_key) DO UPDATE SET
    targets=EXCLUDED.targets,providers=EXCLUDED.providers,
    rejected=EXCLUDED.rejected,updated_at=clock_timestamp()
"""

_INSERT_CELL = """
INSERT INTO stv3_storm_cell(
    cell_id,observed_at,provider_id,precipitation_rate_mm_h,
    reflectivity_dbz,radar_level,geometry
) VALUES(%s,to_timestamp(%s),%s,%s,%s,%s,
    ST_SetSRID(ST_GeomFromGeoJSON(%s),4326))
ON CONFLICT(cell_id,observed_at) DO UPDATE SET
    precipitation_rate_mm_h=EXCLUDED.precipitation_rate_mm_h,
    reflectivity_dbz=EXCLUDED.reflectivity_dbz,
    radar_level=EXCLUDED.radar_level,geometry=EXCLUDED.geometry
"""

_INSERT_TRACK = """
INSERT INTO stv3_track_point(
    cell_id,observed_at,velocity_east_kmh,velocity_north_kmh,
    speed_kmh,heading_deg,position
) VALUES(%s,to_timestamp(%s),%s,%s,%s,%s,
    ST_SetSRID(ST_GeomFromGeoJSON(%s),4326))
ON CONFLICT(cell_id,observed_at) DO UPDATE SET
    velocity_east_kmh=EXCLUDED.velocity_east_kmh,
    velocity_north_kmh=EXCLUDED.velocity_north_kmh,
    speed_kmh=EXCLUDED.speed_kmh,heading_deg=EXCLUDED.heading_deg,
    position=EXCLUDED.position
"""

_INSERT_FORECAST = """
INSERT INTO stv3_target_forecast(
    target_id,evaluated_at,current_state,forecast_state,
    precipitation_rate_mm_h,eta_minutes,confidence,reason_code,contributors
) VALUES(%s,to_timestamp(%s),%s,%s,%s,%s,%s,%s,%s::jsonb)
ON CONFLICT(target_id,evaluated_at) DO UPDATE SET
    current_state=EXCLUDED.current_state,
    forecast_state=EXCLUDED.forecast_state,
    precipitation_rate_mm_h=EXCLUDED.precipitation_rate_mm_h,
    eta_minutes=EXCLUDED.eta_minutes,confidence=EXCLUDED.confidence,
    reason_code=EXCLUDED.reason_code,contributors=EXCLUDED.contributors
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
    method,predicted_intensity_mm_h,footprint,metadata
) VALUES(
    %s,to_timestamp(%s),%s,%s,to_timestamp(%s),to_timestamp(%s),
    %s,%s,ST_SetSRID(ST_GeomFromGeoJSON(%s),4326),%s::jsonb
)
ON CONFLICT(target_id,evaluated_at,cell_id) DO UPDATE SET
    entry_at=EXCLUDED.entry_at,exit_at=EXCLUDED.exit_at,
    method=EXCLUDED.method,
    predicted_intensity_mm_h=EXCLUDED.predicted_intensity_mm_h,
    footprint=EXCLUDED.footprint,metadata=EXCLUDED.metadata
"""

_INSERT_VALIDATION = """
INSERT INTO stv3_forecast_validation(
    validation_key,target_id,evaluated_at,cell_id,provider_id,
    station_provider_id,station_id,outcome,entry_error_seconds,
    exit_error_seconds,predicted_intensity_mm_h,observed_intensity_mm_h,
    details
) VALUES(
    %s,%s,to_timestamp(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb
)
ON CONFLICT(validation_key) DO UPDATE SET
    outcome=EXCLUDED.outcome,
    entry_error_seconds=EXCLUDED.entry_error_seconds,
    exit_error_seconds=EXCLUDED.exit_error_seconds,
    predicted_intensity_mm_h=EXCLUDED.predicted_intensity_mm_h,
    observed_intensity_mm_h=EXCLUDED.observed_intensity_mm_h,
    details=EXCLUDED.details
"""


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


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
            tuple[FusionCycle, ThinNowcast, tuple[ForecastValidation, ...]]
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
            tuple[FusionCycle, ThinNowcast, tuple[ForecastValidation, ...]], ...
        ],
    ) -> dict[str, int]:
        connection = self._connect()
        counts = {
            "observations": 0,
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
                observation_rows: list[tuple[Any, ...]] = []
                for cycle, nowcast, _validations in batch:
                    for observation in cycle.observations:
                        payload = observation.as_dict()
                        geometry = payload["geometry"] or {
                            "type": "Point",
                            "coordinates": [observation.longitude, observation.latitude],
                        }
                        identity = {
                            "provider_id": observation.provider_id,
                            "product_id": observation.product_id,
                            "target_id": observation.target_id,
                            "kind": observation.kind.value,
                            "observed_at": observation.observed_at,
                            "latitude": observation.latitude,
                            "longitude": observation.longitude,
                        }
                        observation_rows.append((
                            _sha(identity),
                            observation.provider_id,
                            observation.product_id,
                            observation.target_id,
                            observation.kind.value,
                            observation.value,
                            observation.unit,
                            observation.observed_at,
                            observation.received_at,
                            observation.quality.value,
                            _json(payload["metadata"]),
                            _json(geometry),
                        ))
                        counts["observations"] += 1

                if observation_rows:
                    cursor.executemany(_INSERT_OBSERVATION, observation_rows)

                for cycle, nowcast, validations in batch:
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
                    ))
                    counts["fusion_cycles"] += 1

                    for cell in nowcast.storm_cells:
                        cell_payload = cell.as_dict()
                        geometry_json = _json(cell_payload["geometry"])
                        cursor.execute(_INSERT_CELL, (
                            cell.cell_id,
                            cell.observed_at,
                            cell.provider_id,
                            cell.precipitation_rate_mm_h,
                            cell.reflectivity_dbz,
                            cell.radar_level,
                            geometry_json,
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
                        ))
                        counts["track_points"] += 1

                    for target_id, forecast in nowcast.target_results.items():
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
                            _json({
                                "fallback_reason": passage.fallback_reason,
                                "horizon_clipped": passage.horizon_clipped,
                            }),
                        ))
                        counts["target_passages"] += 1

                    for validation in validations:
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
                        ))
                        counts["forecast_validations"] += 1
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
    ) -> PostGISWriteResult:
        async with self._lock:
            if len(self._queue) >= MAX_RETRY_CYCLES:
                self._queue.popleft()
                self._dropped_cycles += 1
            self._queue.append((cycle, nowcast, tuple(validations)))
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
