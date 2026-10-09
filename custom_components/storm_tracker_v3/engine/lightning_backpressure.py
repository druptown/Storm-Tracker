"""Coalesce high-rate lightning input before publishing it to Home Assistant."""
from __future__ import annotations

from collections.abc import Callable
from math import floor
from typing import Any


def aggregate_lightning_rows(
    observations: list[dict], *, grid_deg: float = 0.05
) -> list[dict]:
    """Vat ruwe inslagen samen per minuut en geografische rastercel."""
    buckets: dict[tuple, dict] = {}
    for item in observations:
        timestamp = float(item["timestamp"])
        latitude = float(item["lat"])
        longitude = float(item["lon"])
        key = (
            int(timestamp // 60),
            str(item.get("source") or "unknown"),
            floor(latitude / grid_deg),
            floor(longitude / grid_deg),
        )
        bucket = buckets.setdefault(key, {
            "nominal_minute": key[0],
            "source": key[1],
            "grid_lat": key[2],
            "grid_lon": key[3],
            "grid_deg": grid_deg,
            "strike_count": 0,
            "latitude_sum": 0.0,
            "longitude_sum": 0.0,
            "latitude_min": latitude,
            "latitude_max": latitude,
            "longitude_min": longitude,
            "longitude_max": longitude,
            "first_timestamp": timestamp,
            "last_timestamp": timestamp,
            "region_ids": set(),
        })
        bucket["strike_count"] += 1
        bucket["latitude_sum"] += latitude
        bucket["longitude_sum"] += longitude
        bucket["latitude_min"] = min(bucket["latitude_min"], latitude)
        bucket["latitude_max"] = max(bucket["latitude_max"], latitude)
        bucket["longitude_min"] = min(bucket["longitude_min"], longitude)
        bucket["longitude_max"] = max(bucket["longitude_max"], longitude)
        bucket["first_timestamp"] = min(bucket["first_timestamp"], timestamp)
        bucket["last_timestamp"] = max(bucket["last_timestamp"], timestamp)
        bucket["region_ids"].update(str(value) for value in item.get("engine_ids", ()))
    result = []
    for bucket in buckets.values():
        bucket["region_ids"] = sorted(bucket["region_ids"])
        result.append(bucket)
    return result


class LightningUpdateCoalescer:
    """Keep every strike internally, but bound HA events and log jobs."""

    def __init__(
        self,
        schedule_later: Callable[[float, Callable[[], None]], Any],
        publish_sensor_update: Callable[[dict], None],
        publish_map_update: Callable[[dict], None],
        write_log_batch: Callable[[list[dict]], None],
        *,
        sensor_interval_s: float = 2.0,
        map_interval_s: float = 10.0,
    ) -> None:
        self._schedule_later = schedule_later
        self._publish_sensor_update = publish_sensor_update
        self._publish_map_update = publish_map_update
        self._write_log_batch = write_log_batch
        self._base_sensor_interval_s = sensor_interval_s
        self._sensor_interval_s = sensor_interval_s
        self._map_interval_s = map_interval_s
        self._sensor_handle = None
        self._map_handle = None
        self._sensor_count = 0
        self._map_count = 0
        self._last_payload: dict = {}
        self._log_rows: list[dict] = []
        self._closed = False

    def submit(self, payload: dict, *, log: bool = False) -> None:
        """Queue one already-processed strike for bounded public updates."""
        if self._closed:
            return
        self._last_payload = dict(payload)
        self._sensor_count += 1
        self._map_count += 1
        if log:
            self._log_rows.append(dict(payload))
        if self._sensor_handle is None:
            self._sensor_handle = self._schedule_later(
                self._sensor_interval_s, self._flush_sensor
            )
        if self._map_handle is None:
            self._map_handle = self._schedule_later(
                self._map_interval_s, self._flush_map
            )

    def _flush_sensor(self) -> None:
        self._sensor_handle = None
        if self._closed or not self._sensor_count:
            return
        count = self._sensor_count
        rows = self._log_rows
        self._sensor_count = 0
        self._log_rows = []
        # Adapt the next window to the observed input pressure.
        if count > 50:
            self._sensor_interval_s = 10.0
        elif count > 10:
            self._sensor_interval_s = 5.0
        else:
            self._sensor_interval_s = self._base_sensor_interval_s
        self._publish_sensor_update({
            **self._last_payload,
            "batch_count": count,
            "update_interval_s": self._sensor_interval_s,
        })
        if rows:
            self._write_log_batch(aggregate_lightning_rows(rows))

    def _flush_map(self) -> None:
        self._map_handle = None
        if self._closed or not self._map_count:
            return
        count = self._map_count
        self._map_count = 0
        self._publish_map_update({
            **self._last_payload,
            "batch_count": count,
            "update_interval_s": self._map_interval_s,
        })

    def close(self) -> None:
        """Cancel delayed HA writes during shutdown."""
        self._closed = True
        for handle in (self._sensor_handle, self._map_handle):
            if handle is not None:
                handle.cancel()
        self._sensor_handle = None
        self._map_handle = None
        self._sensor_count = 0
        self._map_count = 0
        self._log_rows = []
