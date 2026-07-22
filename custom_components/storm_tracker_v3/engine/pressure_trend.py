"""Regionale luchtdruktrend uit individuele Netatmo-stations."""
from __future__ import annotations

from collections import defaultdict
from statistics import median


class PressureTrendTracker:
    """Bewaar korte stationshistoriek en bereken robuuste regionale trends."""

    WINDOWS_MIN = (15, 30, 60)
    MAX_HISTORY_S = 2 * 60 * 60
    MAX_TARGET_ERROR_S = 8 * 60
    MIN_STATIONS = 3
    MIN_WARMUP_MINUTES = 30
    MAX_SAMPLE_GAP_S = 15 * 60

    def __init__(self) -> None:
        self._history: dict[str, list[tuple[float, float]]] = defaultdict(list)

    def to_snapshot(self) -> dict:
        """Serialiseer de compacte stationshistoriek voor HA-opslag."""
        return {
            "stations": {
                station_id: [[timestamp, pressure] for timestamp, pressure in samples]
                for station_id, samples in self._history.items()
            }
        }

    def restore(self, snapshot: dict | None, now: float) -> int:
        """Herstel geldige recente samples en geef het stationsaantal terug."""
        cutoff = now - self.MAX_HISTORY_S
        restored = 0
        for station_id, raw_samples in (snapshot or {}).get("stations", {}).items():
            samples = []
            for raw in raw_samples:
                try:
                    timestamp, pressure = float(raw[0]), float(raw[1])
                except (TypeError, ValueError, IndexError):
                    continue
                if cutoff <= timestamp <= now and 850.0 <= pressure <= 1100.0:
                    samples.append((timestamp, pressure))
            if samples:
                self._history[str(station_id)] = sorted(samples)
                restored += 1
        return restored

    def update(self, observations: list, timestamp: float | None = None) -> dict:
        """Voeg een poll toe en geef de actuele regionale druktrend terug."""
        valid = [
            obs for obs in observations
            if getattr(obs, "station_id", None)
            and getattr(obs, "pressure", None) is not None
            and 850.0 <= float(obs.pressure) <= 1100.0
        ]
        if timestamp is None:
            timestamp = max((float(obs.timestamp) for obs in valid), default=0.0)

        cutoff = timestamp - self.MAX_HISTORY_S
        for station_id in list(self._history):
            samples = [sample for sample in self._history[station_id] if sample[0] >= cutoff]
            if samples:
                self._history[station_id] = samples
            else:
                del self._history[station_id]

        for obs in valid:
            samples = self._history[str(obs.station_id)]
            sample = (float(timestamp), float(obs.pressure))
            if samples and samples[-1][0] == sample[0]:
                samples[-1] = sample
            else:
                samples.append(sample)

        result = {
            "timestamp": timestamp or None,
            "pressure_station_count": len(valid),
            "median_pressure_hpa": round(median([float(o.pressure) for o in valid]), 1)
            if valid else None,
        }
        active_station_ids = {str(obs.station_id) for obs in valid}
        warm_station_ids = {
            station_id for station_id in active_station_ids
            if self._has_contiguous_warmup(
                self._history.get(station_id, []), timestamp
            )
        }
        result["warmup_complete"] = len(warm_station_ids) >= self.MIN_STATIONS
        result["warmup_stations"] = len(warm_station_ids)
        result["warmup_status"] = (
            "ready" if result["warmup_complete"] else "initializing"
        )
        for minutes in self.WINDOWS_MIN:
            deltas = self._station_deltas(timestamp, minutes, warm_station_ids)
            key = f"delta_{minutes}m_hpa"
            result[key] = round(median(deltas), 2) if len(deltas) >= self.MIN_STATIONS else None
            result[f"stations_{minutes}m"] = len(deltas)

        delta_60m = result["delta_60m_hpa"]
        result["trend"] = (
            self.classify(delta_60m)
            if result["warmup_complete"] else "onvoldoende_data"
        )
        result["rapid_fall"] = delta_60m is not None and delta_60m <= -2.0
        return result

    def _has_contiguous_warmup(
        self, samples: list[tuple[float, float]], now: float
    ) -> bool:
        """Vereis een recente, voldoende dichte meetreeks voor trendgebruik."""
        start = now - self.MIN_WARMUP_MINUTES * 60
        window = [sample for sample in samples if sample[0] >= start]
        if len(window) < 3 or window[0][0] > start + self.MAX_TARGET_ERROR_S:
            return False
        if window[-1][0] < now - 10 * 60:
            return False
        return all(
            current[0] - previous[0] <= self.MAX_SAMPLE_GAP_S
            for previous, current in zip(window, window[1:])
        )

    def _station_deltas(
        self, now: float, minutes: int, active_station_ids: set[str]
    ) -> list[float]:
        target = now - minutes * 60
        deltas = []
        for station_id in active_station_ids:
            samples = self._history.get(station_id, [])
            if len(samples) < 2 or samples[-1][0] < now - 10 * 60:
                continue
            past = min(samples[:-1], key=lambda sample: abs(sample[0] - target))
            if abs(past[0] - target) > self.MAX_TARGET_ERROR_S:
                continue
            delta = samples[-1][1] - past[1]
            if abs(delta) <= 15.0:
                deltas.append(delta)
        return deltas

    @staticmethod
    def classify(delta_60m: float | None) -> str:
        if delta_60m is None:
            return "onvoldoende_data"
        if delta_60m <= -2.0:
            return "snelle_daling"
        if delta_60m <= -1.0:
            return "dalend"
        if delta_60m >= 2.0:
            return "snelle_stijging"
        if delta_60m >= 1.0:
            return "stijgend"
        return "stabiel"
