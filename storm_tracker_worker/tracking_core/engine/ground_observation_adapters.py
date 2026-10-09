"""Fail-closed adapters from worker dictionaries to ground-fusion facts.

The worker payloads are transport records, not yet meteorological
observations.  This shadow-only module splits every transport record into
facts with one temporal meaning each:

* METAR present weather is a point observation.  Vicinity weather is never
  precipitation at the station.  ``Prrrr`` is kept separate and is only
  emitted when both ends of its real accumulation interval are known.
* Synoptic ``precip_intervals`` remain separate per sensor set and interval.
  They are never summed or converted to an instantaneous state.
* Netatmo ``rain_live`` is an instantaneous rate while ``rain_60min`` is a
  one-hour interval accumulation.  Every rain module remains a separate
  provider fact.

Missing temporal support, sensor status, malformed provenance or an older
lossy payload can yield UNKNOWN/UNAVAILABLE evidence, but never fabricated
dry evidence.  Physical-site objects returned here are explicitly unverified
candidates based on one provider identity.  Cross-provider aliases must be
verified by a catalogue before they are merged into one site.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .ground_fusion import (
    GroundObservation,
    MeasurementSemantics,
    PhysicalSite,
    PrecipitationState,
    RawGroundProvenance,
    StationAlias,
)


GROUND_ADAPTER_ALGORITHM = "stv3-ground-observation-adapters-v1"
_PROVIDERS = {"metar", "synoptic", "netatmo"}
_METAR_FAIL_CLOSED_FLAGS = {
    "PWINO",
    "PNO",
    "FZRANO",
    "TSNO",
    "maintenance_required",
}


class GroundAdapterIssueState(str, Enum):
    """Meteorological meaning of a fact which could not be emitted."""

    UNKNOWN = "unknown"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class GroundAdapterIssue:
    """Auditable reason why an input fact was unavailable or ambiguous."""

    provider_id: str
    station_id: str
    fact_kind: str
    code: str
    state: GroundAdapterIssueState
    detail: str


@dataclass(frozen=True, slots=True)
class PhysicalSiteCandidate:
    """Unverified one-provider candidate for a physical measurement site."""

    site: PhysicalSite
    station_alias: StationAlias
    candidate_basis: str = "provider_station_identity_only_unverified"


@dataclass(frozen=True, slots=True)
class AdaptedGroundFact:
    """One immutable ground fact plus the canonical worker record snapshot."""

    observation: GroundObservation
    fact_kind: str
    sensor_id: str | None
    provider_record_sha256: str
    raw_payload_json: str


@dataclass(frozen=True, slots=True)
class GroundAdapterBatch:
    """Deterministic output of one or more worker transport records."""

    facts: tuple[AdaptedGroundFact, ...]
    site_candidates: tuple[PhysicalSiteCandidate, ...]
    issues: tuple[GroundAdapterIssue, ...]
    algorithm_version: str = GROUND_ADAPTER_ALGORITHM

    @property
    def observations(self) -> tuple[GroundObservation, ...]:
        return tuple(fact.observation for fact in self.facts)

    @property
    def station_aliases(self) -> tuple[StationAlias, ...]:
        return tuple(
            candidate.station_alias for candidate in self.site_candidates
        )


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _deduplicated_flags(*groups: Iterable[Any]) -> tuple[str, ...]:
    result: list[str] = []
    for group in groups:
        values = (group,) if isinstance(group, str) else group
        for value in values:
            text = str(value).strip()
            if text and text not in result:
                result.append(text)
    return tuple(result)


def _provider(record: Mapping[str, Any]) -> str:
    provider = str(record.get("source") or "").strip().casefold()
    if provider not in _PROVIDERS:
        raise ValueError(f"niet-ondersteunde groundprovider: {provider!r}")
    return provider


def _station_id(
    record: Mapping[str, Any],
    provider: str,
) -> str:
    raw_payload = record.get("raw_payload")
    raw_payload = raw_payload if isinstance(raw_payload, Mapping) else {}
    provider_station = (
        raw_payload.get("station_id")
        if provider in {"metar", "netatmo"}
        else raw_payload.get("stid")
    )
    station = str(provider_station or record.get("station_id") or "").strip()
    prefix = f"{provider}:"
    if station.casefold().startswith(prefix):
        station = station[len(prefix):]
    if provider in {"metar", "synoptic"}:
        station = station.upper()
    if not station or "\0" in station:
        raise ValueError("groundrecord mist een geldige station-ID")
    return station


def _candidate(
    provider: str,
    station_id: str,
    latitude: float,
    longitude: float,
) -> PhysicalSiteCandidate:
    alias = StationAlias(provider_id=provider, station_id=station_id)
    site_digest = hashlib.sha256(
        f"{provider}\0{station_id.casefold()}".encode("utf-8")
    ).hexdigest()[:24]
    # An exact shared ICAO/STID is an identity signal, not a proximity
    # heuristic.  Keep the physical sites separate until catalogue
    # verification, but let the exact shared identifier declare correlation
    # so METAR and Synoptic cannot double-vote in the meantime.
    if provider in {"metar", "synoptic"}:
        group_digest = hashlib.sha256(
            f"aviation-station\0{station_id.casefold()}".encode("utf-8")
        ).hexdigest()[:24]
        independence_group = f"candidate-aviation-group:{group_digest}"
    else:
        independence_group = f"candidate-group:{provider}:{site_digest}"
    site = PhysicalSite(
        physical_site_id=f"candidate-site:{provider}:{site_digest}",
        latitude=latitude,
        longitude=longitude,
        independence_group=independence_group,
        aliases=(alias,),
    )
    return PhysicalSiteCandidate(site=site, station_alias=alias)


def _provenance(
    record: Mapping[str, Any],
    *,
    provider: str,
    station_id: str,
    fact_kind: str,
    fact_discriminator: str,
) -> tuple[RawGroundProvenance, str, str]:
    parser_version = str(
        record.get("parser_version") or "unversioned-worker-payload"
    ).strip()
    raw_payload = record.get("raw_payload")
    raw_payload = raw_payload if isinstance(raw_payload, Mapping) else {}
    snapshot = {
        "worker_observation": record,
        "selected_fact": {
            "fact_kind": fact_kind,
            "fact_discriminator": fact_discriminator,
        },
    }
    raw_payload_json = _canonical_json(snapshot)
    provider_record_sha256 = hashlib.sha256(
        raw_payload_json.encode("utf-8")
    ).hexdigest()
    receipt_id = (
        record.get("raw_artifact_receipt_id")
        or raw_payload.get("raw_artifact_receipt_id")
    )
    raw_sha256 = (
        record.get("raw_content_sha256")
        or raw_payload.get("raw_content_sha256")
    )
    raw_sha256 = str(raw_sha256).strip().lower() if raw_sha256 else None
    if raw_sha256 is not None and (
        len(raw_sha256) != 64
        or any(character not in "0123456789abcdef" for character in raw_sha256)
    ):
        raw_sha256 = None
    raw_text = (
        record.get("raw_text")
        or raw_payload.get("raw_text")
    )
    provenance = RawGroundProvenance(
        provider_record_id=(
            f"{provider}:{station_id}:{fact_kind}:{provider_record_sha256}"
        ),
        parser_version=parser_version,
        raw_artifact_receipt_id=(
            str(receipt_id).strip() if receipt_id else None
        ),
        raw_content_sha256=raw_sha256,
        raw_text=str(raw_text) if raw_text is not None else None,
    )
    return provenance, provider_record_sha256, raw_payload_json


def _fact(
    record: Mapping[str, Any],
    *,
    provider: str,
    station_id: str,
    latitude: float,
    longitude: float,
    received_at: float,
    fact_kind: str,
    fact_discriminator: str,
    sensor_id: str | None,
    interval_start_at: float,
    interval_end_at: float,
    representative_at: float,
    state: PrecipitationState,
    semantics: MeasurementSemantics,
    amount: float | None,
    unit: str | None,
    quality_flags: tuple[str, ...],
) -> AdaptedGroundFact:
    provenance, record_sha256, raw_payload_json = _provenance(
        record,
        provider=provider,
        station_id=station_id,
        fact_kind=fact_kind,
        fact_discriminator=fact_discriminator,
    )
    observation = GroundObservation(
        observation_id=f"ground:{provenance.provider_record_id}",
        provider_id=provider,
        station_id=station_id,
        latitude=latitude,
        longitude=longitude,
        interval_start_at=interval_start_at,
        interval_end_at=interval_end_at,
        representative_at=representative_at,
        received_at=received_at,
        precipitation_state=state,
        measurement_semantics=semantics,
        provenance=provenance,
        amount=amount,
        measurement_unit=unit,
        quality_flags=quality_flags,
    )
    return AdaptedGroundFact(
        observation=observation,
        fact_kind=fact_kind,
        sensor_id=sensor_id,
        provider_record_sha256=record_sha256,
        raw_payload_json=raw_payload_json,
    )


def _issue(
    provider: str,
    station_id: str,
    fact_kind: str,
    code: str,
    state: GroundAdapterIssueState,
    detail: str,
) -> GroundAdapterIssue:
    return GroundAdapterIssue(
        provider_id=provider,
        station_id=station_id,
        fact_kind=fact_kind,
        code=code,
        state=state,
        detail=detail,
    )


def _common(
    record: Mapping[str, Any],
) -> tuple[str, str, float, float, float, float]:
    provider = _provider(record)
    station_id = _station_id(record, provider)
    latitude = _finite(record.get("lat"))
    longitude = _finite(record.get("lon"))
    timestamp = _finite(record.get("timestamp"))
    received_at = _finite(record.get("received_at"))
    if latitude is None or longitude is None:
        raise ValueError("groundrecord mist geldige coördinaten")
    if timestamp is None:
        raise ValueError("groundrecord mist een geldige meettijd")
    if received_at is None:
        raise ValueError("groundrecord mist een geldige ontvangsttijd")
    return (
        provider,
        station_id,
        latitude,
        longitude,
        timestamp,
        received_at,
    )


def _adapt_metar(
    record: Mapping[str, Any],
) -> tuple[list[AdaptedGroundFact], list[GroundAdapterIssue]]:
    (
        provider,
        station_id,
        latitude,
        longitude,
        timestamp,
        received_at,
    ) = _common(record)
    raw = record.get("raw_payload")
    raw = raw if isinstance(raw, Mapping) else {}
    groups = raw.get("weather_groups")
    facts: list[AdaptedGroundFact] = []
    issues: list[GroundAdapterIssue] = []
    flags = _deduplicated_flags(
        record.get("quality_flags") or (),
        raw.get("sensor_status_flags") or (),
        ("maintenance_required",)
        if raw.get("maintenance_required") else (),
    )
    local_wet = False
    vicinity_wet = False
    structured_groups_available = isinstance(groups, list)
    if structured_groups_available:
        for group in groups:
            if not isinstance(group, Mapping):
                structured_groups_available = False
                break
            precipitation = bool(group.get("precipitation"))
            local = bool(group.get("local_precipitation"))
            proximity = str(group.get("proximity") or "").casefold()
            local_wet = local_wet or (
                precipitation and local and proximity != "vicinity"
            )
            vicinity_wet = vicinity_wet or (
                precipitation and not local
            )

    if not structured_groups_available:
        state = PrecipitationState.UNKNOWN
        flags = _deduplicated_flags(
            flags,
            ("structured_present_weather_missing",),
        )
        issues.append(_issue(
            provider,
            station_id,
            "metar_present_weather",
            "structured_present_weather_missing",
            GroundAdapterIssueState.UNKNOWN,
            "Zonder weather_groups kan vicinity-neerslag niet veilig "
            "van lokale neerslag worden onderscheiden.",
        ))
    elif local_wet:
        state = PrecipitationState.PRESENT
    elif any(flag in _METAR_FAIL_CLOSED_FLAGS for flag in flags):
        state = (
            PrecipitationState.UNAVAILABLE
            if "PWINO" in flags
            else PrecipitationState.UNKNOWN
        )
    else:
        state = PrecipitationState.ABSENT

    if vicinity_wet:
        flags = _deduplicated_flags(flags, ("precipitation_in_vicinity",))
    if bool(record.get("precipitation_present")) != local_wet:
        flags = _deduplicated_flags(
            flags,
            ("transport_precipitation_flag_disagrees",),
        )
        issues.append(_issue(
            provider,
            station_id,
            "metar_present_weather",
            "transport_precipitation_flag_disagrees",
            GroundAdapterIssueState.UNKNOWN,
            "De adapter gebruikt de gestructureerde lokale weather group, "
            "niet de transportbooleaan.",
        ))
    facts.append(_fact(
        record,
        provider=provider,
        station_id=station_id,
        latitude=latitude,
        longitude=longitude,
        received_at=received_at,
        fact_kind="metar_present_weather",
        fact_discriminator=f"{timestamp:.6f}:present-weather",
        sensor_id="present_weather",
        interval_start_at=timestamp,
        interval_end_at=timestamp,
        representative_at=timestamp,
        state=state,
        semantics=MeasurementSemantics.INSTANTANEOUS_STATE,
        amount=None,
        unit=None,
        quality_flags=flags,
    ))

    accumulation = _finite(raw.get("hourly_precipitation_mm"))
    if accumulation is not None:
        interval_start = _finite(
            raw.get("hourly_precipitation_interval_start")
        )
        interval_end = _finite(
            raw.get("hourly_precipitation_interval_end")
        )
        accumulation_flags = _deduplicated_flags(
            flags,
            ("metar_prrrr_accumulation",),
        )
        if (
            interval_start is None
            or interval_end is None
            or interval_end <= interval_start
        ):
            issues.append(_issue(
                provider,
                station_id,
                "metar_interval_accumulation",
                "metar_accumulation_interval_start_unknown",
                GroundAdapterIssueState.UNAVAILABLE,
                "Prrrr betekent accumulatie sinds het vorige METAR-rapport; "
                "die vorige rapporttijd staat niet in de cachepayload.",
            ))
        else:
            failed = any(
                flag in _METAR_FAIL_CLOSED_FLAGS for flag in flags
            )
            facts.append(_fact(
                record,
                provider=provider,
                station_id=station_id,
                latitude=latitude,
                longitude=longitude,
                received_at=received_at,
                fact_kind="metar_interval_accumulation",
                fact_discriminator=(
                    f"{interval_start:.6f}:{interval_end:.6f}:Prrrr"
                ),
                sensor_id="precipitation_gauge",
                interval_start_at=interval_start,
                interval_end_at=interval_end,
                representative_at=interval_end,
                state=(
                    PrecipitationState.UNAVAILABLE
                    if failed
                    else PrecipitationState.PRESENT
                    if accumulation > 0.0
                    else PrecipitationState.ABSENT
                ),
                semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
                amount=None if failed else accumulation,
                unit=None if failed else "mm",
                quality_flags=accumulation_flags,
            ))
    return facts, issues


def _synoptic_qc_failed(
    raw: Mapping[str, Any],
    interval: Mapping[str, Any],
) -> bool:
    flagged = raw.get("qc_flagged")
    if not isinstance(flagged, Mapping):
        return bool(flagged) if isinstance(flagged, bool) else False
    field = str(interval.get("field") or "")
    values = flagged.get(field)
    index = interval.get("sample_index")
    if isinstance(values, list) and isinstance(index, int):
        return bool(values[index]) if 0 <= index < len(values) else False
    return bool(values) if isinstance(values, bool) else False


def _adapt_synoptic(
    record: Mapping[str, Any],
) -> tuple[list[AdaptedGroundFact], list[GroundAdapterIssue]]:
    (
        provider,
        station_id,
        latitude,
        longitude,
        timestamp,
        received_at,
    ) = _common(record)
    raw = record.get("raw_payload")
    raw = raw if isinstance(raw, Mapping) else {}
    intervals = raw.get("precipitation_intervals")
    intervals = intervals if isinstance(intervals, list) else []
    base_flags = _deduplicated_flags(record.get("quality_flags") or ())
    facts: list[AdaptedGroundFact] = []
    issues: list[GroundAdapterIssue] = []
    if not intervals:
        issues.append(_issue(
            provider,
            station_id,
            "synoptic_precip_interval",
            "synoptic_interval_payload_missing",
            GroundAdapterIssueState.UNAVAILABLE,
            "Alleen synoptic_interval_v3/v4 met afzonderlijke intervallen kan "
            "zonder tijdsaanname worden aangepast.",
        ))
        facts.append(_fact(
            record,
            provider=provider,
            station_id=station_id,
            latitude=latitude,
            longitude=longitude,
            received_at=received_at,
            fact_kind="synoptic_precip_interval_unavailable",
            fact_discriminator=f"{timestamp:.6f}:missing-interval-payload",
            sensor_id=None,
            interval_start_at=timestamp,
            interval_end_at=timestamp,
            representative_at=timestamp,
            state=PrecipitationState.UNAVAILABLE,
            semantics=MeasurementSemantics.INSTANTANEOUS_STATE,
            amount=None,
            unit=None,
            quality_flags=_deduplicated_flags(
                base_flags,
                ("synoptic_interval_payload_missing",),
            ),
        ))
        return facts, issues

    for ordinal, interval in enumerate(intervals):
        if not isinstance(interval, Mapping):
            issues.append(_issue(
                provider,
                station_id,
                "synoptic_precip_interval",
                "synoptic_interval_record_malformed",
                GroundAdapterIssueState.UNAVAILABLE,
                f"Intervalpositie {ordinal} is geen mapping.",
            ))
            continue
        field = str(interval.get("field") or "unknown_field")
        sensor_set = str(interval.get("sensor_set") or "unknown_sensor_set")
        sample_index = interval.get("sample_index")
        discriminator = f"{field}:{sample_index}:{ordinal}"
        start = _finite(interval.get("interval_start"))
        end = _finite(interval.get("interval_end"))
        amount = _finite(interval.get("value_mm"))
        qc_failed = _synoptic_qc_failed(raw, interval)
        interval_flags = _deduplicated_flags(
            base_flags,
            (f"synoptic_field:{field}", f"synoptic_sensor_set:{sensor_set}"),
            ("provider_qc_failed",) if qc_failed else (),
        )
        complete = bool(interval.get("interval_complete"))
        if (
            not complete
            or start is None
            or end is None
            or end <= start
            or amount is None
            or amount < 0.0
        ):
            marker_at = end if end is not None else timestamp
            issues.append(_issue(
                provider,
                station_id,
                "synoptic_precip_interval",
                "synoptic_interval_incomplete",
                GroundAdapterIssueState.UNAVAILABLE,
                f"{field}[{sample_index}] mist een volledig begin/einde "
                "of een geldige niet-negatieve hoeveelheid.",
            ))
            facts.append(_fact(
                record,
                provider=provider,
                station_id=station_id,
                latitude=latitude,
                longitude=longitude,
                received_at=received_at,
                fact_kind="synoptic_precip_interval_unavailable",
                fact_discriminator=f"{discriminator}:unavailable",
                sensor_id=sensor_set,
                interval_start_at=marker_at,
                interval_end_at=marker_at,
                representative_at=marker_at,
                state=PrecipitationState.UNAVAILABLE,
                semantics=MeasurementSemantics.INSTANTANEOUS_STATE,
                amount=None,
                unit=None,
                quality_flags=_deduplicated_flags(
                    interval_flags,
                    ("incomplete_precip_interval",),
                ),
            ))
            continue
        facts.append(_fact(
            record,
            provider=provider,
            station_id=station_id,
            latitude=latitude,
            longitude=longitude,
            received_at=received_at,
            fact_kind="synoptic_precip_interval",
            fact_discriminator=(
                f"{discriminator}:{start:.6f}:{end:.6f}"
            ),
            sensor_id=sensor_set,
            interval_start_at=start,
            interval_end_at=end,
            representative_at=end,
            state=(
                PrecipitationState.UNAVAILABLE
                if qc_failed
                else PrecipitationState.PRESENT
                if amount > 0.0
                else PrecipitationState.ABSENT
            ),
            semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
            amount=None if qc_failed else amount,
            unit=None if qc_failed else "mm",
            quality_flags=interval_flags,
        ))
    return facts, issues


def _adapt_netatmo(
    record: Mapping[str, Any],
) -> tuple[list[AdaptedGroundFact], list[GroundAdapterIssue]]:
    (
        provider,
        station_id,
        latitude,
        longitude,
        timestamp,
        received_at,
    ) = _common(record)
    raw = record.get("raw_payload")
    raw = raw if isinstance(raw, Mapping) else {}
    measurements = raw.get("rain_measurements")
    measurements = measurements if isinstance(measurements, list) else []
    base_flags = _deduplicated_flags(record.get("quality_flags") or ())
    facts: list[AdaptedGroundFact] = []
    issues: list[GroundAdapterIssue] = []
    if not measurements:
        issues.append(_issue(
            provider,
            station_id,
            "netatmo_rain_measurement",
            "netatmo_module_provenance_missing",
            GroundAdapterIssueState.UNAVAILABLE,
            "De geaggregeerde v2-velden verliezen module-identiteit en "
            "kunnen niet als afzonderlijke sensorfeiten dienen.",
        ))
        facts.append(_fact(
            record,
            provider=provider,
            station_id=station_id,
            latitude=latitude,
            longitude=longitude,
            received_at=received_at,
            fact_kind="netatmo_rain_measurement_unavailable",
            fact_discriminator=f"{timestamp:.6f}:missing-module-provenance",
            sensor_id=None,
            interval_start_at=timestamp,
            interval_end_at=timestamp,
            representative_at=timestamp,
            state=PrecipitationState.UNAVAILABLE,
            semantics=MeasurementSemantics.INSTANTANEOUS_STATE,
            amount=None,
            unit=None,
            quality_flags=_deduplicated_flags(
                base_flags,
                ("netatmo_module_provenance_missing",),
            ),
        ))
        return facts, issues

    for ordinal, measurement in enumerate(measurements):
        if not isinstance(measurement, Mapping):
            issues.append(_issue(
                provider,
                station_id,
                "netatmo_rain_measurement",
                "netatmo_module_record_malformed",
                GroundAdapterIssueState.UNAVAILABLE,
                f"Regenmodulepositie {ordinal} is geen mapping.",
            ))
            continue
        module_id = str(measurement.get("module_id") or "").strip()
        sensor_id = str(
            measurement.get("sensor_id") or module_id
        ).strip()
        measured_at = _finite(measurement.get("measurement_timestamp"))
        live = _finite(measurement.get("rain_live_mm_h"))
        hour = _finite(measurement.get("rain_60min_mm"))
        discriminator = f"{module_id}:{sensor_id}:{ordinal}"
        sensor_flags = _deduplicated_flags(
            base_flags,
            (
                f"netatmo_module:{module_id or 'unknown'}",
                f"netatmo_sensor:{sensor_id or 'unknown'}",
            ),
        )
        if not module_id or not sensor_id:
            issues.append(_issue(
                provider,
                station_id,
                "netatmo_rain_measurement",
                "netatmo_sensor_identity_missing",
                GroundAdapterIssueState.UNAVAILABLE,
                "Module- en sensor-ID zijn vereist voor een onafhankelijk "
                "providerfeit.",
            ))
            continue
        if live is not None:
            if measured_at is None or live < 0.0:
                marker_at = timestamp
                issues.append(_issue(
                    provider,
                    station_id,
                    "netatmo_rain_live",
                    "netatmo_live_timestamp_or_value_invalid",
                    GroundAdapterIssueState.UNAVAILABLE,
                    f"rain_live van module {module_id} mist een geldige "
                    "sensortijd of niet-negatieve waarde.",
                ))
                facts.append(_fact(
                    record,
                    provider=provider,
                    station_id=station_id,
                    latitude=latitude,
                    longitude=longitude,
                    received_at=received_at,
                    fact_kind="netatmo_rain_live_unavailable",
                    fact_discriminator=f"{discriminator}:live-unavailable",
                    sensor_id=sensor_id,
                    interval_start_at=marker_at,
                    interval_end_at=marker_at,
                    representative_at=marker_at,
                    state=PrecipitationState.UNAVAILABLE,
                    semantics=MeasurementSemantics.INSTANTANEOUS_STATE,
                    amount=None,
                    unit=None,
                    quality_flags=_deduplicated_flags(
                        sensor_flags,
                        ("sensor_timestamp_missing",),
                    ),
                ))
            else:
                facts.append(_fact(
                    record,
                    provider=provider,
                    station_id=station_id,
                    latitude=latitude,
                    longitude=longitude,
                    received_at=received_at,
                    fact_kind="netatmo_rain_live",
                    fact_discriminator=(
                        f"{discriminator}:{measured_at:.6f}:live"
                    ),
                    sensor_id=sensor_id,
                    interval_start_at=measured_at,
                    interval_end_at=measured_at,
                    representative_at=measured_at,
                    state=(
                        PrecipitationState.PRESENT
                        if live >= 0.1
                        else PrecipitationState.ABSENT
                    ),
                    semantics=MeasurementSemantics.INSTANTANEOUS_RATE,
                    amount=live,
                    unit="mm/h",
                    quality_flags=sensor_flags,
                ))
        if hour is not None:
            start = _finite(measurement.get("rain_60min_interval_start"))
            end = _finite(measurement.get("rain_60min_interval_end"))
            if (
                start is None
                or end is None
                or end <= start
                or hour < 0.0
            ):
                marker_at = measured_at if measured_at is not None else timestamp
                issues.append(_issue(
                    provider,
                    station_id,
                    "netatmo_rain_60min",
                    "netatmo_60min_interval_incomplete",
                    GroundAdapterIssueState.UNAVAILABLE,
                    f"rain_60min van module {module_id} mist zijn expliciete "
                    "begin- of eindtijd.",
                ))
                facts.append(_fact(
                    record,
                    provider=provider,
                    station_id=station_id,
                    latitude=latitude,
                    longitude=longitude,
                    received_at=received_at,
                    fact_kind="netatmo_rain_60min_unavailable",
                    fact_discriminator=f"{discriminator}:60min-unavailable",
                    sensor_id=sensor_id,
                    interval_start_at=marker_at,
                    interval_end_at=marker_at,
                    representative_at=marker_at,
                    state=PrecipitationState.UNAVAILABLE,
                    semantics=MeasurementSemantics.INSTANTANEOUS_STATE,
                    amount=None,
                    unit=None,
                    quality_flags=_deduplicated_flags(
                        sensor_flags,
                        ("interval_start_unknown",),
                    ),
                ))
            else:
                facts.append(_fact(
                    record,
                    provider=provider,
                    station_id=station_id,
                    latitude=latitude,
                    longitude=longitude,
                    received_at=received_at,
                    fact_kind="netatmo_rain_60min",
                    fact_discriminator=(
                        f"{discriminator}:{start:.6f}:{end:.6f}:60min"
                    ),
                    sensor_id=sensor_id,
                    interval_start_at=start,
                    interval_end_at=end,
                    representative_at=end,
                    state=(
                        PrecipitationState.PRESENT
                        if hour > 0.0
                        else PrecipitationState.ABSENT
                    ),
                    semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
                    amount=hour,
                    unit="mm",
                    quality_flags=sensor_flags,
                ))
    if not facts:
        issues.append(_issue(
            provider,
            station_id,
            "netatmo_rain_measurement",
            "netatmo_no_usable_rain_facts",
            GroundAdapterIssueState.UNAVAILABLE,
            "Geen module bevatte rain_live of rain_60min met bruikbare "
            "provenance.",
        ))
    return facts, issues


def adapt_worker_observation(
    record: Mapping[str, Any],
) -> GroundAdapterBatch:
    """Adapt one worker record without guessing missing meteorological time."""

    if not isinstance(record, Mapping):
        raise TypeError("worker-groundrecord moet een mapping zijn")
    common = _common(record)
    provider, station_id, latitude, longitude = common[:4]
    adapter = {
        "metar": _adapt_metar,
        "synoptic": _adapt_synoptic,
        "netatmo": _adapt_netatmo,
    }[provider]
    facts, issues = adapter(record)
    if record.get("usable_for_current_decision") is False:
        facts = []
        time_quality = str(record.get("time_quality") or "unknown")
        issues.append(_issue(
            provider,
            station_id,
            "worker_transport_record",
            "time_quality_quarantined",
            GroundAdapterIssueState.UNKNOWN,
            "De originele grondmeting blijft bewaard, maar mag volgens de "
            f"expliciete tijdkwaliteitsbeslissing ({time_quality}) niet als "
            "actuele nat/droog-evidence stemmen.",
        ))
    if not str(record.get("parser_version") or "").strip():
        issues.append(_issue(
            provider,
            station_id,
            "worker_transport_record",
            "parser_version_missing",
            GroundAdapterIssueState.UNKNOWN,
            "De workerrecord heeft geen reproduceerbare parser-versie.",
        ))
    raw = record.get("raw_payload")
    raw = raw if isinstance(raw, Mapping) else {}
    supplied_hash = (
        record.get("raw_content_sha256")
        or raw.get("raw_content_sha256")
    )
    if supplied_hash:
        digest = str(supplied_hash).strip().lower()
        if (
            len(digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in digest
            )
        ):
            issues.append(_issue(
                provider,
                station_id,
                "worker_transport_record",
                "raw_content_sha256_invalid",
                GroundAdapterIssueState.UNKNOWN,
                "De opgegeven upstream content-hash is ongeldig en werd "
                "niet als raw-artifactprovenance gebruikt.",
            ))
    return GroundAdapterBatch(
        facts=tuple(sorted(
            facts,
            key=lambda item: item.observation.observation_id,
        )),
        site_candidates=(
            _candidate(
                provider,
                station_id,
                latitude,
                longitude,
            ),
        ),
        issues=tuple(sorted(
            issues,
            key=lambda item: (
                item.provider_id,
                item.station_id.casefold(),
                item.fact_kind,
                item.code,
            ),
        )),
    )


def adapt_worker_observations(
    records: Iterable[Mapping[str, Any]],
) -> GroundAdapterBatch:
    """Adapt a collection with deterministic exact-candidate de-duplication."""

    facts: dict[str, AdaptedGroundFact] = {}
    candidates: dict[
        tuple[str, str, float, float],
        PhysicalSiteCandidate,
    ] = {}
    issues: list[GroundAdapterIssue] = []
    for record in records:
        batch = adapt_worker_observation(record)
        for fact in batch.facts:
            facts[fact.observation.observation_id] = fact
        issues.extend(batch.issues)
        for candidate in batch.site_candidates:
            alias = candidate.station_alias
            site = candidate.site
            key = (
                alias.provider_id.casefold(),
                alias.station_id.casefold(),
                site.latitude,
                site.longitude,
            )
            candidates[key] = candidate
    aliases: dict[tuple[str, str], set[tuple[float, float]]] = {}
    for provider_id, station_id, latitude, longitude in candidates:
        aliases.setdefault(
            (provider_id, station_id),
            set(),
        ).add((latitude, longitude))
    for (provider_id, station_id), coordinates in aliases.items():
        if len(coordinates) > 1:
            issues.append(_issue(
                provider_id,
                station_id,
                "physical_site_candidate",
                "provider_station_coordinate_conflict",
                GroundAdapterIssueState.UNKNOWN,
                "Dezelfde provideralias heeft meerdere coördinaten; de "
                "kandidaten blijven apart voor expliciete review.",
            ))
    return GroundAdapterBatch(
        facts=tuple(sorted(
            facts.values(),
            key=lambda item: item.observation.observation_id,
        )),
        site_candidates=tuple(
            candidates[key] for key in sorted(candidates)
        ),
        issues=tuple(sorted(
            issues,
            key=lambda item: (
                item.provider_id,
                item.station_id.casefold(),
                item.fact_kind,
                item.code,
            ),
        )),
    )
