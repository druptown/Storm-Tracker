"""Auditable shadow primitive for cell-specific ground precipitation fusion.

This module is deliberately isolated from the live runtime.  It turns raw,
interval-aware ground observations into explicit spatiotemporal assessments
against one immutable CellTrack footprint.  It never treats an unavailable,
stale, temporally unrelated or spatially unrelated observation as evidence
that precipitation is absent.

Provider aliases are resolved through an explicit physical-site catalogue.
Confirmation counts independent groups, not provider rows, so one airport
reported by both METAR and Synoptic can contribute at most one vote.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from ..geometry.distance import haversine
from ..geometry.polygon import point_to_polygon_distance_km


GROUND_FUSION_ALGORITHM = "stv3-celltrack-ground-fusion-v1"


class PrecipitationState(str, Enum):
    """What the upstream measurement actually says about precipitation."""

    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"
    UNAVAILABLE = "unavailable"


class MeasurementSemantics(str, Enum):
    """Temporal meaning of the reported precipitation field."""

    INSTANTANEOUS_STATE = "instantaneous_state"
    INSTANTANEOUS_RATE = "instantaneous_rate"
    INTERVAL_ACCUMULATION = "interval_accumulation"
    INTERVAL_MEAN_RATE = "interval_mean_rate"


class GroundMatchOutcome(str, Enum):
    """Primary, non-ambiguous outcome of one observation assessment."""

    MATCHED_WET = "matched_wet"
    MATCHED_DRY = "matched_dry"
    UNKNOWN_STATION_ALIAS = "unknown_station_alias"
    FRESHNESS_POLICY_MISSING = "freshness_policy_missing"
    OBSERVATION_UNAVAILABLE = "observation_unavailable"
    PRECIPITATION_UNKNOWN = "precipitation_unknown"
    OBSERVATION_FROM_FUTURE = "observation_from_future"
    OBSERVATION_STALE = "observation_stale"
    SITE_LOCATION_CONFLICT = "site_location_conflict"
    OUTSIDE_CELL_TIME = "outside_cell_time"
    INSUFFICIENT_INTERVAL_OVERLAP = "insufficient_interval_overlap"
    OUTSIDE_CELL_FOOTPRINT = "outside_cell_footprint"


class GroundEvidenceRole(str, Enum):
    """How an assessed row participates in an independent decision."""

    INDEPENDENT_WET = "independent_wet"
    INDEPENDENT_DRY = "independent_dry"
    DUPLICATE_INDEPENDENCE_GROUP = "duplicate_independence_group"
    CONFLICTED_INDEPENDENCE_GROUP = "conflicted_independence_group"
    INELIGIBLE = "ineligible"


class GroundConfirmationState(str, Enum):
    """Confirmation is intentionally not a binary weather conclusion."""

    CONFIRMED = "confirmed"
    NOT_CONFIRMED = "not_confirmed"
    INDETERMINATE = "indeterminate"


def _require_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} moet een niet-lege string zijn")
    if "\0" in value:
        raise ValueError(f"{field_name} bevat een ongeldig NUL-teken")
    return value.strip()


def _require_finite(value: float, field_name: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field_name} moet eindig zijn")
    return number


def _optional_sha256(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field_name} moet een lowercase SHA-256 zijn")
    return value


def _validate_coordinates(latitude: float, longitude: float) -> None:
    latitude = _require_finite(latitude, "latitude")
    longitude = _require_finite(longitude, "longitude")
    if not -90.0 <= latitude <= 90.0:
        raise ValueError("latitude moet tussen -90 en 90 graden liggen")
    if not -180.0 <= longitude <= 180.0:
        raise ValueError("longitude moet tussen -180 en 180 graden liggen")


def _alias_key(provider_id: str, station_id: str) -> tuple[str, str]:
    return (
        _require_identifier(provider_id, "provider_id").casefold(),
        _require_identifier(station_id, "station_id").casefold(),
    )


@dataclass(frozen=True, slots=True)
class StationAlias:
    """One provider identity belonging to exactly one physical site."""

    provider_id: str
    station_id: str
    mapping_fingerprint: str | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.provider_id, "provider_id")
        _require_identifier(self.station_id, "station_id")
        object.__setattr__(
            self,
            "mapping_fingerprint",
            _optional_sha256(
                self.mapping_fingerprint,
                "mapping_fingerprint",
            ),
        )

    @property
    def key(self) -> tuple[str, str]:
        return _alias_key(self.provider_id, self.station_id)


@dataclass(frozen=True, slots=True)
class PhysicalSite:
    """Immutable provider-independent identity of a measurement location."""

    physical_site_id: str
    latitude: float
    longitude: float
    independence_group: str
    aliases: tuple[StationAlias, ...]

    def __post_init__(self) -> None:
        _require_identifier(self.physical_site_id, "physical_site_id")
        _validate_coordinates(self.latitude, self.longitude)
        _require_identifier(self.independence_group, "independence_group")
        if not isinstance(self.aliases, tuple) or not self.aliases:
            raise ValueError("PhysicalSite vereist minstens een StationAlias")
        if any(not isinstance(alias, StationAlias) for alias in self.aliases):
            raise TypeError("aliases moeten StationAlias-objecten zijn")
        keys = [alias.key for alias in self.aliases]
        if len(set(keys)) != len(keys):
            raise ValueError("PhysicalSite bevat een dubbele provideralias")


@dataclass(frozen=True, slots=True)
class PhysicalSiteRegistry:
    """Immutable alias registry with fail-closed uniqueness checks."""

    sites: tuple[PhysicalSite, ...]
    registry_version: str
    catalog_fingerprint: str | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.registry_version, "registry_version")
        object.__setattr__(
            self,
            "catalog_fingerprint",
            _optional_sha256(
                self.catalog_fingerprint,
                "catalog_fingerprint",
            ),
        )
        if not isinstance(self.sites, tuple):
            raise TypeError("sites moet een tuple zijn om immutable te blijven")
        if any(not isinstance(site, PhysicalSite) for site in self.sites):
            raise TypeError("sites moeten PhysicalSite-objecten zijn")
        site_ids = [site.physical_site_id for site in self.sites]
        if len(set(site_ids)) != len(site_ids):
            raise ValueError("PhysicalSiteRegistry bevat dubbele site-ID's")
        owners: dict[tuple[str, str], str] = {}
        for site in self.sites:
            for alias in site.aliases:
                previous = owners.setdefault(alias.key, site.physical_site_id)
                if previous != site.physical_site_id:
                    raise ValueError(
                        "een provideralias verwijst naar meerdere fysieke sites"
                    )

    def resolve(
        self,
        provider_id: str,
        station_id: str,
    ) -> PhysicalSite | None:
        resolved = self.resolve_with_alias(provider_id, station_id)
        return resolved[0] if resolved is not None else None

    def resolve_with_alias(
        self,
        provider_id: str,
        station_id: str,
    ) -> tuple[PhysicalSite, StationAlias] | None:
        """Resolve the exact verified alias and retain its mapping proof."""
        key = _alias_key(provider_id, station_id)
        for site in self.sites:
            for alias in site.aliases:
                if alias.key == key:
                    return site, alias
        return None

    @property
    def registry_fingerprint(self) -> str:
        """Content fingerprint of the exact immutable registry projection."""
        payload = {
            "registry_version": self.registry_version,
            "catalog_fingerprint": self.catalog_fingerprint,
            "sites": [
                {
                    "physical_site_id": site.physical_site_id,
                    "latitude": float(site.latitude),
                    "longitude": float(site.longitude),
                    "independence_group": site.independence_group,
                    "aliases": [
                        {
                            "provider_id": alias.key[0],
                            "station_id": alias.key[1],
                            "mapping_fingerprint": alias.mapping_fingerprint,
                        }
                        for alias in sorted(
                            site.aliases,
                            key=lambda candidate: candidate.key,
                        )
                    ],
                }
                for site in sorted(
                    self.sites,
                    key=lambda candidate: candidate.physical_site_id,
                )
            ],
        }
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(
            b"stv3-ground-fusion-registry-v1\0" + canonical
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class RawGroundProvenance:
    """Stable, non-secret link back to the raw provider fact."""

    provider_record_id: str
    parser_version: str
    raw_artifact_receipt_id: str | None = None
    raw_content_sha256: str | None = None
    raw_text: str | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.provider_record_id, "provider_record_id")
        _require_identifier(self.parser_version, "parser_version")
        if self.raw_artifact_receipt_id is not None:
            _require_identifier(
                self.raw_artifact_receipt_id,
                "raw_artifact_receipt_id",
            )
        if self.raw_content_sha256 is not None:
            digest = self.raw_content_sha256
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError(
                    "raw_content_sha256 moet een lowercase SHA-256 hexwaarde zijn"
                )
        if self.raw_text is not None and not isinstance(self.raw_text, str):
            raise TypeError("raw_text moet tekst of None zijn")


@dataclass(frozen=True, slots=True)
class GroundObservation:
    """Immutable precipitation observation with its real temporal support."""

    observation_id: str
    provider_id: str
    station_id: str
    latitude: float
    longitude: float
    interval_start_at: float
    interval_end_at: float
    representative_at: float
    received_at: float
    precipitation_state: PrecipitationState
    measurement_semantics: MeasurementSemantics
    provenance: RawGroundProvenance
    amount: float | None = None
    measurement_unit: str | None = None
    quality_flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.observation_id, "observation_id")
        _require_identifier(self.provider_id, "provider_id")
        _require_identifier(self.station_id, "station_id")
        _validate_coordinates(self.latitude, self.longitude)
        start = _require_finite(self.interval_start_at, "interval_start_at")
        end = _require_finite(self.interval_end_at, "interval_end_at")
        representative = _require_finite(
            self.representative_at,
            "representative_at",
        )
        _require_finite(self.received_at, "received_at")
        if end < start:
            raise ValueError("meetinterval eindigt voor het begint")
        if not start <= representative <= end:
            raise ValueError(
                "representative_at moet binnen het echte meetinterval liggen"
            )
        if not isinstance(self.precipitation_state, PrecipitationState):
            raise TypeError("precipitation_state moet PrecipitationState zijn")
        if not isinstance(self.measurement_semantics, MeasurementSemantics):
            raise TypeError(
                "measurement_semantics moet MeasurementSemantics zijn"
            )
        interval_semantics = self.measurement_semantics in {
            MeasurementSemantics.INTERVAL_ACCUMULATION,
            MeasurementSemantics.INTERVAL_MEAN_RATE,
        }
        if interval_semantics and end <= start:
            raise ValueError("intervalmetingen vereisen een positieve duur")
        if not interval_semantics and end != start:
            raise ValueError(
                "instantane metingen moeten een puntinterval gebruiken"
            )
        if not isinstance(self.provenance, RawGroundProvenance):
            raise TypeError("provenance moet RawGroundProvenance zijn")
        if self.amount is not None:
            amount = _require_finite(self.amount, "amount")
            if amount < 0.0:
                raise ValueError("amount mag niet negatief zijn")
            _require_identifier(self.measurement_unit or "", "measurement_unit")
            if self.precipitation_state in {
                PrecipitationState.UNKNOWN,
                PrecipitationState.UNAVAILABLE,
            }:
                raise ValueError(
                    "onbekende/onbeschikbare neerslag mag geen hoeveelheid dragen"
                )
        elif self.measurement_unit is not None:
            raise ValueError("measurement_unit zonder amount is niet toegestaan")
        if not isinstance(self.quality_flags, tuple) or any(
            not isinstance(flag, str) or not flag
            for flag in self.quality_flags
        ):
            raise TypeError("quality_flags moet een tuple niet-lege strings zijn")

    @property
    def interval_seconds(self) -> float:
        return self.interval_end_at - self.interval_start_at


@dataclass(frozen=True, slots=True)
class CellTrackFootprint:
    """One cell footprint at a bounded meteorological validity interval.

    Legacy ``rings`` are independent outer contours and therefore form a
    MultiPolygon without holes. New lossless callers use ``polygons`` where
    every polygon is ``(outer_ring, hole_ring, ...)``. Exactly one geometry
    representation must be present.
    """

    footprint_id: str
    cell_track_id: str
    cell_detection_id: str
    valid_from: float
    valid_until: float
    representative_at: float
    rings: tuple[tuple[tuple[float, float], ...], ...]
    radar_source: str
    raw_artifact_receipt_id: str | None = None
    polygons: (
        tuple[
            tuple[
                tuple[tuple[float, float], ...],
                ...,
            ],
            ...,
        ]
        | None
    ) = None

    def __post_init__(self) -> None:
        _require_identifier(self.footprint_id, "footprint_id")
        _require_identifier(self.cell_track_id, "cell_track_id")
        _require_identifier(self.cell_detection_id, "cell_detection_id")
        start = _require_finite(self.valid_from, "valid_from")
        end = _require_finite(self.valid_until, "valid_until")
        representative = _require_finite(
            self.representative_at,
            "representative_at",
        )
        if end < start:
            raise ValueError("footprint-geldigheid eindigt voor ze begint")
        if not start <= representative <= end:
            raise ValueError(
                "footprint representative_at moet binnen de geldigheid liggen"
            )
        _require_identifier(self.radar_source, "radar_source")
        if self.raw_artifact_receipt_id is not None:
            _require_identifier(
                self.raw_artifact_receipt_id,
                "raw_artifact_receipt_id",
            )
        if not isinstance(self.rings, tuple):
            raise TypeError("rings moet een immutable tuple zijn")
        if self.polygons is not None and not isinstance(self.polygons, tuple):
            raise TypeError("polygons moet een immutable tuple of None zijn")
        if self.polygons is None:
            if not self.rings:
                raise ValueError(
                    "CellTrackFootprint vereist exact één van rings of polygons"
                )
            polygon_rings = tuple((ring,) for ring in self.rings)
        else:
            if self.rings or not self.polygons:
                raise ValueError(
                    "CellTrackFootprint vereist exact één van rings of polygons"
                )
            polygon_rings = self.polygons
        for polygon in polygon_rings:
            if not isinstance(polygon, tuple) or not polygon:
                raise ValueError(
                    "iedere footprintpolygon vereist minstens een buitenring"
                )
            for ring in polygon:
                if not isinstance(ring, tuple) or len(ring) < 3:
                    raise ValueError(
                        "iedere footprintcontour vereist minstens drie punten"
                    )
                for point in ring:
                    if not isinstance(point, tuple) or len(point) != 2:
                        raise TypeError(
                            "footprintpunten moeten immutable "
                            "(lat, lon)-tuples zijn"
                        )
                    _validate_coordinates(point[0], point[1])
        first_point = polygon_rings[0][0][0]
        if point_to_polygon_distance_km(
            first_point[0],
            first_point[1],
            polygon_rings,
        ) is None:
            raise ValueError(
                "footprintgeometrie is topologisch ongeldig of ambigu"
            )

    @property
    def geometry(
        self,
    ) -> tuple[
        tuple[
            tuple[tuple[float, float], ...],
            ...,
        ],
        ...,
    ]:
        """Return a lossless immutable MultiPolygon representation."""

        if self.polygons is not None:
            return self.polygons
        return tuple((ring,) for ring in self.rings)


@dataclass(frozen=True, slots=True)
class GroundFusionPolicy:
    """All eligibility thresholds are explicit and replayable."""

    freshness_seconds_by_provider: tuple[tuple[str, float], ...]
    minimum_independent_wet_groups: int = 2
    spatial_tolerance_km: float = 0.0
    temporal_tolerance_seconds: float = 0.0
    minimum_interval_overlap_fraction: float = 0.5
    maximum_site_coordinate_disagreement_km: float = 1.0
    maximum_future_skew_seconds: float = 60.0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.freshness_seconds_by_provider, tuple)
            or any(
                not isinstance(item, tuple) or len(item) != 2
                for item in self.freshness_seconds_by_provider
            )
        ):
            raise TypeError(
                "freshness_seconds_by_provider moet een tuple paren zijn"
            )
        providers: list[str] = []
        for provider_id, seconds in self.freshness_seconds_by_provider:
            providers.append(
                _require_identifier(provider_id, "freshness provider").casefold()
            )
            if _require_finite(seconds, "freshness seconds") <= 0.0:
                raise ValueError("freshness seconds moet positief zijn")
        if len(set(providers)) != len(providers):
            raise ValueError("freshnessbeleid bevat een dubbele provider")
        if (
            not isinstance(self.minimum_independent_wet_groups, int)
            or isinstance(self.minimum_independent_wet_groups, bool)
            or self.minimum_independent_wet_groups <= 0
        ):
            raise ValueError(
                "minimum_independent_wet_groups moet een positief geheel getal zijn"
            )
        for value, field_name in (
            (self.spatial_tolerance_km, "spatial_tolerance_km"),
            (
                self.temporal_tolerance_seconds,
                "temporal_tolerance_seconds",
            ),
            (
                self.maximum_site_coordinate_disagreement_km,
                "maximum_site_coordinate_disagreement_km",
            ),
            (
                self.maximum_future_skew_seconds,
                "maximum_future_skew_seconds",
            ),
        ):
            if _require_finite(value, field_name) < 0.0:
                raise ValueError(f"{field_name} mag niet negatief zijn")
        fraction = _require_finite(
            self.minimum_interval_overlap_fraction,
            "minimum_interval_overlap_fraction",
        )
        if not 0.0 <= fraction <= 1.0:
            raise ValueError(
                "minimum_interval_overlap_fraction moet tussen 0 en 1 liggen"
            )

    def freshness_seconds(self, provider_id: str) -> float | None:
        key = _require_identifier(provider_id, "provider_id").casefold()
        return next(
            (
                float(seconds)
                for provider, seconds in self.freshness_seconds_by_provider
                if provider.casefold() == key
            ),
            None,
        )


@dataclass(frozen=True, slots=True)
class GroundMatchAssessment:
    """Complete audit record for one observation against one cell."""

    observation_id: str
    provider_id: str
    station_id: str
    representative_at: float
    physical_site_id: str | None
    independence_group: str | None
    cell_track_id: str
    cell_detection_id: str
    outcome: GroundMatchOutcome
    usable: bool
    supports_precipitation: bool | None
    age_seconds: float
    temporal_overlap_seconds: float
    temporal_overlap_fraction: float
    distance_to_footprint_km: float | None
    reported_to_site_distance_km: float | None
    reason_codes: tuple[str, ...]
    provenance: RawGroundProvenance
    registry_version: str
    registry_fingerprint: str
    catalog_fingerprint: str | None
    mapping_fingerprint: str | None
    algorithm_version: str = GROUND_FUSION_ALGORITHM


@dataclass(frozen=True, slots=True)
class GroundEvidenceContribution:
    """Independent-vote role of one already assessed observation."""

    observation_id: str
    physical_site_id: str | None
    independence_group: str | None
    role: GroundEvidenceRole
    reason: str


@dataclass(frozen=True, slots=True)
class GroundConfirmationDecision:
    """Cell-specific decision retaining every used and excluded input."""

    decision_key: str
    target_id: str
    region_key: str
    evaluated_at: float
    cell_track_id: str
    cell_detection_id: str
    state: GroundConfirmationState
    confirmed: bool
    required_independent_wet_groups: int
    independent_wet_groups: tuple[str, ...]
    independent_dry_groups: tuple[str, ...]
    conflicted_groups: tuple[str, ...]
    reason_codes: tuple[str, ...]
    assessments: tuple[GroundMatchAssessment, ...]
    contributions: tuple[GroundEvidenceContribution, ...]
    registry_version: str
    registry_fingerprint: str
    catalog_fingerprint: str | None
    mapping_fingerprints: tuple[str, ...]
    algorithm_version: str = GROUND_FUSION_ALGORITHM


def _temporal_overlap(
    observation: GroundObservation,
    footprint: CellTrackFootprint,
    tolerance_seconds: float,
) -> tuple[float, float, bool]:
    supported_from = footprint.valid_from - tolerance_seconds
    supported_until = footprint.valid_until + tolerance_seconds
    if observation.interval_seconds == 0.0:
        matched = (
            supported_from
            <= observation.representative_at
            <= supported_until
        )
        return 0.0, 1.0 if matched else 0.0, matched
    overlap = max(
        0.0,
        min(observation.interval_end_at, supported_until)
        - max(observation.interval_start_at, supported_from),
    )
    return overlap, overlap / observation.interval_seconds, overlap > 0.0


def assess_ground_observation(
    observation: GroundObservation,
    registry: PhysicalSiteRegistry,
    footprint: CellTrackFootprint,
    *,
    evaluated_at: float,
    policy: GroundFusionPolicy,
) -> GroundMatchAssessment:
    """Assess one fact without converting missing evidence into dry evidence."""

    if not isinstance(observation, GroundObservation):
        raise TypeError("observation moet GroundObservation zijn")
    if not isinstance(registry, PhysicalSiteRegistry):
        raise TypeError("registry moet PhysicalSiteRegistry zijn")
    if not isinstance(footprint, CellTrackFootprint):
        raise TypeError("footprint moet CellTrackFootprint zijn")
    if not isinstance(policy, GroundFusionPolicy):
        raise TypeError("policy moet GroundFusionPolicy zijn")
    evaluated_at = _require_finite(evaluated_at, "evaluated_at")

    resolution = registry.resolve_with_alias(
        observation.provider_id,
        observation.station_id,
    )
    site = resolution[0] if resolution is not None else None
    alias = resolution[1] if resolution is not None else None
    location_latitude = (
        site.latitude if site is not None else observation.latitude
    )
    location_longitude = (
        site.longitude if site is not None else observation.longitude
    )
    site_distance = (
        haversine(
            observation.latitude,
            observation.longitude,
            site.latitude,
            site.longitude,
        )
        if site is not None
        else None
    )
    footprint_distance = point_to_polygon_distance_km(
        location_latitude,
        location_longitude,
        footprint.geometry,
    )
    overlap_seconds, overlap_fraction, has_temporal_overlap = (
        _temporal_overlap(
            observation,
            footprint,
            policy.temporal_tolerance_seconds,
        )
    )
    freshness_seconds = policy.freshness_seconds(observation.provider_id)
    age_seconds = evaluated_at - observation.interval_end_at

    reasons: list[str] = []
    if site is None:
        reasons.append(GroundMatchOutcome.UNKNOWN_STATION_ALIAS.value)
    if freshness_seconds is None:
        reasons.append(GroundMatchOutcome.FRESHNESS_POLICY_MISSING.value)
    if observation.precipitation_state is PrecipitationState.UNAVAILABLE:
        reasons.append(GroundMatchOutcome.OBSERVATION_UNAVAILABLE.value)
    elif observation.precipitation_state is PrecipitationState.UNKNOWN:
        reasons.append(GroundMatchOutcome.PRECIPITATION_UNKNOWN.value)
    if (
        observation.interval_start_at
        > evaluated_at + policy.maximum_future_skew_seconds
    ):
        reasons.append(GroundMatchOutcome.OBSERVATION_FROM_FUTURE.value)
    if freshness_seconds is not None and age_seconds > freshness_seconds:
        reasons.append(GroundMatchOutcome.OBSERVATION_STALE.value)
    if (
        site_distance is not None
        and site_distance
        > policy.maximum_site_coordinate_disagreement_km
    ):
        reasons.append(GroundMatchOutcome.SITE_LOCATION_CONFLICT.value)
    if not has_temporal_overlap:
        reasons.append(GroundMatchOutcome.OUTSIDE_CELL_TIME.value)
    elif (
        observation.interval_seconds > 0.0
        and overlap_fraction
        < policy.minimum_interval_overlap_fraction
    ):
        reasons.append(
            GroundMatchOutcome.INSUFFICIENT_INTERVAL_OVERLAP.value
        )
    if (
        footprint_distance is None
        or footprint_distance > policy.spatial_tolerance_km
    ):
        reasons.append(GroundMatchOutcome.OUTSIDE_CELL_FOOTPRINT.value)

    precedence = (
        GroundMatchOutcome.UNKNOWN_STATION_ALIAS,
        GroundMatchOutcome.FRESHNESS_POLICY_MISSING,
        GroundMatchOutcome.OBSERVATION_UNAVAILABLE,
        GroundMatchOutcome.PRECIPITATION_UNKNOWN,
        GroundMatchOutcome.OBSERVATION_FROM_FUTURE,
        GroundMatchOutcome.OBSERVATION_STALE,
        GroundMatchOutcome.SITE_LOCATION_CONFLICT,
        GroundMatchOutcome.OUTSIDE_CELL_TIME,
        GroundMatchOutcome.INSUFFICIENT_INTERVAL_OVERLAP,
        GroundMatchOutcome.OUTSIDE_CELL_FOOTPRINT,
    )
    outcome = next(
        (candidate for candidate in precedence if candidate.value in reasons),
        None,
    )
    if outcome is None:
        if observation.precipitation_state is PrecipitationState.PRESENT:
            outcome = GroundMatchOutcome.MATCHED_WET
            reasons.append("matched_precipitation_present")
            support: bool | None = True
        else:
            outcome = GroundMatchOutcome.MATCHED_DRY
            reasons.append("matched_precipitation_absent")
            support = False
        usable = True
    else:
        usable = False
        support = None

    return GroundMatchAssessment(
        observation_id=observation.observation_id,
        provider_id=observation.provider_id,
        station_id=observation.station_id,
        representative_at=observation.representative_at,
        physical_site_id=(
            site.physical_site_id if site is not None else None
        ),
        independence_group=(
            site.independence_group if site is not None else None
        ),
        cell_track_id=footprint.cell_track_id,
        cell_detection_id=footprint.cell_detection_id,
        outcome=outcome,
        usable=usable,
        supports_precipitation=support,
        age_seconds=age_seconds,
        temporal_overlap_seconds=overlap_seconds,
        temporal_overlap_fraction=overlap_fraction,
        distance_to_footprint_km=footprint_distance,
        reported_to_site_distance_km=site_distance,
        reason_codes=tuple(reasons),
        provenance=observation.provenance,
        registry_version=registry.registry_version,
        registry_fingerprint=registry.registry_fingerprint,
        catalog_fingerprint=registry.catalog_fingerprint,
        mapping_fingerprint=(
            alias.mapping_fingerprint if alias is not None else None
        ),
    )


def decide_ground_confirmation(
    assessments: Iterable[GroundMatchAssessment],
    *,
    decision_key: str,
    target_id: str,
    region_key: str,
    evaluated_at: float,
    footprint: CellTrackFootprint,
    registry: PhysicalSiteRegistry,
    policy: GroundFusionPolicy,
) -> GroundConfirmationDecision:
    """Count independent physical groups for exactly one cell track."""

    _require_identifier(decision_key, "decision_key")
    _require_identifier(target_id, "target_id")
    _require_identifier(region_key, "region_key")
    evaluated_at = _require_finite(evaluated_at, "evaluated_at")
    if not isinstance(footprint, CellTrackFootprint):
        raise TypeError("footprint moet CellTrackFootprint zijn")
    if not isinstance(registry, PhysicalSiteRegistry):
        raise TypeError("registry moet PhysicalSiteRegistry zijn")
    if not isinstance(policy, GroundFusionPolicy):
        raise TypeError("policy moet GroundFusionPolicy zijn")
    records = tuple(assessments)
    if any(not isinstance(item, GroundMatchAssessment) for item in records):
        raise TypeError("assessments moeten GroundMatchAssessment-objecten zijn")
    observation_ids = [item.observation_id for item in records]
    if len(set(observation_ids)) != len(observation_ids):
        raise ValueError("dezelfde observation_id komt meermaals in de beslissing")
    if any(
        item.cell_track_id != footprint.cell_track_id
        or item.cell_detection_id != footprint.cell_detection_id
        for item in records
    ):
        raise ValueError(
            "alle ground assessments moeten bij exact dezelfde cel horen"
        )
    if any(
        item.registry_version != registry.registry_version
        or item.registry_fingerprint != registry.registry_fingerprint
        or item.catalog_fingerprint != registry.catalog_fingerprint
        for item in records
    ):
        raise ValueError(
            "ground assessments gebruiken niet exact dezelfde sitecatalogus"
        )

    contributions: dict[str, GroundEvidenceContribution] = {}
    usable_by_group: dict[str, list[GroundMatchAssessment]] = {}
    for item in records:
        if not item.usable:
            contributions[item.observation_id] = GroundEvidenceContribution(
                observation_id=item.observation_id,
                physical_site_id=item.physical_site_id,
                independence_group=item.independence_group,
                role=GroundEvidenceRole.INELIGIBLE,
                reason=item.outcome.value,
            )
            continue
        if item.independence_group is None:
            raise AssertionError(
                "bruikbare ground evidence mist een independence_group"
            )
        usable_by_group.setdefault(item.independence_group, []).append(item)

    wet_groups: list[str] = []
    dry_groups: list[str] = []
    conflicted_groups: list[str] = []
    for group_id in sorted(usable_by_group):
        group = usable_by_group[group_id]
        states = {item.supports_precipitation for item in group}
        if states == {True, False}:
            conflicted_groups.append(group_id)
            for item in group:
                contributions[item.observation_id] = GroundEvidenceContribution(
                    observation_id=item.observation_id,
                    physical_site_id=item.physical_site_id,
                    independence_group=group_id,
                    role=GroundEvidenceRole.CONFLICTED_INDEPENDENCE_GROUP,
                    reason="wet_and_dry_evidence_in_same_independence_group",
                )
            continue

        selected = max(
            group,
            key=lambda item: (
                item.representative_at,
                item.observation_id,
            ),
        )
        wet = selected.supports_precipitation is True
        if wet:
            wet_groups.append(group_id)
            selected_role = GroundEvidenceRole.INDEPENDENT_WET
        else:
            dry_groups.append(group_id)
            selected_role = GroundEvidenceRole.INDEPENDENT_DRY
        for item in group:
            if item is selected:
                role = selected_role
                reason = "selected_latest_independent_group_evidence"
            else:
                role = GroundEvidenceRole.DUPLICATE_INDEPENDENCE_GROUP
                reason = "same_independence_group_already_counted"
            contributions[item.observation_id] = GroundEvidenceContribution(
                observation_id=item.observation_id,
                physical_site_id=item.physical_site_id,
                independence_group=group_id,
                role=role,
                reason=reason,
            )

    if len(wet_groups) >= policy.minimum_independent_wet_groups:
        state = GroundConfirmationState.CONFIRMED
        reasons = ["minimum_independent_wet_groups_met"]
    elif usable_by_group:
        state = GroundConfirmationState.NOT_CONFIRMED
        reasons = ["insufficient_independent_wet_groups"]
        if dry_groups:
            reasons.append("matched_dry_ground_evidence")
        if conflicted_groups:
            reasons.append("conflicting_ground_evidence")
    else:
        state = GroundConfirmationState.INDETERMINATE
        reasons = ["no_usable_spatiotemporal_ground_evidence"]
        reasons.extend(
            f"excluded:{outcome}"
            for outcome in sorted({
                item.outcome.value for item in records
            })
        )

    return GroundConfirmationDecision(
        decision_key=decision_key,
        target_id=target_id,
        region_key=region_key,
        evaluated_at=evaluated_at,
        cell_track_id=footprint.cell_track_id,
        cell_detection_id=footprint.cell_detection_id,
        state=state,
        confirmed=state is GroundConfirmationState.CONFIRMED,
        required_independent_wet_groups=(
            policy.minimum_independent_wet_groups
        ),
        independent_wet_groups=tuple(wet_groups),
        independent_dry_groups=tuple(dry_groups),
        conflicted_groups=tuple(conflicted_groups),
        reason_codes=tuple(reasons),
        assessments=records,
        contributions=tuple(
            contributions[observation_id]
            for observation_id in sorted(contributions)
        ),
        registry_version=registry.registry_version,
        registry_fingerprint=registry.registry_fingerprint,
        catalog_fingerprint=registry.catalog_fingerprint,
        mapping_fingerprints=tuple(sorted({
            item.mapping_fingerprint
            for item in records
            if item.mapping_fingerprint is not None
        })),
    )


def fuse_ground_observations(
    observations: Iterable[GroundObservation],
    registry: PhysicalSiteRegistry,
    footprint: CellTrackFootprint,
    *,
    decision_key: str,
    target_id: str,
    region_key: str,
    evaluated_at: float,
    policy: GroundFusionPolicy,
) -> GroundConfirmationDecision:
    """Convenience entry point retaining all rows, including exclusions."""

    observations = tuple(observations)
    observation_ids = [item.observation_id for item in observations]
    if len(set(observation_ids)) != len(observation_ids):
        raise ValueError("observations bevatten dubbele observation_id's")
    assessments = tuple(
        assess_ground_observation(
            observation,
            registry,
            footprint,
            evaluated_at=evaluated_at,
            policy=policy,
        )
        for observation in observations
    )
    return decide_ground_confirmation(
        assessments,
        decision_key=decision_key,
        target_id=target_id,
        region_key=region_key,
        evaluated_at=evaluated_at,
        footprint=footprint,
        registry=registry,
        policy=policy,
    )
