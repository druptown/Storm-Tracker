"""Versioned, fail-closed catalogue for physical ground-observation sites.

This shadow-only module resolves unverified provider candidates against
explicitly verified alias mappings.  Coordinates are a consistency check,
never an identity heuristic.  Consequently, spatial proximity cannot create
or merge a physical site.
"""
from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from .ground_fusion import (
    PhysicalSite,
    PhysicalSiteRegistry,
    StationAlias,
)


PHYSICAL_SITE_CATALOG_ALGORITHM = "stv3-physical-site-catalog-v1"
EARTH_RADIUS_M = 6_371_008.8
_AVIATION_PROVIDERS = frozenset({"metar", "synoptic"})
_MAX_CANDIDATES_PER_BATCH = 100_000


class CatalogConflictCode(str, Enum):
    """Reasons why a catalogue is unsafe to construct."""

    DUPLICATE_SITE_KEY = "duplicate_site_key"
    DUPLICATE_ALIAS_MAPPING = "duplicate_alias_mapping"
    ALIAS_MAPS_TO_MULTIPLE_SITES = "alias_maps_to_multiple_sites"
    ALIAS_OUTSIDE_SITE_VALIDITY = "alias_outside_site_validity"
    SHARED_AVIATION_GROUP_CONFLICT = (
        "shared_aviation_identifier_independence_group_conflict"
    )
    SHARED_AUTHORITATIVE_ID_GROUP_CONFLICT = (
        "shared_authoritative_identifier_independence_group_conflict"
    )
    DUPLICATE_AUTHORITATIVE_IDENTIFIER = (
        "duplicate_authoritative_identifier"
    )
    DUPLICATE_CAPABILITY = "duplicate_sensor_capability"
    CAPABILITY_OUTSIDE_SITE_VALIDITY = (
        "sensor_capability_outside_site_validity"
    )
    DUPLICATE_SITING_ATTRIBUTE = "duplicate_siting_attribute"
    REPLACED_SITE_UNKNOWN = "replaced_site_unknown"
    REPLACEMENT_CYCLE = "replacement_cycle"
    REPLACEMENT_VALIDITY_OVERLAP = "replacement_validity_overlap"


class ResolutionStatus(str, Enum):
    """Fail-closed result of resolving one adapter candidate."""

    RESOLVED = "resolved"
    UNKNOWN_ALIAS = "unknown_alias"
    ALIAS_INACTIVE = "alias_inactive"
    COORDINATE_CONFLICT = "coordinate_conflict"
    CATALOG_CONFLICT = "catalog_conflict"
    INVALID_CANDIDATE = "invalid_candidate"


class CatalogAuditCode(str, Enum):
    """Stable audit codes emitted while resolving candidates."""

    RESOLVED_VERIFIED_ALIAS = "resolved_verified_alias"
    UNKNOWN_VERIFIED_ALIAS = "unknown_verified_alias"
    ALIAS_INACTIVE = "alias_inactive_at_effective_time"
    COORDINATE_CONFLICT = "candidate_coordinate_conflict"
    PROXIMITY_NOT_IDENTITY = "proximity_not_identity"
    EXACT_AVIATION_ID_CORRELATED_ONLY = (
        "exact_aviation_identifier_correlated_only"
    )
    DUPLICATE_CANDIDATE = "duplicate_adapter_candidate"
    INVALID_ADAPTER_CANDIDATE = "invalid_adapter_candidate"
    CATALOG_CONFLICT = "catalog_conflict_fail_closed"


def _text(value: object, field_name: str, *, fold: bool = False) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} moet een string zijn")
    normalised = unicodedata.normalize("NFKC", value).strip()
    if (
        not normalised
        or "\0" in normalised
        or any(ord(character) < 32 for character in normalised)
    ):
        raise ValueError(f"{field_name} bevat geen geldige identifier")
    return normalised.casefold() if fold else normalised


def _finite(value: object, field_name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} mag geen boolean zijn")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError(f"{field_name} moet een eindig getal zijn") from error
    if not math.isfinite(number):
        raise ValueError(f"{field_name} moet een eindig getal zijn")
    return 0.0 if number == 0.0 else number


def _optional_finite(value: object | None, field_name: str) -> float | None:
    return None if value is None else _finite(value, field_name)


def _coordinates(latitude: object, longitude: object) -> tuple[float, float]:
    latitude_number = _finite(latitude, "latitude")
    longitude_number = _finite(longitude, "longitude")
    if not -90.0 <= latitude_number <= 90.0:
        raise ValueError("latitude moet tussen -90 en 90 liggen")
    if not -180.0 <= longitude_number <= 180.0:
        raise ValueError("longitude moet tussen -180 en 180 liggen")
    if longitude_number == 180.0:
        longitude_number = -180.0
    return latitude_number, longitude_number


def _interval(
    valid_from: object | None,
    valid_until: object | None,
    *,
    prefix: str,
) -> tuple[float | None, float | None]:
    start = _optional_finite(valid_from, f"{prefix}.valid_from")
    end = _optional_finite(valid_until, f"{prefix}.valid_until")
    if start is not None and end is not None and end <= start:
        raise ValueError(
            f"{prefix}.valid_until moet strikt na valid_from liggen"
        )
    return start, end


def _is_active(
    valid_from: float | None,
    valid_until: float | None,
    effective_at: float,
) -> bool:
    return (
        (valid_from is None or effective_at >= valid_from)
        and (valid_until is None or effective_at < valid_until)
    )


def _intervals_overlap(
    first_start: float | None,
    first_end: float | None,
    second_start: float | None,
    second_end: float | None,
) -> bool:
    start = max(
        -math.inf if first_start is None else first_start,
        -math.inf if second_start is None else second_start,
    )
    end = min(
        math.inf if first_end is None else first_end,
        math.inf if second_end is None else second_end,
    )
    return start < end


def _interval_is_within(
    inner_start: float | None,
    inner_end: float | None,
    outer_start: float | None,
    outer_end: float | None,
) -> bool:
    return (
        outer_start is None
        or inner_start is not None
        and inner_start >= outer_start
    ) and (
        outer_end is None
        or inner_end is not None
        and inner_end <= outer_end
    )


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _fingerprint(namespace: str, payload: object) -> str:
    digest = hashlib.sha256()
    digest.update(namespace.encode("utf-8"))
    digest.update(b"\0")
    digest.update(_canonical_json(payload).encode("utf-8"))
    return digest.hexdigest()


def _stable_id(namespace: str, payload: object) -> str:
    return f"{namespace}:{_fingerprint(namespace, payload)[:32]}"


def _sha256(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    digest = _text(value, field_name).casefold()
    if (
        len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ValueError(f"{field_name} moet een lowercase SHA-256 zijn")
    return digest


def _required_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} moet een SHA-256-string zijn")
    digest = _sha256(value, field_name)
    if digest is None:
        raise ValueError(f"{field_name} ontbreekt")
    return digest


def _alias_key(provider_id: str, station_id: str) -> tuple[str, str]:
    return (
        _text(provider_id, "provider_id", fold=True),
        _text(station_id, "station_id", fold=True),
    )


def _distance_m(
    first_latitude: float,
    first_longitude: float,
    second_latitude: float,
    second_longitude: float,
) -> float:
    """Globally stable great-circle distance, including the antimeridian."""
    first_latitude_radians = math.radians(first_latitude)
    second_latitude_radians = math.radians(second_latitude)
    latitude_delta = second_latitude_radians - first_latitude_radians
    longitude_delta = math.radians(
        (second_longitude - first_longitude + 180.0) % 360.0 - 180.0
    )
    haversine = (
        math.sin(latitude_delta / 2.0) ** 2
        + math.cos(first_latitude_radians)
        * math.cos(second_latitude_radians)
        * math.sin(longitude_delta / 2.0) ** 2
    )
    haversine = max(0.0, min(1.0, haversine))
    return (
        2.0
        * EARTH_RADIUS_M
        * math.atan2(math.sqrt(haversine), math.sqrt(1.0 - haversine))
    )


@dataclass(frozen=True, slots=True)
class VerificationEvidence:
    """Immutable proof reference for one authoritative catalogue assertion."""

    authority: str
    reference: str
    verified_at: float
    content_sha256: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "authority",
            _text(self.authority, "authority"),
        )
        object.__setattr__(
            self,
            "reference",
            _text(self.reference, "reference"),
        )
        object.__setattr__(
            self,
            "verified_at",
            _finite(self.verified_at, "verified_at"),
        )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )

    @property
    def fingerprint(self) -> str:
        return _fingerprint("verification-evidence-v1", self._payload())

    def _payload(self) -> dict[str, object]:
        return {
            "authority": self.authority,
            "reference": self.reference,
            "verified_at": self.verified_at,
            "content_sha256": self.content_sha256,
        }


@dataclass(frozen=True, slots=True)
class VerifiedStationAlias:
    """Provider alias explicitly verified to map to its containing site."""

    provider_id: str
    station_id: str
    mapping_evidence: VerificationEvidence
    valid_from: float | None = None
    valid_until: float | None = None

    def __post_init__(self) -> None:
        provider_id = _text(self.provider_id, "provider_id", fold=True)
        station_id = _text(self.station_id, "station_id")
        if not isinstance(self.mapping_evidence, VerificationEvidence):
            raise TypeError(
                "mapping_evidence moet VerificationEvidence zijn"
            )
        valid_from, valid_until = _interval(
            self.valid_from,
            self.valid_until,
            prefix="alias",
        )
        object.__setattr__(self, "provider_id", provider_id)
        object.__setattr__(self, "station_id", station_id)
        object.__setattr__(self, "valid_from", valid_from)
        object.__setattr__(self, "valid_until", valid_until)

    @property
    def key(self) -> tuple[str, str]:
        return _alias_key(self.provider_id, self.station_id)

    @property
    def fingerprint(self) -> str:
        return _fingerprint("verified-station-alias-v1", self._payload())

    def _payload(self) -> dict[str, object]:
        return {
            "provider_id": self.key[0],
            "station_id": self.key[1],
            "mapping_evidence": self.mapping_evidence._payload(),
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
        }


@dataclass(frozen=True, slots=True)
class VerifiedStationIdentifier:
    """Authoritative station identifier; correlation is not site merging."""

    namespace: str
    value: str
    evidence: VerificationEvidence

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "namespace",
            _text(self.namespace, "namespace", fold=True),
        )
        object.__setattr__(self, "value", _text(self.value, "value"))
        if not isinstance(self.evidence, VerificationEvidence):
            raise TypeError("evidence moet VerificationEvidence zijn")

    @property
    def key(self) -> tuple[str, str]:
        return self.namespace, self.value.casefold()

    def _payload(self) -> dict[str, object]:
        return {
            "namespace": self.key[0],
            "value": self.key[1],
            "evidence": self.evidence._payload(),
        }


@dataclass(frozen=True, slots=True)
class SensorCapability:
    """Versioned statement about what one physical sensor can observe."""

    provider_id: str
    sensor_id: str
    measurement_kind: str
    can_observe_absence: bool
    evidence: VerificationEvidence
    valid_from: float | None = None
    valid_until: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "provider_id",
            _text(self.provider_id, "provider_id", fold=True),
        )
        object.__setattr__(
            self,
            "sensor_id",
            _text(self.sensor_id, "sensor_id"),
        )
        object.__setattr__(
            self,
            "measurement_kind",
            _text(self.measurement_kind, "measurement_kind", fold=True),
        )
        if not isinstance(self.can_observe_absence, bool):
            raise TypeError("can_observe_absence moet een boolean zijn")
        if not isinstance(self.evidence, VerificationEvidence):
            raise TypeError("evidence moet VerificationEvidence zijn")
        valid_from, valid_until = _interval(
            self.valid_from,
            self.valid_until,
            prefix="capability",
        )
        object.__setattr__(self, "valid_from", valid_from)
        object.__setattr__(self, "valid_until", valid_until)

    @property
    def key(self) -> tuple[str, str, str]:
        return (
            self.provider_id.casefold(),
            self.sensor_id.casefold(),
            self.measurement_kind.casefold(),
        )

    def _payload(self) -> dict[str, object]:
        return {
            "provider_id": self.key[0],
            "sensor_id": self.key[1],
            "measurement_kind": self.key[2],
            "can_observe_absence": self.can_observe_absence,
            "evidence": self.evidence._payload(),
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
        }


@dataclass(frozen=True, slots=True)
class SitingAttribute:
    """One sourced, immutable siting metadata value."""

    name: str
    value: str
    evidence: VerificationEvidence

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "name",
            _text(self.name, "siting.name", fold=True),
        )
        object.__setattr__(
            self,
            "value",
            _text(self.value, "siting.value"),
        )
        if not isinstance(self.evidence, VerificationEvidence):
            raise TypeError("evidence moet VerificationEvidence zijn")

    @property
    def key(self) -> str:
        return self.name.casefold()

    def _payload(self) -> dict[str, object]:
        return {
            "name": self.key,
            "value": self.value,
            "evidence": self.evidence._payload(),
        }


@dataclass(frozen=True, slots=True)
class PhysicalSiteRecord:
    """One immutable, versioned physical-site catalogue record."""

    site_key: str
    latitude: float
    longitude: float
    coordinate_tolerance_m: float
    independence_group_key: str
    site_evidence: VerificationEvidence
    independence_evidence: VerificationEvidence
    aliases: tuple[VerifiedStationAlias, ...]
    authoritative_identifiers: tuple[VerifiedStationIdentifier, ...] = ()
    capabilities: tuple[SensorCapability, ...] = ()
    siting: tuple[SitingAttribute, ...] = ()
    elevation_m: float | None = None
    valid_from: float | None = None
    valid_until: float | None = None
    replaces_site_key: str | None = None
    lifecycle_evidence: VerificationEvidence | None = None

    def __post_init__(self) -> None:
        site_key = _text(self.site_key, "site_key")
        latitude, longitude = _coordinates(self.latitude, self.longitude)
        tolerance = _finite(
            self.coordinate_tolerance_m,
            "coordinate_tolerance_m",
        )
        if not 0.0 <= tolerance <= 50_000.0:
            raise ValueError(
                "coordinate_tolerance_m moet tussen 0 en 50000 liggen"
            )
        group_key = _text(
            self.independence_group_key,
            "independence_group_key",
        )
        if not isinstance(self.site_evidence, VerificationEvidence):
            raise TypeError("site_evidence moet VerificationEvidence zijn")
        if not isinstance(
            self.independence_evidence,
            VerificationEvidence,
        ):
            raise TypeError(
                "independence_evidence moet VerificationEvidence zijn"
            )
        valid_from, valid_until = _interval(
            self.valid_from,
            self.valid_until,
            prefix="site",
        )
        if not isinstance(self.aliases, tuple) or not self.aliases:
            raise ValueError("aliases moet een niet-lege tuple zijn")
        if any(
            not isinstance(alias, VerifiedStationAlias)
            for alias in self.aliases
        ):
            raise TypeError("aliases bevat geen VerifiedStationAlias")
        for field_name, values, expected_type in (
            (
                "authoritative_identifiers",
                self.authoritative_identifiers,
                VerifiedStationIdentifier,
            ),
            ("capabilities", self.capabilities, SensorCapability),
            ("siting", self.siting, SitingAttribute),
        ):
            if not isinstance(values, tuple):
                raise TypeError(f"{field_name} moet een tuple zijn")
            if any(not isinstance(value, expected_type) for value in values):
                raise TypeError(f"{field_name} bevat een ongeldig object")
        elevation = _optional_finite(self.elevation_m, "elevation_m")
        replaces = (
            None
            if self.replaces_site_key is None
            else _text(self.replaces_site_key, "replaces_site_key")
        )
        if replaces is not None and not isinstance(
            self.lifecycle_evidence,
            VerificationEvidence,
        ):
            raise ValueError(
                "een replacement vereist expliciete lifecycle_evidence"
            )
        if (
            self.lifecycle_evidence is not None
            and not isinstance(
                self.lifecycle_evidence,
                VerificationEvidence,
            )
        ):
            raise TypeError(
                "lifecycle_evidence moet VerificationEvidence zijn"
            )

        object.__setattr__(self, "site_key", site_key)
        object.__setattr__(self, "latitude", latitude)
        object.__setattr__(self, "longitude", longitude)
        object.__setattr__(self, "coordinate_tolerance_m", tolerance)
        object.__setattr__(self, "independence_group_key", group_key)
        object.__setattr__(self, "elevation_m", elevation)
        object.__setattr__(self, "valid_from", valid_from)
        object.__setattr__(self, "valid_until", valid_until)
        object.__setattr__(self, "replaces_site_key", replaces)
        object.__setattr__(
            self,
            "aliases",
            tuple(
                sorted(
                    self.aliases,
                    key=lambda alias: (
                        alias.key,
                        -math.inf
                        if alias.valid_from is None
                        else alias.valid_from,
                        math.inf
                        if alias.valid_until is None
                        else alias.valid_until,
                        alias.fingerprint,
                    ),
                )
            ),
        )
        object.__setattr__(
            self,
            "authoritative_identifiers",
            tuple(
                sorted(
                    self.authoritative_identifiers,
                    key=lambda identifier: (
                        identifier.key,
                        identifier.evidence.fingerprint,
                    ),
                )
            ),
        )
        object.__setattr__(
            self,
            "capabilities",
            tuple(
                sorted(
                    self.capabilities,
                    key=lambda capability: (
                        capability.key,
                        -math.inf
                        if capability.valid_from is None
                        else capability.valid_from,
                        math.inf
                        if capability.valid_until is None
                        else capability.valid_until,
                    ),
                )
            ),
        )
        object.__setattr__(
            self,
            "siting",
            tuple(
                sorted(
                    self.siting,
                    key=lambda attribute: (
                        attribute.key,
                        attribute.evidence.fingerprint,
                    ),
                )
            ),
        )

    @property
    def site_key_canonical(self) -> str:
        return self.site_key.casefold()

    @property
    def independence_group_key_canonical(self) -> str:
        return self.independence_group_key.casefold()

    @property
    def physical_site_id(self) -> str:
        return _stable_id(
            "physical-site",
            {"site_key": self.site_key_canonical},
        )

    @property
    def independence_group(self) -> str:
        return _stable_id(
            "independence-group",
            {"group_key": self.independence_group_key_canonical},
        )

    @property
    def fingerprint(self) -> str:
        return _fingerprint("physical-site-record-v1", self._payload())

    def _payload(self) -> dict[str, object]:
        return {
            "site_key": self.site_key_canonical,
            "physical_site_id": self.physical_site_id,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "elevation_m": self.elevation_m,
            "coordinate_tolerance_m": self.coordinate_tolerance_m,
            "independence_group_key": (
                self.independence_group_key_canonical
            ),
            "independence_group": self.independence_group,
            "site_evidence": self.site_evidence._payload(),
            "independence_evidence": (
                self.independence_evidence._payload()
            ),
            "aliases": [alias._payload() for alias in self.aliases],
            "authoritative_identifiers": [
                identifier._payload()
                for identifier in self.authoritative_identifiers
            ],
            "capabilities": [
                capability._payload()
                for capability in self.capabilities
            ],
            "siting": [attribute._payload() for attribute in self.siting],
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
            "replaces_site_key": (
                None
                if self.replaces_site_key is None
                else self.replaces_site_key.casefold()
            ),
            "lifecycle_evidence": (
                None
                if self.lifecycle_evidence is None
                else self.lifecycle_evidence._payload()
            ),
        }


@dataclass(frozen=True, slots=True)
class CatalogValidationIssue:
    """Deterministic, machine-readable catalogue conflict."""

    code: CatalogConflictCode
    subjects: tuple[str, ...]
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.code, CatalogConflictCode):
            raise TypeError("code moet CatalogConflictCode zijn")
        if not isinstance(self.subjects, tuple) or not self.subjects:
            raise ValueError("subjects moet een niet-lege tuple zijn")
        object.__setattr__(
            self,
            "subjects",
            tuple(sorted(_text(item, "subject") for item in self.subjects)),
        )
        object.__setattr__(self, "detail", _text(self.detail, "detail"))

    @property
    def issue_id(self) -> str:
        return _stable_id(
            "physical-site-catalog-issue",
            {
                "code": self.code.value,
                "subjects": self.subjects,
                "detail": self.detail,
            },
        )


class PhysicalSiteCatalogError(ValueError):
    """Raised when a catalogue could create ambiguous physical identity."""

    def __init__(self, issues: tuple[CatalogValidationIssue, ...]) -> None:
        self.issues = issues
        codes = ", ".join(issue.code.value for issue in issues)
        super().__init__(f"ongeldige PhysicalSite-catalogus: {codes}")


@dataclass(frozen=True, slots=True)
class SiteCandidateSnapshot:
    """Trusted immutable snapshot extracted from an unverified adapter object."""

    provider_id: str
    station_id: str
    latitude: float
    longitude: float
    candidate_basis: str

    def __post_init__(self) -> None:
        provider_id = _text(self.provider_id, "provider_id", fold=True)
        station_id = _text(self.station_id, "station_id")
        latitude, longitude = _coordinates(self.latitude, self.longitude)
        basis = _text(self.candidate_basis, "candidate_basis")
        object.__setattr__(self, "provider_id", provider_id)
        object.__setattr__(self, "station_id", station_id)
        object.__setattr__(self, "latitude", latitude)
        object.__setattr__(self, "longitude", longitude)
        object.__setattr__(self, "candidate_basis", basis)

    @property
    def alias_key(self) -> tuple[str, str]:
        return _alias_key(self.provider_id, self.station_id)

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            "physical-site-candidate-v1",
            {
                "provider_id": self.alias_key[0],
                "station_id": self.alias_key[1],
                "latitude_hex": self.latitude.hex(),
                "longitude_hex": self.longitude.hex(),
                "candidate_basis": self.candidate_basis,
            },
        )

    @classmethod
    def from_adapter_candidate(
        cls,
        candidate: object,
    ) -> SiteCandidateSnapshot:
        """Extract only alias/coordinates; ignore proposed site/group IDs."""
        alias = getattr(candidate, "station_alias")
        site = getattr(candidate, "site")
        provider_id = getattr(alias, "provider_id")
        station_id = getattr(alias, "station_id")
        site_aliases = getattr(site, "aliases")
        candidate_alias_key = _alias_key(provider_id, station_id)
        if not isinstance(site_aliases, tuple) or not any(
            _alias_key(
                getattr(site_alias, "provider_id"),
                getattr(site_alias, "station_id"),
            )
            == candidate_alias_key
            for site_alias in site_aliases
        ):
            raise ValueError(
                "adaptercandidate bevat geen coherente StationAlias"
            )
        return cls(
            provider_id=provider_id,
            station_id=station_id,
            latitude=getattr(site, "latitude"),
            longitude=getattr(site, "longitude"),
            candidate_basis=getattr(
                candidate,
                "candidate_basis",
                "unspecified_unverified_adapter_candidate",
            ),
        )


@dataclass(frozen=True, slots=True)
class CandidateResolution:
    """Immutable resolution result safe for downstream gating."""

    candidate_fingerprint: str
    provider_id: str | None
    station_id: str | None
    status: ResolutionStatus
    physical_site_id: str | None
    independence_group: str | None
    mapping_fingerprint: str | None
    coordinate_distance_m: float | None
    coordinate_tolerance_m: float | None
    provisional_correlation_group: str | None
    reason_codes: tuple[CatalogAuditCode, ...]

    def __post_init__(self) -> None:
        candidate_fingerprint = _required_sha256(
            self.candidate_fingerprint,
            "candidate_fingerprint",
        )
        object.__setattr__(
            self,
            "candidate_fingerprint",
            candidate_fingerprint,
        )
        if not isinstance(self.status, ResolutionStatus):
            raise TypeError("status moet ResolutionStatus zijn")
        if (
            not isinstance(self.reason_codes, tuple)
            or not self.reason_codes
            or any(
                not isinstance(reason, CatalogAuditCode)
                for reason in self.reason_codes
            )
        ):
            raise ValueError("reason_codes bevat geen geldige auditreden")
        if self.status is ResolutionStatus.RESOLVED:
            if (
                self.physical_site_id is None
                or self.independence_group is None
                or self.mapping_fingerprint is None
            ):
                raise ValueError("resolved resultaat mist catalogusidentiteit")
        elif (
            self.physical_site_id is not None
            or self.independence_group is not None
        ):
            raise ValueError(
                "fail-closed resultaat mag geen bruikbare site/groep geven"
            )
        for value, field_name in (
            (self.physical_site_id, "physical_site_id"),
            (self.independence_group, "independence_group"),
            (
                self.provisional_correlation_group,
                "provisional_correlation_group",
            ),
        ):
            if value is not None:
                _text(value, field_name)
        if self.mapping_fingerprint is not None:
            object.__setattr__(
                self,
                "mapping_fingerprint",
                _required_sha256(
                    self.mapping_fingerprint,
                    "mapping_fingerprint",
                ),
            )
        for value, field_name in (
            (self.coordinate_distance_m, "coordinate_distance_m"),
            (self.coordinate_tolerance_m, "coordinate_tolerance_m"),
        ):
            if value is not None and _finite(value, field_name) < 0.0:
                raise ValueError(f"{field_name} mag niet negatief zijn")


@dataclass(frozen=True, slots=True)
class CatalogAuditEvent:
    """One stable audit event, including suppressed exact duplicates."""

    code: CatalogAuditCode
    candidate_fingerprint: str
    alias_key: tuple[str, str] | None
    related_physical_site_ids: tuple[str, ...]
    occurrence_count: int
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.code, CatalogAuditCode):
            raise TypeError("code moet CatalogAuditCode zijn")
        object.__setattr__(
            self,
            "candidate_fingerprint",
            _required_sha256(
                self.candidate_fingerprint,
                "candidate_fingerprint",
            ),
        )
        if self.alias_key is not None:
            if (
                not isinstance(self.alias_key, tuple)
                or len(self.alias_key) != 2
            ):
                raise TypeError("alias_key moet een provider/station-tuple zijn")
            object.__setattr__(
                self,
                "alias_key",
                _alias_key(*self.alias_key),
            )
        if (
            not isinstance(self.occurrence_count, int)
            or isinstance(self.occurrence_count, bool)
            or self.occurrence_count < 1
        ):
            raise ValueError("occurrence_count moet minstens één zijn")
        object.__setattr__(
            self,
            "related_physical_site_ids",
            tuple(
                sorted(
                    _text(item, "related_physical_site_id")
                    for item in self.related_physical_site_ids
                )
            ),
        )
        object.__setattr__(self, "detail", _text(self.detail, "detail"))

    @property
    def event_id(self) -> str:
        return _stable_id(
            "physical-site-catalog-audit",
            {
                "code": self.code.value,
                "candidate_fingerprint": self.candidate_fingerprint,
                "alias_key": self.alias_key,
                "related_physical_site_ids": (
                    self.related_physical_site_ids
                ),
                "occurrence_count": self.occurrence_count,
                "detail": self.detail,
            },
        )


@dataclass(frozen=True, slots=True)
class CatalogResolutionBatch:
    """Canonical immutable result of resolving an adapter candidate batch."""

    catalog_fingerprint: str
    effective_at: float
    resolutions: tuple[CandidateResolution, ...]
    audit_events: tuple[CatalogAuditEvent, ...]
    algorithm_version: str = PHYSICAL_SITE_CATALOG_ALGORITHM

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "catalog_fingerprint",
            _required_sha256(
                self.catalog_fingerprint,
                "catalog_fingerprint",
            ),
        )
        object.__setattr__(
            self,
            "effective_at",
            _finite(self.effective_at, "effective_at"),
        )
        if not isinstance(self.resolutions, tuple) or any(
            not isinstance(item, CandidateResolution)
            for item in self.resolutions
        ):
            raise TypeError("resolutions moet een immutable tuple zijn")
        if not isinstance(self.audit_events, tuple) or any(
            not isinstance(item, CatalogAuditEvent)
            for item in self.audit_events
        ):
            raise TypeError("audit_events moet een immutable tuple zijn")
        if self.algorithm_version != PHYSICAL_SITE_CATALOG_ALGORITHM:
            raise ValueError("onbekende physical-site catalogusalgoritmeversie")

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            "physical-site-resolution-batch-v1",
            {
                "catalog_fingerprint": self.catalog_fingerprint,
                "effective_at": self.effective_at,
                "algorithm_version": self.algorithm_version,
                "resolutions": [
                    {
                        "candidate_fingerprint": item.candidate_fingerprint,
                        "status": item.status.value,
                        "physical_site_id": item.physical_site_id,
                        "independence_group": item.independence_group,
                        "mapping_fingerprint": item.mapping_fingerprint,
                        "coordinate_distance_m": (
                            item.coordinate_distance_m
                        ),
                        "coordinate_tolerance_m": (
                            item.coordinate_tolerance_m
                        ),
                        "provisional_correlation_group": (
                            item.provisional_correlation_group
                        ),
                        "reason_codes": [
                            reason.value for reason in item.reason_codes
                        ],
                    }
                    for item in self.resolutions
                ],
                "audit_event_ids": [
                    event.event_id for event in self.audit_events
                ],
            },
        )


@dataclass(frozen=True, slots=True)
class PhysicalSiteCatalog:
    """Immutable catalogue with validation, resolver and registry export."""

    catalog_name: str
    catalog_version: str
    records: tuple[PhysicalSiteRecord, ...]
    proximity_audit_radius_m: float

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "catalog_name",
            _text(self.catalog_name, "catalog_name"),
        )
        object.__setattr__(
            self,
            "catalog_version",
            _text(self.catalog_version, "catalog_version"),
        )
        if not isinstance(self.records, tuple):
            raise TypeError("records moet een immutable tuple zijn")
        if any(
            not isinstance(record, PhysicalSiteRecord)
            for record in self.records
        ):
            raise TypeError("records bevat geen PhysicalSiteRecord")
        audit_radius = _finite(
            self.proximity_audit_radius_m,
            "proximity_audit_radius_m",
        )
        if not 0.0 <= audit_radius <= 100_000.0:
            raise ValueError(
                "proximity_audit_radius_m moet tussen 0 en 100000 liggen"
            )
        object.__setattr__(
            self,
            "records",
            tuple(
                sorted(
                    self.records,
                    key=lambda record: (
                        record.site_key_canonical,
                        record.fingerprint,
                    ),
                )
            ),
        )
        object.__setattr__(
            self,
            "proximity_audit_radius_m",
            audit_radius,
        )
        issues = _catalog_validation_issues(self.records)
        if issues:
            raise PhysicalSiteCatalogError(issues)

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            "physical-site-catalog-v1",
            {
                "algorithm_version": PHYSICAL_SITE_CATALOG_ALGORITHM,
                "catalog_name": self.catalog_name,
                "catalog_version": self.catalog_version,
                "proximity_audit_radius_m": (
                    self.proximity_audit_radius_m
                ),
                "records": [record._payload() for record in self.records],
            },
        )

    @property
    def canonical_json(self) -> str:
        """Immutable serialisation suitable for hashing and archival."""
        return _canonical_json(
            {
                "algorithm_version": PHYSICAL_SITE_CATALOG_ALGORITHM,
                "catalog_name": self.catalog_name,
                "catalog_version": self.catalog_version,
                "proximity_audit_radius_m": (
                    self.proximity_audit_radius_m
                ),
                "records": [record._payload() for record in self.records],
                "catalog_fingerprint": self.fingerprint,
            }
        )

    def to_ground_fusion_registry(
        self,
        *,
        effective_at: float,
    ) -> PhysicalSiteRegistry:
        """Export only active, verified mappings to the existing fusion API."""
        timestamp = _finite(effective_at, "effective_at")
        sites: list[PhysicalSite] = []
        for record in self.records:
            if not _is_active(
                record.valid_from,
                record.valid_until,
                timestamp,
            ):
                continue
            aliases = tuple(
                StationAlias(
                    provider_id=alias.provider_id,
                    station_id=alias.station_id,
                    mapping_fingerprint=alias.fingerprint,
                )
                for alias in record.aliases
                if _is_active(
                    alias.valid_from,
                    alias.valid_until,
                    timestamp,
                )
            )
            if not aliases:
                continue
            sites.append(
                PhysicalSite(
                    physical_site_id=record.physical_site_id,
                    latitude=record.latitude,
                    longitude=record.longitude,
                    independence_group=record.independence_group,
                    aliases=aliases,
                )
            )
        return PhysicalSiteRegistry(
            sites=tuple(
                sorted(sites, key=lambda site: site.physical_site_id)
            ),
            registry_version=(
                f"{self.catalog_name}:{self.catalog_version}:"
                f"{self.fingerprint[:24]}:{timestamp.hex()}"
            ),
            catalog_fingerprint=self.fingerprint,
        )


def _catalog_validation_issues(
    records: tuple[PhysicalSiteRecord, ...],
) -> tuple[CatalogValidationIssue, ...]:
    issues: list[CatalogValidationIssue] = []
    by_site_key: dict[str, list[PhysicalSiteRecord]] = {}
    for record in records:
        by_site_key.setdefault(record.site_key_canonical, []).append(record)
    for site_key, matches in by_site_key.items():
        if len(matches) > 1:
            issues.append(
                CatalogValidationIssue(
                    code=CatalogConflictCode.DUPLICATE_SITE_KEY,
                    subjects=(site_key,),
                    detail="Meer dan één record gebruikt dezelfde site_key.",
                )
            )

    mappings: dict[
        tuple[str, str],
        list[tuple[PhysicalSiteRecord, VerifiedStationAlias]],
    ] = {}
    for record in records:
        for alias in record.aliases:
            mappings.setdefault(alias.key, []).append((record, alias))
            if not _interval_is_within(
                alias.valid_from,
                alias.valid_until,
                record.valid_from,
                record.valid_until,
            ):
                issues.append(
                    CatalogValidationIssue(
                        code=(
                            CatalogConflictCode.ALIAS_OUTSIDE_SITE_VALIDITY
                        ),
                        subjects=(
                            record.site_key,
                            f"{alias.key[0]}:{alias.key[1]}",
                        ),
                        detail=(
                            "De aliasvaliditeit valt buiten de "
                            "siteversievaliditeit."
                        ),
                    )
                )
    for alias_key, matches in mappings.items():
        for first_index, (first_record, first_alias) in enumerate(matches):
            for second_record, second_alias in matches[first_index + 1 :]:
                if not _intervals_overlap(
                    first_alias.valid_from,
                    first_alias.valid_until,
                    second_alias.valid_from,
                    second_alias.valid_until,
                ):
                    continue
                same_site = (
                    first_record.site_key_canonical
                    == second_record.site_key_canonical
                )
                issues.append(
                    CatalogValidationIssue(
                        code=(
                            CatalogConflictCode.DUPLICATE_ALIAS_MAPPING
                            if same_site
                            else CatalogConflictCode.ALIAS_MAPS_TO_MULTIPLE_SITES
                        ),
                        subjects=(
                            f"{alias_key[0]}:{alias_key[1]}",
                            first_record.site_key,
                            second_record.site_key,
                        ),
                        detail=(
                            "Overlappende aliasmapping is niet eenduidig."
                        ),
                    )
                )

    aviation: dict[
        str,
        list[tuple[PhysicalSiteRecord, VerifiedStationAlias]],
    ] = {}
    for record in records:
        for alias in record.aliases:
            if alias.key[0] in _AVIATION_PROVIDERS:
                aviation.setdefault(alias.key[1], []).append(
                    (record, alias)
                )
    for station_id, station_mappings in aviation.items():
        for first_index, (first_record, first_alias) in enumerate(
            station_mappings
        ):
            for second_record, second_alias in station_mappings[
                first_index + 1 :
            ]:
                if (
                    first_record.site_key_canonical
                    == second_record.site_key_canonical
                    or not _intervals_overlap(
                        first_alias.valid_from,
                        first_alias.valid_until,
                        second_alias.valid_from,
                        second_alias.valid_until,
                    )
                    or (
                        first_record.independence_group_key_canonical
                        == second_record.independence_group_key_canonical
                    )
                ):
                    continue
                issues.append(
                    CatalogValidationIssue(
                        code=(
                            CatalogConflictCode
                            .SHARED_AVIATION_GROUP_CONFLICT
                        ),
                        subjects=(
                            station_id,
                            first_record.site_key,
                            second_record.site_key,
                        ),
                        detail=(
                            "Gelijktijdig exact gedeeld ICAO/STID moet "
                            "conservatief één independence group gebruiken."
                        ),
                    )
                )

    identifiers: dict[
        tuple[str, str],
        list[PhysicalSiteRecord],
    ] = {}
    for record in records:
        for identifier in record.authoritative_identifiers:
            if identifier.key[0] in {"icao", "stid"}:
                identifiers.setdefault(identifier.key, []).append(record)
    for identifier_key, identifier_records in identifiers.items():
        for first_index, first_record in enumerate(identifier_records):
            for second_record in identifier_records[first_index + 1 :]:
                if (
                    first_record.site_key_canonical
                    == second_record.site_key_canonical
                    or not _intervals_overlap(
                        first_record.valid_from,
                        first_record.valid_until,
                        second_record.valid_from,
                        second_record.valid_until,
                    )
                    or (
                        first_record.independence_group_key_canonical
                        == second_record.independence_group_key_canonical
                    )
                ):
                    continue
                issues.append(
                    CatalogValidationIssue(
                        code=(
                            CatalogConflictCode
                            .SHARED_AUTHORITATIVE_ID_GROUP_CONFLICT
                        ),
                        subjects=(
                            f"{identifier_key[0]}:{identifier_key[1]}",
                            first_record.site_key,
                            second_record.site_key,
                        ),
                        detail=(
                            "Gelijktijdig gedeelde autoritatieve identifier "
                            "heeft tegenstrijdige independence groups."
                        ),
                    )
                )

    for record in records:
        identifier_keys = [
            identifier.key
            for identifier in record.authoritative_identifiers
        ]
        for duplicate in sorted(
            {
                key
                for key in identifier_keys
                if identifier_keys.count(key) > 1
            }
        ):
            issues.append(
                CatalogValidationIssue(
                    code=(
                        CatalogConflictCode
                        .DUPLICATE_AUTHORITATIVE_IDENTIFIER
                    ),
                    subjects=(
                        record.site_key,
                        f"{duplicate[0]}:{duplicate[1]}",
                    ),
                    detail=(
                        "Autoritatieve identifier staat dubbel in één "
                        "siteversie."
                    ),
                )
            )
        capabilities: dict[
            tuple[str, str, str],
            list[SensorCapability],
        ] = {}
        for capability in record.capabilities:
            capabilities.setdefault(capability.key, []).append(capability)
            if not _interval_is_within(
                capability.valid_from,
                capability.valid_until,
                record.valid_from,
                record.valid_until,
            ):
                issues.append(
                    CatalogValidationIssue(
                        code=(
                            CatalogConflictCode
                            .CAPABILITY_OUTSIDE_SITE_VALIDITY
                        ),
                        subjects=(
                            record.site_key,
                            ":".join(capability.key),
                        ),
                        detail=(
                            "Sensorcapability valt buiten de "
                            "siteversievaliditeit."
                        ),
                    )
                )
        for capability_key, values in capabilities.items():
            if any(
                _intervals_overlap(
                    first.valid_from,
                    first.valid_until,
                    second.valid_from,
                    second.valid_until,
                )
                for index, first in enumerate(values)
                for second in values[index + 1 :]
            ):
                issues.append(
                    CatalogValidationIssue(
                        code=CatalogConflictCode.DUPLICATE_CAPABILITY,
                        subjects=(
                            record.site_key,
                            ":".join(capability_key),
                        ),
                        detail=(
                            "Sensorcapability heeft overlappende versies."
                        ),
                    )
                )
        siting_keys = [attribute.key for attribute in record.siting]
        for duplicate in sorted(
            {
                key
                for key in siting_keys
                if siting_keys.count(key) > 1
            }
        ):
            issues.append(
                CatalogValidationIssue(
                    code=CatalogConflictCode.DUPLICATE_SITING_ATTRIBUTE,
                    subjects=(record.site_key, duplicate),
                    detail="Sitingattribute komt meermaals voor.",
                )
            )

    by_key = {
        record.site_key_canonical: record
        for record in records
    }
    for record in records:
        if record.replaces_site_key is None:
            continue
        replaced_key = record.replaces_site_key.casefold()
        replaced = by_key.get(replaced_key)
        if replaced is None:
            issues.append(
                CatalogValidationIssue(
                    code=CatalogConflictCode.REPLACED_SITE_UNKNOWN,
                    subjects=(record.site_key, record.replaces_site_key),
                    detail="De vervangen site_key bestaat niet.",
                )
            )
        elif _intervals_overlap(
            record.valid_from,
            record.valid_until,
            replaced.valid_from,
            replaced.valid_until,
        ):
            issues.append(
                CatalogValidationIssue(
                    code=(
                        CatalogConflictCode.REPLACEMENT_VALIDITY_OVERLAP
                    ),
                    subjects=(record.site_key, replaced.site_key),
                    detail=(
                        "Oude en vervangende fysieke site zijn tegelijk "
                        "geldig."
                    ),
                )
            )
    for record in records:
        visited: set[str] = set()
        cursor = record
        while cursor.replaces_site_key is not None:
            next_key = cursor.replaces_site_key.casefold()
            if next_key in visited or next_key == record.site_key_canonical:
                issues.append(
                    CatalogValidationIssue(
                        code=CatalogConflictCode.REPLACEMENT_CYCLE,
                        subjects=tuple(
                            sorted(
                                {
                                    record.site_key,
                                    cursor.replaces_site_key,
                                }
                            )
                        ),
                        detail="Replacementketen bevat een cyclus.",
                    )
                )
                break
            visited.add(next_key)
            next_record = by_key.get(next_key)
            if next_record is None:
                break
            cursor = next_record

    deduplicated = {issue.issue_id: issue for issue in issues}
    return tuple(
        deduplicated[issue_id]
        for issue_id in sorted(deduplicated)
    )


def _candidate_correlation_group(
    candidate: SiteCandidateSnapshot,
    *,
    identifier_is_shared: bool,
) -> str | None:
    if (
        candidate.alias_key[0] not in _AVIATION_PROVIDERS
        or not identifier_is_shared
    ):
        return None
    return _stable_id(
        "unverified-aviation-correlation",
        {"exact_station_identifier": candidate.alias_key[1]},
    )


def _active_mappings(
    catalog: PhysicalSiteCatalog,
    alias_key: tuple[str, str],
    effective_at: float,
) -> tuple[tuple[PhysicalSiteRecord, VerifiedStationAlias], ...]:
    return tuple(
        (record, alias)
        for record in catalog.records
        if _is_active(
            record.valid_from,
            record.valid_until,
            effective_at,
        )
        for alias in record.aliases
        if alias.key == alias_key
        and _is_active(
            alias.valid_from,
            alias.valid_until,
            effective_at,
        )
    )


def _all_mappings(
    catalog: PhysicalSiteCatalog,
    alias_key: tuple[str, str],
) -> tuple[tuple[PhysicalSiteRecord, VerifiedStationAlias], ...]:
    return tuple(
        (record, alias)
        for record in catalog.records
        for alias in record.aliases
        if alias.key == alias_key
    )


def _audit_event(
    code: CatalogAuditCode,
    candidate: SiteCandidateSnapshot,
    *,
    related_site_ids: tuple[str, ...] = (),
    occurrence_count: int = 1,
    detail: str,
) -> CatalogAuditEvent:
    return CatalogAuditEvent(
        code=code,
        candidate_fingerprint=candidate.fingerprint,
        alias_key=candidate.alias_key,
        related_physical_site_ids=related_site_ids,
        occurrence_count=occurrence_count,
        detail=detail,
    )


def _resolve_candidate(
    candidate: SiteCandidateSnapshot,
    catalog: PhysicalSiteCatalog,
    effective_at: float,
    *,
    shared_aviation_identifiers: frozenset[str],
) -> tuple[CandidateResolution, tuple[CatalogAuditEvent, ...]]:
    provisional_group = _candidate_correlation_group(
        candidate,
        identifier_is_shared=(
            candidate.alias_key[1] in shared_aviation_identifiers
        ),
    )
    mappings = _active_mappings(
        catalog,
        candidate.alias_key,
        effective_at,
    )
    if len(mappings) > 1:
        resolution = CandidateResolution(
            candidate_fingerprint=candidate.fingerprint,
            provider_id=candidate.provider_id,
            station_id=candidate.station_id,
            status=ResolutionStatus.CATALOG_CONFLICT,
            physical_site_id=None,
            independence_group=None,
            mapping_fingerprint=None,
            coordinate_distance_m=None,
            coordinate_tolerance_m=None,
            provisional_correlation_group=provisional_group,
            reason_codes=(CatalogAuditCode.CATALOG_CONFLICT,),
        )
        return resolution, (
            _audit_event(
                CatalogAuditCode.CATALOG_CONFLICT,
                candidate,
                related_site_ids=tuple(
                    record.physical_site_id
                    for record, _ in mappings
                ),
                detail=(
                    "Meer dan één actieve mapping; resolver faalt gesloten."
                ),
            ),
        )

    if not mappings:
        historical = _all_mappings(catalog, candidate.alias_key)
        if historical:
            resolution = CandidateResolution(
                candidate_fingerprint=candidate.fingerprint,
                provider_id=candidate.provider_id,
                station_id=candidate.station_id,
                status=ResolutionStatus.ALIAS_INACTIVE,
                physical_site_id=None,
                independence_group=None,
                mapping_fingerprint=None,
                coordinate_distance_m=None,
                coordinate_tolerance_m=None,
                provisional_correlation_group=provisional_group,
                reason_codes=(CatalogAuditCode.ALIAS_INACTIVE,),
            )
            return resolution, (
                _audit_event(
                    CatalogAuditCode.ALIAS_INACTIVE,
                    candidate,
                    related_site_ids=tuple(
                        record.physical_site_id
                        for record, _ in historical
                    ),
                    detail=(
                        "Alias bestaat, maar niet op effective_at."
                    ),
                ),
            )

        nearest: tuple[float, PhysicalSiteRecord] | None = None
        for record in catalog.records:
            if not _is_active(
                record.valid_from,
                record.valid_until,
                effective_at,
            ):
                continue
            distance = _distance_m(
                candidate.latitude,
                candidate.longitude,
                record.latitude,
                record.longitude,
            )
            if nearest is None or distance < nearest[0]:
                nearest = distance, record
        reasons = [CatalogAuditCode.UNKNOWN_VERIFIED_ALIAS]
        events = [
            _audit_event(
                CatalogAuditCode.UNKNOWN_VERIFIED_ALIAS,
                candidate,
                detail=(
                    "Geen expliciet geverifieerde aliasmapping gevonden."
                ),
            )
        ]
        if (
            nearest is not None
            and catalog.proximity_audit_radius_m > 0.0
            and nearest[0] <= catalog.proximity_audit_radius_m
        ):
            reasons.append(CatalogAuditCode.PROXIMITY_NOT_IDENTITY)
            events.append(
                _audit_event(
                    CatalogAuditCode.PROXIMITY_NOT_IDENTITY,
                    candidate,
                    related_site_ids=(nearest[1].physical_site_id,),
                    detail=(
                        "Nabije site uitsluitend als auditcontext; "
                        "geen identity merge."
                    ),
                )
            )
        if provisional_group is not None:
            reasons.append(
                CatalogAuditCode.EXACT_AVIATION_ID_CORRELATED_ONLY
            )
            events.append(
                _audit_event(
                    CatalogAuditCode.EXACT_AVIATION_ID_CORRELATED_ONLY,
                    candidate,
                    detail=(
                        "Exact ICAO/STID geeft alleen een conservatieve "
                        "correlatiegroep, nooit een fysieke-site merge."
                    ),
                )
            )
        resolution = CandidateResolution(
            candidate_fingerprint=candidate.fingerprint,
            provider_id=candidate.provider_id,
            station_id=candidate.station_id,
            status=ResolutionStatus.UNKNOWN_ALIAS,
            physical_site_id=None,
            independence_group=None,
            mapping_fingerprint=None,
            coordinate_distance_m=None,
            coordinate_tolerance_m=None,
            provisional_correlation_group=provisional_group,
            reason_codes=tuple(reasons),
        )
        return resolution, tuple(events)

    record, alias = mappings[0]
    distance = _distance_m(
        candidate.latitude,
        candidate.longitude,
        record.latitude,
        record.longitude,
    )
    if distance > record.coordinate_tolerance_m:
        resolution = CandidateResolution(
            candidate_fingerprint=candidate.fingerprint,
            provider_id=candidate.provider_id,
            station_id=candidate.station_id,
            status=ResolutionStatus.COORDINATE_CONFLICT,
            physical_site_id=None,
            independence_group=None,
            mapping_fingerprint=alias.fingerprint,
            coordinate_distance_m=distance,
            coordinate_tolerance_m=record.coordinate_tolerance_m,
            provisional_correlation_group=provisional_group,
            reason_codes=(CatalogAuditCode.COORDINATE_CONFLICT,),
        )
        return resolution, (
            _audit_event(
                CatalogAuditCode.COORDINATE_CONFLICT,
                candidate,
                related_site_ids=(record.physical_site_id,),
                detail=(
                    "Exacte alias klopt, maar coördinaat ligt buiten de "
                    "expliciete catalogustolerantie."
                ),
            ),
        )

    resolution = CandidateResolution(
        candidate_fingerprint=candidate.fingerprint,
        provider_id=candidate.provider_id,
        station_id=candidate.station_id,
        status=ResolutionStatus.RESOLVED,
        physical_site_id=record.physical_site_id,
        independence_group=record.independence_group,
        mapping_fingerprint=alias.fingerprint,
        coordinate_distance_m=distance,
        coordinate_tolerance_m=record.coordinate_tolerance_m,
        provisional_correlation_group=None,
        reason_codes=(CatalogAuditCode.RESOLVED_VERIFIED_ALIAS,),
    )
    return resolution, (
        _audit_event(
            CatalogAuditCode.RESOLVED_VERIFIED_ALIAS,
            candidate,
            related_site_ids=(record.physical_site_id,),
            detail="Exacte geverifieerde alias en coördinaatcontrole geslaagd.",
        ),
    )


def resolve_adapter_candidates(
    candidates: Iterable[object],
    catalog: PhysicalSiteCatalog,
    *,
    effective_at: float,
) -> CatalogResolutionBatch:
    """Resolve adapter candidates without trusting their proposed identity.

    Exact duplicate snapshots are evaluated once and explicitly audited.
    Malformed adapter objects produce an immutable INVALID_CANDIDATE row.
    """
    if not isinstance(catalog, PhysicalSiteCatalog):
        raise TypeError("catalog moet PhysicalSiteCatalog zijn")
    timestamp = _finite(effective_at, "effective_at")
    if isinstance(candidates, (str, bytes, bytearray)):
        raise TypeError("candidates moet een iterable van objecten zijn")

    snapshots: dict[str, SiteCandidateSnapshot] = {}
    counts: dict[str, int] = {}
    invalid_resolutions: list[CandidateResolution] = []
    invalid_events: list[CatalogAuditEvent] = []
    for ordinal, candidate in enumerate(candidates):
        if ordinal >= _MAX_CANDIDATES_PER_BATCH:
            raise ValueError("te veel PhysicalSite-kandidaten in één batch")
        try:
            snapshot = SiteCandidateSnapshot.from_adapter_candidate(
                candidate
            )
        except Exception:
            candidate_fingerprint = _fingerprint(
                "invalid-adapter-candidate-v1",
                {
                    "ordinal": ordinal,
                    "type": (
                        f"{type(candidate).__module__}."
                        f"{type(candidate).__qualname__}"
                    ),
                },
            )
            invalid_resolutions.append(
                CandidateResolution(
                    candidate_fingerprint=candidate_fingerprint,
                    provider_id=None,
                    station_id=None,
                    status=ResolutionStatus.INVALID_CANDIDATE,
                    physical_site_id=None,
                    independence_group=None,
                    mapping_fingerprint=None,
                    coordinate_distance_m=None,
                    coordinate_tolerance_m=None,
                    provisional_correlation_group=None,
                    reason_codes=(
                        CatalogAuditCode.INVALID_ADAPTER_CANDIDATE,
                    ),
                )
            )
            invalid_events.append(
                CatalogAuditEvent(
                    code=CatalogAuditCode.INVALID_ADAPTER_CANDIDATE,
                    candidate_fingerprint=candidate_fingerprint,
                    alias_key=None,
                    related_physical_site_ids=(),
                    occurrence_count=1,
                    detail=(
                        "Adapterobject mist een coherent immutable "
                        "alias/coördinaatsnapshot."
                    ),
                )
            )
            continue
        snapshots[snapshot.fingerprint] = snapshot
        counts[snapshot.fingerprint] = counts.get(snapshot.fingerprint, 0) + 1

    resolutions = list(invalid_resolutions)
    events = list(invalid_events)
    aviation_sources: dict[str, set[str]] = {}
    for record in catalog.records:
        if not _is_active(
            record.valid_from,
            record.valid_until,
            timestamp,
        ):
            continue
        for alias in record.aliases:
            if (
                alias.key[0] in _AVIATION_PROVIDERS
                and _is_active(
                    alias.valid_from,
                    alias.valid_until,
                    timestamp,
                )
            ):
                aviation_sources.setdefault(alias.key[1], set()).add(
                    alias.key[0]
                )
    for snapshot in snapshots.values():
        if snapshot.alias_key[0] in _AVIATION_PROVIDERS:
            aviation_sources.setdefault(snapshot.alias_key[1], set()).add(
                snapshot.alias_key[0]
            )
    shared_aviation_identifiers = frozenset(
        station_id
        for station_id, providers in aviation_sources.items()
        if len(providers) > 1
    )
    for candidate_fingerprint in sorted(snapshots):
        snapshot = snapshots[candidate_fingerprint]
        resolution, resolution_events = _resolve_candidate(
            snapshot,
            catalog,
            timestamp,
            shared_aviation_identifiers=shared_aviation_identifiers,
        )
        resolutions.append(resolution)
        events.extend(resolution_events)
        occurrence_count = counts[candidate_fingerprint]
        if occurrence_count > 1:
            events.append(
                _audit_event(
                    CatalogAuditCode.DUPLICATE_CANDIDATE,
                    snapshot,
                    occurrence_count=occurrence_count,
                    detail=(
                        "Exact dezelfde adaptersnapshot werd éénmaal "
                        "geresolved."
                    ),
                )
            )

    return CatalogResolutionBatch(
        catalog_fingerprint=catalog.fingerprint,
        effective_at=timestamp,
        resolutions=tuple(
            sorted(
                resolutions,
                key=lambda item: item.candidate_fingerprint,
            )
        ),
        audit_events=tuple(
            sorted(
                events,
                key=lambda event: event.event_id,
            )
        ),
    )
