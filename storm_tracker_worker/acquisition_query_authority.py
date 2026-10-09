"""Content-addressed location authority for worker acquisition queries.

The acquisition plan intentionally carries only region and target identities.
Provider handlers such as RainViewer, Netatmo, Synoptic and Open-Meteo still
need coordinates.  This module closes that boundary without accepting an
arbitrary handler payload: it reconstructs the complete plan from the exact
desired-state snapshot, proves that the slot job belongs to that plan, and
then derives bounded query records from those two authorities.

The module is deliberately inert.  It performs no network I/O, imports no
provider handler, registers no service job and publishes no operational fact.
"""
from __future__ import annotations

from dataclasses import dataclass
import base64
import hashlib
import json
import math
import re
from typing import Any, Mapping

from .desired_state import (
    MAX_CLOCK_FUTURE_SKEW_SECONDS,
    MAX_DYNAMIC_POSITION_AGE_SECONDS,
    DesiredState,
    Target,
    parse_desired_state,
)
from .worker_acquisition_plan import (
    AcquisitionJob,
    AcquisitionPlan,
    AcquisitionRegion,
    COLLECTOR_SCOPE_COVERAGE_RELEVANT,
    build_worker_acquisition_plan,
    build_worker_acquisition_slot_jobs,
    validate_worker_acquisition_job,
    validate_worker_acquisition_plan,
)


CONTRACT = "stv3-acquisition-query-authority-v1"
REGION_QUERY_CONTRACT = "stv3-acquisition-region-query-v1"
TARGET_QUERY_CONTRACT = "stv3-acquisition-target-query-v1"
OPENMETEO_BATCH_CONTRACT = "stv3-openmeteo-target-query-batch-v1"
PUBLIC_SOURCE_REGION_QUERY_CONTRACT = "stv3-public-source-region-query-v1"
METHOD = "desired_state_plan_job_query_binding_v1"
MAX_AUTHORITY_BYTES = 128 * 1024
MAX_QUERY_RECORDS = 64
MAX_RAINVIEWER_HORIZON_KM = 1000.0
MAX_NETATMO_RADIUS_KM = 20.0
MAX_SYNOPTIC_RADIUS_KM = 50.0
WEB_MERCATOR_MAX_LATITUDE = 85.05112878
EARTH_RADIUS_KM = 6371.0088

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ROAMING_KINDS = frozenset({"person", "device_tracker", "test"})
_ACCURACY_REQUIRED_KINDS = frozenset({"person", "device_tracker"})
_PUBLIC_SOURCE_REGION_WORKLOADS = frozenset({
    "model.openmeteo.italiameteo_icon2i.region",
    "radar.kmi.appv4_visual",
    "validation.geosphere.location",
    "validation.meteolux.location",
})
_PUBLIC_SOURCE_REGION_PROFILES = {
    "model.openmeteo.italiameteo_icon2i.region": (
        (35.0, 5.5, 48.5, 19.0), (41.9028, 12.4964)
    ),
    "radar.kmi.appv4_visual": (
        (46.5, -2.5, 53.0, 10.5), (50.8503, 4.3517)
    ),
    "validation.geosphere.location": (
        (45.5, 8.1, 49.5, 17.75), (48.2082, 16.3738)
    ),
    "validation.meteolux.location": (
        (48.5, 4.5, 51.0, 7.5), (49.6116, 6.1319)
    ),
}
_LOCATION_WORKLOADS = frozenset({
    "ground.netatmo.region",
    "ground.synoptic.region",
    "model.openmeteo.region",
    "radar.rainviewer.visual",
    *_PUBLIC_SOURCE_REGION_WORKLOADS,
})
_LEGACY_BATCH_LOCATION_WORKLOADS = frozenset({
    "ground.netatmo.region",
    "ground.synoptic.region",
    "model.openmeteo.region",
    "radar.rainviewer.visual",
})
_BLOCK_REASON_CODES = frozenset(
    {
        "dynamic_position_expired",
        "dynamic_position_future",
    }
)


class AcquisitionQueryAuthorityError(ValueError):
    """The desired-state/plan/job location binding cannot be proven."""


class AcquisitionQueryCoverageError(AcquisitionQueryAuthorityError):
    """A provider query cannot cover the complete authorised target set."""


class AcquisitionQueryUnavailableError(AcquisitionQueryAuthorityError):
    """One location job is unavailable without invalidating other regions."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class AcquisitionQueryBlock:
    job_sha256: str
    workload_id: str
    provider_id: str
    reason_code: str
    block_sha256: str

    def as_dict(self) -> dict[str, str]:
        return {
            "block_sha256": self.block_sha256,
            "job_sha256": self.job_sha256,
            "provider_id": self.provider_id,
            "reason_code": self.reason_code,
            "workload_id": self.workload_id,
        }


@dataclass(frozen=True, slots=True)
class AcquisitionQueryBatch:
    desired_state_sha256: str
    plan_sha256: str
    evaluated_at: float
    authorities: tuple["AcquisitionQueryAuthority", ...]
    blocked: tuple[AcquisitionQueryBlock, ...]
    batch_sha256: str

    def diagnostics(self) -> dict[str, Any]:
        validate_acquisition_query_batch(self)
        return {
            "authority_count": len(self.authorities),
            "batch_sha256": self.batch_sha256,
            "blocked_count": len(self.blocked),
            "blocked_reason_counts": {
                reason: sum(item.reason_code == reason for item in self.blocked)
                for reason in sorted({item.reason_code for item in self.blocked})
            },
            "desired_state_sha256": self.desired_state_sha256,
            "evaluated_at": self.evaluated_at,
            "plan_sha256": self.plan_sha256,
        }


@dataclass(frozen=True, slots=True)
class RegionQuery:
    query_key: str
    source_region_sha256: str
    center_latitude: float
    center_longitude: float
    radar_horizon_km: float | None
    ground_required_radius_km: float | None
    query_kind: str | None
    target_ref_sha256s: tuple[str, ...]
    region_query_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "center_latitude": self.center_latitude,
            "center_longitude": self.center_longitude,
            "contract": REGION_QUERY_CONTRACT,
            "ground_required_radius_km": self.ground_required_radius_km,
            "query_key": self.query_key,
            "query_kind": self.query_kind,
            "radar_horizon_km": self.radar_horizon_km,
            "region_query_sha256": self.region_query_sha256,
            "source_region_sha256": self.source_region_sha256,
            "target_ref_sha256s": list(self.target_ref_sha256s),
        }


@dataclass(frozen=True, slots=True)
class TargetQuery:
    query_key: str
    target_ref_sha256: str
    latitude: float
    longitude: float
    query_kind: str
    position_observed_at: float
    location_accuracy_m: float | None
    target_query_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract": TARGET_QUERY_CONTRACT,
            "latitude": self.latitude,
            "location_accuracy_m": self.location_accuracy_m,
            "longitude": self.longitude,
            "position_observed_at": self.position_observed_at,
            "query_key": self.query_key,
            "query_kind": self.query_kind,
            "target_query_sha256": self.target_query_sha256,
            "target_ref_sha256": self.target_ref_sha256,
        }


@dataclass(frozen=True, slots=True)
class AcquisitionQueryAuthority:
    desired_state_sha256: str
    plan_sha256: str
    job_id: str
    job_sha256: str
    native_job_id: str
    workload_id: str
    provider_id: str
    product_id: str
    evaluated_at: float
    region_queries: tuple[RegionQuery, ...]
    target_queries: tuple[TargetQuery, ...]
    canonical_bytes: bytes
    authority_sha256: str

    def as_dict(self) -> dict[str, Any]:
        validate_acquisition_query_authority(self)
        result = json.loads(self.canonical_bytes)
        result["authority_sha256"] = self.authority_sha256
        return result

    def diagnostics(self) -> dict[str, Any]:
        """Return bounded one-way metadata; never expose coordinates or IDs."""

        validate_acquisition_query_authority(self)
        return {
            "authority_sha256": self.authority_sha256,
            "contract": CONTRACT,
            "desired_state_sha256": self.desired_state_sha256,
            "job_sha256": self.job_sha256,
            "method": METHOD,
            "plan_sha256": self.plan_sha256,
            "provider_sha256": _text_sha256(self.provider_id),
            "region_query_count": len(self.region_queries),
            "target_query_count": len(self.target_queries),
            "workload_sha256": _text_sha256(self.workload_id),
        }

    def rainviewer_payload(self) -> dict[str, Any]:
        validate_acquisition_query_authority(self)
        _require_workload(self, "radar.rainviewer.visual")
        region = _exactly_one_region(self)
        if not -WEB_MERCATOR_MAX_LATITUDE < region.center_latitude < WEB_MERCATOR_MAX_LATITUDE:
            raise AcquisitionQueryCoverageError(
                "RainViewer-regiocentrum valt buiten Web Mercator"
            )
        if region.radar_horizon_km is None:
            raise AcquisitionQueryCoverageError("RainViewer-horizon ontbreekt")
        guaranteed = _rainviewer_guaranteed_radius_km(
            region.center_latitude,
            region.center_longitude,
        )
        if (
            region.radar_horizon_km > MAX_RAINVIEWER_HORIZON_KM
            or region.radar_horizon_km > guaranteed
        ):
            raise AcquisitionQueryCoverageError(
                "RainViewer-horizon overschrijdt de gegarandeerde native 3x3-dekking"
            )
        return {
            "regions": [
                {
                    "center_lat": region.center_latitude,
                    "center_lon": region.center_longitude,
                    "horizon_km": region.radar_horizon_km,
                }
            ]
        }

    def netatmo_payload(self) -> dict[str, Any]:
        validate_acquisition_query_authority(self)
        _require_workload(self, "ground.netatmo.region")
        region = _exactly_one_region(self)
        _require_ground_radius(region, MAX_NETATMO_RADIUS_KM, "Netatmo")
        _validate_netatmo_bbox(region)
        return _ground_payload(region)

    def synoptic_payload(self) -> dict[str, Any]:
        validate_acquisition_query_authority(self)
        _require_workload(self, "ground.synoptic.region")
        region = _exactly_one_region(self)
        _require_ground_radius(region, MAX_SYNOPTIC_RADIUS_KM, "Synoptic")
        payload = _ground_payload(region)
        # This is the exact five-decimal center emitted by the native request.
        # Radius compensation below is calculated against the same point.
        payload["latitude"] = float(f"{region.center_latitude:.5f}")
        payload["longitude"] = float(f"{region.center_longitude:.5f}")
        payload["radius_km"] = _synoptic_center_compensated_radius_km(region)
        return payload

    def openmeteo_shadow_query_batch(self) -> dict[str, Any]:
        """Return inert future-handler input, never the legacy HA payload."""

        validate_acquisition_query_authority(self)
        _require_workload(self, "model.openmeteo.region")
        if not self.target_queries:
            raise AcquisitionQueryCoverageError(
                "Open-Meteo-query mist geautoriseerde targetpunten"
            )
        return {
            "contract": OPENMETEO_BATCH_CONTRACT,
            "operational_handler_compatible": False,
            "queries": [
                {
                    "latitude": item.latitude,
                    "longitude": item.longitude,
                    "query_key": item.query_key,
                    "query_kind": item.query_kind,
                }
                for item in self.target_queries
            ],
            "target_resolution_required": True,
        }

    def public_source_region_payload(self) -> dict[str, Any]:
        """Project one bounded point for a public country-scoped raw source.

        Coverage-relevant jobs retain their authoritative region centre.  The
        all-sources collector may deliberately schedule a source outside the
        user's region; those jobs use one fixed in-coverage sampling point so
        the provider remains collectable without pretending it describes the
        user's weather.  The downstream capture remains semantically
        unverified in both cases.
        """

        validate_acquisition_query_authority(self)
        if self.workload_id not in _PUBLIC_SOURCE_REGION_WORKLOADS:
            raise AcquisitionQueryAuthorityError(
                "querypayload is niet toegestaan voor deze workload"
            )
        region = _exactly_one_region(self)
        bounds, fallback = _PUBLIC_SOURCE_REGION_PROFILES[self.workload_id]
        south, west, north, east = bounds
        latitude = region.center_latitude
        longitude = region.center_longitude
        if not south <= latitude <= north or not west <= longitude <= east:
            latitude, longitude = fallback
        return {
            "contract": PUBLIC_SOURCE_REGION_QUERY_CONTRACT,
            "latitude": latitude,
            "longitude": longitude,
            "query_key": region.query_key,
        }

    def resolve_target_ids(self, desired_state: DesiredState) -> dict[str, str]:
        """Resolve pseudonymous keys parent-side; never include this in child IPC."""

        validate_acquisition_query_authority(self)
        state = _validated_desired_state(desired_state)
        if state.state_sha256 != self.desired_state_sha256:
            raise AcquisitionQueryAuthorityError(
                "targetresolver kreeg een andere desired-state"
            )
        references = {
            _target_reference(state.state_sha256, item.target_id): item.target_id
            for item in state.targets
        }
        result: dict[str, str] = {}
        for item in self.target_queries:
            target_id = references.get(item.target_ref_sha256)
            if target_id is None:
                raise AcquisitionQueryAuthorityError(
                    "targetresolver kan queryreferentie niet bewijzen"
                )
            result[item.query_key] = target_id
        return result


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        encoded = json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as err:
        raise AcquisitionQueryAuthorityError(
            "query-authority is niet canoniek JSON"
        ) from err
    if len(encoded) > MAX_AUTHORITY_BYTES:
        raise AcquisitionQueryAuthorityError(
            "query-authority overschrijdt het bytebudget"
        )
    return encoded


def _sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def _safe_query_key(prefix: str, digest: str) -> str:
    if not _SHA256.fullmatch(digest):
        raise AcquisitionQueryAuthorityError("query-key mist een geldige checksum")
    token = base64.b32encode(bytes.fromhex(digest)).decode("ascii").lower().rstrip("=")
    # Synoptic removes punctuation before scanning identifiers for credential-
    # shaped hex tokens.  Insert a literal non-hex ``z`` between every bounded
    # Base32 chunk so even that compacted form can never contain 32 hex chars.
    chunks = [token[offset : offset + 8] for offset in range(0, len(token), 8)]
    return prefix + ":" + ":z:".join(chunks)


def safe_native_job_id(job_id: str) -> str:
    """Derive the only credential-safe native identity from an origin job ID.

    The original acquisition identity remains the capacity/scheduler authority.
    Location-aware native handlers persist this non-hex alias because an origin
    ``acquisition:<64hex>`` identity is indistinguishable from credential
    material to strict providers such as Synoptic.  Recovery can reproduce the
    alias from its ledger row; no caller-selected alias is ever accepted.
    """

    if (
        not isinstance(job_id, str)
        or not job_id.startswith("acquisition:")
        or not _SHA256.fullmatch(job_id[len("acquisition:") :])
    ):
        raise AcquisitionQueryAuthorityError(
            "native jobidentity vereist een canonieke acquisition-job"
        )
    return _safe_query_key("acq", job_id[len("acquisition:") :])


def _normal_coordinate(value: float) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise AcquisitionQueryAuthorityError("querycoördinaat is niet eindig")
    return 0.0 if result == 0.0 else result


def _finite_time(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AcquisitionQueryAuthorityError(f"{name} moet een getal zijn")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 2**53:
        raise AcquisitionQueryAuthorityError(f"{name} valt buiten het tijdcontract")
    return result


def _distance_km(
    first_latitude: float,
    first_longitude: float,
    second_latitude: float,
    second_longitude: float,
) -> float:
    first_lat = math.radians(first_latitude)
    second_lat = math.radians(second_latitude)
    delta_lat = second_lat - first_lat
    delta_lon = math.radians(second_longitude - first_longitude)
    value = (
        math.sin(delta_lat / 2.0) ** 2
        + math.cos(first_lat) * math.cos(second_lat) * math.sin(delta_lon / 2.0) ** 2
    )
    return EARTH_RADIUS_KM * 2.0 * math.atan2(
        math.sqrt(value), math.sqrt(max(0.0, 1.0 - value))
    )


def _rounded_up_km(value: float) -> float:
    # Round outward to one metre without silently enlarging an exact provider
    # boundary (the configured 20 km Netatmo radius is a valid hard maximum).
    return math.ceil(float(value) * 1000.0) / 1000.0


def _rainviewer_center_tile(latitude: float, longitude: float) -> tuple[int, int]:
    if not -WEB_MERCATOR_MAX_LATITUDE < latitude < WEB_MERCATOR_MAX_LATITUDE:
        raise AcquisitionQueryCoverageError(
            "RainViewer-regiocentrum valt buiten Web Mercator"
        )
    if not -180.0 <= longitude < 180.0:
        raise AcquisitionQueryCoverageError(
            "RainViewer-longitude valt buiten het native contract"
        )
    count = 2**5
    x = int(math.floor((longitude + 180.0) / 360.0 * count)) % count
    radians = math.radians(latitude)
    y = int(
        math.floor(
            (1.0 - math.asinh(math.tan(radians)) / math.pi) / 2.0 * count
        )
    )
    if not 1 <= y < count - 1:
        raise AcquisitionQueryCoverageError(
            "RainViewer 3x3-query raakt de Web-Mercator-poolrand"
        )
    return x, y


def _rainviewer_tile_latitude(y: int) -> float:
    count = 2**5
    return math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / count))))


def _wrapped_longitude_delta(first: float, second: float) -> float:
    return (second - first + 180.0) % 360.0 - 180.0


def _rainviewer_guaranteed_radius_km(latitude: float, longitude: float) -> float:
    center_x, center_y = _rainviewer_center_tile(latitude, longitude)
    count = 2**5
    left_lon = ((center_x - 1) / count * 360.0) - 180.0
    right_lon = ((center_x + 2) / count * 360.0) - 180.0
    top_lat = _rainviewer_tile_latitude(center_y - 1)
    bottom_lat = _rainviewer_tile_latitude(center_y + 2)
    north = EARTH_RADIUS_KM * math.radians(top_lat - latitude)
    south = EARTH_RADIUS_KM * math.radians(latitude - bottom_lat)

    def meridian_distance(boundary_longitude: float) -> float:
        delta = abs(
            math.radians(_wrapped_longitude_delta(longitude, boundary_longitude))
        )
        value = min(
            1.0,
            abs(math.cos(math.radians(latitude)) * math.sin(delta)),
        )
        return EARTH_RADIUS_KM * math.asin(value)

    return max(
        0.0,
        min(
            north,
            south,
            meridian_distance(left_lon),
            meridian_distance(right_lon),
        )
        * 0.95,
    )


def _netatmo_spherical_bbox(
    latitude: float,
    longitude: float,
    radius_km: float,
) -> tuple[float, float, float, float]:
    angular = radius_km / EARTH_RADIUS_KM
    latitude_radians = math.radians(latitude)
    north = latitude + math.degrees(angular)
    south = latitude - math.degrees(angular)
    cosine = math.cos(latitude_radians)
    if north > 90.0 or south < -90.0 or abs(cosine) < 1e-12:
        raise AcquisitionQueryCoverageError(
            "Netatmo-querycirkel kruist een pool"
        )
    ratio = math.sin(angular) / abs(cosine)
    if ratio >= 1.0:
        raise AcquisitionQueryCoverageError(
            "Netatmo-querycirkel omvat een pool"
        )
    longitude_delta = math.degrees(math.asin(ratio))
    east = longitude + longitude_delta
    west = longitude - longitude_delta
    if east > 180.0 or west < -180.0:
        raise AcquisitionQueryCoverageError(
            "Netatmo-querycirkel kruist de datumgrens"
        )
    # Return outward-rounded wire coordinates; the native handler uses the
    # same six-decimal boundary so serialization cannot shrink coverage.
    scale = 1_000_000.0
    return (
        math.ceil(north * scale) / scale,
        math.ceil(east * scale) / scale,
        math.floor(south * scale) / scale,
        math.floor(west * scale) / scale,
    )


def _validate_netatmo_bbox(region: RegionQuery) -> None:
    if region.ground_required_radius_km is None:
        raise AcquisitionQueryCoverageError("Netatmo-grondradius ontbreekt")
    _netatmo_spherical_bbox(
        region.center_latitude,
        region.center_longitude,
        region.ground_required_radius_km,
    )


def _synoptic_center_compensated_radius_km(region: RegionQuery) -> float:
    """Cover the authority circle after Synoptic's five-decimal wire rounding."""

    required = region.ground_required_radius_km
    if required is None:
        raise AcquisitionQueryCoverageError("Synoptic-grondradius ontbreekt")
    wire_latitude = float(f"{region.center_latitude:.5f}")
    wire_longitude = float(f"{region.center_longitude:.5f}")
    center_shift = _distance_km(
        region.center_latitude,
        region.center_longitude,
        wire_latitude,
        wire_longitude,
    )
    compensated = _rounded_up_km(required + center_shift)
    if compensated > MAX_SYNOPTIC_RADIUS_KM:
        raise AcquisitionQueryCoverageError(
            "Synoptic-query vereist provider-specifieke opsplitsing na wireafronding"
        )
    return compensated


def _query_kind(targets: tuple[Target, ...]) -> str:
    return "roaming" if any(item.kind in _ROAMING_KINDS for item in targets) else "local"


def _target_reference(snapshot_sha256: str, target_id: str) -> str:
    return _sha256(
        {
            "snapshot_sha256": snapshot_sha256,
            "target_id": target_id,
        }
    )


def _validated_desired_state(desired_state: DesiredState) -> DesiredState:
    if not isinstance(desired_state, DesiredState):
        raise AcquisitionQueryAuthorityError(
            "query-authority vereist een canonieke DesiredState"
        )
    try:
        reparsed = parse_desired_state(desired_state.canonical_bytes)
    except Exception as err:
        raise AcquisitionQueryAuthorityError(
            "desired-state canonical bytes zijn ongeldig"
        ) from err
    if reparsed != desired_state:
        raise AcquisitionQueryAuthorityError(
            "desired-state velden en canonical bytes verschillen"
        )
    return reparsed


def _validated_state_plan(
    desired_state: DesiredState,
    plan: AcquisitionPlan,
) -> tuple[DesiredState, AcquisitionPlan]:
    reparsed = _validated_desired_state(desired_state)
    try:
        valid_plan = validate_worker_acquisition_plan(plan)
        rebuilt_plan = (
            build_worker_acquisition_plan(reparsed)
            if valid_plan.collector_scope
            == COLLECTOR_SCOPE_COVERAGE_RELEVANT
            else build_worker_acquisition_plan(
                reparsed,
                collector_scope=valid_plan.collector_scope,
            )
        )
    except Exception as err:
        raise AcquisitionQueryAuthorityError("acquisitieplan is ongeldig") from err
    if valid_plan != rebuilt_plan:
        raise AcquisitionQueryAuthorityError(
            "acquisitieplan is niet exact uit deze desired-state afgeleid"
        )
    return reparsed, valid_plan


def _validated_inputs(
    desired_state: DesiredState,
    plan: AcquisitionPlan,
    job: AcquisitionJob,
    *,
    evaluated_at: float,
) -> tuple[DesiredState, AcquisitionPlan, AcquisitionJob, float]:
    reparsed, valid_plan = _validated_state_plan(desired_state, plan)
    evaluated = _finite_time(evaluated_at, "evaluated_at")
    try:
        valid_job = validate_worker_acquisition_job(job)
        slot_jobs = build_worker_acquisition_slot_jobs(
            valid_plan,
            as_of=evaluated,
        )
    except Exception as err:
        raise AcquisitionQueryAuthorityError("acquisitiejob is ongeldig") from err
    matching = tuple(item for item in slot_jobs if item.job_id == valid_job.job_id)
    if matching != (valid_job,):
        raise AcquisitionQueryAuthorityError(
            "acquisitiejob behoort niet exact tot dit plan en cadansslot"
        )
    if valid_job.snapshot_sha256 != reparsed.state_sha256:
        raise AcquisitionQueryAuthorityError(
            "acquisitiejob bindt een andere desired-state"
        )
    if not valid_job.scheduled_for <= evaluated < valid_job.deadline_at:
        raise AcquisitionQueryAuthorityError(
            "query-authority valt buiten het actieve jobvenster"
        )
    return reparsed, valid_plan, valid_job, evaluated


def _assert_target_fresh(target: Target, *, evaluated_at: float) -> None:
    if not target.available or target.position_observed_at is None:
        raise AcquisitionQueryAuthorityError(
            "jobtarget mist een beschikbare geautoriseerde positie"
        )
    if target.kind not in _ROAMING_KINDS:
        return
    observed = target.position_observed_at
    if observed > evaluated_at + MAX_CLOCK_FUTURE_SKEW_SECONDS:
        raise AcquisitionQueryUnavailableError(
            "dynamic_position_future",
            "bewegende targetpositie ligt onplausibel in de toekomst"
        )
    if evaluated_at - observed > MAX_DYNAMIC_POSITION_AGE_SECONDS:
        raise AcquisitionQueryUnavailableError(
            "dynamic_position_expired",
            "bewegende targetpositie is voor deze job verlopen"
        )


def _build_target_queries(
    state: DesiredState,
    job: AcquisitionJob,
    *,
    evaluated_at: float,
) -> tuple[TargetQuery, ...]:
    by_id = {item.target_id: item for item in state.targets}
    result: list[TargetQuery] = []
    for target_id in job.target_ids:
        target = by_id.get(target_id)
        if target is None:
            raise AcquisitionQueryAuthorityError(
                "jobtarget ontbreekt uit de desired-state"
            )
        _assert_target_fresh(target, evaluated_at=evaluated_at)
        target_ref = _target_reference(state.state_sha256, target.target_id)
        identity = {
            "contract": TARGET_QUERY_CONTRACT,
            "job_sha256": job.job_sha256,
            "latitude": _normal_coordinate(target.latitude),
            "location_accuracy_m": target.location_accuracy_m,
            "longitude": _normal_coordinate(target.longitude),
            "position_observed_at": target.position_observed_at,
            "query_kind": "roaming" if target.kind in _ROAMING_KINDS else "local",
            "target_ref_sha256": target_ref,
        }
        digest = _sha256(identity)
        result.append(
            TargetQuery(
                query_key=_safe_query_key("aqt", digest),
                target_ref_sha256=target_ref,
                latitude=identity["latitude"],
                longitude=identity["longitude"],
                query_kind=identity["query_kind"],
                position_observed_at=target.position_observed_at,
                location_accuracy_m=target.location_accuracy_m,
                target_query_sha256=digest,
            )
        )
    if len(result) > MAX_QUERY_RECORDS:
        raise AcquisitionQueryAuthorityError("targetquerygrens is overschreden")
    return tuple(sorted(result, key=lambda item: item.target_ref_sha256))


def _build_region_queries(
    state: DesiredState,
    plan: AcquisitionPlan,
    job: AcquisitionJob,
    *,
    evaluated_at: float,
) -> tuple[RegionQuery, ...]:
    region_by_id = {item.region_id: item for item in plan.regions}
    target_by_id = {item.target_id: item for item in state.targets}
    result: list[RegionQuery] = []
    for region_id in job.region_ids:
        region = region_by_id.get(region_id)
        if region is None:
            raise AcquisitionQueryAuthorityError("job verwijst naar onbekende regio")
        targets = tuple(target_by_id[value] for value in region.target_ids)
        if not targets or any(not item.available for item in targets):
            raise AcquisitionQueryAuthorityError(
                "regio bevat geen volledige beschikbare targetset"
            )
        for target in targets:
            _assert_target_fresh(target, evaluated_at=evaluated_at)
            if (
                target.kind in _ACCURACY_REQUIRED_KINDS
                and target.location_accuracy_m is None
            ):
                raise AcquisitionQueryAuthorityError(
                    "dynamisch target mist location_accuracy_m"
                )
        target_refs = tuple(
            sorted(_target_reference(state.state_sha256, item.target_id) for item in targets)
        )
        is_ground = job.workload_id in {
            "ground.netatmo.region",
            "ground.synoptic.region",
        }
        ground_required = (
            _rounded_up_km(
                max(
                    _distance_km(
                        region.center_latitude,
                        region.center_longitude,
                        target.latitude,
                        target.longitude,
                    )
                    + state.settings.ground_radius_km
                    + (
                        0.0
                        if target.location_accuracy_m is None
                        else float(target.location_accuracy_m) / 1000.0
                    )
                    for target in targets
                )
            )
            if is_ground
            else None
        )
        radar_horizon = (
            _rounded_up_km(
                max(
                    _distance_km(
                        region.center_latitude,
                        region.center_longitude,
                        target.latitude,
                        target.longitude,
                    )
                    + target.observation_radius_km
                    + (
                        0.0
                        if target.location_accuracy_m is None
                        else float(target.location_accuracy_m) / 1000.0
                    )
                    for target in targets
                )
            )
            if job.workload_id == "radar.rainviewer.visual"
            else None
        )
        query_kind = _query_kind(targets) if is_ground else None
        identity = {
            "center_latitude": _normal_coordinate(region.center_latitude),
            "center_longitude": _normal_coordinate(region.center_longitude),
            "contract": REGION_QUERY_CONTRACT,
            "ground_required_radius_km": ground_required,
            "job_sha256": job.job_sha256,
            "query_kind": query_kind,
            "radar_horizon_km": radar_horizon,
            "source_region_sha256": region.region_sha256,
            "target_ref_sha256s": list(target_refs),
        }
        digest = _sha256(identity)
        result.append(
            RegionQuery(
                query_key=_safe_query_key("aqr", digest),
                source_region_sha256=region.region_sha256,
                center_latitude=identity["center_latitude"],
                center_longitude=identity["center_longitude"],
                radar_horizon_km=radar_horizon,
                ground_required_radius_km=ground_required,
                query_kind=query_kind,
                target_ref_sha256s=target_refs,
                region_query_sha256=digest,
            )
        )
    if len(result) > MAX_QUERY_RECORDS:
        raise AcquisitionQueryAuthorityError("regioquerygrens is overschreden")
    return tuple(sorted(result, key=lambda item: item.source_region_sha256))


def _build_authority_from_validated(
    state: DesiredState,
    plan: AcquisitionPlan,
    job: AcquisitionJob,
    *,
    evaluated_at: float,
) -> AcquisitionQueryAuthority:
    if job.workload_id not in _LOCATION_WORKLOADS:
        raise AcquisitionQueryAuthorityError(
            "workload heeft geen toegelaten locatiequeryprofiel"
        )
    target_queries = (
        _build_target_queries(state, job, evaluated_at=evaluated_at)
        if job.workload_id == "model.openmeteo.region"
        else ()
    )
    region_queries = (
        _build_region_queries(
            state,
            plan,
            job,
            evaluated_at=evaluated_at,
        )
        if job.workload_id != "model.openmeteo.region"
        else ()
    )
    payload = {
        "contract": CONTRACT,
        "desired_state_sha256": state.state_sha256,
        "evaluated_at": evaluated_at,
        "job_id": job.job_id,
        "job_sha256": job.job_sha256,
        "native_job_id": safe_native_job_id(job.job_id),
        "method": METHOD,
        "plan_sha256": plan.plan_sha256,
        "product_id": job.product_id,
        "provider_id": job.provider_id,
        "region_queries": [item.as_dict() for item in region_queries],
        "target_queries": [item.as_dict() for item in target_queries],
        "workload_id": job.workload_id,
    }
    canonical = _canonical_bytes(payload)
    return AcquisitionQueryAuthority(
        desired_state_sha256=state.state_sha256,
        plan_sha256=plan.plan_sha256,
        job_id=job.job_id,
        job_sha256=job.job_sha256,
        native_job_id=safe_native_job_id(job.job_id),
        workload_id=job.workload_id,
        provider_id=job.provider_id,
        product_id=job.product_id,
        evaluated_at=evaluated_at,
        region_queries=region_queries,
        target_queries=target_queries,
        canonical_bytes=canonical,
        authority_sha256=hashlib.sha256(canonical).hexdigest(),
    )


def build_acquisition_query_authority(
    desired_state: DesiredState,
    plan: AcquisitionPlan,
    job: AcquisitionJob,
    *,
    evaluated_at: float,
) -> AcquisitionQueryAuthority:
    """Build one authority; batch construction is required for a live cycle."""

    state, valid_plan, valid_job, evaluated = _validated_inputs(
        desired_state,
        plan,
        job,
        evaluated_at=evaluated_at,
    )
    result = _build_authority_from_validated(
        state,
        valid_plan,
        valid_job,
        evaluated_at=evaluated,
    )
    validate_acquisition_query_authority(result)
    return result


def build_acquisition_query_authorities(
    desired_state: DesiredState,
    plan: AcquisitionPlan,
    *,
    evaluated_at: float,
) -> AcquisitionQueryBatch:
    """Build all current location authorities with one state/plan rebuild."""

    state, valid_plan = _validated_state_plan(desired_state, plan)
    evaluated = _finite_time(evaluated_at, "evaluated_at")
    jobs = build_worker_acquisition_slot_jobs(valid_plan, as_of=evaluated)
    result: list[AcquisitionQueryAuthority] = []
    blocked: list[AcquisitionQueryBlock] = []
    for job in jobs:
        if job.workload_id not in _LEGACY_BATCH_LOCATION_WORKLOADS:
            continue
        if not job.scheduled_for <= evaluated < job.deadline_at:
            continue
        try:
            built = _build_authority_from_validated(
                state,
                valid_plan,
                job,
                evaluated_at=evaluated,
            )
        except AcquisitionQueryUnavailableError as err:
            block_identity = {
                "job_sha256": job.job_sha256,
                "provider_id": job.provider_id,
                "reason_code": err.reason_code,
                "workload_id": job.workload_id,
            }
            blocked.append(
                AcquisitionQueryBlock(
                    job_sha256=job.job_sha256,
                    workload_id=job.workload_id,
                    provider_id=job.provider_id,
                    reason_code=err.reason_code,
                    block_sha256=_sha256(block_identity),
                )
            )
            continue
        validate_acquisition_query_authority(built)
        result.append(built)
    ordered_authorities = tuple(
        sorted(
            result,
            key=lambda item: (
                item.workload_id,
                item.provider_id,
                item.product_id,
                item.job_sha256,
            ),
        )
    )
    ordered_blocks = tuple(
        sorted(
            blocked,
            key=lambda item: (
                item.workload_id,
                item.provider_id,
                item.job_sha256,
            ),
        )
    )
    batch_identity = {
        "authority_sha256s": [item.authority_sha256 for item in ordered_authorities],
        "blocked": [item.as_dict() for item in ordered_blocks],
        "desired_state_sha256": state.state_sha256,
        "evaluated_at": evaluated,
        "plan_sha256": valid_plan.plan_sha256,
    }
    batch = AcquisitionQueryBatch(
        desired_state_sha256=state.state_sha256,
        plan_sha256=valid_plan.plan_sha256,
        evaluated_at=evaluated,
        authorities=ordered_authorities,
        blocked=ordered_blocks,
        batch_sha256=_sha256(batch_identity),
    )
    validate_acquisition_query_batch(batch)
    return batch


def validate_acquisition_query_batch(
    value: AcquisitionQueryBatch,
) -> AcquisitionQueryBatch:
    """Revalidate a complete batch before diagnostics or runtime consumption."""

    if not isinstance(value, AcquisitionQueryBatch):
        raise AcquisitionQueryAuthorityError("querybatch heeft het verkeerde type")
    if not _is_sha256(value.desired_state_sha256) or not _is_sha256(value.plan_sha256):
        raise AcquisitionQueryAuthorityError("querybatch mist geldige authoritychecksums")
    if not _is_sha256(value.batch_sha256):
        raise AcquisitionQueryAuthorityError("querybatch mist een geldige checksum")
    evaluated = _finite_time(value.evaluated_at, "batch.evaluated_at")
    if not isinstance(value.authorities, tuple) or not isinstance(value.blocked, tuple):
        raise AcquisitionQueryAuthorityError("querybatchrecords zijn niet immutable")

    authority_order = tuple(
        sorted(
            value.authorities,
            key=lambda item: (
                item.workload_id,
                item.provider_id,
                item.product_id,
                item.job_sha256,
            ),
        )
    )
    if authority_order != value.authorities:
        raise AcquisitionQueryAuthorityError("querybatch authorities zijn niet canoniek geordend")
    block_order = tuple(
        sorted(
            value.blocked,
            key=lambda item: (
                item.workload_id,
                item.provider_id,
                item.job_sha256,
            ),
        )
    )
    if block_order != value.blocked:
        raise AcquisitionQueryAuthorityError("querybatch blocks zijn niet canoniek geordend")

    job_sha256s: list[str] = []
    for item in value.authorities:
        validate_acquisition_query_authority(item)
        if (
            item.desired_state_sha256 != value.desired_state_sha256
            or item.plan_sha256 != value.plan_sha256
            or item.evaluated_at != evaluated
        ):
            raise AcquisitionQueryAuthorityError("querybatch authoritybinding is gedrift")
        job_sha256s.append(item.job_sha256)

    for item in value.blocked:
        if not isinstance(item, AcquisitionQueryBlock):
            raise AcquisitionQueryAuthorityError("querybatch block heeft het verkeerde type")
        if (
            not _is_sha256(item.job_sha256)
            or not _is_sha256(item.block_sha256)
            or item.workload_id not in _LOCATION_WORKLOADS
            or not isinstance(item.provider_id, str)
            or not item.provider_id
            or item.reason_code not in _BLOCK_REASON_CODES
        ):
            raise AcquisitionQueryAuthorityError("querybatch block is ongeldig")
        block_identity = {
            "job_sha256": item.job_sha256,
            "provider_id": item.provider_id,
            "reason_code": item.reason_code,
            "workload_id": item.workload_id,
        }
        if _sha256(block_identity) != item.block_sha256:
            raise AcquisitionQueryAuthorityError("querybatch blockchecksum is gedrift")
        job_sha256s.append(item.job_sha256)

    if len(job_sha256s) != len(set(job_sha256s)):
        raise AcquisitionQueryAuthorityError("querybatch bevat een dubbele job")
    batch_identity = {
        "authority_sha256s": [item.authority_sha256 for item in value.authorities],
        "blocked": [item.as_dict() for item in value.blocked],
        "desired_state_sha256": value.desired_state_sha256,
        "evaluated_at": evaluated,
        "plan_sha256": value.plan_sha256,
    }
    if _sha256(batch_identity) != value.batch_sha256:
        raise AcquisitionQueryAuthorityError("querybatch checksum is gedrift")
    return value


def _region_query_identity(
    item: RegionQuery,
    *,
    job_sha256: str,
) -> dict[str, Any]:
    return {
        "center_latitude": item.center_latitude,
        "center_longitude": item.center_longitude,
        "contract": REGION_QUERY_CONTRACT,
        "ground_required_radius_km": item.ground_required_radius_km,
        "job_sha256": job_sha256,
        "query_kind": item.query_kind,
        "radar_horizon_km": item.radar_horizon_km,
        "source_region_sha256": item.source_region_sha256,
        "target_ref_sha256s": list(item.target_ref_sha256s),
    }


def _target_query_identity(
    item: TargetQuery,
    *,
    job_sha256: str,
) -> dict[str, Any]:
    return {
        "contract": TARGET_QUERY_CONTRACT,
        "job_sha256": job_sha256,
        "latitude": item.latitude,
        "location_accuracy_m": item.location_accuracy_m,
        "longitude": item.longitude,
        "position_observed_at": item.position_observed_at,
        "query_kind": item.query_kind,
        "target_ref_sha256": item.target_ref_sha256,
    }


def validate_acquisition_query_authority(
    value: AcquisitionQueryAuthority,
) -> AcquisitionQueryAuthority:
    """Revalidate every public field before it can emit provider input."""

    if not isinstance(value, AcquisitionQueryAuthority):
        raise AcquisitionQueryAuthorityError("query-authority heeft het verkeerde type")
    if value.workload_id not in _LOCATION_WORKLOADS:
        raise AcquisitionQueryAuthorityError("query-authority workload is niet toegestaan")
    for digest_name, digest in (
        ("desired-state", value.desired_state_sha256),
        ("plan", value.plan_sha256),
        ("job", value.job_sha256),
        ("authority", value.authority_sha256),
    ):
        if not _is_sha256(digest):
            raise AcquisitionQueryAuthorityError(
                f"query-authority mist geldige {digest_name}checksum"
            )
    evaluated = _finite_time(value.evaluated_at, "authority.evaluated_at")
    if value.native_job_id != safe_native_job_id(value.job_id):
        raise AcquisitionQueryAuthorityError("native jobidentity is gedrift")
    if len(value.region_queries) > MAX_QUERY_RECORDS or len(value.target_queries) > MAX_QUERY_RECORDS:
        raise AcquisitionQueryAuthorityError("query-authority overschrijdt de recordgrens")
    if tuple(sorted(value.region_queries, key=lambda item: item.source_region_sha256)) != value.region_queries:
        raise AcquisitionQueryAuthorityError("regioqueries zijn niet canoniek geordend")
    if tuple(sorted(value.target_queries, key=lambda item: item.target_ref_sha256)) != value.target_queries:
        raise AcquisitionQueryAuthorityError("targetqueries zijn niet canoniek geordend")
    if len({item.source_region_sha256 for item in value.region_queries}) != len(value.region_queries):
        raise AcquisitionQueryAuthorityError("query-authority bevat dubbele regio")
    if len({item.target_ref_sha256 for item in value.target_queries}) != len(value.target_queries):
        raise AcquisitionQueryAuthorityError("query-authority bevat dubbele targetreferentie")

    for item in value.region_queries:
        if not isinstance(item, RegionQuery):
            raise AcquisitionQueryAuthorityError("regioquery heeft het verkeerde type")
        if not _is_sha256(item.source_region_sha256):
            raise AcquisitionQueryAuthorityError("regioquery bronchecksum is ongeldig")
        if tuple(sorted(set(item.target_ref_sha256s))) != item.target_ref_sha256s:
            raise AcquisitionQueryAuthorityError("regioquery targetreferenties zijn niet canoniek")
        if not item.target_ref_sha256s or any(
            not _is_sha256(reference)
            for reference in item.target_ref_sha256s
        ):
            raise AcquisitionQueryAuthorityError("regioquery targetreferentie is ongeldig")
        if not -90.0 <= _normal_coordinate(item.center_latitude) <= 90.0:
            raise AcquisitionQueryAuthorityError("regioquery latitude is ongeldig")
        if not -180.0 <= _normal_coordinate(item.center_longitude) <= 180.0:
            raise AcquisitionQueryAuthorityError("regioquery longitude is ongeldig")
        if item.query_kind not in {None, "local", "roaming"}:
            raise AcquisitionQueryAuthorityError("regioquery kind is ongeldig")
        for radius in (item.radar_horizon_km, item.ground_required_radius_km):
            if radius is not None and (
                isinstance(radius, bool)
                or not isinstance(radius, (int, float))
                or not math.isfinite(float(radius))
                or not 0.0 < float(radius) <= 20_100.0
            ):
                raise AcquisitionQueryAuthorityError("regioquery radius is ongeldig")
        digest = _sha256(_region_query_identity(item, job_sha256=value.job_sha256))
        if digest != item.region_query_sha256:
            raise AcquisitionQueryAuthorityError("regioquery checksum is gedrift")
        if item.query_key != _safe_query_key("aqr", digest):
            raise AcquisitionQueryAuthorityError("regioquery key is gedrift")

    for item in value.target_queries:
        if not isinstance(item, TargetQuery):
            raise AcquisitionQueryAuthorityError("targetquery heeft het verkeerde type")
        if not _is_sha256(item.target_ref_sha256):
            raise AcquisitionQueryAuthorityError("targetquery referentie is ongeldig")
        if not -90.0 <= _normal_coordinate(item.latitude) <= 90.0:
            raise AcquisitionQueryAuthorityError("targetquery latitude is ongeldig")
        if not -180.0 <= _normal_coordinate(item.longitude) <= 180.0:
            raise AcquisitionQueryAuthorityError("targetquery longitude is ongeldig")
        if item.query_kind not in {"local", "roaming"}:
            raise AcquisitionQueryAuthorityError("targetquery kind is ongeldig")
        _finite_time(item.position_observed_at, "targetquery.position_observed_at")
        if item.location_accuracy_m is not None and (
            isinstance(item.location_accuracy_m, bool)
            or not isinstance(item.location_accuracy_m, (int, float))
            or not math.isfinite(float(item.location_accuracy_m))
            or not 0.0 <= float(item.location_accuracy_m) <= 100_000.0
        ):
            raise AcquisitionQueryAuthorityError("targetquery accuracy is ongeldig")
        digest = _sha256(_target_query_identity(item, job_sha256=value.job_sha256))
        if digest != item.target_query_sha256:
            raise AcquisitionQueryAuthorityError("targetquery checksum is gedrift")
        if item.query_key != _safe_query_key("aqt", digest):
            raise AcquisitionQueryAuthorityError("targetquery key is gedrift")

    if value.workload_id == "radar.rainviewer.visual":
        if len(value.region_queries) != 1 or value.target_queries:
            raise AcquisitionQueryAuthorityError("RainViewer authorityvorm is ongeldig")
        region = value.region_queries[0]
        if region.radar_horizon_km is None or region.ground_required_radius_km is not None or region.query_kind is not None:
            raise AcquisitionQueryAuthorityError("RainViewer querysemantiek is gedrift")
    elif value.workload_id in {"ground.netatmo.region", "ground.synoptic.region"}:
        if len(value.region_queries) != 1 or value.target_queries:
            raise AcquisitionQueryAuthorityError("ground authorityvorm is ongeldig")
        region = value.region_queries[0]
        if region.radar_horizon_km is not None or region.ground_required_radius_km is None or region.query_kind not in {"local", "roaming"}:
            raise AcquisitionQueryAuthorityError("ground querysemantiek is gedrift")
    elif value.workload_id == "model.openmeteo.region":
        if value.region_queries or not value.target_queries:
            raise AcquisitionQueryAuthorityError("Open-Meteo authorityvorm is ongeldig")
    elif value.workload_id in _PUBLIC_SOURCE_REGION_WORKLOADS:
        if len(value.region_queries) != 1 or value.target_queries:
            raise AcquisitionQueryAuthorityError(
                "publieke bron-authorityvorm is ongeldig"
            )
        region = value.region_queries[0]
        if (
            region.radar_horizon_km is not None
            or region.ground_required_radius_km is not None
            or region.query_kind is not None
        ):
            raise AcquisitionQueryAuthorityError(
                "publieke bron-querysemantiek is gedrift"
            )

    payload = {
        "contract": CONTRACT,
        "desired_state_sha256": value.desired_state_sha256,
        "evaluated_at": evaluated,
        "job_id": value.job_id,
        "job_sha256": value.job_sha256,
        "native_job_id": value.native_job_id,
        "method": METHOD,
        "plan_sha256": value.plan_sha256,
        "product_id": value.product_id,
        "provider_id": value.provider_id,
        "region_queries": [item.as_dict() for item in value.region_queries],
        "target_queries": [item.as_dict() for item in value.target_queries],
        "workload_id": value.workload_id,
    }
    canonical = _canonical_bytes(payload)
    if canonical != value.canonical_bytes:
        raise AcquisitionQueryAuthorityError("authorityvelden en canonical bytes verschillen")
    if hashlib.sha256(canonical).hexdigest() != value.authority_sha256:
        raise AcquisitionQueryAuthorityError("authoritychecksum is gedrift")
    return value


def _require_workload(authority: AcquisitionQueryAuthority, workload_id: str) -> None:
    if authority.workload_id != workload_id:
        raise AcquisitionQueryAuthorityError(
            "querypayload is niet toegestaan voor deze workload"
        )


def _exactly_one_region(authority: AcquisitionQueryAuthority) -> RegionQuery:
    if len(authority.region_queries) != 1:
        raise AcquisitionQueryCoverageError(
            "native regiocapture vereist exact één geautoriseerde regio"
        )
    return authority.region_queries[0]


def _require_ground_radius(
    region: RegionQuery,
    maximum_radius_km: float,
    provider_name: str,
) -> None:
    radius = region.ground_required_radius_km
    if radius is None or not 1.0 <= radius <= maximum_radius_km:
        raise AcquisitionQueryCoverageError(
            f"{provider_name}-query kan de volledige targetregio niet dekken; "
            "provider-specifieke opsplitsing is vereist"
        )


def _ground_payload(region: RegionQuery) -> dict[str, Any]:
    if region.ground_required_radius_km is None or region.query_kind is None:
        raise AcquisitionQueryCoverageError("groundquery mist radius of querykind")
    return {
        "latitude": region.center_latitude,
        "longitude": region.center_longitude,
        "query_key": region.query_key,
        "query_kind": region.query_kind,
        "radius_km": region.ground_required_radius_km,
    }


__all__ = [
    "AcquisitionQueryAuthority",
    "AcquisitionQueryAuthorityError",
    "AcquisitionQueryBatch",
    "AcquisitionQueryBlock",
    "AcquisitionQueryCoverageError",
    "AcquisitionQueryUnavailableError",
    "CONTRACT",
    "MAX_AUTHORITY_BYTES",
    "METHOD",
    "OPENMETEO_BATCH_CONTRACT",
    "PUBLIC_SOURCE_REGION_QUERY_CONTRACT",
    "REGION_QUERY_CONTRACT",
    "RegionQuery",
    "TARGET_QUERY_CONTRACT",
    "TargetQuery",
    "build_acquisition_query_authority",
    "build_acquisition_query_authorities",
    "safe_native_job_id",
    "validate_acquisition_query_authority",
    "validate_acquisition_query_batch",
]
