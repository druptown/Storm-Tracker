"""Operational contracts for the verified PhysicalSite catalogue."""
from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.ground_fusion import (
    PhysicalSite,
    StationAlias,
)
from storm_tracker_worker.tracking_core.engine.ground_observation_adapters import (
    PhysicalSiteCandidate,
    adapt_worker_observation,
)
from storm_tracker_worker.tracking_core.engine.physical_site_catalog import (
    CatalogAuditCode,
    CatalogConflictCode,
    PhysicalSiteCatalog,
    PhysicalSiteCatalogError,
    PhysicalSiteRecord,
    ResolutionStatus,
    SensorCapability,
    SitingAttribute,
    VerificationEvidence,
    VerifiedStationAlias,
    VerifiedStationIdentifier,
    resolve_adapter_candidates,
)


NOW = 1_800_000_000.0


def _evidence(name: str) -> VerificationEvidence:
    return VerificationEvidence(
        authority=f"authority:{name}",
        reference=f"snapshot:{name}:2026-07-30",
        verified_at=NOW - 3600.0,
        content_sha256=hashlib.sha256(name.encode()).hexdigest(),
    )


def _alias(
    provider: str,
    station: str,
    *,
    valid_from: float | None = None,
    valid_until: float | None = None,
) -> VerifiedStationAlias:
    return VerifiedStationAlias(
        provider_id=provider,
        station_id=station,
        mapping_evidence=_evidence(f"alias:{provider}:{station}"),
        valid_from=valid_from,
        valid_until=valid_until,
    )


def _record(
    site_key: str = "airport-kjfk-asos",
    *,
    latitude: float = 40.6398,
    longitude: float = -73.7789,
    tolerance_m: float = 250.0,
    group_key: str = "independence:jfk-asos",
    aliases: tuple[VerifiedStationAlias, ...] | None = None,
    identifiers: tuple[VerifiedStationIdentifier, ...] = (),
    capabilities: tuple[SensorCapability, ...] = (),
    siting: tuple[SitingAttribute, ...] = (),
    valid_from: float | None = None,
    valid_until: float | None = None,
    replaces_site_key: str | None = None,
    lifecycle_evidence: VerificationEvidence | None = None,
) -> PhysicalSiteRecord:
    return PhysicalSiteRecord(
        site_key=site_key,
        latitude=latitude,
        longitude=longitude,
        coordinate_tolerance_m=tolerance_m,
        independence_group_key=group_key,
        site_evidence=_evidence(f"site:{site_key}"),
        independence_evidence=_evidence(f"group:{group_key}"),
        aliases=aliases or (_alias("metar", "KJFK"),),
        authoritative_identifiers=identifiers,
        capabilities=capabilities,
        siting=siting,
        elevation_m=4.0,
        valid_from=valid_from,
        valid_until=valid_until,
        replaces_site_key=replaces_site_key,
        lifecycle_evidence=lifecycle_evidence,
    )


def _catalog(
    *records: PhysicalSiteRecord,
    audit_radius_m: float = 1000.0,
) -> PhysicalSiteCatalog:
    return PhysicalSiteCatalog(
        catalog_name="ground-sites",
        catalog_version="2026-07-30.1",
        records=tuple(records),
        proximity_audit_radius_m=audit_radius_m,
    )


def _candidate(
    provider: str,
    station: str,
    latitude: float,
    longitude: float,
    *,
    proposed_site_id: str = "unverified-proposal",
    proposed_group: str = "unverified-proposal-group",
) -> PhysicalSiteCandidate:
    alias = StationAlias(provider_id=provider, station_id=station)
    site = PhysicalSite(
        physical_site_id=proposed_site_id,
        latitude=latitude,
        longitude=longitude,
        independence_group=proposed_group,
        aliases=(alias,),
    )
    return PhysicalSiteCandidate(site=site, station_alias=alias)


def _by_provider(batch):
    return {
        resolution.provider_id: resolution
        for resolution in batch.resolutions
    }


def test_catalogue_is_immutable_canonical_and_order_independent():
    first = _record()
    second = _record(
        "gauge-brussels",
        latitude=50.8503,
        longitude=4.3517,
        group_key="independence:gauge-brussels",
        aliases=(_alias("netatmo", "module-bxl"),),
    )

    forward = _catalog(first, second)
    reverse = _catalog(second, first)

    assert forward.fingerprint == reverse.fingerprint
    assert forward.canonical_json == reverse.canonical_json
    assert forward.records[0].site_key == "airport-kjfk-asos"
    assert len(first.physical_site_id) == len("physical-site:") + 32
    assert len(first.independence_group) == len("independence-group:") + 32
    with pytest.raises(FrozenInstanceError):
        forward.catalog_version = "changed"
    with pytest.raises(FrozenInstanceError):
        first.latitude = 0.0


def test_explicit_verified_cross_provider_aliases_merge_one_site_only():
    record = _record(
        aliases=(
            _alias("synoptic", "KJFK"),
            _alias("metar", "KJFK"),
        )
    )
    catalog = _catalog(record)
    metar = _candidate(
        "metar",
        "KJFK",
        record.latitude,
        record.longitude,
        proposed_site_id="adapter-metar-site",
        proposed_group="adapter-group",
    )
    synoptic = _candidate(
        "synoptic",
        "KJFK",
        record.latitude,
        record.longitude,
        proposed_site_id="adapter-synoptic-site",
        proposed_group="another-adapter-group",
    )

    batch = resolve_adapter_candidates(
        (synoptic, metar),
        catalog,
        effective_at=NOW,
    )
    results = _by_provider(batch)

    assert set(results) == {"metar", "synoptic"}
    assert {
        result.status for result in results.values()
    } == {ResolutionStatus.RESOLVED}
    assert {
        result.physical_site_id for result in results.values()
    } == {record.physical_site_id}
    assert {
        result.independence_group for result in results.values()
    } == {record.independence_group}
    assert record.physical_site_id not in {
        metar.site.physical_site_id,
        synoptic.site.physical_site_id,
    }

    registry = catalog.to_ground_fusion_registry(effective_at=NOW)
    assert registry.catalog_fingerprint == catalog.fingerprint
    assert len(registry.registry_fingerprint) == 64
    assert {
        alias.mapping_fingerprint
        for site in registry.sites
        for alias in site.aliases
    } == {
        alias.fingerprint
        for alias in record.aliases
    }
    assert registry.resolve("METAR", "kjfk").physical_site_id == (
        record.physical_site_id
    )
    assert registry.resolve("synoptic", "KJFK").physical_site_id == (
        record.physical_site_id
    )


def test_exact_icao_can_correlate_but_cannot_invent_alias_merge():
    record = _record(aliases=(_alias("metar", "KJFK"),))
    candidate = _candidate(
        "synoptic",
        "KJFK",
        record.latitude,
        record.longitude,
    )

    batch = resolve_adapter_candidates(
        (candidate,),
        _catalog(record),
        effective_at=NOW,
    )
    result = batch.resolutions[0]

    assert result.status is ResolutionStatus.UNKNOWN_ALIAS
    assert result.physical_site_id is None
    assert result.independence_group is None
    assert result.provisional_correlation_group is not None
    assert CatalogAuditCode.EXACT_AVIATION_ID_CORRELATED_ONLY in (
        result.reason_codes
    )
    assert CatalogAuditCode.PROXIMITY_NOT_IDENTITY in result.reason_codes


def test_unshared_aviation_identifier_gets_no_fictitious_correlation():
    record = _record(aliases=(_alias("netatmo", "known-gauge"),))
    candidate = _candidate(
        "metar",
        "KALONE",
        record.latitude,
        record.longitude,
    )

    result = resolve_adapter_candidates(
        (candidate,),
        _catalog(record),
        effective_at=NOW,
    ).resolutions[0]

    assert result.status is ResolutionStatus.UNKNOWN_ALIAS
    assert result.provisional_correlation_group is None
    assert CatalogAuditCode.EXACT_AVIATION_ID_CORRELATED_ONLY not in (
        result.reason_codes
    )


def test_two_unverified_provider_rows_can_correlate_without_site_merge():
    catalog = _catalog(_record(aliases=(_alias("netatmo", "known"),)))
    batch = resolve_adapter_candidates(
        (
            _candidate("metar", "KSHARED", 40.0, -74.0),
            _candidate("synoptic", "KSHARED", 41.0, -75.0),
        ),
        catalog,
        effective_at=NOW,
    )

    assert {
        result.status for result in batch.resolutions
    } == {ResolutionStatus.UNKNOWN_ALIAS}
    provisional_groups = {
        result.provisional_correlation_group
        for result in batch.resolutions
    }
    assert len(provisional_groups) == 1
    assert None not in provisional_groups
    assert all(
        result.physical_site_id is None
        and result.independence_group is None
        for result in batch.resolutions
    )


def test_proximity_alone_is_audit_context_and_never_identity():
    record = _record()
    candidate = _candidate(
        "netatmo",
        "unknown-at-same-coordinate",
        record.latitude,
        record.longitude,
    )

    result = resolve_adapter_candidates(
        (candidate,),
        _catalog(record),
        effective_at=NOW,
    ).resolutions[0]

    assert result.status is ResolutionStatus.UNKNOWN_ALIAS
    assert result.physical_site_id is None
    assert result.independence_group is None
    assert result.provisional_correlation_group is None
    assert result.reason_codes == (
        CatalogAuditCode.UNKNOWN_VERIFIED_ALIAS,
        CatalogAuditCode.PROXIMITY_NOT_IDENTITY,
    )


def test_coordinate_tolerance_is_explicit_and_conflict_fails_closed():
    record = _record(tolerance_m=100.0)
    near = _candidate("metar", "KJFK", 40.6402, -73.7789)
    moved = _candidate("metar", "KJFK", 40.6500, -73.7789)
    catalog = _catalog(record)

    near_result = resolve_adapter_candidates(
        (near,),
        catalog,
        effective_at=NOW,
    ).resolutions[0]
    moved_result = resolve_adapter_candidates(
        (moved,),
        catalog,
        effective_at=NOW,
    ).resolutions[0]

    assert near_result.status is ResolutionStatus.RESOLVED
    assert near_result.coordinate_distance_m < 100.0
    assert moved_result.status is ResolutionStatus.COORDINATE_CONFLICT
    assert moved_result.coordinate_distance_m > 1000.0
    assert moved_result.coordinate_tolerance_m == 100.0
    assert moved_result.mapping_fingerprint is not None
    assert moved_result.physical_site_id is None
    assert moved_result.independence_group is None


@pytest.mark.parametrize(
    ("site_lat", "site_lon", "candidate_lat", "candidate_lon", "tolerance"),
    (
        (0.0, 179.999, 0.0, -179.999, 250.0),
        (89.0, 179.9, 89.0, -179.9, 500.0),
    ),
)
def test_global_distance_handles_antimeridian_and_high_latitude(
    site_lat,
    site_lon,
    candidate_lat,
    candidate_lon,
    tolerance,
):
    record = _record(
        "global-site",
        latitude=site_lat,
        longitude=site_lon,
        tolerance_m=tolerance,
        aliases=(_alias("netatmo", "global-station"),),
    )
    candidate = _candidate(
        "netatmo",
        "global-station",
        candidate_lat,
        candidate_lon,
    )

    result = resolve_adapter_candidates(
        (candidate,),
        _catalog(record),
        effective_at=NOW,
    ).resolutions[0]

    assert result.status is ResolutionStatus.RESOLVED
    assert 0.0 < result.coordinate_distance_m < tolerance


def test_exact_duplicate_candidate_is_resolved_once_and_audited():
    record = _record()
    candidate = _candidate(
        "metar",
        "KJFK",
        record.latitude,
        record.longitude,
    )

    batch = resolve_adapter_candidates(
        (candidate, candidate, candidate),
        _catalog(record),
        effective_at=NOW,
    )

    assert len(batch.resolutions) == 1
    duplicate = next(
        event
        for event in batch.audit_events
        if event.code is CatalogAuditCode.DUPLICATE_CANDIDATE
    )
    assert duplicate.occurrence_count == 3


def test_invalid_adapter_candidate_produces_no_identity():
    batch = resolve_adapter_candidates(
        (object(),),
        _catalog(_record()),
        effective_at=NOW,
    )

    assert len(batch.resolutions) == 1
    result = batch.resolutions[0]
    assert result.status is ResolutionStatus.INVALID_CANDIDATE
    assert result.physical_site_id is None
    assert result.independence_group is None
    assert batch.audit_events[0].code is (
        CatalogAuditCode.INVALID_ADAPTER_CANDIDATE
    )


def test_overlapping_alias_mapping_fails_with_deterministic_reason():
    first = _record("site-first")
    second = _record(
        "site-second",
        latitude=41.0,
        longitude=-74.0,
        group_key="independence:second",
    )

    with pytest.raises(PhysicalSiteCatalogError) as raised:
        _catalog(first, second)

    assert CatalogConflictCode.ALIAS_MAPS_TO_MULTIPLE_SITES in {
        issue.code for issue in raised.value.issues
    }
    assert len({issue.issue_id for issue in raised.value.issues}) == len(
        raised.value.issues
    )


def test_shared_aviation_id_correlates_distinct_sites_without_merging():
    metar_record = _record(
        "kjfk-metar-observation-point",
        group_key="correlated:kjfk",
        aliases=(_alias("metar", "KJFK"),),
    )
    synoptic_record = _record(
        "kjfk-synoptic-observation-point",
        latitude=40.6401,
        longitude=-73.7790,
        group_key="correlated:kjfk",
        aliases=(_alias("synoptic", "KJFK"),),
    )
    catalog = _catalog(metar_record, synoptic_record)
    batch = resolve_adapter_candidates(
        (
            _candidate("metar", "KJFK", 40.6398, -73.7789),
            _candidate("synoptic", "KJFK", 40.6401, -73.7790),
        ),
        catalog,
        effective_at=NOW,
    )

    assert {
        result.physical_site_id for result in batch.resolutions
    } == {
        metar_record.physical_site_id,
        synoptic_record.physical_site_id,
    }
    assert {
        result.independence_group for result in batch.resolutions
    } == {metar_record.independence_group}
    assert metar_record.physical_site_id != synoptic_record.physical_site_id


def test_shared_aviation_id_with_different_groups_is_rejected():
    metar_record = _record(
        "kjfk-metar",
        group_key="group-a",
        aliases=(_alias("metar", "KJFK"),),
    )
    synoptic_record = _record(
        "kjfk-synoptic",
        group_key="group-b",
        aliases=(_alias("synoptic", "KJFK"),),
    )

    with pytest.raises(PhysicalSiteCatalogError) as raised:
        _catalog(metar_record, synoptic_record)

    assert CatalogConflictCode.SHARED_AVIATION_GROUP_CONFLICT in {
        issue.code for issue in raised.value.issues
    }


def test_moved_site_uses_non_overlapping_versioned_alias_mapping():
    cutover = NOW
    old = _record(
        "station-old-location",
        latitude=40.0,
        longitude=-74.0,
        group_key="old-installation",
        aliases=(
            _alias("metar", "KMOV", valid_until=cutover),
        ),
        valid_until=cutover,
    )
    new = _record(
        "station-new-location",
        latitude=40.1,
        longitude=-74.1,
        group_key="new-installation",
        aliases=(
            _alias("metar", "KMOV", valid_from=cutover),
        ),
        valid_from=cutover,
        replaces_site_key=old.site_key,
        lifecycle_evidence=_evidence("station-move"),
    )
    catalog = _catalog(old, new)

    old_result = resolve_adapter_candidates(
        (_candidate("metar", "KMOV", 40.0, -74.0),),
        catalog,
        effective_at=cutover - 1.0,
    ).resolutions[0]
    new_result = resolve_adapter_candidates(
        (_candidate("metar", "KMOV", 40.1, -74.1),),
        catalog,
        effective_at=cutover,
    ).resolutions[0]

    assert old_result.physical_site_id == old.physical_site_id
    assert new_result.physical_site_id == new.physical_site_id
    assert old_result.independence_group != new_result.independence_group
    assert catalog.to_ground_fusion_registry(
        effective_at=cutover - 1.0
    ).resolve("metar", "KMOV").physical_site_id == old.physical_site_id
    assert catalog.to_ground_fusion_registry(
        effective_at=cutover
    ).resolve("metar", "KMOV").physical_site_id == new.physical_site_id


def test_capability_siting_and_authoritative_identifiers_are_sourced():
    capability = SensorCapability(
        provider_id="metar",
        sensor_id="present-weather-sensor",
        measurement_kind="present_weather",
        can_observe_absence=True,
        evidence=_evidence("capability"),
    )
    siting = SitingAttribute(
        name="exposure",
        value="airport-open-field",
        evidence=_evidence("siting"),
    )
    identifier = VerifiedStationIdentifier(
        namespace="icao",
        value="KJFK",
        evidence=_evidence("icao"),
    )
    record = _record(
        capabilities=(capability,),
        siting=(siting,),
        identifiers=(identifier,),
    )

    catalog = _catalog(record)

    assert catalog.records[0].capabilities == (capability,)
    assert catalog.records[0].siting == (siting,)
    assert catalog.records[0].authoritative_identifiers == (identifier,)
    assert '"can_observe_absence":true' in catalog.canonical_json
    assert _catalog(record).fingerprint == catalog.fingerprint


def test_duplicate_authoritative_identifier_is_rejected_with_reason():
    identifier = VerifiedStationIdentifier(
        namespace="icao",
        value="KJFK",
        evidence=_evidence("icao"),
    )
    record = _record(identifiers=(identifier, identifier))

    with pytest.raises(PhysicalSiteCatalogError) as raised:
        _catalog(record)

    assert CatalogConflictCode.DUPLICATE_AUTHORITATIVE_IDENTIFIER in {
        issue.code for issue in raised.value.issues
    }


def test_real_ground_adapter_candidate_resolves_through_catalogue():
    record = _record(aliases=(_alias("metar", "KJFK"),))
    adapted = adapt_worker_observation(
        {
            "source": "metar",
            "station_id": "metar:KJFK",
            "lat": record.latitude,
            "lon": record.longitude,
            "timestamp": NOW,
            "received_at": NOW + 30.0,
            "parser_version": "metar_wx_v4",
            "precipitation_present": False,
            "quality_flags": [],
            "raw_text": "METAR KJFK 301200Z 00000KT CAVOK",
            "raw_payload": {
                "station_id": "KJFK",
                "weather_groups": [],
                "sensor_status_flags": [],
                "maintenance_required": False,
                "raw_text": "METAR KJFK 301200Z 00000KT CAVOK",
            },
        }
    )

    result = resolve_adapter_candidates(
        adapted.site_candidates,
        _catalog(record),
        effective_at=NOW,
    ).resolutions[0]

    assert result.status is ResolutionStatus.RESOLVED
    assert result.physical_site_id == record.physical_site_id


def test_resolution_output_is_order_independent_and_fingerprinted():
    first_record = _record()
    second_record = _record(
        "brussels-gauge",
        latitude=50.85,
        longitude=4.35,
        group_key="brussels-gauge",
        aliases=(_alias("netatmo", "bxl"),),
    )
    catalog = _catalog(first_record, second_record)
    first = _candidate("metar", "KJFK", 40.6398, -73.7789)
    second = _candidate("netatmo", "bxl", 50.85, 4.35)

    forward = resolve_adapter_candidates(
        (first, second),
        catalog,
        effective_at=NOW,
    )
    reverse = resolve_adapter_candidates(
        (second, first),
        catalog,
        effective_at=NOW,
    )

    assert forward == reverse
    assert forward.fingerprint == reverse.fingerprint
    with pytest.raises(FrozenInstanceError):
        forward.effective_at = 0.0


def test_ha_worker_mirrors_are_exact_and_shadow_only():
    root = Path(__file__).resolve().parents[1]
    integration = (
        root
        / "custom_components/storm_tracker_v3/engine/"
        "physical_site_catalog.py"
    )
    worker = (
        root
        / "storm_tracker_worker/tracking_core/engine/"
        "physical_site_catalog.py"
    )

    assert integration.read_bytes() == worker.read_bytes()
    assert "physical_site_catalog" not in (
        root / "custom_components/storm_tracker_v3/__init__.py"
    ).read_text(encoding="utf-8")
    assert "physical_site_catalog" not in (
        root / "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")
