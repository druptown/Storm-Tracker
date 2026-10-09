"""Adversarial tests for the isolated durable ground-fusion shadow runtime."""
from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import sqlite3
import threading
import time

import pytest

from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
)
from storm_tracker_worker import ground_fusion_shadow_runtime as runtime_module
from storm_tracker_worker.ground_fusion_shadow_runtime import (
    DurableGroundFusionShadowRuntime,
    GeographicFootprintProof,
    GroundCellTrackBinding,
    GroundFactDispositionState,
    GroundFusionShadowCapacityError,
    GroundFusionShadowDeadlineError,
    GroundFusionShadowLimits,
    GroundFusionShadowRequest,
    GroundFusionShadowStoreError,
    GroundTargetSnapshot,
    bind_cell_track_footprint,
    bind_science_shadow_footprint,
    footprint_geometry_sha256,
)
from storm_tracker_worker.tracking_core.engine.canonical_ground_fusion_bridge import (
    AdaptedCanonicalGroundFact,
    CanonicalGroundBridgeBatch,
    GroundBridgeDeadlineError,
    GroundBridgeIssue,
    GroundBridgeIssueState,
    GroundSemanticsCertificate,
    GroundSemanticsProfile,
)
from storm_tracker_worker.tracking_core.engine.cell_track import CellDetection
from storm_tracker_worker.tracking_core.engine.cell_track_lifecycle import (
    CellTrackLifecyclePolicy,
    start_cell_track,
)
from storm_tracker_worker.tracking_core.engine.ground_fusion import (
    CellTrackFootprint,
    GroundConfirmationState,
    GroundFusionPolicy,
    GroundObservation,
    MeasurementSemantics,
    PrecipitationState,
    RawGroundProvenance,
)
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
)
from storm_tracker_worker.tracking_core.engine.physical_site_catalog import (
    PhysicalSiteCatalog,
    PhysicalSiteRecord,
    SensorCapability,
    VerificationEvidence,
    VerifiedStationAlias,
)


NOW = 1_800_000_000.0
CONFIG = "a" * 64
REVIEW = "b" * 64


def _evidence(label: str) -> VerificationEvidence:
    return VerificationEvidence(
        authority="test-authority",
        reference=f"https://example.test/{label}",
        verified_at=NOW - 10_000.0,
        content_sha256="c" * 64,
    )


def _record(
    site: str,
    group: str,
    latitude: float,
    longitude: float,
    *aliases: tuple[str, str],
) -> PhysicalSiteRecord:
    verified_aliases = tuple(
        VerifiedStationAlias(
            provider_id=provider,
            station_id=station,
            mapping_evidence=_evidence(f"alias/{provider}/{station}"),
        )
        for provider, station in aliases
    )
    capabilities = tuple(
        SensorCapability(
            provider_id=provider,
            sensor_id=station,
            measurement_kind=(
                GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1.value
                if provider == "metar"
                else GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1.value
            ),
            can_observe_absence=True,
            evidence=_evidence(f"capability/{provider}/{station}"),
        )
        for provider, station in aliases
        if provider in {"metar", "synoptic"}
    )
    return PhysicalSiteRecord(
        site_key=site,
        latitude=latitude,
        longitude=longitude,
        coordinate_tolerance_m=500.0,
        independence_group_key=group,
        site_evidence=_evidence(f"site/{site}"),
        independence_evidence=_evidence(f"group/{group}"),
        aliases=verified_aliases,
        capabilities=capabilities,
    )


def _catalog(*records: PhysicalSiteRecord) -> PhysicalSiteCatalog:
    return PhysicalSiteCatalog(
        catalog_name="ground-shadow-tests",
        catalog_version="1",
        records=tuple(records),
        proximity_audit_radius_m=1_000.0,
    )


def _default_catalog() -> PhysicalSiteCatalog:
    return _catalog(
        _record(
            "airport-a",
            "independent-a",
            40.750,
            -73.980,
            ("metar", "KAAA"),
            ("synoptic", "SAAA"),
        ),
        _record(
            "gauge-b",
            "independent-b",
            40.755,
            -73.975,
            ("synoptic", "SBBB"),
        ),
        _record(
            "far-dry",
            "independent-far",
            41.500,
            -75.000,
            ("metar", "KFAR"),
        ),
    )


def _certificate(profile: GroundSemanticsProfile) -> GroundSemanticsCertificate:
    provider = "synoptic" if "synoptic" in profile.value else "metar"
    return GroundSemanticsCertificate(
        certificate_id=f"test:{profile.value}",
        certificate_version="1",
        provider=provider,
        product=f"{provider}-native-test",
        decoder_identity=f"{provider}-decoder",
        decoder_version="1",
        config_sha256=CONFIG,
        profile=profile,
        primary_sources=("https://example.test/provider-contract",),
        review_evidence_sha256=REVIEW,
    )


def _certificates() -> tuple[GroundSemanticsCertificate, ...]:
    return (
        _certificate(GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1),
        _certificate(GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
    )


def _policy() -> GroundFusionPolicy:
    return GroundFusionPolicy(
        freshness_seconds_by_provider=(
            ("metar", 600.0),
            ("synoptic", 600.0),
            ("netatmo", 300.0),
        ),
        minimum_independent_wet_groups=2,
        spatial_tolerance_km=0.0,
        temporal_tolerance_seconds=0.0,
        minimum_interval_overlap_fraction=0.5,
        maximum_site_coordinate_disagreement_km=1.0,
        maximum_future_skew_seconds=60.0,
    )


def _footprint() -> CellTrackFootprint:
    return CellTrackFootprint(
        footprint_id="footprint-track-1-detection-1",
        cell_track_id="track-1",
        cell_detection_id="detection-1",
        valid_from=NOW - 60.0,
        valid_until=NOW + 60.0,
        representative_at=NOW,
        rings=((
            (40.700, -74.050),
            (40.800, -74.050),
            (40.800, -73.900),
            (40.700, -73.900),
        ),),
        radar_source="certified-radar-shadow",
        raw_artifact_receipt_id="radar-receipt-track-1",
    )


def _binding(footprint: CellTrackFootprint | None = None) -> GroundCellTrackBinding:
    footprint = footprint or _footprint()
    proof = GeographicFootprintProof(
        proof_id="geographic-proof-1",
        proof_version="1",
        source_science_receipt_sha256="1" * 64,
        source_target_grid_sha256="2" * 64,
        source_transform_receipt_sha256="3" * 64,
        geometry_builder_id="certified-grid-cell-contour",
        geometry_certificate_sha256="4" * 64,
        footprint_geometry_sha256=footprint_geometry_sha256(footprint),
    )
    return GroundCellTrackBinding(
        footprint=footprint,
        engine_id="cell-engine-1",
        engine_state_revision=4,
        track_revision=3,
        analysis_grid_id="analysis-grid-1",
        science_run_id="science-run-1",
        science_transition_id="transition-4",
        science_receipt_sha256="1" * 64,
        source_frame_id="radar-frame-4",
        target_grid_sha256="2" * 64,
        radar_semantics_certificate_sha256="5" * 64,
        transform_certificate_sha256="6" * 64,
        parent_track_ids=(),
        child_track_ids=(),
        lineage_event_ids=("birth-track-1",),
        applied_lineage_hypothesis_ids=(),
        footprint_proof=proof,
    )


def _target(**changes) -> GroundTargetSnapshot:
    values = {
        "target_id": "device_tracker.private_person",
        "target_revision": "revision-10",
        "latitude": 40.750,
        "longitude": -73.980,
        "observed_at": NOW - 10.0,
        "source_state_sha256": "7" * 64,
    }
    values.update(changes)
    return GroundTargetSnapshot(**values)


def _request(*, attempt: str = "attempt-1", target=None, cell=None, evaluated=NOW):
    return GroundFusionShadowRequest(
        attempt_id=attempt,
        decision_key="target-track-1-detection-1",
        region_key="new-york-test-region",
        target=target or _target(),
        cell=cell or _binding(),
        evaluated_at=evaluated,
    )


def _observation(
    identifier: str,
    provider: str,
    station: str,
    latitude: float,
    longitude: float,
    *,
    state: PrecipitationState = PrecipitationState.PRESENT,
    semantics: MeasurementSemantics = MeasurementSemantics.INSTANTANEOUS_STATE,
    start: float = NOW - 30.0,
    end: float | None = None,
    amount: float | None = None,
    unit: str | None = None,
) -> GroundObservation:
    end = start if end is None else end
    return GroundObservation(
        observation_id=identifier,
        provider_id=provider,
        station_id=station,
        latitude=latitude,
        longitude=longitude,
        interval_start_at=start,
        interval_end_at=end,
        representative_at=end,
        received_at=end + 5.0,
        precipitation_state=state,
        measurement_semantics=semantics,
        provenance=RawGroundProvenance(
            provider_record_id=f"record-{identifier}",
            parser_version=f"{provider}-decoder-1",
            raw_artifact_receipt_id=f"raw-receipt-{identifier}",
            raw_content_sha256="8" * 64,
            raw_text=None,
        ),
        amount=amount,
        measurement_unit=unit,
        quality_flags=("provider-qc-clean",),
    )


def _adapted(index, observation, registry, certificate_index=0):
    resolved = registry.resolve_with_alias(
        observation.provider_id,
        observation.station_id,
    )
    assert resolved is not None
    site, alias = resolved
    assert alias.mapping_fingerprint is not None
    marker = format((index + 1) % 16, "x")
    return AdaptedCanonicalGroundFact(
        source_index=index,
        fact_id=f"fact-{index}",
        fact_sha256=marker * 64,
        receipt_sha256=format((index + 8) % 16, "x") * 64,
        certificate_fingerprint=_certificates()[certificate_index].fingerprint,
        physical_site_id=site.physical_site_id,
        mapping_fingerprint=alias.mapping_fingerprint,
        observation=observation,
    )


def _bridge_issue(index, provider, station, code, *, state=GroundBridgeIssueState.WITHHELD):
    marker = format((index + 1) % 16, "x")
    return GroundBridgeIssue(
        source_index=index,
        fact_id=f"fact-{index}",
        fact_sha256=marker * 64,
        receipt_sha256=format((index + 8) % 16, "x") * 64,
        provider=provider,
        provider_station_id=station,
        state=state,
        code=code,
        detail="Authorization=Bearer super-secret-token must never be persisted",
    )


def _batch(registry, *, facts=(), issues=()):
    return CanonicalGroundBridgeBatch(
        source_fact_count=len(facts) + len(issues),
        facts=tuple(facts),
        issues=tuple(issues),
        registry_fingerprint=registry.registry_fingerprint,
    )


def _install_batch(monkeypatch, factory):
    calls = []

    def query(store, **kwargs):
        calls.append(kwargs)
        return factory(kwargs["site_registry"])

    monkeypatch.setattr(runtime_module, "query_and_adapt_canonical_ground", query)
    return calls


def _runtime(tmp_path, *, catalog=None, limits=None):
    store = object.__new__(CanonicalGroundEvidenceStore)
    return DurableGroundFusionShadowRuntime(
        store,
        site_catalog=catalog or _default_catalog(),
        semantics_certificates=_certificates(),
        fusion_policy=_policy(),
        receipt_path=tmp_path / "ground-shadow.sqlite3",
        limits=limits or GroundFusionShadowLimits(),
    )


def _deadline():
    return time.monotonic() + 5.0


def test_two_independent_wet_sites_confirm_only_shadow_and_preserve_semantics(
    tmp_path,
    monkeypatch,
):
    def factory(registry):
        metar = _observation(
            "metar-wet",
            "metar",
            "KAAA",
            40.750,
            -73.980,
        )
        synoptic = _observation(
            "synoptic-wet",
            "synoptic",
            "SBBB",
            40.755,
            -73.975,
            semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
            start=NOW - 120.0,
            end=NOW,
            amount=0.4,
            unit="mm",
        )
        return _batch(
            registry,
            facts=(
                _adapted(0, metar, registry, 0),
                _adapted(1, synoptic, registry, 1),
            ),
        )

    calls = _install_batch(monkeypatch, factory)
    result = _runtime(tmp_path).process(_request(), deadline_at=_deadline())

    assert result.receipt.candidate_state is GroundConfirmationState.CONFIRMED
    assert result.receipt.candidate_confirmed is True
    assert result.receipt.operational_confirmation_claim is False
    assert result.receipt.operational_snapshot_claim_generated is False
    assert result.receipt.meteorological_admission_granted is False
    assert result.receipt.accounting.full_scan_complete is True
    assert result.receipt.accounting.counts["used"] == 2
    assert {
        item.measurement_semantics
        for item in result.receipt.accounting.dispositions
    } == {"instantaneous_state", "interval_accumulation"}
    accumulation = next(
        item for item in result.receipt.accounting.dispositions
        if item.measurement_semantics == "interval_accumulation"
    )
    assert accumulation.measurement_amount == pytest.approx(0.4)
    assert accumulation.measurement_unit == "mm"
    assert calls[0].get("provider") is None
    assert calls[0]["limit"] == 257
    assert calls[0]["observed_from"] == NOW - 600.0
    assert calls[0]["observed_through"] == NOW + 60.0


def test_station_outside_cell_never_counts_as_dry_counterevidence(tmp_path, monkeypatch):
    def factory(registry):
        inside = _observation(
            "inside-dry",
            "metar",
            "KAAA",
            40.750,
            -73.980,
            state=PrecipitationState.ABSENT,
        )
        outside = _observation(
            "outside-dry",
            "metar",
            "KFAR",
            41.500,
            -75.000,
            state=PrecipitationState.ABSENT,
        )
        return _batch(
            registry,
            facts=(
                _adapted(0, inside, registry),
                _adapted(1, outside, registry),
            ),
        )

    _install_batch(monkeypatch, factory)
    result = _runtime(tmp_path).process(_request(), deadline_at=_deadline())

    assert result.receipt.candidate_state is GroundConfirmationState.NOT_CONFIRMED
    dispositions = result.receipt.accounting.dispositions
    assert dispositions[0].state is GroundFactDispositionState.USED
    assert dispositions[0].supports_precipitation is False
    assert dispositions[0].dry_capability_fingerprint is not None
    assert dispositions[1].state is GroundFactDispositionState.OUTSIDE
    assert "outside_cell_footprint" in dispositions[1].reason_codes
    assert len(result.receipt.independent_dry_groups) == 1


def test_dry_fact_without_exact_absence_capability_is_withheld(tmp_path, monkeypatch):
    record = replace(
        _record(
            "no-dry-capability",
            "no-dry-capability-group",
            40.750,
            -73.980,
            ("metar", "KNODRY"),
        ),
        capabilities=(),
    )
    catalog = _catalog(record)

    def factory(registry):
        dry = _observation(
            "unproven-dry",
            "metar",
            "KNODRY",
            40.750,
            -73.980,
            state=PrecipitationState.ABSENT,
        )
        return _batch(registry, facts=(_adapted(0, dry, registry, 0),))

    _install_batch(monkeypatch, factory)
    result = _runtime(tmp_path, catalog=catalog).process(
        _request(),
        deadline_at=_deadline(),
    )

    item = result.receipt.accounting.dispositions[0]
    assert result.receipt.candidate_state is GroundConfirmationState.INDETERMINATE
    assert item.state is GroundFactDispositionState.WITHHELD
    assert "dry_absence_capability_unverified" in item.reason_codes
    assert result.receipt.independent_dry_groups == ()


def test_metar_synoptic_aliases_at_same_physical_site_vote_once(tmp_path, monkeypatch):
    def factory(registry):
        return _batch(
            registry,
            facts=(
                _adapted(0, _observation(
                    "metar-alias", "metar", "KAAA", 40.750, -73.980
                ), registry, 0),
                _adapted(1, _observation(
                    "synoptic-alias",
                    "synoptic",
                    "SAAA",
                    40.750,
                    -73.980,
                    semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
                    start=NOW - 100.0,
                    end=NOW,
                    amount=0.2,
                    unit="mm",
                ), registry, 1),
            ),
        )

    _install_batch(monkeypatch, factory)
    result = _runtime(tmp_path).process(_request(), deadline_at=_deadline())

    assert result.receipt.candidate_state is GroundConfirmationState.NOT_CONFIRMED
    assert result.receipt.accounting.counts["used"] == 1
    assert result.receipt.accounting.counts["duplicate"] == 1
    assert len(result.receipt.independent_wet_groups) == 1
    dispositions = result.receipt.accounting.dispositions
    assert dispositions[0].physical_site_id == dispositions[1].physical_site_id
    assert dispositions[0].independence_group == dispositions[1].independence_group


def test_conflicting_aliases_fail_closed_to_indeterminate(tmp_path, monkeypatch):
    def factory(registry):
        wet = _observation("wet", "metar", "KAAA", 40.750, -73.980)
        dry = _observation(
            "dry",
            "synoptic",
            "SAAA",
            40.750,
            -73.980,
            state=PrecipitationState.ABSENT,
            semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
            start=NOW - 100.0,
            end=NOW,
            amount=0.0,
            unit="mm",
        )
        return _batch(
            registry,
            facts=(
                _adapted(0, wet, registry, 0),
                _adapted(1, dry, registry, 1),
            ),
        )

    _install_batch(monkeypatch, factory)
    result = _runtime(tmp_path).process(_request(), deadline_at=_deadline())

    assert result.receipt.candidate_state is GroundConfirmationState.INDETERMINATE
    assert "conflicted_independence_group" in result.receipt.reason_codes
    assert result.receipt.accounting.counts["indeterminate"] == 2
    assert len(result.receipt.conflicted_groups) == 1


def test_cumulative_only_weak_overlap_never_becomes_instantaneous_rate(
    tmp_path,
    monkeypatch,
):
    def factory(registry):
        cumulative = _observation(
            "one-hour-cumulative",
            "synoptic",
            "SBBB",
            40.755,
            -73.975,
            semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
            start=NOW - 3600.0,
            end=NOW,
            amount=0.1,
            unit="mm",
        )
        return _batch(registry, facts=(_adapted(0, cumulative, registry, 1),))

    _install_batch(monkeypatch, factory)
    result = _runtime(tmp_path).process(_request(), deadline_at=_deadline())
    item = result.receipt.accounting.dispositions[0]

    assert item.state is GroundFactDispositionState.OUTSIDE
    assert item.measurement_semantics == "interval_accumulation"
    assert item.measurement_amount == pytest.approx(0.1)
    assert item.measurement_unit == "mm"
    assert item.supports_precipitation is None
    assert "insufficient_interval_overlap" in item.reason_codes
    assert "instantaneous_rate" not in result.receipt.to_mapping().__repr__()


def test_withheld_netatmo_future_stale_and_unavailable_are_all_accounted(
    tmp_path,
    monkeypatch,
):
    def factory(registry):
        stale = _observation(
            "stale",
            "metar",
            "KAAA",
            40.750,
            -73.980,
            start=NOW - 700.0,
        )
        future = _observation(
            "future",
            "metar",
            "KAAA",
            40.750,
            -73.980,
            start=NOW + 120.0,
        )
        unavailable = _observation(
            "unavailable",
            "synoptic",
            "SBBB",
            40.755,
            -73.975,
            state=PrecipitationState.UNAVAILABLE,
            semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
            start=NOW - 100.0,
            end=NOW,
        )
        return _batch(
            registry,
            facts=(
                _adapted(0, stale, registry, 0),
                _adapted(1, future, registry, 0),
                _adapted(2, unavailable, registry, 1),
            ),
            issues=(
                _bridge_issue(
                    3,
                    "netatmo",
                    "70:ee:50:29:59:56",
                    "netatmo_semantics_uncertified",
                ),
                _bridge_issue(
                    4,
                    "metar",
                    "KBAD",
                    "canonical_fact_contract_invalid",
                    state=GroundBridgeIssueState.INVALID,
                ),
            ),
        )

    _install_batch(monkeypatch, factory)
    result = _runtime(tmp_path).process(_request(), deadline_at=_deadline())
    states = tuple(item.state for item in result.receipt.accounting.dispositions)

    assert result.receipt.candidate_state is GroundConfirmationState.INDETERMINATE
    assert states == (
        GroundFactDispositionState.STALE,
        GroundFactDispositionState.STALE,
        GroundFactDispositionState.INDETERMINATE,
        GroundFactDispositionState.WITHHELD,
        GroundFactDispositionState.INDETERMINATE,
    )
    assert "bridge_evidence_invalid" in (
        result.receipt.accounting.dispositions[4].reason_codes
    )
    assert result.receipt.accounting.source_fact_count == 5
    assert sum(result.receipt.accounting.counts.values()) == 5


def test_overflow_marks_every_returned_fact_and_never_fuses_subset(tmp_path, monkeypatch):
    limits = replace(GroundFusionShadowLimits(), max_source_facts=2)

    def factory(registry):
        observations = tuple(
            _observation(
                f"wet-{index}",
                "metar",
                "KAAA",
                40.750,
                -73.980,
                start=NOW - 30.0 - index,
            )
            for index in range(3)
        )
        return _batch(
            registry,
            facts=tuple(
                _adapted(index, item, registry)
                for index, item in enumerate(observations)
            ),
        )

    calls = _install_batch(monkeypatch, factory)
    result = _runtime(tmp_path, limits=limits).process(
        _request(), deadline_at=_deadline()
    )

    accounting = result.receipt.accounting
    assert calls[0]["limit"] == 3
    assert result.receipt.candidate_state is GroundConfirmationState.INDETERMINATE
    assert accounting.full_scan_complete is False
    assert accounting.overflowed is True
    assert accounting.unscanned_fact_count_lower_bound == 1
    assert accounting.counts["overflow"] == 3
    assert result.decision is None


def test_exact_retry_and_restart_replay_skip_source_query(tmp_path, monkeypatch):
    def factory(registry):
        return _batch(registry)

    calls = _install_batch(monkeypatch, factory)
    request = _request()
    first = _runtime(tmp_path).process(request, deadline_at=_deadline())
    assert len(calls) == 1

    def forbidden(*args, **kwargs):
        raise AssertionError("durable replay must not query ground source")

    monkeypatch.setattr(runtime_module, "query_and_adapt_canonical_ground", forbidden)
    second = _runtime(tmp_path).process(request, deadline_at=_deadline())

    assert second.duplicate is True
    assert second.decision is None
    assert second.receipt.receipt_sha256 == first.receipt.receipt_sha256


def test_concurrent_exact_retry_commits_once_and_reports_duplicate(tmp_path, monkeypatch):
    rendezvous = threading.Barrier(2)

    def query(store, **kwargs):
        rendezvous.wait(timeout=5.0)
        return _batch(kwargs["site_registry"])

    monkeypatch.setattr(runtime_module, "query_and_adapt_canonical_ground", query)
    first_runtime = _runtime(tmp_path)
    second_runtime = _runtime(tmp_path)
    request = _request()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(first_runtime.process, request, deadline_at=_deadline()),
            executor.submit(second_runtime.process, request, deadline_at=_deadline()),
        )
        results = tuple(item.result(timeout=10.0) for item in futures)

    assert sorted(item.duplicate for item in results) == [False, True]
    assert len({item.receipt.receipt_sha256 for item in results}) == 1
    assert first_runtime.receipt_store.diagnostics()["receipt_count"] == 1


def test_receipt_tamper_and_schema_tamper_fail_closed(tmp_path, monkeypatch):
    _install_batch(monkeypatch, lambda registry: _batch(registry))
    runtime = _runtime(tmp_path)
    request = _request()
    runtime.process(request, deadline_at=_deadline())
    database_path = tmp_path / "ground-shadow.sqlite3"
    with closing(sqlite3.connect(database_path)) as database:
        database.execute("DROP TRIGGER receipts_no_update")
        database.execute(
            "UPDATE receipts SET payload=?",
            (b'{}',),
        )
        database.commit()

    with pytest.raises(GroundFusionShadowStoreError):
        runtime.receipt_store.load_existing(
            attempt_id=request.attempt_id,
            request_sha256=runtime._request_sha256(request),
        )


def test_deadline_failure_writes_no_shadow_weather_receipt(tmp_path, monkeypatch):
    def expired(*args, **kwargs):
        raise GroundBridgeDeadlineError("expired")

    monkeypatch.setattr(runtime_module, "query_and_adapt_canonical_ground", expired)
    runtime = _runtime(tmp_path)

    with pytest.raises(GroundFusionShadowDeadlineError):
        runtime.process(_request(), deadline_at=_deadline())
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 0


def test_durable_row_capacity_fails_closed_without_overwriting(tmp_path, monkeypatch):
    limits = replace(GroundFusionShadowLimits(), max_receipts=1)
    _install_batch(monkeypatch, lambda registry: _batch(registry))
    runtime = _runtime(tmp_path, limits=limits)
    runtime.process(_request(attempt="attempt-1"), deadline_at=_deadline())

    with pytest.raises(GroundFusionShadowCapacityError):
        runtime.process(
            _request(attempt="attempt-2", evaluated=NOW + 1.0),
            deadline_at=_deadline(),
        )
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 1


def test_receipt_byte_capacity_fails_closed_without_partial_row(tmp_path, monkeypatch):
    limits = replace(
        GroundFusionShadowLimits(),
        max_accounting_bytes=4096,
        max_receipt_bytes=4096,
    )

    def factory(registry):
        return _batch(
            registry,
            issues=tuple(
                _bridge_issue(
                    index,
                    "netatmo",
                    f"private-station-{index}",
                    "netatmo_semantics_uncertified",
                )
                for index in range(8)
            ),
        )

    _install_batch(monkeypatch, factory)
    runtime = _runtime(tmp_path, limits=limits)
    with pytest.raises(GroundFusionShadowCapacityError):
        runtime.process(_request(), deadline_at=_deadline())
    assert runtime.receipt_store.diagnostics()["receipt_count"] == 0


def test_stale_target_skips_ground_query_and_is_indeterminate(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("stale target must fail before ground query")

    monkeypatch.setattr(runtime_module, "query_and_adapt_canonical_ground", forbidden)
    runtime = _runtime(tmp_path)
    request = _request(target=_target(observed_at=NOW - 1_000.0))
    result = runtime.process(request, deadline_at=_deadline())

    assert result.receipt.candidate_state is GroundConfirmationState.INDETERMINATE
    assert result.receipt.accounting.query_executed is False
    assert result.receipt.reason_codes == ("target_context_stale",)


def test_alias_must_cover_the_observations_complete_interval(tmp_path, monkeypatch):
    base_record = _record(
        "interval-site",
        "interval-group",
        40.750,
        -73.980,
        ("synoptic", "SINT"),
    )
    alias = replace(base_record.aliases[0], valid_from=NOW - 10.0)
    catalog = _catalog(replace(base_record, aliases=(alias,)))

    def factory(registry):
        observation = _observation(
            "crosses-alias-boundary",
            "synoptic",
            "SINT",
            40.750,
            -73.980,
            semantics=MeasurementSemantics.INTERVAL_ACCUMULATION,
            start=NOW - 20.0,
            end=NOW,
            amount=0.1,
            unit="mm",
        )
        return _batch(
            registry,
            facts=(_adapted(0, observation, registry, 1),),
        )

    _install_batch(monkeypatch, factory)
    result = _runtime(tmp_path, catalog=catalog).process(
        _request(),
        deadline_at=_deadline(),
    )

    item = result.receipt.accounting.dispositions[0]
    assert result.receipt.candidate_state is GroundConfirmationState.INDETERMINATE
    assert item.state is GroundFactDispositionState.WITHHELD
    assert item.certificate_fingerprint == _certificates()[1].fingerprint
    assert item.mapping_fingerprint is not None
    assert item.measurement_semantics == "interval_accumulation"
    assert item.measurement_amount == pytest.approx(0.1)
    assert item.measurement_unit == "mm"
    assert (
        "physical_site_mapping_inactive_for_observation_interval"
        in item.reason_codes
    )


def test_receipt_and_diagnostics_do_not_persist_secret_target_station_or_coordinates(
    tmp_path,
    monkeypatch,
):
    def factory(registry):
        return _batch(
            registry,
            issues=(
                _bridge_issue(
                    0,
                    "netatmo",
                    "70:ee:50:29:59:56",
                    "netatmo_semantics_uncertified",
                ),
            ),
        )

    _install_batch(monkeypatch, factory)
    runtime = _runtime(tmp_path)
    result = runtime.process(_request(), deadline_at=_deadline())
    database_bytes = (tmp_path / "ground-shadow.sqlite3").read_bytes()
    serialized = repr(result.receipt.to_mapping()).encode()

    for forbidden in (
        b"super-secret-token",
        b"device_tracker.private_person",
        b"70:ee:50:29:59:56",
        b"40.75",
        b"-73.98",
    ):
        assert forbidden not in database_bytes
        assert forbidden not in serialized
    diagnostics = runtime.receipt_store.diagnostics()
    assert set(diagnostics) == {
        "admission_status",
        "archive_anchor_sha256",
        "archive_bytes",
        "archive_segment_count",
        "archived_receipt_count",
        "candidate_states",
        "database_bytes",
        "last_archived_sequence",
        "max_database_bytes",
        "max_receipts",
        "next_sequence",
        "operational_confirmation_claim",
        "receipt_count",
    }
    assert diagnostics["archive_segment_count"] == 0
    assert diagnostics["archived_receipt_count"] == 0


def test_real_celltrack_latest_detection_binding_is_exact_and_lineage_complete():
    detection = CellDetection(
        detection_id="detection-1",
        valid_at=NOW,
        analysis_grid_id="analysis-grid-1",
        x_m=1000.0,
        y_m=2000.0,
        position_sigma_m=500.0,
        source="shadow-radar",
    )
    track = start_cell_track(
        "track-1",
        detection,
        "rain_shield",
        CellTrackLifecyclePolicy(
            policy_id="test-policy",
            confirmation_observations=1,
            maximum_gap_frames=2,
        ),
        lineage_event_ids=("birth-track-1",),
    )
    hierarchical = HierarchicalCellDetection(
        detection=detection,
        object_kind="rain_shield",
        parent_detection_id=None,
        footprint_pixels=((0, 0),),
        area_m2=1_000_000.0,
        geometric_centroid_x_m=1000.0,
        geometric_centroid_y_m=2000.0,
        value_weighted_centroid_x_m=None,
        value_weighted_centroid_y_m=None,
        mean_value=2.0,
        maximum_value=2.0,
        threshold=1.0,
        centroid_method="geometric",
    )
    base = _binding()
    bound = bind_cell_track_footprint(
        track,
        hierarchical,
        _footprint(),
        engine_id=base.engine_id,
        engine_state_revision=1,
        science_run_id=base.science_run_id,
        science_transition_id=base.science_transition_id,
        science_receipt_sha256=base.science_receipt_sha256,
        source_frame_id=base.source_frame_id,
        target_grid_sha256=base.target_grid_sha256,
        radar_semantics_certificate_sha256=(
            base.radar_semantics_certificate_sha256
        ),
        transform_certificate_sha256=base.transform_certificate_sha256,
        applied_lineage_hypothesis_ids=(),
        footprint_proof=base.footprint_proof,
    )

    assert bound.track_revision == track.revision
    assert bound.lineage_event_ids == ("birth-track-1",)
    assert bound.footprint.cell_detection_id == track.latest_detection.detection_id
    with pytest.raises(ValueError, match="exact CellTrack"):
        bind_cell_track_footprint(
            track,
            hierarchical,
            replace(_footprint(), representative_at=NOW + 1.0),
            engine_id=base.engine_id,
            engine_state_revision=1,
            science_run_id=base.science_run_id,
            science_transition_id=base.science_transition_id,
            science_receipt_sha256=base.science_receipt_sha256,
            source_frame_id=base.source_frame_id,
            target_grid_sha256=base.target_grid_sha256,
            radar_semantics_certificate_sha256=(
                base.radar_semantics_certificate_sha256
            ),
            transform_certificate_sha256=base.transform_certificate_sha256,
            applied_lineage_hypothesis_ids=(),
            footprint_proof=base.footprint_proof,
        )


def test_runtime_reads_real_raw_reverified_canonical_store_without_bypass(
    tmp_path,
):
    from tests.test_worker_canonical_ground_evidence_store import (
        _request as canonical_request,
        _store as canonical_store,
    )
    from storm_tracker_worker.native_capture import (
        NativeCaptureMemberInput,
        capture_native_set,
    )
    from storm_tracker_worker.raw_artifacts import RawArtifactStore

    raw_store = RawArtifactStore((tmp_path / "raw-ground").resolve())
    raw_store.initialize()
    capture = capture_native_set(
        raw_store,
        provider="synoptic",
        capture_product="synoptic_timeseries_query_capture",
        fetch_id="ground:real-runtime",
        upstream_capture_id="ground/capture/real-runtime",
        product_timestamp=NOW,
        collector="ground-shadow-integration-test",
        collector_version="1",
        members=(
            NativeCaptureMemberInput(
                role="station_metadata_json",
                product="synoptic_station_metadata",
                content_type="application/json",
                received_at=NOW + 1.0,
                upstream_id="ground/stations.json",
                payload=b'{"station":"SBBB"}',
                provenance={"semantic_class": "station_metadata"},
            ),
            NativeCaptureMemberInput(
                role="observations_json",
                product="synoptic_station_observations",
                content_type="application/json",
                received_at=NOW + 2.0,
                upstream_id="ground/observations.json",
                payload=b'{"station":"SBBB","rain":"0.40"}',
                provenance={"semantic_class": "station_observations"},
            ),
        ),
    ).capture
    variable = (
        "synoptic.native.precip_intervals_set_1d."
        "between_consecutive_reports"
    )
    request_document = canonical_request(
        capture,
        decoder={
            "identity": "synoptic_timeseries_ground_evidence",
            "version": "synoptic-ground-evidence-shadow-v1",
            "config_sha256": CONFIG,
        },
        station={
            "provider_station_id": "SBBB",
            "site_id": "synoptic:SBBB",
            "network": "SYNOPTIC",
            "station_name": None,
            "position_state": "known",
            "latitude_decimal": "40.755",
            "longitude_decimal": "-73.975",
            "elevation_state": "unknown",
            "elevation_m_decimal": None,
        },
        time={
            "observed_at": NOW,
            "representative_interval_start": NOW - 60.0,
            "representative_interval_end": NOW,
            "received_at": NOW + 2.0,
            "ingested_at": NOW + 3.0,
        },
        measurement={
            "variable": variable,
            "state": "observed",
            "representation": "decimal",
            "exact_value": "0.40",
            "unit": "Millimeters",
            "censor_limit": None,
        },
        provider_qc=[
            {
                "flag": "synoptic.provider.provisional",
                "value": "true",
                "text": None,
            },
            {
                "flag": "synoptic.sensor_set",
                "value": "precip_intervals_set_1d",
                "text": None,
            },
            {
                "flag": "synoptic.decoder.temporal_semantics",
                "value": "between_consecutive_provider_observations",
                "text": None,
            },
            {
                "flag": "synoptic.qc_flagged",
                "value": "false",
                "text": None,
            },
        ],
    )
    evidence_store = canonical_store(
        tmp_path / "canonical-ground.sqlite3",
        raw_store,
    )
    committed = evidence_store.commit(request_document)
    certificate = GroundSemanticsCertificate(
        certificate_id="test:real-synoptic-interval",
        certificate_version="1",
        provider="synoptic",
        product="synoptic_timeseries_query_capture",
        decoder_identity="synoptic_timeseries_ground_evidence",
        decoder_version="synoptic-ground-evidence-shadow-v1",
        config_sha256=CONFIG,
        profile=GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1,
        primary_sources=("https://example.test/synoptic-contract",),
        review_evidence_sha256=REVIEW,
    )
    runtime = DurableGroundFusionShadowRuntime(
        evidence_store,
        site_catalog=_default_catalog(),
        semantics_certificates=(certificate,),
        fusion_policy=_policy(),
        receipt_path=tmp_path / "real-ground-shadow.sqlite3",
    )

    result = runtime.process(_request(), deadline_at=_deadline())

    assert result.receipt.accounting.source_fact_count == 1
    disposition = result.receipt.accounting.dispositions[0]
    assert disposition.state is GroundFactDispositionState.USED
    assert disposition.fact_id == committed.fact_id
    assert disposition.fact_sha256 == committed.fact_sha256
    assert disposition.receipt_sha256 == committed.receipt_sha256
    assert disposition.measurement_semantics == "interval_accumulation"
    assert disposition.measurement_amount == pytest.approx(0.4)
    assert disposition.measurement_unit == "mm"


def test_fresh_science_shadow_result_binds_exact_track_and_restart_fails_closed(
    tmp_path,
):
    # Reuse the science orchestrator's real typed fixture rather than a mock
    # result so this test detects receipt/transition contract drift.
    from tests.test_worker_science_shadow_orchestrator import (
        FakeComposer,
        _detector,
        _evidence as science_evidence,
        _frame as science_frame,
        _orchestrator as science_orchestrator,
        _policy as science_policy,
        _run as science_run,
    )

    detector = _detector()
    run_id = science_run(501)
    evidence = science_evidence(
        run_id,
        science_frame(run_id, NOW, ((1, 2, 2.0),)),
        detector,
    )
    orchestrator = science_orchestrator(
        tmp_path,
        FakeComposer(detector, {run_id: evidence}),
        science_policy(detector),
    )
    result = orchestrator.process(
        attempt_id="ground-binder-science-attempt",
        run_id=run_id,
        deadline_at=20.0,
    )
    assert result.transition is not None
    assert result.receipt.provenance is not None
    track = result.transition.next_state.ledger.tracks[0]
    footprint = CellTrackFootprint(
        footprint_id="science-footprint-501",
        cell_track_id=track.track_id,
        cell_detection_id=track.latest_detection.detection_id,
        valid_from=NOW - 60.0,
        valid_until=NOW + 60.0,
        representative_at=track.latest_detection.valid_at,
        rings=_footprint().rings,
        radar_source="science-shadow-certified-geographic-proof",
        raw_artifact_receipt_id=(
            result.receipt.provenance.capture_receipt_sha256
        ),
    )
    proof = GeographicFootprintProof(
        proof_id="science-geographic-proof-501",
        proof_version="1",
        source_science_receipt_sha256=result.receipt.receipt_sha256,
        source_target_grid_sha256=(
            result.receipt.provenance.target_grid_sha256
        ),
        source_transform_receipt_sha256=(
            result.receipt.provenance.transform_receipt_sha256
        ),
        geometry_builder_id="certified-grid-cell-contour",
        geometry_certificate_sha256="4" * 64,
        footprint_geometry_sha256=footprint_geometry_sha256(footprint),
    )

    bound = bind_science_shadow_footprint(
        result,
        footprint,
        footprint_proof=proof,
    )

    assert bound.science_receipt_sha256 == result.receipt.receipt_sha256
    assert bound.science_transition_id == result.transition.transition_id
    assert len(bound.fingerprint) == 64
    assert bound.footprint.cell_track_id == track.track_id
    assert bound.lineage_event_ids == track.lineage_event_ids
    assert (
        bound.applied_lineage_hypothesis_ids
        == result.receipt.applied_lineage_hypothesis_ids
    )

    replay = orchestrator.process(
        attempt_id="ground-binder-science-attempt",
        run_id=run_id,
        deadline_at=20.0,
    )
    assert replay.duplicate is True
    with pytest.raises(ValueError, match="fresh advanced science"):
        bind_science_shadow_footprint(
            replay,
            footprint,
            footprint_proof=proof,
        )


def test_module_imports_no_home_assistant_or_worker_service():
    source = (runtime_module.Path(runtime_module.__file__)).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)
    assert not any(name.startswith("homeassistant") for name in imported)
    assert not any(name.startswith("custom_components") for name in imported)
    assert "storm_tracker_worker.service" not in imported
    assert not any(name.endswith(".service") for name in imported)
