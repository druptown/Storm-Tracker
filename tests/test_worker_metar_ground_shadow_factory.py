"""Closed-world proofs for the real-cache METAR ground shadow factory."""
from __future__ import annotations

import ast
import copy
import hashlib
import json
from pathlib import Path
import time

import pytest

from storm_tracker_worker import metar_ground_shadow_factory as factory_module
from storm_tracker_worker.ground_fusion_shadow_process_isolation import (
    GroundShadowIsolatedRunner,
    GroundShadowIsolationLimits,
)
from storm_tracker_worker.ground_fusion_shadow_runtime import (
    GroundFactDispositionState,
)
from storm_tracker_worker.hard_decoder_isolation import _canonical_json_bytes
from storm_tracker_worker.metar_ground_shadow_factory import (
    METAR_CLOSED_WORLD_FACTORY_ID,
    MetarGroundFactoryError,
    _build_runtime,
    runtime_profile_for_authority,
)
from tests.helpers.metar_ground_closed_world_authority import (
    PRIVATE_TARGET,
    build_closed_world_authority,
    fixture_request,
)
from storm_tracker_worker.tracking_core.engine.canonical_ground_fusion_bridge import (
    GroundSemanticsProfile,
)
from storm_tracker_worker.tracking_core.engine.physical_site_catalog import (
    PhysicalSiteCatalog,
    PhysicalSiteRecord,
    SensorCapability,
    VerificationEvidence,
    VerifiedStationAlias,
)


ROOT = Path(__file__).parents[1]
REAL_CACHE = ROOT / "metars_current.csv.gz"
REAL_CACHE_SHA256 = "959f4c8ba1d91aef2b7b4a3e5dbdc6c92ba2a98530acd0d4af8b0f80751f9ebf"
REAL_CACHE_FACT_COUNT = 10_294
PRIVATE_STATION = "PAKU"


@pytest.fixture(scope="module")
def metar_bundle(tmp_path_factory):
    if not REAL_CACHE.is_file():
        pytest.skip("exact preserved AWC cache is unavailable")
    requests = tuple(fixture_request(ordinal=index) for index in range(1, 13))
    return build_closed_world_authority(
        tmp_path_factory.mktemp("metar-ground-closed-world"),
        cache_path=REAL_CACHE,
        requests=requests,
    )


def _rewrite_authority(bundle, changes: dict[str, object], name: str):
    document = copy.deepcopy(bundle.document)
    document.update(changes)
    body = _canonical_json_bytes(document)
    path = (bundle.root / name).resolve()
    path.write_bytes(body)
    return document, path, hashlib.sha256(body).hexdigest()


def test_exact_real_cache_reaches_bridge_and_every_fact_is_accounted_unadmitted(
    metar_bundle,
):
    assert metar_bundle.fixture_sha256 == REAL_CACHE_SHA256
    assert metar_bundle.decode_fact_count == REAL_CACHE_FACT_COUNT
    runtime = _build_runtime(metar_bundle.document, enforce_audit=False)
    result = runtime.process(
        fixture_request(ordinal=1),
        deadline_at=time.monotonic() + 15.0,
    )
    receipt = result.receipt

    assert receipt.candidate_state.value == "indeterminate"
    assert receipt.candidate_confirmed is False
    assert receipt.accounting.query_executed
    assert receipt.accounting.full_scan_complete
    assert receipt.accounting.overflowed is False
    assert receipt.accounting.source_fact_count == 56
    assert len(receipt.accounting.dispositions) == 56
    assert all(
        item.state is GroundFactDispositionState.WITHHELD
        for item in receipt.accounting.dispositions
    )
    reasons = {
        reason
        for item in receipt.accounting.dispositions
        for reason in item.reason_codes
    }
    assert "physical_site_alias_unverified" in reasons
    assert "provider_variable_semantics_uncertified" in reasons
    assert result.decision is not None
    assert result.decision.confirmed is False
    assert metar_bundle.document["factory_config"]["operational_admission_granted"] is False


def test_exact_target_cell_and_footprint_binding_rejects_any_substitution(
    metar_bundle,
):
    runtime = _build_runtime(metar_bundle.document, enforce_audit=False)
    receipt_count_before = metar_bundle.receipt_store().diagnostics()[
        "receipt_count"
    ]
    request = fixture_request(ordinal=2)
    replacement = fixture_request(ordinal=3)
    substituted = type(request)(
        attempt_id=request.attempt_id,
        decision_key=request.decision_key,
        region_key=request.region_key,
        target=replacement.target,
        cell=request.cell,
        evaluated_at=request.evaluated_at,
    )

    with pytest.raises(ValueError, match="target/cell/footprint authority"):
        runtime.process(substituted, deadline_at=time.monotonic() + 5.0)
    assert (
        metar_bundle.receipt_store().diagnostics()["receipt_count"]
        == receipt_count_before
    )


def test_raw_tree_extra_file_and_object_tamper_fail_before_runtime(
    metar_bundle,
):
    extra_directory = metar_bundle.raw_root / "not-in-sealed-manifest"
    extra_directory.mkdir()
    try:
        with pytest.raises(MetarGroundFactoryError, match="raw tree"):
            runtime_profile_for_authority(metar_bundle.document)
    finally:
        extra_directory.rmdir()

    extra = metar_bundle.raw_root / "not-in-sealed-manifest.bin"
    extra.write_bytes(b"not-authorised")
    try:
        with pytest.raises(MetarGroundFactoryError, match="raw tree"):
            runtime_profile_for_authority(metar_bundle.document)
    finally:
        extra.unlink()

    manifest = json.loads(metar_bundle.raw_manifest_path.read_text("ascii"))
    object_entry = next(
        item for item in manifest["entries"]
        if item["size_bytes"] == REAL_CACHE.stat().st_size
    )
    object_path = metar_bundle.raw_root / object_entry["relative_path"]
    original = object_path.read_bytes()
    object_path.write_bytes(original[:-1] + bytes((original[-1] ^ 1,)))
    try:
        with pytest.raises(MetarGroundFactoryError, match="identity differs"):
            runtime_profile_for_authority(metar_bundle.document)
    finally:
        object_path.write_bytes(original)


@pytest.mark.parametrize("role", ("site", "semantics", "footprint"))
def test_every_immutable_authority_document_tamper_fails_closed(
    metar_bundle,
    role,
):
    path = {
        "site": metar_bundle.site_path,
        "semantics": metar_bundle.semantics_path,
        "footprint": metar_bundle.footprint_path,
    }[role]
    original = path.read_bytes()
    path.write_bytes(original + b"\n")
    try:
        with pytest.raises(MetarGroundFactoryError, match="checksum differs"):
            runtime_profile_for_authority(metar_bundle.document)
    finally:
        path.write_bytes(original)


def test_canonical_database_content_and_authority_scope_are_both_immutable(
    metar_bundle,
):
    original_size = metar_bundle.canonical_path.stat().st_size
    with metar_bundle.canonical_path.open("ab") as stream:
        stream.write(b"tamper")
    try:
        with pytest.raises(MetarGroundFactoryError, match="canonical ground content"):
            runtime_profile_for_authority(metar_bundle.document)
    finally:
        with metar_bundle.canonical_path.open("r+b") as stream:
            stream.truncate(original_size)

    changed = copy.deepcopy(metar_bundle.document)
    changed["factory_config"]["authority_scope_sha256"] = "f" * 64
    with pytest.raises(MetarGroundFactoryError, match="authority scope"):
        runtime_profile_for_authority(changed)


def test_audit_guard_denies_network_and_unowned_paths_without_path_echo(
    metar_bundle,
):
    guard = factory_module._ClosedWorldAuditGuard(
        exact_files=(metar_bundle.site_path,),
        raw_root=metar_bundle.raw_root,
        receipt_path=metar_bundle.receipt_path,
        archive_root=metar_bundle.archive_path,
        directory_paths=(metar_bundle.raw_root, metar_bundle.archive_path),
        mutable_sidecars=(),
    )
    guard.admit_raw_files(())
    with pytest.raises(PermissionError) as network:
        guard("socket.connect", ())
    with pytest.raises(PermissionError) as path:
        guard("open", (ROOT / "not-authorised", "r", 0))
    assert str(ROOT) not in str(network.value)
    assert str(ROOT) not in str(path.value)
    assert guard.network_attempts == guard.denied_accesses == 1


def test_nonempty_site_and_capability_schema_round_trips_only_as_structure():
    """Structural parser proof; it is not used by the real-cache authority."""

    evidence = VerificationEvidence(
        authority="fixture-only-structure-authority",
        reference="https://example.invalid/not-operational-evidence",
        verified_at=1_700_000_000.0,
        content_sha256="a" * 64,
    )
    alias = VerifiedStationAlias(
        provider_id="metar",
        station_id="STRUCTURE-ONLY",
        mapping_evidence=evidence,
    )
    record = PhysicalSiteRecord(
        site_key="structure-only-site",
        latitude=1.0,
        longitude=2.0,
        coordinate_tolerance_m=100.0,
        independence_group_key="structure-only-group",
        site_evidence=evidence,
        independence_evidence=evidence,
        aliases=(alias,),
        capabilities=(SensorCapability(
            provider_id="metar",
            sensor_id=alias.station_id,
            measurement_kind=(
                GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1.value
            ),
            can_observe_absence=False,
            evidence=evidence,
        ),),
    )
    catalog = PhysicalSiteCatalog(
        catalog_name="structure-only-catalog",
        catalog_version="fixture-only-unadmitted",
        records=(record,),
        proximity_audit_radius_m=1_000.0,
    )
    catalog_mapping = json.loads(catalog.canonical_json)
    admission = {
        "granted": False,
        "missing": ["real_authoritative_site_evidence"],
        "status": "candidate_only_unadmitted",
    }
    assertions = {
        "admission": admission,
        "catalog_name": catalog.catalog_name,
        "proximity_audit_radius_m": catalog.proximity_audit_radius_m,
        "records": catalog_mapping["records"],
    }
    scope = "b" * 64
    document = {
        "admission": admission,
        "authority_scope_sha256": scope,
        "catalog": catalog_mapping,
        "contract": factory_module.METAR_SITE_AUTHORITY_CONTRACT,
        "site_assertions_sha256": factory_module._digest(assertions),
    }

    loaded = factory_module._site_catalog(document, scope)
    assert loaded.fingerprint == catalog.fingerprint
    assert loaded.records[0].capabilities[0].can_observe_absence is False


def test_real_spawn_restart_receipt_and_privacy_are_exact(metar_bundle):
    request = fixture_request(ordinal=4)
    first = metar_bundle.runner().execute(request)
    restart = metar_bundle.runner().execute(request)

    assert first.status == "success", first
    assert first.execution_outcome == "succeeded"
    assert first.committed and first.duplicate is False
    assert restart.status == "success"
    assert restart.execution_outcome == "durable_duplicate"
    assert restart.duplicate is True
    assert restart.receipt_metadata == first.receipt_metadata
    assert first.receipt_metadata["candidate_state"] == "indeterminate"
    assert first.receipt_metadata["candidate_confirmed"] is False
    assert first.receipt_metadata["meteorological_admission_granted"] is False

    diagnostics = json.dumps(first.to_mapping(), sort_keys=True)
    assert str(metar_bundle.root) not in diagnostics
    assert PRIVATE_TARGET not in diagnostics
    assert PRIVATE_STATION not in diagnostics
    assert "70.331" not in diagnostics and "-149.613" not in diagnostics
    assert "token" not in diagnostics.casefold()
    assert "password" not in diagnostics.casefold()


def test_archived_retry_is_exact_and_never_recomputes(metar_bundle):
    first_request = fixture_request(ordinal=5)
    second_request = fixture_request(ordinal=6)
    first = metar_bundle.runner().execute(first_request)
    second = metar_bundle.runner().execute(second_request)
    assert first.committed and second.committed

    rotation = metar_bundle.receipt_store().rotate_archive(
        metar_bundle.archive_path,
        policy=metar_bundle.retention,
        deadline_at=time.monotonic() + 10.0,
        directory_fsync=lambda _path: None,
    )
    assert rotation.archived_receipts >= 1
    archived = metar_bundle.runner().execute(first_request)
    assert archived.status == "success"
    assert archived.execution_outcome == "durable_duplicate"
    assert archived.duplicate is True
    assert archived.receipt_metadata == first.receipt_metadata


def test_authority_tamper_deadline_and_result_capacity_are_explicit(
    metar_bundle,
):
    stale = metar_bundle.authority_path.read_bytes() + b"\n"
    stale_path = (metar_bundle.root / "stale-authority.json").resolve()
    stale_path.write_bytes(stale)
    stale_runner = GroundShadowIsolatedRunner(
        authority_path=stale_path,
        expected_authority_sha256=metar_bundle.authority_sha256,
        factory_contract=metar_bundle.factory,
        limits=metar_bundle.isolation_limits,
    )
    stale_result = stale_runner.execute(fixture_request(ordinal=7))
    assert stale_result.status == "corrupt_contract"
    assert stale_result.execution_outcome == "authority_rejected"

    deadline_limits = GroundShadowIsolationLimits(
        wall_seconds=1.0,
        cpu_seconds=metar_bundle.isolation_limits.cpu_seconds,
        rss_bytes=metar_bundle.isolation_limits.rss_bytes,
        max_authority_bytes=metar_bundle.isolation_limits.max_authority_bytes,
        max_request_bytes=metar_bundle.isolation_limits.max_request_bytes,
        max_result_bytes=metar_bundle.isolation_limits.max_result_bytes,
        max_authorized_paths=metar_bundle.isolation_limits.max_authorized_paths,
    )
    deadline_document = copy.deepcopy(metar_bundle.document)
    deadline_document["isolation_limits"] = deadline_limits.to_mapping()
    deadline_body = _canonical_json_bytes(deadline_document)
    deadline_path = (metar_bundle.root / "deadline-authority.json").resolve()
    deadline_path.write_bytes(deadline_body)
    deadline_runner = GroundShadowIsolatedRunner(
        authority_path=deadline_path,
        expected_authority_sha256=hashlib.sha256(deadline_body).hexdigest(),
        factory_contract=metar_bundle.factory,
        limits=deadline_limits,
    )
    deadline = deadline_runner.execute(fixture_request(ordinal=8))
    assert deadline.status == "deadline"
    assert deadline.execution_outcome == "deadline_exceeded"
    assert not deadline.committed

    output_limits = GroundShadowIsolationLimits(
        wall_seconds=metar_bundle.isolation_limits.wall_seconds,
        cpu_seconds=metar_bundle.isolation_limits.cpu_seconds,
        rss_bytes=metar_bundle.isolation_limits.rss_bytes,
        max_authority_bytes=metar_bundle.isolation_limits.max_authority_bytes,
        max_request_bytes=metar_bundle.isolation_limits.max_request_bytes,
        max_result_bytes=1024,
        max_authorized_paths=metar_bundle.isolation_limits.max_authorized_paths,
    )
    output_document = copy.deepcopy(metar_bundle.document)
    output_document["isolation_limits"] = output_limits.to_mapping()
    output_body = _canonical_json_bytes(output_document)
    output_path = (metar_bundle.root / "output-authority.json").resolve()
    output_path.write_bytes(output_body)
    output_runner = GroundShadowIsolatedRunner(
        authority_path=output_path,
        expected_authority_sha256=hashlib.sha256(output_body).hexdigest(),
        factory_contract=metar_bundle.factory,
        limits=output_limits,
    )
    output = output_runner.execute(fixture_request(ordinal=9))
    assert output.status == "resource"
    assert output.execution_outcome == "result_too_large"
    assert output.committed


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text("utf-8"), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def test_factory_is_shadow_only_and_has_no_live_or_secret_ingress():
    module_path = ROOT / "storm_tracker_worker" / "metar_ground_shadow_factory.py"
    candidate = "storm_tracker_worker.metar_ground_shadow_factory"
    assert METAR_CLOSED_WORLD_FACTORY_ID in module_path.read_text("utf-8")
    assert candidate not in _imports(ROOT / "storm_tracker_worker" / "service.py")
    for path in (ROOT / "custom_components" / "storm_tracker_v3").rglob("*.py"):
        assert candidate not in _imports(path)
    imports = _imports(module_path)
    assert not any(
        name.startswith("homeassistant")
        or name.startswith("custom_components")
        or name == "storm_tracker_worker.service"
        or name in {"requests", "urllib", "socket", "subprocess"}
        for name in imports
    )
    source = module_path.read_text("utf-8")
    assert "os.environ" not in source
    assert "os.getenv" not in source
    assert "pickle" not in source
