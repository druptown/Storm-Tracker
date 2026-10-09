"""Contract tests for the shadow-only provider raw-artifact specification."""
from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
HA_MODULE = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "engine"
    / "provider_artifact_contracts.py"
)
WORKER_MODULE = (
    REPO_ROOT
    / "storm_tracker_worker"
    / "tracking_core"
    / "engine"
    / "provider_artifact_contracts.py"
)
HA_RETENTION_PLANNER = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "engine"
    / "artifact_retention_planner.py"
)
WORKER_RETENTION_PLANNER = (
    REPO_ROOT
    / "storm_tracker_worker"
    / "tracking_core"
    / "engine"
    / "artifact_retention_planner.py"
)
HA_CAPTURE_ADMISSION = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "engine"
    / "artifact_capture_admission.py"
)
WORKER_CAPTURE_ADMISSION = (
    REPO_ROOT
    / "storm_tracker_worker"
    / "tracking_core"
    / "engine"
    / "artifact_capture_admission.py"
)
PROVIDER_REGISTRY = (
    REPO_ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "providers"
    / "registry.py"
)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def contracts():
    return _load("stv3_test_provider_artifact_contracts", HA_MODULE)


@pytest.fixture(scope="module")
def worker_contracts():
    return _load("stv3_test_worker_provider_artifact_contracts", WORKER_MODULE)


@pytest.fixture(scope="module")
def provider_registry():
    return _load("stv3_test_provider_registry", PROVIDER_REGISTRY)


def test_contract_registry_exactly_covers_implemented_sources(
    contracts, provider_registry
):
    implemented = (
        provider_registry.IMPLEMENTED_RADAR_PROVIDERS
        | provider_registry.IMPLEMENTED_GROUND_PROVIDERS
    )
    assert len(provider_registry.IMPLEMENTED_RADAR_PROVIDERS) == 12
    assert len(provider_registry.IMPLEMENTED_GROUND_PROVIDERS) == 3
    assert set(contracts.CONTRACTS) == implemented
    contracts.validate_registry_complete(implemented)

    with pytest.raises(ValueError, match="missing=.*future_source"):
        contracts.validate_registry_complete(implemented | {"future_source"})
    with pytest.raises(ValueError, match="extra=.*kmi"):
        contracts.validate_registry_complete(implemented - {"kmi"})


def test_every_contract_is_complete_explicit_and_serialisable(contracts):
    required_text_fields = (
        "registry_role",
        "product",
        "quantity",
        "upstream_authority",
        "authoritative_artifact",
        "capture_boundary",
        "record_id_strategy",
        "parser_responsibility",
        "parser_version",
        "timestamp_policy",
        "transport_gap",
    )
    fact_fields = (
        "nominal_timestamp_source",
        "native_crs",
        "unit_semantics",
        "gain_offset_semantics",
        "nodata_semantics",
        "undetect_semantics",
        "qc_semantics",
        "native_resolution",
        "native_dimensions",
        "license_terms",
        "attribution",
    )
    for provider, contract in contracts.CONTRACTS.items():
        assert contract.provider == provider
        for field in required_text_fields:
            assert getattr(contract, field).strip(), (provider, field)
        assert contract.record_id_components
        assert contract.allowed_media_types
        assert contract.primary_sources
        for field in fact_fields:
            fact = getattr(contract, field)
            assert isinstance(fact, contracts.Fact)
            if fact.kind is contracts.FactKind.UNKNOWN:
                assert fact.value is None
                assert fact.note.strip()
            else:
                assert fact.value and fact.value.strip()

        encoded = json.dumps(
            contract.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        assert json.loads(encoded)["provider"] == provider
        assert json.loads(encoded)["shadow_only"] is True


def test_unknown_and_artifact_metadata_cannot_masquerade_as_values(contracts):
    with pytest.raises(ValueError, match="UNKNOWN"):
        contracts.Fact(contracts.FactKind.UNKNOWN, "EPSG:4326", "invented")
    with pytest.raises(ValueError, match="requires a value"):
        contracts.Fact(contracts.FactKind.ARTIFACT_METADATA, None, "")

    assert contracts.CONTRACTS["kmi"].native_crs.kind is contracts.FactKind.UNKNOWN
    assert (
        contracts.CONTRACTS["opera"].native_crs.kind
        is contracts.FactKind.ARTIFACT_METADATA
    )
    assert (
        contracts.CONTRACTS["noaa_enterprise_rainrate"].native_resolution.kind
        is contracts.FactKind.ARTIFACT_METADATA
    )
    assert "meteolux" not in contracts.CONTRACTS


def test_aemet_contract_rejects_legacy_byte_classes_as_quantitative_truth(
    contracts,
):
    aemet = contracts.CONTRACTS["aemet_radar"]

    assert "reflectivity intervals in dBZ" in aemet.quantity
    assert "not legacy byte classes 1..8" in aemet.quantity
    assert aemet.unit_semantics.kind is contracts.FactKind.ARTIFACT_METADATA
    assert "ESCALA" in aemet.unit_semantics.value
    assert aemet.gain_offset_semantics.kind is contracts.FactKind.UNKNOWN
    assert aemet.nodata_semantics.kind is contracts.FactKind.UNKNOWN
    assert aemet.undetect_semantics.kind is contracts.FactKind.UNKNOWN
    assert aemet.qc_semantics.kind is contracts.FactKind.UNKNOWN
    assert aemet.nominal_timestamp_source.kind is contracts.FactKind.UNKNOWN
    assert aemet.raw_replay is contracts.RawReplay.CONDITIONAL
    assert (
        aemet.retention_class
        is contracts.RetentionClass.ATTRIBUTION_REPLAY_EVIDENCE
    )
    assert aemet.license_terms.kind is contracts.FactKind.FIXED
    assert "three-member OpenData capture" in aemet.transport_gap
    assert len(aemet.primary_sources) == 3


def test_meteofrance_contract_retains_accumulation_without_inventing_rate(
    contracts,
):
    product = contracts.CONTRACTS["meteofrance_radar"]

    assert "five-minute precipitation accumulation" in product.quantity
    assert "not an instantaneous rain rate" in product.quantity
    assert product.native_crs.kind is contracts.FactKind.UNKNOWN
    assert product.unit_semantics.kind is contracts.FactKind.FIXED
    assert "0.01 mm" in product.unit_semantics.value
    assert product.gain_offset_semantics.kind is contracts.FactKind.UNKNOWN
    assert product.nodata_semantics.kind is contracts.FactKind.UNKNOWN
    assert product.undetect_semantics.kind is contracts.FactKind.UNKNOWN
    assert product.qc_semantics.kind is contracts.FactKind.UNKNOWN
    assert product.native_resolution.value == "500 m square mesh"
    assert product.native_dimensions.value == "3472 x 3472"
    assert product.license_terms.kind is contracts.FactKind.FIXED
    assert (
        product.retention_class
        is contracts.RetentionClass.ATTRIBUTION_REPLAY_EVIDENCE
    )
    assert len(product.primary_sources) == 3


def test_radar_registry_contains_only_precipitation_raster_implementations(
    contracts, provider_registry
):
    assert {
        provider
        for provider in provider_registry.IMPLEMENTED_RADAR_PROVIDERS
        if (
            contracts.CONTRACTS[provider].capability_status
            is not contracts.CapabilityStatus.PRECIPITATION_RASTER
        )
    } == set()
    assert all(
        contracts.CONTRACTS[provider].capability_status
        is contracts.CapabilityStatus.GROUND_OBSERVATIONS
        for provider in provider_registry.IMPLEMENTED_GROUND_PROVIDERS
    )
    assert all(
        contract.operational_eligible
        for contract in contracts.CONTRACTS.values()
    )


def test_capability_mismatch_is_intrinsically_fail_closed(contracts):
    radar = contracts.CONTRACTS["kmi"]
    with pytest.raises(ValueError, match="cannot be raw-replayable"):
        replace(
            radar,
            capability_status=contracts.CapabilityStatus.CAPABILITY_MISMATCH,
        )
    mismatch = replace(
        radar,
        capability_status=contracts.CapabilityStatus.CAPABILITY_MISMATCH,
        raw_replay=contracts.RawReplay.NOT_CURRENTLY_POSSIBLE,
    )
    assert mismatch.operational_eligible is False
    assert mismatch.raw_replay is contracts.RawReplay.NOT_CURRENTLY_POSSIBLE


def test_policy_model_guidance_is_never_advertised_as_operational_radar(
    contracts, provider_registry
):
    policy = json.loads(
        (REPO_ROOT / "custom_components" / "storm_tracker_v3" / "provider_policy.json")
        .read_text(encoding="utf-8")
    )
    sections = [policy["default"], *policy["countries"].values()]
    for section in sections:
        radar = set(section.get("radar", ()))
        ground = set(section.get("ground_validation", ()))
        guidance = set(section.get("model_guidance", ()))
        assert radar.isdisjoint(guidance)
        assert ground.isdisjoint(guidance)

    assert "meteolux" in policy["countries"]["LU"]["model_guidance"]
    assert all("meteolux" not in section.get("radar", ()) for section in sections)
    assert all(
        "meteolux" not in section.get("ground_validation", ())
        for section in sections
    )
    assert "meteolux" not in provider_registry.IMPLEMENTED_RADAR_PROVIDERS
    assert "meteolux" not in provider_registry.IMPLEMENTED_GROUND_PROVIDERS
    assert "meteolux" not in contracts.CONTRACTS


def test_registry_snapshot_and_record_ids_are_deterministic(contracts):
    first = contracts.contract_snapshot()
    second = contracts.contract_snapshot()
    assert first == second
    assert list(first) == sorted(first)
    assert contracts.contract_registry_sha256() == hashlib.sha256(
        json.dumps(
            first,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    digest = contracts.payload_sha256(b"exact upstream bytes")
    record_id = contracts.derive_record_id(
        "opera",
        source_record_id="2026/07/30/OPERA/frame.h5",
        nominal_timestamp="2026-07-30T10:00:00Z",
        payload_digest=digest,
    )
    assert record_id == contracts.derive_record_id(
        "opera",
        source_record_id="2026/07/30/OPERA/frame.h5",
        nominal_timestamp="2026-07-30T10:00:00Z",
        payload_digest=digest,
    )
    assert record_id.startswith("opera:")
    assert len(record_id.split(":", 1)[1]) == 64
    assert "frame.h5" not in record_id


@pytest.mark.parametrize(
    "unsafe",
    (
        {"Authorization": "opaque"},
        {"token": "opaque"},
        {"nested": {"api_key": "opaque"}},
        {"header": "Bearer abcdefghijklmnopqrstuvwxyz"},
        {"header": "Basic Y2xpZW50OnNlY3JldA=="},
        {"jwt": "abcdefghijkl.mn0123456789op.qrstuvwxyz12"},
        {"url": "https://example.invalid/frame?X-Amz-Signature=opaque"},
        {"url": "https://app.meteo.invalid/frame?k=opaque"},
        {"url": "https://user:password@example.invalid/frame"},
        {"value": "access_token=opaque"},
    ),
)
def test_redaction_validator_rejects_credentials(contracts, unsafe):
    with pytest.raises(contracts.RedactionError):
        contracts.validate_redacted(unsafe)


def test_redaction_allows_plain_primary_url_but_record_ids_forbid_urls(contracts):
    contracts.validate_redacted(
        {"source": "https://example.invalid/data/frame.h5", "etag": "abc-123"}
    )
    with pytest.raises(contracts.RedactionError, match="full URL"):
        contracts.derive_record_id(
            "opera",
            source_record_id="https://example.invalid/frame.h5",
            payload_digest="0" * 64,
        )
    with pytest.raises(contracts.RedactionError, match="query"):
        contracts.derive_record_id(
            "kmi",
            source_record_id="frame.png?credential=opaque",
            payload_digest="0" * 64,
        )
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        contracts.derive_record_id("opera", payload_digest="not-a-digest")
    with pytest.raises(KeyError, match="unknown provider"):
        contracts.derive_record_id("not_registered", payload_digest="0" * 64)


def test_capture_inventory_matches_current_transport_boundaries(contracts):
    status = {
        provider: contract.raw_capture_status
        for provider, contract in contracts.CONTRACTS.items()
    }
    assert status["kmi"] is contracts.RawCaptureStatus.EXACT_BYTES_TRANSIENT
    assert status["rainviewer"] is contracts.RawCaptureStatus.MULTI_ARTIFACT_TRANSIENT
    assert (
        status["noaa_enterprise_rainrate"]
        is contracts.RawCaptureStatus.WORKER_DERIVED_ARTIFACT_ONLY
    )
    assert (
        status["opera"]
        is contracts.RawCaptureStatus.WORKER_DERIVED_ARTIFACT_ONLY
    )
    assert (
        status["dwd_radolan"]
        is contracts.RawCaptureStatus.WORKER_DERIVED_ARTIFACT_ONLY
    )
    assert (
        status["synoptic"]
        is contracts.RawCaptureStatus.EXACT_BYTES_TRANSIENT
    )
    assert (
        status["netatmo"]
        is contracts.RawCaptureStatus.EXACT_BYTES_TRANSIENT
    )
    assert status["metar"] is contracts.RawCaptureStatus.EXACT_BYTES_TRANSIENT
    assert all(
        contract.raw_capture_status
        is not getattr(contracts.RawCaptureStatus, "RAW_PERSISTED", None)
        for contract in contracts.CONTRACTS.values()
    )


def test_ha_and_worker_contract_modules_are_exact_byte_mirrors(
    contracts, worker_contracts
):
    ha_bytes = HA_MODULE.read_bytes()
    worker_bytes = WORKER_MODULE.read_bytes()
    assert ha_bytes == worker_bytes
    assert hashlib.sha256(ha_bytes).hexdigest() == hashlib.sha256(
        worker_bytes
    ).hexdigest()
    assert contracts.contract_snapshot() == worker_contracts.contract_snapshot()
    assert (
        contracts.contract_registry_sha256()
        == worker_contracts.contract_registry_sha256()
    )


def test_contract_module_is_inert_and_not_live_wired(contracts):
    assert contracts.SHADOW_ONLY is True
    tree = ast.parse(HA_MODULE.read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "").split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert imported_roots <= {
        "__future__",
        "dataclasses",
        "enum",
        "hashlib",
        "json",
        "re",
        "types",
        "typing",
        "urllib",
    }
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert not (called_names & {"open", "urlopen", "request", "get", "post"})

    excluded = {
        HA_MODULE.resolve(),
        WORKER_MODULE.resolve(),
        HA_RETENTION_PLANNER.resolve(),
        WORKER_RETENTION_PLANNER.resolve(),
        HA_CAPTURE_ADMISSION.resolve(),
        WORKER_CAPTURE_ADMISSION.resolve(),
        Path(__file__).resolve(),
    }
    def _imports_contract(candidate: Path) -> bool:
        try:
            candidate_tree = ast.parse(
                candidate.read_text(encoding="utf-8", errors="strict")
            )
        except (SyntaxError, UnicodeError):
            return False
        for node in ast.walk(candidate_tree):
            if isinstance(node, ast.Import) and any(
                alias.name.endswith("provider_artifact_contracts")
                for alias in node.names
            ):
                return True
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.endswith("provider_artifact_contracts"):
                    return True
                if any(
                    alias.name == "provider_artifact_contracts"
                    for alias in node.names
                ):
                    return True
            if not isinstance(node, ast.Call) or not node.args:
                continue
            function = node.func
            function_name = (
                function.id
                if isinstance(function, ast.Name)
                else function.attr
                if isinstance(function, ast.Attribute)
                else None
            )
            argument = node.args[0]
            if (
                function_name in {"__import__", "import_module"}
                and isinstance(argument, ast.Constant)
                and isinstance(argument.value, str)
                and argument.value.endswith("provider_artifact_contracts")
            ):
                return True
        return False

    consumers = []
    for path in (
        REPO_ROOT / "custom_components" / "storm_tracker_v3",
        REPO_ROOT / "storm_tracker_worker",
    ):
        for candidate in path.rglob("*.py"):
            if candidate.resolve() in excluded:
                continue
            if _imports_contract(candidate):
                consumers.append(candidate)
    assert consumers == []
