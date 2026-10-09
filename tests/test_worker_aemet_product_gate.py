from __future__ import annotations

import inspect
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from storm_tracker_worker import aemet_product_gate as gate


EXPECTED_BLOCKERS = {
    "rain_rate_interpretation",
    "exact_grid_geometry",
    "pixel_scale_mapping",
    "timestamp_timezone",
    "observation_support",
    "nodata_noecho_coverage",
    "pixel_qc_semantics",
    "legacy_transport_contract",
    "complete_artifact_set",
    "official_golden_artifact",
}


def test_contract_is_shadow_only_and_fail_closed_for_every_weather_use():
    contract = gate.AEMET_NATIONAL_RADAR_GATE

    assert contract.shadow_only is True
    assert set(contract.blocking_gaps) == EXPECTED_BLOCKERS
    assert contract.exact_native_capture_candidate_eligible is False
    assert contract.quantitative_decode_eligible is False
    assert contract.rain_rate_eligible is False
    assert contract.detection_eligible is False
    assert contract.forecast_eligible is False
    assert contract.operational_admission_eligible is False
    assert contract.to_dict()["weather_state"] == "unknown"


def test_verified_truth_is_reflectivity_not_precipitation_rate():
    by_key = {
        item.key: item for item in gate.AEMET_NATIONAL_RADAR_GATE.findings
    }

    assert by_key["product_identity"].state is gate.EvidenceState.VERIFIED
    assert by_key["meteorological_quantity"].state is gate.EvidenceState.VERIFIED
    assert "reflectivity" in by_key["meteorological_quantity"].finding
    assert by_key["physical_unit"].state is gate.EvidenceState.VERIFIED
    assert "dBZ" in by_key["physical_unit"].finding
    assert by_key["rain_rate_interpretation"].state is gate.EvidenceState.BLOCKED
    assert "not one-to-one" in by_key["rain_rate_interpretation"].finding


def test_existing_byte_classes_are_not_promoted_over_escala_metadata():
    by_key = {
        item.key: item for item in gate.AEMET_NATIONAL_RADAR_GATE.findings
    }
    scale = by_key["pixel_scale_mapping"]

    assert scale.state is gate.EvidenceState.BLOCKED
    assert "ESCALA" in scale.finding
    assert "1..8" in scale.finding
    assert by_key["nodata_noecho_coverage"].state is gate.EvidenceState.UNVERIFIED
    assert by_key["pixel_qc_semantics"].state is gate.EvidenceState.UNVERIFIED


def test_open_data_capture_requires_envelope_data_and_metadata_atomically():
    assert gate.OPENDATA_NATIONAL_RADAR_PATH == "/api/red/radar/nacional"
    assert gate.REQUIRED_EXACT_CAPTURE_ROLES == (
        "opendata_envelope_json",
        "opendata_data_response",
        "opendata_metadata_response",
    )
    by_key = {
        item.key: item for item in gate.AEMET_NATIONAL_RADAR_GATE.findings
    }
    assert by_key["documented_api_access"].state is gate.EvidenceState.VERIFIED
    assert by_key["legacy_transport_contract"].state is gate.EvidenceState.UNVERIFIED
    assert by_key["complete_artifact_set"].state is gate.EvidenceState.BLOCKED


def test_reuse_is_verified_but_does_not_open_meteorological_gates():
    contract = gate.AEMET_NATIONAL_RADAR_GATE
    by_key = {item.key: item for item in contract.findings}

    assert contract.retention_rights_verified is True
    assert by_key["retention_and_reuse"].state is gate.EvidenceState.VERIFIED
    assert (
        by_key["attribution_and_metadata_duties"].state
        is gate.EvidenceState.VERIFIED
    )
    assert contract.quantitative_decode_eligible is False


def test_legacy_member_name_retains_token_without_inventing_utc_or_support():
    facts = gate.parse_legacy_member_facts(
        "down_radw202608021230_4326.tif"
    )

    assert facts.timestamp_token == "202608021230"
    assert facts.crs_token == "4326"
    assert facts.timezone is None
    assert facts.nominal_timestamp is None
    assert facts.support_start is None
    assert facts.support_end is None
    assert facts.quantitative_decode_eligible is False
    assert "not_official" in facts.to_dict()["identity_basis"]


@pytest.mark.parametrize(
    "member_name",
    (
        "../down_radw202608021230_4326.tif",
        "folder/down_radw202608021230_4326.tif",
        "down_radw202602301230_4326.tif",
        "down_radw202608021230_3812.tif",
        "down_radw202608021230_4326.tiff",
        "down_radw202608021230_4326.tif?api_key=secret",
    ),
)
def test_legacy_member_parser_rejects_unsafe_or_unproven_shapes(member_name):
    with pytest.raises(gate.AemetProductGateError):
        gate.parse_legacy_member_facts(member_name)


def test_all_cited_sources_are_primary_aemet_https_without_credentials():
    assert set(gate.OFFICIAL_PRIMARY_SOURCES) == {
        gate.AEMET_RADAR_HELP,
        gate.AEMET_OPENDATA_SPECIFICATION,
        gate.AEMET_LEGAL_NOTICE,
    }
    for finding in gate.AEMET_NATIONAL_RADAR_GATE.findings:
        assert finding.source_urls
        for source in finding.source_urls:
            parsed = urlsplit(source)
            assert parsed.scheme == "https"
            assert parsed.hostname in {"www.aemet.es", "opendata.aemet.es"}
            assert parsed.username is None
            assert parsed.password is None
            assert parsed.query == ""


def test_module_has_no_transport_decoder_store_or_live_import():
    source = inspect.getsource(gate)
    forbidden = (
        "import requests",
        "import aiohttp",
        "import httpx",
        "from urllib.request",
        "RawArtifactStore",
        "provider_radar",
    )
    assert not any(token in source for token in forbidden)
    assert "decode" not in gate.__all__

    worker_root = Path(gate.__file__).resolve().parent
    for candidate in worker_root.glob("*.py"):
        if candidate.name in {
            "aemet_product_gate.py",
            # Closed audit inventory only: it neither imports nor dispatches
            # this product gate.  Keeping the gap visible here is not wiring.
            "worker_cutover_manifest.py",
        }:
            continue
        assert "aemet_product_gate" not in candidate.read_text(
            encoding="utf-8", errors="strict"
        )


def test_documentation_names_every_blocker_and_official_source():
    root = Path(__file__).resolve().parents[1]
    text = (root / "docs" / "AEMET_RADAR_PRODUCT_GATE.md").read_text(
        encoding="utf-8"
    )

    for blocker in EXPECTED_BLOCKERS:
        assert blocker in text
    for source in gate.OFFICIAL_PRIMARY_SOURCES:
        assert source in text
    assert "niet als neerslagintensiteit" in " ".join(text.split())
    assert "geen live wiring" in text
