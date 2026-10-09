from __future__ import annotations

import ast
from copy import deepcopy
import json
from pathlib import Path

import pytest

from storm_tracker_worker.knmi_rtcor_analysis_grid_gate import (
    KNMI_RTCOR_GRID_GATE_SCHEMA,
    KnmiRtcorGridCertificateWithheldError,
    KnmiRtcorGridGateState,
    evaluate_knmi_rtcor_analysis_grid_gate,
    issue_knmi_rtcor_analysis_grid_certificate,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "knmi_rtcor_analysis_grid_authority.json"


def _python_imports_module(content: str, module_name: str) -> bool:
    tree = ast.parse(content)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) and any(
            alias.name.endswith(module_name) for alias in node.names
        ):
            return True
        if isinstance(node, ast.ImportFrom) and (
            (node.module or "").endswith(module_name)
            or any(alias.name == module_name for alias in node.names)
        ):
            return True
        if not isinstance(node, ast.Call) or not node.args:
            continue
        argument = node.args[0]
        if not (
            isinstance(argument, ast.Constant)
            and isinstance(argument.value, str)
            and argument.value.endswith(module_name)
        ):
            continue
        if isinstance(node.func, ast.Name) and node.func.id == "__import__":
            return True
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "import_module"
        ):
            return True
    return False


def _manifest() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_official_authority_snapshot_is_deliberately_withheld() -> None:
    result = evaluate_knmi_rtcor_analysis_grid_gate(_manifest())

    assert result.state is KnmiRtcorGridGateState.WITHHELD
    assert result.certificate is None
    assert not result.official_certificate_eligible
    assert len(result.evidence_sha256) == 64
    assert {blocker.code for blocker in result.blockers} == {
        "official_crs_authorities_conflict",
        "pixel_lu_edge_semantics_not_formally_defined",
        "hdf36_payload_nl25_profile_version_gap",
        "official_live_hdf_grid_evidence_not_retained",
        "array_display_origin_not_decoder_bound",
        "kilometre_to_metre_crs_equivalence_not_certified",
    }


def test_printed_nl25_values_are_informational_not_a_certificate() -> None:
    result = evaluate_knmi_rtcor_analysis_grid_gate(_manifest())
    geometry = result.informational_geometry

    assert geometry["authority"] == "informational_only_not_a_transform_certificate"
    assert geometry["array_shape"] == (765, 700)
    assert geometry["width"] == 700
    assert geometry["height"] == 765
    assert geometry["candidate_west_anchor_km"] == 0.0
    assert geometry["candidate_north_anchor_km"] == 3649.9814
    assert geometry["candidate_east_edge_if_lu_is_outer_edge_km"] == 700.00245
    assert geometry["candidate_south_edge_if_lu_is_outer_edge_km"] == 2884.9776515
    assert geometry["catalog_crs_axes_km"] != geometry["hdf38_nl25_crs_axes_km"]


def test_gate_mapping_never_serializes_a_certificate_or_eligibility() -> None:
    mapping = evaluate_knmi_rtcor_analysis_grid_gate(_manifest()).to_mapping()

    assert mapping["certificate"] is None
    assert mapping["official_certificate_eligible"] is False
    assert mapping["state"] == "withheld"
    assert mapping["blockers"]


def test_synthetic_evidence_cannot_be_promoted_to_official_provider_proof() -> None:
    manifest = _manifest()
    synthetic = deepcopy(manifest["evidence_records"][2])
    synthetic["record_id"] = "synthetic_matching_nl25_fixture"
    synthetic["evidence_class"] = "synthetic_fixture"
    synthetic["source_url"] = "https://example.invalid/synthetic-knmi-grid"
    manifest["evidence_records"].append(synthetic)

    result = evaluate_knmi_rtcor_analysis_grid_gate(manifest)

    assert result.certificate is None
    assert not result.official_certificate_eligible
    blocker = next(
        item
        for item in result.blockers
        if item.code == "authority_manifest_incomplete_or_untrusted"
    )
    assert "synthetic_evidence_cannot_open_official_gate" in blocker.detail


def test_array_shape_and_matching_numbers_alone_never_open_gate() -> None:
    manifest = {
        "dataset": "nl_rdr_data_rtcor_5m/1.0",
        "evidence_records": [
            {
                "evidence_class": "synthetic_fixture",
                "facts": {
                    "columns": 700,
                    "rows": 765,
                    "pixel_size_x_km": 1.0000035,
                    "pixel_size_y_km": -1.0000049,
                },
                "record_id": "synthetic_array_shape_only",
                "source_url": "https://example.invalid/array-shape-only",
            }
        ],
        "internal_product": "RAD_NL25_COR_NA",
        "product": "knmi_nl_rdr_data_rtcor_5m_capture",
        "provider": "knmi",
        "schema": KNMI_RTCOR_GRID_GATE_SCHEMA,
        "status": "withheld",
    }

    result = evaluate_knmi_rtcor_analysis_grid_gate(manifest)

    assert result.certificate is None
    codes = {item.code for item in result.blockers}
    assert "authority_manifest_incomplete_or_untrusted" in codes
    assert "pixel_lu_edge_semantics_not_formally_defined" in codes


def test_issue_api_always_raises_with_all_open_gates() -> None:
    with pytest.raises(KnmiRtcorGridCertificateWithheldError) as raised:
        issue_knmi_rtcor_analysis_grid_certificate(_manifest())

    message = str(raised.value)
    assert "official_crs_authorities_conflict" in message
    assert "kilometre_to_metre_crs_equivalence_not_certified" in message


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: value.update(schema="wrong"),
        lambda value: value.update(provider="synthetic_knmi"),
        lambda value: value["evidence_records"].pop(),
        lambda value: value["evidence_records"].append(
            deepcopy(value["evidence_records"][0])
        ),
        lambda value: value["evidence_records"][0]["facts"].update(
            reference_system="synthetic:copied-array-shape"
        ),
    ),
)
def test_manifest_identity_drift_is_retained_as_explicit_blocker(mutation) -> None:
    manifest = _manifest()
    mutation(manifest)

    result = evaluate_knmi_rtcor_analysis_grid_gate(manifest)

    assert result.certificate is None
    assert any(
        item.code == "authority_manifest_incomplete_or_untrusted"
        for item in result.blockers
    )


def test_manifest_rejects_non_finite_and_unbounded_values() -> None:
    manifest = _manifest()
    manifest["evidence_records"][0]["facts"]["bad"] = float("nan")
    with pytest.raises(ValueError, match="outside its bound"):
        evaluate_knmi_rtcor_analysis_grid_gate(manifest)

    manifest = _manifest()
    manifest["evidence_records"][0]["facts"]["bad"] = "x" * 3000
    with pytest.raises(ValueError, match="invalid text"):
        evaluate_knmi_rtcor_analysis_grid_gate(manifest)


def test_candidate_isolation_no_service_scheduler_config_or_ha_wiring() -> None:
    needle = "knmi_rtcor_analysis_grid_gate"
    hits: list[str] = []
    for base in (ROOT / "storm_tracker_worker", ROOT / "custom_components"):
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in {
                ".py", ".json", ".yaml", ".yml", ".sh"
            }:
                continue
            if path.name in {
                "knmi_rtcor_analysis_grid_gate.py",
                "worker_cutover_manifest.py",
            }:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            referenced = (
                _python_imports_module(text, needle)
                if path.suffix.lower() == ".py"
                else needle in text
            )
            if referenced:
                hits.append(str(path.relative_to(ROOT)))
    assert hits == []
