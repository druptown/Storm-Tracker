"""Bounded HA-side retention of worker-owned KMI raw provenance."""
from __future__ import annotations

from copy import deepcopy
import importlib.util
from pathlib import Path
import sys
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
CLIENT_PATH = (
    ROOT / "custom_components" / "storm_tracker_v3" / "worker_radar_client.py"
)


@pytest.fixture()
def client_module(monkeypatch):
    """Load the pure client helper without importing Home Assistant."""
    package_name = "_stv3_worker_radar_client_test"
    package = types.ModuleType(package_name)
    package.__path__ = []
    providers = types.ModuleType(f"{package_name}.providers")
    providers.__path__ = []
    dwd = types.ModuleType(f"{package_name}.providers.dwd_radolan")
    dwd.read_radar_worker_artifact = lambda *_args, **_kwargs: ([], {})
    bridge = types.ModuleType(f"{package_name}.worker_bridge")

    async def async_request_worker_job(*_args, **_kwargs):
        return {}

    bridge.async_request_worker_job = async_request_worker_job
    const = types.ModuleType(f"{package_name}.const")
    const.DOMAIN = "storm_tracker_v3"
    for name, module in (
        (package_name, package),
        (f"{package_name}.providers", providers),
        (f"{package_name}.providers.dwd_radolan", dwd),
        (f"{package_name}.worker_bridge", bridge),
        (f"{package_name}.const", const),
    ):
        monkeypatch.setitem(sys.modules, name, module)
    module_name = f"{package_name}.worker_radar_client"
    spec = importlib.util.spec_from_file_location(module_name, CLIENT_PATH)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _result() -> dict:
    job_id = "kmi-slot-001"
    forecast_receipt = "raw-" + "1" * 64
    image_receipt = "raw-" + "2" * 64
    capture_id = "capture-" + "3" * 64
    manifest_sha256 = "4" * 64
    collector_version = "worker-kmi-appv4-exact-raw-v1"
    parser_versions = {
        "forecast_json": "kmi-appv4-forecast-json-parser-v1",
        "radar_png": "kmi-appv4-categorical-png-parser-v1",
    }
    product_timestamp = 1_800_000_000.0

    def source(
        role: str,
        receipt_id: str,
        content_sha256: str,
        *,
        semantic_class: str,
        content_type: str,
        received_at: float,
    ) -> dict:
        return {
            "role": role,
            "raw_receipt_id": receipt_id,
            "raw_content_sha256": content_sha256,
            "fetch_id": f"{job_id}:{role}",
            "upstream_record_id": (
                f"kmi_{role}|sequence_time={product_timestamp:.6f}"
                f"|payload_sha256={content_sha256}|location=51.000000,4.000000"
            ),
            "received_at": received_at,
            "content_type": content_type,
            "parser_version": parser_versions[role],
            "collector_version": collector_version,
            "provenance_grade": "exact_raw",
            "provenance_scope": "upstream_bytes_only",
            "semantic_class": semantic_class,
            "quantitative_semantics_eligible": False,
        }

    raw_provenance = {
        "raw_capture_id": capture_id,
        "raw_capture_manifest_sha256": manifest_sha256,
        "raw_capture_fetch_id": f"{job_id}:capture",
        "upstream_capture_id": (
            "kmi_appv4_visualisation_capture|sequence_time=1800000000.000000"
            "|uri_path_sha256=" + "5" * 64
            + "|forecast_sha256=" + "6" * 64
            + "|payload_sha256=" + "7" * 64
            + "|location=51.000000,4.000000"
        ),
        "product_timestamp": product_timestamp,
        "collector_version": collector_version,
        "parser_versions": parser_versions,
        "provenance_grade": "exact_raw",
        "provenance_scope": "upstream_bytes_only",
        "raw_sources": {
            "forecast_json": source(
                "forecast_json",
                forecast_receipt,
                "8" * 64,
                semantic_class="forecast_manifest",
                content_type="application/json",
                received_at=1_800_000_001.0,
            ),
            "radar_png": source(
                "radar_png",
                image_receipt,
                "9" * 64,
                semantic_class="categorical_rendered_image",
                content_type="image/png",
                received_at=1_800_000_002.0,
            ),
        },
    }
    return {
        "artifact_path": "/share/storm_tracker_worker/job-digest.json.gz",
        "artifact_sha256": "a" * 64,
        "artifact_bytes": 4_096,
        "content_type": "application/json+gzip",
        "frame_timestamp": product_timestamp,
        "observation_count": 0,
        "provider": "kmi",
        "product": "kmi_appv4_visualisation",
        "semantic_class": "categorical_rendered_image",
        "quantitative_semantics_eligible": False,
        "operational_use": "visualisation_only",
        "operational_admission_eligible": False,
        "admission_status": "uncertified_visualisation_only",
        "legacy_decoder_quality_constant": 0.95,
        "legacy_decoder_quality_operational": False,
        "provenance_grade": "exact_raw",
        "provenance_scope": "upstream_bytes_only",
        "raw_provenance": raw_provenance,
        "radar_cycle_bridge": {
            "status": "not_bridged_linked_capture",
            "current_contract": "stv3-radar-cycle-request-v1",
            "required_representation": "raw_capture_set",
            "raw_capture_id": capture_id,
            "raw_capture_manifest_sha256": manifest_sha256,
            "member_receipt_ids": [forecast_receipt, image_receipt],
            "single_receipt_complete_set": False,
            "exact_raw_artifacts_complete_in_v1": False,
        },
        "diagnostics": {"transport_token": "must-not-be-retained"},
        "unrelated": "must-not-be-retained",
    }


def test_complete_kmi_chain_is_retained_as_an_independent_copy(client_module):
    result = _result()
    expected_provenance = deepcopy(result["raw_provenance"])
    expected_bridge = deepcopy(result["radar_cycle_bridge"])

    metadata = client_module.worker_radar_artifact_metadata(result)

    assert metadata["raw_provenance"] == expected_provenance
    assert metadata["radar_cycle_bridge"] == expected_bridge
    for key in (
        "provider",
        "product",
        "semantic_class",
        "quantitative_semantics_eligible",
        "operational_use",
        "operational_admission_eligible",
        "admission_status",
        "legacy_decoder_quality_constant",
        "legacy_decoder_quality_operational",
        "provenance_grade",
        "provenance_scope",
    ):
        assert metadata[key] == result[key]
    assert metadata["observation_count"] == 0
    assert "diagnostics" not in metadata
    assert "unrelated" not in metadata

    result["raw_provenance"]["parser_versions"]["forecast_json"] = "mutated"
    result["radar_cycle_bridge"]["member_receipt_ids"].clear()
    assert metadata["raw_provenance"] == expected_provenance
    assert metadata["radar_cycle_bridge"] == expected_bridge


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: value["raw_provenance"]["raw_sources"][
            "forecast_json"
        ].__setitem__("token", "secret"),
        lambda value: value["raw_provenance"]["raw_sources"][
            "radar_png"
        ].__setitem__("upstream_record_id", "frame?k=secret"),
        lambda value: value["raw_provenance"].__setitem__(
            "upstream_capture_id", "Bearer abcdefghijklmnop"
        ),
        lambda value: value["raw_provenance"].__setitem__(
            "upstream_capture_id", "x" * 1_025
        ),
    ),
)
def test_kmi_chain_rejects_secrets_unknown_fields_and_oversize(
    client_module, mutation,
):
    result = _result()
    mutation(result)
    with pytest.raises(ValueError):
        client_module.worker_radar_artifact_metadata(result)


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: value.pop("radar_cycle_bridge"),
        lambda value: value["radar_cycle_bridge"].__setitem__(
            "member_receipt_ids", ["raw-" + "f" * 64, "raw-" + "e" * 64]
        ),
        lambda value: value.__setitem__("quantitative_semantics_eligible", True),
        lambda value: value.__setitem__("operational_admission_eligible", True),
        lambda value: value.__setitem__("observation_count", 1),
        lambda value: value["raw_provenance"].__setitem__(
            "product_timestamp", value["frame_timestamp"] + 300
        ),
    ),
)
def test_kmi_chain_rejects_partial_or_false_meteorological_claims(
    client_module, mutation,
):
    result = _result()
    mutation(result)
    with pytest.raises(ValueError):
        client_module.worker_radar_artifact_metadata(result)


def test_non_kmi_worker_metadata_remains_small_and_drops_diagnostics(client_module):
    result = {
        "artifact_path": "/share/storm_tracker_worker/radar.json.gz",
        "artifact_sha256": "b" * 64,
        "artifact_bytes": 1_024,
        "content_type": "application/json+gzip",
        "frame_timestamp": 1_800_000_000.0,
        "observation_count": 12,
        "diagnostics": {"access_token": "secret"},
        "token": "secret",
    }
    assert client_module.worker_radar_artifact_metadata(result) == {
        key: result[key]
        for key in (
            "artifact_path",
            "artifact_sha256",
            "artifact_bytes",
            "content_type",
            "frame_timestamp",
            "observation_count",
        )
    }


def test_remember_stores_the_complete_safe_kmi_chain(client_module):
    hass = types.SimpleNamespace(data={"storm_tracker_v3": {}})
    result = _result()
    metadata = client_module.remember_worker_radar_artifact(hass, "kmi", result)
    assert hass.data["storm_tracker_v3"]["provider_worker_artifacts"]["kmi"] == metadata
    assert metadata["raw_provenance"]["raw_capture_id"].startswith("capture-")
    assert metadata["radar_cycle_bridge"]["status"] == (
        "not_bridged_linked_capture"
    )
