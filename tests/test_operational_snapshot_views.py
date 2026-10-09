"""Tests for the compute-free Home Assistant operational snapshot views."""
from __future__ import annotations

import ast
import importlib.util
import json
from pathlib import Path
import sys
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"


def _load_modules():
    package_name = "storm_tracker_v3_snapshot_views_test"
    package = sys.modules.get(package_name)
    if package is None:
        package = types.ModuleType(package_name)
        package.__path__ = [str(COMPONENT)]
        sys.modules[package_name] = package

    loaded = []
    for short_name in ("operational_snapshot", "operational_snapshot_views"):
        full_name = f"{package_name}.{short_name}"
        module = sys.modules.get(full_name)
        if module is None:
            spec = importlib.util.spec_from_file_location(
                full_name, COMPONENT / f"{short_name}.py"
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[full_name] = module
            assert spec.loader is not None
            spec.loader.exec_module(module)
        loaded.append(module)
    return tuple(loaded)


contract, views = _load_modules()


def _ground(*, status="unknown", freshness="current"):
    return {
        "status": status,
        "freshness": freshness,
        "confirmation": None,
        "same_track": None,
        "evidence_valid_at": None,
        "observed_at_min": None,
        "observed_at_max": None,
        "received_at": None,
        "physical_site_count": 0,
        "wet_site_count": 0,
        "source_ids": [],
        "reason_codes": [],
    }


def _snapshot(*, revision=1, current_state="dry", threat_state="available"):
    current_known = current_state in {"dry", "raining"}
    threat_available = threat_state == "available"
    return {
        "contract": "operational_snapshot_v1",
        "revision": revision,
        "desired_state_sha256": "d" * 64,
        "product_valid_at": 1_000.0,
        "received_at": 1_002.0,
        "evaluated_at": 1_004.0,
        "valid_until": 1_304.0,
        "provenance_complete": True,
        "provenance_source_ids": ["opera"],
        "provenance_reason_codes": [],
        "worker": {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
            "status_text": "operationeel",
        },
        "regions": {
            "complete": True,
            "overflow_count": 0,
            "items": [{
                "region_id": "region-home",
                "display_name": "Thuisregio",
                "state": "current",
                "provider_ids": ["opera"],
                "target_ids": ["home"],
            }],
        },
        "targets": {
            "complete": True,
            "overflow_count": 0,
            "items": [{
                "target_id": "home",
                "display_name": "Thuis",
                "region_id": "region-home",
                "status_text": "Actuele en volgende toestand zijn afzonderlijk",
                "current_precipitation": {
                    "state": current_state,
                    "freshness": "current" if current_known else "unknown",
                    "rain_rate_mm_h": (
                        0.0 if current_state == "dry"
                        else 1.25 if current_state == "raining"
                        else None
                    ),
                    "product_valid_at": 1_000.0 if current_known else None,
                    "provider_ids": ["opera"] if current_known else [],
                    "reason_codes": [] if current_known else ["radar_unknown"],
                    "ground_evidence": (
                        _ground() if current_known
                        else _ground(freshness="unknown")
                    ),
                },
                "next_threat": {
                    "state": threat_state,
                    "freshness": "current" if threat_available else "unknown",
                    "track_id": "track-7" if threat_available else None,
                    "calibrated_probability_percent": (
                        72.5 if threat_available else None
                    ),
                    "probability_calibration_id": (
                        "empirical:held-out-v1" if threat_available else None
                    ),
                    "eta_minutes": 18.0 if threat_available else None,
                    "distance_km": 12.5 if threat_available else None,
                    "passage_status": "expected" if threat_available else "unknown",
                    "product_valid_at": 1_000.0 if threat_available else None,
                    "provider_ids": ["opera"] if threat_available else [],
                    "reason_codes": [],
                },
            }],
        },
        "providers": {
            "complete": True,
            "overflow_count": 0,
            "items": [{
                "provider_id": "opera",
                "display_name": "OPERA",
                "state": "operational",
                "freshness": "current",
                "product_valid_at": 1_000.0,
                "received_at": 1_002.0,
                "error": None,
                "reason_codes": [],
            }],
        },
        "ui_storms": {
            "complete": True,
            "overflow_count": 0,
            "items": [],
        },
        "map_artifact": {"artifact_handle": None, "etag": None},
    }


def _cache(snapshot=None):
    cache = contract.OperationalSnapshotCache()
    cache.apply_snapshot(_snapshot() if snapshot is None else snapshot)
    return cache


def test_current_rain_and_next_threat_are_projected_as_separate_facts():
    view = views.target_precipitation_view(
        _cache(_snapshot(current_state="raining")), "home"
    )

    assert view.native_value == "regen"
    assert view.attributes["current_precipitation_state"] == "raining"
    assert view.attributes["rain_rate_mm_h"] == 1.25
    assert view.attributes["next_threat_state"] == "available"
    assert view.attributes["next_threat_eta_minutes"] == 18.0


def test_available_future_threat_never_turns_unknown_current_fact_into_rain():
    view = views.target_precipitation_view(
        _cache(_snapshot(current_state="unknown")), "home"
    )

    assert view.native_value == "onvoldoende_data"
    assert view.attributes["current_precipitation_state"] == "unknown"
    assert view.attributes["rain_rate_mm_h"] is None
    assert view.attributes["next_threat"] == "beschikbaar"


def test_worker_offline_fails_closed_to_unknown_and_stale_not_dry():
    cache = _cache(_snapshot(current_state="raining"))
    assert cache.mark_worker_offline() is True

    view = views.target_precipitation_view(cache, "home")
    assert view.native_value == "onvoldoende_data"
    assert view.attributes["current_freshness"] == "stale"
    assert view.attributes["next_threat_state"] == "unknown"
    assert view.attributes["snapshot_availability"] == "stale"


def test_expired_snapshot_keeps_provider_ready_but_marks_status_stale():
    cache = _cache()
    assert cache.mark_expired(now=1_304.0) is True

    view = views.provider_status_view(cache, "opera")

    assert view.native_value == "stale"
    assert view.attributes["readiness"] == "ready"
    assert view.attributes["effective_status"] == "stale"
    assert view.attributes["freshness"] == "stale"
    assert "snapshot_expired" in view.attributes["reason_codes"]


def test_current_heartbeat_provider_status_survives_stale_weather_snapshot():
    cache = _cache()
    assert cache.mark_stale(
        reason="desired_state_transition",
        worker_state="unknown",
    ) is True
    heartbeat_status = {
        "desired_state_sha256": "d" * 64,
        "evaluated_at": 1_200.0,
        "valid_until": 1_500.0,
        "items": [{
            "provider_id": "opera",
            "display_name": "OPERA",
            "readiness": "ready",
            "state": "operational",
            "freshness": "current",
            "product_valid_at": 1_190.0,
            "received_at": 1_195.0,
            "error": None,
            "reason_codes": [],
        }],
    }

    view = views.provider_status_view(
        cache,
        "opera",
        runtime_status_snapshot=heartbeat_status,
        now=1_250.0,
    )

    assert view.native_value == "operational"
    assert view.attributes["readiness"] == "ready"
    assert view.attributes["provider_status_availability"] == "current"
    assert view.attributes["snapshot_availability"] == "stale"


def test_expired_heartbeat_provider_status_becomes_individually_stale():
    heartbeat_status = {
        "desired_state_sha256": "d" * 64,
        "evaluated_at": 1_200.0,
        "valid_until": 1_500.0,
        "items": [{
            "provider_id": "opera",
            "display_name": "OPERA",
            "readiness": "ready",
            "state": "operational",
            "freshness": "current",
            "product_valid_at": 1_190.0,
            "received_at": 1_195.0,
            "error": None,
            "reason_codes": [],
        }],
    }

    view = views.provider_status_view(
        _cache(),
        "opera",
        runtime_status_snapshot=heartbeat_status,
        now=1_501.0,
    )

    assert view.native_value == "stale"
    assert view.attributes["readiness"] == "ready"
    assert view.attributes["provider_status_availability"] == "stale"
    assert "provider_status_expired" in view.attributes["reason_codes"]


def test_missing_target_is_explicit_unknown_but_sensor_can_report_reason():
    view = views.target_precipitation_view(_cache(), "person.wim")
    assert view.available is True
    assert view.native_value == "onvoldoende_data"
    assert view.attributes["current_reason_codes"] == [
        "target_snapshot_missing"
    ]


def test_count_and_provider_views_use_preindexed_cache_records():
    cache = _cache()
    storms = views.collection_count_view(cache, "ui_storms")
    provider = views.provider_status_view(cache, "opera")

    assert storms.native_value == 0
    assert storms.attributes["complete"] is True
    assert provider.native_value == "operational"
    assert provider.attributes["freshness"] == "current"
    assert provider.attributes["readiness"] == "ready"
    assert provider.attributes["effective_status"] == "operational"


def _providerless_unknown_snapshot(*, transport_omitted: bool):
    snapshot = _snapshot(current_state="unknown", threat_state="unknown")
    snapshot["product_valid_at"] = None
    snapshot["received_at"] = None
    snapshot["provenance_complete"] = False
    snapshot["provenance_source_ids"] = []
    snapshot["provenance_reason_codes"] = [
        "awaiting_meteorological_evidence"
    ]
    snapshot["regions"]["items"][0]["state"] = "unknown"
    snapshot["regions"]["items"][0]["provider_ids"] = []
    snapshot["targets"]["items"][0]["status_text"] = ""
    snapshot["providers"] = {
        "complete": not transport_omitted,
        "overflow_count": 1 if transport_omitted else 0,
        "items": [],
    }
    return snapshot


def test_empty_safe_fallback_provider_is_unknown_without_false_error():
    view = views.provider_status_view(
        _cache(_providerless_unknown_snapshot(transport_omitted=False)),
        "opera",
    )

    assert view.available is True
    assert view.native_value == "unknown"
    assert view.attributes["error"] is None
    assert view.attributes["reason_codes"] == [
        "awaiting_meteorological_evidence"
    ]


def test_transport_omitted_provider_is_unknown_without_false_error():
    view = views.provider_status_view(
        _cache(_providerless_unknown_snapshot(transport_omitted=True)),
        "opera",
    )

    assert view.available is True
    assert view.native_value == "unknown"
    assert view.attributes["error"] is None
    assert view.attributes["reason_codes"] == [
        "provider_projection_omitted_for_transport"
    ]


def test_repeated_reads_are_side_effect_free_and_coordinate_free():
    cache = _cache(_snapshot(current_state="raining"))
    revision = cache.revision
    digest = cache.snapshot_sha256
    values = [
        views.target_precipitation_view(cache, "home")
        for _ in range(1_000)
    ]

    assert cache.revision == revision
    assert cache.snapshot_sha256 == digest
    encoded = json.dumps(values[-1].attribute_copy(), sort_keys=True)
    assert "latitude" not in encoded
    assert "longitude" not in encoded
    assert "credential" not in encoded


def test_view_module_has_no_home_assistant_provider_storage_or_compute_imports():
    tree = ast.parse(
        (COMPONENT / "operational_snapshot_views.py").read_text(
            encoding="utf-8"
        )
    )
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden = (
        "homeassistant",
        "sqlite3",
        "numpy",
        "pysteps",
        "providers",
        "engine",
        "requests",
        "urllib",
    )
    assert not any(
        name == prefix or name.startswith(prefix + ".")
        for name in imported
        for prefix in forbidden
    )


@pytest.mark.parametrize("collection", ["unknown", "", "storm"])
def test_unknown_collection_is_rejected(collection):
    with pytest.raises(ValueError, match="onbekende"):
        views.collection_count_view(_cache(), collection)
