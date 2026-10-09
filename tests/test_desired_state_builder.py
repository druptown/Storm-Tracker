"""Parity and isolation tests for the pure HA-side desired-state builder."""
from __future__ import annotations

import ast
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

from storm_tracker_worker import desired_state as worker_contract


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "custom_components/storm_tracker_v3/desired_state_builder.py"


def _load_builder():
    name = "storm_tracker_v3_desired_state_builder_test"
    existing = sys.modules.get(name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


builder = _load_builder()


def _target(
    target_id: str = "zone.home",
    *,
    available: bool = True,
    observed_at: float | None = 1_800_000_000.0,
) -> dict:
    return {
        "target_id": target_id,
        "display_name": "Thuis" if target_id == "zone.home" else target_id,
        "kind": "home" if target_id == "zone.home" else "device_tracker",
        "available": available,
        "latitude": 51.04,
        "longitude": 4.48,
        "position_observed_at": observed_at,
        "location_accuracy_m": None,
        "observation_radius_km": 300,
        "country_code": "BE",
    }


def _settings() -> dict:
    return {
        "ground_radius_km": 20,
        "lightning_source_mode": "auto",
        "model_guidance_enabled": False,
        "region_sharing_distance_km": 150,
    }


def _document() -> dict:
    return {
        "contract": builder.CONTRACT,
        "producer_id": "ha-boot-a",
        "authority_epoch": 3,
        "generation": 7,
        "issued_at": 1_800_000_001,
        "previous_state_sha256": "a" * 64,
        "targets": [
            _target("device_tracker.wim"),
            _target("zone.home"),
        ],
        "settings": _settings(),
    }


def test_builder_bytes_and_sha_are_byte_identical_to_worker_parser():
    envelope = builder.build_desired_state_document(_document())
    parsed = worker_contract.parse_desired_state(envelope.canonical_bytes)

    assert parsed.canonical_bytes == envelope.canonical_bytes
    assert parsed.state_sha256 == envelope.envelope_sha256
    assert envelope.state_sha256 == envelope.envelope_sha256
    assert envelope.envelope_sha256 == hashlib.sha256(
        envelope.canonical_bytes
    ).hexdigest()
    assert len(envelope.canonical_bytes) <= 64 * 1024
    assert envelope.target_count == 2


def test_target_input_order_does_not_change_bytes_or_digest():
    first = _document()
    second = copy.deepcopy(first)
    second["targets"].reverse()
    left = builder.build_desired_state_document(first)
    right = builder.build_desired_state_document(second)
    assert left.canonical_bytes == right.canonical_bytes
    assert left.envelope_sha256 == right.envelope_sha256
    assert [item["target_id"] for item in left.as_dict()["targets"]] == [
        "device_tracker.wim",
        "zone.home",
    ]


def test_current_and_unavailable_last_known_positions_match_worker_contract():
    value = _document()
    value["targets"] = [
        _target("device_tracker.current", available=True, observed_at=1_800_000_000),
        _target("device_tracker.unavailable", available=False, observed_at=None),
    ]
    envelope = builder.build_desired_state_document(value)
    parsed = worker_contract.parse_desired_state(envelope.canonical_bytes)
    by_id = {target.target_id: target for target in parsed.targets}
    assert by_id["device_tracker.current"].available is True
    assert by_id["device_tracker.current"].position_observed_at == 1_800_000_000.0
    assert by_id["device_tracker.unavailable"].available is False
    assert by_id["device_tracker.unavailable"].position_observed_at is None
    # Worker v1 still requires bounded last-known coordinates when unavailable.
    assert by_id["device_tracker.unavailable"].latitude == 51.04


def test_available_position_requires_timestamp_but_unavailable_can_be_null():
    value = _document()
    value["targets"] = [_target(observed_at=None)]
    with pytest.raises(builder.DesiredStateBuildError, match="mist"):
        builder.build_desired_state_document(value)

    value["targets"][0]["available"] = False
    assert builder.build_desired_state_document(value).target_count == 1


def test_all_64_minimal_targets_fit_and_65th_fails_before_encoding():
    value = _document()
    value["targets"] = [
        {
            **_target(f"fixed.{index:02d}"),
            "display_name": f"T{index:02d}",
            "kind": "fixed",
        }
        for index in range(builder.MAX_TARGETS)
    ]
    envelope = builder.build_desired_state_document(value)
    assert envelope.target_count == 64
    assert len(envelope.canonical_bytes) <= builder.MAX_CONTRACT_BYTES
    worker_contract.parse_desired_state(envelope.canonical_bytes)

    value["targets"].append({
        **_target("fixed.64"),
        "display_name": "T64",
        "kind": "fixed",
    })
    with pytest.raises(builder.DesiredStateBuildError, match="lijstgrens|64"):
        builder.build_desired_state_document(value)


def test_duplicate_target_id_fails_closed():
    value = _document()
    value["targets"].append(copy.deepcopy(value["targets"][0]))
    with pytest.raises(builder.DesiredStateBuildError, match="uniek"):
        builder.build_desired_state_document(value)


@pytest.mark.parametrize(
    "mutation,match",
    [
        (lambda value: value.update(authority_epoch=0), "authority_epoch"),
        (lambda value: value.update(authority_epoch=True), "authority_epoch"),
        (lambda value: value.update(generation=-1), "generation"),
        (lambda value: value.update(issued_at=float("nan")), "eindig"),
        (lambda value: value["targets"][0].update(latitude=91), "latitude"),
        (lambda value: value["targets"][0].update(longitude=-181), "longitude"),
        (
            lambda value: value["targets"][0].update(
                location_accuracy_m=float("inf")
            ),
            "location_accuracy_m",
        ),
        (
            lambda value: value["targets"][0].update(observation_radius_km=0),
            "observation_radius_km",
        ),
        (
            lambda value: value["targets"][0].update(position_observed_at=-1),
            "position_observed_at",
        ),
        (lambda value: value["targets"][0].update(country_code="be"), "country"),
        (
            lambda value: value["settings"].update(ground_radius_km=101),
            "ground_radius_km",
        ),
        (
            lambda value: value["settings"].update(
                region_sharing_distance_km=501
            ),
            "region_sharing_distance_km",
        ),
        (
            lambda value: value["settings"].update(
                lightning_source_mode="magic"
            ),
            "lightning_source_mode",
        ),
        (
            lambda value: value["settings"].update(model_guidance_enabled=1),
            "model_guidance_enabled",
        ),
        (
            lambda value: value.update(previous_state_sha256="A" * 64),
            "previous_state_sha256",
        ),
    ],
)
def test_typed_bounds_fail_closed_before_worker_boundary(mutation, match):
    value = _document()
    mutation(value)
    with pytest.raises(builder.DesiredStateBuildError, match=match):
        builder.build_desired_state_document(value)


@pytest.mark.parametrize(
    "mutation,match",
    [
        (lambda value: value.update(extra=True), "sleutels"),
        (lambda value: value["targets"][0].update(extra=True), "sleutels"),
        (lambda value: value["settings"].update(extra=True), "sleutels"),
        (
            lambda value: value["targets"][0].update(display_name="Cafe\u0301"),
            "NFC",
        ),
        (lambda value: value.update(access_token="secret"), "credentialveld"),
        (
            lambda value: value["targets"][0].update(
                display_name="Bearer abcdefghijklmnopqrstuvwxyz"
            ),
            "credentialvormige",
        ),
        (
            lambda value: value.update(producer_id="access_token"),
            "credentialveld|veilig",
        ),
    ],
)
def test_extra_unicode_and_secret_surfaces_fail_closed(mutation, match):
    value = _document()
    mutation(value)
    with pytest.raises(builder.DesiredStateBuildError, match=match):
        builder.build_desired_state_document(value)


def test_direct_builder_and_document_builder_produce_identical_bytes():
    document = _document()
    exact = builder.build_desired_state_document(document)
    direct = builder.build_desired_state(
        producer_id=document["producer_id"],
        authority_epoch=document["authority_epoch"],
        generation=document["generation"],
        issued_at=document["issued_at"],
        previous_state_sha256=document["previous_state_sha256"],
        targets=document["targets"],
        settings=document["settings"],
    )
    assert direct == exact


def test_result_is_detached_from_mutable_inputs():
    value = _document()
    envelope = builder.build_desired_state_document(value)
    original = bytes(envelope.canonical_bytes)
    value["targets"][0]["latitude"] = 0
    value["settings"]["ground_radius_km"] = 99
    first = envelope.as_dict()
    first["targets"][0]["latitude"] = -1
    assert envelope.canonical_bytes == original
    assert envelope.as_dict()["targets"][0]["latitude"] != -1


def test_hard_byte_budget_is_checked_on_canonical_uncompressed_bytes(monkeypatch):
    monkeypatch.setattr(builder, "MAX_CONTRACT_BYTES", 128)
    with pytest.raises(builder.DesiredStateSizeError, match="64-KiB"):
        builder.build_desired_state_document(_document())


def test_canonical_transport_has_no_whitespace_or_noncanonical_number_types():
    envelope = builder.build_desired_state_document(_document())
    decoded = json.loads(envelope.canonical_bytes)
    assert b"\n" not in envelope.canonical_bytes
    assert b": " not in envelope.canonical_bytes
    assert decoded["issued_at"] == 1_800_000_001.0
    assert decoded["settings"]["ground_radius_km"] == 20.0
    assert decoded["targets"][0]["observation_radius_km"] == 300.0


def test_ast_import_isolation_and_no_side_effect_api():
    tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_roots.add(node.module.split(".")[0])
    assert imported_roots.isdisjoint({
        "aiohttp",
        "asyncio",
        "homeassistant",
        "mqtt",
        "pathlib",
        "requests",
        "socket",
        "sqlite3",
        "storm_tracker_worker",
        "urllib",
    })
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"open", "print"}
        for node in ast.walk(tree)
    )
    names = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
    assert names == {
        "canonical_json_bytes",
        "_exact_keys",
        "_reject_secret_surface",
        "_identifier",
        "_display_text",
        "_number",
        "_optional_number",
        "_integer",
        "_target",
        "_settings",
        "build_desired_state",
        "build_desired_state_document",
    }
