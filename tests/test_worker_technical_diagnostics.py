from __future__ import annotations

import ast
import importlib.util
import json
import math
from pathlib import Path
import sys
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = (
    ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "worker_technical_diagnostics.py"
)


def _load_module():
    name = "storm_tracker_v3_worker_technical_diagnostics_contract_test"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


diagnostics = _load_module()


def _source(**overrides: Any) -> dict[str, Any]:
    source: dict[str, Any] = {
        "contract": diagnostics.SOURCE_CONTRACT,
        "worker_state": "online",
        "worker_observed_at": 990.0,
        "worker_version": "9.8.7",
        "build_sha256": "B" * 64,
        "config_sha256": "sha256:" + "C" * 64,
        "desired_state_sha256": "d" * 64,
        "provider_age_seconds": 8.25,
        "provider_status": "current",
        "frame_age_seconds": 13,
        "frame_status": "current",
        "cache_age_seconds": 2.1254,
        "cache_status": "current",
        "queue_depth": 3,
        "terminal_jobs": 41,
        "failed_jobs": 2,
        "retention_alarm": "warning",
        "capacity_alarm": "clear",
        "headroom_bytes": 8_000_000,
        "headroom_percent": 37.1254,
        "storage_reason_codes": ["retention_guard_active", "capacity_ok"],
        "transition_status": "pending",
        "current_version": "9.8.7",
        "next_version": "1.17.0",
        "transition_reason_codes": ["restart_required"],
        "replay_contract_version": "stv3-replay-contract-v2",
        "replay_algorithm_version": "sha256-event-resampling-v1",
    }
    source.update(overrides)
    return source


def test_full_projection_has_fixed_bounded_technical_contract():
    result = diagnostics.build_worker_technical_diagnostics(
        _source(),
        now=1_000.0,
    )

    assert set(result) == {
        "contract",
        "source_contract",
        "status",
        "projected_at",
        "projection_reason_codes",
        "worker",
        "freshness",
        "jobs",
        "storage",
        "transition",
        "replay",
        "claims",
    }
    assert result["contract"] == diagnostics.CONTRACT
    assert result["source_contract"] == diagnostics.SOURCE_CONTRACT
    assert result["status"] == "current"
    assert result["projection_reason_codes"] == []
    assert result["worker"] == {
        "state": "online",
        "observed_at": 990.0,
        "age_seconds": 10.0,
        "version": "9.8.7",
        "build_sha256": "sha256:" + "b" * 64,
        "config_sha256": "sha256:" + "c" * 64,
        "desired_state_sha256": "sha256:" + "d" * 64,
    }
    assert result["freshness"] == {
        "provider": {"status": "current", "age_seconds": 8.25},
        "frame": {"status": "current", "age_seconds": 13.0},
        "cache": {"status": "current", "age_seconds": 2.125},
    }
    assert result["jobs"] == {
        "status": "current",
        "queue_depth": 3,
        "terminal_jobs": 41,
        "failed_jobs": 2,
        "counts_saturated": False,
    }
    assert result["storage"] == {
        "status": "current",
        "retention_alarm": "warning",
        "capacity_alarm": "clear",
        "headroom_bytes": 8_000_000,
        "headroom_percent": 37.125,
        "reason_codes": ["capacity_ok", "retention_guard_active"],
        "reason_codes_complete": True,
    }
    assert result["transition"] == {
        "status": "current",
        "phase": "pending",
        "current_version": "9.8.7",
        "next_version": "1.17.0",
        "reason_codes": ["restart_required"],
        "reason_codes_complete": True,
    }
    assert result["replay"] == {
        "status": "current",
        "contract_version": "stv3-replay-contract-v2",
        "algorithm_version": "sha256-event-resampling-v1",
    }
    assert result["claims"] == {
        "meteorological_truth": False,
        "operational_admission": False,
        "alert": False,
    }
    assert len(diagnostics.canonical_diagnostics_bytes(result)) < 4_096
    assert len(diagnostics.canonical_diagnostics_bytes(result)) <= (
        diagnostics.MAX_DIAGNOSTIC_BYTES
    )


def test_exact_wire_normalizer_binds_local_liveness_and_detaches_values():
    wire = _source(
        worker_state="online",
        worker_observed_at=1.0,
        storage_reason_codes=["retention_guard_active", "capacity_ok"],
    )
    normalized = diagnostics.normalize_worker_technical_source(
        wire,
        worker_state="offline",
        observed_at=995.0,
    )

    assert normalized is not None
    assert set(normalized) == diagnostics.SOURCE_FIELDS
    assert normalized["worker_state"] == "offline"
    assert normalized["worker_observed_at"] == 995.0
    assert normalized["build_sha256"] == "sha256:" + "b" * 64
    assert normalized["config_sha256"] == "sha256:" + "c" * 64
    assert normalized["storage_reason_codes"] == [
        "capacity_ok",
        "retention_guard_active",
    ]
    wire["storage_reason_codes"].append("late_mutation")
    assert "late_mutation" not in normalized["storage_reason_codes"]


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: value.pop("cache_status"),
        lambda value: value.update({"credential": "SUPERS3CRETTOKEN"}),
        lambda value: value.update({"worker_version": "SUPERS3CRETTOKEN"}),
        lambda value: value.update({"provider_status": "available"}),
        lambda value: value.update({"queue_depth": 1_000_000_001}),
        lambda value: value.update({"storage_reason_codes": ["bad/reason"]}),
    ],
)
def test_wire_normalizer_rejects_schema_drift_or_unsafe_values(mutator):
    wire = _source()
    mutator(wire)

    assert diagnostics.normalize_worker_technical_source(
        wire,
        worker_state="online",
        observed_at=1_000.0,
    ) is None


@pytest.mark.parametrize(
    ("source", "reason"),
    [
        (None, "diagnostic_source_missing"),
        ([], "diagnostic_source_missing"),
        ({}, "diagnostic_source_contract_invalid"),
        (
            {"contract": "some-other-contract"},
            "diagnostic_source_contract_invalid",
        ),
    ],
)
def test_missing_or_wrong_source_contract_is_explicitly_unknown(source, reason):
    result = diagnostics.build_worker_technical_diagnostics(source, now=1_000.0)

    assert result["status"] == "unknown"
    assert result["projection_reason_codes"] == [reason]
    assert result["worker"]["state"] == "unknown"
    assert result["worker"]["observed_at"] is None
    assert result["jobs"]["queue_depth"] is None
    assert result["storage"]["headroom_bytes"] is None
    assert result["transition"]["next_version"] is None
    assert result["replay"]["algorithm_version"] is None
    assert all(
        item["status"] == "unknown"
        for item in result["freshness"].values()
    )
    assert result["claims"] == {
        "meteorological_truth": False,
        "operational_admission": False,
        "alert": False,
    }


def test_valid_empty_source_never_invents_availability_or_values():
    result = diagnostics.build_worker_technical_diagnostics(
        {"contract": diagnostics.SOURCE_CONTRACT},
        now=1_000.0,
    )

    assert result["status"] == "unknown"
    assert result["projection_reason_codes"] == [
        "worker_state_unknown",
        "worker_observation_unknown",
    ]
    assert result["worker"]["state"] == "unknown"
    assert result["worker"]["age_seconds"] is None
    for group in ("jobs", "storage", "transition", "replay"):
        assert result[group]["status"] == "unknown"


@pytest.mark.parametrize(
    "now",
    [-1, float("nan"), float("inf"), True, "1", 10**100],
)
def test_now_must_be_a_finite_non_negative_timestamp(now):
    with pytest.raises(ValueError, match="now must be"):
        diagnostics.build_worker_technical_diagnostics(_source(), now=now)


def test_stale_or_non_live_worker_marks_every_known_group_stale():
    stale = diagnostics.build_worker_technical_diagnostics(
        _source(worker_observed_at=900.0),
        now=1_000.0,
    )
    assert stale["status"] == "stale"
    assert stale["projection_reason_codes"] == ["worker_heartbeat_stale"]
    assert stale["worker"]["age_seconds"] == 100.0
    assert all(
        stale[group]["status"] == "stale"
        for group in ("jobs", "storage", "transition", "replay")
    )
    assert all(
        item["status"] == "stale"
        for item in stale["freshness"].values()
    )

    offline = diagnostics.build_worker_technical_diagnostics(
        _source(worker_state="offline", worker_observed_at=999.0),
        now=1_000.0,
    )
    assert offline["status"] == "stale"
    assert offline["projection_reason_codes"] == ["worker_state_not_live"]


def test_unknown_or_invalid_heartbeat_never_claims_current_or_stale_data():
    unknown_state = diagnostics.build_worker_technical_diagnostics(
        _source(worker_state="invented", worker_observed_at=999.0),
        now=1_000.0,
    )
    assert unknown_state["status"] == "unknown"
    assert unknown_state["projection_reason_codes"] == [
        "worker_state_unknown"
    ]
    assert unknown_state["jobs"]["status"] == "unknown"
    assert unknown_state["freshness"]["provider"]["status"] == "unknown"

    future = diagnostics.build_worker_technical_diagnostics(
        _source(worker_observed_at=1_006.0),
        now=1_000.0,
    )
    assert future["status"] == "unknown"
    assert future["projection_reason_codes"] == [
        "worker_observation_invalid"
    ]
    assert future["worker"]["observed_at"] is None
    assert future["worker"]["age_seconds"] is None


def test_invalid_numbers_identifiers_and_partial_inputs_remain_unknown():
    result = diagnostics.build_worker_technical_diagnostics(
        _source(
            worker_version="SUPERS3CRETTOKEN",
            build_sha256="not-a-content-address",
            config_sha256="a" * 63,
            desired_state_sha256="sha256:" + "z" * 64,
            provider_age_seconds=float("nan"),
            frame_age_seconds=float("inf"),
            cache_age_seconds=10**100,
            queue_depth=True,
            terminal_jobs=-1,
            failed_jobs="2",
            retention_alarm="red",
            capacity_alarm=object(),
            headroom_bytes=-1,
            headroom_percent=10**100,
            storage_reason_codes=None,
            transition_status="ready",
            current_version="v" + "1" * 10_000,
            next_version="SUPERS3CRETTOKEN",
            transition_reason_codes="not-an-array",
            replay_contract_version="secret",
            replay_algorithm_version="also_secret",
        ),
        now=1_000.0,
    )

    assert result["status"] == "current"
    assert result["worker"]["version"] is None
    assert result["worker"]["build_sha256"] is None
    assert result["worker"]["config_sha256"] is None
    assert result["worker"]["desired_state_sha256"] is None
    assert all(
        value == {"status": "unknown", "age_seconds": None}
        for value in result["freshness"].values()
    )
    assert result["jobs"] == {
        "status": "unknown",
        "queue_depth": None,
        "terminal_jobs": None,
        "failed_jobs": None,
        "counts_saturated": False,
    }
    assert result["storage"] == {
        "status": "unknown",
        "retention_alarm": "unknown",
        "capacity_alarm": "unknown",
        "headroom_bytes": None,
        "headroom_percent": None,
        "reason_codes": [],
        "reason_codes_complete": False,
    }
    assert result["transition"]["status"] == "unknown"
    assert result["transition"]["phase"] == "unknown"
    assert result["transition"]["current_version"] is None
    assert result["transition"]["next_version"] is None
    assert result["replay"]["status"] == "unknown"
    assert b"NaN" not in diagnostics.canonical_diagnostics_bytes(result)
    assert b"Infinity" not in diagnostics.canonical_diagnostics_bytes(result)


def test_counters_are_saturated_without_unbounded_output_growth():
    result = diagnostics.build_worker_technical_diagnostics(
        _source(
            queue_depth=diagnostics.MAX_COUNTER + 1,
            terminal_jobs=10**100,
            failed_jobs=diagnostics.MAX_COUNTER,
        ),
        now=1_000.0,
    )

    assert result["jobs"]["queue_depth"] == diagnostics.MAX_COUNTER
    assert result["jobs"]["terminal_jobs"] == diagnostics.MAX_COUNTER
    assert result["jobs"]["failed_jobs"] == diagnostics.MAX_COUNTER
    assert result["jobs"]["counts_saturated"] is True
    assert len(diagnostics.canonical_diagnostics_bytes(result)) <= (
        diagnostics.MAX_DIAGNOSTIC_BYTES
    )


class _EndlessReasonCodes:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.yielded = 0

    def __iter__(self):
        while True:
            self.yielded += 1
            yield f"{self.prefix}_{self.yielded:03d}"


def test_reason_code_projection_reads_seventeen_items_and_emits_sixteen():
    storage = _EndlessReasonCodes("storage_reason")
    transition = _EndlessReasonCodes("transition_reason")
    result = diagnostics.build_worker_technical_diagnostics(
        _source(
            storage_reason_codes=storage,
            transition_reason_codes=transition,
        ),
        now=1_000.0,
    )

    assert storage.yielded == diagnostics.MAX_REASON_CODES + 1
    assert transition.yielded == diagnostics.MAX_REASON_CODES + 1
    assert len(result["storage"]["reason_codes"]) == 16
    assert len(result["transition"]["reason_codes"]) == 16
    assert result["storage"]["reason_codes_complete"] is False
    assert result["transition"]["reason_codes_complete"] is False
    assert len(diagnostics.canonical_diagnostics_bytes(result)) <= (
        diagnostics.MAX_DIAGNOSTIC_BYTES
    )


def test_invalid_and_duplicate_reason_codes_are_dropped_and_marked_incomplete():
    result = diagnostics.build_worker_technical_diagnostics(
        _source(
            storage_reason_codes=[
                "capacity_ok",
                "capacity_ok",
                "SUPERS3CRETTOKEN",
                "contains spaces",
                "x" * 10_000,
                7,
            ],
            transition_reason_codes=["restart_required", "bad/reason"],
        ),
        now=1_000.0,
    )

    assert result["storage"]["reason_codes"] == ["capacity_ok"]
    assert result["storage"]["reason_codes_complete"] is False
    assert result["transition"]["reason_codes"] == ["restart_required"]
    assert result["transition"]["reason_codes_complete"] is False


class _NoIterationMapping(dict):
    def __init__(self, values):
        super().__init__()
        self._values = values
        self.get_calls = 0

    def get(self, key, default=None):
        self.get_calls += 1
        return self._values.get(key, default)

    def __iter__(self):
        raise AssertionError("source mapping must never be iterated")

    def __len__(self):
        raise AssertionError("source mapping size must never be read")


def test_source_projection_uses_a_fixed_number_of_named_reads():
    source = _NoIterationMapping(_source(unknown_nested={"items": range(10**9)}))

    result = diagnostics.build_worker_technical_diagnostics(
        source,
        now=1_000.0,
    )

    assert result["status"] == "current"
    assert source.get_calls == 27


def test_unknown_secret_shaped_fields_and_values_are_never_projected():
    sentinel = "SUPERS3CRETTOKEN-DO-NOT-LEAK"
    source = _source(
        worker_version=sentinel,
        current_version=sentinel,
        next_version=sentinel,
        replay_contract_version=sentinel,
        replay_algorithm_version=sentinel,
        storage_reason_codes=[sentinel, "capacity_ok"],
        transition_reason_codes=[sentinel, "restart_required"],
    )
    source.update(
        {
            "provider_payload": {"credential": sentinel},
            "credential": sentinel,
            "password": sentinel,
            "token": sentinel,
            "filesystem_path": f"C:/private/{sentinel}",
            "mqtt_topic": f"private/{sentinel}",
            "raw_payload": sentinel,
            "provider_name": sentinel,
            "url": f"https://user:{sentinel}@example.invalid",
        }
    )

    result = diagnostics.build_worker_technical_diagnostics(
        source,
        now=1_000.0,
    )
    wire = diagnostics.canonical_diagnostics_bytes(result).decode("ascii")
    lowered = wire.lower()

    assert sentinel not in wire
    for forbidden in (
        "credential",
        "password",
        "token",
        "filesystem_path",
        "mqtt_topic",
        "raw_payload",
        "provider_name",
        "example.invalid",
    ):
        assert forbidden not in lowered
    assert result["storage"]["reason_codes"] == ["capacity_ok"]
    assert result["transition"]["reason_codes"] == ["restart_required"]


def test_canonical_wire_is_deterministic_ascii_and_json_safe():
    source = _source()
    reverse_source = dict(reversed(tuple(source.items())))
    first = diagnostics.build_worker_technical_diagnostics(source, now=1_000.0)
    second = diagnostics.build_worker_technical_diagnostics(
        reverse_source,
        now=1_000.0,
    )

    first_wire = diagnostics.canonical_diagnostics_bytes(first)
    second_wire = diagnostics.canonical_diagnostics_bytes(second)
    assert first_wire == second_wire
    assert b" " not in first_wire
    assert b"\n" not in first_wire
    assert json.loads(first_wire) == first
    assert all(byte < 128 for byte in first_wire)
    assert not any(
        isinstance(value, float) and not math.isfinite(value)
        for group in first.values()
        if isinstance(group, dict)
        for value in group.values()
    )


def test_module_is_a_pure_projection_without_runtime_or_io_dependencies():
    source = MODULE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots <= {
        "__future__",
        "collections",
        "itertools",
        "json",
        "math",
        "re",
        "typing",
    }
    assert imported_roots.isdisjoint(
        {
            "homeassistant",
            "os",
            "pathlib",
            "paho",
            "requests",
            "socket",
            "sqlite3",
            "storm_tracker_worker",
            "urllib",
        }
    )
    forbidden_calls = {
        "call_api",
        "connect",
        "fetch",
        "open",
        "publish",
        "read_text",
        "subscribe",
        "write_text",
    }
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert forbidden_calls.isdisjoint(called_names | called_attributes)
