from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from storm_tracker_worker import worker_first_service_activation as activation
from storm_tracker_worker.blitzortung_event_ingress import (
    BlitzortungEventIngressLimits,
)
from storm_tracker_worker.raw_artifacts import RawArtifactLimits
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityProfile,
)
from storm_tracker_worker.worker_first_service_composition import (
    MAX_CONFIG_BYTES as SERVICE_COMPOSITION_MAX_CONFIG_BYTES,
    WorkerFirstServiceCanaryConfig,
    WorkerFirstServiceCompositionConfig,
    WorkerFirstServiceSourcePaths,
)
from test_worker_first_composition import _config as _worker_first_config


AUTHORITY_SHA256 = "a" * 64
_SUBPROCESS_ENV_KEYS = frozenset({
    "COMSPEC", "HOME", "LOCALAPPDATA", "PATH", "PATHEXT",
    "SYSTEMDRIVE", "SYSTEMROOT", "TEMP", "TMP", "TMPDIR",
    "USERPROFILE", "WINDIR",
})


def _service_config(root: Path) -> WorkerFirstServiceCanaryConfig:
    root = root.resolve()
    capacity = RawCapacityProfile(
        global_max_bytes=128 * 1024 * 1024,
        global_max_entries=1024,
        providers=(ProviderCapacityLimit(
            provider="dwd",
            max_bytes=64 * 1024 * 1024,
            max_entries=512,
        ),),
        max_reservations=4096,
        max_catalog_bytes=4 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )
    return WorkerFirstServiceCanaryConfig(
        worker_first=_worker_first_config(root / "pipeline"),
        source_paths=WorkerFirstServiceSourcePaths(
            scheduler_db=root / "source" / "scheduler.sqlite3",
            capacity_ledger_db=root / "source" / "capacity.sqlite3",
            execution_receipts_db=root / "source" / "executions.sqlite3",
            raw_store_root=root / "source" / "raw",
        ),
        capacity_profile=capacity,
        raw_store_limits=RawArtifactLimits(
            max_object_bytes=8 * 1024 * 1024,
            max_total_bytes=128 * 1024 * 1024,
            max_object_entries=1024,
            max_staging_entries=16,
            max_receipt_entries=4096,
            max_capture_entries=4096,
            max_capture_members=16,
            max_catalog_bytes=4 * 1024 * 1024,
        ),
    )


def _v1_service_config(root: Path) -> WorkerFirstServiceCompositionConfig:
    canary = _service_config(root)
    return WorkerFirstServiceCompositionConfig(
        worker_first=canary.worker_first,
        source_paths=canary.source_paths,
        capacity_profile=canary.capacity_profile,
        raw_store_limits=canary.raw_store_limits,
        acquisition_timeout_seconds=canary.acquisition_timeout_seconds,
        bridge_timeout_seconds=canary.bridge_timeout_seconds,
        stop_timeout_seconds=canary.stop_timeout_seconds,
    )


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _write_config(
    root: Path,
    payload: bytes,
) -> tuple[Path, dict[str, str]]:
    path = (root / "input" / "worker-first.json").resolve()
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    return path, {
        activation.RUNTIME_MODE_ENV: activation.WORKER_FIRST_CANARY_MODE,
        activation.CONFIG_PATH_ENV: str(path),
        activation.CONFIG_SHA256_ENV: hashlib.sha256(payload).hexdigest(),
        activation.AUTHORITY_SHA256_ENV: AUTHORITY_SHA256,
    }


def test_absent_or_explicit_legacy_mode_is_inert_and_reads_no_config(
    monkeypatch,
) -> None:
    def forbidden_read(_path: Path) -> bytes:
        raise AssertionError("legacy mode attempted to read a canary config")

    monkeypatch.setattr(activation, "_read_config_bytes", forbidden_read)
    for environment in ({}, {activation.RUNTIME_MODE_ENV: activation.LEGACY_MODE}):
        plan = activation.load_worker_first_service_activation(environment)
        assert plan.runtime_mode == activation.LEGACY_MODE
        assert plan.worker_first_selected is False
        assert plan.config_path is None
        assert plan.config_sha256 is None
        assert plan.authority_sha256 is None
        assert plan.composition_config is None
        assert plan.to_mapping() == {
            "activation_performed": False,
            "authority_sha256": None,
            "config_loaded": False,
            "config_path": None,
            "config_sha256": None,
            "contract": activation.ACTIVATION_CONTRACT,
            "runtime_mode": activation.LEGACY_MODE,
            "worker_first_selected": False,
        }


def test_fresh_legacy_import_does_not_load_worker_first_composition_graph(
) -> None:
    script = """
import json
import sys

from storm_tracker_worker import worker_first_service_activation as activation

plan = activation.load_worker_first_service_activation({})
worker_modules = sorted(
    name
    for name in sys.modules
    if name.startswith("storm_tracker_worker.")
)
print(json.dumps({
    "config_bytes": activation.MAX_ACTIVATION_CONFIG_BYTES,
    "mode": plan.runtime_mode,
    "worker_modules": worker_modules,
}, sort_keys=True))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        env={
            **{
                key: value for key, value in os.environ.items()
                if key in _SUBPROCESS_ENV_KEYS
            },
            "PYTHONPATH": os.pathsep.join((
                str(Path(__file__).resolve().parents[1] / ".testdeps"),
                str(Path(__file__).resolve().parents[1]),
            )),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
        },
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "config_bytes": 384 * 1024,
        "mode": activation.LEGACY_MODE,
        "worker_modules": [
            "storm_tracker_worker.worker_first_service_activation",
        ],
    }


@pytest.mark.parametrize("mode", ("", " ", "worker_first", "LEGACY", 1, None))
def test_provided_blank_whitespace_or_unknown_runtime_mode_fails(mode) -> None:
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^runtime_mode_invalid$",
    ):
        activation.load_worker_first_service_activation({
            activation.RUNTIME_MODE_ENV: mode,
        })


@pytest.mark.parametrize(
    "key,value",
    (
        (activation.CONFIG_PATH_ENV, "C:\\unused\\config.json"),
        (activation.CONFIG_SHA256_ENV, "b" * 64),
        (activation.AUTHORITY_SHA256_ENV, "c" * 64),
    ),
)
def test_legacy_rejects_stray_canary_pins(key: str, value: str) -> None:
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^legacy_canary_configuration_forbidden$",
    ):
        activation.load_worker_first_service_activation({key: value})


def test_raw_capture_mode_is_distinct_and_requires_the_same_three_pins(
    tmp_path: Path,
    monkeypatch,
) -> None:
    payload = b"{}"
    path = (tmp_path / "raw-capture.json").resolve()
    path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    environment = {
        activation.RUNTIME_MODE_ENV: activation.RAW_CAPTURE_SHADOW_MODE,
        activation.CONFIG_PATH_ENV: str(path),
        activation.CONFIG_SHA256_ENV: digest,
        activation.AUTHORITY_SHA256_ENV: AUTHORITY_SHA256,
    }
    for missing, error_code in (
        (activation.CONFIG_PATH_ENV, "config_path_required"),
        (activation.CONFIG_SHA256_ENV, "config_sha256_required"),
        (activation.AUTHORITY_SHA256_ENV, "authority_sha256_required"),
    ):
        incomplete = dict(environment)
        incomplete.pop(missing)
        with pytest.raises(
            activation.WorkerFirstServiceActivationError,
            match=f"^{error_code}$",
        ):
            activation.load_worker_first_service_activation(incomplete)

    typed = SimpleNamespace(fingerprint=digest)
    monkeypatch.setattr(
        activation,
        "_parse_raw_capture_config",
        lambda value: typed if value == {} else None,
    )
    plan = activation.load_worker_first_service_activation(environment)

    assert plan.runtime_mode == activation.RAW_CAPTURE_SHADOW_MODE
    assert plan.raw_capture_selected is True
    assert plan.worker_first_selected is False
    assert plan.isolated_runtime_selected is True
    assert plan.composition_config is typed
    assert plan.to_mapping()["raw_capture_selected"] is True


def test_worker_only_mode_reuses_pinned_all_source_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    payload = b"{}"
    path = (tmp_path / "worker-only.json").resolve()
    path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    typed = SimpleNamespace(fingerprint=digest)
    monkeypatch.setattr(
        activation,
        "_parse_raw_capture_config",
        lambda value: typed if value == {} else None,
    )

    plan = activation.load_worker_first_service_activation({
        activation.RUNTIME_MODE_ENV: activation.WORKER_ONLY_MODE,
        activation.CONFIG_PATH_ENV: str(path),
        activation.CONFIG_SHA256_ENV: digest,
        activation.AUTHORITY_SHA256_ENV: AUTHORITY_SHA256,
    })

    assert plan.runtime_mode == activation.WORKER_ONLY_MODE
    assert plan.worker_only_selected is True
    assert plan.raw_capture_selected is False
    assert plan.worker_first_selected is False
    assert plan.isolated_runtime_selected is True
    assert plan.composition_config is typed
    assert plan.to_mapping()["worker_only_selected"] is True


def test_valid_raw_capture_config_returns_its_distinct_typed_plan(
    tmp_path: Path,
) -> None:
    from storm_tracker_worker.raw_capture_service_composition import (
        RawCaptureServiceConfig,
    )

    canary = _service_config(tmp_path / "raw-configured")
    event_limits = BlitzortungEventIngressLimits()
    config = RawCaptureServiceConfig(
        worker_first=canary.worker_first,
        source_paths=canary.source_paths,
        capacity_profile=canary.capacity_profile,
        raw_store_limits=canary.raw_store_limits,
        blitzortung_control_root=(
            tmp_path / "raw-configured" / "source" / "blitzortung-control"
        ).resolve(),
        blitzortung_raw_store_root=(
            tmp_path / "raw-configured" / "source" / "blitzortung-raw"
        ).resolve(),
        blitzortung_raw_store_limits=RawArtifactLimits(),
        blitzortung_ingress_limits=event_limits,
        blitzortung_enabled=True,
        host_operational_free_reserve_bytes=2 * 1024 * 1024 * 1024,
    )
    payload = _canonical(config.to_mapping())
    path = (tmp_path / "input" / "raw-capture.json").resolve()
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    environment = {
        activation.RUNTIME_MODE_ENV: activation.RAW_CAPTURE_SHADOW_MODE,
        activation.CONFIG_PATH_ENV: str(path),
        activation.CONFIG_SHA256_ENV: hashlib.sha256(payload).hexdigest(),
        activation.AUTHORITY_SHA256_ENV: AUTHORITY_SHA256,
    }
    before = tuple(sorted(
        item.relative_to(tmp_path).as_posix()
        for item in tmp_path.rglob("*")
        if item.is_file()
    ))

    plan = activation.load_worker_first_service_activation(environment)

    assert plan.runtime_mode == activation.RAW_CAPTURE_SHADOW_MODE
    assert isinstance(plan.composition_config, RawCaptureServiceConfig)
    assert plan.composition_config == config
    assert plan.config_sha256 == config.fingerprint
    assert plan.authority_sha256 == AUTHORITY_SHA256
    assert tuple(sorted(
        item.relative_to(tmp_path).as_posix()
        for item in tmp_path.rglob("*")
        if item.is_file()
    )) == before


@pytest.mark.parametrize(
    "missing,error_code",
    (
        (activation.CONFIG_PATH_ENV, "config_path_required"),
        (activation.CONFIG_SHA256_ENV, "config_sha256_required"),
        (activation.AUTHORITY_SHA256_ENV, "authority_sha256_required"),
    ),
)
def test_canary_requires_all_three_independent_pins(
    tmp_path: Path,
    missing: str,
    error_code: str,
) -> None:
    path = (tmp_path / "config.json").resolve()
    path.write_bytes(b"{}")
    environment = {
        activation.RUNTIME_MODE_ENV: activation.WORKER_FIRST_CANARY_MODE,
        activation.CONFIG_PATH_ENV: str(path),
        activation.CONFIG_SHA256_ENV: hashlib.sha256(b"{}").hexdigest(),
        activation.AUTHORITY_SHA256_ENV: AUTHORITY_SHA256,
    }
    environment.pop(missing)
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match=f"^{error_code}$",
    ):
        activation.load_worker_first_service_activation(environment)


def test_valid_canary_returns_typed_read_only_plan_without_creating_state(
    tmp_path: Path,
) -> None:
    config = _service_config(tmp_path / "configured")
    payload = _canonical(config.to_mapping())
    path, environment = _write_config(tmp_path, payload)
    before_files = tuple(sorted(
        item.relative_to(tmp_path).as_posix()
        for item in tmp_path.rglob("*")
        if item.is_file()
    ))

    plan = activation.load_worker_first_service_activation(environment)

    assert plan.runtime_mode == activation.WORKER_FIRST_CANARY_MODE
    assert plan.worker_first_selected is True
    assert plan.config_path == path
    assert plan.config_sha256 == config.fingerprint
    assert plan.authority_sha256 == AUTHORITY_SHA256
    assert isinstance(
        plan.composition_config,
        WorkerFirstServiceCanaryConfig,
    )
    assert plan.composition_config.to_mapping() == config.to_mapping()
    assert path.read_bytes() == payload
    assert not config.worker_first.paths.authority_file.exists()
    assert tuple(sorted(
        item.relative_to(tmp_path).as_posix()
        for item in tmp_path.rglob("*")
        if item.is_file()
    )) == before_files
    assert plan.to_mapping()["activation_performed"] is False


def test_config_path_and_both_hash_pins_fail_closed(tmp_path: Path) -> None:
    config = _service_config(tmp_path / "configured")
    payload = _canonical(config.to_mapping())
    path, environment = _write_config(tmp_path, payload)

    cases = (
        (
            {**environment, activation.CONFIG_PATH_ENV: path.name},
            "config_path_invalid",
        ),
        (
            {**environment, activation.CONFIG_PATH_ENV: str(path.parent / "missing")},
            "config_path_unavailable",
        ),
        (
            {**environment, activation.CONFIG_SHA256_ENV: "A" * 64},
            "config_sha256_invalid",
        ),
        (
            {**environment, activation.CONFIG_SHA256_ENV: "b" * 64},
            "config_sha256_mismatch",
        ),
        (
            {**environment, activation.AUTHORITY_SHA256_ENV: "bad"},
            "authority_sha256_invalid",
        ),
    )
    for changed, error_code in cases:
        with pytest.raises(
            activation.WorkerFirstServiceActivationError,
            match=f"^{error_code}$",
        ):
            activation.load_worker_first_service_activation(changed)


def test_v1_off_by_default_service_config_is_not_a_canary_authority(
    tmp_path: Path,
) -> None:
    config = _v1_service_config(tmp_path / "configured-v1")
    payload = _canonical(config.to_mapping())
    path, environment = _write_config(tmp_path, payload)

    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^service_config_schema_invalid$",
    ):
        activation.load_worker_first_service_activation(environment)

    assert path.read_bytes() == payload


@pytest.mark.parametrize("alias_kind", ("dot_segment", "trailing_separator"))
def test_config_path_must_be_lexically_identical_to_resolved_path(
    tmp_path: Path,
    alias_kind: str,
) -> None:
    config = _service_config(tmp_path / "configured")
    payload = _canonical(config.to_mapping())
    path, environment = _write_config(tmp_path, payload)
    if alias_kind == "dot_segment":
        detour = path.parent / "detour"
        detour.mkdir()
        alias = str(detour / ".." / path.name)
    else:
        alias = str(path) + os.sep
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^config_path_not_canonical$",
    ):
        activation.load_worker_first_service_activation({
            **environment,
            activation.CONFIG_PATH_ENV: alias,
        })


def test_config_entry_swap_after_read_is_detected_hermetically(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = (tmp_path / "config.json").resolve()
    replacement = (tmp_path / "replacement.json").resolve()
    moved = (tmp_path / "moved.json").resolve()
    path.write_bytes(b'{"value":1}')
    replacement.write_bytes(b'{"value":2}')
    original_lstat = Path.lstat
    target_lstats = 0

    def swapping_lstat(candidate: Path):
        nonlocal target_lstats
        if candidate == path:
            target_lstats += 1
            if target_lstats == 2:
                path.replace(moved)
                replacement.replace(path)
        return original_lstat(candidate)

    monkeypatch.setattr(Path, "lstat", swapping_lstat)
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^config_file_changed$",
    ):
        activation._read_config_bytes(path)
    assert target_lstats == 2


def test_config_parent_swap_after_read_is_detected_hermetically(
    tmp_path: Path,
    monkeypatch,
) -> None:
    parent = (tmp_path / "input").resolve()
    replacement_parent = (tmp_path / "replacement-input").resolve()
    moved_parent = (tmp_path / "moved-input").resolve()
    parent.mkdir()
    replacement_parent.mkdir()
    path = parent / "config.json"
    path.write_bytes(b'{"value":1}')
    (replacement_parent / path.name).write_bytes(b'{"value":1}')
    original_lstat = Path.lstat
    parent_lstats = 0

    def swapping_lstat(candidate: Path):
        nonlocal parent_lstats
        if candidate == parent:
            parent_lstats += 1
            if parent_lstats == 2:
                parent.replace(moved_parent)
                replacement_parent.replace(parent)
        return original_lstat(candidate)

    monkeypatch.setattr(Path, "lstat", swapping_lstat)
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^config_file_changed$",
    ):
        activation._read_config_bytes(path)
    assert parent_lstats == 2


def test_duplicate_noncanonical_oversized_and_typed_drift_are_rejected(
    tmp_path: Path,
) -> None:
    config = _service_config(tmp_path / "configured")
    mapping = config.to_mapping()
    drifted = {**mapping, "unexpected": True}
    payloads = (
        (b'{"mode":"a","mode":"b"}', "config_json_duplicate_key"),
        (
            json.dumps(mapping, indent=2, sort_keys=True).encode("ascii"),
            "config_json_not_canonical",
        ),
        (
            b" " * (activation.MAX_ACTIVATION_CONFIG_BYTES + 1),
            "config_size_invalid",
        ),
        (_canonical(drifted), "service_config_schema_invalid"),
    )
    for index, (payload, error_code) in enumerate(payloads):
        path, environment = _write_config(tmp_path / str(index), payload)
        with pytest.raises(
            activation.WorkerFirstServiceActivationError,
            match=f"^{error_code}$",
        ):
            activation.load_worker_first_service_activation(environment)
        assert path.read_bytes() == payload


def _nested_json(depth: int) -> object:
    value: object = None
    for _ in range(depth):
        value = [value]
    return {"value": value}


@pytest.mark.parametrize(
    "value,error_code",
    (
        (
            _nested_json(activation.MAX_JSON_DEPTH),
            "config_json_resource_limit",
        ),
        (
            {"value": [None] * activation.MAX_JSON_NODES},
            "config_json_resource_limit",
        ),
        (
            {"value": "x" * (activation.MAX_JSON_STRING_CHARS + 1)},
            "config_json_resource_limit",
        ),
        (
            {"k" * (activation.MAX_JSON_KEY_CHARS + 1): None},
            "config_json_resource_limit",
        ),
    ),
)
def test_json_shape_resource_limits_fail_with_one_stable_error(
    value: object,
    error_code: str,
) -> None:
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match=f"^{error_code}$",
    ):
        activation._decode_canonical_json(_canonical(value))


@pytest.mark.parametrize(
    "payload",
    (
        b'{"value":123456789012345678901}',
        b'{"value":1e2}',
        b'{"value":1e999999}',
        b'{"value":NaN}',
        b'{"value":Infinity}',
    ),
)
def test_integer_exponent_and_nonfinite_numbers_fail_with_stable_error(
    payload: bytes,
) -> None:
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^config_json_number_invalid$",
    ):
        activation._decode_canonical_json(payload)


def test_bounded_canonical_float_and_arithmetic_error_contract(
    monkeypatch,
) -> None:
    assert activation._decode_canonical_json(
        b'{"value":1e-06}'
    ) == {"value": 1e-06}
    oversized_float = (
        b'{"value":0.'
        + b"1" * activation.MAX_JSON_FLOAT_CHARS
        + b'}'
    )
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^config_json_number_invalid$",
    ):
        activation._decode_canonical_json(oversized_float)

    def overflow(*_args, **_kwargs):
        raise OverflowError("host-specific detail")

    monkeypatch.setattr(activation.json, "loads", overflow)
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^config_json_invalid$",
    ):
        activation._decode_canonical_json(b"{}")


def test_service_config_parser_reuses_typed_canary_parser(
    tmp_path: Path,
) -> None:
    config = _service_config(tmp_path / "configured")
    mapping = config.to_mapping()
    mapping["worker_first"]["unexpected"] = True
    payload = _canonical(mapping)
    _path, environment = _write_config(tmp_path, payload)
    with pytest.raises(
        activation.WorkerFirstServiceActivationError,
        match="^service_config_invalid$",
    ):
        activation.load_worker_first_service_activation(environment)


def test_product_module_contains_no_activation_or_write_primitives() -> None:
    source = Path(activation.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "build_worker_first_service_composition",
        "build_worker_first_service_canary",
        "WorkerFirstOwnershipFence",
        "OperationalSnapshotStore(",
        "mkdir(",
        "write_bytes(",
        "write_text(",
        "open(\"w",
        "open('w",
        "threading",
        "paho",
    ):
        assert forbidden not in source
    assert "    WorkerFirstServiceCompositionConfig,\n" not in source
    assert "WorkerFirstServiceCompositionConfig(" not in source
    assert "WorkerFirstServiceCanaryConfig.from_mapping" in source
    assert (
        activation.MAX_ACTIVATION_CONFIG_BYTES
        == SERVICE_COMPOSITION_MAX_CONFIG_BYTES
        == 384 * 1024
    )
