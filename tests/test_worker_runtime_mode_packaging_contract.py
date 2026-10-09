from __future__ import annotations

import ast
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "storm_tracker_worker"
RUN_PATH = WORKER / "run.sh"
CONFIG_PATH = WORKER / "config.yaml"
COMPOSE_PATH = WORKER / "compose.yaml"
ENV_EXAMPLE_PATH = WORKER / ".env.example"
DOCKERIGNORE_PATH = WORKER / ".dockerignore"
ACTIVATION_PATH = WORKER / "worker_first_service_activation.py"
SERVICE_PATH = WORKER / "service.py"
GITIGNORE_PATH = ROOT / ".gitignore"
GITATTRIBUTES_PATH = ROOT / ".gitattributes"

RUNTIME_ENV = {
    "runtime_mode": "STV3_RUNTIME_MODE",
    "worker_first_config_path": "STV3_WORKER_FIRST_CONFIG_PATH",
    "worker_first_config_sha256": "STV3_WORKER_FIRST_CONFIG_SHA256",
    "worker_first_authority_sha256": (
        "STV3_WORKER_FIRST_AUTHORITY_SHA256"
    ),
}
PIN_ENVS = tuple(
    value for key, value in RUNTIME_ENV.items() if key != "runtime_mode"
)
CANARY_UNUSED_SECRET_ENVS = (
    "STV3_AEMET_API_KEY",
    "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY",
    "STV3_EUMETSAT_CONSUMER_KEY",
    "STV3_EUMETSAT_CONSUMER_SECRET",
    "STV3_HSAF_PASSWORD",
    "STV3_HSAF_USERNAME",
    "STV3_JOB_RECEIPT_DB",
    "STV3_KNMI_API_KEY",
    "STV3_METEOFRANCE_API_TOKEN",
    "STV3_METEOFRANCE_APPLICATION_ID",
    "STV3_NETATMO_CLIENT_ID",
    "STV3_NETATMO_CLIENT_SECRET",
    "STV3_NETATMO_REFRESH_TOKEN",
    "STV3_NETATMO_STATION_ID",
    "STV3_NETATMO_TOKEN_STATE",
    "STV3_OPERA_RAW_RETENTION_AUTHORITY",
    "STV3_WUNDERGROUND_API_KEY",
)
SUBPROCESS_ENV_ALLOWLIST = frozenset({
    "COMSPEC",
    "HOME",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "TMPDIR",
    "USERPROFILE",
    "WINDIR",
})


def _bash_path() -> str | None:
    discovered = shutil.which("bash")
    if discovered is not None:
        return discovered
    windows_git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
    return str(windows_git_bash) if windows_git_bash.is_file() else None


def _runtime_gate_script(*, invoke_service: bool = False) -> str:
    source = RUN_PATH.read_text(encoding="utf-8")
    gate_end = source.index("# This release owns exactly two")
    prefix = source[:gate_end]
    prelude = "function bashio::log.fatal { :; }\n"
    if invoke_service:
        suffix = (
            'exec "${STV3_TEST_PYTHON}" -B '
            "-m storm_tracker_worker.service\n"
        )
    else:
        suffix = """
{
  printf 'mode=%s\\n' "${STV3_RUNTIME_MODE}"
  for name in \\
    STV3_WORKER_FIRST_CONFIG_PATH \\
    STV3_WORKER_FIRST_CONFIG_SHA256 \\
    STV3_WORKER_FIRST_AUTHORITY_SHA256; do
    if [[ -v "${name}" ]]; then
      printf '%s=set\\n' "${name}"
    else
      printf '%s=unset\\n' "${name}"
    fi
  done
  if env | grep -Eq '^STV3_(AEMET|BLITZORTUNG|EUMETSAT|HSAF|JOB_RECEIPT|KNMI|METEOFRANCE|NETATMO|OPERA|WUNDERGROUND)'; then
    printf 'canary_unused_environment=present\n'
  else
    printf 'canary_unused_environment=absent\n'
  fi
} > "${STV3_TEST_RESULT}"
"""
    return prelude + prefix + suffix


def _run_gate(
    tmp_path: Path,
    *,
    mode: str | None,
    pins: tuple[str, ...] = (),
    invoke_service: bool = False,
) -> subprocess.CompletedProcess[str]:
    bash = _bash_path()
    if bash is None:
        pytest.skip("bash is niet beschikbaar op deze host")
    script = tmp_path / "runtime-gate.sh"
    script.write_text(
        _runtime_gate_script(invoke_service=invoke_service),
        encoding="utf-8",
        newline="\n",
    )
    environment = {
        key: value
        for key, value in os.environ.items()
        if key in SUBPROCESS_ENV_ALLOWLIST
    }
    environment.update({
        "STV3_MQTT_HOST": "offline.invalid",
        "STV3_TEST_PYTHON": sys.executable,
        "STV3_TEST_RESULT": "gate-result.txt",
        "PYTHONPATH": os.pathsep.join((
            str(ROOT / ".testdeps"),
            str(ROOT),
        )),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
    })
    environment.update({
        key: "secret-sentinel" for key in CANARY_UNUSED_SECRET_ENVS
    })
    if mode is not None:
        environment[RUNTIME_ENV["runtime_mode"]] = mode
    assert len(pins) <= len(PIN_ENVS)
    for index, value in enumerate(pins):
        environment[PIN_ENVS[index]] = value
    return subprocess.run(
        [bash, str(script)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _top_level_mapping(source: str, name: str) -> dict[str, str]:
    matches = tuple(re.finditer(rf"^( *){re.escape(name)}:\n", source, re.M))
    assert len(matches) == 1
    parent_indent = len(matches[0].group(1))
    child_indent = parent_indent + 2
    result: dict[str, str] = {}
    for line in source[matches[0].end():].splitlines():
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent <= parent_indent:
            break
        if indent != child_indent:
            continue
        key, separator, value = line.strip().partition(": ")
        assert separator
        assert key not in result
        result[key] = value
    return result


def _dotenv(source: str) -> dict[str, str]:
    result = {}
    for line in source.splitlines():
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        assert separator and key and key not in result
        result[key] = value
    return result


def _logger_calls(source: str) -> tuple[str, ...]:
    tree = ast.parse(source)
    result = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "_LOGGER"
        ):
            segment = ast.get_source_segment(source, node)
            assert segment is not None
            result.append(segment)
    return tuple(result)


def test_packaging_files_keep_lf_entrypoint_and_option_schema_parity() -> None:
    for path in (
        RUN_PATH,
        CONFIG_PATH,
        COMPOSE_PATH,
        ENV_EXAMPLE_PATH,
        DOCKERIGNORE_PATH,
    ):
        payload = path.read_bytes()
        assert payload.endswith(b"\n")
        assert b"\r" not in payload
    assert RUN_PATH.read_bytes().startswith(
        b"#!/usr/bin/with-contenv bashio\nset -e\n"
    )
    assert "RUN chmod a+x /run.sh" in (WORKER / "Dockerfile").read_text(
        encoding="utf-8"
    )
    assert "!storm_tracker_worker/.env.example\n" in (
        GITIGNORE_PATH.read_text(encoding="utf-8")
    )
    assert GITATTRIBUTES_PATH.read_text(encoding="utf-8").splitlines() == [
        ".gitattributes text eol=lf",
        ".gitignore text eol=lf",
        "storm_tracker_worker/run.sh text eol=lf",
        "storm_tracker_worker/config.yaml text eol=lf",
        "storm_tracker_worker/compose.yaml text eol=lf",
        "storm_tracker_worker/.env.example text eol=lf",
        "storm_tracker_worker/.dockerignore text eol=lf",
    ]

    assert DOCKERIGNORE_PATH.read_text(encoding="utf-8").splitlines() == [
        ".git",
        ".gitignore",
        ".env",
        ".env.*",
        "!.env.example",
        "**/__pycache__/",
        "**/*.py[cod]",
        "**/.pytest_cache/",
        "**/.pytest*/",
        "**/.analysis/",
        "**/*.sqlite3",
        "**/*.sqlite3-*",
        "**/*.db",
        "**/*.db-*",
        "**/*.log",
        "**/*.tmp",
        "**/*.tar",
        "**/*.tar.gz",
        "**/*.zip",
    ]

    config = CONFIG_PATH.read_text(encoding="utf-8")
    options = _top_level_mapping(config, "options")
    schema = _top_level_mapping(config, "schema")
    assert "synoptic_token" not in options
    assert "synoptic_token" not in schema
    assert "synoptic_raw_retention_terms_accepted" not in options
    assert "synoptic_raw_retention_terms_accepted" not in schema
    assert set(options) == set(schema)
    assert {name: options[name] for name in RUNTIME_ENV} == {
        "runtime_mode": '"worker_only"',
        "worker_first_config_path": '""',
        "worker_first_config_sha256": '""',
        "worker_first_authority_sha256": '""',
    }
    assert {name: schema[name] for name in RUNTIME_ENV} == {
        "runtime_mode": (
            '"list(legacy|worker_first_canary|raw_capture_shadow|worker_only|thin_operational)"'
        ),
        "worker_first_config_path": '"str?"',
        "worker_first_config_sha256": '"str?"',
        "worker_first_authority_sha256": '"str?"',
    }
    assert options["blitzortung_raw_retention_authorized"] == "false"
    assert schema["blitzortung_raw_retention_authorized"] == '"bool"'
    assert options["compute_profile"] == '"shared_ha"'
    assert schema["compute_profile"] == (
        '"list(shared_ha|dedicated_nas|dedicated_nas_fast)"'
    )
    assert schema["cpu_limit_percent"] == '"int(25,200)"'
    assert schema["provider_cycle_seconds"] == '"int(2,300)"'

    activation = ACTIVATION_PATH.read_text(encoding="utf-8")
    assert dict(re.findall(
        r'^(LEGACY_MODE|RAW_CAPTURE_SHADOW_MODE|WORKER_FIRST_CANARY_MODE|WORKER_ONLY_MODE)'
        r': Final = "([^"]+)"$',
        activation,
        re.M,
    )) == {
        "LEGACY_MODE": "legacy",
        "RAW_CAPTURE_SHADOW_MODE": "raw_capture_shadow",
        "WORKER_FIRST_CANARY_MODE": "worker_first_canary",
        "WORKER_ONLY_MODE": "worker_only",
    }
    assert "RUNTIME_MODES: Final = frozenset({" in activation
    for constant in (
        "LEGACY_MODE",
        "RAW_CAPTURE_SHADOW_MODE",
        "WORKER_FIRST_CANARY_MODE",
        "WORKER_ONLY_MODE",
    ):
        assert constant in activation


def test_standalone_and_compose_map_the_exact_four_environment_names() -> None:
    runner = RUN_PATH.read_text(encoding="utf-8")
    for option, environment_name in RUNTIME_ENV.items():
        expected = 1 if option == "runtime_mode" else 2
        assert runner.count(f"export {environment_name}\n") == expected
    assert 'STV3_RUNTIME_MODE="${STV3_RUNTIME_MODE-legacy}"' in runner
    assert "${STV3_RUNTIME_MODE:-legacy}" not in runner
    for environment_name in PIN_ENVS:
        assert f'{environment_name}="${{{environment_name}-}}"' in runner

    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    compose_environment = _top_level_mapping(compose, "environment")
    assert compose_environment["STV3_RUNTIME_MODE"] == (
        '"${STV3_RUNTIME_MODE-worker_only}"'
    )
    assert "${STV3_RUNTIME_MODE:-legacy}" not in compose
    for environment_name in PIN_ENVS:
        assert compose_environment[environment_name] == (
            f'"${{{environment_name}-}}"'
        )

    example = _dotenv(ENV_EXAMPLE_PATH.read_text(encoding="utf-8"))
    assert example["STV3_RUNTIME_MODE"] == "worker_only"
    for environment_name in PIN_ENVS:
        assert example[environment_name] == ""


def test_nas_worker_only_bootstrap_is_persistent_and_resource_bounded() -> None:
    runner = RUN_PATH.read_text(encoding="utf-8")
    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    compose_environment = _top_level_mapping(compose, "environment")
    example = _dotenv(ENV_EXAMPLE_PATH.read_text(encoding="utf-8"))
    nas_root = "/share/storm_tracker_worker/runtime/worker_only_production_v1"

    assert runner.count("-m storm_tracker_worker.worker_only_bootstrap") == 1
    assert '--root "${STV3_WORKER_ONLY_ROOT}"' in runner
    assert "/share/storm_tracker_worker/*)" in runner
    assert "/data/*)" in runner
    assert runner.index("worker_only_bootstrap") < runner.index(
        'case "${STV3_RUNTIME_MODE}" in'
    )
    assert runner.index("unset STV3_WORKER_ONLY_ROOT") < runner.index(
        'case "${STV3_RUNTIME_MODE}" in'
    )
    assert compose_environment["STV3_WORKER_ONLY_ROOT"] == (
        f'"${{STV3_WORKER_ONLY_ROOT:-{nas_root}}}"'
    )
    assert example["STV3_WORKER_ONLY_ROOT"] == nas_root
    assert example["STV3_COMPUTE_PROFILE"] == "dedicated_nas"
    assert example["STV3_CPU_LIMIT_PERCENT"] == "40"
    assert example["STV3_HEARTBEAT_SECONDS"] == "30"
    assert example["STV3_PROVIDER_CYCLE_SECONDS"] == "60"
    assert 'cpus: "${STV3_CONTAINER_CPU_LIMIT:-2.0}"' in compose
    assert "cpu_shares: 128" in compose
    assert "pids_limit: 128" in compose
    assert "stop_grace_period: 35s" in compose


def test_home_assistant_reads_new_options_safely_for_old_installations() -> None:
    runner = RUN_PATH.read_text(encoding="utf-8")
    assert "if bashio::config.has_value compute_profile; then" in runner
    assert 'STV3_COMPUTE_PROFILE="$(bashio::config compute_profile)"' in runner
    assert 'STV3_COMPUTE_PROFILE="shared_ha"' in runner
    for option, environment_name in RUNTIME_ENV.items():
        guard = f"if bashio::config.has_value {option}; then"
        read = f'{environment_name}="$(bashio::config {option})"'
        assert runner.count(guard) == 1
        assert runner.count(read) == 1
        assert runner.index(guard) < runner.index(read)
    assert runner.count('STV3_RUNTIME_MODE="worker_only"') == 1
    for environment_name in PIN_ENVS:
        assert runner.count(f'{environment_name}=""') == 1


def test_home_assistant_runtime_modes_include_explicit_reversible_legacy() -> None:
    const_source = (
        ROOT / "custom_components" / "storm_tracker_v3" / "const.py"
    ).read_text(encoding="utf-8")
    assert (
        "RUNTIME_MODES = frozenset({"
        "RUNTIME_MODE_LEGACY, RUNTIME_MODE_WORKER_ONLY})"
    ) in const_source
    assert "migration tombstone" not in const_source


def test_runtime_mode_gate_is_closed_and_precedes_topology_and_exec() -> None:
    runner = RUN_PATH.read_text(encoding="utf-8")
    gate_start = runner.index('case "${STV3_RUNTIME_MODE}" in')
    gate_end = runner.index("# This release owns exactly two", gate_start)
    gate = runner[gate_start:gate_end]
    legacy_label = gate.index("  legacy)\n")
    canary_label = gate.index("  worker_first_canary)\n")
    raw_label = gate.index("  raw_capture_shadow|worker_only)\n")
    thin_label = gate.index("  thin_operational)\n")
    unknown_label = gate.index("  *)\n")
    assert re.findall(r"^  ([^)\n]+)\)$", gate, re.M) == [
        "legacy",
        "worker_first_canary",
        "raw_capture_shadow|worker_only",
        "thin_operational",
        "*",
    ]
    assert gate.count(
        'bashio::log.fatal "Ongeldige worker-runtimeconfiguratie"'
    ) == 5
    for environment_name in PIN_ENVS:
        assert f'-n "${{{environment_name}}}"' in gate
        assert f'-z "${{{environment_name}}}"' in gate
        assert gate.count(f"unset {environment_name}\n") == 2
        assert gate.count(f"export {environment_name}\n") == 2

    topology = runner.index("storm_tracker_worker.cpu_topology")
    process_exec = runner.index("exec nice -n")
    assert gate_start < gate_end < topology < process_exec
    assert gate_start < runner.index("export STV3_RUNTIME_MODE\n") < gate_end
    for environment_name in PIN_ENVS:
        unset_position = gate.index(f"unset {environment_name}\n")
        export_position = gate.index(f"export {environment_name}\n")
        assert legacy_label < unset_position < canary_label
        assert canary_label < export_position < unknown_label
        assert raw_label < gate.rindex(
            f"export {environment_name}\n"
        ) < thin_label
        assert thin_label < gate.rindex(
            f"unset {environment_name}\n"
        ) < unknown_label
        assert runner.index(f"unset {environment_name}\n") < topology
    for environment_name in CANARY_UNUSED_SECRET_ENVS:
        expected = 3 if environment_name == "STV3_JOB_RECEIPT_DB" else (
            2 if environment_name == "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY" else 1
        )
        assert gate.count(f"unset {environment_name}\n") == expected
        assert canary_label < gate.rindex(
            f"unset {environment_name}\n"
        ) < unknown_label


def test_blitzortung_raw_retention_opt_in_maps_only_the_exact_authority() -> None:
    runner = RUN_PATH.read_text(encoding="utf-8")
    authority = (
        "worker_authority.blitzortung_participant_raw_retention_v1"
    )
    assert runner.count(authority) == 1
    assert (
        'if [[ "$(bashio::config '
        'blitzortung_raw_retention_authorized)" == "true" ]]; then'
    ) in runner
    assert (
        f'STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY="{authority}"'
    ) in runner
    assert runner.count(
        'STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY=""'
    ) == 1
    compose_environment = _top_level_mapping(
        COMPOSE_PATH.read_text(encoding="utf-8"),
        "environment",
    )
    assert compose_environment[
        "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY"
    ] == '"${STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY:-}"'
    example = _dotenv(ENV_EXAMPLE_PATH.read_text(encoding="utf-8"))
    assert example["STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY"] == ""


@pytest.mark.parametrize(
    ("mode", "pins", "expected_returncode", "expected_lines"),
    (
        (
            None,
            (),
            0,
            (
                "mode=legacy",
                "STV3_WORKER_FIRST_CONFIG_PATH=unset",
                "STV3_WORKER_FIRST_CONFIG_SHA256=unset",
                "STV3_WORKER_FIRST_AUTHORITY_SHA256=unset",
                "canary_unused_environment=present",
            ),
        ),
        ("", (), 1, None),
        ("future", (), 1, None),
        ("legacy", ("unexpected",), 1, None),
        ("worker_first_canary", ("config.json", "a" * 64), 1, None),
        (
            "worker_first_canary",
            ("config.json", "a" * 64, "b" * 64),
            0,
            (
                "mode=worker_first_canary",
                "STV3_WORKER_FIRST_CONFIG_PATH=set",
                "STV3_WORKER_FIRST_CONFIG_SHA256=set",
                "STV3_WORKER_FIRST_AUTHORITY_SHA256=set",
                "canary_unused_environment=absent",
            ),
        ),
        ("raw_capture_shadow", ("config.json", "a" * 64), 1, None),
        (
            "raw_capture_shadow",
            ("config.json", "a" * 64, "b" * 64),
            0,
            (
                "mode=raw_capture_shadow",
                "STV3_WORKER_FIRST_CONFIG_PATH=set",
                "STV3_WORKER_FIRST_CONFIG_SHA256=set",
                "STV3_WORKER_FIRST_AUTHORITY_SHA256=set",
                "canary_unused_environment=present",
            ),
        ),
    ),
)
def test_standalone_runtime_gate_executes_fail_closed(
    tmp_path,
    mode,
    pins,
    expected_returncode,
    expected_lines,
) -> None:
    result = _run_gate(tmp_path, mode=mode, pins=pins)

    assert result.returncode == expected_returncode
    assert result.stdout == ""
    assert result.stderr == ""
    result_path = tmp_path / "gate-result.txt"
    if expected_lines is None:
        assert not result_path.exists()
    else:
        assert result_path.read_text(encoding="utf-8").splitlines() == list(
            expected_lines
        )


def test_runtime_gate_reaches_real_service_loader_before_mqtt(tmp_path) -> None:
    missing_config = tmp_path / "missing-config.json"
    result = _run_gate(
        tmp_path,
        mode="worker_first_canary",
        pins=(str(missing_config), "a" * 64, "b" * 64),
        invoke_service=True,
    )

    combined = result.stdout + result.stderr
    assert result.returncode != 0
    assert (
        "WorkerFirstServiceActivationError: config_path_unavailable"
        in combined
    )
    assert str(missing_config) not in combined
    assert "a" * 64 not in combined
    assert "b" * 64 not in combined
    assert "ConnectionRefused" not in combined


def test_runtime_mode_and_pin_values_are_never_logged() -> None:
    runner = RUN_PATH.read_text(encoding="utf-8")
    log_lines = [
        line for line in runner.splitlines() if "bashio::log." in line
    ]
    forbidden = tuple(RUNTIME_ENV) + tuple(RUNTIME_ENV.values())
    assert all(
        token not in line
        for line in log_lines
        for token in forbidden
    )
    python_log_calls = (
        _logger_calls(SERVICE_PATH.read_text(encoding="utf-8"))
        + _logger_calls(ACTIVATION_PATH.read_text(encoding="utf-8"))
    )
    assert all(
        token not in call
        for call in python_log_calls
        for token in forbidden + ("activation_plan", "plan.to_mapping")
    )
