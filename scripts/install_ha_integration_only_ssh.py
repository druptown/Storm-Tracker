"""Install only a verified Storm Tracker integration on the known HA host.

The paired worker archive is verified from release evidence but is never
uploaded, copied, stopped, installed, or started by this route.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import sys
from typing import Iterable, Mapping

from paramiko.client import AutoAddPolicy, SSHClient


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.release_artifacts import (  # noqa: E402
    verify_release_candidate_evidence,
)


DEFAULT_HOST = "192.168.0.15"
DEFAULT_USER = "hassio"
ACTIVE_INTEGRATION = PurePosixPath(
    "/homeassistant/custom_components/storm_tracker_v3"
)
DEPLOY_ROOT = PurePosixPath("/backup/storm_tracker_deploy_runs")
_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_RUN_ID_RE = re.compile(r"^integration_[0-9_]+_[0-9]{8}T[0-9]{6}Z$")


class InstallError(RuntimeError):
    """The bounded integration-only installation could not be proven safe."""


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Installeer alleen de geverifieerde Storm Tracker-integratie; "
            "de gekoppelde worker wordt uitsluitend read-only geverifieerd."
        )
    )
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument(
        "--confirm-integration-disabled",
        action="store_true",
        help="bevestig dat de Storm Tracker-config-entry uitgeschakeld is",
    )
    parser.add_argument("--apply", action="store_true")
    arguments = parser.parse_args()
    if not arguments.confirm_integration_disabled or not arguments.apply:
        parser.error(
            "--confirm-integration-disabled en --apply zijn beide verplicht"
        )
    return arguments


def _validated_release(
    evidence_path: Path,
) -> tuple[Mapping[str, object], Path]:
    evidence = evidence_path.resolve(strict=True)
    document = verify_release_candidate_evidence(evidence)
    version = document.get("integration_version")
    if not isinstance(version, str) or _VERSION_RE.fullmatch(version) is None:
        raise InstallError("release-evidence mist geldige integration_version")
    # verify_release_candidate_evidence has already re-read and verified both
    # paired archives, including the worker archive that this route never uses.
    return document, evidence.parent


def _q(value: object) -> str:
    return shlex.quote(str(value))


def _run(client: SSHClient, commands: Iterable[str]) -> str:
    command = "set -eu; " + "; ".join(commands)
    _stdin, stdout, stderr = client.exec_command(command, timeout=900)
    output = stdout.read().decode("utf-8", "replace")
    error = stderr.read().decode("utf-8", "replace")
    status = stdout.channel.recv_exit_status()
    if status:
        raise InstallError(
            f"remote installatiestap faalde met exitcode {status}: "
            f"{error.strip()}"
        )
    return output


def _upload_as_root(
    client: SSHClient, source: Path, destination: PurePosixPath
) -> None:
    transport = client.get_transport()
    if transport is None:
        raise InstallError("SSH-transport ontbreekt")
    channel = transport.open_session()
    channel.exec_command(f"sudo -n tee {_q(destination)} >/dev/null")
    with source.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            channel.sendall(chunk)
    channel.shutdown_write()
    status = channel.recv_exit_status()
    if status:
        raise InstallError(
            f"upload naar deploymap faalde met exitcode {status}"
        )


def _manifest_gate() -> str:
    return (
        "test \"$(find /homeassistant/custom_components -type f "
        "-name manifest.json -exec grep -l "
        "'\"domain\": \"storm_tracker_v3\"' {} + | wc -l)\" -eq 1"
    )


def _install(
    client: SSHClient,
    *,
    archive: Path,
    version: str,
    archive_sha256: str,
    run_id: str,
) -> tuple[PurePosixPath, str]:
    run = DEPLOY_ROOT / run_id
    incoming = run / "incoming"
    stage = run / "staging"
    staged_component = stage / "storm_tracker_v3"
    predeploy = run / "predeploy-snapshot" / "storm_tracker_v3"
    quarantine = run / "quarantine"
    remote_archive = incoming / archive.name

    _run(client, (
        "sudo -n true",
        f"test ! -L {_q(ACTIVE_INTEGRATION)}",
        f"test -d {_q(ACTIVE_INTEGRATION)}",
        f"test \"$(readlink -f {_q(ACTIVE_INTEGRATION)})\" = "
        f"{_q(ACTIVE_INTEGRATION)}",
        _manifest_gate(),
        f"test ! -e {_q(run)}",
        f"sudo -n mkdir -p {_q(incoming)} {_q(stage)} "
        f"{_q(predeploy.parent)} {_q(quarantine)}",
    ))
    _upload_as_root(client, archive, remote_archive)
    _run(client, (
        f"test \"$(sha256sum {_q(remote_archive)} | cut -d' ' -f1)\" "
        f"= {_q(archive_sha256)}",
        f"sudo -n python3 -m zipfile -e {_q(remote_archive)} {_q(stage)}",
        f"test -d {_q(staged_component)}",
        f"test ! -L {_q(staged_component)}",
        f"grep -q '\"version\": \"{version}\"' "
        f"{_q(staged_component / 'manifest.json')}",
        f"test -f {_q(staged_component / '__init__.py')}",
    ))
    predeploy_sha256 = _run(client, (
        # HA OS ships a deliberately small tar implementation.  Hash the
        # exact snapshot stream without GNU-only normalization flags.
        f"sudo -n tar -C {_q(ACTIVE_INTEGRATION)} -cf - . "
        f"| sha256sum | cut -d' ' -f1",
    )).strip()
    if re.fullmatch(r"[0-9a-f]{64}", predeploy_sha256) is None:
        raise InstallError("predeploy-componentsnapshot mist geldige SHA-256")

    activation = (
        f"sudo -n mv {_q(ACTIVE_INTEGRATION)} {_q(predeploy)} && "
        f"if sudo -n mv {_q(staged_component)} {_q(ACTIVE_INTEGRATION)} "
        f"&& sudo -n chown -R root:root {_q(ACTIVE_INTEGRATION)} "
        f"&& grep -q '\"version\": \"{version}\"' "
        f"{_q(ACTIVE_INTEGRATION / 'manifest.json')} "
        f"&& {_manifest_gate()}; then true; else "
        f"if test -e {_q(ACTIVE_INTEGRATION)}; then sudo -n mv "
        f"{_q(ACTIVE_INTEGRATION)} {_q(quarantine / 'storm_tracker_v3.candidate_failed')}; fi; "
        f"sudo -n mv {_q(predeploy)} {_q(ACTIVE_INTEGRATION)}; "
        f"{_manifest_gate()}; exit 1; fi"
    )
    _run(client, (activation,))
    return run, predeploy_sha256


def main() -> int:
    arguments = _arguments()
    document, archive_directory = _validated_release(arguments.evidence)
    integration = document["archives"]["integration"]
    archive = archive_directory / integration["filename"]
    version = str(document["integration_version"])
    run_id = (
        f"integration_{version.replace('.', '_')}_"
        f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    )
    if _RUN_ID_RE.fullmatch(run_id) is None:
        raise InstallError("gegenereerde deployrun-ID is ongeldig")

    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())
    client.connect(
        arguments.host,
        username=arguments.user,
        password=os.environ.get("STV3_SSH_PASSWORD"),
        key_filename=os.environ.get("STV3_SSH_KEY"),
        timeout=15,
        look_for_keys=not bool(
            os.environ.get("STV3_SSH_PASSWORD")
            or os.environ.get("STV3_SSH_KEY")
        ),
        allow_agent=False,
    )
    try:
        run, predeploy_sha256 = _install(
            client,
            archive=archive,
            version=version,
            archive_sha256=str(integration["sha256"]),
            run_id=run_id,
        )
    finally:
        client.close()

    print(json.dumps({
        "active_integration_path": str(ACTIVE_INTEGRATION),
        "deployment_run": str(run),
        "integration_sha256": integration["sha256"],
        "integration_version": version,
        "predeploy_snapshot_sha256": predeploy_sha256,
        "source_installation_complete": True,
        "worker_archive_installed": False,
        "worker_app_touched": False,
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, InstallError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
