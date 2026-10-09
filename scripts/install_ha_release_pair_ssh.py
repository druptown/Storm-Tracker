"""Install one verified Storm Tracker release pair on the known HA host.

This script implements the root-owned Home Assistant path discovered live on
2026-08-25.  The SSH account is deliberately unprivileged; every write below
``/homeassistant``, ``/addons`` or ``/backup`` therefore goes through
``sudo -n``.  Uploads use ``sudo tee`` directly into a unique deploy run, so a
caller never first tries (and fails) to write those paths as the SSH user.

Deploy runs deliberately live outside ``/homeassistant``.  Keeping extracted
archives and rollback copies below the HA configuration directory made every
subsequent Supervisor backup recursively include all earlier deploy runs.

The script only installs source trees.  The caller must stop the worker App and
disable the integration before running it, then rebuild/restart/enable through
the official Home Assistant APIs as described in the deploy runbook.
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
    ReleaseArtifactError,
    verify_release_candidate_evidence,
)


DEFAULT_HOST = "192.168.0.15"
DEFAULT_USER = "hassio"
NAS_SSH_DISABLED_CIPHERS = (
    "aes128-ctr", "aes192-ctr", "aes256-ctr",
    "aes128-cbc", "aes192-cbc", "aes256-cbc", "3des-cbc",
)
ACTIVE_INTEGRATION = PurePosixPath(
    "/homeassistant/custom_components/storm_tracker_v3"
)
ACTIVE_WORKER = PurePosixPath("/addons/storm_tracker_worker")
DEPLOY_ROOT = PurePosixPath("/backup/storm_tracker_deploy_runs")
WORKER_SLUG = "local_storm_tracker_worker"
_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_RUN_ID_RE = re.compile(r"^release_[0-9_]+_[0-9]{8}T[0-9]{6}Z$")


class InstallError(RuntimeError):
    """The bounded source installation could not be proven safe."""


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Installeer een reeds geverifieerd Storm Tracker releasepaar via "
            "de vaste root-owned Home Assistant-route."
        )
    )
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument(
        "--confirm-components-stopped",
        action="store_true",
        help="bevestig dat integratie uitgeschakeld en worker App gestopt zijn",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="voer de broninstallatie werkelijk uit",
    )
    args = parser.parse_args()
    if not args.confirm_components_stopped or not args.apply:
        parser.error(
            "--confirm-components-stopped en --apply zijn beide verplicht"
        )
    return args


def _validated_release(evidence_path: Path) -> tuple[Mapping[str, object], Path]:
    evidence = evidence_path.resolve(strict=True)
    document = verify_release_candidate_evidence(evidence)
    for field in ("integration_version", "worker_version"):
        value = document.get(field)
        if not isinstance(value, str) or _VERSION_RE.fullmatch(value) is None:
            raise InstallError(f"release-evidence mist geldige {field}")
    return document, evidence.parent


def _run_id(integration_version: str, worker_version: str) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    value = (
        f"release_{integration_version.replace('.', '_')}_"
        f"{worker_version.replace('.', '_')}_{stamp}"
    )
    if _RUN_ID_RE.fullmatch(value) is None:
        raise InstallError("gegenereerde deployrun-ID is ongeldig")
    return value


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
            f"remote installatiestap faalde met exitcode {status}: {error.strip()}"
        )
    return output


def _upload_as_root(client: SSHClient, source: Path, destination: PurePosixPath) -> None:
    channel = client.get_transport().open_session()
    channel.exec_command(f"sudo -n tee {_q(destination)} >/dev/null")
    with source.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            channel.sendall(chunk)
    channel.shutdown_write()
    status = channel.recv_exit_status()
    if status:
        raise InstallError(
            f"upload naar vaste deploymap faalde met exitcode {status}"
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
    integration_archive: Path,
    worker_archive: Path,
    integration_version: str,
    worker_version: str,
    integration_sha256: str,
    worker_sha256: str,
    run_id: str,
) -> PurePosixPath:
    run = DEPLOY_ROOT / run_id
    incoming = run / "incoming"
    integration_stage = run / "staging" / "integration"
    worker_stage = run / "staging" / "worker"
    remote_integration = incoming / integration_archive.name
    remote_worker = incoming / worker_archive.name

    _run(client, (
        "sudo -n true",
        f"test ! -L {_q(ACTIVE_INTEGRATION)}",
        f"test ! -L {_q(ACTIVE_WORKER)}",
        f"test -d {_q(ACTIVE_INTEGRATION)}",
        f"test -d {_q(ACTIVE_WORKER)}",
        f"test \"$(readlink -f {_q(ACTIVE_INTEGRATION)})\" = {_q(ACTIVE_INTEGRATION)}",
        f"test \"$(readlink -f {_q(ACTIVE_WORKER)})\" = {_q(ACTIVE_WORKER)}",
        _manifest_gate(),
        f"test ! -e {_q(run)}",
        f"sudo -n mkdir -p {_q(incoming)} {_q(integration_stage)} {_q(worker_stage)}",
        f"sudo -n chown root:root {_q(run)} {_q(incoming)} {_q(run / 'staging')}",
    ))

    _upload_as_root(client, integration_archive, remote_integration)
    _upload_as_root(client, worker_archive, remote_worker)

    _run(client, (
        f"test \"$(sha256sum {_q(remote_integration)} | cut -d' ' -f1)\" = {_q(integration_sha256)}",
        f"test \"$(sha256sum {_q(remote_worker)} | cut -d' ' -f1)\" = {_q(worker_sha256)}",
        f"sudo -n python3 -m zipfile -e {_q(remote_integration)} {_q(integration_stage)}",
        f"sudo -n tar -xzf {_q(remote_worker)} -C {_q(worker_stage)}",
        f"grep -q '\"version\": \"{integration_version}\"' {_q(integration_stage / 'storm_tracker_v3' / 'manifest.json')}",
        f"grep -q 'version: \"{worker_version}\"' {_q(worker_stage / 'storm_tracker_worker' / 'config.yaml')}",
        f"test -f {_q(integration_stage / 'storm_tracker_v3' / '__init__.py')}",
        f"test -f {_q(worker_stage / 'storm_tracker_worker' / 'service.py')}",
        f"test -f {_q(worker_stage / 'storm_tracker_worker' / 'run.sh')}",
    ))

    _run(client, (
        f"sudo -n cp -a {_q(ACTIVE_INTEGRATION)} {_q(run / 'predeploy_integration')}",
        f"sudo -n cp -a {_q(ACTIVE_WORKER)} {_q(run / 'predeploy_worker')}",
        f"sudo -n find {_q(ACTIVE_INTEGRATION)} -mindepth 1 -maxdepth 1 -exec rm -rf -- {{}} +",
        f"sudo -n tar -C {_q(integration_stage / 'storm_tracker_v3')} -cf - . | sudo -n tar -C {_q(ACTIVE_INTEGRATION)} -xf -",
        f"sudo -n find {_q(ACTIVE_WORKER)} -mindepth 1 -maxdepth 1 -exec rm -rf -- {{}} +",
        f"sudo -n tar -C {_q(worker_stage / 'storm_tracker_worker')} -cf - . | sudo -n tar -C {_q(ACTIVE_WORKER)} -xf -",
        f"sudo -n chown -R root:root {_q(ACTIVE_INTEGRATION)} {_q(ACTIVE_WORKER)}",
        f"grep -q '\"version\": \"{integration_version}\"' {_q(ACTIVE_INTEGRATION / 'manifest.json')}",
        f"grep -q 'version: \"{worker_version}\"' {_q(ACTIVE_WORKER / 'config.yaml')}",
        f"test \"$(stat -c %U:%G {_q(ACTIVE_INTEGRATION)})\" = root:root",
        f"test \"$(stat -c %U:%G {_q(ACTIVE_WORKER)})\" = root:root",
        _manifest_gate(),
    ))
    return run


def main() -> int:
    args = _arguments()
    document, archive_directory = _validated_release(args.evidence)
    archives = document["archives"]
    integration = archives["integration"]
    worker = archives["worker"]
    integration_archive = archive_directory / integration["filename"]
    worker_archive = archive_directory / worker["filename"]
    integration_version = str(document["integration_version"])
    worker_version = str(document["worker_version"])
    run_id = _run_id(integration_version, worker_version)

    password = os.environ.get("STV3_SSH_PASSWORD")
    key_filename = os.environ.get("STV3_SSH_KEY")
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())
    client.connect(
        args.host,
        username=args.user,
        password=password,
        key_filename=key_filename,
        timeout=15,
        look_for_keys=not bool(password or key_filename),
        allow_agent=False,
        disabled_algorithms=(
            {"ciphers": NAS_SSH_DISABLED_CIPHERS}
            if args.host == DEFAULT_HOST
            else None
        ),
    )
    try:
        run = _install(
            client,
            integration_archive=integration_archive,
            worker_archive=worker_archive,
            integration_version=integration_version,
            worker_version=worker_version,
            integration_sha256=str(integration["sha256"]),
            worker_sha256=str(worker["sha256"]),
            run_id=run_id,
        )
    finally:
        client.close()

    print(json.dumps({
        "active_integration_path": str(ACTIVE_INTEGRATION),
        "active_worker_path": str(ACTIVE_WORKER),
        "deployment_run": str(run),
        "integration_sha256": integration["sha256"],
        "integration_version": integration_version,
        "next_required_actions": [
            "refresh de lokale App Store met 'ha store reload --no-progress'",
            f"update App {WORKER_SLUG} via de officiële Supervisor/HA-API",
            "validate HA config and restart HA",
            "enable the Storm Tracker integration",
            f"start App {WORKER_SLUG}",
            "perform the complete live verification from the runbook",
        ],
        "source_installation_complete": True,
        "worker_sha256": worker["sha256"],
        "worker_version": worker_version,
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (InstallError, OSError, ReleaseArtifactError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
