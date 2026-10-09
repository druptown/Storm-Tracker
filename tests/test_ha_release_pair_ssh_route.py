from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "install_ha_release_pair_ssh.py"


def _load_script():
    client_stub = types.ModuleType("paramiko.client")
    client_stub.AutoAddPolicy = type("AutoAddPolicy", (), {})
    client_stub.SSHClient = type("SSHClient", (), {})
    package_stub = types.ModuleType("paramiko")
    package_stub.client = client_stub
    previous_package = sys.modules.get("paramiko")
    previous_client = sys.modules.get("paramiko.client")
    sys.modules["paramiko"] = package_stub
    sys.modules["paramiko.client"] = client_stub
    spec = importlib.util.spec_from_file_location(
        "stv3_install_ha_release_pair_ssh",
        SCRIPT,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    finally:
        if previous_package is None:
            sys.modules.pop("paramiko", None)
        else:
            sys.modules["paramiko"] = previous_package
        if previous_client is None:
            sys.modules.pop("paramiko.client", None)
        else:
            sys.modules["paramiko.client"] = previous_client
    return module


def test_root_owned_ha_route_uses_sudo_upload_backup_and_final_ownership(
    monkeypatch,
    tmp_path,
):
    module = _load_script()
    command_batches = []
    uploads = []

    monkeypatch.setattr(
        module,
        "_run",
        lambda _client, commands: command_batches.append(tuple(commands)) or "",
    )
    monkeypatch.setattr(
        module,
        "_upload_as_root",
        lambda _client, source, destination: uploads.append(
            (Path(source), str(destination))
        ),
    )

    run = module._install(
        object(),
        integration_archive=tmp_path / "storm_tracker_v3_v1.2.3_deploy.zip",
        worker_archive=tmp_path / "storm_tracker_worker_v4.5.6_deploy.tar.gz",
        integration_version="1.2.3",
        worker_version="4.5.6",
        integration_sha256="a" * 64,
        worker_sha256="b" * 64,
        run_id="release_1_2_3_4_5_6_20260825T000000Z",
    )

    assert str(run).startswith("/backup/storm_tracker_deploy_runs/release_")
    assert len(uploads) == 2
    assert all(
        destination.startswith("/backup/storm_tracker_deploy_runs/release_")
        for _source, destination in uploads
    )
    commands = "\n".join(
        command for batch in command_batches for command in batch
    )
    assert "sudo -n true" in commands
    assert "sudo -n mkdir -p /backup/storm_tracker_deploy_runs/" in commands
    assert "predeploy_integration" in commands
    assert "predeploy_worker" in commands
    assert "sudo -n chown -R root:root" in commands
    assert (
        "sudo -n tar -C "
        in commands
    )
    assert " -cf - . | sudo -n tar -C " in commands
    assert " -xf -" in commands
    assert "cp -a /homeassistant/custom_components/storm_tracker_v3/." not in commands
    assert "cp -a /addons/storm_tracker_worker/." not in commands
    assert "/tmp/" not in commands


def test_release_pair_route_defaults_exclusively_to_nas_ha():
    module = _load_script()

    assert module.DEFAULT_HOST == "192.168.0.15"
    assert module.DEFAULT_USER == "hassio"
    integration_only_source = (
        ROOT / "scripts" / "install_ha_integration_only_ssh.py"
    ).read_text(encoding="utf-8")
    assert 'DEFAULT_HOST = "192.168.0.15"' in integration_only_source
    assert 'DEFAULT_USER = "hassio"' in integration_only_source


def test_post_install_actions_refresh_store_before_official_app_update():
    source = SCRIPT.read_text(encoding="utf-8")
    store_reload = source.index("ha store reload --no-progress")
    app_update = source.index("update App {WORKER_SLUG}")

    assert store_reload < app_update
    assert "rebuild App {WORKER_SLUG}" not in source


def test_historical_direct_worker_deploy_is_blocked():
    source = (ROOT / "scripts" / "deploy_worker_ssh.py").read_text(
        encoding="utf-8"
    )
    assert "Geblokkeerde historische deployroute" in source
    assert "find \"$target\"" not in source
