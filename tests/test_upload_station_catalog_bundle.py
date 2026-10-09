from __future__ import annotations

import io
import json
from pathlib import Path
import zipfile

import pytest

from scripts.upload_station_catalog_bundle import (
    _response_succeeded,
    build_importer_environment,
    inject_environment,
    select_synology_entry,
)


@pytest.mark.parametrize(
    "document",
    ({"success": True, "data": {}}, {"result": "ok", "data": {}}),
)
def test_remote_success_accepts_dsm_and_supervisor_shapes(document):
    assert _response_succeeded(document)


def test_select_synology_entry_requires_the_expected_nas():
    document = {
        "data": {
            "entries": [
                {"domain": "synology_dsm", "data": {"host": "192.168.0.99"}},
                {
                    "domain": "synology_dsm",
                    "data": {
                        "host": "192.168.0.21",
                        "username": "nas-user",
                        "password": "nas-password",
                        "device_token": "trusted-device",
                        "port": 5001,
                    },
                },
            ]
        }
    }

    selected = select_synology_entry(document, "192.168.0.21")

    assert selected["device_token"] == "trusted-device"


def test_importer_environment_is_complete_and_quotes_values():
    environment = build_importer_environment({
        "postgis_host": "192.168.0.21",
        "postgis_port": 5433,
        "postgis_database": "storm_tracker",
        "postgis_user": "storm_ingest",
        "postgis_password": "quote'and space",
        "postgis_sslmode": "disable",
        "netatmo_client_id": "client",
        "netatmo_client_secret": "secret",
        "netatmo_refresh_token": "refresh",
    })

    assert "STV3_POSTGIS_ENABLED=true" in environment
    assert "STV3_POSTGIS_PASSWORD='quote'\"'\"'and space'" in environment
    assert "STV3_NETATMO_CLIENT_SECRET=secret" in environment
    assert "STV3_NETATMO_REFRESH_TOKEN=refresh" in environment
    assert "STV3_NETATMO_IMPORT_MINIMUM_CALL_INTERVAL_SECONDS=10" in environment


def test_importer_environment_rejects_missing_secret():
    with pytest.raises(ValueError, match="importer_options_incomplete"):
        build_importer_environment({"postgis_host": "192.168.0.21"})


def test_inject_environment_places_private_env_beside_compose(tmp_path: Path):
    source = tmp_path / "source.zip"
    target = tmp_path / "target.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            "storm-tracker-netatmo-importer/station_catalog_importer/compose.yaml",
            "services: {}\n",
        )

    inject_environment(source, target, "SECRET='value'\n")

    with zipfile.ZipFile(target) as archive:
        info = archive.getinfo(
            "storm-tracker-netatmo-importer/station_catalog_importer/.env"
        )
        assert archive.read(info) == b"SECRET='value'\n"
        assert (info.external_attr >> 16) & 0o777 == 0o600
