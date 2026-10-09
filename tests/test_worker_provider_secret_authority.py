"""Closed inventory for every acquisition credential authority.

This is a package/configuration contract only.  It never loads a credential
value and does not make a blocked provider executable.
"""
from __future__ import annotations

from pathlib import Path

from storm_tracker_worker import worker_acquisition_plan as acquisition


ROOT = Path(__file__).resolve().parents[1]


# A requirement may be backed by one option or by an inseparable credential
# bundle.  Netatmo deliberately has no static access-token option: the three
# seeds belong to the parent OAuth broker.  KMI RADQPE is externally blocked
# and therefore deliberately has no configured secret authority.
_AUTHORITIES = {
    "worker_secret.aemet_api_key": (
        ("aemet_api_key",),
        ("STV3_AEMET_API_KEY",),
        "direct_environment",
    ),
    "worker_secret.eumetsat_consumer_key": (
        ("eumetsat_consumer_key", "eumetsat_consumer_secret"),
        ("STV3_EUMETSAT_CONSUMER_KEY", "STV3_EUMETSAT_CONSUMER_SECRET"),
        "parent_oauth_broker_only",
    ),
    "worker_secret.eumetsat_consumer_secret": (
        ("eumetsat_consumer_key", "eumetsat_consumer_secret"),
        ("STV3_EUMETSAT_CONSUMER_KEY", "STV3_EUMETSAT_CONSUMER_SECRET"),
        "parent_oauth_broker_only",
    ),
    "worker_secret.hsaf_ftp_credentials": (
        ("hsaf_username", "hsaf_password"),
        ("STV3_HSAF_USERNAME", "STV3_HSAF_PASSWORD"),
        "environment_bundle",
    ),
    "worker_secret.kmi_radqpe_access": ((), (), "external_contract_blocked"),
    "worker_secret.knmi_api_key": (
        ("knmi_api_key",),
        ("STV3_KNMI_API_KEY",),
        "direct_environment",
    ),
    "worker_secret.meteofrance_oauth": (
        ("meteofrance_api_token", "meteofrance_application_id"),
        ("STV3_METEOFRANCE_API_TOKEN", "STV3_METEOFRANCE_APPLICATION_ID"),
        "parent_or_native_oauth_bundle",
    ),
    "worker_secret.netatmo_access_token": (
        (
            "netatmo_client_id",
            "netatmo_client_secret",
            "netatmo_refresh_token",
        ),
        (
            "STV3_NETATMO_CLIENT_ID",
            "STV3_NETATMO_CLIENT_SECRET",
            "STV3_NETATMO_REFRESH_TOKEN",
        ),
        "parent_oauth_broker_only",
    ),
    "worker_secret.synoptic_api_token": (
        ("synoptic_token",),
        ("STV3_SYNOPTIC_TOKEN",),
        "direct_environment",
    ),
}

_RETENTION_AUTHORITIES = {
    "worker_authority.synoptic_raw_retention_terms_v1": (
        "synoptic_raw_retention_terms_accepted",
        "STV3_SYNOPTIC_RAW_RETENTION_AUTHORITY",
    ),
    "worker_authority.rainviewer_cache_attribution_v1": (
        "rainviewer_cache_attribution_accepted",
        "STV3_RAINVIEWER_CACHE_ATTRIBUTION_AUTHORITY",
    ),
    "worker_authority.kmi_appv4_raw_retention_v1": (
        "kmi_appv4_raw_retention_authorized",
        "STV3_KMI_APPV4_RAW_RETENTION_AUTHORITY",
    ),
    "worker_authority.opera_raw_retention_v1": (
        "opera_raw_retention_authorized",
        "STV3_OPERA_RAW_RETENTION_AUTHORITY",
    ),
    "worker_authority.blitzortung_participant_raw_retention_v1": (
        "blitzortung_raw_retention_authorized",
        "STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY",
    ),
}


def test_every_catalog_secret_has_exactly_one_declared_authority():
    assert set(_AUTHORITIES) == set(acquisition.KNOWN_SECRET_REQUIREMENT_IDS)
    assert set(_AUTHORITIES) == {
        requirement
        for row in acquisition.acquisition_catalog_snapshot()["workloads"]
        for requirement in row["secret_requirement_ids"]
    }


def test_addon_exports_every_nonexternal_secret_authority():
    config = (ROOT / "storm_tracker_worker/config.yaml").read_text(
        encoding="utf-8"
    )
    runner = (ROOT / "storm_tracker_worker/run.sh").read_text(
        encoding="utf-8"
    )
    for requirement, (options, variables, authority_kind) in _AUTHORITIES.items():
        if authority_kind == "external_contract_blocked":
            assert not options
            assert not variables
            continue
        assert options
        assert len(options) == len(variables)
        for option, variable in zip(options, variables, strict=True):
            assert f"  {option}:" in config, requirement
            assert f"export {variable}" in runner, requirement
            assert f'{variable}="$(bashio::config {option})"' in runner, requirement


def test_standalone_compose_forwards_same_secret_authorities():
    compose = (ROOT / "storm_tracker_worker/compose.yaml").read_text(
        encoding="utf-8"
    )
    example = (ROOT / "storm_tracker_worker/.env.example").read_text(
        encoding="utf-8"
    )
    for requirement, (_options, variables, authority_kind) in _AUTHORITIES.items():
        if authority_kind == "external_contract_blocked":
            continue
        for variable in variables:
            assert f"{variable}:" in compose, requirement
            assert f"{variable}=" in example, requirement
    assert ":/share/storm_tracker_worker" in compose
    assert "STV3_CONTAINER_MEMORY_LIMIT:-1536m" in compose
    assert (
        "STV3_NETATMO_TOKEN_STATE:-/share/storm_tracker_worker/private/"
        "netatmo_token_state.json"
    ) in compose
    assert (
        "STV3_JOB_RECEIPT_DB:-/share/storm_tracker_worker/state/"
        "service_job_receipts.sqlite3"
    ) in compose


def test_supervisor_retention_options_map_only_true_to_exact_authority_ids():
    config = (ROOT / "storm_tracker_worker/config.yaml").read_text(
        encoding="utf-8"
    )
    runner = (ROOT / "storm_tracker_worker/run.sh").read_text(
        encoding="utf-8"
    )
    for authority_id, (option, variable) in _RETENTION_AUTHORITIES.items():
        assert f"  {option}: false" in config
        assert f'  {option}: "bool"' in config
        assert f'[[ "$(bashio::config {option})" == "true" ]]' in runner
        assert f'{variable}="{authority_id}"' in runner
        assert f'{variable}=""' in runner
        assert f"unset {variable}" in runner


def test_standalone_retention_authorities_are_exact_passthroughs():
    compose = (ROOT / "storm_tracker_worker/compose.yaml").read_text(
        encoding="utf-8"
    )
    example = (ROOT / "storm_tracker_worker/.env.example").read_text(
        encoding="utf-8"
    )
    runner = (ROOT / "storm_tracker_worker/run.sh").read_text(
        encoding="utf-8"
    )
    for authority_id, (_option, variable) in _RETENTION_AUTHORITIES.items():
        assert f'{variable}: "${{{variable}:-}}"' in compose
        assert f'{variable}="${{{variable}:-}}"' in runner
        assert f"{variable}=" in example
        assert authority_id in example


def test_launcher_selects_standalone_before_supervisor_discovery():
    runner = (ROOT / "storm_tracker_worker/run.sh").read_text(
        encoding="utf-8"
    )
    standalone = 'if [[ -n "${STV3_MQTT_HOST:-}" ]]'
    supervisor = 'STV3_MQTT_HOST="$(bashio::services mqtt host)"'
    assert runner.index(standalone) < runner.index(supervisor)
    assert 'STV3_CPU_THREADS="2"' in runner
    assert runner.index('STV3_CPU_THREADS="2"') > runner.index("fi\n")


def test_ephemeral_access_tokens_are_not_static_addon_options():
    config = (ROOT / "storm_tracker_worker/config.yaml").read_text(
        encoding="utf-8"
    )
    runner = (ROOT / "storm_tracker_worker/run.sh").read_text(
        encoding="utf-8"
    )
    assert "netatmo_access_token:" not in config
    assert "STV3_NETATMO_ACCESS_TOKEN" not in runner
    assert "eumetsat_ephemeral_access_token:" not in config
    assert "STV3_EUMETSAT_EPHEMERAL_ACCESS_TOKEN" not in runner
    for option in (
        "netatmo_client_id",
        "netatmo_client_secret",
        "netatmo_refresh_token",
    ):
        assert f"  {option}:" in config


def test_external_kmi_radqpe_secret_cannot_be_silently_configured():
    rows = {
        row["workload_id"]: row
        for row in acquisition.acquisition_catalog_snapshot()["workloads"]
    }
    radqpe = rows["radar.kmi.radqpe_native"]
    assert radqpe["status"] == "external_contract_blocked"
    assert radqpe["budget"] is None
    assert radqpe["durable_production_retention_eligible"] is False
