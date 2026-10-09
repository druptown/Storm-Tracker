from __future__ import annotations

import importlib
import json

import pytest

from storm_tracker_worker.provider_capture_registry import (
    CaptureStatus,
    ProductSemantics,
    PROVIDER_CAPTURE_POLICIES,
    PROVIDER_FETCH_JOB_TYPES,
    ProviderCapturePolicy,
    assert_registry_complete,
    capture_matrix_snapshot,
    capture_policy,
)
from storm_tracker_worker.service import PRODUCTION_JOB_HANDLERS


EXPECTED_FETCH_JOBS = frozenset({
    "aemet_radar_fetch",
    "dpc_radar_fetch",
    "dwd_radolan_fetch",
    "enterprise_rainrate_fetch",
    "eumetsat_li_fetch",
    "goes_glm_fetch",
    "goes_rrqpe_fetch",
    "hsaf_h40b_fetch",
    "kmi_fetch",
    "knmi_fetch",
    "metar_fetch",
    "meteofrance_radar_fetch",
    "netatmo_fetch",
    "odim_hdf5_fetch",
    "open_meteo_fetch",
    "opera_hdf5_fetch",
    "rainviewer_fetch",
    "synoptic_fetch",
})


def test_registry_is_closed_over_every_production_provider_fetch_job():
    production_fetches = {
        name for name in PRODUCTION_JOB_HANDLERS if name.endswith("_fetch")
    }
    assert production_fetches == EXPECTED_FETCH_JOBS
    assert PROVIDER_FETCH_JOB_TYPES == EXPECTED_FETCH_JOBS
    assert_registry_complete(production_fetches)


def test_kmi_and_dwd_are_exact_raw_connected_but_neither_is_admitted():
    exact = [
        policy for policy in PROVIDER_CAPTURE_POLICIES.values()
        if policy.exact_native_bytes_complete
    ]
    assert [policy.job_type for policy in exact] == [
        "dwd_radolan_fetch",
        "kmi_fetch",
    ]
    dwd = capture_policy("dwd_radolan_fetch")
    assert dwd.capture_status is CaptureStatus.EXACT_RAW_CONNECTED
    assert dwd.product_semantics is ProductSemantics.QUANTITATIVE_RADAR
    assert dwd.required_roles == ("rv_tar",)
    assert dwd.vnext_operational_admission is False
    assert dwd.reason_code == (
        "exact_raw_rv_tar_but_legacy_component_decoder_uncertified"
    )
    kmi = capture_policy("kmi_fetch")
    assert kmi.capture_status is CaptureStatus.EXACT_RAW_CONNECTED
    assert kmi.required_roles == ("forecast_json", "radar_png")
    assert kmi.vnext_operational_admission is False
    assert "visualisation_only" in kmi.reason_code
    assert not any(
        policy.vnext_operational_admission
        for policy in PROVIDER_CAPTURE_POLICIES.values()
    )


def test_legacy_artifact_or_result_is_never_mislabeled_raw():
    for job_type, policy in PROVIDER_CAPTURE_POLICIES.items():
        if job_type in {"dwd_radolan_fetch", "kmi_fetch"}:
            continue
        assert policy.capture_status in {
            CaptureStatus.LEGACY_ARTIFACT_ONLY,
            CaptureStatus.RESULT_ONLY,
        }
        assert policy.exact_native_bytes_complete is False
        assert policy.vnext_operational_admission is False
        assert policy.reason_code == "native_capture_not_connected"


def test_unwired_native_candidates_are_tracked_without_false_promotion():
    candidates = {
        job_type: policy
        for job_type, policy in PROVIDER_CAPTURE_POLICIES.items()
        if policy.candidate_exact_raw_tested
    }
    assert set(candidates) == {
        "dpc_radar_fetch",
        "enterprise_rainrate_fetch",
        "goes_rrqpe_fetch",
        "hsaf_h40b_fetch",
        "knmi_fetch",
        "metar_fetch",
        "meteofrance_radar_fetch",
        "netatmo_fetch",
        "odim_hdf5_fetch",
        "opera_hdf5_fetch",
        "rainviewer_fetch",
        "synoptic_fetch",
    }
    assert candidates["dpc_radar_fetch"].candidate_handler == (
        "storm_tracker_worker.dpc_native:capture_latest_dpc_sri"
    )
    assert candidates["dpc_radar_fetch"].candidate_required_roles == (
        "catalog_json",
        "geotiff",
    )
    assert candidates["enterprise_rainrate_fetch"].candidate_handler == (
        "storm_tracker_worker.noaa_err_native:"
        "capture_latest_noaa_err_netcdf4"
    )
    assert candidates["enterprise_rainrate_fetch"].candidate_required_roles == (
        "listing_xml",
        "netcdf4",
    )
    assert candidates["goes_rrqpe_fetch"].candidate_handler == (
        "storm_tracker_worker.goes_rrqpe_native:"
        "capture_latest_goes_rrqpe"
    )
    assert candidates["goes_rrqpe_fetch"].candidate_required_roles == (
        "listing_current",
        "listing_previous",
        "netcdf4",
    )
    assert candidates["hsaf_h40b_fetch"].candidate_handler == (
        "storm_tracker_worker.hsaf_h40b_native:"
        "capture_latest_hsaf_h40b"
    )
    assert candidates["hsaf_h40b_fetch"].candidate_required_roles == (
        "listing_nlst",
        "netcdf_gzip",
    )
    assert candidates["knmi_fetch"].candidate_handler == (
        "storm_tracker_worker.knmi_native:capture_latest_knmi_hdf5"
    )
    assert candidates["knmi_fetch"].candidate_required_roles == ("hdf5",)
    assert candidates["metar_fetch"].candidate_handler == (
        "storm_tracker_worker.metar_native:capture_latest_metar_cache"
    )
    assert candidates["metar_fetch"].candidate_required_roles == (
        "csv_gzip",
    )
    assert candidates["meteofrance_radar_fetch"].candidate_handler == (
        "storm_tracker_worker.meteofrance_native:"
        "capture_latest_meteofrance_hdf5"
    )
    assert candidates["meteofrance_radar_fetch"].candidate_required_roles == (
        "catalog_json",
        "hdf5",
    )
    assert candidates["netatmo_fetch"].candidate_handler == (
        "storm_tracker_worker.netatmo_native:"
        "capture_netatmo_public_data"
    )
    assert candidates["netatmo_fetch"].candidate_required_roles == (
        "response_json",
    )
    assert candidates["odim_hdf5_fetch"].candidate_handler == (
        "storm_tracker_worker.met_office_native:"
        "capture_latest_met_office_odim"
    )
    assert candidates["odim_hdf5_fetch"].candidate_required_roles == (
        "listing_current",
        "listing_previous",
        "odim_hdf5",
    )
    assert candidates["opera_hdf5_fetch"].candidate_handler == (
        "storm_tracker_worker.opera_native:capture_opera_dbzh_hdf5"
    )
    assert candidates["opera_hdf5_fetch"].candidate_required_roles == (
        "hdf5",
    )
    assert candidates["rainviewer_fetch"].candidate_handler == (
        "storm_tracker_worker.rainviewer_native:"
        "capture_latest_rainviewer_region"
    )
    assert len(candidates["rainviewer_fetch"].candidate_required_roles) == 19
    assert candidates["rainviewer_fetch"].candidate_required_roles[9] == (
        "manifest"
    )
    assert sum(
        role.startswith("radar_")
        for role in candidates["rainviewer_fetch"].candidate_required_roles
    ) == 9
    assert sum(
        role.startswith("coverage_")
        for role in candidates["rainviewer_fetch"].candidate_required_roles
    ) == 9
    assert candidates["synoptic_fetch"].candidate_handler == (
        "storm_tracker_worker.synoptic_native:capture_synoptic_timeseries"
    )
    assert candidates["synoptic_fetch"].candidate_required_roles == (
        "response_json",
    )
    for policy in candidates.values():
        assert policy.exact_native_bytes_complete is False
        assert policy.vnext_operational_admission is False
        assert policy.capture_status is not CaptureStatus.EXACT_RAW_CONNECTED


def test_every_candidate_handler_reference_resolves_to_a_callable():
    for policy in PROVIDER_CAPTURE_POLICIES.values():
        if policy.candidate_handler is None:
            continue
        module_name, attribute_name = policy.candidate_handler.split(":", 1)
        module = importlib.import_module(module_name)
        assert callable(getattr(module, attribute_name))


def test_matrix_is_bounded_secret_free_and_machine_readable():
    matrix = capture_matrix_snapshot()
    assert matrix["provider_fetch_jobs"] == len(EXPECTED_FETCH_JOBS)
    assert matrix["exact_raw_connected"] == 2
    assert matrix["isolated_exact_raw_candidates"] == 12
    assert matrix["vnext_operational_admitted"] == 0
    encoded = json.dumps(matrix, sort_keys=True).encode()
    assert len(encoded) < 32 * 1024
    assert b"token" not in encoded.lower()
    assert b"password" not in encoded.lower()
    assert b"credential" not in encoded.lower()


def test_unknown_or_drifted_registry_fails_loudly():
    with pytest.raises(KeyError, match="onbekende"):
        capture_policy("forgotten_fetch")
    with pytest.raises(RuntimeError, match="ontbrekend"):
        assert_registry_complete(EXPECTED_FETCH_JOBS | {"forgotten_fetch"})
    with pytest.raises(RuntimeError, match="verweesd"):
        assert_registry_complete(EXPECTED_FETCH_JOBS - {"metar_fetch"})


def test_policy_invariants_forbid_admission_without_complete_raw_members():
    sample = capture_policy("metar_fetch")
    with pytest.raises(ValueError, match="admission vereist complete"):
        ProviderCapturePolicy(
            job_type=sample.job_type,
            provider_id=sample.provider_id,
            capture_status=sample.capture_status,
            product_semantics=sample.product_semantics,
            exact_native_bytes_complete=False,
            vnext_operational_admission=True,
            reason_code="invalid",
            required_roles=("response",),
        )
