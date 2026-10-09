"""End-to-end contracts for the deliberately unwired acquisition candidate."""
from __future__ import annotations

import ast
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_capture_recovery as recovery
from storm_tracker_worker import acquisition_execution_store as execution_store_module
from storm_tracker_worker import acquisition_handler_child as child
from storm_tracker_worker import acquisition_query_authority as query_authority
from storm_tracker_worker import acquisition_runtime as runtime_module
from storm_tracker_worker import acquisition_scheduler_bridge as bridge
from storm_tracker_worker import autonomous_scheduler
from storm_tracker_worker import desired_state
from storm_tracker_worker import dpc_native
from storm_tracker_worker import netatmo_native
from storm_tracker_worker import goes_rrqpe_native
from storm_tracker_worker import hsaf_h40b_native
from storm_tracker_worker import met_office_native
from storm_tracker_worker import noaa_err_native
from storm_tracker_worker import opera_native
from storm_tracker_worker import worker_acquisition_plan as acquisition
from storm_tracker_worker.acquisition_execution_store import (
    ExecutionReceiptConflict,
    ExecutionReceiptStore,
)
from storm_tracker_worker.hard_decoder_isolation import DecoderProcessResult
from storm_tracker_worker.hard_decoder_isolation import HardDecoderExecutor
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactLimits,
    RawArtifactStore,
)
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)


NOW = 1_800_000_000.0
SECRET_SENTINEL = "do-not-persist-this-worker-secret-9137"


class Clock:
    def __init__(self, value: float = NOW) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def _desired_payload(*, latitude=51.04, longitude=4.48) -> dict:
    return {
        "authority_epoch": 1,
        "contract": desired_state.CONTRACT,
        "generation": 1,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "producer_id": "ha-acquisition-runtime-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Home",
            "kind": "home",
            "latitude": latitude,
            "location_accuracy_m": None,
            "longitude": longitude,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW - 60.0,
            "target_id": "zone.home",
        }],
    }


def _multi_region_desired_payload() -> dict:
    payload = _desired_payload()
    payload["targets"] = [
        payload["targets"][0],
        {
            **payload["targets"][0],
            "country_code": "IT",
            "display_name": "Rome",
            "latitude": 41.9028,
            "longitude": 12.4964,
            "target_id": "zone.rome",
        },
        {
            **payload["targets"][0],
            "country_code": "GB",
            "display_name": "London",
            "latitude": 51.5072,
            "longitude": -0.1276,
            "target_id": "zone.london",
        },
        {
            **payload["targets"][0],
            "country_code": "US",
            "display_name": "Los Angeles",
            "latitude": 34.0522,
            "longitude": -118.2437,
            "target_id": "zone.los_angeles",
        },
        {
            **payload["targets"][0],
            "country_code": "US",
            "display_name": "New York",
            "latitude": 40.7128,
            "longitude": -74.006,
            "target_id": "zone.new_york",
        },
    ]
    return payload


def _plan() -> acquisition.AcquisitionPlan:
    return acquisition.build_worker_acquisition_plan(
        desired_state.parse_desired_state(_desired_payload())
    )


def _jobs() -> tuple[acquisition.AcquisitionJob, ...]:
    plan = _plan()
    return acquisition.build_worker_acquisition_dispatch(
        plan,
        as_of=NOW,
        available_secret_requirement_ids=acquisition.KNOWN_SECRET_REQUIREMENT_IDS,
        capacity=acquisition.DispatchCapacity(
            max_jobs=len(plan.intents),
            max_transport_bytes=sum(x.budget.max_transport_bytes for x in plan.intents),
            max_artifacts=sum(x.budget.max_artifacts for x in plan.intents),
            max_wall_seconds=sum(x.budget.max_wall_seconds for x in plan.intents),
        ),
    ).ready_jobs


def _job(workload_id: str) -> acquisition.AcquisitionJob:
    return next(item for item in _jobs() if item.workload_id == workload_id)


def _raw_limits(*, total=512 * 1024 * 1024) -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=101 * 1024 * 1024,
        max_total_bytes=total,
        max_object_entries=100,
        max_staging_entries=8,
        max_receipt_entries=200,
        max_capture_entries=200,
        max_capture_members=14,
        max_catalog_bytes=8 * 1024 * 1024,
    )


def _dwd_raw_limits(*, total=512 * 1024 * 1024) -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=8 * 1024 * 1024,
        max_total_bytes=total,
        max_object_entries=100,
        max_staging_entries=8,
        max_receipt_entries=200,
        max_capture_entries=200,
        max_capture_members=1,
        max_catalog_bytes=8 * 1024 * 1024,
    )


def _profile(*, total=512 * 1024 * 1024) -> RawCapacityProfile:
    return RawCapacityProfile(
        global_max_bytes=total,
        global_max_entries=100,
        providers=(
            ProviderCapacityLimit("aemet", total, 100),
            ProviderCapacityLimit("aviationweather", total, 100),
            ProviderCapacityLimit("dpc", total, 100),
            ProviderCapacityLimit("dwd", total, 100),
            ProviderCapacityLimit("hsaf", total, 100),
            ProviderCapacityLimit("knmi", total, 100),
            ProviderCapacityLimit("meteofrance", total, 100),
            ProviderCapacityLimit("metoffice", total, 100),
            ProviderCapacityLimit("noaa", total, 100),
            ProviderCapacityLimit("noaa_goes", total, 100),
        ),
        max_reservations=100,
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )


def _profile_with_netatmo(
    *, total=512 * 1024 * 1024,
) -> RawCapacityProfile:
    base = _profile(total=total)
    return replace(
        base,
        providers=tuple(sorted(
            base.providers + (
                ProviderCapacityLimit("netatmo", total, 100),
            ),
            key=lambda item: item.provider,
        )),
    )


def _profile_with_kmi(
    *, total=512 * 1024 * 1024,
) -> RawCapacityProfile:
    base = _profile(total=total)
    return replace(
        base,
        providers=tuple(sorted(
            base.providers + (
                ProviderCapacityLimit("kmi", total, 100),
            ),
            key=lambda item: item.provider,
        )),
    )


def _profile_with_eumetsat(
    *, total=512 * 1024 * 1024,
) -> RawCapacityProfile:
    base = _profile(total=total)
    return replace(
        base,
        providers=tuple(sorted(
            base.providers + (
                ProviderCapacityLimit("eumetsat", total, 100),
            ),
            key=lambda item: item.provider,
        )),
    )


def _profile_with_opera(
    *, total=768 * 1024 * 1024,
) -> RawCapacityProfile:
    base = _profile(total=total)
    return replace(
        base,
        providers=tuple(sorted(
            base.providers + (
                ProviderCapacityLimit("opera", total, 100),
            ),
            key=lambda item: item.provider,
        )),
    )


def _opera_raw_limits(*, total=768 * 1024 * 1024) -> RawArtifactLimits:
    return replace(
        _raw_limits(total=total),
        max_object_bytes=384 * 1024 * 1024,
    )


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _success_evidence(
    *,
    handler_id=child.HANDLER_ID,
    workload_id=child.WORKLOAD_ID,
    role="csv_gzip",
    actual_bytes=123,
) -> dict:
    base = {
        "actual_bytes": actual_bytes,
        "actual_entries": 1,
        "capture_sha256": "3" * 64,
        "content_sha256": "1" * 64,
        "handler_id": handler_id,
        "operational_admission_eligible": False,
        "receipt_sha256": "2" * 64,
        "roles": [role],
        "status": "captured_shadow_raw",
        "workload_id": workload_id,
    }
    digest = hashlib.sha256(_canonical(base)).hexdigest()
    return {"contract": child.RESULT_CONTRACT, **base, "evidence_sha256": digest}


def _role_payloads(spec, actual_bytes: int) -> dict[str, bytes]:
    if actual_bytes < len(spec.roles):
        raise ValueError("synthetic aggregate is smaller than its role set")
    quotient, remainder = divmod(actual_bytes, len(spec.roles))
    values = {}
    for index, role in enumerate(spec.roles):
        size = quotient + (1 if index < remainder else 0)
        seed = f"{spec.handler_id}|{role.role}|".encode("ascii")
        values[role.role] = (seed * (size // len(seed) + 1))[:size]
    assert sum(map(len, values.values())) == actual_bytes
    return values


def _materialize_fake_multi_role_capture(
    request: dict,
    *,
    actual_bytes: int,
) -> dict:
    """Create provider-exact durable evidence for an admitted multi-role job."""

    spec = child._HANDLERS[request["handler_id"]]  # noqa: SLF001
    binding = child._recovery_binding(spec)  # noqa: SLF001
    job_id = request["acquisition_job_id"]
    payloads = _role_payloads(spec, actual_bytes)
    store = RawArtifactStore(
        request["raw_store_root"],
        limits=RawArtifactLimits(**request["raw_store_limits"]),
    )
    store.initialize()

    def put(role, *, content_type, upstream_id, provenance):
        member = binding.member(role)
        return store.store(
            provider=binding.raw_provider,
            product=member.raw_product,
            fetch_id=f"{job_id}{member.receipt_suffix}",
            content_type=content_type,
            received_at=NOW,
            upstream_id=upstream_id,
            provenance=provenance,
            payload=payloads[role],
        )

    receipts = {}
    if spec.handler_id == child.DPC_HANDLER_ID:
        catalog_sha = hashlib.sha256(payloads["catalog_json"]).hexdigest()
        receipts["catalog_json"] = put(
            "catalog_json",
            content_type="application/json",
            upstream_id=(
                "radar-api.protezionecivile.it/findLastProductByType?type=SRI"
                f"|content_sha256={catalog_sha}"
            ),
            provenance=dpc_native._catalog_provenance(),
        )
        key = "SRI/02-08-2026-05-30.tif"
        product_time_ms = 1_785_648_600_000
        identity = dpc_native.DpcDownloadIdentity(
            product_type="SRI",
            product_time_ms=product_time_ms,
            bucket=dpc_native.DPC_BUCKET,
            key=key,
            expires_seconds=300,
            object_origin=dpc_native.DPC_S3_ORIGIN,
            object_path=f"/{key}",
        )
        response = dpc_native.DpcHttpResponse(
            payloads["geotiff"],
            "image/tiff",
            NOW,
            len(payloads["geotiff"]),
            '"23300b18b47a941478a64eed513a3998"',
            "bounded-modified",
        )
        geotiff_sha = hashlib.sha256(payloads["geotiff"]).hexdigest()
        receipts["geotiff"] = put(
            "geotiff",
            content_type=response.content_type,
            upstream_id=(
                f"s3:{identity.bucket}:{identity.key}"
                f"|content_sha256={geotiff_sha}"
            ),
            provenance=dpc_native._geotiff_provenance(identity, response),
        )
        product_timestamp = product_time_ms / 1000.0
        capture_upstream = (
            f"{binding.capture_product}|product_time_ms={product_time_ms}"
            f"|key={key}|catalog={receipts['catalog_json'].content_sha256}"
            f"|geotiff={receipts['geotiff'].content_sha256}"
        )
    elif spec.handler_id == child.NOAA_ERR_HANDLER_ID:
        prefix = "BLEND/RainRate-Blend-INST/2026/07/18/"
        listing_response = noaa_err_native.S3HttpResponse(
            payloads["listing_xml"],
            "application/xml",
            len(payloads["listing_xml"]),
            None,
            NOW,
            request_id="listing-request",
        )
        listing_sha = hashlib.sha256(payloads["listing_xml"]).hexdigest()
        receipts["listing_xml"] = put(
            "listing_xml",
            content_type=listing_response.content_type,
            upstream_id=(
                f"s3-list-v2://{noaa_err_native.NOAA_ERR_BUCKET_NAME}/{prefix}"
                f"|max_keys=1000|content_sha256={listing_sha}"
            ),
            provenance=noaa_err_native._listing_provenance(
                prefix,
                listing_response,
            ),
        )
        key = (
            f"{prefix}23/RRQPE-INST-GLB-5_v1r1_blend_"
            "s202607182350000_e202607182359599_c202607190008336.nc"
        )
        etag = '"6ff682687fc8b2e28749a45efab19099"'
        start = datetime(2026, 7, 18, 23, 50, tzinfo=timezone.utc).timestamp()
        end = datetime(2026, 7, 18, 23, 59, tzinfo=timezone.utc).timestamp()
        created = datetime(2026, 7, 19, 0, 8, 33, tzinfo=timezone.utc).timestamp()
        selected = noaa_err_native.NoaaErrObject(
            key,
            etag,
            "2026-07-19T00:09:28.000Z",
            NOW,
            len(payloads["netcdf4"]),
            "STANDARD",
            ("CRC32",),
            "FULL_OBJECT",
            start,
            end,
            created,
            "v1r1",
        )
        object_response = noaa_err_native.S3HttpResponse(
            payloads["netcdf4"],
            "application/netcdf",
            len(payloads["netcdf4"]),
            len(payloads["netcdf4"]),
            NOW,
            etag=etag,
            last_modified="Sun, 19 Jul 2026 00:09:28 GMT",
            last_modified_timestamp=NOW,
            request_id="object-request",
        )
        receipts["netcdf4"] = put(
            "netcdf4",
            content_type=object_response.content_type,
            upstream_id=f"s3://{noaa_err_native.NOAA_ERR_BUCKET_NAME}/{key}",
            provenance=noaa_err_native._object_provenance(
                selected,
                object_response,
            ),
        )
        product_timestamp = start
        capture_upstream = (
            f"{binding.capture_product}|key={key}|etag={etag}"
            f"|listing={receipts['listing_xml'].content_sha256}"
            f"|netcdf4={receipts['netcdf4'].content_sha256}"
        )
    elif spec.handler_id in {child.GOES18_HANDLER_ID, child.GOES19_HANDLER_ID}:
        satellite = int(spec.satellite)
        prefixes = (
            "ABI-L2-RRQPEF/2026/214/06/",
            "ABI-L2-RRQPEF/2026/214/07/",
        )
        for role, prefix in zip(
            ("listing_previous", "listing_current"),
            prefixes,
        ):
            response = goes_rrqpe_native.S3Response(
                payloads[role],
                "application/xml",
                NOW,
                len(payloads[role]),
                None,
                None,
                None,
                f"{role}-request",
                None,
                None,
            )
            digest = hashlib.sha256(payloads[role]).hexdigest()
            receipts[role] = put(
                role,
                content_type=response.content_type,
                upstream_id=(
                    f"s3-list-v2:noaa-goes{satellite}:{prefix}"
                    f":max-1000:sha256:{digest}"
                ),
                provenance=goes_rrqpe_native._listing_provenance(
                    satellite,
                    role,
                    prefix,
                    response,
                ),
            )
        filename = (
            f"OR_ABI-L2-RRQPEF-M6_G{satellite}_"
            "s20262140720226_e20262140729535_c20262140729595.nc"
        )
        key = f"{prefixes[1]}{filename}"
        etag = '"76d365f56dbb2930d78127f76dc742f3"'
        product_timestamp = datetime(
            2026,
            8,
            2,
            7,
            20,
            22,
            600_000,
            tzinfo=timezone.utc,
        ).timestamp()
        selected = goes_rrqpe_native.GoesRrqpeObject(
            satellite,
            key,
            filename,
            6,
            product_timestamp,
            product_timestamp + 571,
            product_timestamp + 577,
            NOW,
            etag,
            len(payloads["netcdf4"]),
            "STANDARD",
            (),
            None,
        )
        object_response = goes_rrqpe_native.S3Response(
            payloads["netcdf4"],
            "application/netcdf",
            NOW,
            len(payloads["netcdf4"]),
            etag,
            NOW,
            None,
            "object-request",
            None,
            None,
        )
        receipts["netcdf4"] = put(
            "netcdf4",
            content_type=object_response.content_type,
            upstream_id=f"s3:noaa-goes{satellite}:{key}:etag:{etag}",
            provenance=goes_rrqpe_native._object_provenance(
                selected,
                object_response,
            ),
        )
        capture_upstream = (
            f"{binding.capture_product}|G{satellite}|key={key}|etag={etag}"
            f"|previous={receipts['listing_previous'].content_sha256}"
            f"|current={receipts['listing_current'].content_sha256}"
            f"|netcdf4={receipts['netcdf4'].content_sha256}"
        )
    elif spec.handler_id == child.METOFFICE_HANDLER_ID:
        prefixes = ("radar/2026/08/01/", "radar/2026/08/02/")
        for role, prefix in zip(
            ("listing_previous", "listing_current"),
            prefixes,
        ):
            response = met_office_native.S3Response(
                payloads[role],
                "application/xml",
                NOW,
                None,
                None,
                None,
                f"{role}-request",
                None,
                None,
            )
            receipts[role] = put(
                role,
                content_type=response.content_type,
                upstream_id=(
                    f"s3-list-v2:{met_office_native.BUCKET_NAME}:{prefix}:max-1000"
                ),
                provenance=met_office_native._listing_provenance(
                    role,
                    prefix,
                    response,
                ),
            )
        key = (
            "radar/2026/08/02/"
            "202608020515_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
        )
        etag = '"cab59e871637a772dc15f01f55009924"'
        product_timestamp = datetime(
            2026,
            8,
            2,
            5,
            15,
            tzinfo=timezone.utc,
        ).timestamp()
        selected = met_office_native.MetOfficeObject(
            key,
            product_timestamp,
            NOW,
            etag,
            len(payloads["odim_hdf5"]),
            "STANDARD",
            "SHA256",
            "FULL_OBJECT",
        )
        object_response = met_office_native.S3Response(
            payloads["odim_hdf5"],
            "application/x-hdf5",
            NOW,
            etag,
            NOW,
            None,
            "object-request",
            None,
            "AES256",
        )
        receipts["odim_hdf5"] = put(
            "odim_hdf5",
            content_type=object_response.content_type,
            upstream_id=f"s3:{met_office_native.BUCKET_NAME}:{key}:etag:{etag}",
            provenance=met_office_native._object_provenance(
                selected,
                object_response,
            ),
        )
        capture_upstream = (
            f"{binding.capture_product}|key={key}|etag={etag}"
            f"|previous={receipts['listing_previous'].content_sha256}"
            f"|current={receipts['listing_current'].content_sha256}"
            f"|hdf5={receipts['odim_hdf5'].content_sha256}"
        )
    elif spec.handler_id == child.HSAF_H40B_HANDLER_ID:
        basename = "h40_20260802_1210_fdk.nc.gz"
        product_timestamp = datetime(
            2026, 8, 2, 12, 10, tzinfo=timezone.utc
        ).timestamp()
        identity = hsaf_h40b_native.HsafListingIdentity(
            selected_basename=basename,
            selected_remote_path=f"{hsaf_h40b_native.FTP_DIRECTORY}/{basename}",
            nominal_slot_time=product_timestamp,
            candidate_count=1,
            first_slot_time=product_timestamp,
            last_slot_time=product_timestamp,
        )
        listing_response = hsaf_h40b_native.HsafFtpResponse(
            payloads["listing_nlst"], NOW, "226 Transfer complete"
        )
        listing_sha = hashlib.sha256(payloads["listing_nlst"]).hexdigest()
        receipts["listing_nlst"] = put(
            "listing_nlst",
            content_type="text/plain",
            upstream_id=(
                f"ftp:{hsaf_h40b_native.FTP_HOST}:{hsaf_h40b_native.FTP_PORT}"
                f"{hsaf_h40b_native.FTP_DIRECTORY}:NLST"
                f"|content_sha256={listing_sha}"
            ),
            provenance=hsaf_h40b_native._listing_provenance(  # noqa: SLF001
                listing_response
            ),
        )
        product_response = hsaf_h40b_native.HsafFtpResponse(
            payloads["netcdf_gzip"],
            NOW,
            "226 Transfer complete",
            len(payloads["netcdf_gzip"]),
        )
        product_sha = hashlib.sha256(payloads["netcdf_gzip"]).hexdigest()
        receipts["netcdf_gzip"] = put(
            "netcdf_gzip",
            content_type="application/gzip",
            upstream_id=(
                f"{hsaf_h40b_native.PRODUCT_ID}:{basename}"
                f"|content_sha256={product_sha}"
            ),
            provenance=hsaf_h40b_native._product_provenance(  # noqa: SLF001
                product_response, identity
            ),
        )
        capture_upstream = hsaf_h40b_native._upstream_capture_id(  # noqa: SLF001
            receipts, identity
        )
    else:  # pragma: no cover - closed caller set
        raise AssertionError("unknown multi-role test handler")

    capture = store.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{job_id}{binding.capture_suffix}",
        upstream_capture_id=capture_upstream,
        product_timestamp=product_timestamp,
        members={role: receipt.receipt_id for role, receipt in receipts.items()},
    )
    digest = recovery.recovery_evidence_digest(
        binding=binding,
        receipts_by_role=receipts,
        capture=capture,
    )
    base = {
        "actual_bytes": digest.actual_bytes,
        "actual_entries": digest.actual_entries,
        "capture_sha256": digest.capture_sha256,
        "content_sha256": digest.content_sha256,
        "handler_id": request["handler_id"],
        "operational_admission_eligible": False,
        "receipt_sha256": digest.receipt_sha256,
        "roles": list(binding.required_roles),
        "status": "captured_shadow_raw",
        "workload_id": request["workload_id"],
    }
    return {
        "contract": child.RESULT_CONTRACT,
        **base,
        "evidence_sha256": hashlib.sha256(_canonical(base)).hexdigest(),
    }


def _materialize_fake_capture(request: dict, *, actual_bytes: int) -> dict:
    """Make fake process success obey the real durable-effect contract."""

    if request["handler_id"] in {
        child.DPC_HANDLER_ID,
        child.NOAA_ERR_HANDLER_ID,
        child.GOES18_HANDLER_ID,
        child.GOES19_HANDLER_ID,
        child.METOFFICE_HANDLER_ID,
        child.HSAF_H40B_HANDLER_ID,
    }:
        return _materialize_fake_multi_role_capture(
            request,
            actual_bytes=actual_bytes,
        )

    specs = {
        child.METAR_HANDLER_ID: {
            "provider": "metar",
            "raw_product": "aviationweather_current_metars_csv_gzip",
            "capture_product": "aviationweather_current_metars_capture",
            "role": "csv_gzip",
            "content_type": "application/gzip",
            "provenance": {
                "representation": NATIVE_REPRESENTATION,
                "capture_stage": NATIVE_CAPTURE_STAGE,
                "collector": "aviationweather_metars_cache",
                "collector_version": "worker-metar-cache-exact-raw-v2",
                "structure_inspector_version": "aviationweather-metars-csv-structure-v2",
                "upstream_filename": "metars.cache.csv.gz",
                "declared_semantic_class": "raw_terminal_observation_reports",
                "coverage": "worldwide_current_reports",
                "provider_update_cadence_seconds": 60,
                "meteorological_interpretation_verified_after_decode": False,
            },
        },
        child.DWD_HANDLER_ID: {
            "provider": "dwd",
            "raw_product": "dwd_open_data_composite_rv_tar",
            "capture_product": "dwd_open_data_composite_rv_raw_capture",
            "role": "rv_tar",
            "content_type": "application/x-tar",
            "provenance": {
                "representation": NATIVE_REPRESENTATION,
                "capture_stage": NATIVE_CAPTURE_STAGE,
                "collector": "dwd_open_data_composite_rv",
                "collector_version": "worker-dwd-rv-tar-exact-raw-v2",
                "structure_inspector_version": "dwd-rv-tar-layout-only-v1",
                "provenance_scope": "exact_upstream_tar_bytes_only",
            },
        },
        child.KNMI_HANDLER_ID: {
            "provider": "knmi",
            "raw_product": "knmi_nl_rdr_data_rtcor_5m_hdf5",
            "capture_product": "knmi_nl_rdr_data_rtcor_5m_capture",
            "role": "hdf5",
            "content_type": "application/x-hdf5",
            "provenance": {
                "representation": NATIVE_REPRESENTATION,
                "capture_stage": NATIVE_CAPTURE_STAGE,
                "collector": "knmi_open_data_native_file",
                "collector_version": "worker-knmi-rtcor-5m-exact-raw-v1",
                "structure_inspector_version": "knmi-rtcor-live-golden-identity-v2",
                "dataset_name": "nl_rdr_data_rtcor_5m",
                "dataset_version": "1.0",
                "dataset_license": "CC-BY-4.0",
                "selection_order": "created_descending",
                "selection_limit": 1,
                "upstream_filename": "RAD_NL25_RAC_RT_202701151200.h5",
                "listing_created": "2027-01-15T12:00:00Z",
                "listing_last_modified": "2027-01-15T12:00:00Z",
                "listing_size_bytes": actual_bytes,
                "download_last_modified": "2027-01-15T12:00:00Z",
                "download_size_bytes": actual_bytes,
                "download_content_type": "application/x-hdf5",
                "declared_semantic_class": "quantitative_precipitation_accumulation",
                "declared_quantity": "precipitation_accumulation",
                "declared_unit": "mm",
                "declared_accumulation_minutes": 5.0,
                "quantitative_semantics_verified_after_decode": False,
            },
        },
    }
    spec = specs[request["handler_id"]]
    role = spec["role"]
    job_id = request["acquisition_job_id"]
    payload_seed = (request["handler_id"] + "|exact-raw-test|").encode()
    payload = (payload_seed * (actual_bytes // len(payload_seed) + 1))[:actual_bytes]
    store = RawArtifactStore(
        request["raw_store_root"],
        limits=RawArtifactLimits(**request["raw_store_limits"]),
    )
    store.initialize()
    content_sha = hashlib.sha256(payload).hexdigest()
    if request["handler_id"] == child.METAR_HANDLER_ID:
        upstream_id = f"aviationweather/metars.cache.csv.gz|content_sha256={content_sha}"
        capture_upstream = (
            f"{spec['capture_product']}|content_sha256={content_sha}"
        )
        product_timestamp = None
    elif request["handler_id"] == child.DWD_HANDLER_ID:
        upstream_id = f"{spec['raw_product']}|latest|content_sha256={content_sha}"
        capture_upstream = (
            f"{spec['capture_product']}|member=composite_rv_20270115_1200_000-hd5"
            f"|content_sha256={content_sha}"
        )
        product_timestamp = 1_800_014_400.0
    else:
        filename = spec["provenance"]["upstream_filename"]
        upstream_id = f"nl_rdr_data_rtcor_5m/1.0|filename={filename}"
        product_timestamp = 1_800_014_400.0
        capture_upstream = (
            f"{spec['capture_product']}|filename={filename}"
            f"|product_timestamp={product_timestamp:.6f}"
            f"|content_sha256={content_sha}"
        )
    receipt = store.store(
        provider=spec["provider"],
        product=spec["raw_product"],
        fetch_id=f"{job_id}:{role}",
        content_type=spec["content_type"],
        received_at=NOW,
        upstream_id=upstream_id,
        provenance=spec["provenance"],
        payload=payload,
    )
    capture = store.commit_capture(
        provider=spec["provider"],
        product=spec["capture_product"],
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=capture_upstream,
        product_timestamp=product_timestamp,
        members={role: receipt.receipt_id},
    )
    base = {
        "actual_bytes": receipt.size_bytes,
        "actual_entries": 1,
        "capture_sha256": capture.capture_receipt_sha256,
        "content_sha256": receipt.content_sha256,
        "handler_id": request["handler_id"],
        "operational_admission_eligible": False,
        "receipt_sha256": receipt.receipt_sha256,
        "roles": [role],
        "status": "captured_shadow_raw",
        "workload_id": request["workload_id"],
    }
    return {
        "contract": child.RESULT_CONTRACT,
        **base,
        "evidence_sha256": hashlib.sha256(_canonical(base)).hexdigest(),
    }


def _materialize_netatmo_capture(
    request: dict,
    *,
    access_token: str,
) -> dict:
    query = netatmo_native._validate_query_payload(  # noqa: SLF001
        request["handler_payload"],
        access_token,
    )
    context = netatmo_native._query_context(query)  # noqa: SLF001
    payload = b'{"body":[],"status":"ok","time_exec":1}'
    job_id = query_authority.safe_native_job_id(
        request["acquisition_job_id"]
    )
    store = RawArtifactStore(
        request["raw_store_root"],
        limits=RawArtifactLimits(**request["raw_store_limits"]),
    )
    store.initialize()
    content_sha256 = hashlib.sha256(payload).hexdigest()
    receipt = store.store(
        provider="netatmo",
        product=netatmo_native.NETATMO_JSON_PRODUCT,
        fetch_id=f"{job_id}:response_json",
        content_type="application/json",
        received_at=NOW,
        upstream_id=netatmo_native._expected_upstream_id(  # noqa: SLF001
            context,
            content_sha256,
        ),
        provenance=netatmo_native._native_provenance(  # noqa: SLF001
            context,
            transport_attempt=1,
        ),
        payload=payload,
    )
    capture = netatmo_native._commit_capture(  # noqa: SLF001
        store,
        job_id=job_id,
        receipt=receipt,
        context=context,
    )
    binding = recovery.RECOVERY_BINDINGS[
        "netatmo_getpublicdata_exact_raw_v1"
    ]
    digest = recovery.recovery_evidence_digest(
        binding=binding,
        receipts_by_role={"response_json": receipt},
        capture=capture,
    )
    base = {
        "actual_bytes": digest.actual_bytes,
        "actual_entries": digest.actual_entries,
        "capture_sha256": digest.capture_sha256,
        "content_sha256": digest.content_sha256,
        "handler_id": request["handler_id"],
        "operational_admission_eligible": False,
        "receipt_sha256": digest.receipt_sha256,
        "roles": ["response_json"],
        "status": "captured_shadow_raw",
        "workload_id": request["workload_id"],
    }
    return {
        "contract": child.RESULT_CONTRACT,
        **base,
        "evidence_sha256": hashlib.sha256(_canonical(base)).hexdigest(),
    }


class FakeExecutor:
    def __init__(self, results=None) -> None:
        self.results = None if results is None else list(results)
        self.calls = []

    def execute(
        self,
        *,
        target,
        request,
        effect_contract,
        cancel_event=None,
        environment_overrides=None,
    ):
        self.calls.append((target, request, effect_contract))
        if self.results is not None:
            result = self.results.pop(0)
            if result.succeeded and isinstance(result.result, dict):
                actual = result.result.get("actual_bytes", 123)
                return DecoderProcessResult(
                    "succeeded",
                    result=_materialize_fake_capture(
                        request,
                        actual_bytes=int(actual),
                    ),
                )
            return result
        return DecoderProcessResult(
            "succeeded",
            result=_materialize_fake_capture(request, actual_bytes=123),
        )


def _components(
    tmp_path: Path,
    *,
    executor=None,
    clock=None,
    lease_seconds=2.0,
    desired_payload=None,
    allowed_workload_ids=None,
    collector_scope=acquisition.COLLECTOR_SCOPE_COVERAGE_RELEVANT,
    netatmo_access_token_broker=None,
    eumetsat_access_token_broker=None,
    capacity_profile=None,
    raw_store_limits=None,
    authorization_requirement_ids=(),
    defer_startup_recovery=False,
):
    clock = clock or Clock()
    desired_payload = desired_payload or _desired_payload()
    desired = desired_state.DesiredStateStore(tmp_path / "desired.sqlite3")
    desired.claim_producer(
        "ha-acquisition-runtime-test", "claim-1", claimed_at=NOW - 100.0
    )
    desired.apply(desired_payload, received_at=NOW - 59.0)
    scheduler = autonomous_scheduler.AutonomousScheduler(
        tmp_path / "scheduler.sqlite3",
        desired_state_store=desired,
        plan_builder=lambda state: bridge.acquisition_scheduler_plan_builder(
            state,
            collector_scope=collector_scope,
        ),
        clock=clock,
        lease_seconds=lease_seconds,
        retry_base_seconds=0.1,
        retry_max_seconds=1.0,
        max_active_leases=max(1, len(
            allowed_workload_ids or activation.DEFAULT_SHADOW_WORKLOAD_IDS
        )),
    )
    ledger = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=capacity_profile or _profile(),
        clock=clock,
    )
    receipts = ExecutionReceiptStore(
        (tmp_path / "execution.sqlite3").resolve(), clock=clock
    )
    fake = executor or FakeExecutor()
    runtime = runtime_module.AcquisitionShadowRuntime(
        desired_state_store=desired,
        scheduler=scheduler,
        capacity_ledger=ledger,
        execution_store=receipts,
        raw_store_root=(tmp_path / "raw").resolve(),
        raw_store_limits=raw_store_limits or _raw_limits(),
        secret_resolver=activation.MappingSecretResolver({
            **{
                key: SECRET_SENTINEL
                for key in acquisition.KNOWN_SECRET_REQUIREMENT_IDS
            },
            **{
                key: "worker_local_authority_present"
                for key in authorization_requirement_ids
            },
        }),
        executor_factory=lambda _remaining: fake,
        netatmo_access_token_broker=netatmo_access_token_broker,
        eumetsat_access_token_broker=eumetsat_access_token_broker,
        allowed_workload_ids=allowed_workload_ids,
        collector_scope=collector_scope,
        clock=clock,
        defer_startup_recovery=defer_startup_recovery,
    )
    return runtime, scheduler, ledger, receipts, fake, clock, desired


def test_deferred_startup_recovery_runs_once_on_explicit_service_start(tmp_path):
    runtime, *_rest = _components(
        tmp_path,
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        raw_store_limits=_dwd_raw_limits(),
        defer_startup_recovery=True,
    )

    before = runtime.diagnostics()
    assert before["capture_recovery"]["audits"] == 0
    assert before["startup_recovery"]["capture_count"] == 0

    first = runtime.startup_recover()
    second = runtime.startup_recover()

    assert first is second
    assert runtime.diagnostics()["capture_recovery"]["audits"] == 1


@pytest.mark.parametrize(
    ("error_type", "reason"),
    [
        (recovery.CaptureRecoveryBusyError, "existing_raw_capture_accounting_busy"),
        (
            recovery.CaptureRecoveryCapacityError,
            "existing_raw_capture_accounting_capacity",
        ),
        (
            recovery.CaptureRecoveryIntegrityError,
            "existing_raw_capture_accounting_integrity",
        ),
    ],
)
def test_deferred_startup_recovery_reports_safe_failure_class(
    monkeypatch, tmp_path, error_type, reason
):
    runtime, *_rest = _components(
        tmp_path,
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        raw_store_limits=_dwd_raw_limits(),
        defer_startup_recovery=True,
    )

    def fail():
        raise error_type("untrusted-detail-must-not-escape")

    monkeypatch.setattr(runtime._capture_reconciler, "audit_existing", fail)
    with pytest.raises(runtime_module.AcquisitionRuntimeError, match=f"^{reason}$"):
        runtime.startup_recover()


def test_deferred_startup_recovery_reports_safe_known_integrity_reason(
    monkeypatch, tmp_path
):
    runtime, *_rest = _components(
        tmp_path,
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        raw_store_limits=_dwd_raw_limits(),
        defer_startup_recovery=True,
    )

    def fail():
        raise recovery.CaptureRecoveryIntegrityError(
            "committed capacity has no durable raw capture"
        )

    monkeypatch.setattr(runtime._capture_reconciler, "audit_existing", fail)
    with pytest.raises(
        runtime_module.AcquisitionRuntimeError,
        match="^existing_raw_capture_committed_missing$",
    ):
        runtime.startup_recover()


def test_deferred_startup_recovery_reports_only_known_suffix_shape(
    monkeypatch, tmp_path
):
    runtime, *_rest = _components(
        tmp_path,
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        raw_store_limits=_dwd_raw_limits(),
        defer_startup_recovery=True,
    )

    def fail():
        raise recovery.CaptureRecoveryIntegrityError(
            "active raw capture fetch suffix drifted:radar.dwd.radolan_rv:capture_revision"
        )

    monkeypatch.setattr(runtime._capture_reconciler, "audit_existing", fail)
    with pytest.raises(
        runtime_module.AcquisitionRuntimeError,
        match=(
            "^existing_raw_capture_fetch_suffix_drift_"
            "radar_dwd_radolan_rv_capture_revision$"
        ),
    ):
        runtime.startup_recover()


def _run_until(runtime, workload_id, *, now=NOW, maximum=100):
    values = []
    for _ in range(maximum):
        result = runtime.tick(now=now)
        values.append(result)
        if result.workload_id == workload_id:
            return result, values
    raise AssertionError(f"workload {workload_id} was never reached")


def _scheduler_job_id(job: acquisition.AcquisitionJob) -> str:
    return bridge.expected_autoscheduler_job_id(
        snapshot_sha256=job.snapshot_sha256,
        intent_id=job.intent_id,
        cadence_seconds=job.cadence_seconds,
        slot_index=job.slot_index,
    )


def _outbox_rows_except(
    path: Path,
    *,
    workload_id: str,
) -> dict[str, tuple]:
    database = sqlite3.connect(path)
    try:
        database.row_factory = sqlite3.Row
        rows = database.execute(
            "SELECT * FROM scheduler_outbox ORDER BY job_id"
        ).fetchall()
    finally:
        database.close()
    return {
        str(row["job_id"]): tuple(row)
        for row in rows
        if json.loads(bytes(row["payload_json"]).decode("utf-8"))[
            "workload_id"
        ] != workload_id
    }


@pytest.mark.parametrize(
    "reason_code",
    (
        "desired_state_changed_during_plan",
        "desired_state_future",
        "desired_state_stale",
    ),
)
def test_recoverable_desired_state_gate_idles_without_provider_work(
    tmp_path, monkeypatch, reason_code
):
    runtime, scheduler, _ledger, _receipts, fake, clock, _desired = (
        _components(tmp_path)
    )

    def blocked_reconcile(*, now=None, allowed_workload_ids=None):
        del now, allowed_workload_ids
        raise autonomous_scheduler.SchedulerDesiredStateError(reason_code)

    monkeypatch.setattr(scheduler, "reconcile", blocked_reconcile)
    result = runtime.tick(now=clock.value)

    assert result.status == "idle"
    assert result.reason_code == reason_code
    assert result.terminal is False
    assert result.workload_id is None
    assert fake.calls == []
    assert runtime.diagnostics()["last_tick"]["reason_code"] == reason_code


def test_inconsistent_desired_state_gate_remains_fatal(tmp_path, monkeypatch):
    runtime, scheduler, _ledger, _receipts, fake, clock, _desired = (
        _components(tmp_path)
    )

    def inconsistent_reconcile(*, now=None, allowed_workload_ids=None):
        del now, allowed_workload_ids
        raise autonomous_scheduler.SchedulerDesiredStateError(
            "desired_state_inconsistent"
        )

    monkeypatch.setattr(scheduler, "reconcile", inconsistent_reconcile)
    with pytest.raises(
        autonomous_scheduler.SchedulerDesiredStateError,
        match="desired_state_inconsistent",
    ):
        runtime.tick(now=clock.value)
    assert fake.calls == []


def test_stale_dynamic_head_idles_until_fresh_control_apply_recovers(tmp_path):
    payload = _desired_payload()
    payload["targets"][0].update({
        "display_name": "Static tracker",
        "kind": "person",
        "target_id": "person.static_tracker",
    })
    runtime, _scheduler, _ledger, _receipts, fake, clock, store = (
        _components(tmp_path, desired_payload=payload)
    )
    clock.value = NOW + 841.0

    stale = runtime.tick(now=clock.value)
    assert stale.status == "idle"
    assert stale.reason_code == "desired_state_stale"
    assert fake.calls == []

    active = store.load_active()
    assert active is not None
    claim = store.claim_producer(
        payload["producer_id"],
        "claim-2",
        claimed_at=clock.value,
    )
    repaired = json.loads(json.dumps(payload))
    repaired.update({
        "authority_epoch": claim["authority_epoch"],
        "generation": 1,
        "issued_at": clock.value,
        "previous_state_sha256": active.state_sha256,
    })
    repaired["targets"][0]["available"] = False
    store.apply(repaired, received_at=clock.value + 0.1)

    clock.value += 0.2
    recovered = runtime.tick(now=clock.value)
    assert recovered.reason_code != "desired_state_stale"


def test_activation_matrix_accounts_for_every_catalog_workload_exactly_once():
    matrix = activation.activation_matrix_snapshot()
    catalogue = acquisition.acquisition_catalog_snapshot()["workloads"]
    assert matrix["workload_count"] == len(catalogue) == len(matrix["rules"])
    assert {row["workload_id"] for row in matrix["rules"]} == {
        row["workload_id"] for row in catalogue
    }
    active = [row for row in matrix["rules"] if row["executable"]]
    assert [row["workload_id"] for row in active] == sorted(
        activation.RAW_CAPTURE_WORKLOAD_IDS
    )
    transport = {
        row["workload_id"]: row["maximum_transport_bytes"] for row in active
    }
    assert {
        key: transport[key] for key in activation.DEFAULT_SHADOW_WORKLOAD_IDS
    } == {
        "ground.metar.global": 16 * 1024 * 1024,
        "radar.aemet.composite": 101 * 1024 * 1024,
        "radar.dpc.sri": 32 * 1024 * 1024,
        "radar.dwd.radolan_rv": 8 * 1024 * 1024,
        "radar.knmi.rtcor": 40 * 1024 * 1024,
        "radar.meteofrance.dpradar": 48 * 1024 * 1024,
        "radar.metoffice.uk_rate": 32 * 1024 * 1024,
        "satellite.goes18.rrqpe": 20 * 1024 * 1024,
        "satellite.goes19.rrqpe": 20 * 1024 * 1024,
        "satellite.hsaf.h40b": 66 * 1024 * 1024,
        "satellite.noaa.err": 36 * 1024 * 1024,
    }
    assert transport["ground.kmi.aws_10min"] == 32 * 1024 * 1024
    assert transport["radar.kmi.public_wcs_unknown_quantity"] == 64 * 1024 * 1024
    hsaf = next(
        row for row in active if row["workload_id"] == "satellite.hsaf.h40b"
    )
    assert hsaf == {
        "executable": True,
        "handler_id": child.HSAF_H40B_HANDLER_ID,
        "handler_payload_contract": "exact_empty_object_v1",
        "authorization_delivery": "none",
        "authorization_requirements": 0,
        "authorization_unavailable_reason_code": None,
        "dynamic_role_maximum": 0,
        "dynamic_role_minimum": 0,
        "dynamic_role_pattern": None,
        "maximum_artifacts": 2,
        "maximum_transport_bytes": 66 * 1024 * 1024,
        "maximum_wall_seconds": 90,
        "product_id": "hsaf_h40b_native_netcdf_gzip",
        "provider_id": "hsaf",
        "reason_code": "exact_multi_role_shadow_capture_audited",
        "raw_capture_service_only": False,
        "required_roles": ["listing_nlst", "netcdf_gzip"],
        "satellite_bound": False,
        "secret_delivery": "worker_environment_presence_v1",
        "secret_requirements": 1,
        "status": "active_shadow_raw",
        "workload_id": "satellite.hsaf.h40b",
    }
    dwd = next(row for row in matrix["rules"] if row["workload_id"] == "radar.dwd.radolan_rv")
    assert dwd["reason_code"] == "dedicated_tar_only_shadow_capture_audited"
    opera = next(row for row in matrix["rules"] if row["workload_id"] == "radar.opera.dbzh")
    assert opera["executable"]
    assert opera["handler_id"] == child.OPERA_HANDLER_ID
    assert opera["handler_payload_contract"] == (
        "canonical_previous_complete_slot_timestamp_v1"
    )
    assert opera["maximum_transport_bytes"] == 384 * 1024 * 1024
    assert opera["reason_code"] == "exact_raw_shadow_capture_audited"
    assert opera["authorization_requirements"] == 1
    assert opera["raw_capture_service_only"] is True
    kmi_rows = {
        row["workload_id"]: row
        for row in matrix["rules"]
        if row["provider_id"] == "kmi"
    }
    assert set(kmi_rows) == {
        "ground.kmi.aws_10min",
        "radar.kmi.appv4_visual",
        "radar.kmi.public_wcs_unknown_quantity",
        "radar.kmi.radqpe_native",
    }
    assert {
        workload_id for workload_id, row in kmi_rows.items() if row["executable"]
    } == {
        "ground.kmi.aws_10min",
        "radar.kmi.appv4_visual",
        "radar.kmi.public_wcs_unknown_quantity",
    }
    unsupported = {row["workload_id"] for row in matrix["rules"]
                   if row["status"] == "blocked_unsupported"}
    assert unsupported == set()
    encoded = json.dumps(matrix, sort_keys=True)
    assert "process_target" not in encoded
    assert "acquisition_handler_child" not in encoded
    assert SECRET_SENTINEL not in encoded


def test_runtime_workload_allowlist_requires_an_immutable_active_subset():
    dwd_only = frozenset({child.DWD_WORKLOAD_ID})

    assert runtime_module._validated_allowed_workload_ids(None) is None
    assert runtime_module._validated_allowed_workload_ids(frozenset()) == frozenset()
    assert runtime_module._validated_allowed_workload_ids(dwd_only) is dwd_only

    for mutable_or_malformed in (
        {child.DWD_WORKLOAD_ID},
        (child.DWD_WORKLOAD_ID,),
        frozenset({1}),
    ):
        with pytest.raises(TypeError, match="allowed_workload_ids"):
            runtime_module._validated_allowed_workload_ids(mutable_or_malformed)

    assert runtime_module._validated_allowed_workload_ids(
        frozenset({"radar.opera.dbzh"})
    ) == frozenset({"radar.opera.dbzh"})
    for unavailable in (frozenset({"not.in.the.activation.matrix"}),):
        with pytest.raises(ValueError, match="ACTIVE_SHADOW_RAW"):
            runtime_module._validated_allowed_workload_ids(unavailable)


def test_metar_handler_payload_is_exact_and_scope_mapping_is_hash_bound():
    job = _job("ground.metar.global")
    decision = activation.assess_acquisition_job(
        job, resolver=activation.NullSecretResolver()
    )
    assert decision.executable
    assert decision.handler_payload == {}
    assert job.scope_id == "global"
    assert job.region_ids and job.target_ids == ("zone.home",)
    assert len(decision.scope_binding_sha256) == 64

    moved_plan = acquisition.build_worker_acquisition_plan(
        desired_state.parse_desired_state(
            _desired_payload(latitude=40.71, longitude=-74.01)
        )
    )
    moved = acquisition.build_worker_acquisition_dispatch(
        moved_plan,
        as_of=NOW,
        available_secret_requirement_ids=acquisition.KNOWN_SECRET_REQUIREMENT_IDS,
    ).ready_jobs
    moved_job = next(x for x in moved if x.workload_id == "ground.metar.global")
    moved_decision = activation.assess_acquisition_job(
        moved_job, resolver=activation.NullSecretResolver()
    )
    assert moved_decision.scope_binding_sha256 != decision.scope_binding_sha256
    assert moved_decision.handler_payload == {}


def test_radar_provider_product_scope_and_payload_bindings_are_exact():
    expected = {
        "radar.dwd.radolan_rv": (
            "dwd", "dwd_radolan_rv_native_tar",
            "coverage:dwd_central_europe", (),
        ),
        "radar.knmi.rtcor": (
            "knmi", "knmi_rtcor_5m_native_hdf5",
            "coverage:knmi_nl", ("worker_secret.knmi_api_key",),
        ),
    }
    resolver = activation.MappingSecretResolver({
        "worker_secret.knmi_api_key": SECRET_SENTINEL,
    })
    for workload_id, (provider, product, scope, requirements) in expected.items():
        job = _job(workload_id)
        decision = activation.assess_acquisition_job(job, resolver=resolver)
        assert decision.executable and decision.handler_payload == {}
        assert (job.provider_id, job.product_id, job.scope_id) == (
            provider, product, scope
        )
        assert job.secret_requirement_ids == requirements
        assert job.region_ids and job.target_ids == ("zone.home",)

    opera = _job("radar.opera.dbzh")
    opera_decision = activation.assess_acquisition_job(
        opera, resolver=activation.NullSecretResolver()
    )
    assert not opera_decision.executable
    assert opera_decision.handler_payload is None
    assert opera_decision.reason_code == "provider_raw_retention_approval_required"
    opera_allowed = activation.assess_acquisition_job(
        opera,
        resolver=activation.MappingSecretResolver({
            "worker_authority.opera_raw_retention_v1": (
                "worker_local_authority_present"
            ),
        }),
    )
    assert opera_allowed.executable
    assert opera_allowed.handler_payload == {"product_timestamp": NOW - 300.0}
    assert opera.scope_id == "coverage:opera_europe"


def test_opera_previous_complete_slot_binding_crosses_utc_day_safely():
    midnight = datetime(2026, 9, 6, tzinfo=timezone.utc).timestamp()
    plan = _plan()
    jobs = acquisition.build_worker_acquisition_dispatch(
        plan,
        as_of=midnight,
        available_secret_requirement_ids=(
            acquisition.KNOWN_SECRET_REQUIREMENT_IDS
        ),
        capacity=acquisition.DispatchCapacity(
            max_jobs=len(plan.intents),
            max_transport_bytes=sum(
                item.budget.max_transport_bytes for item in plan.intents
            ),
            max_artifacts=sum(
                item.budget.max_artifacts for item in plan.intents
            ),
            max_wall_seconds=sum(
                item.budget.max_wall_seconds for item in plan.intents
            ),
        ),
    ).ready_jobs
    opera = next(
        item for item in jobs if item.workload_id == "radar.opera.dbzh"
    )
    allowed = activation.assess_acquisition_job(
        opera,
        resolver=activation.MappingSecretResolver({
            "worker_authority.opera_raw_retention_v1": (
                "worker_local_authority_present"
            ),
        }),
    )

    assert allowed.handler_payload == {
        "product_timestamp": datetime(
            2026, 9, 5, 23, 55, tzinfo=timezone.utc
        ).timestamp()
    }


def test_activation_rejects_forged_target_or_region_even_when_scope_name_matches():
    dwd = _job("radar.dwd.radolan_rv")
    with pytest.raises(activation.ActivationError, match="identity"):
        activation.assess_acquisition_job(
            replace(dwd, target_ids=("forged.target",)),
            resolver=activation.NullSecretResolver(),
        )
    with pytest.raises(activation.ActivationError, match="identity"):
        activation.assess_acquisition_job(
            replace(dwd, region_ids=("region:forged",)),
            resolver=activation.NullSecretResolver(),
        )


def test_secret_gating_is_visible_but_never_returns_or_hashes_secret_value():
    knmi = _job("radar.knmi.rtcor")
    knmi_missing = activation.assess_acquisition_job(
        knmi, resolver=activation.NullSecretResolver()
    )
    knmi_present = activation.assess_acquisition_job(
        knmi,
        resolver=activation.MappingSecretResolver({
            "worker_secret.knmi_api_key": SECRET_SENTINEL
        }),
    )
    assert knmi_missing.reason_code == "secret_requirement_unavailable"
    assert knmi_present.executable and knmi_present.handler_payload == {}
    environment = activation.EnvironmentSecretResolver({
        "STV3_KNMI_API_KEY": "worker-local-key"
    })
    assert activation.assess_acquisition_job(knmi, resolver=environment).executable

    hsaf = _job("satellite.hsaf.h40b")
    assert not activation.assess_acquisition_job(
        hsaf,
        resolver=activation.EnvironmentSecretResolver({
            "STV3_HSAF_USERNAME": "worker-user",
        }),
    ).executable
    hsaf_environment = activation.EnvironmentSecretResolver({
        "STV3_HSAF_USERNAME": "worker-user",
        "STV3_HSAF_PASSWORD": SECRET_SENTINEL,
    })
    assert hsaf_environment.resolve(
        "worker_secret.hsaf_ftp_credentials"
    ) == "worker_environment_secret_present"
    hsaf_decision = activation.assess_acquisition_job(
        hsaf, resolver=hsaf_environment
    )
    assert hsaf_decision.executable and hsaf_decision.handler_payload == {}
    assert SECRET_SENTINEL not in repr(hsaf_decision)


def test_activation_rejects_provider_or_budget_drift():
    metar = _job("ground.metar.global")
    with pytest.raises(activation.ActivationError):
        activation.assess_acquisition_job(
            replace(metar, provider_id="forged"),
            resolver=activation.NullSecretResolver(),
        )
    with pytest.raises(activation.ActivationError):
        activation.assess_acquisition_job(
            replace(metar, budget=replace(metar.budget, max_transport_bytes=12)),
            resolver=activation.NullSecretResolver(),
        )


@pytest.mark.parametrize(
    ("handler_id", "workload_id", "provider", "role", "decode_status"),
    (
        (child.METAR_HANDLER_ID, child.METAR_WORKLOAD_ID, "metar", "csv_gzip", "not_meteorologically_decoded"),
        (child.DWD_HANDLER_ID, child.DWD_WORKLOAD_ID, "dwd", "rv_tar", "not_decoded"),
        (child.KNMI_HANDLER_ID, child.KNMI_WORKLOAD_ID, "knmi", "hdf5", "not_decoded"),
    ),
)
def test_child_adapter_returns_only_durable_raw_hashes(
    monkeypatch, tmp_path, handler_id, workload_id, provider, role, decode_status
):
    def fake_native(
        spec,
        *,
        job_id,
        store,
        execution_deadline_at=None,
    ):
        assert execution_deadline_at is None
        role_spec = spec.roles[0]
        receipt = store.store(
            provider=provider,
            product=role_spec.raw_product,
            fetch_id=f"{job_id}:{role}",
            content_type=sorted(role_spec.allowed_content_types)[0],
            received_at=NOW,
            upstream_id="test-upstream",
            provenance={
                "representation": NATIVE_REPRESENTATION,
                "capture_stage": NATIVE_CAPTURE_STAGE,
                "collector": "test",
                "collector_version": "test-v1",
            },
            payload=b"exact-gzip-placeholder",
        )
        capture = store.commit_capture(
            provider=provider,
            product=spec.capture_product,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id="test-capture-upstream",
            product_timestamp=None,
            members={role: receipt.receipt_id},
        )
        return {
            "provider": provider,
            "decode_status": decode_status,
            "capture_state": "captured_not_decoded",
            "weather_state": "unknown",
            "operational_admission_eligible": False,
            "raw_provenance": {
                "raw_capture_manifest_sha256": capture.manifest_sha256,
                "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
                "raw_capture_id": capture.capture_id,
                "raw_sources": {role: {
                    "role": role,
                    "raw_receipt_id": receipt.receipt_id,
                    "raw_content_sha256": receipt.content_sha256,
                    "raw_receipt_sha256": receipt.receipt_sha256,
                }},
            },
        }

    monkeypatch.setattr(child, "_run_native", fake_native)
    raw_root = (tmp_path / "raw-child").resolve()
    RawArtifactStore(raw_root, limits=_raw_limits()).initialize()
    result = child.execute_shadow_capture({
        "acquisition_job_id": "acquisition:" + "a" * 64,
        "contract": child.CONTRACT,
        "handler_id": handler_id,
        "handler_payload": {},
        "raw_store_limits": runtime_module._raw_limits_mapping(_raw_limits()),
        "raw_store_root": str(raw_root),
        "workload_id": workload_id,
    })
    assert result["status"] == "captured_shadow_raw"
    assert result["actual_bytes"] == len(b"exact-gzip-placeholder")
    assert result["roles"] == [role]
    encoded = json.dumps(result)
    assert "artifact_path" not in encoded
    assert "weather_state" not in encoded
    assert "latitude" not in encoded and "longitude" not in encoded


def test_legacy_opera_candidate_handler_is_absent_from_child_dispatch(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        child, "_run_native", lambda *_args, **_kwargs: pytest.fail("provider ran")
    )
    with pytest.raises(ValueError, match="absent"):
        child.execute_shadow_capture({
            "acquisition_job_id": "acquisition:" + "a" * 64,
            "contract": child.CONTRACT,
            "handler_id": "opera_dbzh_hdf5_exact_raw_candidate_v1",
            "handler_payload": {"product_timestamp": NOW},
            "raw_store_limits": runtime_module._raw_limits_mapping(_raw_limits()),
            "raw_store_root": str((tmp_path / "raw-opera-blocked").resolve()),
            "workload_id": "radar.opera.dbzh",
        })


def test_default_runtime_executor_is_a_fresh_hard_bounded_process_executor():
    executor = runtime_module._default_executor_factory(12.0)
    assert isinstance(executor, HardDecoderExecutor)
    assert executor._limits.wall_seconds == 12.0
    assert executor._limits.cpu_seconds == 12.0
    assert executor._limits.memory_bytes == 512 * 1024 * 1024
    assert executor._limits.max_request_bytes == runtime_module.MAX_CHILD_REQUEST_BYTES
    assert executor._limits.max_result_bytes == runtime_module.MAX_CHILD_RESULT_BYTES
    longest = runtime_module._default_executor_factory(300.0)
    assert longest._limits.wall_seconds == runtime_module.MAX_EXECUTOR_WALL_SECONDS
    assert longest._limits.cpu_seconds == runtime_module.MAX_EXECUTOR_WALL_SECONDS


def test_every_executable_child_payload_contract_matches_activation_authority():
    mismatches = {
        workload_id: (
            rule.handler_payload_contract,
            child._HANDLERS[rule.handler_id].payload_contract,  # noqa: SLF001
        )
        for workload_id, rule in activation.ACTIVATION_RULES.items()
        if rule.executable
        and rule.handler_id in child._HANDLERS  # noqa: SLF001
        and rule.handler_payload_contract
        != child._HANDLERS[rule.handler_id].payload_contract  # noqa: SLF001
    }

    assert mismatches == {}


def test_recovery_failures_use_short_probe_with_periodic_full_budget():
    runtime = object.__new__(runtime_module.AcquisitionShadowRuntime)
    workload_id = "radar.knmi.rtcor"
    runtime._consecutive_terminal_failures = {
        workload_id: runtime_module.ADAPTIVE_ATTEMPT_FAILURE_THRESHOLD,
    }
    base = _job(workload_id)
    phase = int(
        hashlib.sha256(workload_id.encode("utf-8")).hexdigest()[:8], 16
    ) % runtime_module.ADAPTIVE_FULL_PROBE_INTERVAL
    short_slot = (phase + 1) % runtime_module.ADAPTIVE_FULL_PROBE_INTERVAL
    short_job = replace(base, slot_index=short_slot)
    full_job = replace(base, slot_index=phase)

    assert runtime._attempt_wall_seconds(
        short_job,
        rule_wall_seconds=90.0,
        deadline_remaining_seconds=75.0,
    ) == runtime_module.ADAPTIVE_ATTEMPT_WALL_SECONDS
    assert runtime._attempt_wall_seconds(
        full_job,
        rule_wall_seconds=90.0,
        deadline_remaining_seconds=75.0,
    ) == 75.0

    runtime._consecutive_terminal_failures.clear()
    assert runtime._attempt_wall_seconds(
        short_job,
        rule_wall_seconds=90.0,
        deadline_remaining_seconds=75.0,
    ) == 75.0


def test_execution_receipts_are_restart_safe_idempotent_and_conflict_closed(tmp_path):
    clock = Clock()
    path = (tmp_path / "receipts.sqlite3").resolve()
    store = ExecutionReceiptStore(path, clock=clock)
    request = {
        "scheduler_job_id": "autosched:" + "a" * 64,
        "acquisition_job_id": "acquisition:" + "b" * 64,
        "binding_sha256": "c" * 64,
        "attempt_count": 1,
        "outcome": "blocked_permanent",
        "reason_code": "handler_unwired",
        "terminal": True,
    }
    first = store.record(**request)
    clock.value += 5.0
    replay = ExecutionReceiptStore(path, clock=clock).record(**request)
    assert replay == first
    assert store.load_terminal(request["acquisition_job_id"]) == first
    with pytest.raises(ExecutionReceiptConflict):
        store.record(**{**request, "reason_code": "different_reason"})
    assert store.stats()["terminal_count"] == 1


def test_execution_terminal_head_and_receipt_roll_back_as_one_transaction(tmp_path):
    armed = {"value": True}

    def fault(point):
        if armed["value"] and point == "record.after_terminal_head":
            armed["value"] = False
            raise RuntimeError("synthetic receipt crash")

    path = (tmp_path / "fault-receipts.sqlite3").resolve()
    store = ExecutionReceiptStore(path, clock=lambda: NOW, fault_injector=fault)
    request = {
        "scheduler_job_id": "autosched:" + "a" * 64,
        "acquisition_job_id": "acquisition:" + "b" * 64,
        "binding_sha256": "c" * 64,
        "attempt_count": 1,
        "outcome": "blocked_permanent",
        "reason_code": "handler_unwired",
        "terminal": True,
    }
    with pytest.raises(RuntimeError, match="synthetic receipt crash"):
        store.record(**request)
    assert ExecutionReceiptStore(path).load_terminal(request["acquisition_job_id"]) is None
    recovered = ExecutionReceiptStore(path, clock=lambda: NOW).record(**request)
    assert recovered.terminal


def test_runtime_processes_all_jobs_serially_and_only_closed_subset_executes(tmp_path):
    runtime, scheduler, ledger, receipts, fake, _clock, _desired = _components(tmp_path)
    active_workloads = tuple(
        intent.workload_id
        for intent in _plan().intents
        if intent.workload_id in activation.DEFAULT_SHADOW_WORKLOAD_IDS
    )
    metar, values = _run_until(runtime, "ground.metar.global")
    assert metar.status == "completed_shadow_raw"
    assert metar.terminal
    assert len(fake.calls) == len(active_workloads) == 6
    target, request, effect = next(
        call for call in fake.calls if call[1]["workload_id"] == "ground.metar.global"
    )
    assert target == activation.METAR_TARGET
    assert request["handler_payload"] == {}
    assert "lease" not in json.dumps(request).lower()
    assert SECRET_SENTINEL not in json.dumps(request)
    assert effect.admitted and effect.durable_effects == "idempotent"
    knmi_request = next(
        call[1] for call in fake.calls
        if call[1]["workload_id"] == "radar.knmi.rtcor"
    )
    knmi_encoded = json.dumps(knmi_request)
    assert "worker_secret" not in knmi_encoded
    assert "STV3_KNMI_API_KEY" not in knmi_encoded
    assert SECRET_SENTINEL not in knmi_encoded
    hsaf_request = next(
        call[1] for call in fake.calls
        if call[1]["workload_id"] == "satellite.hsaf.h40b"
    )
    hsaf_encoded = json.dumps(hsaf_request)
    assert hsaf_request["handler_payload"] == {}
    assert "worker_secret" not in hsaf_encoded
    assert "STV3_HSAF_USERNAME" not in hsaf_encoded
    assert "STV3_HSAF_PASSWORD" not in hsaf_encoded
    assert SECRET_SENTINEL not in hsaf_encoded
    assert all(
        value.status in {"blocked", "completed_shadow_raw", "not_admitted"}
        for value in values
    )
    assert scheduler.diagnostics()["active_lease"] is False
    stats = ledger.stats()
    assert stats["reservation_rows"] == stats["committed_count"] == len(
        active_workloads
    )
    assert stats["committed_bytes"] == len(active_workloads) * 123
    for workload_id in active_workloads:
        job = _job(workload_id)
        reservation = ledger.load(runtime_module._reservation_id(job))
        assert reservation is not None and reservation.status == "COMMITTED"
        assert reservation.requested_bytes == job.budget.max_transport_bytes
        assert reservation.requested_entries == job.budget.max_artifacts
    assert receipts.stats()["terminal_count"] == len(values)


def test_runtime_recovers_latest_verified_capture_for_worker_restart(tmp_path):
    runtime, _scheduler, _ledger, receipts, _fake, _clock, _desired = (
        _components(
            tmp_path,
            allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
            raw_store_limits=_dwd_raw_limits(),
        )
    )

    result, _values = _run_until(runtime, child.DWD_WORKLOAD_ID)
    assert result.status == "completed_shadow_raw"
    restored = runtime.retained_successful_captures()

    assert len(restored) == 1
    workload_id, capture = restored[0]
    assert workload_id == child.DWD_WORKLOAD_ID
    assert capture.capture_id == result.source_capture_id
    terminal = receipts.load_terminal(str(result.acquisition_job_id))
    assert terminal is not None
    assert capture.capture_receipt_sha256 == terminal.capture_sha256


def test_dwd_only_candidate_leaves_ten_other_active_sources_effect_free(tmp_path):
    clock = Clock()
    fake = FakeExecutor()
    paths = runtime_module.ShadowRuntimePaths(
        desired_state=(tmp_path / "desired.sqlite3").resolve(),
        scheduler=(tmp_path / "scheduler.sqlite3").resolve(),
        capacity_ledger=(tmp_path / "capacity.sqlite3").resolve(),
        execution_receipts=(tmp_path / "execution.sqlite3").resolve(),
        raw_store_root=(tmp_path / "raw").resolve(),
    )
    runtime = runtime_module.create_shadow_candidate(
        paths,
        capacity_profile=_profile(),
        raw_store_limits=_dwd_raw_limits(),
        secret_resolver=activation.MappingSecretResolver({
            key: SECRET_SENTINEL
            for key in acquisition.KNOWN_SECRET_REQUIREMENT_IDS
        }),
        executor_factory=lambda _remaining: fake,
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        clock=clock,
    )
    desired = runtime.desired_state_store
    desired.claim_producer(
        "ha-acquisition-runtime-test", "claim-1", claimed_at=NOW - 100.0
    )
    all_active_payload = _multi_region_desired_payload()
    home = all_active_payload["targets"][0]
    all_active_payload["targets"].extend((
        {
            **home,
            "country_code": "ES",
            "display_name": "Madrid",
            "latitude": 40.4168,
            "longitude": -3.7038,
            "target_id": "zone.madrid",
        },
        {
            **home,
            "country_code": "FR",
            "display_name": "Paris",
            "latitude": 48.8566,
            "longitude": 2.3522,
            "target_id": "zone.paris",
        },
    ))
    desired.apply(all_active_payload, received_at=NOW - 59.0)
    plan = acquisition.build_worker_acquisition_plan(desired.load_active())
    jobs = acquisition.build_worker_acquisition_slot_jobs(plan, as_of=NOW)
    active_workloads = set(activation.DEFAULT_SHADOW_WORKLOAD_IDS)
    disallowed_active = active_workloads - {child.DWD_WORKLOAD_ID}
    assert len(active_workloads) == 11
    assert len(disallowed_active) == 10
    assert active_workloads <= {job.workload_id for job in jobs}

    results = []
    for _ in range(len(plan.intents) + 2):
        result = runtime.tick(now=clock.value)
        results.append(result)
        if result.reason_code == "no_ready_serial_job":
            break
    else:  # pragma: no cover - explicit hard bound
        raise AssertionError("DWD-only serial slot did not close")

    dwd = next(
        item for item in results if item.workload_id == child.DWD_WORKLOAD_ID
    )
    assert dwd.status == "completed_shadow_raw"
    assert dwd.receipt_sha256 is not None
    assert all(item.reason_code != "workload_not_allowed" for item in results)
    assert [call[1]["workload_id"] for call in fake.calls] == [
        child.DWD_WORKLOAD_ID
    ]

    ledger = runtime.capacity_ledger
    receipts = runtime.execution_store
    raw = runtime.raw_store.stats()
    assert ledger.stats()["reservation_rows"] == 1
    assert ledger.stats()["committed_count"] == 1
    assert receipts.stats()["receipt_count"] == 1
    assert receipts.stats()["terminal_count"] == 1
    assert raw["object_count"] == raw["receipt_count"] == raw["capture_count"] == 1
    for job in jobs:
        if job.workload_id not in disallowed_active:
            continue
        assert runtime.scheduler.get_record(_scheduler_job_id(job)) is None
        assert ledger.load(runtime_module._reservation_id(job)) is None
        assert receipts.load_terminal(job.job_id) is None


def test_dwd_only_startup_audit_repairs_only_selected_capture_across_restart(
    tmp_path,
):
    clock = Clock()
    desired_path = (tmp_path / "desired.sqlite3").resolve()
    scheduler_path = (tmp_path / "scheduler.sqlite3").resolve()
    capacity_path = (tmp_path / "capacity.sqlite3").resolve()
    execution_path = (tmp_path / "execution.sqlite3").resolve()
    raw_root = (tmp_path / "raw").resolve()
    limits = _dwd_raw_limits()
    desired = desired_state.DesiredStateStore(desired_path)
    desired.claim_producer(
        "ha-acquisition-runtime-test", "claim-1", claimed_at=NOW - 100.0
    )
    desired.apply(_desired_payload(), received_at=NOW - 59.0)
    scheduler = autonomous_scheduler.AutonomousScheduler(
        scheduler_path,
        desired_state_store=desired,
        plan_builder=bridge.acquisition_scheduler_plan_builder,
        clock=clock,
    )
    ledger = RawCapacityLedger(capacity_path, profile=_profile(), clock=clock)
    execution = ExecutionReceiptStore(execution_path, clock=clock)
    store = RawArtifactStore(raw_root, limits=limits)
    store.initialize()
    evidence_by_workload = {}
    jobs = {
        workload_id: _job(workload_id)
        for workload_id in (
            child.DWD_WORKLOAD_ID,
            child.KNMI_WORKLOAD_ID,
        )
    }
    for index, (workload_id, job) in enumerate(jobs.items(), start=1):
        rule = activation.ACTIVATION_RULES[workload_id]
        ledger.reserve(
            reservation_id=runtime_module._reservation_id(job),
            provider=job.provider_id,
            job_identity=job.job_id,
            requested_bytes=job.budget.max_transport_bytes,
            requested_entries=job.budget.max_artifacts,
        )
        evidence_by_workload[workload_id] = _materialize_fake_capture(
            {
                "acquisition_job_id": job.job_id,
                "contract": child.CONTRACT,
                "handler_id": rule.handler_id,
                "handler_payload": {},
                "raw_store_limits": runtime_module._raw_limits_mapping(limits),
                "raw_store_root": str(raw_root),
                "workload_id": workload_id,
            },
            actual_bytes=100 + index,
        )
    knmi_job = jobs[child.KNMI_WORKLOAD_ID]
    knmi_evidence = evidence_by_workload[child.KNMI_WORKLOAD_ID]
    knmi_terminal = execution.record(
        scheduler_job_id=_scheduler_job_id(knmi_job),
        acquisition_job_id=knmi_job.job_id,
        binding_sha256="b" * 64,
        attempt_count=1,
        outcome="completed_shadow_raw",
        reason_code="shadow_raw_captured",
        terminal=True,
        content_sha256=knmi_evidence["content_sha256"],
        raw_receipt_sha256=knmi_evidence["receipt_sha256"],
        capture_sha256=knmi_evidence["capture_sha256"],
        capacity_commit_sha256="c" * 64,
    )
    knmi_receipt = next(
        item for item in store.accounting_receipts() if item.provider == "knmi"
    )
    knmi_capture = next(
        item for item in store.accounting_captures() if item.provider == "knmi"
    )
    knmi_bytes = store.read_bytes(knmi_receipt.receipt_id)
    knmi_reservation = ledger.load(runtime_module._reservation_id(knmi_job))
    assert knmi_reservation is not None and knmi_reservation.status == "RESERVED"

    runtime = runtime_module.AcquisitionShadowRuntime(
        desired_state_store=desired,
        scheduler=scheduler,
        capacity_ledger=ledger,
        execution_store=execution,
        raw_store_root=raw_root,
        raw_store_limits=limits,
        executor_factory=lambda _remaining: FakeExecutor(),
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        clock=clock,
    )
    startup = runtime.diagnostics()["startup_recovery"]
    assert startup == {
        "already_committed_count": 0,
        "capture_count": 1,
        "contract": recovery.CONTRACT,
        "incomplete_receipt_count": 0,
        "receipt_count": 1,
        "recovered_commit_count": 1,
    }
    dwd_reservation = ledger.load(
        runtime_module._reservation_id(jobs[child.DWD_WORKLOAD_ID])
    )
    assert dwd_reservation is not None and dwd_reservation.status == "COMMITTED"
    assert ledger.load(runtime_module._reservation_id(knmi_job)) == knmi_reservation
    assert execution.load_terminal(knmi_job.job_id) == knmi_terminal
    assert runtime.raw_store.load_receipt(knmi_receipt.receipt_id) == knmi_receipt
    assert runtime.raw_store.load_capture(knmi_capture.capture_id) == knmi_capture
    assert runtime.raw_store.read_bytes(knmi_receipt.receipt_id) == knmi_bytes

    reopened = runtime_module.AcquisitionShadowRuntime(
        desired_state_store=desired_state.DesiredStateStore(desired_path),
        scheduler=autonomous_scheduler.AutonomousScheduler(
            scheduler_path,
            desired_state_store=desired_state.DesiredStateStore(desired_path),
            plan_builder=bridge.acquisition_scheduler_plan_builder,
            clock=clock,
        ),
        capacity_ledger=RawCapacityLedger(
            capacity_path, profile=_profile(), clock=clock
        ),
        execution_store=ExecutionReceiptStore(execution_path, clock=clock),
        raw_store_root=raw_root,
        raw_store_limits=limits,
        executor_factory=lambda _remaining: FakeExecutor(),
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        clock=clock,
    )
    replay = reopened.diagnostics()["startup_recovery"]
    assert replay["recovered_commit_count"] == 0
    assert replay["already_committed_count"] == 1
    assert reopened.capacity_ledger.load(
        runtime_module._reservation_id(knmi_job)
    ) == knmi_reservation
    assert reopened.execution_store.load_terminal(knmi_job.job_id) == knmi_terminal
    assert reopened.raw_store.load_receipt(knmi_receipt.receipt_id) == knmi_receipt
    assert reopened.raw_store.load_capture(knmi_capture.capture_id) == knmi_capture
    assert reopened.raw_store.read_bytes(knmi_receipt.receipt_id) == knmi_bytes


def test_dwd_only_scheduler_does_not_reclaim_preexisting_non_dwd_lease(tmp_path):
    clock = Clock()
    desired = desired_state.DesiredStateStore(tmp_path / "desired.sqlite3")
    desired.claim_producer(
        "ha-acquisition-runtime-test", "claim-1", claimed_at=NOW - 100.0
    )
    desired.apply(_desired_payload(), received_at=NOW - 59.0)
    scheduler_path = (tmp_path / "scheduler.sqlite3").resolve()
    scheduler = autonomous_scheduler.AutonomousScheduler(
        scheduler_path,
        desired_state_store=desired,
        plan_builder=bridge.acquisition_scheduler_plan_builder,
        clock=clock,
        lease_seconds=2.0,
    )
    scheduler.reconcile(now=clock.value)
    claimed = scheduler.claim_next("old-all-sources-worker", now=clock.value)
    assert claimed is not None
    assert claimed.payload["workload_id"] != child.DWD_WORKLOAD_ID
    before = _outbox_rows_except(
        scheduler_path, workload_id=child.DWD_WORKLOAD_ID
    )
    clock.value = claimed.lease_expires_at + 0.01
    fake = FakeExecutor()
    runtime = runtime_module.AcquisitionShadowRuntime(
        desired_state_store=desired,
        scheduler=scheduler,
        capacity_ledger=RawCapacityLedger(
            (tmp_path / "capacity.sqlite3").resolve(),
            profile=_profile(),
            clock=clock,
        ),
        execution_store=ExecutionReceiptStore(
            (tmp_path / "execution.sqlite3").resolve(), clock=clock
        ),
        raw_store_root=(tmp_path / "raw").resolve(),
        raw_store_limits=_dwd_raw_limits(),
        executor_factory=lambda _remaining: fake,
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        clock=clock,
    )
    result = runtime.tick(now=clock.value)
    assert result.reason_code == "no_ready_serial_job"
    assert _outbox_rows_except(
        scheduler_path, workload_id=child.DWD_WORKLOAD_ID
    ) == before
    assert fake.calls == []
    assert runtime.capacity_ledger.stats()["reservation_rows"] == 0
    assert runtime.execution_store.stats()["receipt_count"] == 0


def test_dwd_only_scheduler_does_not_expire_preexisting_non_dwd_deadline(tmp_path):
    clock = Clock()
    desired = desired_state.DesiredStateStore(tmp_path / "desired.sqlite3")
    desired.claim_producer(
        "ha-acquisition-runtime-test", "claim-1", claimed_at=NOW - 100.0
    )
    desired.apply(_desired_payload(), received_at=NOW - 59.0)
    scheduler_path = (tmp_path / "scheduler.sqlite3").resolve()
    scheduler = autonomous_scheduler.AutonomousScheduler(
        scheduler_path,
        desired_state_store=desired,
        plan_builder=bridge.acquisition_scheduler_plan_builder,
        clock=clock,
    )
    scheduler.reconcile(now=clock.value)
    before = _outbox_rows_except(
        scheduler_path, workload_id=child.DWD_WORKLOAD_ID
    )
    assert before
    deadlines = [float(row[9]) for row in before.values()]
    clock.value = min(deadlines) + 0.01
    fake = FakeExecutor()
    runtime = runtime_module.AcquisitionShadowRuntime(
        desired_state_store=desired,
        scheduler=scheduler,
        capacity_ledger=RawCapacityLedger(
            (tmp_path / "capacity.sqlite3").resolve(),
            profile=_profile(),
            clock=clock,
        ),
        execution_store=ExecutionReceiptStore(
            (tmp_path / "execution.sqlite3").resolve(), clock=clock
        ),
        raw_store_root=(tmp_path / "raw").resolve(),
        raw_store_limits=_dwd_raw_limits(),
        executor_factory=lambda _remaining: fake,
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        clock=clock,
    )
    runtime.tick(now=clock.value)
    assert _outbox_rows_except(
        scheduler_path, workload_id=child.DWD_WORKLOAD_ID
    ) == before
    for job in acquisition.build_worker_acquisition_slot_jobs(
        acquisition.build_worker_acquisition_plan(desired.load_active()),
        as_of=NOW,
    ):
        if job.workload_id != child.DWD_WORKLOAD_ID:
            assert runtime.execution_store.load_terminal(job.job_id) is None
            assert runtime.capacity_ledger.load(
                runtime_module._reservation_id(job)
            ) is None


def test_complete_slot_accounting_leaves_no_silent_drop_or_active_lease(tmp_path):
    runtime, scheduler, _ledger, receipts, fake, clock, _desired = _components(tmp_path)
    plan = _plan()
    active_count = sum(
        intent.workload_id in activation.DEFAULT_SHADOW_WORKLOAD_IDS
        for intent in plan.intents
    )
    results = []
    for _ in range(len(plan.intents) + 2):
        value = runtime.tick(now=clock.value)
        results.append(value)
        if value.reason_code == "no_ready_serial_job":
            break
    else:  # pragma: no cover - explicit hard bound
        raise AssertionError("serial slot did not reach a closed accounting state")
    diagnostics = scheduler.diagnostics()
    assert diagnostics["state_counts"]["completed"] == len(plan.intents)
    assert diagnostics["state_counts"]["pending"] == 0
    assert diagnostics["state_counts"]["leased"] == 0
    assert diagnostics["state_counts"]["retry"] == 0
    assert receipts.stats()["terminal_count"] == len(plan.intents)
    assert sum(item.status == "completed_shadow_raw" for item in results) == active_count
    assert sum(item.status == "blocked" for item in results) + sum(
        item.status == "not_admitted" for item in results
    ) == len(plan.intents) - active_count
    assert len(fake.calls) == active_count == 6
    for path in tmp_path.rglob("*"):
        if path.is_file():
            assert SECRET_SENTINEL.encode() not in path.read_bytes()


def test_all_new_multi_role_handlers_cross_the_serial_runtime_and_recovery_boundary(
    tmp_path,
):
    payload = _multi_region_desired_payload()
    runtime, scheduler, ledger, receipts, fake, clock, _desired = _components(
        tmp_path,
        desired_payload=payload,
    )
    plan = acquisition.build_worker_acquisition_plan(
        desired_state.parse_desired_state(payload)
    )
    jobs = acquisition.build_worker_acquisition_slot_jobs(plan, as_of=NOW)
    new_workloads = {
        child.DPC_WORKLOAD_ID,
        child.NOAA_ERR_WORKLOAD_ID,
        child.GOES18_WORKLOAD_ID,
        child.GOES19_WORKLOAD_ID,
        child.METOFFICE_WORKLOAD_ID,
        child.HSAF_H40B_WORKLOAD_ID,
    }
    planned = {job.workload_id: job for job in jobs}
    assert new_workloads <= set(planned)

    for _ in range(len(plan.intents) + 2):
        result = runtime.tick(now=clock.value)
        if result.reason_code == "no_ready_serial_job":
            break
    else:  # pragma: no cover - explicit hard bound
        raise AssertionError("multi-region serial slot did not close")

    executed = {call[1]["workload_id"] for call in fake.calls}
    assert new_workloads <= executed
    for workload_id in new_workloads:
        job = planned[workload_id]
        rule = activation.ACTIVATION_RULES[workload_id]
        reservation = ledger.load(runtime_module._reservation_id(job))
        assert reservation is not None and reservation.status == "COMMITTED"
        assert reservation.provider == rule.provider_id
        assert reservation.actual_bytes == 123
        assert reservation.actual_entries == rule.maximum_artifacts
    assert scheduler.diagnostics()["active_lease"] is False
    assert receipts.stats()["terminal_count"] == len(plan.intents)


def test_opera_is_terminally_accounted_without_provider_dispatch(tmp_path):
    runtime, scheduler, ledger, receipts, fake, clock, _desired = _components(
        tmp_path,
        allowed_workload_ids=frozenset({"radar.opera.dbzh"}),
        capacity_profile=_profile_with_opera(),
        raw_store_limits=_opera_raw_limits(),
    )
    assert not any(
        job.workload_id == "radar.kmi.radqpe_native"
        for job in _jobs()
    )
    opera, _ = _run_until(runtime, "radar.opera.dbzh", now=clock.value)
    assert opera.status == "blocked" and opera.terminal
    assert opera.reason_code == "provider_raw_retention_approval_required"
    assert all(call[1]["workload_id"] != "radar.opera.dbzh" for call in fake.calls)
    assert ledger.load(runtime_module._reservation_id(_job("radar.opera.dbzh"))) is None
    assert receipts.load_terminal(_job("radar.opera.dbzh").job_id) is not None
    assert scheduler.diagnostics()["active_lease"] is False


def test_opera_authority_executes_child_and_commits_fixed_raw_capture(
    monkeypatch,
    tmp_path,
):
    payload = b"opaque-opera-authorized-runtime-capture"
    revision = opera_native.OperaRevisionIdentity(
        etag='"0123456789abcdef0123456789abcdef"',
        version_id=None,
        content_length=len(payload),
        content_type="application/x-hdf5",
    )
    calls = {"head": 0, "download": 0}

    def head(_object_key, *, deadline):
        assert deadline > 0
        calls["head"] += 1
        return revision

    def download(_object_key, observed, *, deadline, destination_path):
        assert observed == revision and deadline > 0
        calls["download"] += 1
        path = Path(destination_path)
        path.write_bytes(payload)
        return opera_native.OperaDownloadedFile(
            path=str(path),
            size_bytes=len(payload),
            content_sha256=hashlib.sha256(payload).hexdigest(),
            content_type=revision.content_type,
            received_at=NOW,
            etag=revision.etag,
            version_id=revision.version_id,
        )

    monkeypatch.setattr(opera_native, "_head_hdf5", head)
    monkeypatch.setattr(opera_native, "_download_hdf5", download)

    class OperaExecutor:
        def __init__(self):
            self.calls = []

        def execute(
            self,
            *,
            target,
            request,
            effect_contract,
            cancel_event=None,
            environment_overrides=None,
        ):
            del cancel_event, environment_overrides
            self.calls.append((target, request, effect_contract))
            return DecoderProcessResult(
                "succeeded",
                result=child.execute_shadow_capture(
                    request,
                    _clock=lambda: NOW,
                ),
            )

    executor = OperaExecutor()
    runtime, scheduler, ledger, receipts, _fake, clock, _desired = _components(
        tmp_path,
        executor=executor,
        allowed_workload_ids=frozenset({"radar.opera.dbzh"}),
        capacity_profile=_profile_with_opera(),
        raw_store_limits=_opera_raw_limits(),
        authorization_requirement_ids=(
            "worker_authority.opera_raw_retention_v1",
        ),
    )
    result, _ = _run_until(
        runtime,
        "radar.opera.dbzh",
        now=clock.value,
    )

    job = _job("radar.opera.dbzh")
    reservation = ledger.load(runtime_module._reservation_id(job))
    assert result.status == "completed_shadow_raw" and result.terminal
    assert result.reason_code == "shadow_raw_capture_complete"
    assert calls == {"head": 1, "download": 1}
    assert len(executor.calls) == 1
    assert reservation is not None and reservation.status == "COMMITTED"
    assert reservation.actual_bytes == len(payload)
    assert reservation.actual_entries == 1
    assert receipts.load_terminal(job.job_id) is not None
    store = RawArtifactStore(
        (tmp_path / "raw").resolve(),
        limits=_opera_raw_limits(),
    )
    store.initialize()
    receipt = next(
        item
        for item in store.accounting_receipts()
        if item.provider == "opera"
    )
    capture = next(
        item
        for item in store.accounting_captures()
        if item.provider == "opera"
    )
    assert receipt.fetch_id == f"{job.job_id}:hdf5"
    assert capture.fetch_id == f"{job.job_id}:capture"
    assert scheduler.diagnostics()["active_lease"] is False


def test_netatmo_raw_capture_uses_parent_brokered_ephemeral_child_env(tmp_path):
    token = "netatmo-one-shot-access-token-9137"
    observed = {}

    class NetatmoObservingExecutor:
        calls = []

        def execute(
            self,
            *,
            target,
            request,
            effect_contract,
            cancel_event=None,
            environment_overrides=None,
        ):
            del cancel_event
            self.calls.append((target, request, effect_contract))
            observed["keys"] = tuple(sorted(environment_overrides or {}))
            observed["token_sha256"] = hashlib.sha256(
                (environment_overrides or {})[
                    "STV3_NETATMO_ACCESS_TOKEN"
                ].encode("ascii")
            ).hexdigest()
            return DecoderProcessResult(
                "succeeded",
                result=_materialize_netatmo_capture(
                    request,
                    access_token=token,
                ),
            )

    broker_calls = []

    def broker(payload, deadline):
        broker_calls.append((dict(payload), deadline))
        return token

    fake = NetatmoObservingExecutor()
    runtime, scheduler, _ledger, receipts, _fake, clock, _desired = _components(
        tmp_path,
        executor=fake,
        allowed_workload_ids=frozenset({"ground.netatmo.region"}),
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
        netatmo_access_token_broker=broker,
        capacity_profile=_profile_with_netatmo(),
        raw_store_limits=replace(
            _raw_limits(),
            max_object_bytes=8 * 1024 * 1024,
            max_capture_members=1,
        ),
    )

    result, _ = _run_until(
        runtime,
        "ground.netatmo.region",
        now=clock.value,
    )

    assert result.status == "completed_shadow_raw"
    assert len(broker_calls) == 1
    assert set(broker_calls[0][0]) == {
        "latitude",
        "longitude",
        "query_key",
        "query_kind",
        "radius_km",
    }
    assert observed == {
        "keys": ("STV3_NETATMO_ACCESS_TOKEN",),
        "token_sha256": hashlib.sha256(token.encode("ascii")).hexdigest(),
    }
    request = fake.calls[0][1]
    assert token not in json.dumps(request, sort_keys=True)
    assert token not in json.dumps(runtime.diagnostics(), sort_keys=True)
    assert token.encode("ascii") not in scheduler.path.read_bytes()
    assert token.encode("ascii") not in receipts.path.read_bytes()


def test_netatmo_parent_broker_failure_is_secret_free_and_does_not_spawn(
    tmp_path,
):
    response_secret = "provider-response-secret-4421"
    fake = FakeExecutor()

    def failing_broker(_payload, _deadline):
        raise RuntimeError(f"OAuth provider body {response_secret}")

    runtime, scheduler, _ledger, receipts, _fake, clock, _desired = _components(
        tmp_path,
        executor=fake,
        allowed_workload_ids=frozenset({"ground.netatmo.region"}),
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
        netatmo_access_token_broker=failing_broker,
        capacity_profile=_profile_with_netatmo(),
        raw_store_limits=replace(
            _raw_limits(),
            max_object_bytes=8 * 1024 * 1024,
            max_capture_members=1,
        ),
    )

    result, _ = _run_until(
        runtime,
        "ground.netatmo.region",
        now=clock.value,
    )

    assert result.status == "retry_scheduled"
    assert result.reason_code == "netatmo_oauth_broker_unavailable"
    assert fake.calls == []
    assert response_secret not in json.dumps(runtime.diagnostics(), sort_keys=True)
    assert response_secret.encode("ascii") not in scheduler.path.read_bytes()
    assert response_secret.encode("ascii") not in receipts.path.read_bytes()


def test_eumetsat_raw_capture_gets_only_ephemeral_parent_broker_env(tmp_path):
    token = "eumetsat-one-shot-access-token-7319"
    observed = {}

    class EumetsatObservingExecutor:
        calls = []

        def execute(
            self,
            *,
            target,
            request,
            effect_contract,
            cancel_event=None,
            environment_overrides=None,
        ):
            del cancel_event
            self.calls.append((target, request, effect_contract))
            observed["keys"] = tuple(sorted(environment_overrides or {}))
            observed["token_sha256"] = hashlib.sha256(
                (environment_overrides or {})[
                    "STV3_EUMETSAT_EPHEMERAL_ACCESS_TOKEN"
                ].encode("ascii")
            ).hexdigest()
            return DecoderProcessResult(
                "decoder_failed",
                detail_code="synthetic_provider_failure",
            )

    deadlines = []

    def broker(deadline):
        deadlines.append(deadline)
        return token

    fake = EumetsatObservingExecutor()
    runtime, scheduler, _ledger, receipts, _fake, clock, _desired = _components(
        tmp_path,
        executor=fake,
        allowed_workload_ids=frozenset({"lightning.eumetsat.li"}),
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
        eumetsat_access_token_broker=broker,
        capacity_profile=_profile_with_eumetsat(),
        raw_store_limits=replace(
            _raw_limits(),
            max_object_bytes=14 * 1024 * 1024,
            max_capture_members=2,
        ),
    )

    result, _ = _run_until(
        runtime,
        "lightning.eumetsat.li",
        now=clock.value,
    )

    assert result.status == "retry_scheduled"
    assert len(deadlines) == 1
    assert 0 < deadlines[0] - clock.value <= 50.0
    assert observed == {
        "keys": ("STV3_EUMETSAT_EPHEMERAL_ACCESS_TOKEN",),
        "token_sha256": hashlib.sha256(token.encode("ascii")).hexdigest(),
    }
    request = fake.calls[0][1]
    assert request["execution_deadline_at"] == deadlines[0]
    assert "execution_deadline_at" not in request["handler_payload"]
    assert token not in json.dumps(request, sort_keys=True)
    assert token not in json.dumps(runtime.diagnostics(), sort_keys=True)
    assert token.encode("ascii") not in scheduler.path.read_bytes()
    assert token.encode("ascii") not in receipts.path.read_bytes()


@pytest.mark.parametrize(
    ("error_type", "expected_status", "expected_reason"),
    (
        (
            runtime_module.ParentCredentialTemporaryError,
            "retry_scheduled",
            "eumetsat_oauth_temporary_unavailable",
        ),
        (
            runtime_module.ParentCredentialPermanentError,
            "blocked",
            "eumetsat_oauth_contract_rejected",
        ),
    ),
)
def test_eumetsat_parent_broker_failure_is_fixed_code_and_never_spawns(
    tmp_path,
    error_type,
    expected_status,
    expected_reason,
):
    response_secret = "eumetsat-private-provider-detail-5512"
    fake = FakeExecutor()

    def failing_broker(_deadline):
        raise error_type(response_secret)

    runtime, scheduler, _ledger, receipts, _fake, clock, _desired = _components(
        tmp_path,
        executor=fake,
        allowed_workload_ids=frozenset({"lightning.eumetsat.li"}),
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
        eumetsat_access_token_broker=failing_broker,
        capacity_profile=_profile_with_eumetsat(),
        raw_store_limits=replace(
            _raw_limits(),
            max_object_bytes=14 * 1024 * 1024,
            max_capture_members=2,
        ),
    )

    result, _ = _run_until(
        runtime,
        "lightning.eumetsat.li",
        now=clock.value,
    )

    assert result.status == expected_status
    assert result.reason_code == expected_reason
    assert fake.calls == []
    assert response_secret not in json.dumps(runtime.diagnostics(), sort_keys=True)
    assert response_secret.encode("ascii") not in scheduler.path.read_bytes()
    assert response_secret.encode("ascii") not in receipts.path.read_bytes()


def test_process_failure_retries_same_lease_job_and_capacity_reservation(tmp_path):
    fake = FakeExecutor([
        DecoderProcessResult("deadline_exceeded", detail_code="hard_wall_deadline"),
        DecoderProcessResult("succeeded", result=_success_evidence(
            handler_id=child.KNMI_HANDLER_ID,
            workload_id=child.KNMI_WORKLOAD_ID,
            role="hdf5",
            actual_bytes=222,
        )),
    ])
    runtime, _scheduler, ledger, _receipts, _fake, clock, _desired = _components(
        tmp_path,
        executor=fake,
        allowed_workload_ids=frozenset({child.KNMI_WORKLOAD_ID}),
        raw_store_limits=replace(
            _raw_limits(),
            max_object_bytes=40 * 1024 * 1024,
        ),
    )
    first, _ = _run_until(runtime, "radar.knmi.rtcor")
    assert first.status == "retry_scheduled"
    assert first.reason_code == "process_deadline_exceeded"
    assert ledger.stats()["reserved_count"] == 1
    clock.value += 1.0
    second, _ = _run_until(runtime, "radar.knmi.rtcor", now=clock.value)
    assert second.status == "completed_shadow_raw"
    assert ledger.stats()["reservation_rows"] == 1
    assert ledger.stats()["committed_bytes"] == 222
    assert [call[1]["acquisition_job_id"] for call in fake.calls] == [
        call[1]["acquisition_job_id"] for call in fake.calls[:1]
    ] * 2


def test_terminal_receipt_replays_completion_after_scheduler_crash(tmp_path, monkeypatch):
    runtime, scheduler, ledger, receipts, fake, clock, desired = _components(tmp_path)
    original_complete = scheduler.complete
    fail = {"armed": True}

    def crash_after_receipt(*args, **kwargs):
        if fail["armed"] and fake.calls:
            fail["armed"] = False
            raise RuntimeError("synthetic parent crash before scheduler completion")
        return original_complete(*args, **kwargs)

    monkeypatch.setattr(scheduler, "complete", crash_after_receipt)
    with pytest.raises(RuntimeError, match="synthetic parent crash"):
        _run_until(runtime, "radar.knmi.rtcor")
    assert len(fake.calls) == 1
    assert ledger.stats()["committed_count"] == 1
    assert receipts.stats()["terminal_count"] > 0

    # Recreate the coordinator around the same durable stores.  Once the old
    # lease expires and its backoff passes, the terminal head closes the new
    # lease without contacting the provider again.
    clock.value += 4.0
    reopened_scheduler = autonomous_scheduler.AutonomousScheduler(
        tmp_path / "scheduler.sqlite3",
        desired_state_store=desired,
        plan_builder=bridge.acquisition_scheduler_plan_builder,
        clock=clock,
        lease_seconds=2.0,
        retry_base_seconds=0.1,
        retry_max_seconds=1.0,
    )
    reopened = runtime_module.AcquisitionShadowRuntime(
        desired_state_store=desired,
        scheduler=reopened_scheduler,
        capacity_ledger=ledger,
        execution_store=receipts,
        raw_store_root=(tmp_path / "raw").resolve(),
        raw_store_limits=_raw_limits(),
        secret_resolver=activation.MappingSecretResolver({
            key: SECRET_SENTINEL for key in acquisition.KNOWN_SECRET_REQUIREMENT_IDS
        }),
        executor_factory=lambda _remaining: fake,
        clock=clock,
    )
    # First reconcile recovers the lease and schedules retry; advance through
    # the small durable backoff if necessary.
    reopened.tick(now=clock.value)
    clock.value += 1.0
    replay, _ = _run_until(reopened, "radar.knmi.rtcor", now=clock.value)
    assert replay.status == "completion_replayed"
    assert sum(
        call[1]["workload_id"] == "radar.knmi.rtcor" for call in fake.calls
    ) == 1


def test_child_killed_after_durable_capture_is_recovered_without_refetch(tmp_path):
    class CaptureThenDieExecutor:
        def __init__(self):
            self.calls = []

        def execute(self, *, target, request, effect_contract, cancel_event=None):
            self.calls.append((target, request, effect_contract))
            _materialize_fake_capture(request, actual_bytes=317)
            return DecoderProcessResult(
                "deadline_exceeded",
                detail_code="synthetic_kill_after_raw_capture",
            )

    executor = CaptureThenDieExecutor()
    runtime, _scheduler, ledger, receipts, _fake, clock, _desired = _components(
        tmp_path,
        executor=executor,
    )
    result, _ = _run_until(runtime, "radar.dwd.radolan_rv", now=clock.value)
    assert result.status == "completed_shadow_raw"
    assert result.reason_code == "shadow_raw_capture_recovered"
    assert sum(
        call[1]["workload_id"] == "radar.dwd.radolan_rv"
        for call in executor.calls
    ) == 1
    reservation = ledger.load(
        runtime_module._reservation_id(_job("radar.dwd.radolan_rv"))
    )
    assert reservation is not None and reservation.status == "COMMITTED"
    terminal = receipts.load_terminal(_job("radar.dwd.radolan_rv").job_id)
    assert terminal is not None and terminal.outcome == "completed_shadow_raw"


def test_parent_crash_after_capacity_commit_restarts_without_provider_call(
    tmp_path,
    monkeypatch,
):
    runtime, scheduler, ledger, receipts, fake, clock, desired = _components(tmp_path)
    original_record = runtime._record
    crashed = {"workload_id": None}

    def crash_before_execution_receipt(bound, claimed, **kwargs):
        if kwargs.get("outcome") == "completed_shadow_raw":
            crashed["workload_id"] = bound.acquisition_job.workload_id
            raise RuntimeError("synthetic crash after capacity commit")
        return original_record(bound, claimed, **kwargs)

    monkeypatch.setattr(runtime, "_record", crash_before_execution_receipt)
    with pytest.raises(RuntimeError, match="after capacity commit"):
        for _ in range(100):
            runtime.tick(now=clock.value)
    workload_id = crashed["workload_id"]
    assert workload_id in {
        "ground.metar.global",
        "radar.dwd.radolan_rv",
        "radar.knmi.rtcor",
    }
    job = _job(workload_id)
    assert ledger.load(runtime_module._reservation_id(job)).status == "COMMITTED"
    assert receipts.load_terminal(job.job_id) is None
    calls_before = sum(
        call[1]["workload_id"] == workload_id for call in fake.calls
    )
    assert calls_before == 1

    clock.value += 4.0
    reopened_scheduler = autonomous_scheduler.AutonomousScheduler(
        tmp_path / "scheduler.sqlite3",
        desired_state_store=desired,
        plan_builder=bridge.acquisition_scheduler_plan_builder,
        clock=clock,
        lease_seconds=2.0,
        retry_base_seconds=0.1,
        retry_max_seconds=1.0,
    )
    reopened = runtime_module.AcquisitionShadowRuntime(
        desired_state_store=desired,
        scheduler=reopened_scheduler,
        capacity_ledger=ledger,
        execution_store=receipts,
        raw_store_root=(tmp_path / "raw").resolve(),
        raw_store_limits=_raw_limits(),
        # Recovery of exact durable raw evidence is intentionally independent
        # from current secret availability.
        secret_resolver=activation.NullSecretResolver(),
        executor_factory=lambda _remaining: fake,
        clock=clock,
    )
    reopened.tick(now=clock.value)
    clock.value += 1.0
    result, _ = _run_until(reopened, workload_id, now=clock.value)
    assert result.status == "completed_shadow_raw"
    assert result.reason_code == "shadow_raw_capture_recovered"
    assert sum(
        call[1]["workload_id"] == workload_id for call in fake.calls
    ) == calls_before


def test_tampered_child_evidence_never_becomes_a_success_claim(tmp_path):
    class TamperedEvidenceExecutor:
        def __init__(self):
            self.calls = []

        def execute(self, *, target, request, effect_contract, cancel_event=None):
            self.calls.append((target, request, effect_contract))
            evidence = _materialize_fake_capture(request, actual_bytes=197)
            evidence["content_sha256"] = "f" * 64
            checksum_input = dict(evidence)
            checksum_input.pop("contract")
            checksum_input.pop("evidence_sha256")
            evidence["evidence_sha256"] = hashlib.sha256(
                _canonical(checksum_input)
            ).hexdigest()
            return DecoderProcessResult("succeeded", result=evidence)

    executor = TamperedEvidenceExecutor()
    runtime, _scheduler, ledger, receipts, _fake, clock, _desired = _components(
        tmp_path,
        executor=executor,
    )
    result, _ = _run_until(runtime, "radar.dwd.radolan_rv", now=clock.value)
    assert result.status == "blocked"
    assert result.reason_code == "handler_evidence_commit_failed"
    job = _job("radar.dwd.radolan_rv")
    # Exact raw bytes remain safely charged even though the untrusted child
    # message was rejected and no success receipt was published.
    assert ledger.load(runtime_module._reservation_id(job)).status == "COMMITTED"
    terminal = receipts.load_terminal(job.job_id)
    assert terminal is not None and terminal.outcome == "blocked_permanent"
    assert terminal.content_sha256 is None


def _dynamic_glm_child_result(roles: list[str]) -> tuple[
    acquisition.AcquisitionJob,
    activation.ActivationDecision,
    DecoderProcessResult,
]:
    job = _job(child.GOES19_GLM_WORKLOAD_ID)
    decision = activation.assess_acquisition_job(
        job,
        resolver=activation.NullSecretResolver(),
    )
    assert decision.executable is True
    evidence = {
        "actual_bytes": 123,
        "actual_entries": len(roles),
        "capture_sha256": "a" * 64,
        "content_sha256": "b" * 64,
        "handler_id": decision.rule.handler_id,
        "operational_admission_eligible": False,
        "receipt_sha256": "c" * 64,
        "roles": roles,
        "status": "captured_shadow_raw",
        "workload_id": job.workload_id,
    }
    result = {
        "contract": child.RESULT_CONTRACT,
        **evidence,
        "evidence_sha256": hashlib.sha256(_canonical(evidence)).hexdigest(),
    }
    return job, decision, DecoderProcessResult("succeeded", result=result)


def test_dynamic_glm_child_roles_cross_parent_validation_boundary():
    roles = ["listing_previous", "listing_current", "netcdf_00", "netcdf_01"]
    job, decision, process = _dynamic_glm_child_result(roles)

    validated = runtime_module.AcquisitionShadowRuntime._validate_child_result(
        process,
        job,
        decision,
    )

    assert validated["roles"] == roles
    assert validated["actual_entries"] == 4


def test_dynamic_glm_child_roles_reject_a_non_contiguous_family():
    roles = ["listing_previous", "listing_current", "netcdf_01"]
    job, decision, process = _dynamic_glm_child_result(roles)

    with pytest.raises(
        runtime_module.AcquisitionRuntimeError,
        match="role set drifted",
    ):
        runtime_module.AcquisitionShadowRuntime._validate_child_result(
            process,
            job,
            decision,
        )


def test_pending_deadline_receipt_is_durable_before_scheduler_expiry(
    tmp_path, monkeypatch
):
    runtime, scheduler, _ledger, receipts, _fake, clock, _desired = _components(
        tmp_path
    )
    scheduler.reconcile(now=NOW)
    job = _job("radar.dwd.radolan_rv")
    scheduler_job_id = _scheduler_job_id(job)
    assert scheduler.get_record(scheduler_job_id).state == "pending"
    clock.value = NOW + 245.0
    original_reconcile = scheduler.reconcile

    def assert_receipt_precedes_expiry(
        *, now=None, allowed_workload_ids=None
    ):
        terminal = receipts.load_terminal(job.job_id)
        assert terminal is not None
        assert terminal.reason_code == "deadline_expired"
        assert terminal.attempt_count == 0
        assert scheduler.get_record(scheduler_job_id).state == "pending"
        return original_reconcile(
            now=now, allowed_workload_ids=allowed_workload_ids
        )

    monkeypatch.setattr(scheduler, "reconcile", assert_receipt_precedes_expiry)
    runtime.tick(now=clock.value)
    assert scheduler.get_record(scheduler_job_id).state == "expired"


def test_never_inserted_missed_slot_gets_zero_attempt_receipt_before_reconcile(
    tmp_path, monkeypatch
):
    runtime, scheduler, _ledger, receipts, _fake, clock, _desired = _components(
        tmp_path
    )
    job = _job("radar.dwd.radolan_rv")
    scheduler_job_id = _scheduler_job_id(job)
    assert scheduler.get_record(scheduler_job_id) is None
    clock.value = NOW + 245.0
    original_reconcile = scheduler.reconcile

    def assert_missed_receipt_precedes_reconcile(
        *, now=None, allowed_workload_ids=None
    ):
        terminal = receipts.load_terminal(job.job_id)
        assert terminal is not None
        assert terminal.reason_code == "deadline_expired"
        assert terminal.attempt_count == 0
        assert scheduler.get_record(scheduler_job_id) is None
        return original_reconcile(
            now=now, allowed_workload_ids=allowed_workload_ids
        )

    monkeypatch.setattr(scheduler, "reconcile", assert_missed_receipt_precedes_reconcile)
    runtime.tick(now=clock.value)
    assert scheduler.get_record(scheduler_job_id) is None
    assert receipts.load_terminal(job.job_id).attempt_count == 0


def test_deadline_receipt_replays_after_crash_before_scheduler_expiry(
    tmp_path, monkeypatch
):
    runtime, scheduler, _ledger, receipts, _fake, clock, _desired = _components(
        tmp_path
    )
    scheduler.reconcile(now=NOW)
    job = _job("radar.dwd.radolan_rv")
    scheduler_job_id = _scheduler_job_id(job)
    clock.value = NOW + 245.0
    original_reconcile = scheduler.reconcile

    def crash_after_deadline_receipt(
        *, now=None, allowed_workload_ids=None
    ):
        del now, allowed_workload_ids
        assert receipts.load_terminal(job.job_id) is not None
        raise RuntimeError("synthetic crash before scheduler expiry")

    monkeypatch.setattr(scheduler, "reconcile", crash_after_deadline_receipt)
    with pytest.raises(RuntimeError, match="synthetic crash"):
        runtime.tick(now=clock.value)
    first = receipts.load_terminal(job.job_id)
    assert first is not None and first.reason_code == "deadline_expired"
    assert scheduler.get_record(scheduler_job_id).state == "pending"

    monkeypatch.setattr(scheduler, "reconcile", original_reconcile)
    runtime.tick(now=clock.value)
    replay = receipts.load_terminal(job.job_id)
    assert replay == first
    assert scheduler.get_record(scheduler_job_id).state == "expired"


def test_process_crossing_deadline_records_terminal_before_retry_expires(
    tmp_path, monkeypatch
):
    clock = Clock()

    class CrossingExecutor:
        def __init__(self):
            self.calls = []

        def execute(self, *, target, request, effect_contract, cancel_event=None):
            self.calls.append((target, request, effect_contract))
            clock.value = NOW + 241.0
            return DecoderProcessResult(
                "deadline_exceeded", detail_code="hard_wall_deadline"
            )

    executor = CrossingExecutor()
    runtime, scheduler, ledger, receipts, _fake, _clock, _desired = _components(
        tmp_path, executor=executor, clock=clock
    )
    job = _job("radar.knmi.rtcor")
    original_retry = scheduler.retry

    def assert_receipt_precedes_retry(job_id, lease_token, error_code, *, now=None):
        terminal = receipts.load_terminal(job.job_id)
        assert terminal is not None
        assert terminal.reason_code == "deadline_expired"
        return original_retry(job_id, lease_token, error_code, now=now)

    monkeypatch.setattr(scheduler, "retry", assert_receipt_precedes_retry)
    result, _ = _run_until(runtime, "radar.knmi.rtcor", now=NOW)
    assert result.status == "blocked" and result.terminal
    assert result.reason_code == "deadline_expired"
    assert scheduler.get_record(_scheduler_job_id(job)).state == "expired"
    reservation = ledger.load(runtime_module._reservation_id(job))
    assert reservation is not None and reservation.status == "RESERVED"


def test_missing_requirement_is_not_retried_or_allowed_to_starve_public_capture(
    tmp_path,
):
    selected = frozenset({
        child.KNMI_WORKLOAD_ID,
        child.DWD_WORKLOAD_ID,
    })
    runtime, scheduler, _ledger, receipts, fake, clock, _desired = _components(
        tmp_path,
        allowed_workload_ids=selected,
        raw_store_limits=replace(
            _raw_limits(),
            max_object_bytes=40 * 1024 * 1024,
        ),
    )
    runtime._secret_resolver = activation.NullSecretResolver()

    missing = runtime.tick(now=clock.value)
    assert missing.workload_id == child.KNMI_WORKLOAD_ID
    assert missing.reason_code == "secret_requirement_unavailable"
    assert missing.status == "retry_scheduled"
    clock.value += 10.0
    public = runtime.tick(now=clock.value)
    assert public.workload_id == child.DWD_WORKLOAD_ID
    assert public.status == "completed_shadow_raw"
    assert [call[1]["workload_id"] for call in fake.calls] == [
        child.DWD_WORKLOAD_ID
    ]
    clock.value += 10.0
    idle = runtime.tick(now=clock.value)
    assert idle.reason_code == "no_ready_serial_job"
    knmi_job = _job(child.KNMI_WORKLOAD_ID)
    knmi_record = scheduler.get_record(_scheduler_job_id(knmi_job))
    assert knmi_record is not None
    assert knmi_record.state == "retry"
    assert knmi_record.attempt_count == 1
    assert receipts.load_terminal(knmi_job.job_id) is None
    diagnostics = runtime.diagnostics()["activation"]
    assert diagnostics["requirements_unavailable_workload_ids"] == [
        child.KNMI_WORKLOAD_ID
    ]


def test_all_sources_reserves_first_ready_slot_for_quantitative_dwd(tmp_path):
    selected = frozenset({
        child.KNMI_WORKLOAD_ID,
        child.DWD_WORKLOAD_ID,
    })
    runtime, _scheduler, _ledger, _receipts, _fake, clock, _desired = (
        _components(
            tmp_path,
            allowed_workload_ids=selected,
            collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
            raw_store_limits=replace(
                _raw_limits(),
                max_object_bytes=40 * 1024 * 1024,
            ),
        )
    )

    first = runtime.tick(now=clock.value)
    assert first.workload_id == child.DWD_WORKLOAD_ID
    assert first.status == "completed_shadow_raw"


def test_belgian_priority_lane_services_kmi_before_general_backlog(tmp_path):
    selected = frozenset({
        "ground.kmi.aws_10min",
        child.DWD_WORKLOAD_ID,
        "radar.kmi.public_wcs_unknown_quantity",
        child.KNMI_WORKLOAD_ID,
    })
    failed_attempt = DecoderProcessResult(
        "deadline_exceeded",
        detail_code="hard_wall_deadline",
    )
    executor = FakeExecutor(results=[failed_attempt] * len(selected))
    runtime, _scheduler, _ledger, _receipts, _fake, clock, _desired = (
        _components(
            tmp_path,
            executor=executor,
            allowed_workload_ids=selected,
            collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
            capacity_profile=_profile_with_kmi(),
            raw_store_limits=replace(
                _raw_limits(),
                max_object_bytes=64 * 1024 * 1024,
            ),
        )
    )

    observed = []
    for _ in range(len(selected)):
        result = runtime.tick(now=clock.value)
        observed.append(result.workload_id)
        assert result.reason_code != "deadline_expired"
        clock.value += 60.0

    assert observed == [
        "radar.kmi.public_wcs_unknown_quantity",
        child.DWD_WORKLOAD_ID,
        "ground.kmi.aws_10min",
        child.KNMI_WORKLOAD_ID,
    ]


def test_adaptive_batch_runs_distinct_kmi_products_concurrently(tmp_path):
    class ObservedExecutor(FakeExecutor):
        def __init__(self) -> None:
            super().__init__()
            self._gate = threading.Lock()
            self._kmi_barrier = threading.Barrier(2)
            self._active_kmi = 0
            self.peak_kmi = 0
            self.clock = None

        def execute(self, **kwargs):
            workload_id = kwargs["request"]["workload_id"]
            is_kmi = workload_id.startswith(("ground.kmi.", "radar.kmi."))
            if is_kmi:
                with self._gate:
                    self._active_kmi += 1
                    self.peak_kmi = max(self.peak_kmi, self._active_kmi)
            try:
                if is_kmi:
                    try:
                        self._kmi_barrier.wait(timeout=2.0)
                    except threading.BrokenBarrierError:
                        pass
                if workload_id == "ground.kmi.aws_10min":
                    # Cross the deliberately short scheduler lease while the
                    # second KMI product is still active.  A per-lane
                    # reconcile used to expire that live lease and made its
                    # later retry fail with scheduler_retry_conflict.
                    assert self.clock is not None
                    self.clock.value += 3.0
                    time.sleep(0.01)
                elif is_kmi:
                    time.sleep(0.1)
                else:
                    time.sleep(0.02)
                return DecoderProcessResult(
                    "failed", detail_code="synthetic_parallel_probe"
                )
            finally:
                if is_kmi:
                    with self._gate:
                        self._active_kmi -= 1

    selected = frozenset({
        "ground.kmi.aws_10min",
        child.DWD_WORKLOAD_ID,
        "radar.kmi.public_wcs_unknown_quantity",
        child.KNMI_WORKLOAD_ID,
    })
    executor = ObservedExecutor()
    runtime, _scheduler, _ledger, _receipts, _fake, clock, _desired = (
        _components(
            tmp_path,
            executor=executor,
            allowed_workload_ids=selected,
            collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
            capacity_profile=_profile_with_kmi(),
            raw_store_limits=replace(
                _raw_limits(),
                max_object_bytes=64 * 1024 * 1024,
            ),
        )
    )
    executor.clock = clock

    results = runtime.tick_all_due()

    assert {
        item.workload_id for item in results if item.workload_id is not None
    } == selected
    assert executor.peak_kmi == 2, [
        (item.workload_id, item.status, item.reason_code) for item in results
    ]


def test_all_sources_dwd_preemption_cannot_starve_unattempted_source(tmp_path):
    selected = frozenset({
        child.KNMI_WORKLOAD_ID,
        child.DWD_WORKLOAD_ID,
    })
    runtime, _scheduler, _ledger, _receipts, _fake, clock, _desired = (
        _components(
            tmp_path,
            allowed_workload_ids=selected,
            collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
            raw_store_limits=replace(
                _raw_limits(),
                max_object_bytes=40 * 1024 * 1024,
            ),
        )
    )

    first = runtime.tick(now=clock.value)
    assert first.workload_id == child.DWD_WORKLOAD_ID
    assert first.status == "completed_shadow_raw"

    # On a slow host the next serial tick can begin only after another DWD
    # cadence slot is ready.  The primary source may not pre-empt twice in a
    # row while another applicable source has never received a turn.
    clock.value += _job(child.DWD_WORKLOAD_ID).cadence_seconds
    second = runtime.tick(now=clock.value)
    assert second.workload_id == child.KNMI_WORKLOAD_ID
    assert second.status == "completed_shadow_raw"


def test_missing_static_authority_is_suppressed_after_one_exact_probe(tmp_path):
    selected = frozenset({
        child.DWD_WORKLOAD_ID,
        child.OPERA_WORKLOAD_ID,
    })
    runtime, _scheduler, _ledger, _receipts, fake, clock, _desired = (
        _components(
            tmp_path,
            allowed_workload_ids=selected,
            collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
            capacity_profile=_profile_with_opera(),
            raw_store_limits=_opera_raw_limits(),
        )
    )

    assert runtime.tick(now=clock.value).workload_id == child.DWD_WORKLOAD_ID
    clock.value += 1.0
    blocked = runtime.tick(now=clock.value)
    assert blocked.workload_id == child.OPERA_WORKLOAD_ID
    assert blocked.reason_code == "provider_raw_retention_approval_required"
    clock.value += 1.0
    assert runtime.tick(now=clock.value).reason_code == "no_ready_serial_job"
    assert [call[1]["workload_id"] for call in fake.calls] == [
        child.DWD_WORKLOAD_ID
    ]


def test_suppressed_requirement_job_runs_when_secret_becomes_available(
    tmp_path,
):
    runtime, scheduler, _ledger, _receipts, fake, clock, _desired = _components(
        tmp_path,
        allowed_workload_ids=frozenset({child.KNMI_WORKLOAD_ID}),
        raw_store_limits=replace(
            _raw_limits(),
            max_object_bytes=40 * 1024 * 1024,
        ),
    )
    runtime._secret_resolver = activation.NullSecretResolver()
    missing = runtime.tick(now=clock.value)
    assert missing.reason_code == "secret_requirement_unavailable"
    clock.value += 10.0
    assert runtime.tick(now=clock.value).reason_code == "no_ready_serial_job"

    runtime._secret_resolver = activation.MappingSecretResolver({
        "worker_secret.knmi_api_key": SECRET_SENTINEL,
    })
    clock.value += 10.0
    recovered = runtime.tick(now=clock.value)
    assert recovered.workload_id == child.KNMI_WORKLOAD_ID
    assert recovered.status == "completed_shadow_raw"
    assert scheduler.get_record(_scheduler_job_id(_job(child.KNMI_WORKLOAD_ID))).attempt_count == 2
    assert [call[1]["workload_id"] for call in fake.calls] == [
        child.KNMI_WORKLOAD_ID
    ]


def test_requirement_is_rechecked_after_claim_before_provider_execution(
    tmp_path,
):
    runtime, scheduler, _ledger, _receipts, fake, clock, _desired = _components(
        tmp_path,
        allowed_workload_ids=frozenset({child.KNMI_WORKLOAD_ID}),
        raw_store_limits=replace(
            _raw_limits(),
            max_object_bytes=40 * 1024 * 1024,
        ),
    )

    class VanishingResolver:
        calls = 0

        def resolve(self, requirement_id):
            assert requirement_id == "worker_secret.knmi_api_key"
            self.calls += 1
            return SECRET_SENTINEL if self.calls == 1 else None

    resolver = VanishingResolver()
    runtime._secret_resolver = resolver
    result = runtime.tick(now=clock.value)
    assert result.workload_id == child.KNMI_WORKLOAD_ID
    assert result.reason_code == "secret_requirement_unavailable"
    assert result.status == "retry_scheduled"
    assert resolver.calls == 2
    assert fake.calls == []
    record = scheduler.get_record(_scheduler_job_id(_job(child.KNMI_WORKLOAD_ID)))
    assert record is not None and record.attempt_count == 1


def test_diagnostics_are_bounded_aggregate_and_secret_coordinate_free(tmp_path):
    runtime, *_ = _components(tmp_path)
    runtime.tick(now=NOW)
    diagnostics = runtime.diagnostics()
    encoded = _canonical(diagnostics)
    assert len(encoded) <= runtime_module.MAX_DIAGNOSTIC_BYTES
    text = encoded.decode()
    assert SECRET_SENTINEL not in text
    assert "51.04" not in text and "4.48" not in text
    assert "target_ids" not in text and "region_ids" not in text
    assert diagnostics["operational_admission_eligible"] is False
    assert diagnostics["activation"]["active_shadow_raw"] == len(
        activation.RAW_CAPTURE_WORKLOAD_IDS
    )


def test_existing_unaccounted_raw_bytes_block_runtime_before_scheduler_work(tmp_path):
    root = (tmp_path / "raw").resolve()
    store = RawArtifactStore(root, limits=_raw_limits())
    store.initialize()
    store.store(
        provider="metar", product="foreign", fetch_id="foreign-1",
        content_type="application/octet-stream", received_at=NOW,
        upstream_id="foreign", provenance={
            "representation": NATIVE_REPRESENTATION,
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "foreign-test",
            "collector_version": "test-v1",
        }, payload=b"unaccounted",
    )
    desired = desired_state.DesiredStateStore(tmp_path / "desired.sqlite3")
    scheduler = autonomous_scheduler.AutonomousScheduler(
        tmp_path / "scheduler.sqlite3", desired_state_store=desired,
        plan_builder=bridge.acquisition_scheduler_plan_builder,
    )
    ledger = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(), profile=_profile()
    )
    receipts = ExecutionReceiptStore((tmp_path / "execution.sqlite3").resolve())
    with pytest.raises(runtime_module.AcquisitionRuntimeError, match="outside"):
        runtime_module.AcquisitionShadowRuntime(
            desired_state_store=desired, scheduler=scheduler,
            capacity_ledger=ledger, execution_store=receipts,
            raw_store_root=root, raw_store_limits=_raw_limits(),
        )
    assert scheduler.diagnostics()["outbox_rows"] == 0


def test_runtime_requires_capacity_for_every_active_provider_and_exact_object_cap(tmp_path):
    (tmp_path / "base").mkdir()
    _runtime, scheduler, _ledger, receipts, _fake, clock, desired = _components(
        tmp_path / "base"
    )
    missing_knmi = RawCapacityProfile(
        global_max_bytes=128 * 1024 * 1024,
        global_max_entries=100,
        providers=(
            ProviderCapacityLimit("aviationweather", 128 * 1024 * 1024, 100),
            ProviderCapacityLimit("dwd", 128 * 1024 * 1024, 100),
        ),
        max_reservations=100,
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )
    bad_ledger = RawCapacityLedger(
        (tmp_path / "missing-knmi.sqlite3").resolve(),
        profile=missing_knmi,
        clock=clock,
    )
    with pytest.raises(runtime_module.AcquisitionRuntimeError, match="every active"):
        runtime_module.AcquisitionShadowRuntime(
            desired_state_store=desired,
            scheduler=scheduler,
            capacity_ledger=bad_ledger,
            execution_store=receipts,
            raw_store_root=(tmp_path / "raw-missing-provider").resolve(),
            raw_store_limits=_raw_limits(),
            clock=clock,
        )

    good_ledger = RawCapacityLedger(
        (tmp_path / "bad-object-cap.sqlite3").resolve(),
        profile=_profile(),
        clock=clock,
    )
    too_small = replace(_raw_limits(), max_object_bytes=16 * 1024 * 1024)
    with pytest.raises(runtime_module.AcquisitionRuntimeError, match="largest active"):
        runtime_module.AcquisitionShadowRuntime(
            desired_state_store=desired,
            scheduler=scheduler,
            capacity_ledger=good_ledger,
            execution_store=receipts,
            raw_store_root=(tmp_path / "raw-small-object").resolve(),
            raw_store_limits=too_small,
            clock=clock,
        )


def test_runtime_requires_aggregate_shared_provider_and_global_batch_capacity(tmp_path):
    (tmp_path / "base").mkdir()
    _runtime, scheduler, _ledger, receipts, _fake, clock, desired = _components(
        tmp_path / "base"
    )

    base_profile = _profile()
    shared_too_small = replace(
        base_profile,
        providers=tuple(
            replace(item, max_bytes=20 * 1024 * 1024, max_entries=3)
            if item.provider == "noaa_goes"
            else item
            for item in base_profile.providers
        ),
    )
    shared_ledger = RawCapacityLedger(
        (tmp_path / "shared-provider.sqlite3").resolve(),
        profile=shared_too_small,
        clock=clock,
    )
    with pytest.raises(runtime_module.AcquisitionRuntimeError, match="provider capacity"):
        runtime_module.AcquisitionShadowRuntime(
            desired_state_store=desired,
            scheduler=scheduler,
            capacity_ledger=shared_ledger,
            execution_store=receipts,
            raw_store_root=(tmp_path / "raw-shared-provider").resolve(),
            raw_store_limits=_raw_limits(),
            clock=clock,
        )

    global_bytes = 203 * 1024 * 1024
    global_too_small = RawCapacityProfile(
        global_max_bytes=global_bytes,
        global_max_entries=100,
        providers=tuple(
            ProviderCapacityLimit(item.provider, global_bytes, 100)
            for item in base_profile.providers
        ),
        max_reservations=100,
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )
    global_ledger = RawCapacityLedger(
        (tmp_path / "global-batch.sqlite3").resolve(),
        profile=global_too_small,
        clock=clock,
    )
    with pytest.raises(runtime_module.AcquisitionRuntimeError, match="global capacity"):
        runtime_module.AcquisitionShadowRuntime(
            desired_state_store=desired,
            scheduler=scheduler,
            capacity_ledger=global_ledger,
            execution_store=receipts,
            raw_store_root=(tmp_path / "raw-global-batch").resolve(),
            raw_store_limits=_raw_limits(total=global_bytes),
            clock=clock,
        )

    raw_store_too_small = replace(
        _raw_limits(),
        max_total_bytes=203 * 1024 * 1024,
    )
    sufficient_ledger = RawCapacityLedger(
        (tmp_path / "raw-batch.sqlite3").resolve(),
        profile=_profile(),
        clock=clock,
    )
    with pytest.raises(runtime_module.AcquisitionRuntimeError, match="raw store"):
        runtime_module.AcquisitionShadowRuntime(
            desired_state_store=desired,
            scheduler=scheduler,
            capacity_ledger=sufficient_ledger,
            execution_store=receipts,
            raw_store_root=(tmp_path / "raw-batch").resolve(),
            raw_store_limits=raw_store_too_small,
            clock=clock,
        )


def test_runtime_rejects_a_filesystem_root_as_raw_store_target(tmp_path):
    desired = desired_state.DesiredStateStore(tmp_path / "desired.sqlite3")
    scheduler = autonomous_scheduler.AutonomousScheduler(
        tmp_path / "scheduler.sqlite3", desired_state_store=desired,
        plan_builder=bridge.acquisition_scheduler_plan_builder,
    )
    ledger = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(), profile=_profile()
    )
    receipts = ExecutionReceiptStore((tmp_path / "execution.sqlite3").resolve())
    drive_root = Path(tmp_path.anchor)
    with pytest.raises(ValueError, match="specific"):
        runtime_module.AcquisitionShadowRuntime(
            desired_state_store=desired, scheduler=scheduler,
            capacity_ledger=ledger, execution_store=receipts,
            raw_store_root=drive_root, raw_store_limits=_raw_limits(),
        )


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            values.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            values.add(node.module)
    return values


def test_raw_capture_candidate_is_live_wired_without_snapshot_publication():
    root = Path(__file__).parents[1]
    service = root / "storm_tracker_worker" / "service.py"
    imports = _imports(service)
    assert imports & {
        "storm_tracker_worker.acquisition_runtime",
        "acquisition_runtime",
    }
    assert imports & {
        "storm_tracker_worker.acquisition_activation",
        "acquisition_activation",
    }
    direct_child_imports = {
        "storm_tracker_worker.acquisition_handler_child",
        "acquisition_handler_child",
    }
    assert imports.isdisjoint(direct_child_imports)
    service_source = service.read_text(encoding="utf-8")
    assert "RAW_CAPTURE_SHADOW_MODE" in service_source
    assert '"worker_first_runtime_factory": _raw_capture_runtime_factory' in (
        service_source
    )
    source = Path(runtime_module.__file__).read_text(encoding="utf-8")
    assert "operational_snapshot" not in source
    assert "mqtt" not in source.lower()


_EXECUTION_STORE_NATIVE_CRASH_POINTS = (
    "record.after_receipt",
    "record.after_terminal_head",
    "record.before_commit",
)


def _execution_store_terminal_request() -> dict:
    return {
        "scheduler_job_id": "autosched:" + "a" * 64,
        "acquisition_job_id": "acquisition:" + "b" * 64,
        "binding_sha256": "c" * 64,
        "attempt_count": 1,
        "outcome": "completed_shadow_raw",
        "reason_code": "capture_committed",
        "terminal": True,
        "content_sha256": "d" * 64,
        "raw_receipt_sha256": "e" * 64,
        "capture_sha256": "f" * 64,
        "capacity_commit_sha256": "1" * 64,
    }


def _execution_store_record_faultpoints() -> tuple[str, ...]:
    source = Path(execution_store_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(execution_store_module.__file__))
    store_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "ExecutionReceiptStore"
    )
    record = next(
        node
        for node in store_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "record"
    )
    calls = tuple(
        call
        for call in ast.walk(record)
        if (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "self"
            and call.func.attr == "_fault"
            and len(call.args) == 1
            and isinstance(call.args[0], ast.Constant)
            and isinstance(call.args[0].value, str)
        )
    )
    return tuple(
        str(call.args[0].value)
        for call in sorted(calls, key=lambda item: (item.lineno, item.col_offset))
    )


def test_execution_store_native_crash_inventory_pins_every_record_occurrence():
    discovered = _execution_store_record_faultpoints()
    assert discovered == _EXECUTION_STORE_NATIVE_CRASH_POINTS
    assert len(discovered) == len(set(discovered)) == 3


def test_terminal_provider_result_survives_runtime_restart(tmp_path):
    failed = DecoderProcessResult("succeeded", result=None)
    runtime, _scheduler, _ledger, _receipts, _fake, clock, _desired = (
        _components(
            tmp_path,
            executor=FakeExecutor([failed]),
            allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
            raw_store_limits=_dwd_raw_limits(),
        )
    )

    result, _values = _run_until(
        runtime, child.DWD_WORKLOAD_ID, now=NOW
    )
    assert result.terminal is True
    assert result.reason_code == "handler_evidence_commit_failed"
    expected = ((
        child.DWD_WORKLOAD_ID,
        "blocked",
        "handler_evidence_commit_failed",
    ),)
    assert runtime.latest_terminal_results() == expected

    desired = desired_state.DesiredStateStore(tmp_path / "desired.sqlite3")
    reopened = runtime_module.AcquisitionShadowRuntime(
        desired_state_store=desired,
        scheduler=autonomous_scheduler.AutonomousScheduler(
            tmp_path / "scheduler.sqlite3",
            desired_state_store=desired,
            plan_builder=bridge.acquisition_scheduler_plan_builder,
            clock=clock,
            lease_seconds=2.0,
            retry_base_seconds=0.1,
            retry_max_seconds=1.0,
        ),
        capacity_ledger=RawCapacityLedger(
            (tmp_path / "capacity.sqlite3").resolve(),
            profile=_profile(),
            clock=clock,
        ),
        execution_store=ExecutionReceiptStore(
            (tmp_path / "execution.sqlite3").resolve(),
            clock=clock,
        ),
        raw_store_root=(tmp_path / "raw").resolve(),
        raw_store_limits=_dwd_raw_limits(),
        secret_resolver=activation.MappingSecretResolver({
            key: SECRET_SENTINEL
            for key in acquisition.KNOWN_SECRET_REQUIREMENT_IDS
        }),
        executor_factory=lambda _remaining: FakeExecutor(),
        allowed_workload_ids=frozenset({child.DWD_WORKLOAD_ID}),
        clock=clock,
    )

    assert reopened.latest_terminal_results() == expected
    row = reopened.diagnostics()["activation"]["configured_workloads"][0]
    assert row["last_terminal"] == {
        "reason_code": "handler_evidence_commit_failed",
        "status": "blocked",
    }


@pytest.mark.parametrize("checkpoint", _EXECUTION_STORE_NATIVE_CRASH_POINTS)
def test_execution_store_native_crash_is_old_then_retry_and_replay_are_exact(
    tmp_path: Path,
    checkpoint: str,
) -> None:
    database = (tmp_path / checkpoint.replace(".", "-") / "execution.sqlite3").resolve()
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_acquisition_execution_store_crash_child.py"
    )
    environment = dict(os.environ)
    root = str(Path(__file__).resolve().parents[1])
    environment["PYTHONPATH"] = os.pathsep.join(
        entry
        for entry in (root, environment.get("PYTHONPATH", ""))
        if entry
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    crashed = subprocess.run(
        [sys.executable, str(helper), str(database), checkpoint],
        cwd=root,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    request = _execution_store_terminal_request()
    acquisition_job = request["acquisition_job_id"]
    reopened = ExecutionReceiptStore(database, clock=lambda: NOW + 10.0)
    assert reopened.load_terminal(acquisition_job) is None
    assert reopened.load_job_receipts(acquisition_job) == ()
    old_stats = reopened.stats()
    assert old_stats["receipt_count"] == 0
    assert old_stats["terminal_count"] == 0
    assert set(old_stats["outcomes"].values()) == {0}

    committed = reopened.record(**request)
    assert committed.terminal is True
    assert committed.created_at == NOW + 10.0
    assert reopened.load_terminal(acquisition_job) == committed
    assert reopened.load_job_receipts(acquisition_job) == (committed,)
    committed_stats = reopened.stats()
    assert committed_stats["receipt_count"] == 1
    assert committed_stats["terminal_count"] == 1
    assert committed_stats["outcomes"]["completed_shadow_raw"] == 1

    replay_store = ExecutionReceiptStore(database, clock=lambda: NOW + 999.0)
    replay = replay_store.record(**request)
    assert replay == committed
    assert replay.receipt_sha256 == committed.receipt_sha256
    assert replay.created_at == NOW + 10.0
    assert replay_store.stats() == committed_stats
    assert replay_store.terminal_receipts() == (committed,)
    page = replay_store.terminal_receipt_page(after_cursor=0, max_rows=1)
    assert page.receipts == (committed,)
    assert page.exhausted is True

    raw = sqlite3.connect(database)
    try:
        assert raw.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert raw.execute("SELECT count(*) FROM execution_receipts").fetchone()[0] == 1
        assert raw.execute("SELECT count(*) FROM terminal_heads").fetchone()[0] == 1
        joined = raw.execute(
            "SELECT r.receipt_sha256,h.receipt_sha256 "
            "FROM execution_receipts r JOIN terminal_heads h "
            "ON h.receipt_sha256=r.receipt_sha256"
        ).fetchone()
    finally:
        raw.close()
    assert joined == (committed.receipt_sha256, committed.receipt_sha256)
