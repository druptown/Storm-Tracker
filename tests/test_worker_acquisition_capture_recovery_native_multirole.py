"""Parity tests between native provenance writers and recovery bindings."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
from pathlib import Path

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_capture_recovery as recovery
from storm_tracker_worker import dpc_native
from storm_tracker_worker import goes_rrqpe_native
from storm_tracker_worker import hsaf_h40b_native
from storm_tracker_worker import kmi_opendata_raw
from storm_tracker_worker import met_office_native
from storm_tracker_worker import noaa_err_native
from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)


NOW = 1_800_000_000.0
JOB_ID = "acquisition:" + "e" * 64


def _store_and_ledger(tmp_path: Path, binding: recovery.RawRecoveryBinding):
    limits = RawArtifactLimits(
        max_object_bytes=binding.maximum_transport_bytes,
        max_total_bytes=128 * 1024 * 1024,
        max_object_entries=16,
        max_staging_entries=4,
        max_receipt_entries=32,
        max_capture_entries=16,
        max_capture_members=4,
        max_catalog_bytes=8 * 1024 * 1024,
    )
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=limits)
    store.initialize()
    profile = RawCapacityProfile(
        global_max_bytes=128 * 1024 * 1024,
        global_max_entries=16,
        providers=(ProviderCapacityLimit(
            binding.ledger_provider, 128 * 1024 * 1024, 16
        ),),
        max_reservations=16,
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )
    ledger = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=profile,
        clock=lambda: NOW,
    )
    reservation_id = "acqcap:" + hashlib.sha256(JOB_ID.encode()).hexdigest()
    ledger.reserve(
        reservation_id=reservation_id,
        provider=binding.ledger_provider,
        job_identity=JOB_ID,
        requested_bytes=binding.maximum_transport_bytes,
        requested_entries=binding.maximum_artifacts,
    )
    return store, ledger, reservation_id


def _active_contract(workload_id: str):
    blocked = activation.ACTIVATION_RULES[workload_id]
    rule = replace(
        blocked,
        status=activation.ActivationStatus.ACTIVE_SHADOW_RAW,
        reason_code="native_recovery_parity_test",
    )
    binding = recovery.RECOVERY_BINDING_CATALOG[str(rule.handler_id)]
    bindings = {binding.handler_id: binding}
    rules = {rule.workload_id: rule}
    recovery.validate_closed_recovery_bindings(bindings, rules)
    return binding, bindings, rules


def _put(store, binding, role, payload, content_type, upstream_id, provenance):
    member = binding.member(role)
    return store.store(
        provider=binding.raw_provider,
        product=member.raw_product,
        fetch_id=f"{JOB_ID}{member.receipt_suffix}",
        content_type=content_type,
        received_at=NOW,
        upstream_id=upstream_id,
        provenance=provenance,
        payload=payload,
    )


def _kmi_opendata(
    store,
    binding,
    *,
    unexpected_url_field=False,
    credential_transport="none",
):
    descriptor = kmi_opendata_raw.KMI_AWS_10MIN_LATEST
    request = kmi_opendata_raw.canonical_request(descriptor)
    payload = b'{"type":"FeatureCollection","features":[]}'
    response = kmi_opendata_raw.KmiHttpResponse(
        payload=payload,
        status_code=200,
        final_url=request.url,
        content_type="application/json",
        content_type_header="application/json;charset=UTF-8",
        content_encoding="identity",
        declared_content_length=len(payload),
        etag=None,
        last_modified=None,
        received_at=NOW,
    )
    transport = kmi_opendata_raw._transport_document(  # noqa: SLF001
        response,
        request_sha256=request.request_sha256,
        expected_content_types=descriptor.expected_content_types,
    )
    provenance = kmi_opendata_raw._provenance(  # noqa: SLF001
        descriptor, request, transport
    )
    provenance["credential_transport"] = credential_transport
    if unexpected_url_field:
        provenance["unexpected_locator"] = {"url": request.url}
    receipt = _put(
        store,
        binding,
        "geoserver_response",
        payload,
        response.content_type,
        kmi_opendata_raw._upstream_id(  # noqa: SLF001
            request.request_sha256, hashlib.sha256(payload).hexdigest()
        ),
        provenance,
    )
    return store.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{JOB_ID}{binding.capture_suffix}",
        upstream_capture_id=kmi_opendata_raw._upstream_capture_id(  # noqa: SLF001
            descriptor, request, receipt
        ),
        product_timestamp=None,
        members={"geoserver_response": receipt.receipt_id},
    )


def _dpc(store, binding):
    catalog_payload = b'{"exact":"catalog"}'
    catalog_sha = hashlib.sha256(catalog_payload).hexdigest()
    catalog = _put(
        store, binding, "catalog_json", catalog_payload, "application/json",
        "radar-api.protezionecivile.it/findLastProductByType?type=SRI"
        f"|content_sha256={catalog_sha}",
        dpc_native._catalog_provenance(),
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
    geotiff_payload = b"exact-geotiff-container"
    response = dpc_native.DpcHttpResponse(
        geotiff_payload, "image/tiff", NOW, len(geotiff_payload),
        '"23300b18b47a941478a64eed513a3998"', "bounded-modified",
    )
    geotiff_sha = hashlib.sha256(geotiff_payload).hexdigest()
    geotiff = _put(
        store, binding, "geotiff", geotiff_payload, response.content_type,
        f"s3:{identity.bucket}:{identity.key}|content_sha256={geotiff_sha}",
        dpc_native._geotiff_provenance(identity, response),
    )
    return store.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{JOB_ID}{binding.capture_suffix}",
        upstream_capture_id=(
            f"{binding.capture_product}|product_time_ms={product_time_ms}"
            f"|key={key}|catalog={catalog.content_sha256}"
            f"|geotiff={geotiff.content_sha256}"
        ),
        product_timestamp=product_time_ms / 1000.0,
        members={"catalog_json": catalog.receipt_id, "geotiff": geotiff.receipt_id},
    )


def _noaa_err(store, binding):
    prefix = "BLEND/RainRate-Blend-INST/2026/07/18/"
    listing_payload = b"<ListBucketResult/>"
    listing_response = noaa_err_native.S3HttpResponse(
        listing_payload, "application/xml", len(listing_payload), None, NOW,
        request_id="listing-request",
    )
    listing_sha = hashlib.sha256(listing_payload).hexdigest()
    listing = _put(
        store, binding, "listing_xml", listing_payload, "application/xml",
        f"s3-list-v2://{noaa_err_native.NOAA_ERR_BUCKET_NAME}/{prefix}"
        f"|max_keys=1000|content_sha256={listing_sha}",
        noaa_err_native._listing_provenance(prefix, listing_response),
    )
    key = (
        f"{prefix}23/RRQPE-INST-GLB-5_v1r1_blend_"
        "s202607182350000_e202607182359599_c202607190008336.nc"
    )
    etag = '"6ff682687fc8b2e28749a45efab19099"'
    start = datetime(2026, 7, 18, 23, 50, tzinfo=timezone.utc).timestamp()
    end = datetime(2026, 7, 18, 23, 59, tzinfo=timezone.utc).timestamp()
    created = datetime(2026, 7, 19, 0, 8, 33, tzinfo=timezone.utc).timestamp()
    payload = b"exact-netcdf4"
    selected = noaa_err_native.NoaaErrObject(
        key, etag, "2026-07-19T00:09:28.000Z", NOW, len(payload), "STANDARD",
        ("CRC32",), "FULL_OBJECT", start, end, created, "v1r1",
    )
    response = noaa_err_native.S3HttpResponse(
        payload, "application/netcdf", len(payload), len(payload), NOW,
        etag=etag, last_modified="Sun, 19 Jul 2026 00:09:28 GMT",
        last_modified_timestamp=NOW, request_id="object-request",
    )
    netcdf = _put(
        store, binding, "netcdf4", payload, response.content_type,
        f"s3://{noaa_err_native.NOAA_ERR_BUCKET_NAME}/{key}",
        noaa_err_native._object_provenance(selected, response),
    )
    return store.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{JOB_ID}{binding.capture_suffix}",
        upstream_capture_id=(
            f"{binding.capture_product}|key={key}|etag={etag}"
            f"|listing={listing.content_sha256}|netcdf4={netcdf.content_sha256}"
        ),
        product_timestamp=start,
        members={"listing_xml": listing.receipt_id, "netcdf4": netcdf.receipt_id},
    )


def _goes(store, binding, satellite):
    prefixes = (
        "ABI-L2-RRQPEF/2026/214/06/",
        "ABI-L2-RRQPEF/2026/214/07/",
    )
    receipts = {}
    for role, prefix in zip(("listing_previous", "listing_current"), prefixes):
        payload = f"<{role}/>".encode()
        response = goes_rrqpe_native.S3Response(
            payload, "application/xml", NOW, len(payload), None, None,
            None, f"{role}-request", None, None,
        )
        digest = hashlib.sha256(payload).hexdigest()
        receipts[role] = _put(
            store, binding, role, payload, response.content_type,
            f"s3-list-v2:noaa-goes{satellite}:{prefix}:max-1000:sha256:{digest}",
            goes_rrqpe_native._listing_provenance(satellite, role, prefix, response),
        )
    filename = (
        f"OR_ABI-L2-RRQPEF-M6_G{satellite}_"
        "s20262140720226_e20262140729535_c20262140729595.nc"
    )
    key = f"{prefixes[1]}{filename}"
    payload = b"exact-goes-netcdf4"
    etag = '"76d365f56dbb2930d78127f76dc742f3"'
    start = datetime(2026, 8, 2, 7, 20, 22, 600_000, tzinfo=timezone.utc).timestamp()
    selected = goes_rrqpe_native.GoesRrqpeObject(
        satellite, key, filename, 6, start, start + 571, start + 577,
        NOW, etag, len(payload), "STANDARD", (), None,
    )
    response = goes_rrqpe_native.S3Response(
        payload, "application/netcdf", NOW, len(payload), etag, NOW,
        None, "object-request", None, None,
    )
    receipts["netcdf4"] = _put(
        store, binding, "netcdf4", payload, response.content_type,
        f"s3:noaa-goes{satellite}:{key}:etag:{etag}",
        goes_rrqpe_native._object_provenance(selected, response),
    )
    return store.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{JOB_ID}{binding.capture_suffix}",
        upstream_capture_id=(
            f"{binding.capture_product}|G{satellite}|key={key}|etag={etag}"
            f"|previous={receipts['listing_previous'].content_sha256}"
            f"|current={receipts['listing_current'].content_sha256}"
            f"|netcdf4={receipts['netcdf4'].content_sha256}"
        ),
        product_timestamp=start,
        members={role: receipt.receipt_id for role, receipt in receipts.items()},
    )


def _metoffice(store, binding):
    prefixes = ("radar/2026/08/01/", "radar/2026/08/02/")
    receipts = {}
    for role, prefix in zip(("listing_previous", "listing_current"), prefixes):
        payload = f"<{role}/>".encode()
        response = met_office_native.S3Response(
            payload, "application/xml", NOW, None, None, None,
            f"{role}-request", None, None,
        )
        receipts[role] = _put(
            store, binding, role, payload, response.content_type,
            f"s3-list-v2:{met_office_native.BUCKET_NAME}:{prefix}:max-1000",
            met_office_native._listing_provenance(role, prefix, response),
        )
    key = (
        "radar/2026/08/02/"
        "202608020515_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
    )
    payload = b"exact-odim-hdf5"
    etag = '"cab59e871637a772dc15f01f55009924"'
    product_timestamp = datetime(2026, 8, 2, 5, 15, tzinfo=timezone.utc).timestamp()
    selected = met_office_native.MetOfficeObject(
        key, product_timestamp, NOW, etag, len(payload), "STANDARD",
        "SHA256", "FULL_OBJECT",
    )
    response = met_office_native.S3Response(
        payload, "application/x-hdf5", NOW, etag, NOW,
        None, "object-request", None, "AES256",
    )
    receipts["odim_hdf5"] = _put(
        store, binding, "odim_hdf5", payload, response.content_type,
        f"s3:{met_office_native.BUCKET_NAME}:{key}:etag:{etag}",
        met_office_native._object_provenance(selected, response),
    )
    return store.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{JOB_ID}{binding.capture_suffix}",
        upstream_capture_id=(
            f"{binding.capture_product}|key={key}|etag={etag}"
            f"|previous={receipts['listing_previous'].content_sha256}"
            f"|current={receipts['listing_current'].content_sha256}"
            f"|hdf5={receipts['odim_hdf5'].content_sha256}"
        ),
        product_timestamp=product_timestamp,
        members={role: receipt.receipt_id for role, receipt in receipts.items()},
    )


def _hsaf_h40b(store, binding):
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
    listing_payload = (
        f"{hsaf_h40b_native.FTP_DIRECTORY}/{basename}\r\n"
    ).encode("ascii")
    listing_response = hsaf_h40b_native.HsafFtpResponse(
        listing_payload, NOW, "226 Transfer complete"
    )
    listing_sha = hashlib.sha256(listing_payload).hexdigest()
    listing = _put(
        store,
        binding,
        "listing_nlst",
        listing_payload,
        "text/plain",
        (
            f"ftp:{hsaf_h40b_native.FTP_HOST}:{hsaf_h40b_native.FTP_PORT}"
            f"{hsaf_h40b_native.FTP_DIRECTORY}:NLST"
            f"|content_sha256={listing_sha}"
        ),
        hsaf_h40b_native._listing_provenance(  # noqa: SLF001
            listing_response
        ),
    )
    product_payload = b"opaque-gzip-product"
    product_response = hsaf_h40b_native.HsafFtpResponse(
        product_payload,
        NOW,
        "226 Transfer complete",
        len(product_payload),
    )
    product_sha = hashlib.sha256(product_payload).hexdigest()
    product = _put(
        store,
        binding,
        "netcdf_gzip",
        product_payload,
        "application/gzip",
        (
            f"{hsaf_h40b_native.PRODUCT_ID}:{basename}"
            f"|content_sha256={product_sha}"
        ),
        hsaf_h40b_native._product_provenance(  # noqa: SLF001
            product_response, identity
        ),
    )
    receipts = {
        "listing_nlst": listing,
        "netcdf_gzip": product,
    }
    return store.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{JOB_ID}{binding.capture_suffix}",
        upstream_capture_id=hsaf_h40b_native._upstream_capture_id(  # noqa: SLF001
            receipts, identity
        ),
        product_timestamp=product_timestamp,
        members={role: receipt.receipt_id for role, receipt in receipts.items()},
    )


@pytest.mark.parametrize(
    "workload_id,materializer",
    (
        ("ground.kmi.aws_10min", _kmi_opendata),
        ("radar.dpc.sri", _dpc),
        ("satellite.noaa.err", _noaa_err),
        ("satellite.goes18.rrqpe", lambda store, binding: _goes(store, binding, 18)),
        ("satellite.goes19.rrqpe", lambda store, binding: _goes(store, binding, 19)),
        ("radar.metoffice.uk_rate", _metoffice),
        ("satellite.hsaf.h40b", _hsaf_h40b),
    ),
)
def test_native_writer_provenance_and_capture_are_recovery_exact(
    tmp_path, workload_id, materializer
):
    binding, bindings, rules = _active_contract(workload_id)
    store, ledger, reservation_id = _store_and_ledger(tmp_path, binding)
    materializer(store, binding)
    result = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
        recovery_bindings=bindings,
        activation_rules=rules,
    ).audit_existing()
    committed = ledger.load(reservation_id)
    assert result.recovered_commit_count == 1
    assert committed is not None and committed.status == "COMMITTED"
    assert committed.actual_entries == binding.maximum_artifacts


@pytest.mark.parametrize("satellite", (18, 19))
def test_goes_recovery_accepts_only_legacy_generic_capture_suffix(
    tmp_path, satellite
):
    workload_id = f"satellite.goes{satellite}.rrqpe"
    binding, bindings, rules = _active_contract(workload_id)
    store, ledger, reservation_id = _store_and_ledger(tmp_path, binding)
    _goes(store, replace(binding, capture_suffix=":capture"), satellite)

    result = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
        recovery_bindings=bindings,
        activation_rules=rules,
    ).audit_existing()

    committed = ledger.load(reservation_id)
    assert result.recovered_commit_count == 1
    assert committed is not None and committed.status == "COMMITTED"


def test_kmi_recovery_allows_only_the_exact_canonical_request_url_field(
    tmp_path,
):
    binding, bindings, rules = _active_contract("ground.kmi.aws_10min")
    store, ledger, reservation_id = _store_and_ledger(tmp_path, binding)
    _kmi_opendata(store, binding, unexpected_url_field=True)

    with pytest.raises(
        recovery.CaptureRecoveryIntegrityError,
        match="raw provenance contains a forbidden secret field",
    ):
        recovery.CaptureCapacityReconciler(
            raw_store=store,
            capacity_ledger=ledger,
            recovery_bindings=bindings,
            activation_rules=rules,
        ).audit_existing()
    assert ledger.load(reservation_id).status == "RESERVED"


def test_kmi_recovery_rejects_non_none_credential_transport(tmp_path):
    binding, bindings, rules = _active_contract("ground.kmi.aws_10min")
    store, ledger, reservation_id = _store_and_ledger(tmp_path, binding)
    _kmi_opendata(store, binding, credential_transport="api_key")

    with pytest.raises(
        recovery.CaptureRecoveryIntegrityError,
        match="KMI OpenData receipt provenance drifted",
    ):
        recovery.CaptureCapacityReconciler(
            raw_store=store,
            capacity_ledger=ledger,
            recovery_bindings=bindings,
            activation_rules=rules,
        ).audit_existing()
    assert ledger.load(reservation_id).status == "RESERVED"


def test_native_provenance_literal_drift_fails_closed(tmp_path):
    binding, bindings, rules = _active_contract("satellite.noaa.err")
    store, ledger, reservation_id = _store_and_ledger(tmp_path, binding)
    # Exact store receipt, but with a semantically misleading native literal.
    payload = b"<listing/>"
    response = noaa_err_native.S3HttpResponse(
        payload, "application/xml", len(payload), None, NOW,
    )
    provenance = noaa_err_native._listing_provenance(
        "BLEND/RainRate-Blend-INST/2026/07/18/", response
    )
    provenance["structure_inspector_version"] = "drifted-inspector"
    digest = hashlib.sha256(payload).hexdigest()
    _put(
        store, binding, "listing_xml", payload, "application/xml",
        "s3-list-v2://noaa-enterprise-rainrate-pds/"
        "BLEND/RainRate-Blend-INST/2026/07/18/"
        f"|max_keys=1000|content_sha256={digest}",
        provenance,
    )
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        recovery.CaptureCapacityReconciler(
            raw_store=store,
            capacity_ledger=ledger,
            recovery_bindings=bindings,
            activation_rules=rules,
        ).audit_existing()
    assert ledger.load(reservation_id).status == "RESERVED"
