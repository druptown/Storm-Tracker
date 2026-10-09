from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import ast
import gc
import io
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from urllib.parse import quote
from xml.etree import ElementTree as ET

import h5py
import numpy as np
import pytest

from storm_tracker_worker import goes_rrqpe_native as native
from storm_tracker_worker.raw_artifacts import (
    RawArtifactCapacityError,
    RawArtifactLimits,
    RawArtifactStore,
)


START = "20262140720226"
END = "20262140729535"
CREATED = "20262140729595"
START_TIMESTAMP = datetime(2026, 8, 2, 7, 20, 22, 600_000, tzinfo=timezone.utc).timestamp()
END_TIMESTAMP = datetime(2026, 8, 2, 7, 29, 53, 500_000, tzinfo=timezone.utc).timestamp()
CREATED_TIMESTAMP = datetime(2026, 8, 2, 7, 29, 59, 500_000, tzinfo=timezone.utc).timestamp()
LAST_MODIFIED = datetime(2026, 8, 2, 7, 30, 17, tzinfo=timezone.utc).timestamp()
PREFIX_PREVIOUS = "ABI-L2-RRQPEF/2026/214/06/"
PREFIX_CURRENT = "ABI-L2-RRQPEF/2026/214/07/"
ETAG = '"76d365f56dbb2930d78127f76dc742f3"'


def _job(job_id: str = "goes-native-slot-001", payload: dict | None = None):
    return SimpleNamespace(job_id=job_id, payload={} if payload is None else payload)


def _filename(satellite: int = 18, *, mode: int = 6) -> str:
    return (
        f"OR_ABI-L2-RRQPEF-M{mode}_G{satellite}_"
        f"s{START}_e{END}_c{CREATED}.nc"
    )


def _key(satellite: int = 18, *, mode: int = 6) -> str:
    return f"{PREFIX_CURRENT}{_filename(satellite, mode=mode)}"


def _listing(
    satellite: int,
    prefix: str,
    *,
    objects: tuple[dict[str, object], ...] = (),
    truncated: str = "false",
    extra_root: tuple[str, str] | None = None,
) -> bytes:
    ET.register_namespace("", native.S3_NAMESPACE)
    root = ET.Element(f"{{{native.S3_NAMESPACE}}}ListBucketResult")
    for name, value in (
        ("Name", f"noaa-goes{satellite}"),
        ("Prefix", quote(prefix, safe="")),
        ("KeyCount", str(len(objects))),
        ("MaxKeys", str(native.LIST_MAX_KEYS)),
        ("IsTruncated", truncated),
        ("EncodingType", "url"),
    ):
        ET.SubElement(root, f"{{{native.S3_NAMESPACE}}}{name}").text = value
    if extra_root is not None:
        ET.SubElement(root, f"{{{native.S3_NAMESPACE}}}{extra_root[0]}").text = extra_root[1]
    for obj in objects:
        contents = ET.SubElement(root, f"{{{native.S3_NAMESPACE}}}Contents")
        for name in ("Key", "LastModified", "ETag", "Size", "StorageClass"):
            value = str(obj[name])
            if name == "Key":
                value = quote(value, safe="")
            ET.SubElement(contents, f"{{{native.S3_NAMESPACE}}}{name}").text = value
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _object_entry(
    satellite: int = 18,
    *,
    size: int,
    key: str | None = None,
    etag: str = ETAG,
) -> dict[str, object]:
    return {
        "Key": _key(satellite) if key is None else key,
        "LastModified": "2026-08-02T07:30:17.000Z",
        "ETag": etag,
        "Size": size,
        "StorageClass": "STANDARD",
    }


def _netcdf4(
    satellite: int = 18,
    *,
    missing: str | None = None,
    dqf_shape: tuple[int, int] | None = None,
    projection_longitude: float | None = None,
    time_start: str | None = None,
    external_dqf: bool = False,
) -> bytes:
    buffer = io.BytesIO()
    with h5py.File(buffer, "w") as product:
        product.attrs.update({
            "dataset_name": _filename(satellite),
            "platform_ID": f"G{satellite}",
            "scene_id": "Full Disk",
            "timeline_id": "ABI Mode 6",
            "time_coverage_start": (
                "2026-08-02T07:20:22.6Z" if time_start is None else time_start
            ),
            "time_coverage_end": "2026-08-02T07:29:53.5Z",
            "date_created": "2026-08-02T07:29:59.5Z",
            "spatial_resolution": native.FIXED_GRID_RESOLUTION,
            "orbital_slot": native.ORBITAL_SLOT[satellite],
            "production_site": "GCCS",
        })
        if missing != "RRQPE":
            rrqpe = product.create_dataset(
                "RRQPE",
                shape=(native.GRID_ROWS, native.GRID_COLUMNS),
                dtype="u2",
                chunks=(24, native.GRID_COLUMNS),
                compression="gzip",
                compression_opts=1,
                fillvalue=np.uint16(65_535),
            )
            rrqpe.attrs.update({
                "_FillValue": np.array([65_535], dtype="u2"),
                "valid_range": np.array([0, 65_530], dtype="u2"),
                "scale_factor": np.array([0.00152602], dtype="f4"),
                "add_offset": np.array([0.0], dtype="f4"),
                "units": "mm h-1",
                "standard_name": "rainfall_rate",
                "grid_mapping": "goes_imager_projection",
                "ancillary_variables": "DQF",
            })
        if missing != "DQF":
            if external_dqf:
                product["DQF"] = h5py.ExternalLink("elsewhere.h5", "/DQF")
            else:
                dqf = product.create_dataset(
                    "DQF",
                    shape=(native.GRID_ROWS, native.GRID_COLUMNS)
                    if dqf_shape is None
                    else dqf_shape,
                    dtype="u1",
                    chunks=(48, native.GRID_COLUMNS),
                    compression="gzip",
                    compression_opts=1,
                    fillvalue=np.uint8(255),
                )
                dqf.attrs.update({
                    "_FillValue": np.array([255], dtype="u1"),
                    "valid_range": np.array([0, 127], dtype="u1"),
                    "units": "1",
                    "standard_name": "status_flag",
                    "grid_mapping": "goes_imager_projection",
                    "number_of_qf_values": np.array([8], dtype="u1"),
                    "flag_masks": np.array([0, 3, 7, 7, 7, 7, 7, 7], dtype="u1"),
                    "flag_values": np.array([0, 1, 2, 3, 4, 5, 6, 7], dtype="u1"),
                    "flag_meanings": "metadata_only_not_interpreted",
                })
        for name, axis, standard_name, scale, offset in (
            ("x", "X", "projection_x_coordinate", 5.6e-5, -0.151844),
            ("y", "Y", "projection_y_coordinate", -5.6e-5, 0.151844),
        ):
            if missing == name:
                continue
            dataset = product.create_dataset(name, shape=(native.GRID_COLUMNS,), dtype="i2")
            dataset.attrs.update({
                "axis": axis,
                "standard_name": standard_name,
                "units": "rad",
                "scale_factor": np.array([scale], dtype="f4"),
                "add_offset": np.array([offset], dtype="f4"),
            })
        if missing != "goes_imager_projection":
            projection = product.create_dataset("goes_imager_projection", shape=(), dtype="i4")
            projection.attrs.update({
                "grid_mapping_name": "geostationary",
                "perspective_point_height": native.PROJECTION_HEIGHT_METRES,
                "semi_major_axis": native.SEMI_MAJOR_AXIS_METRES,
                "semi_minor_axis": native.SEMI_MINOR_AXIS_METRES,
                "inverse_flattening": native.INVERSE_FLATTENING,
                "latitude_of_projection_origin": 0.0,
                "longitude_of_projection_origin": (
                    native.PROJECTION_LONGITUDE[satellite]
                    if projection_longitude is None
                    else projection_longitude
                ),
                "sweep_angle_axis": "x",
            })
    return buffer.getvalue()


def _response(
    payload: bytes,
    content_type: str,
    received_at: float,
    *,
    etag: str | None = None,
    last_modified: float | None = None,
) -> native.S3Response:
    return native.S3Response(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
        content_length=None if "xml" in content_type else len(payload),
        etag=etag,
        last_modified=last_modified,
        version_id=None,
        request_id="request-id",
        request_id_2="request-id-2",
        server_side_encryption=None,
    )


def _fixture_payloads(satellite: int = 18):
    netcdf = _netcdf4(satellite)
    previous = _listing(satellite, PREFIX_PREVIOUS)
    current = _listing(
        satellite,
        PREFIX_CURRENT,
        objects=(_object_entry(satellite, size=len(netcdf)),),
    )
    return previous, current, netcdf


def _install_responses(monkeypatch, satellite: int = 18, *, netcdf: bytes | None = None):
    previous, current, default_netcdf = _fixture_payloads(satellite)
    object_payload = default_netcdf if netcdf is None else netcdf
    if netcdf is not None:
        current = _listing(
            satellite,
            PREFIX_CURRENT,
            objects=(_object_entry(satellite, size=len(netcdf)),),
        )
    calls = {"listing_previous": 0, "listing_current": 0, "netcdf4": 0}

    def fetch_listing(actual_satellite, prefix, *, deadline):
        assert actual_satellite == satellite
        assert deadline > time.monotonic()
        role = "listing_previous" if prefix == PREFIX_PREVIOUS else "listing_current"
        calls[role] += 1
        return _response(
            previous if role == "listing_previous" else current,
            "application/xml",
            1_785_651_000.0 + calls[role],
        )

    def fetch_object(selected, *, deadline):
        assert selected.satellite == satellite
        assert selected.key == _key(satellite)
        assert deadline > time.monotonic()
        calls["netcdf4"] += 1
        return _response(
            object_payload,
            "binary/octet-stream",
            1_785_651_003.0,
            etag=ETAG,
            last_modified=LAST_MODIFIED,
        )

    monkeypatch.setattr(native, "_fetch_listing", fetch_listing)
    monkeypatch.setattr(native, "_fetch_object", fetch_object)
    return calls, (previous, current, object_payload)


def _capture(monkeypatch, tmp_path, satellite: int = 18):
    store = RawArtifactStore(tmp_path / f"raw-g{satellite}")
    calls, payloads = _install_responses(monkeypatch, satellite)
    result = native.capture_latest_goes_rrqpe(
        _job(),
        satellite=satellite,
        raw_store=store,
        now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
    )
    return store, calls, payloads, result


@pytest.mark.parametrize("satellite", [18, 19])
def test_exact_three_members_are_durable_and_complete(monkeypatch, tmp_path, satellite):
    store, calls, payloads, result = _capture(monkeypatch, tmp_path, satellite)
    provenance = result["raw_provenance"]
    assert calls == {
        "listing_previous": 1,
        "listing_current": 1,
        "netcdf4": 1,
    }
    assert result == {
        **result,
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "decode_status": "not_decoded",
        "pixel_values_read": False,
        "forecast_eligible": False,
        "quantitative_semantics_eligible": False,
        "operational_admission_eligible": False,
    }
    assert provenance["raw_store_schema_version"] == 3
    assert provenance["satellite"] == satellite
    assert provenance["selected_key"] == _key(satellite)
    assert provenance["abi_mode"] == 6
    assert provenance["filename_observation_start"] == START_TIMESTAMP
    assert provenance["filename_observation_end"] == END_TIMESTAMP
    assert provenance["filename_creation_time"] == CREATED_TIMESTAMP
    assert provenance["raw_member_count"] == 3
    assert set(provenance["sources"]) == {
        "listing_previous",
        "listing_current",
        "netcdf4",
    }
    assert store.stats()["receipt_count"] == 3
    assert store.stats()["capture_count"] == 1
    for role, expected in zip(
        ("listing_previous", "listing_current", "netcdf4"), payloads, strict=True
    ):
        assert store.read_bytes(provenance["sources"][role]["receipt_id"]) == expected
    structure = provenance["structure"]
    assert structure["rows"] == 5_424
    assert structure["columns"] == 5_424
    assert structure["dqf_present"] is True
    assert structure["dqf_codes_certified"] is False
    assert structure["pixel_values_read"] is False
    assert structure["longitude_of_projection_origin"] == (
        -137.0 if satellite == 18 else -75.0
    )
    maturity = provenance["official_product_maturity"]
    assert maturity["level"] == ("full" if satellite == 18 else "provisional")
    assert maturity["provenance_only_not_a_quality_score"] is True


def test_originals_are_durable_before_parse_or_hdf5_access(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch)
    real_parse = native._parse_listing
    real_inspect = native._inspect_netcdf4
    events: list[str] = []

    def parse(payload, *, satellite, expected_prefix):
        assert store.stats()["receipt_count"] == 2
        assert store.stats()["capture_count"] == 0
        events.append(f"parse:{expected_prefix}")
        return real_parse(
            payload, satellite=satellite, expected_prefix=expected_prefix
        )

    def inspect(payload, *, selected):
        assert store.stats()["receipt_count"] == 3
        assert store.stats()["capture_count"] == 0
        events.append("inspect:netcdf4")
        return real_inspect(payload, selected=selected)

    monkeypatch.setattr(native, "_parse_listing", parse)
    monkeypatch.setattr(native, "_inspect_netcdf4", inspect)
    native.capture_latest_goes_rrqpe(
        _job(),
        satellite=18,
        raw_store=store,
        now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
    )
    assert events == [
        f"parse:{PREFIX_PREVIOUS}",
        f"parse:{PREFIX_CURRENT}",
        "inspect:netcdf4",
    ]


def test_complete_retry_is_network_free_and_deterministic(monkeypatch, tmp_path):
    store, calls, _payloads, first = _capture(monkeypatch, tmp_path)

    def forbidden(*_args, **_kwargs):
        pytest.fail("complete retry mag geen netwerk gebruiken")

    monkeypatch.setattr(native, "_fetch_listing", forbidden)
    monkeypatch.setattr(native, "_fetch_object", forbidden)
    retry = native.capture_latest_goes_rrqpe(
        _job(),
        satellite=18,
        raw_store=RawArtifactStore(store.root),
        now=datetime(2030, 1, 1, tzinfo=timezone.utc),
    )
    assert retry == first
    assert calls == {
        "listing_previous": 1,
        "listing_current": 1,
        "netcdf4": 1,
    }


class InjectedFailure(RuntimeError):
    pass


@pytest.mark.parametrize(
    ("checkpoint", "receipt_count", "expected_calls"),
    [
        (
            "goes_rrqpe_native.after_listing_previous_store",
            1,
            {"listing_previous": 1, "listing_current": 1, "netcdf4": 1},
        ),
        (
            "goes_rrqpe_native.after_listing_current_store",
            2,
            {"listing_previous": 1, "listing_current": 1, "netcdf4": 1},
        ),
        (
            "goes_rrqpe_native.after_netcdf4_store",
            3,
            {"listing_previous": 1, "listing_current": 1, "netcdf4": 1},
        ),
        (
            "goes_rrqpe_native.before_capture_commit",
            3,
            {"listing_previous": 1, "listing_current": 1, "netcdf4": 1},
        ),
    ],
)
def test_partial_failures_resume_without_refetching_durable_members(
    monkeypatch,
    tmp_path,
    checkpoint,
    receipt_count,
    expected_calls,
):
    store = RawArtifactStore(tmp_path / checkpoint.replace(".", "_"))
    calls, _payloads = _install_responses(monkeypatch)

    def fail(actual):
        if actual == checkpoint:
            raise InjectedFailure(actual)

    with pytest.raises(InjectedFailure):
        native.capture_latest_goes_rrqpe(
            _job(),
            satellite=18,
            raw_store=store,
            now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
            _fault_injector=fail,
        )
    assert store.stats()["receipt_count"] == receipt_count
    assert store.stats()["capture_count"] == 0
    recovered = native.capture_latest_goes_rrqpe(
        _job(),
        satellite=18,
        raw_store=RawArtifactStore(store.root),
        now=datetime(2030, 1, 1, tzinfo=timezone.utc),
    )
    assert recovered["capture_state"] == "captured_not_decoded"
    assert calls == expected_calls


def test_after_commit_failure_leaves_complete_offline_retry(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    calls, _payloads = _install_responses(monkeypatch)

    def fail(checkpoint):
        if checkpoint == "goes_rrqpe_native.after_capture_commit":
            raise InjectedFailure(checkpoint)

    with pytest.raises(InjectedFailure):
        native.capture_latest_goes_rrqpe(
            _job(),
            satellite=18,
            raw_store=store,
            now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
            _fault_injector=fail,
        )
    assert store.stats()["capture_count"] == 1
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda *_args, **_kwargs: pytest.fail("retry gebruikte listingnetwerk"),
    )
    monkeypatch.setattr(
        native,
        "_fetch_object",
        lambda *_args, **_kwargs: pytest.fail("retry gebruikte objectnetwerk"),
    )
    recovered = native.capture_latest_goes_rrqpe(
        _job(), satellite=18, raw_store=RawArtifactStore(store.root)
    )
    assert recovered["capture_state"] == "captured_not_decoded"
    assert calls == {
        "listing_previous": 1,
        "listing_current": 1,
        "netcdf4": 1,
    }


def test_two_concurrent_threads_publish_one_capture(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    calls, _payloads = _install_responses(monkeypatch)

    def run(_index):
        return native.capture_latest_goes_rrqpe(
            _job(),
            satellite=18,
            raw_store=store,
            now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(run, range(2)))
    assert results[0] == results[1]
    assert calls == {
        "listing_previous": 1,
        "listing_current": 1,
        "netcdf4": 1,
    }


def test_same_job_captures_both_satellites_without_identity_collision(
    monkeypatch, tmp_path
):
    fixtures = {satellite: _fixture_payloads(satellite) for satellite in (18, 19)}
    calls = {18: [0, 0, 0], 19: [0, 0, 0]}

    def fetch_listing(satellite, prefix, *, deadline):
        assert deadline > time.monotonic()
        index = 0 if prefix == PREFIX_PREVIOUS else 1
        calls[satellite][index] += 1
        return _response(
            fixtures[satellite][index], "application/xml", 1_785_651_001.0
        )

    def fetch_object(selected, *, deadline):
        assert deadline > time.monotonic()
        calls[selected.satellite][2] += 1
        return _response(
            fixtures[selected.satellite][2],
            "binary/octet-stream",
            1_785_651_003.0,
            etag=ETAG,
            last_modified=LAST_MODIFIED,
        )

    monkeypatch.setattr(native, "_fetch_listing", fetch_listing)
    monkeypatch.setattr(native, "_fetch_object", fetch_object)
    store = RawArtifactStore(tmp_path / "raw")
    results = [
        native.capture_latest_goes_rrqpe(
            _job(),
            satellite=satellite,
            raw_store=store,
            now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
        )
        for satellite in (18, 19)
    ]
    assert [result["raw_provenance"]["satellite"] for result in results] == [18, 19]
    assert results[0]["raw_provenance"]["raw_capture_id"] != (
        results[1]["raw_provenance"]["raw_capture_id"]
    )
    assert calls == {18: [1, 1, 1], 19: [1, 1, 1]}
    assert store.stats()["receipt_count"] == 6
    assert store.stats()["capture_count"] == 2


def _selected(satellite: int, payload: bytes) -> native.GoesRrqpeObject:
    listing = _listing(
        satellite,
        PREFIX_CURRENT,
        objects=(_object_entry(satellite, size=len(payload)),),
    )
    return native._parse_listing(
        listing,
        satellite=satellite,
        expected_prefix=PREFIX_CURRENT,
    )[0]


@pytest.mark.parametrize(
    ("payload_factory", "message"),
    [
        (lambda size: _listing(18, PREFIX_CURRENT, truncated="true"), "truncated"),
        (
            lambda size: _listing(
                18, PREFIX_CURRENT, extra_root=("ContinuationToken", "unexpected")
            ),
            "onbekende velden",
        ),
        (
            lambda size: _listing(
                19,
                PREFIX_CURRENT,
                objects=(_object_entry(18, size=size),),
            ),
            "verkeerde bucket",
        ),
        (
            lambda size: _listing(
                18,
                PREFIX_CURRENT,
                objects=(
                    _object_entry(
                        18,
                        size=size,
                        key=_key(18).replace("-M6_", "-M5_"),
                    ),
                ),
            ),
            "vreemde objectkey",
        ),
    ],
)
def test_listing_parser_rejects_incomplete_or_wrong_authority(payload_factory, message):
    netcdf = _netcdf4()
    with pytest.raises(native.GoesRrqpeNativeContractError, match=message):
        native._parse_listing(
            payload_factory(len(netcdf)),
            satellite=18,
            expected_prefix=PREFIX_CURRENT,
        )


def test_listing_parser_rejects_doctype_bad_percent_duplicate_and_time_path():
    for payload, message in (
        (
            b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "x">]><x>&a;</x>',
            "verboden XML",
        ),
        (
            _listing(18, PREFIX_CURRENT).replace(
                quote(PREFIX_CURRENT, safe="").encode(), b"ABI%ZZ"
            ),
            "URL-tekst",
        ),
    ):
        with pytest.raises(native.GoesRrqpeNativeContractError, match=message):
            native._parse_listing(
                payload,
                satellite=18,
                expected_prefix=PREFIX_CURRENT,
            )

    netcdf = _netcdf4()
    entry = _object_entry(18, size=len(netcdf))
    duplicate = _listing(18, PREFIX_CURRENT, objects=(entry, entry))
    with pytest.raises(native.GoesRrqpeNativeContractError, match="dubbele objectkey"):
        native._parse_listing(
            duplicate, satellite=18, expected_prefix=PREFIX_CURRENT
        )
    wrong_path = _key(18).replace("/214/07/", "/213/07/")
    with pytest.raises(native.GoesRrqpeNativeContractError, match="vreemde objectkey"):
        native._parse_listing(
            _listing(
                18,
                PREFIX_CURRENT,
                objects=(_object_entry(18, size=len(netcdf), key=wrong_path),),
            ),
            satellite=18,
            expected_prefix=PREFIX_CURRENT,
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"missing": "DQF"}, "DQF"),
        ({"dqf_shape": (5_400, 5_424)}, "DQF-structuurprofiel"),
        ({"projection_longitude": -120.0}, "projectie"),
        ({"time_start": "2026-08-02T07:21:22.6Z"}, "tijden verschillen"),
        ({"external_dqf": True}, "externe/symbolische link"),
    ],
)
def test_netcdf_inspector_rejects_missing_or_conflicting_structure(kwargs, message):
    payload = _netcdf4(**kwargs)
    selected = _selected(18, payload)
    with pytest.raises(native.GoesRrqpeNativeContractError, match=message):
        native._inspect_netcdf4(payload, selected=selected)


def test_netcdf_inspector_never_reads_dataset_values(monkeypatch):
    payload = _netcdf4()
    selected = _selected(18, payload)

    def forbidden(*_args, **_kwargs):
        pytest.fail("RRQPE/DQF/assen mogen niet als waarden gelezen worden")

    monkeypatch.setattr(h5py.Dataset, "__getitem__", forbidden)
    identity = native._inspect_netcdf4(payload, selected=selected)
    assert identity.pixel_values_read is False
    assert identity.dqf_present is True


def test_capacity_failure_keeps_partial_receipts_without_capture(monkeypatch, tmp_path):
    netcdf = _netcdf4()
    store = RawArtifactStore(
        tmp_path / "raw",
        limits=RawArtifactLimits(
            max_object_bytes=len(netcdf) - 1,
            max_total_bytes=4 * len(netcdf),
        ),
    )
    _install_responses(monkeypatch, netcdf=netcdf)
    with pytest.raises(RawArtifactCapacityError):
        native.capture_latest_goes_rrqpe(
            _job(),
            satellite=18,
            raw_store=store,
            now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
        )
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 0


def test_job_payload_satellite_time_and_deadline_are_strict(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    for payload in (
        {"url": "https://attacker.invalid"},
        {"token": "secret"},
        {"satellite": 18},
    ):
        with pytest.raises(ValueError, match="exact lege"):
            native.capture_latest_goes_rrqpe(
                _job(payload=payload), satellite=18, raw_store=store
            )
    for satellite in (True, 16, 20, "18"):
        with pytest.raises(ValueError, match="satelliet"):
            native.capture_latest_goes_rrqpe(
                _job(), satellite=satellite, raw_store=store
            )
    with pytest.raises(ValueError, match="tijdzonebewust"):
        native._listing_prefixes(datetime(2026, 8, 2, 7, 40))
    for deadline in (True, float("nan"), "later"):
        with pytest.raises(ValueError, match="deadline_at"):
            native.capture_latest_goes_rrqpe(
                _job(), satellite=18, raw_store=store, deadline_at=deadline
            )


def test_expired_and_thread_lock_deadlines_fail_without_network(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda *_args, **_kwargs: pytest.fail("deadline gebruikte netwerk"),
    )
    with pytest.raises(native.GoesRrqpeNativeDeadlineError):
        native.capture_latest_goes_rrqpe(
            _job(),
            satellite=18,
            raw_store=store,
            deadline_at=time.monotonic() - 1,
        )

    entered = threading.Event()
    release = threading.Event()

    def holder():
        with native._CAPTURE_LOCK:
            entered.set()
            release.wait(2)

    thread = threading.Thread(target=holder)
    thread.start()
    assert entered.wait(1)
    try:
        with pytest.raises(native.GoesRrqpeNativeDeadlineError):
            native.capture_latest_goes_rrqpe(
                _job("goes-lock-timeout"),
                satellite=18,
                raw_store=store,
                deadline_at=time.monotonic() + 0.05,
            )
    finally:
        release.set()
        thread.join(2)


class _FakeResponse(io.BytesIO):
    def __init__(
        self,
        payload: bytes,
        url: str,
        *,
        content_type: str,
        declared: int | None,
        encoding: str | None = None,
        etag: str | None = None,
        modified: str | None = None,
        status: int = 200,
    ):
        super().__init__(payload)
        self.status = status
        self._url = url
        self.headers = {"Content-Type": content_type}
        if declared is not None:
            self.headers["Content-Length"] = str(declared)
        if encoding is not None:
            self.headers["Content-Encoding"] = encoding
        if etag is not None:
            self.headers["ETag"] = etag
        if modified is not None:
            self.headers["Last-Modified"] = modified

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def test_transport_is_anonymous_fixed_host_bounded_and_closes_response():
    payload = b"<listing/>"
    url, path, query = native._listing_request(18, PREFIX_CURRENT)
    captured = {}
    response = _FakeResponse(
        payload,
        url,
        content_type="application/xml",
        declared=None,
    )

    def opener(request, *, timeout):
        captured["headers"] = dict(request.header_items())
        captured["timeout"] = timeout
        return response

    result = native._fetch_s3(
        url,
        satellite=18,
        expected_path=path,
        expected_query=query,
        maximum_bytes=1024,
        allowed_content_types=frozenset({"application/xml"}),
        object_metadata_required=False,
        deadline=time.monotonic() + 2,
        opener=opener,
    )
    assert result.payload == payload
    lowered = {name.lower(): value for name, value in captured["headers"].items()}
    assert lowered["accept-encoding"] == "identity"
    assert "authorization" not in lowered
    assert "cookie" not in lowered
    assert "x-amz-security-token" not in lowered
    assert 0 < captured["timeout"] <= 2
    assert response.closed


@pytest.mark.parametrize(
    ("declared", "encoding", "response_url", "message"),
    [
        (11, None, None, "responslengte"),
        (10, "gzip", None, "transportcompressie"),
        (10, None, "https://attacker.invalid/object", "vaste S3-endpoint"),
    ],
)
def test_transport_rejects_length_encoding_and_endpoint_changes(
    declared, encoding, response_url, message
):
    payload = b"0123456789"
    url, path, query = native._listing_request(18, PREFIX_CURRENT)

    def opener(_request, *, timeout):
        assert timeout > 0
        return _FakeResponse(
            payload,
            url if response_url is None else response_url,
            content_type="application/xml",
            declared=declared,
            encoding=encoding,
        )

    with pytest.raises(native.GoesRrqpeNativeContractError, match=message):
        native._fetch_s3(
            url,
            satellite=18,
            expected_path=path,
            expected_query=query,
            maximum_bytes=1024,
            allowed_content_types=frozenset({"application/xml"}),
            object_metadata_required=False,
            deadline=time.monotonic() + 2,
            opener=opener,
        )


def test_object_transport_requires_listing_matched_metadata():
    payload = _netcdf4()
    selected = _selected(18, payload)
    url, path = native._object_url(18, selected.key)
    captured = {}

    def opener(request, *, timeout):
        captured["headers"] = {
            name.lower(): value for name, value in request.header_items()
        }
        return _FakeResponse(
            payload,
            url,
            content_type="binary/octet-stream",
            declared=len(payload),
            etag=ETAG,
            modified="Sun, 02 Aug 2026 07:30:17 GMT",
        )

    response = native._fetch_s3(
        url,
        satellite=18,
        expected_path=path,
        expected_query="",
        maximum_bytes=native.MAX_NETCDF4_BYTES,
        allowed_content_types=native._ALLOWED_OBJECT_CONTENT_TYPES,
        object_metadata_required=True,
        deadline=time.monotonic() + 2,
        if_match=ETAG,
        opener=opener,
    )
    assert response.etag == ETAG
    assert response.last_modified == LAST_MODIFIED
    assert captured["headers"]["if-match"] == ETAG
    assert "authorization" not in captured["headers"]


def test_process_file_lock_respects_absolute_deadline(tmp_path):
    root = tmp_path / "process-lock"
    helper = Path(__file__).parent / "helpers" / "goes_rrqpe_native_lock_child.py"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(Path(__file__).resolve().parents[1]), environment.get("PYTHONPATH", ""))
    )
    process = subprocess.Popen(
        [sys.executable, str(helper), str(root)],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "locked"
        store = RawArtifactStore(root)
        store.initialize()
        with pytest.raises(native.GoesRrqpeNativeDeadlineError):
            with native._deadline_file_lock(
                store, deadline=time.monotonic() + 0.08
            ):
                pytest.fail("cross-process lock mocht niet verkregen worden")
    finally:
        if process.stdin is not None:
            process.stdin.write("release\n")
            process.stdin.flush()
            process.stdin.close()
        process.wait(timeout=5)
        if process.returncode != 0:
            stderr = "" if process.stderr is None else process.stderr.read()
            pytest.fail(f"lockhelper faalde: {stderr}")
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()


@pytest.mark.parametrize(
    "checkpoint",
    [
        "goes_rrqpe_native.after_listing_previous_store",
        "goes_rrqpe_native.after_listing_current_store",
        "goes_rrqpe_native.after_netcdf4_store",
        "goes_rrqpe_native.before_capture_commit",
        "goes_rrqpe_native.after_capture_commit",
    ],
)
def test_abrupt_process_crash_is_recoverable(monkeypatch, tmp_path, checkpoint):
    root = tmp_path / checkpoint.replace(".", "_")
    helper = Path(__file__).parent / "helpers" / "goes_rrqpe_native_crash_child.py"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(Path(__file__).resolve().parents[1]), environment.get("PYTHONPATH", ""))
    )
    result = subprocess.run(
        [sys.executable, str(helper), str(root), checkpoint],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 73, (result.stdout, result.stderr)

    store = RawArtifactStore(root)
    calls, _payloads = _install_responses(monkeypatch)
    recovered = native.capture_latest_goes_rrqpe(
        _job(),
        satellite=18,
        raw_store=store,
        now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
    )
    assert recovered["capture_state"] == "captured_not_decoded"
    if checkpoint in {
        "goes_rrqpe_native.after_netcdf4_store",
        "goes_rrqpe_native.before_capture_commit",
        "goes_rrqpe_native.after_capture_commit",
    }:
        assert calls == {
            "listing_previous": 0,
            "listing_current": 0,
            "netcdf4": 0,
        }


def test_candidate_is_unwired_and_has_no_decoder_or_admission_path():
    module_path = Path(native.__file__)
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    source = module_path.read_text(encoding="utf-8")
    function_names = {
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    }
    assert not any(name.startswith("decode") for name in function_names)
    assert "observations" not in source
    assert "forecast_output" not in source
    assert "admit(" not in source
    assert "pixel_values_read: bool = False" in source
    repo_root = Path(__file__).resolve().parents[1]
    service_path = repo_root / "storm_tracker_worker" / "service.py"
    assert "goes_rrqpe_native" not in service_path.read_text(encoding="utf-8")
    # The closed migration registry may retain the callable as audit metadata;
    # it is not a production handler and must keep every admission flag false.
    from storm_tracker_worker.provider_capture_registry import capture_policy

    policy = capture_policy("goes_rrqpe_fetch")
    assert policy.candidate_handler == (
        "storm_tracker_worker.goes_rrqpe_native:capture_latest_goes_rrqpe"
    )
    assert policy.candidate_exact_raw_tested is True
    assert policy.exact_native_bytes_complete is False
    assert policy.vnext_operational_admission is False
    assert not any(
        "goes_rrqpe_native" in path.read_text(encoding="utf-8", errors="ignore")
        for path in (repo_root / "custom_components").rglob("*.py")
    )


def test_no_credentials_secrets_or_dqf_semantic_claims_are_persisted(monkeypatch, tmp_path):
    store, _calls, _payloads, result = _capture(monkeypatch, tmp_path)
    serialized = str(result).lower()
    assert "authorization" not in serialized
    assert "secret" not in serialized
    assert "access_key" not in serialized
    assert "credentials_used': true" not in serialized
    assert result["raw_provenance"]["structure"]["dqf_codes_certified"] is False
    for source in result["raw_provenance"]["sources"].values():
        receipt = store.load_receipt(source["receipt_id"])
        assert receipt is not None
        provenance = receipt.provenance
        assert provenance["credentials_used"] is False
        assert provenance["anonymous_public_access"] is True


def test_capture_has_no_resource_warning(monkeypatch, tmp_path):
    import warnings

    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ResourceWarning)
        result = native.capture_latest_goes_rrqpe(
            _job(),
            satellite=18,
            raw_store=store,
            now=datetime(2026, 8, 2, 7, 40, tzinfo=timezone.utc),
        )
        assert result["capture_state"] == "captured_not_decoded"
        gc.collect()


@pytest.mark.skipif(
    os.environ.get("STV3_RUN_GOES_RRQPE_NATIVE_LIVE") != "1",
    reason="set STV3_RUN_GOES_RRQPE_NATIVE_LIVE=1 for official public-S3 golden",
)
@pytest.mark.parametrize("satellite", [18, 19])
def test_optional_live_golden_public_s3(tmp_path, satellite):
    store = RawArtifactStore(tmp_path / f"live-g{satellite}")
    result = native.capture_latest_goes_rrqpe(
        _job(f"goes-live-g{satellite}"),
        satellite=satellite,
        raw_store=store,
    )
    provenance = result["raw_provenance"]
    assert provenance["raw_store_schema_version"] == 3
    assert provenance["satellite"] == satellite
    assert provenance["selected_key"].startswith(
        f"ABI-L2-RRQPEF/"
    )
    assert provenance["raw_member_count"] == 3
    assert provenance["structure"]["rows"] == 5_424
    assert provenance["structure"]["columns"] == 5_424
    assert provenance["structure"]["dqf_present"] is True
    assert provenance["structure"]["pixel_values_read"] is False
