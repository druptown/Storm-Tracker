from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import inspect
import io
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

import h5py
import numpy as np
import pytest

from storm_tracker_worker import met_office_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


PRODUCT_TIME = datetime(2026, 8, 2, 5, 15, tzinfo=timezone.utc).timestamp()
PREVIOUS_PREFIX = "radar/2026/08/01/"
CURRENT_PREFIX = "radar/2026/08/02/"
KEY = (
    "radar/2026/08/02/"
    "202608020515_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
)
ETAG = '"cab59e871637a772dc15f01f55009924"'
LAST_MODIFIED = PRODUCT_TIME + 364


def _job(job_id: str = "met-office:slot:20260802T0515"):
    return SimpleNamespace(job_id=job_id, payload={})


def _response(
    payload: bytes,
    *,
    content_type: str,
    selected: native.MetOfficeObject | None = None,
) -> native.S3Response:
    return native.S3Response(
        payload=payload,
        content_type=content_type,
        received_at=PRODUCT_TIME + 400,
        etag=None if selected is None else selected.etag,
        last_modified=None if selected is None else selected.last_modified,
        version_id=None,
        request_id="request-1",
        request_id_2="request-2",
        server_side_encryption=None if selected is None else "AES256",
    )


def _add(parent, name: str, value: object) -> None:
    child = ET.SubElement(parent, f"{{{native.S3_NAMESPACE}}}{name}")
    child.text = str(value)


def _listing(
    prefix: str,
    objects: tuple[native.MetOfficeObject, ...],
    *,
    truncated: bool = False,
    extra_field: bool = False,
) -> bytes:
    root = ET.Element(f"{{{native.S3_NAMESPACE}}}ListBucketResult")
    _add(root, "Name", native.BUCKET_NAME)
    _add(root, "Prefix", prefix)
    _add(root, "KeyCount", len(objects))
    _add(root, "MaxKeys", native.LIST_MAX_KEYS)
    _add(root, "IsTruncated", str(truncated).lower())
    _add(root, "EncodingType", "url")
    if extra_field:
        _add(root, "Unexpected", "x")
    for item in objects:
        child = ET.SubElement(root, f"{{{native.S3_NAMESPACE}}}Contents")
        _add(child, "Key", item.key)
        _add(
            child,
            "LastModified",
            datetime.fromtimestamp(item.last_modified, timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
        )
        _add(child, "ETag", item.etag)
        if item.checksum_algorithm is not None:
            _add(child, "ChecksumAlgorithm", item.checksum_algorithm)
            _add(child, "ChecksumType", item.checksum_type)
        _add(child, "Size", item.size_bytes)
        _add(child, "StorageClass", item.storage_class)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _object(
    *,
    key: str = KEY,
    timestamp: float = PRODUCT_TIME,
    size_bytes: int = 1,
    etag: str = ETAG,
    last_modified: float = LAST_MODIFIED,
) -> native.MetOfficeObject:
    return native.MetOfficeObject(
        key=key,
        product_timestamp=timestamp,
        last_modified=last_modified,
        etag=etag,
        size_bytes=size_bytes,
        storage_class="STANDARD",
        checksum_algorithm="SHA256",
        checksum_type="FULL_OBJECT",
    )


@lru_cache(maxsize=32)
def _odim(
    *,
    nominal_offset: int = 0,
    quantity: str = native.ODIM_QUANTITY,
    columns: int = native.GRID_COLUMNS,
    external_data_link: bool = False,
    allocate_all_chunks: bool = True,
    extra_root_attribute: bool = False,
) -> bytes:
    nominal = PRODUCT_TIME + nominal_offset
    start = nominal - 52
    end = nominal + 9
    moment = datetime.fromtimestamp(nominal, timezone.utc)
    start_moment = datetime.fromtimestamp(start, timezone.utc)
    end_moment = datetime.fromtimestamp(end, timezone.utc)
    target = io.BytesIO()
    with h5py.File(target, "w") as root:
        root.attrs["Conventions"] = np.bytes_(native.ODIM_CONVENTIONS)
        if extra_root_attribute:
            root.attrs["unexpected"] = np.bytes_(b"x")
        what = root.create_group("what")
        what.attrs.update({
            "date": np.bytes_(moment.strftime("%Y%m%d")),
            "object": np.bytes_(native.ODIM_OBJECT),
            "source": np.bytes_("WMO:03523,WMO:03675"),
            "time": np.bytes_(moment.strftime("%H%M%S")),
            "version": np.bytes_(native.ODIM_VERSION),
        })
        where = root.create_group("where")
        where.attrs.update({
            **native.GRID_CORNERS,
            "projdef": np.bytes_(native.GRID_PROJECTION),
            "xscale": native.GRID_SCALE_METRES,
            "xsize": columns,
            "yscale": native.GRID_SCALE_METRES,
            "ysize": native.GRID_ROWS,
        })
        how = root.create_group("how")
        how.attrs.update({
            "VPRCorr": np.bytes_(b"True"),
            "camethod": np.bytes_(b"DOMAIN"),
            "endepochs": int(end),
            "nodes": np.bytes_(b"ukcle,ukche"),
            "simulated": np.bytes_(b"False"),
            "software": np.bytes_(b"RADARNET"),
            "startepochs": int(start),
            "sw_version": np.bytes_(b"2021.12.1"),
            "task": np.bytes_(b"MO.UK.RADARNET.PCompMulti"),
            "zr_a": 200.0,
            "zr_b": 1.6,
        })
        dataset = root.create_group("dataset1")
        dataset_how = dataset.create_group("how")
        dataset_how.attrs["origin"] = np.bytes_(b"UPPER LEFT")
        dataset_what = dataset.create_group("what")
        dataset_what.attrs.update({
            "enddate": np.bytes_(end_moment.strftime("%Y%m%d")),
            "endtime": np.bytes_(end_moment.strftime("%H%M%S")),
            "product": np.bytes_(native.ODIM_PRODUCT),
            "startdate": np.bytes_(start_moment.strftime("%Y%m%d")),
            "starttime": np.bytes_(start_moment.strftime("%H%M%S")),
        })
        data_group = dataset.create_group("data1")
        data_what = data_group.create_group("what")
        data_what.attrs.update({
            "gain": 1.0,
            "nodata": -1.0,
            "offset": 0.0,
            "quantity": np.bytes_(quantity),
            "undetect": 0.0,
        })
        if external_data_link:
            data_group["data"] = h5py.ExternalLink("outside.h5", "/data")
        else:
            raster = data_group.create_dataset(
                "data",
                shape=(native.GRID_ROWS, columns),
                dtype="float32",
                chunks=(75, columns),
                compression="gzip",
                compression_opts=6,
            )
            chunk = np.zeros((75, columns), dtype="float32")
            stop = native.GRID_ROWS if allocate_all_chunks else 75
            for row in range(0, stop, 75):
                raster[row : min(row + 75, native.GRID_ROWS), :] = chunk[
                    : min(75, native.GRID_ROWS - row), :
                ]
    return target.getvalue()


def _selected_for(payload: bytes, **changes) -> native.MetOfficeObject:
    values = {
        "key": KEY,
        "timestamp": PRODUCT_TIME,
        "size_bytes": len(payload),
        "etag": ETAG,
        "last_modified": LAST_MODIFIED,
    }
    values.update(changes)
    return _object(**values)


def _transport(monkeypatch, payload: bytes):
    selected = _selected_for(payload)
    previous_object = _object(
        key=(
            "radar/2026/08/01/"
            "202608012345_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
        ),
        timestamp=PRODUCT_TIME - 5.5 * 60 * 60,
        size_bytes=123,
        etag='"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"',
        last_modified=PRODUCT_TIME - 5.5 * 60 * 60 + 300,
    )
    listings = {
        PREVIOUS_PREFIX: _listing(PREVIOUS_PREFIX, (previous_object,)),
        CURRENT_PREFIX: _listing(CURRENT_PREFIX, (selected,)),
    }
    calls = {"list": [], "object": []}

    def fetch_listing(prefix):
        calls["list"].append(prefix)
        return _response(listings[prefix], content_type="application/xml")

    def fetch_object(value):
        calls["object"].append(value)
        assert value == selected
        return _response(payload, content_type="binary/octet-stream", selected=value)

    monkeypatch.setattr(native, "_fetch_listing", fetch_listing)
    monkeypatch.setattr(native, "_fetch_object", fetch_object)
    return selected, listings, calls


def test_prefix_pair_is_utc_day_boundary_and_adjacent():
    aware = datetime(2026, 8, 2, 0, 1, tzinfo=timezone(timedelta(hours=9)))
    assert native._listing_prefixes(aware) == (
        "radar/2026/07/31/",
        "radar/2026/08/01/",
    )
    assert native._adjacent_prefix(PREVIOUS_PREFIX) == CURRENT_PREFIX


def test_listing_parser_preserves_revision_checksum_and_orders_slots():
    later = _object(size_bytes=22)
    earlier = _object(
        key=(
            "radar/2026/08/02/"
            "202608020500_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
        ),
        timestamp=PRODUCT_TIME - 900,
        size_bytes=11,
        etag='"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"',
    )
    parsed = native._parse_listing(
        _listing(CURRENT_PREFIX, (later, earlier)),
        expected_prefix=CURRENT_PREFIX,
    )
    assert parsed == (earlier, later)
    assert parsed[-1].checksum_algorithm == "SHA256"
    assert parsed[-1].checksum_type == "FULL_OBJECT"


@pytest.mark.parametrize(
    "payload,match",
    (
        (_listing(CURRENT_PREFIX, (), truncated=True), "onvolledig"),
        (_listing(CURRENT_PREFIX, (), extra_field=True), "onbekende"),
        (b"<not-xml", "ongeldig"),
    ),
)
def test_listing_rejects_truncation_unknown_schema_and_malformed_xml(payload, match):
    with pytest.raises(native.MetOfficeNativeError, match=match):
        native._parse_listing(payload, expected_prefix=CURRENT_PREFIX)


def test_key_profile_rejects_wrong_day_or_non_quarter_hour():
    for key in (
        "radar/2026/08/01/202608020515_ODIM_ng_radar_rainrate_composite_1km_UK.h5",
        "radar/2026/08/02/202608020517_ODIM_ng_radar_rainrate_composite_1km_UK.h5",
        "radar/2026/08/02/202608020515_other.h5",
    ):
        with pytest.raises(native.MetOfficeNativeError):
            native._parse_product_key(key, expected_prefix=CURRENT_PREFIX)


def test_fixed_endpoint_rejects_ssrf_redirect_query_and_credentials():
    for url in (
        "http://" + native.S3_HOST + "/",
        "https://evil.invalid/",
        "https://user:pass@" + native.S3_HOST + "/",
        "https://" + native.S3_HOST + ":444/",
        "https://" + native.S3_HOST + "/?unexpected=1",
        "https://127.0.0.1/",
    ):
        with pytest.raises(native.MetOfficeNativeError):
            native._fixed_url(url, expected_path="/", expected_query="")


def test_odim_inspector_binds_grid_window_rate_and_processing_without_pixels():
    payload = _odim()
    identity = native._inspect_odim(payload, selected=_selected_for(payload))
    assert identity.product_timestamp == PRODUCT_TIME
    assert identity.input_window_start == PRODUCT_TIME - 52
    assert identity.input_window_end == PRODUCT_TIME + 9
    assert (identity.rows, identity.columns) == (
        native.GRID_ROWS,
        native.GRID_COLUMNS,
    )
    assert identity.quantity == "RATE"
    assert identity.dtype == "float32"
    assert identity.chunk_count == 29
    assert identity.source_wmo_ids == ("WMO:03523", "WMO:03675")
    assert identity.source_nodes == ("ukcle", "ukche")
    assert identity.zr_a == 200.0
    assert identity.zr_b == 1.6
    source = inspect.getsource(native._inspect_odim)
    assert "raster[" not in source
    assert ".read_direct(" not in source


@pytest.mark.parametrize(
    "payload_factory,match",
    (
        (lambda: _odim(quantity="DBZH"), "productsemantiek"),
        (lambda: _odim(columns=native.GRID_COLUMNS - 1), "gridprofiel"),
        (lambda: _odim(nominal_offset=900), "tijden verschillen"),
        (lambda: _odim(external_data_link=True), "link"),
        (lambda: _odim(allocate_all_chunks=False), "rasteropslagprofiel"),
        (lambda: _odim(extra_root_attribute=True), "root-attributen"),
    ),
)
def test_odim_inspector_fails_closed_on_semantic_or_storage_drift(
    payload_factory, match
):
    payload = payload_factory()
    with pytest.raises(native.MetOfficeNativeError, match=match):
        native._inspect_odim(payload, selected=_selected_for(payload))


def test_complete_capture_is_three_exact_members_and_no_weather_claim(monkeypatch, tmp_path):
    payload = _odim()
    selected, _listings, calls = _transport(monkeypatch, payload)
    store = RawArtifactStore(tmp_path / "raw")
    result = native.capture_latest_met_office_odim(
        _job(),
        raw_store=store,
        now=datetime(2026, 8, 2, 5, 20, tzinfo=timezone.utc),
    )
    assert calls == {
        "list": [PREVIOUS_PREFIX, CURRENT_PREFIX],
        "object": [selected],
    }
    assert result["capture_state"] == "captured_not_decoded"
    assert result["weather_state"] == "unknown"
    assert result["quantitative_semantics_eligible"] is False
    assert result["operational_admission_eligible"] is False
    assert result["raw_provenance"]["structure"]["pixel_values_read"] is False
    assert set(result["raw_provenance"]["sources"]) == {
        "listing_previous", "listing_current", "odim_hdf5"
    }
    stats = store.stats()
    assert stats["receipt_count"] == 3
    assert stats["capture_count"] == 1


def test_complete_retry_is_networkless_and_byte_identical(monkeypatch, tmp_path):
    payload = _odim()
    _transport(monkeypatch, payload)
    store = RawArtifactStore(tmp_path / "raw")
    first = native.capture_latest_met_office_odim(
        _job(), raw_store=store, now=datetime(2026, 8, 2, tzinfo=timezone.utc)
    )
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda _prefix: pytest.fail("complete retry deed listingnetwerk"),
    )
    monkeypatch.setattr(
        native,
        "_fetch_object",
        lambda _selected: pytest.fail("complete retry deed objectnetwerk"),
    )
    retry = native.capture_latest_met_office_odim(
        _job(), raw_store=store, now=datetime(2030, 1, 1, tzinfo=timezone.utc)
    )
    assert retry == first
    assert store.stats()["receipt_count"] == 3
    assert store.stats()["capture_count"] == 1


def test_bounded_reader_rejects_stream_overflow_and_declared_length_drift():
    with pytest.raises(native.MetOfficeNativeError, match="bytegrens"):
        native._read_bounded(io.BytesIO(b"1234"), maximum_bytes=3, declared=None)
    with pytest.raises(native.MetOfficeNativeError, match="payloadlengte"):
        native._read_bounded(io.BytesIO(b"123"), maximum_bytes=4, declared=2)


@pytest.mark.parametrize(
    ("remaining", "expected_timeout"),
    ((75.0, 5.0), (2.0, 2.0)),
)
def test_http_socket_operation_is_capped_inside_the_shared_deadline(
    monkeypatch,
    remaining,
    expected_timeout,
):
    clock = 1_000.0
    monkeypatch.setattr(native.time, "monotonic", lambda: clock)
    prefix = PREVIOUS_PREFIX
    url, path, query = native._listing_request(prefix)
    observed = []

    class Response:
        status = 200
        headers = {"Content-Type": "application/xml"}

        def __init__(self):
            self._blocks = [b"<bounded/>", b""]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return url

        def read(self, _maximum):
            return self._blocks.pop(0)

    def open_response(_request, *, timeout):
        observed.append(timeout)
        return Response()

    monkeypatch.setattr(native, "_open_without_redirects", open_response)
    with native._capture_deadline(clock + remaining):
        response = native._fetch_s3(
            url,
            expected_path=path,
            expected_query=query,
            maximum_bytes=native.MAX_LISTING_BYTES,
            allowed_content_types=frozenset({"application/xml"}),
            object_metadata_required=False,
        )
    assert response.payload == b"<bounded/>"
    assert observed == [expected_timeout]


def test_one_cooperative_deadline_covers_both_listings_and_object(
    monkeypatch, tmp_path
):
    payload = _odim()
    selected, _listings, calls = _transport(monkeypatch, payload)
    original_listing = native._fetch_listing
    original_object = native._fetch_object
    clock = {"value": 1_000.0}
    monkeypatch.setattr(native.time, "monotonic", lambda: clock["value"])

    def slow_listing(prefix):
        clock["value"] += 31.0
        return original_listing(prefix)

    def slow_object(value):
        clock["value"] += 20.0
        return original_object(value)

    monkeypatch.setattr(native, "_fetch_listing", slow_listing)
    monkeypatch.setattr(native, "_fetch_object", slow_object)
    store = RawArtifactStore(tmp_path / "raw-deadline")
    with pytest.raises(native.MetOfficeNativeDeadlineError, match="deadline"):
        native.capture_latest_met_office_odim(
            _job("met-office:deadline:20260802T0515"),
            raw_store=store,
            now=datetime(2026, 8, 2, 5, 20, tzinfo=timezone.utc),
        )
    assert calls == {
        "list": [PREVIOUS_PREFIX, CURRENT_PREFIX],
        "object": [selected],
    }
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 0

    # The next attempt owns a fresh bounded deadline and reuses both durable
    # listing authorities instead of repeating their network calls.
    retry = native.capture_latest_met_office_odim(
        _job("met-office:deadline:20260802T0515"),
        raw_store=store,
        now=datetime(2030, 1, 1, tzinfo=timezone.utc),
    )
    assert retry["capture_state"] == "captured_not_decoded"
    assert calls["list"] == [PREVIOUS_PREFIX, CURRENT_PREFIX]
    assert calls["object"] == [selected, selected]
    assert store.stats()["receipt_count"] == 3
    assert store.stats()["capture_count"] == 1


@pytest.mark.parametrize("deadline", (True, float("nan"), float("inf"), "soon"))
def test_explicit_deadline_is_finite_numeric(monkeypatch, tmp_path, deadline):
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda _prefix: pytest.fail("invalid deadline reached the network"),
    )
    with pytest.raises(ValueError, match="deadline_at"):
        native.capture_latest_met_office_odim(
            _job("met-office:invalid-deadline"),
            raw_store=RawArtifactStore(tmp_path / "raw-invalid-deadline"),
            deadline_at=deadline,
        )


def test_expired_explicit_deadline_fails_before_network(monkeypatch, tmp_path):
    monkeypatch.setattr(native.time, "monotonic", lambda: 500.0)
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda _prefix: pytest.fail("expired deadline reached the network"),
    )
    with pytest.raises(native.MetOfficeNativeDeadlineError, match="deadline"):
        native.capture_latest_met_office_odim(
            _job("met-office:expired-deadline"),
            raw_store=RawArtifactStore(tmp_path / "raw-expired-deadline"),
            deadline_at=499.0,
        )


def test_native_total_deadline_leaves_parent_wall_margin():
    assert native.CAPTURE_TOTAL_DEADLINE_SECONDS == 75.0
    assert native.SOCKET_OPERATION_TIMEOUT_SECONDS == 5.0
    assert (
        native.CAPTURE_TOTAL_DEADLINE_SECONDS
        + native.SOCKET_OPERATION_TIMEOUT_SECONDS
        < 90.0
    )


def test_crash_after_hdf5_receipt_retries_without_network(monkeypatch, tmp_path):
    payload = _odim()
    _transport(monkeypatch, payload)
    store = RawArtifactStore(tmp_path / "raw")
    original = native._inspect_odim
    monkeypatch.setattr(
        native,
        "_inspect_odim",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("simulated post-raw crash")
        ),
    )
    with pytest.raises(RuntimeError, match="post-raw crash"):
        native.capture_latest_met_office_odim(
            _job(), raw_store=store, now=datetime(2026, 8, 2, tzinfo=timezone.utc)
        )
    assert store.stats()["receipt_count"] == 3
    assert store.stats()["capture_count"] == 0
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda _prefix: pytest.fail("crashretry deed listingnetwerk"),
    )
    monkeypatch.setattr(
        native,
        "_fetch_object",
        lambda _selected: pytest.fail("crashretry deed objectnetwerk"),
    )
    monkeypatch.setattr(native, "_inspect_odim", original)
    result = native.capture_latest_met_office_odim(
        _job(), raw_store=store, now=datetime(2030, 1, 1, tzinfo=timezone.utc)
    )
    assert result["capture_state"] == "captured_not_decoded"
    assert store.stats()["capture_count"] == 1


def test_concurrent_same_job_fetches_one_exact_member_set(monkeypatch, tmp_path):
    payload = _odim()
    selected, _listings, calls = _transport(monkeypatch, payload)
    store = RawArtifactStore(tmp_path / "raw")
    job = _job("met-office:concurrent:20260802T0515")

    def run_capture():
        return native.capture_latest_met_office_odim(
            job,
            raw_store=store,
            now=datetime(2026, 8, 2, 5, 20, tzinfo=timezone.utc),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(lambda _index: run_capture(), range(2)))
    assert results[0] == results[1]
    assert calls == {
        "list": [PREVIOUS_PREFIX, CURRENT_PREFIX],
        "object": [selected],
    }
    assert store.stats()["receipt_count"] == 3
    assert store.stats()["capture_count"] == 1


def test_both_listing_documents_are_durable_before_xml_parse(monkeypatch, tmp_path):
    payload = _odim()
    _selected, listings, _calls = _transport(monkeypatch, payload)
    listings[CURRENT_PREFIX] = b"<invalid"
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda prefix: _response(listings[prefix], content_type="application/xml"),
    )
    store = RawArtifactStore(tmp_path / "raw")
    with pytest.raises(native.MetOfficeNativeError, match="XML"):
        native.capture_latest_met_office_odim(
            _job(), raw_store=store, now=datetime(2026, 8, 2, tzinfo=timezone.utc)
        )
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 0


def test_invalid_hdf5_remains_raw_but_never_becomes_complete_capture(
    monkeypatch, tmp_path
):
    payload = b"not-hdf5"
    _transport(monkeypatch, payload)
    store = RawArtifactStore(tmp_path / "raw")
    with pytest.raises(native.MetOfficeNativeError, match="signature"):
        native.capture_latest_met_office_odim(
            _job(), raw_store=store, now=datetime(2026, 8, 2, tzinfo=timezone.utc)
        )
    assert store.stats()["receipt_count"] == 3
    assert store.stats()["capture_count"] == 0


def test_partial_retry_uses_durable_day_pair_after_clock_crosses_midnight(
    monkeypatch, tmp_path
):
    payload = _odim()
    selected, _listings, calls = _transport(monkeypatch, payload)
    store = RawArtifactStore(tmp_path / "raw")
    monkeypatch.setattr(
        native,
        "_fetch_object",
        lambda _selected: (_ for _ in ()).throw(TimeoutError("object timeout")),
    )
    with pytest.raises(TimeoutError):
        native.capture_latest_met_office_odim(
            _job(), raw_store=store, now=datetime(2026, 8, 2, tzinfo=timezone.utc)
        )
    assert store.stats()["receipt_count"] == 2
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda _prefix: pytest.fail("partial retry deed listingnetwerk"),
    )
    monkeypatch.setattr(
        native,
        "_fetch_object",
        lambda value: _response(payload, content_type="binary/octet-stream", selected=value),
    )
    result = native.capture_latest_met_office_odim(
        _job(), raw_store=store, now=datetime(2030, 1, 1, tzinfo=timezone.utc)
    )
    assert result["raw_provenance"]["selected_key"] == selected.key
    assert calls["list"] == [PREVIOUS_PREFIX, CURRENT_PREFIX]


def test_empty_complete_listings_are_preserved_but_not_interpreted_as_dry(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda prefix: _response(_listing(prefix, ()), content_type="application/xml"),
    )
    monkeypatch.setattr(
        native,
        "_fetch_object",
        lambda _selected: pytest.fail("lege listing deed objectfetch"),
    )
    store = RawArtifactStore(tmp_path / "raw")
    with pytest.raises(native.MetOfficeNativeError, match="geen HDF5"):
        native.capture_latest_met_office_odim(
            _job(), raw_store=store, now=datetime(2026, 8, 2, tzinfo=timezone.utc)
        )
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 0


def test_conflicting_duplicate_object_revision_is_rejected():
    first = _object(size_bytes=1)
    second = replace(first, size_bytes=2)
    with pytest.raises(native.MetOfficeNativeError, match="conflicterende"):
        native._select_latest(((first,), (second,)))


def test_result_is_payload_path_url_and_secret_free(monkeypatch, tmp_path):
    payload = _odim()
    _transport(monkeypatch, payload)
    result = native.capture_latest_met_office_odim(
        _job(),
        raw_store=RawArtifactStore(tmp_path / "raw"),
        now=datetime(2026, 8, 2, tzinfo=timezone.utc),
    )
    encoded = json.dumps(result, sort_keys=True).lower()
    for forbidden in (
        "artifact_path", "https://", "authorization", "password", "token=",
        '"raining"', '"dry"', '"cells"', '"observations"',
    ):
        assert forbidden not in encoded


@pytest.mark.parametrize(
    "job",
    (
        SimpleNamespace(job_id="bad id", payload={}),
        SimpleNamespace(job_id="valid", payload={"url": "https://evil.invalid"}),
        SimpleNamespace(payload={}),
    ),
)
def test_job_contract_is_exact_and_credential_free(job, tmp_path):
    with pytest.raises(native.MetOfficeNativeError):
        native.capture_latest_met_office_odim(
            job, raw_store=RawArtifactStore(tmp_path / "raw")
        )


def test_fetch_object_requires_listing_revision_match(monkeypatch):
    selected = _object(size_bytes=10)
    mismatched = _response(
        b"x" * 10,
        content_type="binary/octet-stream",
        selected=native.MetOfficeObject(
            key=selected.key,
            product_timestamp=selected.product_timestamp,
            last_modified=selected.last_modified,
            etag='"dddddddddddddddddddddddddddddddd"',
            size_bytes=selected.size_bytes,
            storage_class=selected.storage_class,
            checksum_algorithm=selected.checksum_algorithm,
            checksum_type=selected.checksum_type,
        ),
    )
    monkeypatch.setattr(native, "_fetch_s3", lambda *_args, **_kwargs: mismatched)
    with pytest.raises(native.MetOfficeNativeError, match="revision"):
        native._fetch_object(selected)


def test_real_golden_metadata_is_native_and_not_a_synthetic_decoder_claim():
    path = Path(__file__).with_name("fixtures") / "met_office_uk_odim_golden.json"
    golden = json.loads(path.read_text(encoding="utf-8"))
    assert golden["key"] == KEY
    assert golden["sha256"] == (
        "854a29f7b3dd340554edbfc81e89cecb754db9469aefb437785210acee8141d2"
    )
    assert golden["quantity"] == "RATE"
    assert golden["pixel_values_read"] is False
    assert golden["operational_admission_eligible"] is False
    assert golden["input_window_end"] > golden["product_timestamp"]


def test_candidate_is_not_wired_into_worker_or_ha_runtime():
    service = Path(native.__file__).with_name("service.py").read_text(encoding="utf-8")
    integration = (
        Path(native.__file__).parents[1]
        / "custom_components"
        / "storm_tracker_v3"
        / "__init__.py"
    ).read_text(encoding="utf-8")
    assert "storm_tracker_worker.met_office_native" not in service
    assert "capture_latest_met_office_odim" not in service
    assert "met_office_native" not in integration
