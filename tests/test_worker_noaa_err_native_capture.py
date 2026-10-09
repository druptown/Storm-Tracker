from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import json
from types import SimpleNamespace

import h5py
import numpy as np
import pytest

from storm_tracker_worker import noaa_err_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


PREFIX = "BLEND/RainRate-Blend-INST/2026/07/18/"
KEY = (
    PREFIX
    + "23/RRQPE-INST-GLB-5_v1r1_blend_"
    "s202607182350000_e202607182359599_c202607190008336.nc"
)
ETAG = '"6ff682687fc8b2e28749a45efab19099"'
LAST_MODIFIED = "2026-07-19T00:09:28.000Z"
HTTP_LAST_MODIFIED = "Sun, 19 Jul 2026 00:09:28 GMT"
PRODUCT_START = datetime(2026, 7, 18, 23, 50, tzinfo=timezone.utc).timestamp()
PRODUCT_END = datetime(2026, 7, 18, 23, 59, tzinfo=timezone.utc).timestamp()
CREATED = datetime(2026, 7, 19, 0, 8, 33, tzinfo=timezone.utc).timestamp() + 0.6
INPUT_FILES = (
    "RRQPE-ABI-INST_v1r1_g19_s202607182350190_e202607182359498_c202607190000118.nc "
    "RRQPE-ABI-INST_v1r1_g18_s202607182350232_e202607182359541_c202607190000216.nc "
    "RRQPE-AHI-INST_v1r1_h09_s202607182350206_e202607182359400_c202607190004343.nc "
    "RRQPE-SEVIRI-INST_v1r1_met9_s202607182345100_e202607182357398_c202607190006155.nc "
    "RRQPE-SEVIRI-INST_v1r1_met10_s202607182345108_e202607182357434_c202607190007369.nc"
)


def _job(job_id: str = "noaa-err-slot-001", payload: dict | None = None):
    return SimpleNamespace(job_id=job_id, payload={} if payload is None else payload)


def _hdf_bytes(
    *,
    marker: str = "a",
    rows: int = native.NOAA_ERR_GRID_ROWS,
    columns: int = native.NOAA_ERR_GRID_COLUMNS,
    rrqpe_dtype: str = "i2",
    dqf_dtype: str = "i1",
    platforms: str = "GOES-19, GOES-18, Himawari-9, MSG2, MSG3",
    title: str = "RRQPE_INST_GLB",
    start: str = "2026-07-18T23:50:00Z",
    end: str = "2026-07-18T23:59:00Z",
    created: str = "2026-07-19T00:08:33Z",
    inputs: int = 5,
    missing_inputs: int = 0,
    input_files: str = INPUT_FILES,
    latitude_resolution: float = 0.02,
    rrqpe_units: str = "mm/h",
) -> bytes:
    stream = io.BytesIO()
    with h5py.File(stream, "w") as root:
        root.attrs["_NCProperties"] = np.bytes_(b"version=2,netcdf=4.7.0,hdf5=1.8.22,")
        root.attrs["Conventions"] = np.bytes_(b"CF-1.6")
        root.attrs["title"] = np.bytes_(title.encode())
        root.attrs["production_environment"] = np.bytes_(b"prod")
        root.attrs["processing_level"] = np.bytes_(b"NOAA Level 3")
        root.attrs["platform"] = np.bytes_(platforms.encode())
        root.attrs["time_coverage_start"] = np.bytes_(start.encode())
        root.attrs["time_coverage_end"] = np.bytes_(end.encode())
        root.attrs["date_created"] = np.bytes_(created.encode())
        root.attrs["geospatial_lat_min"] = np.asarray([-60.0], dtype="f4")
        root.attrs["geospatial_lat_max"] = np.asarray([70.0], dtype="f4")
        root.attrs["geospatial_lon_min"] = np.asarray([-180.0], dtype="f4")
        root.attrs["geospatial_lon_max"] = np.asarray([179.98], dtype="f4")
        root.attrs["geospatial_lat_resolution"] = np.asarray(
            [latitude_resolution], dtype="f4"
        )
        root.attrs["geospatial_lon_resolution"] = np.asarray([0.02], dtype="f4")
        root.attrs["test_marker"] = np.bytes_(marker.encode())
        root.create_dataset("Rows", shape=(rows,), dtype="f4")
        root.create_dataset("Columns", shape=(columns,), dtype="f4")
        rrqpe = root.create_dataset(
            "RRQPE",
            shape=(rows, columns),
            dtype=rrqpe_dtype,
            chunks=(min(813, rows), min(2250, columns)),
            compression="gzip",
        )
        rr_fill = -9990 if np.dtype(rrqpe_dtype).kind == "i" else 0
        rrqpe.attrs["_FillValue"] = np.asarray([rr_fill], dtype=rrqpe_dtype)
        rrqpe.attrs["scale_factor"] = np.asarray([0.1], dtype="f4")
        rrqpe.attrs["add_offset"] = np.asarray([0.0], dtype="f4")
        rrqpe.attrs["units"] = np.bytes_(rrqpe_units.encode())
        dqf = root.create_dataset(
            "DQF",
            shape=(rows, columns),
            dtype=dqf_dtype,
            chunks=(min(1084, rows), min(3000, columns)),
            compression="gzip",
        )
        dqf_fill = -1 if np.dtype(dqf_dtype).kind == "i" else 0
        dqf.attrs["_FillValue"] = np.asarray([dqf_fill], dtype=dqf_dtype)
        monitoring = root.create_dataset("MonitoringMetaData", shape=(), dtype="i1")
        monitoring.attrs["Number_of_Input_Files"] = np.asarray([inputs], dtype="i4")
        monitoring.attrs["Number_of_Missing_Inputs"] = np.asarray(
            [missing_inputs], dtype="i4"
        )
        monitoring.attrs["Input_Files"] = np.bytes_(input_files.encode("ascii"))
        root.create_dataset("quality_information", shape=(), dtype="S1")
    return stream.getvalue()


def _listing_xml(
    *,
    key: str = KEY,
    size: int,
    etag: str = ETAG,
    last_modified: str = LAST_MODIFIED,
    truncated: str = "false",
    key_count: int = 1,
    include_checksum: bool = True,
) -> bytes:
    checksum = (
        "<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"
        "<ChecksumType>FULL_OBJECT</ChecksumType>"
        if include_checksum
        else ""
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<ListBucketResult xmlns="{native.NOAA_ERR_XML_NAMESPACE}">'
        f"<Name>{native.NOAA_ERR_BUCKET_NAME}</Name>"
        f"<Prefix>{PREFIX}</Prefix>"
        f"<KeyCount>{key_count}</KeyCount>"
        f"<MaxKeys>{native.NOAA_ERR_LIST_MAX_KEYS}</MaxKeys>"
        "<EncodingType>url</EncodingType>"
        f"<IsTruncated>{truncated}</IsTruncated>"
        "<Contents>"
        f"<Key>{key}</Key>"
        f"<LastModified>{last_modified}</LastModified>"
        f"<ETag>{etag.replace('&', '&amp;').replace(chr(34), '&quot;')}</ETag>"
        f"{checksum}"
        f"<Size>{size}</Size>"
        "<StorageClass>STANDARD</StorageClass>"
        "</Contents>"
        "</ListBucketResult>"
    ).encode()


def _listing_response(xml: bytes, *, received_at: float = 1_784_419_769.0):
    return native.S3HttpResponse(
        payload=xml,
        content_type="application/xml",
        content_length=len(xml),
        declared_content_length=None,
        received_at=received_at,
        request_id="LISTREQUEST123",
    )


def _object_response(payload: bytes, *, received_at: float = 1_784_419_770.0):
    return native.S3HttpResponse(
        payload=payload,
        content_type="binary/octet-stream",
        content_length=len(payload),
        declared_content_length=len(payload),
        received_at=received_at,
        etag=ETAG,
        last_modified=HTTP_LAST_MODIFIED,
        last_modified_timestamp=datetime(
            2026, 7, 19, 0, 9, 28, tzinfo=timezone.utc
        ).timestamp(),
        request_id="OBJECTREQUEST123",
        request_id_2="bounded-secondary-request-id",
        server_side_encryption="AES256",
    )


def _install_responses(monkeypatch, hdf: bytes, *, xml: bytes | None = None):
    listing = _listing_xml(size=len(hdf)) if xml is None else xml
    calls = {"listing": 0, "object": 0}

    def fetch_listing(prefix):
        calls["listing"] += 1
        assert prefix == PREFIX
        return _listing_response(listing)

    def fetch_object(selected):
        calls["object"] += 1
        assert selected.key == KEY
        assert selected.size_bytes == len(hdf)
        return _object_response(hdf)

    monkeypatch.setattr(native, "_fetch_listing", fetch_listing)
    monkeypatch.setattr(native, "_fetch_object", fetch_object)
    return calls


def _capture(monkeypatch, tmp_path, *, hdf: bytes | None = None):
    payload = _hdf_bytes() if hdf is None else hdf
    store = RawArtifactStore(tmp_path / "raw")
    calls = _install_responses(monkeypatch, payload)
    result = native.capture_latest_noaa_err_netcdf4(
        _job(),
        raw_store=store,
        now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
    )
    return store, payload, result, calls


def test_exact_listing_and_netcdf_are_durable_before_each_inspection(
    monkeypatch, tmp_path,
):
    hdf = _hdf_bytes(marker="durable-before-inspect")
    xml = _listing_xml(size=len(hdf))
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, hdf, xml=xml)
    real_xml_inspector = native._parse_listing_xml
    real_hdf_inspector = native._inspect_netcdf4_identity
    events = []

    def inspect_xml(payload, *, expected_prefix):
        assert payload == xml
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        events.append("xml")
        return real_xml_inspector(payload, expected_prefix=expected_prefix)

    def inspect_hdf(payload, *, selected):
        assert payload == hdf
        assert store.stats()["receipt_count"] == 2
        assert store.stats()["capture_count"] == 0
        events.append("hdf")
        return real_hdf_inspector(payload, selected=selected)

    monkeypatch.setattr(native, "_parse_listing_xml", inspect_xml)
    monkeypatch.setattr(native, "_inspect_netcdf4_identity", inspect_hdf)
    result = native.capture_latest_noaa_err_netcdf4(
        _job(),
        raw_store=store,
        now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
    )

    assert events == ["xml", "hdf"]
    assert store.stats()["capture_count"] == 1
    sources = result["raw_provenance"]["raw_sources"]
    assert store.read_bytes(sources["listing_xml"]["raw_receipt_id"]) == xml
    assert store.read_bytes(sources["netcdf4"]["raw_receipt_id"]) == hdf


def test_capture_is_payload_free_non_operational_unknown_evidence(monkeypatch, tmp_path):
    store, payload, result, _calls = _capture(monkeypatch, tmp_path)

    assert result["provider"] == "noaa_enterprise_rainrate"
    assert result["declared_quantity"] == "instantaneous_precipitation_rate"
    assert result["declared_unit"] == "mm/h"
    assert result["decode_status"] == "not_decoded"
    assert result["capture_state"] == "captured_not_decoded"
    assert result["weather_state"] == "unknown"
    assert result["quantitative_semantics_eligible"] is False
    assert result["operational_admission_eligible"] is False
    assert result["raw_provenance"]["structure"]["pixel_values_read"] is False
    assert set(result["raw_provenance"]["raw_sources"]) == {
        "listing_xml", "netcdf4"
    }
    for forbidden in (
        "areas",
        "observations",
        "observation_count",
        "overlay",
        "ground_confirmation",
        "is_raining",
        "is_dry",
    ):
        assert forbidden not in result
    encoded = json.dumps(result, sort_keys=True).encode()
    assert len(encoded) < 32 * 1024
    assert payload not in encoded
    assert store.integrity_check()["healthy"] is True


def test_listing_selection_binds_every_s3_fact_and_uses_latest_glb5():
    hdf = _hdf_bytes()
    older = KEY.replace("2350000_e202607182359599_c202607190008336", "2340000_e202607182349599_c202607182355000")
    partial = KEY.replace("GLB-5", "GLB-4").replace("2350000", "2355000")
    contents = "".join((
        f"<Contents><Key>{older}</Key><LastModified>2026-07-19T00:30:00.000Z</LastModified><ETag>&quot;{'1' * 32}&quot;</ETag><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType><Size>{len(hdf)}</Size><StorageClass>STANDARD</StorageClass></Contents>",
        f"<Contents><Key>{partial}</Key><LastModified>2026-07-19T00:40:00.000Z</LastModified><ETag>&quot;{'2' * 32}&quot;</ETag><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType><Size>{len(hdf)}</Size><StorageClass>STANDARD</StorageClass></Contents>",
        _listing_xml(size=len(hdf)).decode().split("<Contents>", 1)[1].split("</ListBucketResult>", 1)[0].join(("<Contents>", "")),
    ))
    xml = (
        f'<ListBucketResult xmlns="{native.NOAA_ERR_XML_NAMESPACE}">'
        f"<Name>{native.NOAA_ERR_BUCKET_NAME}</Name><Prefix>{PREFIX}</Prefix>"
        f"<KeyCount>3</KeyCount><MaxKeys>{native.NOAA_ERR_LIST_MAX_KEYS}</MaxKeys>"
        f"<EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>{contents}"
        "</ListBucketResult>"
    ).encode()

    selected = native._parse_listing_xml(xml, expected_prefix=PREFIX)

    assert selected.key == KEY
    assert selected.etag == ETAG
    assert selected.last_modified == LAST_MODIFIED
    assert selected.size_bytes == len(hdf)
    assert selected.checksum_algorithms == ("CRC32",)
    assert selected.checksum_type == "FULL_OBJECT"
    assert selected.product_start == PRODUCT_START


@pytest.mark.parametrize(
    ("xml", "message"),
    (
        (b'<!DOCTYPE x [<!ENTITY e SYSTEM "file:///etc/passwd">]><x>&e;</x>', "DTD/entity"),
        (b'<ListBucketResult xmlns="urn:evil"/>', "namespace"),
        (
            _listing_xml(size=len(_hdf_bytes()), truncated="true"),
            "getrunceerd",
        ),
        (
            _listing_xml(size=len(_hdf_bytes()), key_count=2),
            "KeyCount",
        ),
        (
            _listing_xml(size=len(_hdf_bytes()), key=KEY.replace("GLB-5", "GLB-4")),
            "geen complete GLB-5",
        ),
        (
            _listing_xml(size=len(_hdf_bytes()), key=KEY.replace(PREFIX, PREFIX + "../")),
            "onveilige objectsleutel",
        ),
    ),
)
def test_adversarial_or_incomplete_listing_fails_closed(xml, message):
    with pytest.raises(ValueError, match=message):
        native._parse_listing_xml(xml, expected_prefix=PREFIX)


def test_malformed_listing_is_retained_but_never_parsed_before_storage(
    monkeypatch, tmp_path,
):
    malformed = b"<not-closed"
    hdf = _hdf_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, hdf, xml=malformed)

    with pytest.raises(ValueError, match="veilige geldige"):
        native.capture_latest_noaa_err_netcdf4(
            _job(),
            raw_store=store,
            now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
        )

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    objects = list((tmp_path / "raw" / "objects").glob("*.raw"))
    assert len(objects) == 1 and objects[0].read_bytes() == malformed


def test_store_failure_stops_before_xml_inspection(monkeypatch, tmp_path):
    hdf = _hdf_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, hdf)
    store.initialize()
    monkeypatch.setattr(
        store,
        "store",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("store unavailable")),
    )
    monkeypatch.setattr(
        native,
        "_parse_listing_xml",
        lambda *_args, **_kwargs: pytest.fail("XML werd voor raw opslag bekeken"),
    )

    with pytest.raises(RuntimeError, match="store unavailable"):
        native.capture_latest_noaa_err_netcdf4(
            _job(), raw_store=store,
            now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize(
    ("hdf", "message"),
    (
        (b"not-hdf5", "signatuur"),
        (_hdf_bytes(rows=6500), "rastervorm|dimensieschalen"),
        (_hdf_bytes(columns=17999), "rastervorm|dimensieschalen"),
        (_hdf_bytes(rrqpe_dtype="u2"), "rasterdtype"),
        (_hdf_bytes(dqf_dtype="u1"), "rasterdtype"),
        (_hdf_bytes(platforms="GOES-19, GOES-18"), "vijf-inputblend"),
        (_hdf_bytes(title="FOREIGN_PRODUCT"), "productidentiteit"),
        (_hdf_bytes(start="2026-07-18T23:40:00Z"), "starttijd"),
        (_hdf_bytes(end="2026-07-18T23:51:00Z"), "tienminutenvenster"),
        (_hdf_bytes(inputs=4), "geen vijf inputs"),
        (_hdf_bytes(rrqpe_units="kg m-2"), "eenheid"),
    ),
)
def test_netcdf_substitutions_leave_two_receipts_but_no_capture(
    monkeypatch, tmp_path, hdf, message,
):
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, hdf)

    with pytest.raises(ValueError, match=message):
        native.capture_latest_noaa_err_netcdf4(
            _job(), raw_store=store,
            now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
        )

    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 0


def _mutated_hdf(mutator) -> bytes:
    stream = io.BytesIO(_hdf_bytes())
    with h5py.File(stream, "r+") as root:
        mutator(root)
    return stream.getvalue()


@pytest.mark.parametrize(
    "hdf",
    (
        _mutated_hdf(lambda root: root.__setitem__("soft", h5py.SoftLink("/RRQPE"))),
        _mutated_hdf(
            lambda root: root.__setitem__(
                "external", h5py.ExternalLink("outside.h5", "/data")
            )
        ),
    ),
    ids=("soft-link", "external-link"),
)
def test_external_or_symbolic_links_never_complete_capture(monkeypatch, tmp_path, hdf):
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, hdf)
    with pytest.raises(ValueError, match="externe of symbolische link"):
        native.capture_latest_noaa_err_netcdf4(
            _job(), raw_store=store,
            now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
        )
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 0


def test_inspector_never_indexes_any_hdf_dataset(monkeypatch):
    hdf = _hdf_bytes()
    selected = native._parse_listing_xml(
        _listing_xml(size=len(hdf)), expected_prefix=PREFIX
    )

    def forbidden_getitem(*_args, **_kwargs):
        raise AssertionError("pixel/datasetelement werd gelezen")

    monkeypatch.setattr(h5py.Dataset, "__getitem__", forbidden_getitem)
    identity = native._inspect_netcdf4_identity(hdf, selected=selected)
    assert identity.rows == 6501
    assert identity.columns == 18000


def test_same_job_and_receipt_only_crash_resume_without_network(monkeypatch, tmp_path):
    hdf = _hdf_bytes(marker="resume")
    store = RawArtifactStore(tmp_path / "raw")
    calls = _install_responses(monkeypatch, hdf)
    real_inspector = native._inspect_netcdf4_identity
    monkeypatch.setattr(
        native,
        "_inspect_netcdf4_identity",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("crash after raw")),
    )

    with pytest.raises(RuntimeError, match="crash after raw"):
        native.capture_latest_noaa_err_netcdf4(
            _job(), raw_store=store,
            now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
        )
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 0

    monkeypatch.setattr(native, "_inspect_netcdf4_identity", real_inspector)
    monkeypatch.setattr(
        native, "_fetch_listing", lambda *_args: pytest.fail("listingnetwerk op retry")
    )
    monkeypatch.setattr(
        native, "_fetch_object", lambda *_args: pytest.fail("objectnetwerk op retry")
    )
    first = native.capture_latest_noaa_err_netcdf4(
        _job(), raw_store=store,
        now=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
    )
    retry = native.capture_latest_noaa_err_netcdf4(
        _job(), raw_store=store,
        now=datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc),
    )

    assert first == retry
    assert calls == {"listing": 1, "object": 1}
    assert store.stats()["capture_count"] == 1


def test_complete_retry_authority_reads_each_member_exactly_once(
    monkeypatch, tmp_path,
):
    hdf = _hdf_bytes(marker="one-authority-pass")
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, hdf)
    first = native.capture_latest_noaa_err_netcdf4(
        _job("noaa-complete-one-pass"),
        raw_store=store,
        now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
    )
    real_read_verified = store._read_verified
    authority_reads: list[str] = []

    def counted(path, **kwargs):
        authority_reads.append(str(path))
        return real_read_verified(path, **kwargs)

    monkeypatch.setattr(store, "_read_verified", counted)
    monkeypatch.setattr(
        native, "_fetch_listing", lambda *_args: pytest.fail("listingnetwerk op retry")
    )
    monkeypatch.setattr(
        native, "_fetch_object", lambda *_args: pytest.fail("objectnetwerk op retry")
    )
    retry = native.capture_latest_noaa_err_netcdf4(
        _job("noaa-complete-one-pass"),
        raw_store=store,
        now=datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc),
    )
    assert retry == first
    assert len(authority_reads) == 2
    assert len(set(authority_reads)) == 2


def test_deterministic_inspector_rejection_is_cached_after_authority_read(
    monkeypatch, tmp_path,
):
    hdf = _hdf_bytes(marker="cached-rejection", missing_inputs=5)
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, hdf)
    real_inspector = native._inspect_netcdf4_identity
    inspector_calls = 0

    def counted(*args, **kwargs):
        nonlocal inspector_calls
        inspector_calls += 1
        return real_inspector(*args, **kwargs)

    monkeypatch.setattr(native, "_inspect_netcdf4_identity", counted)
    with pytest.raises(native.NoaaErrNativeUnsupportedError):
        native.capture_latest_noaa_err_netcdf4(
            _job("noaa-cached-rejection"),
            raw_store=store,
            now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
        )
    assert inspector_calls == 1
    assert native.NoaaErrNativeUnsupportedError.retryable is False
    assert native.NoaaErrNativeUnsupportedError.deterministic is True
    real_read_verified = store._read_verified
    authority_reads: list[str] = []

    def counted_read(path, **kwargs):
        authority_reads.append(str(path))
        return real_read_verified(path, **kwargs)

    monkeypatch.setattr(store, "_read_verified", counted_read)
    monkeypatch.setattr(
        native, "_fetch_listing", lambda *_args: pytest.fail("listingnetwerk op retry")
    )
    monkeypatch.setattr(
        native, "_fetch_object", lambda *_args: pytest.fail("objectnetwerk op retry")
    )
    with pytest.raises(native.NoaaErrNativeUnsupportedError):
        native.capture_latest_noaa_err_netcdf4(
            _job("noaa-cached-rejection"),
            raw_store=store,
            now=datetime(2026, 7, 21, 0, 0, tzinfo=timezone.utc),
        )
    assert inspector_calls == 1
    assert len(authority_reads) == 2
    assert len(set(authority_reads)) == 2


@pytest.mark.parametrize(
    ("error_type", "message"),
    (
        (TimeoutError, "transient inspector timeout"),
        (OSError, "transient inspector I/O"),
        (native.NoaaErrNativeCorruptionError, "possibly transient corruption"),
    ),
)
def test_timeout_io_or_corruption_inspector_failure_is_never_cached(
    monkeypatch, error_type, message,
):
    hdf = _hdf_bytes(marker=f"not-cached-{error_type.__name__}")
    selected = native._parse_listing_xml(
        _listing_xml(size=len(hdf)), expected_prefix=PREFIX
    )
    calls = 0

    def transient_failure(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise error_type(message)

    monkeypatch.setattr(native, "_inspect_netcdf4_identity", transient_failure)
    digest = hashlib.sha256(hdf).hexdigest()
    for _ in range(2):
        with pytest.raises(error_type, match="transient"):
            native._inspect_netcdf4_identity_cached(
                hdf, selected=selected, content_sha256=digest
            )
    assert calls == 2
    assert native.NoaaErrNativeCorruptionError.retryable is True
    assert native.NoaaErrNativeCorruptionError.deterministic is False


def test_crash_after_listing_resumes_fixed_selection_without_relisting(
    monkeypatch, tmp_path,
):
    hdf = _hdf_bytes(marker="resume-listing")
    store = RawArtifactStore(tmp_path / "raw")
    calls = _install_responses(monkeypatch, hdf)
    real_parser = native._parse_listing_xml
    monkeypatch.setattr(
        native,
        "_parse_listing_xml",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("parser crash")),
    )
    with pytest.raises(RuntimeError, match="parser crash"):
        native.capture_latest_noaa_err_netcdf4(
            _job(), raw_store=store,
            now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
        )
    assert store.stats()["receipt_count"] == 1

    monkeypatch.setattr(native, "_parse_listing_xml", real_parser)
    monkeypatch.setattr(
        native, "_fetch_listing", lambda *_args: pytest.fail("listingnetwerk op retry")
    )
    result = native.capture_latest_noaa_err_netcdf4(
        _job(), raw_store=store,
        now=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
    )
    assert result["capture_state"] == "captured_not_decoded"
    assert calls == {"listing": 1, "object": 1}


@pytest.mark.parametrize(
    "payload",
    (
        {"url": "https://localhost/"},
        {"token": "secret"},
        {"areas": []},
        {"now": "2026-07-19T00:00:00Z"},
        {"harmless": True},
    ),
)
def test_nonempty_job_payload_is_rejected_before_network(monkeypatch, tmp_path, payload):
    monkeypatch.setattr(
        native, "_fetch_listing", lambda *_args: pytest.fail("netwerk mocht niet starten")
    )
    with pytest.raises(ValueError, match="exact lege"):
        native.capture_latest_noaa_err_netcdf4(
            _job(payload=payload), raw_store=RawArtifactStore(tmp_path / "raw")
        )


@pytest.mark.parametrize(
    "url",
    (
        "http://noaa-enterprise-rainrate-pds.s3.amazonaws.com/object.nc",
        "https://localhost/object.nc",
        "https://127.0.0.1/object.nc",
        "https://noaa-enterprise-rainrate-pds.s3.amazonaws.com.evil.test/object.nc",
        "https://user@noaa-enterprise-rainrate-pds.s3.amazonaws.com/object.nc",
        "https://noaa-enterprise-rainrate-pds.s3.amazonaws.com:444/object.nc",
        "https://noaa-enterprise-rainrate-pds.s3.amazonaws.com/other.nc",
        "https://noaa-enterprise-rainrate-pds.s3.amazonaws.com/object.nc?redirect=x",
        "https://noaa-enterprise-rainrate-pds.s3.amazonaws.com/object.nc#fragment",
    ),
)
def test_fixed_s3_origin_path_query_contract_blocks_ssrf_and_substitution(url):
    with pytest.raises(ValueError, match="vaste publieke origin/pad"):
        native._validate_fixed_s3_url(
            url,
            expected_path="/object.nc",
            expected_query="",
        )


def test_redirect_handler_rejects_every_redirect():
    with pytest.raises(ValueError, match="redirect"):
        native._RejectRedirects().redirect_request(None, None, None, None, None, None)


def test_object_http_metadata_must_match_listing(monkeypatch):
    hdf = _hdf_bytes()
    selected = native._parse_listing_xml(
        _listing_xml(size=len(hdf)), expected_prefix=PREFIX
    )
    response = _object_response(hdf)
    bad = native.S3HttpResponse(
        payload=response.payload,
        content_type=response.content_type,
        content_length=response.content_length,
        declared_content_length=response.declared_content_length,
        received_at=response.received_at,
        etag='"00000000000000000000000000000000"',
        last_modified=response.last_modified,
        last_modified_timestamp=response.last_modified_timestamp,
    )
    monkeypatch.setattr(native, "_fetch_s3", lambda *_args, **_kwargs: bad)
    with pytest.raises(ValueError, match="ETag"):
        native._fetch_object(selected)


def test_real_golden_contract_is_documented_as_optional_network_check():
    """Pin the exact public golden facts verified during adapter construction."""

    assert native.NOAA_ERR_GRID_ROWS == 6501
    assert native.NOAA_ERR_GRID_COLUMNS == 18000
    assert native.NOAA_ERR_FINAL_BLEND_INPUTS == frozenset({
        "GOES-19", "GOES-18", "Himawari-9", "MSG2", "MSG3"
    })
    assert native.NOAA_ERR_ALLOWED_OBJECT_CONTENT_TYPES >= {"binary/octet-stream"}


def test_candidate_is_not_wired_into_the_production_worker_handler_table():
    from storm_tracker_worker.service import PRODUCTION_JOB_HANDLERS

    reference = PRODUCTION_JOB_HANDLERS["enterprise_rainrate_fetch"]
    assert reference.module == "storm_tracker_worker.enterprise_rainrate"
    assert reference.qualname == "enterprise_rainrate_fetch"
