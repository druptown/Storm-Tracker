from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
from types import SimpleNamespace

import h5py
import numpy as np
import pytest

from storm_tracker_worker import knmi_native
from storm_tracker_worker.raw_artifacts import (
    RawArtifactConflictError,
    RawArtifactStore,
)


FILENAME = "RAD_NL25_RAC_RT_202608021200.h5"
SIGNED_URL = (
    "https://knmi-shadow.s3.eu-west-1.amazonaws.com/knmi/radar.h5"
    "?X-Amz-Credential=temporary&X-Amz-Signature=do-not-store"
)
PRODUCT_START = "02-AUG-2026;11:55:00.000"
PRODUCT_END = "02-AUG-2026;12:00:00.000"
LAST_MODIFIED = "2026-08-02T12:00:15Z"
CREATED = "2026-08-02T12:00:16Z"


def _hdf_bytes(
    *,
    marker: int = 0,
    product_name: str = "RAD_NL25_COR_NA",
    image_product_name: str | None = None,
    tag_version: str = "3.6",
    product_start: str = PRODUCT_START,
    product_end: str = PRODUCT_END,
    shape: tuple[int, ...] = (765, 700),
    dtype=np.uint16,
    image_geo_parameter: str = "PRECIP_[MM]",
    compressed: bool = True,
) -> bytes:
    stream = io.BytesIO()
    with h5py.File(stream, "w") as root:
        overview = root.create_group("overview")
        overview.attrs["product_group_name"] = np.bytes_(product_name.encode())
        overview.attrs["hdftag_version_number"] = np.bytes_(tag_version.encode())
        overview.attrs["product_datetime_start"] = np.bytes_(product_start.encode())
        overview.attrs["product_datetime_end"] = np.bytes_(product_end.encode())
        geographic = root.create_group("geographic")
        geographic.attrs["geo_number_columns"] = 700
        geographic.attrs["geo_number_rows"] = 765
        image = root.create_group("image1")
        image.attrs["image_product_name"] = np.bytes_((
            product_name if image_product_name is None else image_product_name
        ).encode())
        image.attrs["image_geo_parameter"] = np.bytes_(image_geo_parameter.encode())
        itemsize = np.dtype(dtype).itemsize
        image.attrs["image_bytes_per_pixel"] = itemsize
        image.attrs["image_size"] = int(np.prod(shape)) * itemsize
        values = np.zeros(shape, dtype=dtype)
        values.flat[0] = marker
        storage = (
            {"chunks": shape, "compression": "gzip"}
            if compressed
            else {}
        )
        image.create_dataset("image_data", data=values, **storage)
        for group_name, parameter in (
            ("image2", "QUALITY_[-]"),
            ("image3", "ADJUSTMENT_FACTOR_[DB]"),
        ):
            auxiliary = root.create_group(group_name)
            auxiliary.attrs["image_product_name"] = np.bytes_(product_name.encode())
            auxiliary.attrs["image_geo_parameter"] = np.bytes_(parameter.encode())
            # The live upstream file declares bpp/size as if these uint8
            # layers were uint16. Preserve that observed inconsistency in the
            # golden-shaped fixture; the structural inspector does not trust
            # these two attributes for the auxiliary layers.
            auxiliary.attrs["image_bytes_per_pixel"] = 2
            auxiliary.attrs["image_size"] = 765 * 700 * 2
            auxiliary_storage = (
                {"chunks": (765, 700), "compression": "gzip"}
                if compressed
                else {}
            )
            auxiliary.create_dataset(
                "image_data",
                data=np.zeros((765, 700), dtype=np.uint8),
                **auxiliary_storage,
            )
    return stream.getvalue()


def _mutated_hdf(mutator) -> bytes:
    stream = io.BytesIO(_hdf_bytes())
    with h5py.File(stream, "r+") as root:
        mutator(root)
    return stream.getvalue()


def _add_external_dataset_storage(root) -> None:
    root.create_dataset(
        "external_storage",
        shape=(1,),
        dtype="u1",
        external=[("outside.bin", 0, 1)],
    )


def _add_virtual_dataset(root) -> None:
    layout = h5py.VirtualLayout(shape=(1,), dtype="u1")
    layout[:] = h5py.VirtualSource("outside.h5", "data", shape=(1,))
    root.create_virtual_dataset("virtual_storage", layout)


def _remove_image2(root) -> None:
    del root["image2"]


def _change_image3_quantity(root) -> None:
    root["image3"].attrs["image_geo_parameter"] = np.bytes_(b"UNKNOWN_[-]")


def _job(job_id: str = "knmi-native-slot-001", payload: dict | None = None):
    return SimpleNamespace(job_id=job_id, payload={} if payload is None else payload)


def _response(payload: bytes, content_type: str, received_at: float):
    return knmi_native._HttpBytes(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
    )


def _summary(hdf_size: int) -> knmi_native.KnmiFileSummary:
    return knmi_native.KnmiFileSummary(
        filename=FILENAME,
        last_modified=LAST_MODIFIED,
        last_modified_timestamp=datetime.fromisoformat(
            LAST_MODIFIED.replace("Z", "+00:00")
        ).timestamp(),
        size_bytes=hdf_size,
        created=CREATED,
        created_timestamp=datetime.fromisoformat(
            CREATED.replace("Z", "+00:00")
        ).timestamp(),
    )


def _descriptor(hdf_size: int) -> knmi_native.KnmiDownloadDescriptor:
    return knmi_native.KnmiDownloadDescriptor(
        content_type="application/x-hdf5",
        size_bytes=hdf_size,
        last_modified=LAST_MODIFIED,
        last_modified_timestamp=datetime.fromisoformat(
            LAST_MODIFIED.replace("Z", "+00:00")
        ).timestamp(),
        temporary_download_url=SIGNED_URL,
    )


def _install_native_responses(monkeypatch, responses):
    responses = tuple(responses)
    queue = iter(responses)
    hdf_size = len(responses[0].payload)
    summary = _summary(hdf_size)
    descriptor = _descriptor(hdf_size)
    calls = {"latest": 0, "descriptor": 0, "download": 0}
    monkeypatch.setenv("STV3_KNMI_API_KEY", "worker-only-key")

    def latest(*, api_key):
        calls["latest"] += 1
        return (
            summary
            if api_key == "worker-only-key"
            else pytest.fail("onjuiste API-key")
        )

    def describe(selected, *, api_key):
        calls["descriptor"] += 1
        return (
            descriptor
            if selected == summary and api_key == "worker-only-key"
            else pytest.fail("onjuiste URL-selectie")
        )

    def download(selected):
        calls["download"] += 1
        return (
            next(queue)
            if selected == descriptor
            else pytest.fail("onjuiste download-URL")
        )

    monkeypatch.setattr(
        knmi_native,
        "_latest_filename",
        latest,
    )
    monkeypatch.setattr(
        knmi_native,
        "_temporary_download_url",
        describe,
    )
    monkeypatch.setattr(
        knmi_native,
        "_download_hdf5",
        download,
    )
    return calls


def _capture(monkeypatch, tmp_path, *, hdf: bytes | None = None):
    payload = _hdf_bytes() if hdf is None else hdf
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(payload, "application/x-hdf5", 1_806_837_603.25)],
    )
    result = knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)
    return store, payload, result


def test_exact_hdf_is_durable_before_first_hdf_inspection(monkeypatch, tmp_path):
    hdf = _hdf_bytes(marker=23)
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(hdf, "application/x-hdf5", 1_806_837_603.25)],
    )
    real_inspector = knmi_native._inspect_hdf_identity
    inspections: list[bytes] = []

    def inspect(payload: bytes):
        stats = store.stats()
        assert stats["object_count"] == 1
        assert stats["receipt_count"] == 1
        assert stats["capture_count"] == 0
        objects = list((tmp_path / "raw" / "objects").glob("*.raw"))
        assert len(objects) == 1
        assert objects[0].read_bytes() == hdf
        inspections.append(payload)
        return real_inspector(payload)

    monkeypatch.setattr(knmi_native, "_inspect_hdf_identity", inspect)
    result = knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)

    assert inspections == [hdf]
    assert store.stats()["capture_count"] == 1
    source = result["raw_provenance"]["raw_sources"]["hdf5"]
    assert store.read_bytes(source["raw_receipt_id"]) == hdf
    assert source["raw_content_sha256"] == hashlib.sha256(hdf).hexdigest()
    assert source["upstream_filename"] == FILENAME
    assert source["received_at"] == 1_806_837_603.25
    assert source["product_timestamp"] == datetime(
        2026, 8, 2, 12, 0, tzinfo=timezone.utc
    ).timestamp()
    assert source["received_at"] != source["product_timestamp"]


def test_capture_result_is_evidence_only_and_contains_no_dry_claim(
    monkeypatch, tmp_path,
):
    store, _payload, result = _capture(monkeypatch, tmp_path)

    assert result["provider"] == "knmi"
    assert result["dataset_name"] == "nl_rdr_data_rtcor_5m"
    assert result["dataset_version"] == "1.0"
    assert result["declared_quantity"] == "precipitation_accumulation"
    assert result["declared_unit"] == "mm"
    assert result["declared_accumulation_minutes"] == 5.0
    assert result["quantitative_semantics_eligible"] is False
    assert result["operational_admission_eligible"] is False
    assert result["decode_status"] == "not_decoded"
    assert result["weather_state"] == "unknown"
    assert result["capture_state"] == "captured_not_decoded"
    assert "observation_count" not in result
    assert "observations" not in result
    assert "overlay" not in result
    assert "rain" not in result
    assert "dry" not in result
    assert "artifact_path" not in result
    assert store.integrity_check()["healthy"] is True


def test_control_plane_auth_is_not_forwarded_or_persisted(monkeypatch, tmp_path):
    hdf = _hdf_bytes()
    listing = json.dumps({
        "isTruncated": True,
        "files": [{
            "filename": FILENAME,
            "lastModified": LAST_MODIFIED,
            "size": len(hdf),
            "created": CREATED,
        }],
        "resultCount": 1,
        "maxResults": 1,
        "startAfterFilename": "",
        "nextPageToken": "opaque-next-page-token",
    }).encode()
    url_body = json.dumps({
        "contentType": "application/octet-stream",
        "size": str(len(hdf)),
        "lastModified": LAST_MODIFIED,
        "temporaryDownloadUrl": SIGNED_URL,
    }).encode()
    payloads = iter(
        (
            (listing, "application/json"),
            (url_body, "application/json; charset=utf-8"),
            (hdf, "application/octet-stream"),
        )
    )
    requests = []

    class Response(io.BytesIO):
        def __init__(self, body: bytes, content_type: str):
            super().__init__(body)
            self.headers = {
                "Content-Encoding": "identity",
                "Content-Length": str(len(body)),
                "Content-Type": content_type,
            }

    def open_request(request, *, timeout):
        requests.append((request, timeout))
        body, content_type = next(payloads)
        return Response(body, content_type)

    monkeypatch.setattr(knmi_native, "_open_without_redirects", open_request)
    monkeypatch.setenv("STV3_KNMI_API_KEY", "worker-only-key")
    store = RawArtifactStore(tmp_path / "raw")

    result = knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)

    assert len(requests) == 3
    assert requests[0][0].get_header("Authorization") == "worker-only-key"
    assert requests[1][0].get_header("Authorization") == "worker-only-key"
    assert requests[2][0].full_url == SIGNED_URL
    assert requests[2][0].get_header("Authorization") is None
    assert requests[2][0].get_header("Accept-encoding") == "identity"

    serialized = json.dumps(result, sort_keys=True)
    receipt = store.load_receipt(
        result["raw_provenance"]["raw_sources"]["hdf5"]["raw_receipt_id"]
    )
    assert receipt is not None
    persisted = json.dumps(receipt.to_metadata(), sort_keys=True)
    for forbidden in (
        "worker-only-key",
        "X-Amz-Credential",
        "X-Amz-Signature",
        "do-not-store",
        "knmi-shadow.s3.eu-west-1.amazonaws.com",
        "https://",
    ):
        assert forbidden not in serialized
        assert forbidden not in persisted


def test_malformed_hdf_retains_receipt_but_never_complete_capture(
    monkeypatch, tmp_path,
):
    payload = b"not-an-hdf5-file"
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(payload, "application/octet-stream", 1_806_837_603.0)],
    )

    with pytest.raises(ValueError, match="HDF5-container"):
        knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)

    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    objects = list((tmp_path / "raw" / "objects").glob("*.raw"))
    assert len(objects) == 1
    assert objects[0].read_bytes() == payload


def test_wrong_hdf_product_retains_receipt_only(monkeypatch, tmp_path):
    payload = _hdf_bytes(product_name="SAT MSG CLOUD")
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(payload, "application/x-hdf5", 1_806_837_603.0)],
    )

    with pytest.raises(ValueError, match="realtime radaraccumulatieproduct"):
        knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


@pytest.mark.parametrize(
    ("hdf", "message"),
    (
        (_hdf_bytes(product_name="RAD BX21 RAC RT"), "realtime radaraccumulatieproduct"),
        (_hdf_bytes(product_name="RAD NL25 COR NA"), "realtime radaraccumulatieproduct"),
        (_hdf_bytes(shape=(700, 765)), "rastervorm"),
        (_hdf_bytes(shape=(765 * 700,)), "rastervorm"),
        (
            _hdf_bytes(product_start=PRODUCT_END, product_end=PRODUCT_END),
            "exacte vijfminutenperiode",
        ),
        (_hdf_bytes(tag_version="3.7"), "HDF-tagversie"),
        (
            _hdf_bytes(image_product_name="RAD NL25 RAC RT"),
            "image is niet het NL25 realtime product",
        ),
        (_hdf_bytes(dtype=np.int16), "rasterdtype"),
        (_hdf_bytes(dtype=np.uint8), "rasterdtype"),
        (_hdf_bytes(image_geo_parameter="DBZH_[DBZ]"), "PRECIP"),
    ),
    ids=(
        "foreign-grid-product",
        "token-equivalent-product-literal",
        "transposed-grid",
        "one-dimensional-grid",
        "zero-duration",
        "unsupported-hdftag",
        "mismatched-image-product",
        "signed-dtype",
        "uint8-primary-dtype",
        "wrong-quantity",
    ),
)
def test_structural_hdf_substitutions_remain_receipt_only(
    monkeypatch, tmp_path, hdf, message,
):
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(hdf, "application/x-hdf5", 1_806_837_603.0)],
    )

    with pytest.raises(ValueError, match=message):
        knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


@pytest.mark.parametrize(
    "hdf",
    (
        _mutated_hdf(lambda root: root.__setitem__("soft", h5py.SoftLink("/image1"))),
        _mutated_hdf(
            lambda root: root.__setitem__(
                "external", h5py.ExternalLink("outside.h5", "/data")
            )
        ),
    ),
    ids=("soft-link", "external-link"),
)
def test_non_internal_hdf_links_are_rejected_before_capture(
    monkeypatch, tmp_path, hdf,
):
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(hdf, "application/x-hdf5", 1_806_837_603.0)],
    )

    with pytest.raises(ValueError, match="externe of symbolische link"):
        knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


@pytest.mark.parametrize(
    "hdf",
    (
        _mutated_hdf(_add_external_dataset_storage),
        _mutated_hdf(_add_virtual_dataset),
    ),
    ids=("external-dataset-storage", "virtual-dataset"),
)
def test_external_or_virtual_dataset_storage_remains_receipt_only(
    monkeypatch, tmp_path, hdf,
):
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(hdf, "application/x-hdf5", 1_806_837_603.0)],
    )

    with pytest.raises(ValueError, match="externe of virtuele datasetopslag"):
        knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


@pytest.mark.parametrize(
    ("hdf", "message"),
    (
        (_mutated_hdf(_remove_image2), "pad image2"),
        (_mutated_hdf(_change_image3_quantity), "image3 heeft een andere quantity"),
    ),
    ids=("missing-quality-layer", "wrong-adjustment-layer"),
)
def test_required_quality_and_adjustment_layers_are_structurally_bound(
    monkeypatch, tmp_path, hdf, message,
):
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(hdf, "application/x-hdf5", 1_806_837_603.0)],
    )

    with pytest.raises(ValueError, match=message):
        knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_filename_product_time_mismatch_is_not_a_complete_capture(monkeypatch, tmp_path):
    hdf = _hdf_bytes(
        product_start="02-AUG-2026;12:00:00.000",
        product_end="02-AUG-2026;12:05:00.000",
    )
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(hdf, "application/x-hdf5", 1_806_837_903.0)],
    )

    with pytest.raises(ValueError, match="bestandsnaam en HDF5-producttijd"):
        knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_store_failure_prevents_any_hdf_inspection(monkeypatch, tmp_path):
    hdf = _hdf_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(hdf, "application/x-hdf5", 1_806_837_603.0)],
    )
    store.initialize()
    monkeypatch.setattr(
        store,
        "store",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("store unavailable")),
    )
    monkeypatch.setattr(
        knmi_native,
        "_inspect_hdf_identity",
        lambda _payload: pytest.fail("HDF5 werd vóór duurzame opslag geïnspecteerd"),
    )

    with pytest.raises(RuntimeError, match="store unavailable"):
        knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=store)


def test_same_job_retry_is_idempotent_and_next_slot_has_new_receipts(
    monkeypatch, tmp_path,
):
    hdf = _hdf_bytes(marker=7)
    store = RawArtifactStore(tmp_path / "raw")
    calls = _install_native_responses(
        monkeypatch,
        [
            _response(hdf, "application/x-hdf5", 1_806_837_603.0),
            _response(hdf, "application/x-hdf5", 1_806_837_903.0),
        ],
    )

    first = knmi_native.capture_latest_knmi_hdf5(
        _job("knmi-slot-001"), raw_store=store
    )
    retry = knmi_native.capture_latest_knmi_hdf5(
        _job("knmi-slot-001"), raw_store=store
    )
    next_slot = knmi_native.capture_latest_knmi_hdf5(
        _job("knmi-slot-002"), raw_store=store
    )
    first_raw = first["raw_provenance"]
    retry_raw = retry["raw_provenance"]
    next_raw = next_slot["raw_provenance"]

    assert retry_raw == first_raw
    assert next_raw["raw_capture_id"] != first_raw["raw_capture_id"]
    first_source = first_raw["raw_sources"]["hdf5"]
    next_source = next_raw["raw_sources"]["hdf5"]
    assert next_source["raw_receipt_id"] != first_source["raw_receipt_id"]
    assert next_source["raw_content_sha256"] == first_source["raw_content_sha256"]
    assert first_source["received_at"] == 1_806_837_603.0
    assert next_source["received_at"] == 1_806_837_903.0
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 2
    assert calls == {"latest": 2, "descriptor": 2, "download": 2}


def test_retry_after_store_resumes_exact_receipt_without_network_or_api_key(
    monkeypatch, tmp_path,
):
    hdf = _hdf_bytes(marker=31)
    store = RawArtifactStore(tmp_path / "raw")
    calls = _install_native_responses(
        monkeypatch,
        [_response(hdf, "application/x-hdf5", 1_806_837_603.0)],
    )
    real_inspector = knmi_native._inspect_hdf_identity
    monkeypatch.setattr(
        knmi_native,
        "_inspect_hdf_identity",
        lambda _payload: (_ for _ in ()).throw(RuntimeError("simulated crash")),
    )

    with pytest.raises(RuntimeError, match="simulated crash"):
        knmi_native.capture_latest_knmi_hdf5(
            _job("knmi-resume-001"), raw_store=store
        )

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    monkeypatch.delenv("STV3_KNMI_API_KEY")
    monkeypatch.setattr(knmi_native, "_inspect_hdf_identity", real_inspector)

    result = knmi_native.capture_latest_knmi_hdf5(
        _job("knmi-resume-001"), raw_store=store
    )

    assert result["capture_state"] == "captured_not_decoded"
    assert store.stats()["capture_count"] == 1
    assert calls == {"latest": 1, "descriptor": 1, "download": 1}


def test_stable_filename_cannot_silently_change_bytes(monkeypatch, tmp_path):
    first_hdf = _hdf_bytes(marker=1)
    changed_hdf = _hdf_bytes(marker=2)
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [
            _response(first_hdf, "application/x-hdf5", 1_806_837_603.0),
            _response(changed_hdf, "application/x-hdf5", 1_806_837_903.0),
        ],
    )
    knmi_native.capture_latest_knmi_hdf5(
        _job("knmi-slot-001"), raw_store=store
    )

    with pytest.raises(RawArtifactConflictError, match="upstream-id"):
        knmi_native.capture_latest_knmi_hdf5(
            _job("knmi-slot-002"), raw_store=store
        )

    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1


@pytest.mark.parametrize(
    "payload",
    (
        {"api_key": "leak"},
        {"Authorization": "leak"},
        {"temporaryDownloadUrl": SIGNED_URL},
        {"access_token": "leak"},
        {"nested": {"token": "leak"}},
        {"harmless": 1},
    ),
)
def test_job_payload_secrets_are_rejected_before_network(
    monkeypatch, tmp_path, payload,
):
    monkeypatch.setenv("STV3_KNMI_API_KEY", "worker-only-key")
    monkeypatch.setattr(
        knmi_native,
        "_latest_filename",
        lambda **_kwargs: pytest.fail("netwerkselectie had niet mogen starten"),
    )

    with pytest.raises(ValueError, match="exact leeg"):
        knmi_native.capture_latest_knmi_hdf5(
            _job(payload=payload), raw_store=RawArtifactStore(tmp_path / "raw")
        )


@pytest.mark.parametrize(
    "filename",
    (
        "../radar.h5",
        "directory/radar.h5",
        "directory\\radar.h5",
        "radar.png",
        ".hidden.h5",
        "radar..h5",
    ),
)
def test_unsafe_or_non_hdf_filename_is_rejected(filename):
    with pytest.raises(ValueError, match="onveilige HDF5-bestandsnaam"):
        knmi_native._safe_filename(filename)


def test_duplicate_control_json_keys_fail_closed():
    with pytest.raises(ValueError, match="dubbele JSON-sleutels"):
        knmi_native._json_without_duplicates(
            b'{"files":[],"files":[{"filename":"radar.h5"}]}'
        )


def test_oversized_control_response_is_rejected_before_body_read(monkeypatch):
    class OversizeResponse(io.BytesIO):
        def __init__(self):
            super().__init__(b"must-not-be-read")
            self.headers = {
                "Content-Encoding": "identity",
                "Content-Length": str(knmi_native.MAX_CONTROL_JSON_BYTES + 1),
                "Content-Type": "application/json",
            }

        def read(self, *_args, **_kwargs):
            pytest.fail("oversized KNMI-responsebody werd gelezen")

    monkeypatch.setattr(
        knmi_native,
        "_open_without_redirects",
        lambda *_args, **_kwargs: OversizeResponse(),
    )
    endpoint = (
        f"{knmi_native.KNMI_OPEN_DATA_BASE}/datasets/"
        f"{knmi_native.KNMI_DATASET_NAME}/versions/1.0/files"
    )

    with pytest.raises(ValueError, match="contractlimiet"):
        knmi_native._api_json(endpoint, api_key="worker-only-key")


def test_invalid_signed_url_error_never_echoes_credential():
    secret = "https://user:ultra-secret@example.invalid/file.h5"
    with pytest.raises(ValueError) as caught:
        response = {
            "contentType": "application/x-hdf5",
            "size": "1234",
            "lastModified": LAST_MODIFIED,
            "temporaryDownloadUrl": secret,
        }
        original = knmi_native._api_json
        try:
            knmi_native._api_json = lambda *_args, **_kwargs: response
            summary = _summary(1234)
            knmi_native._temporary_download_url(summary, api_key="key")
        finally:
            knmi_native._api_json = original
    assert "ultra-secret" not in str(caught.value)
    assert secret not in str(caught.value)


def test_signed_url_control_character_and_transport_errors_are_redacted(monkeypatch):
    secret_url = (
        "https://bucket.s3.amazonaws.com/file.h5"
        "?X-Amz-Credential=ultra secret&X-Amz-Signature=hidden"
    )
    with pytest.raises(ValueError) as caught:
        knmi_native._validated_download_url(secret_url)
    assert "ultra secret" not in str(caught.value)
    assert secret_url not in str(caught.value)

    descriptor = _descriptor(1234)
    monkeypatch.setattr(
        knmi_native,
        "_read_response",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError(f"transport failed for {SIGNED_URL}")
        ),
    )
    with pytest.raises(ValueError, match="objectdownload mislukt") as caught:
        knmi_native._download_hdf5(descriptor)
    assert "X-Amz-Credential" not in str(caught.value)
    assert SIGNED_URL not in str(caught.value)


@pytest.mark.parametrize(
    "url",
    (
        "https://127.0.0.1/radar.h5",
        "https://[::1]/radar.h5",
        "https://localhost/radar.h5",
        "https://169.254.169.254/latest/meta-data",
        "https://bucket.s3.amazonaws.com.evil.invalid/radar.h5",
        "https://bucket.s3.amazonaws.com:444/radar.h5",
        "http://bucket.s3.amazonaws.com/radar.h5",
    ),
)
def test_signed_url_ssrf_and_origin_substitutions_are_rejected(url):
    with pytest.raises(ValueError, match="tijdelijke downloadlink is ongeldig"):
        knmi_native._validated_download_url(url)


def test_redirects_are_never_followed():
    with pytest.raises(ValueError, match="redirect"):
        knmi_native._RejectRedirects().redirect_request(
            object(), None, 302, "Found", {}, SIGNED_URL
        )


def _listing_response(*, size: int = 1234, extra: dict | None = None):
    result = {
        "isTruncated": True,
        "files": [{
            "filename": FILENAME,
            "lastModified": LAST_MODIFIED,
            "size": size,
            "created": CREATED,
        }],
        "resultCount": 1,
        "maxResults": 1,
        "startAfterFilename": "",
        "nextPageToken": "opaque-token",
    }
    if extra:
        result.update(extra)
    return result


def test_listing_must_match_complete_official_openapi_schema(monkeypatch):
    incomplete = {"files": [{"filename": FILENAME}]}
    monkeypatch.setattr(knmi_native, "_api_json", lambda *_args, **_kwargs: incomplete)
    with pytest.raises(ValueError, match="vaste KNMI OpenAPI-schema"):
        knmi_native._latest_filename(api_key="key")

    wrong_size_type = _listing_response(size=1234)
    wrong_size_type["files"][0]["size"] = "1234"
    monkeypatch.setattr(
        knmi_native, "_api_json", lambda *_args, **_kwargs: wrong_size_type
    )
    with pytest.raises(ValueError, match="geen bestandsgrootte"):
        knmi_native._latest_filename(api_key="key")

    unexpected = _listing_response(extra={"surprise": "field"})
    monkeypatch.setattr(knmi_native, "_api_json", lambda *_args, **_kwargs: unexpected)
    with pytest.raises(ValueError, match="vaste KNMI OpenAPI-schema"):
        knmi_native._latest_filename(api_key="key")


def test_download_descriptor_is_bound_to_listing_metadata(monkeypatch):
    summary = _summary(1234)
    response = {
        "contentType": "application/x-hdf5",
        "size": "1235",
        "lastModified": LAST_MODIFIED,
        "temporaryDownloadUrl": SIGNED_URL,
    }
    monkeypatch.setattr(knmi_native, "_api_json", lambda *_args, **_kwargs: response)
    with pytest.raises(ValueError, match="bestandsgrootte veranderde"):
        knmi_native._temporary_download_url(summary, api_key="key")

    response["size"] = 1234
    monkeypatch.setattr(knmi_native, "_api_json", lambda *_args, **_kwargs: response)
    with pytest.raises(ValueError, match="geen bestandsgrootte"):
        knmi_native._temporary_download_url(summary, api_key="key")


def test_download_body_is_bound_to_descriptor_size_and_content_type(monkeypatch):
    descriptor = _descriptor(1234)
    monkeypatch.setattr(
        knmi_native,
        "_read_response",
        lambda *_args, **_kwargs: _response(
            b"too-short", "application/x-hdf5", 1_806_837_603.0
        ),
    )
    with pytest.raises(ValueError, match="downloadgrootte"):
        knmi_native._download_hdf5(descriptor)

    descriptor = knmi_native.KnmiDownloadDescriptor(
        content_type="application/octet-stream",
        size_bytes=len(b"exact"),
        last_modified=LAST_MODIFIED,
        last_modified_timestamp=_summary(1234).last_modified_timestamp,
        temporary_download_url=SIGNED_URL,
    )
    monkeypatch.setattr(
        knmi_native,
        "_read_response",
        lambda *_args, **_kwargs: _response(
            b"exact", "application/x-hdf5", 1_806_837_603.0
        ),
    )
    with pytest.raises(ValueError, match="Content-Type"):
        knmi_native._download_hdf5(descriptor)


def test_hdf_structural_identity_does_not_include_raster_values():
    first = knmi_native._inspect_hdf_identity(_hdf_bytes(marker=1))
    second = knmi_native._inspect_hdf_identity(_hdf_bytes(marker=65000))
    assert first == second


def test_safe_physical_storage_layout_is_not_meteorological_identity():
    compressed = knmi_native._inspect_hdf_identity(_hdf_bytes(compressed=True))
    contiguous = knmi_native._inspect_hdf_identity(_hdf_bytes(compressed=False))
    assert compressed == contiguous


def test_result_and_receipt_are_bounded_and_payload_free(monkeypatch, tmp_path):
    store, payload, result = _capture(monkeypatch, tmp_path)
    metadata = deepcopy(result)
    encoded = json.dumps(metadata, sort_keys=True).encode()
    assert len(encoded) < 32 * 1024
    assert payload not in encoded
    assert "payload" not in metadata
    assert "temporaryDownloadUrl" not in encoded.decode()
    assert store.stats()["filesystem_bytes"] >= len(payload)


def test_complete_retry_reopens_store_reads_member_once_and_never_reinspects(
    monkeypatch, tmp_path,
):
    store, _payload, first = _capture(monkeypatch, tmp_path)
    reopened = RawArtifactStore(store.root)
    reopened.initialize()
    real_read = reopened._read_verified  # noqa: SLF001 - counted authority read
    reads = []

    def counted(*args, **kwargs):
        reads.append(args[0])
        return real_read(*args, **kwargs)

    monkeypatch.setattr(reopened, "_read_verified", counted)
    monkeypatch.setattr(
        knmi_native,
        "_inspect_hdf_identity",
        lambda *_args, **_kwargs: pytest.fail("successverdict moest HDF-reparse voorkomen"),
    )
    monkeypatch.setattr(
        knmi_native,
        "_download_hdf5",
        lambda *_args, **_kwargs: pytest.fail("complete retry mocht niet downloaden"),
    )

    retry = knmi_native.capture_latest_knmi_hdf5(_job(), raw_store=reopened)

    assert retry == first
    assert len(reads) == 1


def test_deterministic_rejection_survives_store_reopen_without_reinspection(
    monkeypatch, tmp_path,
):
    payload = _hdf_bytes(product_name="SAT MSG CLOUD")
    root = tmp_path / "raw"
    store = RawArtifactStore(root)
    _install_native_responses(
        monkeypatch,
        [_response(payload, "application/x-hdf5", 1_806_837_603.0)],
    )
    job = _job("knmi-durable-rejection")
    with pytest.raises(ValueError, match="realtime radaraccumulatieproduct"):
        knmi_native.capture_latest_knmi_hdf5(job, raw_store=store)

    reopened = RawArtifactStore(root)
    monkeypatch.setattr(
        knmi_native,
        "_inspect_hdf_identity",
        lambda *_args, **_kwargs: pytest.fail("duurzame afkeur mocht niet herinspecteren"),
    )
    monkeypatch.delenv("STV3_KNMI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="realtime radaraccumulatieproduct"):
        knmi_native.capture_latest_knmi_hdf5(job, raw_store=reopened)


def test_timeout_is_not_persisted_as_deterministic_knmi_rejection(
    monkeypatch, tmp_path,
):
    payload = _hdf_bytes(marker=91)
    store = RawArtifactStore(tmp_path / "raw")
    _install_native_responses(
        monkeypatch,
        [_response(payload, "application/x-hdf5", 1_806_837_603.0)],
    )
    job = _job("knmi-transient-timeout")
    real_inspector = knmi_native._inspect_hdf_identity
    monkeypatch.setattr(
        knmi_native,
        "_inspect_hdf_identity",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("synthetic timeout")),
    )
    with pytest.raises(TimeoutError, match="synthetic"):
        knmi_native.capture_latest_knmi_hdf5(job, raw_store=store)
    verdicts = store.root / "knmi_validation_verdicts"
    assert list(verdicts.glob("*.json")) == []

    monkeypatch.setattr(knmi_native, "_inspect_hdf_identity", real_inspector)
    monkeypatch.delenv("STV3_KNMI_API_KEY", raising=False)
    result = knmi_native.capture_latest_knmi_hdf5(job, raw_store=RawArtifactStore(store.root))
    assert result["capture_state"] == "captured_not_decoded"


def test_rtcor_filename_identity_is_case_sensitive():
    identity = knmi_native._inspect_hdf_identity(_hdf_bytes())
    with pytest.raises(ValueError, match="vaste NL25 RTCOR-product"):
        knmi_native._validate_filename_identity(FILENAME.lower(), identity)
