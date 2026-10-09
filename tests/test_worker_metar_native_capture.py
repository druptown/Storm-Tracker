from __future__ import annotations

from datetime import datetime, timezone
import csv
import gzip
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from storm_tracker_worker import metar_native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


OBSERVED_AT = "2026-08-02T12:00:00Z"
RECEIVED_AT = datetime(2026, 8, 2, 12, 1, tzinfo=timezone.utc).timestamp()
REAL_CACHE = Path(__file__).parents[1] / "metars_current.csv.gz"


def _csv_gzip(rows=None, *, fieldnames=None) -> bytes:
    names = list(fieldnames or metar_native.METAR_EXPECTED_HEADER)
    values = rows or [{
        "raw_text": "METAR EBAW 021200Z -RA",
        "station_id": "EBAW",
        "observation_time": OBSERVED_AT,
        "latitude": "51.1894",
        "longitude": "4.4603",
        "wx_string": "-RA",
        "metar_type": "METAR",
    }]
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=names)
    writer.writeheader()
    writer.writerows(values)
    return gzip.compress(stream.getvalue().encode("utf-8"), mtime=0)


def _job(job_id="metar-global-slot-001", payload=None):
    return SimpleNamespace(job_id=job_id, payload={} if payload is None else payload)


def _response(payload: bytes, received_at=RECEIVED_AT):
    return metar_native._MetarHttpResponse(
        payload=payload,
        content_type="application/gzip",
        received_at=received_at,
    )


def _install(monkeypatch, responses):
    queue = iter(responses)
    calls = []

    def download():
        calls.append(True)
        return next(queue)

    monkeypatch.setattr(metar_native, "_download_cache", download)
    return calls


def test_exact_gzip_is_durable_before_csv_or_gzip_inspection(monkeypatch, tmp_path):
    payload = _csv_gzip()
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(payload)])
    real_inspector = metar_native._inspect_cache
    inspections = []

    def inspect(value, *, received_at):
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        objects = list((tmp_path / "raw" / "objects").glob("*.raw"))
        assert len(objects) == 1
        assert objects[0].read_bytes() == payload
        inspections.append((value, received_at))
        return real_inspector(value, received_at=received_at)

    monkeypatch.setattr(metar_native, "_inspect_cache", inspect)
    result = metar_native.capture_latest_metar_cache(_job(), raw_store=store)

    assert inspections == [(payload, RECEIVED_AT)]
    assert store.stats()["capture_count"] == 1
    source = result["raw_provenance"]["raw_sources"]["csv_gzip"]
    assert store.read_bytes(source["raw_receipt_id"]) == payload
    assert source["received_at"] == RECEIVED_AT
    assert source["declared_newest_observation_timestamp"] == datetime(
        2026, 8, 2, 12, 0, tzinfo=timezone.utc
    ).timestamp()


def test_global_cache_does_not_invent_one_product_time_or_local_dry_fact(
    monkeypatch, tmp_path,
):
    payload = _csv_gzip()
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(payload)])
    result = metar_native.capture_latest_metar_cache(_job(), raw_store=store)
    capture = store.load_capture(result["raw_provenance"]["raw_capture_id"])

    assert capture is not None
    assert capture.product_timestamp is None
    assert result["raw_provenance"]["product_timestamp"] is None
    assert result["weather_state"] == "unknown"
    assert result["capture_state"] == "captured_not_decoded"
    assert "observation_count" not in result
    assert result["operational_admission_eligible"] is False
    assert "observations" not in result
    assert "wet_station_count" not in result
    assert "dry" not in result
    assert "rain" not in result


def test_invalid_gzip_leaves_exact_receipt_only(monkeypatch, tmp_path):
    payload = b"not-gzip"
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(payload)])

    with pytest.raises(ValueError, match="gecomprimeerde payload"):
        metar_native.capture_latest_metar_cache(_job(), raw_store=store)

    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    objects = list((tmp_path / "raw" / "objects").glob("*.raw"))
    assert objects[0].read_bytes() == payload


@pytest.mark.parametrize(
    "fieldnames",
    (
        list(metar_native.METAR_EXPECTED_HEADER[:-1]),
        [
            "station_id" if index == 2 else name
            for index, name in enumerate(metar_native.METAR_EXPECTED_HEADER)
        ],
        [*metar_native.METAR_EXPECTED_HEADER, "sky_cover"],
    ),
)
def test_unknown_or_ambiguous_csv_schema_leaves_receipt_only(
    monkeypatch, tmp_path, fieldnames,
):
    rows = [{name: "x" for name in fieldnames}]
    payload = _csv_gzip(rows, fieldnames=fieldnames)
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(payload)])

    with pytest.raises(ValueError, match="CSV-schema"):
        metar_native.capture_latest_metar_cache(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_valid_schema_without_valid_observations_is_not_empty_dry_capture(
    monkeypatch, tmp_path,
):
    payload = _csv_gzip([{
        "station_id": "EBAW",
        "observation_time": "not-a-time",
        "latitude": "51.1",
        "longitude": "4.4",
        "wx_string": "",
        "raw_text": "",
        "metar_type": "METAR",
    }])
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(payload)])

    with pytest.raises(ValueError, match="geen structureel geldige"):
        metar_native.capture_latest_metar_cache(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_same_job_retry_is_exact_and_next_slot_reuses_physical_object(
    monkeypatch, tmp_path,
):
    payload = _csv_gzip()
    store = RawArtifactStore(tmp_path / "raw")
    calls = _install(monkeypatch, [
        _response(payload, RECEIVED_AT),
        _response(payload, RECEIVED_AT + 60.0),
    ])

    first = metar_native.capture_latest_metar_cache(
        _job("metar-slot-001"), raw_store=store
    )
    retry = metar_native.capture_latest_metar_cache(
        _job("metar-slot-001"), raw_store=store
    )
    assert len(calls) == 1
    next_slot = metar_native.capture_latest_metar_cache(
        _job("metar-slot-002"), raw_store=store
    )

    assert retry["raw_provenance"] == first["raw_provenance"]
    assert next_slot["raw_provenance"]["raw_capture_id"] != (
        first["raw_provenance"]["raw_capture_id"]
    )
    first_source = first["raw_provenance"]["raw_sources"]["csv_gzip"]
    next_source = next_slot["raw_provenance"]["raw_sources"]["csv_gzip"]
    assert next_source["raw_receipt_id"] != first_source["raw_receipt_id"]
    assert next_source["raw_content_sha256"] == first_source["raw_content_sha256"]
    assert first_source["received_at"] == RECEIVED_AT
    assert next_source["received_at"] == RECEIVED_AT + 60.0
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 2
    assert len(calls) == 2


@pytest.mark.parametrize("payload", (None, [], {"latitude": 51.0}, {"token": "x"}))
def test_capture_requires_one_global_empty_payload_before_network(
    monkeypatch, tmp_path, payload,
):
    monkeypatch.setattr(
        metar_native,
        "_download_cache",
        lambda: pytest.fail("download had niet mogen starten"),
    )
    job = SimpleNamespace(job_id="metar-slot", payload=payload)

    with pytest.raises(ValueError, match="exact lege jobpayload"):
        metar_native.capture_latest_metar_cache(
            job, raw_store=RawArtifactStore(tmp_path / "raw")
        )


def test_http_download_uses_identity_encoding_and_exact_length(monkeypatch):
    payload = _csv_gzip()
    requests = []

    class Response(io.BytesIO):
        def __init__(self):
            super().__init__(payload)
            self.status = 200
            self.headers = {
                "Content-Encoding": "identity",
                "Content-Length": str(len(payload)),
                "Content-Type": "application/octet-stream",
            }

        def geturl(self):
            return metar_native.METAR_CACHE_URL

    def open_request(request, *, timeout):
        requests.append((request, timeout))
        return Response()

    monkeypatch.setattr(metar_native, "_open_cache_request", open_request)
    response = metar_native._download_cache()

    assert response.payload == payload
    assert response.content_type == "application/octet-stream"
    assert requests[0][0].full_url == metar_native.METAR_CACHE_URL
    assert requests[0][0].get_header("Accept-encoding") == "identity"
    assert requests[0][0].get_header("Authorization") is None
    assert requests[0][1] == metar_native.HTTP_IO_TIMEOUT_SECONDS


def test_oversized_http_response_is_rejected_before_body_read(monkeypatch):
    class OversizeResponse(io.BytesIO):
        def __init__(self):
            super().__init__(b"must-not-be-read")
            self.status = 200
            self.headers = {
                "Content-Encoding": "identity",
                "Content-Length": str(metar_native.MAX_COMPRESSED_BYTES + 1),
                "Content-Type": "application/octet-stream",
            }

        def geturl(self):
            return metar_native.METAR_CACHE_URL

        def read(self, *_args, **_kwargs):
            pytest.fail("oversized METAR-body werd gelezen")

    monkeypatch.setattr(
        metar_native,
        "_open_cache_request",
        lambda *_args, **_kwargs: OversizeResponse(),
    )
    with pytest.raises(ValueError, match="contractlimiet"):
        metar_native._download_cache()


def test_csv_row_budget_is_enforced(monkeypatch):
    rows = [
        {
            "station_id": f"K{index}",
            "observation_time": OBSERVED_AT,
            "latitude": "40",
            "longitude": "-74",
            "wx_string": "",
            "raw_text": "METAR",
        }
        for index in range(3)
    ]
    monkeypatch.setattr(metar_native, "MAX_CSV_ROWS", 2)
    with pytest.raises(ValueError, match="rijenlimiet"):
        metar_native._inspect_cache(
            _csv_gzip(rows),
            received_at=RECEIVED_AT,
        )


def test_concatenated_gzip_members_are_rejected():
    payload = _csv_gzip() + _csv_gzip()
    with pytest.raises(ValueError, match="extra leden|trailing"):
        metar_native._inspect_cache(payload, received_at=RECEIVED_AT)


def test_official_header_has_only_the_four_expected_cloud_pairs():
    header = metar_native.METAR_EXPECTED_HEADER
    assert len(header) == 44
    assert header.count("sky_cover") == 4
    assert header.count("cloud_base_ft_agl") == 4
    assert len(set(header) | {"sky_cover", "cloud_base_ft_agl"}) == 38


@pytest.mark.skipif(not REAL_CACHE.is_file(), reason="lokale echte AWC-cache ontbreekt")
def test_real_awc_cache_golden_is_accepted_end_to_end(monkeypatch, tmp_path):
    payload = REAL_CACHE.read_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    received_at = REAL_CACHE.stat().st_mtime
    _install(monkeypatch, [_response(payload, received_at=received_at)])

    result = metar_native.capture_latest_metar_cache(
        _job("metar-real-awc-golden"),
        raw_store=store,
    )

    source = result["raw_provenance"]["raw_sources"]["csv_gzip"]
    assert store.read_bytes(source["raw_receipt_id"]) == payload
    assert result["csv_row_count"] > 1_000
    assert result["structurally_valid_row_count"] > 1_000
    assert result["semantic_class"] == "raw_terminal_observation_reports"
    assert result["timestamp_quality_state"] == "unverified_non_operational"
    assert result["operational_admission_eligible"] is False


@pytest.mark.parametrize(
    ("status", "final_url", "content_type", "message"),
    (
        (302, metar_native.METAR_CACHE_URL, "application/octet-stream", "HTTP 200"),
        (
            200,
            "http://127.0.0.1:8123/api/",
            "application/octet-stream",
            "exacte bron-URL",
        ),
        (200, metar_native.METAR_CACHE_URL, "text/html", "Content-Type"),
        (200, metar_native.METAR_CACHE_URL, "", "Content-Type"),
    ),
)
def test_http_status_origin_and_content_type_fail_before_body_read(
    monkeypatch,
    status,
    final_url,
    content_type,
    message,
):
    class Response(io.BytesIO):
        def __init__(self):
            super().__init__(b"must-not-be-read")
            self.status = status
            self.headers = {
                "Content-Encoding": "identity",
                "Content-Length": "16",
                "Content-Type": content_type,
            }

        def geturl(self):
            return final_url

        def read(self, *_args, **_kwargs):
            pytest.fail("ongeldige METAR-responsebody werd gelezen")

    monkeypatch.setattr(
        metar_native,
        "_open_cache_request",
        lambda *_args, **_kwargs: Response(),
    )
    with pytest.raises(ValueError, match=message):
        metar_native._download_cache()


def test_redirect_handler_never_creates_a_followup_request():
    assert metar_native._RejectRedirects().redirect_request(
        None,
        None,
        302,
        "Found",
        {},
        metar_native.METAR_CACHE_URL,
    ) is None


def test_receipt_only_crash_resumes_without_network(monkeypatch, tmp_path):
    payload = _csv_gzip()
    store = RawArtifactStore(tmp_path / "raw")
    calls = _install(monkeypatch, [_response(payload)])
    real_inspector = metar_native._inspect_cache

    def crash_after_receipt(_payload, *, received_at):
        assert received_at == RECEIVED_AT
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        raise RuntimeError("gesimuleerde decodercrash")

    monkeypatch.setattr(metar_native, "_inspect_cache", crash_after_receipt)
    with pytest.raises(RuntimeError, match="decodercrash"):
        metar_native.capture_latest_metar_cache(
            _job("metar-resume-slot"),
            raw_store=store,
        )
    assert len(calls) == 1

    monkeypatch.setattr(metar_native, "_inspect_cache", real_inspector)
    monkeypatch.setattr(
        metar_native,
        "_download_cache",
        lambda: pytest.fail("resume mocht netwerk niet opnieuw gebruiken"),
    )
    result = metar_native.capture_latest_metar_cache(
        _job("metar-resume-slot"),
        raw_store=store,
    )

    assert result["capture_state"] == "captured_not_decoded"
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1


def test_timestamp_quality_is_diagnostic_and_never_weather_state(
    monkeypatch,
    tmp_path,
):
    rows = [
        {
            "raw_text": "METAR EBAW 021200Z -RA",
            "station_id": "EBAW",
            "observation_time": "2026-08-02T12:00:00Z",
            "latitude": "51.1",
            "longitude": "4.4",
            "metar_type": "METAR",
        },
        {
            "raw_text": "METAR KJFK 020800Z",
            "station_id": "KJFK",
            "observation_time": "2026-08-02T08:00:00Z",
            "latitude": "40.6",
            "longitude": "-73.7",
            "metar_type": "METAR",
        },
        {
            "raw_text": "SPECI EGLL 021215Z +RA",
            "station_id": "EGLL",
            "observation_time": "2026-08-02T12:15:00Z",
            "latitude": "51.4",
            "longitude": "-0.4",
            "metar_type": "SPECI",
        },
        {
            "raw_text": "METAR LFPG 021159Z",
            "station_id": "LFPG",
            "observation_time": "geen-tijd",
            "latitude": "49.0",
            "longitude": "2.5",
            "metar_type": "METAR",
        },
    ]
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(_csv_gzip(rows))])
    result = metar_native.capture_latest_metar_cache(_job(), raw_store=store)

    assert result["current_timestamp_row_count"] == 1
    assert result["stale_timestamp_row_count"] == 1
    assert result["future_timestamp_row_count"] == 1
    assert result["invalid_timestamp_row_count"] == 1
    assert result["weather_state"] == "unknown"
    assert result["timestamp_quality_state"] == "unverified_non_operational"
    assert "observations" not in result
    assert "ground_confirmation" not in result


def test_streaming_inspection_rejects_expansion_bomb():
    payload = gzip.compress(b"A" * (2 * 1024 * 1024), mtime=0)
    with pytest.raises(ValueError, match="uitgepakte payload|fysieke CSV-regel"):
        metar_native._inspect_cache(payload, received_at=RECEIVED_AT)


def test_streaming_inspection_has_a_total_deadline(monkeypatch):
    moments = iter((100.0, 111.0))
    monkeypatch.setattr(metar_native.time, "monotonic", lambda: next(moments))
    with pytest.raises(TimeoutError, match="inspectie"):
        metar_native._inspect_cache(_csv_gzip(), received_at=RECEIVED_AT)


def test_result_and_persisted_provenance_are_small_and_url_free(
    monkeypatch, tmp_path,
):
    payload = _csv_gzip()
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(payload)])
    result = metar_native.capture_latest_metar_cache(_job(), raw_store=store)
    receipt_id = result["raw_provenance"]["raw_sources"]["csv_gzip"][
        "raw_receipt_id"
    ]
    receipt = store.load_receipt(receipt_id)
    assert receipt is not None

    encoded = json.dumps(result, sort_keys=True).encode()
    persisted = json.dumps(receipt.to_metadata(), sort_keys=True).encode()
    assert len(encoded) < 32 * 1024
    assert payload not in encoded
    assert b"https://" not in encoded
    assert b"https://" not in persisted
    assert b"payload" not in encoded.lower()


def test_complete_retry_reopens_store_reads_once_and_uses_success_verdict(
    monkeypatch, tmp_path,
):
    payload = _csv_gzip()
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(payload)])
    first = metar_native.capture_latest_metar_cache(_job(), raw_store=store)

    reopened = RawArtifactStore(store.root)
    reopened.initialize()
    real_read = reopened._read_verified  # noqa: SLF001 - counted authority read
    reads = []

    def counted(*args, **kwargs):
        reads.append(args[0])
        return real_read(*args, **kwargs)

    monkeypatch.setattr(reopened, "_read_verified", counted)
    monkeypatch.setattr(
        metar_native,
        "_inspect_cache",
        lambda *_args, **_kwargs: pytest.fail("successverdict moest CSV-reparse voorkomen"),
    )
    monkeypatch.setattr(
        metar_native,
        "_download_cache",
        lambda: pytest.fail("complete retry mocht niet downloaden"),
    )
    retry = metar_native.capture_latest_metar_cache(_job(), raw_store=reopened)

    assert retry == first
    assert len(reads) == 1


def test_deterministic_metar_rejection_survives_store_reopen(
    monkeypatch, tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(b"not-gzip")])
    job = _job("metar-durable-rejection")
    with pytest.raises(ValueError, match="gecomprimeerde payload"):
        metar_native.capture_latest_metar_cache(job, raw_store=store)

    monkeypatch.setattr(
        metar_native,
        "_inspect_cache",
        lambda *_args, **_kwargs: pytest.fail("duurzame afkeur mocht niet herinspecteren"),
    )
    monkeypatch.setattr(
        metar_native,
        "_download_cache",
        lambda: pytest.fail("receipt-retry mocht niet downloaden"),
    )
    with pytest.raises(ValueError, match="gecomprimeerde payload"):
        metar_native.capture_latest_metar_cache(
            job,
            raw_store=RawArtifactStore(store.root),
        )


def test_metar_inspection_timeout_is_never_persisted_as_rejection(
    monkeypatch, tmp_path,
):
    payload = _csv_gzip()
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch, [_response(payload)])
    job = _job("metar-transient-timeout")
    real_inspector = metar_native._inspect_cache
    monkeypatch.setattr(
        metar_native,
        "_inspect_cache",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("synthetic timeout")),
    )
    with pytest.raises(TimeoutError, match="synthetic"):
        metar_native.capture_latest_metar_cache(job, raw_store=store)
    verdicts = store.root / "metar_validation_verdicts"
    assert list(verdicts.glob("*.json")) == []

    monkeypatch.setattr(metar_native, "_inspect_cache", real_inspector)
    monkeypatch.setattr(
        metar_native,
        "_download_cache",
        lambda: pytest.fail("receipt-retry mocht niet downloaden"),
    )
    result = metar_native.capture_latest_metar_cache(
        job,
        raw_store=RawArtifactStore(store.root),
    )
    assert result["capture_state"] == "captured_not_decoded"
