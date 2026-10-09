from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlsplit

import pytest

from storm_tracker_worker import kmi_opendata_raw as raw
from storm_tracker_worker.raw_artifacts import RawArtifactStore


PAYLOAD = b'{"raw":"KMI bytes stay exact","quality":"not-decided-here"}'
RECEIVED_AT = 1_786_665_000.25


def _job(job_id: str = "kmi-aws-raw-job", payload: dict | None = None):
    return SimpleNamespace(
        job_id=job_id,
        payload={} if payload is None else payload,
    )


class _Response:
    def __init__(
        self,
        payload: bytes,
        url: str,
        *,
        status: int = 200,
        content_type: str | None = "application/json;charset=UTF-8",
        content_length: int | None | object = ...,
        encoding: str | None = None,
        oversized_chunk: bool = False,
    ) -> None:
        self.payload = payload
        self.url = url
        self.status = status
        self.offset = 0
        self.oversized_chunk = oversized_chunk
        self.read_sizes: list[int] = []
        self.headers: dict[str, str] = {
            "ETag": '"kmi-etag"',
            "Last-Modified": "Fri, 14 Aug 2026 00:00:00 GMT",
        }
        if content_type is not None:
            self.headers["Content-Type"] = content_type
        if content_length is ...:
            self.headers["Content-Length"] = str(len(payload))
        elif content_length is not None:
            self.headers["Content-Length"] = str(content_length)
        if encoding is not None:
            self.headers["Content-Encoding"] = encoding

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def geturl(self) -> str:
        return self.url

    def read(self, size: int) -> bytes:
        self.read_sizes.append(size)
        if self.oversized_chunk and self.offset == 0:
            self.offset = len(self.payload)
            return self.payload + b"x"
        part = self.payload[self.offset : self.offset + size]
        self.offset += len(part)
        return part


def _opener(
    payload: bytes = PAYLOAD,
    *,
    content_type: str | None = "application/json;charset=UTF-8",
    content_length: int | None | object = ...,
    status: int = 200,
    final_url: str | None = None,
    encoding: str | None = None,
):
    calls: list[dict[str, object]] = []

    def open_request(request, *, timeout):
        calls.append(
            {
                "url": request.full_url,
                "headers": dict(request.header_items()),
                "timeout": timeout,
            }
        )
        return _Response(
            payload,
            request.full_url if final_url is None else final_url,
            status=status,
            content_type=content_type,
            content_length=content_length,
            encoding=encoding,
        )

    return calls, open_request


def _capture(
    tmp_path: Path,
    *,
    payload: bytes = PAYLOAD,
    content_type: str | None = "application/json;charset=UTF-8",
    job_id: str = "kmi-aws-raw-job",
    descriptor=raw.KMI_AWS_10MIN_LATEST,
):
    store = RawArtifactStore(tmp_path / "raw")
    calls, opener = _opener(payload, content_type=content_type)
    result = raw.capture_kmi_opendata(
        _job(job_id),
        descriptor=descriptor,
        raw_store=store,
        _opener=opener,
    )
    return store, calls, result


def test_aws_descriptor_builds_one_canonical_allowlisted_request():
    request = raw.canonical_request(raw.KMI_AWS_10MIN_LATEST)
    parts = urlsplit(request.url)
    assert (parts.scheme, parts.hostname, parts.path) == (
        "https",
        raw.KMI_OPENDATA_HOST,
        raw.KMI_OPENDATA_PATH,
    )
    assert dict(parse_qsl(parts.query)) == {
        "COUNT": "512",
        "OUTPUTFORMAT": "application/json",
        "REQUEST": "GetFeature",
        "SERVICE": "WFS",
        "SORTBY": "timestamp D",
        "TYPENAMES": raw.AWS_10MIN_LAYER,
        "VERSION": "2.0.0",
    }
    assert request.method == "GET"
    assert request.to_mapping()["headers"]["Accept-Encoding"] == "identity"
    assert request.request_sha256 == hashlib.sha256(
        json.dumps(
            request.to_mapping(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


@pytest.mark.parametrize(
    "descriptor",
    [
        replace(raw.KMI_AWS_10MIN_LATEST, endpoint="http://opendata.meteo.be/geoserver/ows"),
        replace(raw.KMI_AWS_10MIN_LATEST, endpoint="https://evil.invalid/geoserver/ows"),
        replace(raw.KMI_AWS_10MIN_LATEST, endpoint="https://user@opendata.meteo.be/geoserver/ows"),
        replace(raw.KMI_AWS_10MIN_LATEST, endpoint="https://opendata.meteo.be:443/geoserver/ows"),
        replace(raw.KMI_AWS_10MIN_LATEST, endpoint="https://opendata.meteo.be:bad/geoserver/ows"),
        replace(raw.KMI_AWS_10MIN_LATEST, endpoint="https://opendata.meteo.be/geoserver/wfs"),
        replace(raw.KMI_AWS_10MIN_LATEST, endpoint="https://opendata.meteo.be/geoserver/ows?q=x"),
        replace(raw.KMI_AWS_10MIN_LATEST, service="WMS"),
        replace(raw.KMI_AWS_10MIN_LATEST, layer="aws:not_allowlisted"),
        replace(raw.KMI_AWS_10MIN_LATEST, parameters=(("SERVICE", "WCS"),)),
        replace(raw.KMI_AWS_10MIN_LATEST, parameters=(("COUNT", "50001"),)),
        replace(
            raw.KMI_AWS_10MIN_LATEST,
            parameters=(("CQL_FILTER", "INCLUDE"), ("CQL_FILTER", "EXCLUDE")),
        ),
    ],
)
def test_endpoint_service_layer_and_query_allowlist_fail_closed(descriptor):
    with pytest.raises(raw.KmiOpenDataContractError):
        raw.canonical_request(descriptor)


def test_exact_response_bytes_and_canonical_transport_receipt_are_durable(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(raw.time, "time", lambda: RECEIVED_AT)
    store, calls, result = _capture(tmp_path)
    assert len(calls) == 1
    assert calls[0]["url"] == raw.canonical_request(
        raw.KMI_AWS_10MIN_LATEST
    ).url
    assert calls[0]["timeout"] <= raw.HTTP_IO_TIMEOUT_SECONDS
    assert result["capture_state"] == "captured_exact_raw"
    assert result["weather_state"] == "unknown"
    assert result["operational_admission_eligible"] is False
    assert result["meteorological_semantics"] == "unknown_unverified"
    assert result["operational_admission"] == "not_evaluated"
    assert result["semantic_or_admission_status_blocked_storage"] is False

    capture_id = result["raw_provenance"]["raw_capture_id"]
    capture, receipts, payloads = store.load_capture_bundle_bytes(capture_id)
    assert capture.complete is True
    assert payloads == {"geoserver_response": PAYLOAD}
    assert result["raw_provenance"]["sources"] == {
        "geoserver_response": result["raw_provenance"]["source"]
    }
    receipt = receipts["geoserver_response"]
    raw.validate_kmi_opendata_receipt_for_recovery(
        receipt,
        job_id="kmi-aws-raw-job",
    )
    raw.validate_kmi_opendata_capture_for_recovery(
        capture,
        receipt,
        job_id="kmi-aws-raw-job",
    )
    assert receipt.received_at == RECEIVED_AT
    assert receipt.content_type == "application/json"
    provenance = receipt.provenance
    assert provenance["response_body_parsed_before_store"] is False
    assert provenance["semantic_or_admission_status_blocks_storage"] is False
    assert provenance["credential_transport"] == "none"
    request = raw.canonical_request(raw.KMI_AWS_10MIN_LATEST)
    assert provenance["canonical_request"] == request.to_mapping()
    assert provenance["canonical_request_sha256"] == request.request_sha256
    transport = provenance["transport_receipt"]
    assert transport == result["raw_provenance"]["transport_receipt"]
    assert provenance["transport_receipt_sha256"] == hashlib.sha256(
        json.dumps(
            transport,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    assert result["raw_provenance"]["transport_receipt_sha256"] == (
        provenance["transport_receipt_sha256"]
    )
    assert transport["body_sha256"] == hashlib.sha256(PAYLOAD).hexdigest()
    assert transport["body_size_bytes"] == len(PAYLOAD)
    assert transport["expected_content_type_match"] is True


@pytest.mark.parametrize(
    ("content_type", "payload"),
    [
        ("text/html", b"<html>unverified upstream body</html>"),
        ("application/xml", b"<ExceptionReport>unknown semantics</ExceptionReport>"),
        (None, b"no content type but exact bytes"),
        ("not a media type", b"malformed header but exact bytes"),
    ],
)
def test_unexpected_semantics_or_content_type_never_blocks_raw_storage(
    monkeypatch, tmp_path, content_type, payload
):
    monkeypatch.setattr(raw.time, "time", lambda: RECEIVED_AT)
    store, _calls, result = _capture(
        tmp_path,
        payload=payload,
        content_type=content_type,
        job_id="kmi-unverified-" + hashlib.sha256(payload).hexdigest()[:12],
    )
    assert result["storage_complete"] is True
    assert result["meteorological_semantics"] == "unknown_unverified"
    transport = result["raw_provenance"]["transport_receipt"]
    assert transport["expected_content_type_match"] is False
    loaded = store.load_capture_bundle_bytes(
        result["raw_provenance"]["raw_capture_id"]
    )
    assert loaded is not None
    assert loaded[2]["geoserver_response"] == payload
    assert loaded[1]["geoserver_response"].content_type == (
        "application/octet-stream"
    )


def test_response_is_durable_before_capture_commit_and_resumes_offline(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(raw.time, "time", lambda: RECEIVED_AT)
    store = RawArtifactStore(tmp_path / "raw")
    calls, opener = _opener()

    class StopAfterStore(RuntimeError):
        pass

    def fault(name: str) -> None:
        if name == "kmi_opendata.after_response_store":
            assert store.stats()["receipt_count"] == 1
            assert store.stats()["capture_count"] == 0
            raise StopAfterStore(name)

    with pytest.raises(StopAfterStore):
        raw.capture_kmi_aws_10min(
            _job("kmi-resume-job"),
            raw_store=store,
            _opener=opener,
            _fault_injector=fault,
        )
    assert len(calls) == 1

    def forbidden(*_args, **_kwargs):
        pytest.fail("durable response retry must be network-free")

    result = raw.capture_kmi_aws_10min(
        _job("kmi-resume-job"),
        raw_store=RawArtifactStore(store.root),
        _opener=forbidden,
    )
    assert result["storage_complete"] is True
    assert store.stats()["capture_count"] == 1
    retry = raw.capture_kmi_aws_10min(
        _job("kmi-resume-job"),
        raw_store=RawArtifactStore(store.root),
        _opener=forbidden,
    )
    assert retry == result


def test_stream_and_declared_lengths_are_hard_bounded(tmp_path):
    descriptor = replace(raw.KMI_AWS_10MIN_LATEST, maximum_bytes=8)
    store = RawArtifactStore(tmp_path / "raw")
    calls, opener = _opener(b"123456789", content_length=None)
    with pytest.raises(raw.KmiOpenDataCapacityError):
        raw.capture_kmi_opendata(
            _job("kmi-stream-overflow"),
            descriptor=descriptor,
            raw_store=store,
            _opener=opener,
        )
    assert len(calls) == 1
    assert store.stats()["receipt_count"] == 0

    declared_store = RawArtifactStore(tmp_path / "declared")
    declared_calls, declared_opener = _opener(
        b"x", content_length=9
    )
    with pytest.raises(raw.KmiOpenDataCapacityError):
        raw.capture_kmi_opendata(
            _job("kmi-declared-overflow"),
            descriptor=descriptor,
            raw_store=declared_store,
            _opener=declared_opener,
        )
    assert len(declared_calls) == 1
    assert declared_store.stats()["receipt_count"] == 0


@pytest.mark.parametrize(
    "opener_kwargs",
    [
        {"status": 503},
        {"final_url": "https://opendata.meteo.be/geoserver/ows?SERVICE=WMS"},
        {"encoding": "gzip"},
        {"content_length": len(PAYLOAD) + 1},
    ],
)
def test_transport_drift_is_not_mislabelled_as_raw_data(tmp_path, opener_kwargs):
    store = RawArtifactStore(tmp_path / "raw")
    calls, opener = _opener(**opener_kwargs)
    with pytest.raises(raw.KmiOpenDataContractError):
        raw.capture_kmi_aws_10min(
            _job("kmi-transport-drift"),
            raw_store=store,
            _opener=opener,
        )
    assert len(calls) == 1
    assert store.stats()["receipt_count"] == 0


def test_aws_window_is_half_open_unlimited_and_maximum_one_day():
    start = datetime(2026, 8, 14, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=20)
    descriptor = raw.aws_10min_window_descriptor(start, end)
    query = dict(parse_qsl(urlsplit(raw.canonical_request(descriptor).url).query))
    assert "COUNT" not in query
    assert query["CQL_FILTER"] == (
        "timestamp >= '2026-08-14T00:00:00Z' "
        "AND timestamp < '2026-08-14T00:20:00Z'"
    )
    assert query["TYPENAMES"] == raw.AWS_10MIN_LAYER

    with pytest.raises(raw.KmiOpenDataContractError, match="24 uur"):
        raw.aws_10min_window_descriptor(
            start, start + timedelta(days=1, seconds=1)
        )
    with pytest.raises(raw.KmiOpenDataContractError):
        raw.aws_10min_window_descriptor(end, start)
    with pytest.raises(raw.KmiOpenDataContractError):
        raw.aws_10min_window_descriptor(
            start.replace(microsecond=1), end
        )


def test_same_descriptor_model_supports_allowlisted_wcs_request():
    nominal = datetime(2026, 8, 14, 0, 0, tzinfo=timezone.utc)
    descriptor = raw.public_radar_time_descriptor(nominal)
    request = raw.canonical_request(descriptor)
    query = dict(parse_qsl(urlsplit(request.url).query))
    assert query == {
        "COVERAGEID": raw.PUBLIC_RADAR_COVERAGE,
        "FORMAT": "image/tiff",
        "REQUEST": "GetCoverage",
        "SERVICE": "WCS",
        "SUBSET": 'time("2026-08-14T00:00:00Z")',
        "VERSION": "2.0.1",
    }


def test_public_radar_latest_uses_provider_default_time():
    request = raw.canonical_request(raw.KMI_PUBLIC_RADAR_LATEST)
    query = dict(parse_qsl(urlsplit(request.url).query))
    assert query == {
        "COVERAGEID": raw.PUBLIC_RADAR_COVERAGE,
        "FORMAT": "image/tiff",
        "REQUEST": "GetCoverage",
        "SERVICE": "WCS",
        "VERSION": "2.0.1",
    }
    assert "SUBSET" not in query


def test_job_payload_cannot_carry_filters_or_secrets(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()

    def forbidden(*_args, **_kwargs):
        pytest.fail("invalid job must fail before network")

    for payload in (
        {"token": "secret"},
        {"url": "https://evil.invalid"},
        {"CQL_FILTER": "INCLUDE"},
    ):
        with pytest.raises(raw.KmiOpenDataContractError, match="credential"):
            raw.capture_kmi_aws_10min(
                _job("kmi-invalid-payload", payload),
                raw_store=store,
                _opener=forbidden,
            )
    assert store.stats()["receipt_count"] == 0


def test_expired_deadline_fails_before_network(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")

    def forbidden(*_args, **_kwargs):
        pytest.fail("expired deadline must fail before network")

    with pytest.raises(raw.KmiOpenDataDeadlineError):
        raw.capture_kmi_aws_10min(
            _job("kmi-expired"),
            raw_store=store,
            deadline_at=0.0,
            _opener=forbidden,
        )


def test_module_has_no_home_assistant_mqtt_or_secret_dependency():
    source = Path(raw.__file__).read_text(encoding="utf-8").lower()
    assert "import homeassistant" not in source
    assert "import paho" not in source
    assert "mqtt." not in source
    assert "getenv(" not in source
    assert "authorization" not in source
