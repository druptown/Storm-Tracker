from __future__ import annotations

import base64
import hashlib
import io
import json
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlsplit

import pytest

from storm_tracker_worker import synoptic_native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


SECRET = "0123456789abcdef0123456789abcdef"
RECEIVED_AT = 1_785_662_460.0


def _payload(**overrides):
    value = {
        "latitude": 51.02500,
        "longitude": 4.47700,
        "radius_km": 20.0,
        "query_key": "tracker:home",
        "query_kind": "local",
    }
    value.update(overrides)
    return value


def _job(job_id="synoptic-home-slot-001", payload=None):
    return SimpleNamespace(
        job_id=job_id,
        payload=_payload() if payload is None else payload,
    )


def _json_bytes(*, stations=None, response_code=1, response_message="OK"):
    station_items = [
        {
            "STID": "TEST1",
            "LATITUDE": "51.02",
            "LONGITUDE": "4.48",
            "QC_FLAGGED": {
                "precip_intervals_set_1d": [False],
                "precip_accumulated_set_1d": [False],
            },
            "SENSOR_VARIABLES": {
                "precip_intervals": {
                    "precip_intervals_set_1d": {"position": "main"},
                },
                "precip_accumulated": {
                    "precip_accumulated_set_1d": {"position": "main"},
                },
            },
            "OBSERVATIONS": {
                "date_time": ["2026-08-02T12:00:00Z"],
                "precip_intervals_set_1d": [0.2],
                "precip_accumulated_set_1d": [0.2],
            },
        }
    ] if stations is None else stations
    document = {
        "UNITS": {
            "precip_intervals": "Millimeters",
            "precip_accumulated": "Millimeters",
        },
        "QC_SUMMARY": {
            "QC_TESTS_APPLIED": ["sl_range_check"],
            "TOTAL_OBSERVATIONS_FLAGGED": 0,
        },
        "STATION": station_items,
        "SUMMARY": {
            "NUMBER_OF_OBJECTS": len(station_items),
            "RESPONSE_CODE": response_code,
            "RESPONSE_MESSAGE": response_message,
            "VERSION": "v2.21.3",
        },
    }
    return json.dumps(document, separators=(",", ":")).encode("utf-8")


def _code_two_json():
    return json.dumps({
        "SUMMARY": {
            "NUMBER_OF_OBJECTS": 0,
            "RESPONSE_CODE": 2,
            "RESPONSE_MESSAGE": "No stations found for this request.",
            "VERSION": "v2.21.3",
        }
    }, separators=(",", ":")).encode("utf-8")


def _code_two_message_json(message: str):
    return json.dumps({
        "SUMMARY": {
            "NUMBER_OF_OBJECTS": 0,
            "RESPONSE_CODE": 2,
            "RESPONSE_MESSAGE": message,
            "VERSION": "v2.21.3",
        }
    }, separators=(",", ":")).encode("utf-8")


def _response(payload: bytes, *, received_at=RECEIVED_AT):
    return synoptic_native.SynopticHttpResponse(
        payload=payload,
        content_type="application/json",
        received_at=received_at,
    )


def _install_download(monkeypatch, responses):
    queue = iter(responses)
    calls = []

    def download(query, *, context, credential, deadline=None):
        calls.append((query, context, credential, deadline))
        return next(queue)

    monkeypatch.setattr(synoptic_native, "_download_response", download)
    return calls


def _set_secret(monkeypatch, value=SECRET):
    monkeypatch.setenv(synoptic_native.SYNOPTIC_CREDENTIAL_ENV, value)


def test_exact_response_and_query_context_are_durable_before_json_inspection(
    monkeypatch, tmp_path,
):
    raw = _json_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])
    real_inspector = synoptic_native._inspect_response
    inspections = []

    def inspect(value, **kwargs):
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        objects = list((tmp_path / "raw" / "objects").glob("*.raw"))
        assert len(objects) == 1
        assert objects[0].read_bytes() == raw
        receipt_id = synoptic_native._receipt_id(
            store, "synoptic-home-slot-001"
        )
        receipt = store.load_receipt(receipt_id)
        assert receipt is not None
        context = receipt.provenance["query_context"]
        assert context["query_identity"] == "tracker:home"
        assert context["origin"] == synoptic_native.SYNOPTIC_ORIGIN
        encoded_context = json.dumps(context, sort_keys=True)
        assert SECRET not in encoded_context
        assert "token" not in encoded_context.lower()
        inspections.append(value)
        return real_inspector(value, **kwargs)

    monkeypatch.setattr(synoptic_native, "_inspect_response", inspect)
    result = synoptic_native.capture_synoptic_timeseries(
        _job(), raw_store=store
    )

    assert inspections == [raw]
    source = result["raw_provenance"]["raw_sources"]["response_json"]
    assert store.read_bytes(source["raw_receipt_id"]) == raw
    assert store.stats()["capture_count"] == 1


def test_result_is_explicitly_non_operational_and_contains_no_weather_claims(
    monkeypatch, tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(_json_bytes())])
    result = synoptic_native.capture_synoptic_timeseries(
        _job(), raw_store=store
    )

    assert result["weather_state"] == "unknown"
    assert result["capture_state"] == "captured_not_decoded"
    assert result["decode_status"] == "not_meteorologically_decoded"
    assert result["operational_admission_eligible"] is False
    assert result["query_scope"] == "per_tracker"
    assert result["query_load"] == {
        "network_requests_per_new_job": 1,
        "network_requests_per_exact_job_retry": 0,
        "legacy_minimum_interval_seconds": 300,
        "result_cache_scope": "exact_job_receipt_only",
        "maximum_request_attempts_per_job": 8,
    }
    forbidden = {
        "observations",
        "station_count",
        "fresh_station_count",
        "wet_station_count",
        "dry_station_count",
        "precipitation_present",
        "rain_mm",
        "ground_confirmation",
        "ground_confirmed",
    }
    assert forbidden.isdisjoint(result)
    assert forbidden.isdisjoint(
        result["raw_provenance"]["raw_sources"]["response_json"]
    )
    capture = store.load_capture(result["raw_provenance"]["raw_capture_id"])
    assert capture is not None
    assert capture.product_timestamp is None


def test_credential_comes_only_from_environment_and_is_never_persisted(
    monkeypatch, tmp_path,
):
    raw = _json_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    requests = []

    class Response(io.BytesIO):
        def __init__(self, request_url):
            super().__init__(raw)
            self.status = 200
            self.headers = {
                "Content-Length": str(len(raw)),
                "Content-Type": "application/json; charset=utf-8",
                "Content-Encoding": "identity",
            }
            self._request_url = request_url

        def geturl(self):
            return self._request_url

    def open_request(request, *, timeout):
        requests.append((request, timeout))
        return Response(request.full_url)

    monkeypatch.setattr(
        synoptic_native, "_open_synoptic_request", open_request
    )
    real_credential_scan = synoptic_native._contains_credential_echo
    credential_scan_calls = 0

    def count_credential_scan(*args, **kwargs):
        nonlocal credential_scan_calls
        credential_scan_calls += 1
        return real_credential_scan(*args, **kwargs)

    monkeypatch.setattr(
        synoptic_native,
        "_contains_credential_echo",
        count_credential_scan,
    )
    monkeypatch.setattr(synoptic_native.time, "time", lambda: RECEIVED_AT)
    result = synoptic_native.capture_synoptic_timeseries(
        _job(), raw_store=store
    )

    request, timeout = requests[0]
    parts = urlsplit(request.full_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    assert parts.scheme == "https"
    assert parts.netloc == "api.synopticdata.com"
    assert parts.path == "/v2/stations/timeseries"
    assert "token" not in query
    assert query["radius"] == "51.02500,4.47700,12.43"
    assert query["vars"] == "precip"
    assert query["precip"] == "1"
    assert query["all_reports"] == "1"
    assert query["sensorvars"] == "1"
    assert query["qc"] == "on"
    assert query["qc_remove_data"] == "off"
    assert query["qc_flags"] == "on"
    assert query["qc_checks"] == "synopticlabs,madis,mesowest"
    assert query["recent"] == "60"
    assert query["limit"] == "50"
    assert query["output"] == "json"
    assert request.get_header("Authorization") == f"Bearer {SECRET}"
    assert request.get_header("Accept-encoding") == "identity"
    assert timeout == synoptic_native.HTTP_IO_TIMEOUT_SECONDS
    assert credential_scan_calls == 1

    encoded_result = json.dumps(result, sort_keys=True)
    receipt_id = result["raw_provenance"]["raw_sources"]["response_json"][
        "raw_receipt_id"
    ]
    receipt = store.load_receipt(receipt_id)
    assert receipt is not None
    persisted = json.dumps(receipt.to_metadata(), sort_keys=True)
    assert SECRET not in encoded_result
    assert SECRET not in persisted
    assert "token" not in persisted.lower()
    assert request.full_url not in encoded_result
    assert request.full_url not in persisted


def test_query_is_precipitation_only_and_durable_limit_remains_incomplete():
    query = synoptic_native._validate_query_payload(_payload())
    context = synoptic_native._query_context(query)
    parameters = {
        item["name"]: item["value"] for item in context["parameters"]
    }

    assert parameters["vars"] == "precip"
    assert not {
        "air_temp",
        "relative_humidity",
        "wind_speed",
        "wind_direction",
        "wind_gust",
        "pressure",
    }.intersection(parameters["vars"].split(","))
    assert parameters["limit"] == "50"
    assert context["station_selection"] == {
        "mode": "precipitation_variables_only",
        "provider_derived_precipitation_intervals_requested": True,
        "exact_station_limit": 50,
        "coverage_status": "coverage_incomplete",
    }
    assert context["information_preservation"] == {
        "all_reports": True,
        "sensor_metadata": True,
        "provider_qc_values_removed": False,
        "provider_qc_flags_requested": True,
        "provider_qc_sources": ["synopticlabs", "madis", "mesowest"],
    }


def test_success_remains_provider_derived_qc_preserved_and_non_instantaneous(
    monkeypatch, tmp_path,
):
    raw = _json_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    result = synoptic_native.capture_synoptic_timeseries(
        _job(), raw_store=store
    )

    assert result["semantic_class"] == (
        "provider_derived_precipitation_timeseries_with_qc_metadata"
    )
    assert "raw_in_situ" not in json.dumps(result, sort_keys=True)
    assert result["evidence_origin"] == "provider_derived"
    assert result["provider_qc_status"] == (
        "preserved_structurally_verified_not_interpreted"
    )
    assert result["precipitation_interval_semantics"] == (
        "rolling_or_report_interval_not_instantaneous"
    )
    assert result["station_selection_mode"] == (
        "precipitation_variables_only"
    )
    assert result["coverage_status"] == "coverage_incomplete"
    assert result["area_coverage_evidence"] == "none"
    assert result["provider_response_class"] == "success"
    receipt = store.load_receipt(
        result["raw_provenance"]["raw_sources"]["response_json"][
            "raw_receipt_id"
        ]
    )
    assert receipt is not None
    assert store.read_bytes(receipt.receipt_id) == raw
    assert receipt.provenance["provider_qc_request"] == (
        "requested_and_exact_bytes_preserved"
    )
    assert result["response_structure_status"] == (
        "verified_precip_sensor_qc_structure"
    )
    assert result["precipitation_structure_verified"] is True
    assert result["sensor_metadata_structure_verified"] is True
    assert result["qc_structure_verified"] is True


def test_success_envelope_without_precip_sensor_qc_shape_is_explicitly_unverified(
    monkeypatch, tmp_path,
):
    raw = _json_bytes(stations=[{
        "STID": "PARTIAL",
        "LATITUDE": "51.02",
        "LONGITUDE": "4.48",
        "OBSERVATIONS": {
            "date_time": ["2026-08-02T12:00:00Z"],
            "air_temp_set_1": [20.0],
        },
    }])
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    result = synoptic_native.capture_synoptic_timeseries(
        _job(), raw_store=store
    )

    assert result["provider_response_class"] == (
        "success_structurally_unverified"
    )
    assert result["response_structure_status"] == "structurally_unverified"
    assert result["semantic_class"] == "uninterpreted_synoptic_json_response"
    assert result["evidence_origin"] == (
        "provider_response_not_structurally_verified"
    )
    assert result["provider_qc_status"] == (
        "not_claimed_structure_unverified"
    )
    assert result["precipitation_interval_semantics"] == (
        "not_claimed_structure_unverified"
    )
    assert result["precipitation_structure_verified"] is False
    assert result["sensor_metadata_structure_verified"] is False
    assert result["qc_structure_verified"] is False
    assert result["operational_admission_eligible"] is False


@pytest.mark.parametrize(
    "payload",
    (
        None,
        {},
        {**_payload(), "token": SECRET},
        {**_payload(), "latitude": True},
        {**_payload(), "latitude": 91.0},
        {**_payload(), "longitude": -181.0},
        {**_payload(), "radius_km": 0.5},
        {**_payload(), "radius_km": 51.0},
        {**_payload(), "query_kind": "global"},
        {**_payload(), "query_key": "unsafe key"},
    ),
)
def test_job_payload_is_exact_and_bounded_before_environment_or_network(
    monkeypatch, tmp_path, payload,
):
    monkeypatch.delenv(synoptic_native.SYNOPTIC_CREDENTIAL_ENV, raising=False)
    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *_args, **_kwargs: pytest.fail("netwerk mocht niet starten"),
    )
    with pytest.raises(ValueError):
        synoptic_native.capture_synoptic_timeseries(
            _job(payload=payload), raw_store=RawArtifactStore(tmp_path / "raw")
        )


@pytest.mark.parametrize(
    ("job_id", "query_key"),
    (
        (f"job-{SECRET}-slot", "tracker:home"),
        ("safe-job", f"tracker:{SECRET}:home"),
        ("safe-job", f"tracker:{SECRET.upper()}:home"),
    ),
)
def test_durable_identities_reject_credential_like_32_hex_substrings(
    monkeypatch, tmp_path, job_id, query_key,
):
    monkeypatch.delenv(synoptic_native.SYNOPTIC_CREDENTIAL_ENV, raising=False)
    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *_args, **_kwargs: pytest.fail("netwerk mocht niet starten"),
    )
    store = RawArtifactStore(tmp_path / "raw")
    with pytest.raises(ValueError, match="credentialachtige"):
        synoptic_native.capture_synoptic_timeseries(
            _job(job_id=job_id, payload=_payload(query_key=query_key)),
            raw_store=store,
        )
    store.initialize()
    assert store.stats()["receipt_count"] == 0


@pytest.mark.parametrize(
    ("identity_kind", "value"),
    (
        ("job", "job-%30%31%32%33"),
        ("job", "job-\\u0030"),
        ("query", "tracker:%30%31%32%33"),
        (
            "query",
            "tracker:01-23-45-67-89-ab-cd-ef-01-23-45-67-89-ab-cd-ef",
        ),
        (
            "job",
            "01-23-45-67-89-ab-cd-ef-01-23-45-67-89-ab-cd-ef",
        ),
        ("job", "x" * (synoptic_native.MAX_DURABLE_ID_CHARS + 1)),
    ),
)
def test_durable_identities_are_short_canonical_and_not_reconstructable(
    identity_kind, value,
):
    if identity_kind == "job":
        with pytest.raises(ValueError):
            synoptic_native._validate_job_id(value)
    else:
        with pytest.raises(ValueError):
            synoptic_native._validate_query_payload(
                _payload(query_key=value)
            )


def test_uuid_shaped_32_hex_identity_is_not_exempted_as_a_secret_channel():
    value = "550e8400-e29b-41d4-a716-446655440000"
    with pytest.raises(ValueError, match="credentialachtige"):
        synoptic_native._validate_job_id(value)
    with pytest.raises(ValueError, match="credentialachtige"):
        synoptic_native._validate_query_payload(
            _payload(query_key=f"tracker:{value}")
        )


@pytest.mark.parametrize(
    "credential",
    (
        None,
        "",
        " short ",
        "short",
        "g" * 32,
        "A" * 32,
        "a" * 31,
        "a" * 33,
    ),
)
def test_invalid_environment_credential_fails_before_network(
    monkeypatch, tmp_path, credential,
):
    if credential is None:
        monkeypatch.delenv(synoptic_native.SYNOPTIC_CREDENTIAL_ENV, raising=False)
    else:
        monkeypatch.setenv(synoptic_native.SYNOPTIC_CREDENTIAL_ENV, credential)
    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *_args, **_kwargs: pytest.fail("netwerk mocht niet starten"),
    )
    with pytest.raises(ValueError, match="workercredential"):
        synoptic_native.capture_synoptic_timeseries(
            _job(), raw_store=RawArtifactStore(tmp_path / "raw")
        )


def test_transport_error_never_repeats_url_or_credential(monkeypatch):
    _set_secret(monkeypatch)
    query = synoptic_native._validate_query_payload(_payload())
    context = synoptic_native._query_context(query)

    def explode(_request, *, timeout):
        raise URLError(f"remote leaked {SECRET}")

    monkeypatch.setattr(synoptic_native, "_open_synoptic_request", explode)
    with pytest.raises(synoptic_native.SynopticNativeTransportError) as caught:
        synoptic_native._download_response(
            query, context=context, credential=SECRET
        )
    assert SECRET not in str(caught.value)
    assert synoptic_native.SYNOPTIC_TIMESERIES_URL not in str(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    "echo",
    (
        SECRET,
        SECRET.upper(),
        "".join(
            character.upper() if index % 2 else character
            for index, character in enumerate(SECRET)
        ),
        "".join(f"\\u{ord(character):04x}" for character in SECRET),
        "".join(
            f"\\u{ord(character.upper() if index % 2 else character):04x}"
            for index, character in enumerate(SECRET)
        ),
        "".join(f"%{ord(character):02X}" for character in SECRET),
        "".join(
            character if index % 3 == 0 else f"%{ord(character):02x}"
            for index, character in enumerate(SECRET)
        ),
    ),
)
def test_encoded_credential_echoes_fail_before_raw_store(
    monkeypatch, tmp_path, echo,
):
    raw = _json_bytes().replace(b'"OK"', f'"{echo}"'.encode())
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)

    class Response(io.BytesIO):
        status = 200
        headers = {
            "Content-Length": str(len(raw)),
            "Content-Type": "application/json",
        }

        def geturl(self):
            return self.request_url

    def open_request(request, *, timeout):
        response = Response(raw)
        response.request_url = request.full_url
        return response

    monkeypatch.setattr(synoptic_native, "_open_synoptic_request", open_request)
    with pytest.raises(ValueError) as caught:
        synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    assert SECRET not in str(caught.value)
    store.initialize()
    assert store.stats()["receipt_count"] == 0


def test_five_layer_mixed_case_echo_fails_before_raw_store(
    monkeypatch, tmp_path,
):
    mixed = "".join(
        character.upper() if index % 2 else character
        for index, character in enumerate(SECRET)
    ).encode("ascii")
    echo = b"".join(f"%{value:02X}".encode("ascii") for value in mixed)
    for _ in range(5):
        echo = echo.replace(b"%", b"%25")
    raw = _json_bytes().replace(b'"OK"', b'"' + echo + b'"')
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    with pytest.raises(ValueError, match="credential"):
        synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    store.initialize()
    assert store.stats()["receipt_count"] == 0


def test_escape_chain_beyond_hard_bound_fails_closed_before_raw_store(
    monkeypatch, tmp_path,
):
    echo = b"".join(f"%{value:02X}".encode("ascii") for value in SECRET.encode())
    for _ in range(synoptic_native.MAX_CREDENTIAL_NORMALIZATION_ROUNDS + 2):
        echo = echo.replace(b"%", b"%25")
    assert len(echo) < synoptic_native.MAX_RESPONSE_BYTES
    raw = _json_bytes().replace(b'"OK"', b'"' + echo + b'"')
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    with pytest.raises(ValueError, match="normalisatiegrens"):
        synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    store.initialize()
    assert store.stats()["receipt_count"] == 0


@pytest.mark.parametrize(
    "encoded_secret",
    (
        base64.b64encode(SECRET.encode("ascii")),
        base64.b64encode(SECRET.upper().encode("ascii")),
        base64.urlsafe_b64encode(SECRET.encode("ascii")).rstrip(b"="),
        base64.b32encode(SECRET.encode("ascii")),
        base64.b32encode(SECRET.upper().encode("ascii")).lower(),
        "".join(f"&#{ord(character)};" for character in SECRET).encode(),
        "".join(f"&#x{ord(character):x};" for character in SECRET).encode(),
        "".join(chr(ord(character) + 0xFEE0) for character in SECRET).encode(),
        "".join(
            f"\\u{ord(chr(ord(character) + 0xFEE0)):04x}"
            for character in SECRET
        ).encode(),
        "".join(f"\\U{ord(character):08x}" for character in SECRET).encode(),
        "".join(f"\\x{ord(character):02x}" for character in SECRET).encode(),
        b"01234567-89ab-cdef-0123-456789abcdef",
    ),
)
def test_additional_encoded_credential_echoes_fail_before_raw_store(
    monkeypatch, tmp_path, encoded_secret,
):
    raw = _json_bytes().replace(b'"OK"', b'"' + encoded_secret + b'"')
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    with pytest.raises(ValueError, match="credential"):
        synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    store.initialize()
    assert store.stats()["receipt_count"] == 0


def test_unrelated_uuid_and_base64_are_not_credential_echo_false_positives(
    monkeypatch, tmp_path,
):
    harmless = (
        "550e8400-e29b-41d4-a716-446655440000 "
        + base64.b64encode(b"not-the-synoptic-credential-12345").decode()
    )
    raw = _json_bytes(response_message=harmless)
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    result = synoptic_native.capture_synoptic_timeseries(
        _job(), raw_store=store
    )

    assert result["provider_response_class"] == "success"
    assert store.stats()["receipt_count"] == 1


def test_credential_encoded_candidate_budget_fails_closed(monkeypatch):
    monkeypatch.setattr(
        synoptic_native,
        "MAX_CREDENTIAL_ENCODED_CANDIDATES",
        2,
    )
    adversarial = b" ".join([b"A" * 43] * 3)
    with pytest.raises(ValueError, match="kandidaatgrens"):
        synoptic_native._contains_credential_echo(adversarial, SECRET)


def test_credential_candidate_budget_is_global_across_normalization_rounds(
    monkeypatch,
):
    monkeypatch.setattr(
        synoptic_native,
        "MAX_CREDENTIAL_ENCODED_CANDIDATES",
        1,
    )
    # The same non-secret Base64-shaped candidate survives while the HTML
    # suffix needs another normalization round. It may only be examined once
    # across the complete scan, not once per round.
    adversarial = (b"A" * 43) + b" &amp;amp;"
    with pytest.raises(ValueError, match="kandidaatgrens"):
        synoptic_native._contains_credential_echo(adversarial, SECRET)


@pytest.mark.parametrize(
    ("status", "content_type", "content_encoding", "foreign_url", "message"),
    (
        (302, "application/json", "identity", False, "HTTP 200"),
        (200, "text/html", "identity", False, "Content-Type"),
        (200, "", "identity", False, "Content-Type"),
        (200, "application/json", "gzip", False, "transportcompressie"),
        (200, "application/json", "identity", True, "exacte request-URL"),
    ),
)
def test_http_status_type_encoding_and_exact_geturl_fail_before_body_read(
    monkeypatch,
    status,
    content_type,
    content_encoding,
    foreign_url,
    message,
):
    query = synoptic_native._validate_query_payload(_payload())
    context = synoptic_native._query_context(query)

    class Response(io.BytesIO):
        def __init__(self, request_url):
            super().__init__(b"must-not-be-read")
            self.status = status
            self.headers = {
                "Content-Length": "16",
                "Content-Type": content_type,
                "Content-Encoding": content_encoding,
            }
            self.request_url = request_url

        def geturl(self):
            if foreign_url:
                return "https://evil.invalid/redirected"
            return self.request_url

        def read(self, *_args, **_kwargs):
            pytest.fail("ongeldige Synoptic-body werd gelezen")

    monkeypatch.setattr(
        synoptic_native,
        "_open_synoptic_request",
        lambda request, **_kwargs: Response(request.full_url),
    )
    with pytest.raises(ValueError, match=message):
        synoptic_native._download_response(
            query, context=context, credential=SECRET
        )


@pytest.mark.parametrize("status", (401, 403, 429, 503))
def test_http_auth_rate_and_provider_statuses_are_provider_errors(
    monkeypatch, status,
):
    query = synoptic_native._validate_query_payload(_payload())
    context = synoptic_native._query_context(query)

    class Response(io.BytesIO):
        headers = {"Content-Type": "application/json"}

        def __init__(self, request_url):
            super().__init__(b'{"must":"not be read"}')
            self.status = status
            self.request_url = request_url

        def geturl(self):
            return self.request_url

        def read(self, *_args, **_kwargs):
            pytest.fail("provider-errorbody mocht niet worden gelezen")

    monkeypatch.setattr(
        synoptic_native,
        "_open_synoptic_request",
        lambda request, **_kwargs: Response(request.full_url),
    )
    with pytest.raises(
        synoptic_native.SynopticNativeProviderResponseError
    ) as caught:
        synoptic_native._download_response(
            query, context=context, credential=SECRET
        )
    assert caught.value.response_class == "provider_error"


@pytest.mark.parametrize(
    ("status", "retryable"),
    ((401, False), (403, False), (429, True), (500, True), (503, True)),
)
def test_urllib_http_errors_are_typed_payload_free_and_leave_no_receipt(
    monkeypatch, tmp_path, status, retryable,
):
    class ForbiddenBody(io.BytesIO):
        def read(self, *_args, **_kwargs):
            pytest.fail("HTTPError-body mocht niet gelezen worden")

    def open_request(request, **_kwargs):
        raise HTTPError(
            request.full_url,
            status,
            "remote details must not survive",
            {},
            ForbiddenBody(b'{"credential":"' + SECRET.encode() + b'"}'),
        )

    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    monkeypatch.setattr(
        synoptic_native,
        "_open_synoptic_request",
        open_request,
    )

    with pytest.raises(
        synoptic_native.SynopticNativeProviderResponseError
    ) as caught:
        synoptic_native.capture_synoptic_timeseries(
            _job(), raw_store=store
        )

    assert caught.value.response_class == "provider_error"
    assert caught.value.http_status == status
    assert caught.value.retryable is retryable
    assert SECRET not in str(caught.value)
    assert caught.value.__cause__ is None
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["capture_count"] == 0


def test_absolute_deadline_is_shared_with_open_and_body_read(monkeypatch):
    query = synoptic_native._validate_query_payload(_payload())
    context = synoptic_native._query_context(query)
    ticks = iter((0.0, 0.0, 11.0))
    monkeypatch.setattr(
        synoptic_native.time,
        "monotonic",
        lambda: next(ticks),
    )

    class Response(io.BytesIO):
        status = 200
        headers = {
            "Content-Length": "2",
            "Content-Type": "application/json",
        }

        def __init__(self, request_url):
            super().__init__(b"{}")
            self.request_url = request_url

        def geturl(self):
            return self.request_url

        def read(self, *_args, **_kwargs):
            pytest.fail("body mocht na de absolute deadline niet gelezen worden")

    monkeypatch.setattr(
        synoptic_native,
        "_open_synoptic_request",
        lambda request, **_kwargs: Response(request.full_url),
    )
    with pytest.raises(ValueError, match="absolute deadline"):
        synoptic_native._download_response(
            query,
            context=context,
            credential=SECRET,
            deadline=10.0,
        )


def test_streaming_body_has_a_preparse_chunk_budget(monkeypatch):
    monkeypatch.setattr(synoptic_native, "MAX_HTTP_READ_CHUNKS", 2)

    class EndlessSingleByteResponse:
        def read(self, _size):
            return b"x"

    with pytest.raises(ValueError, match="chunkgrens"):
        synoptic_native._read_response_body(
            EndlessSingleByteResponse(),
            declared_bytes=None,
            deadline=synoptic_native.time.monotonic() + 30.0,
        )


def test_stream_rejects_a_read_that_ignores_requested_size(monkeypatch):
    monkeypatch.setattr(synoptic_native, "MAX_RESPONSE_BYTES", 32)

    class OversizedReadResponse:
        def read(self, _size):
            return b"x" * 33

    with pytest.raises(ValueError, match="aangevraagde bytegrens"):
        synoptic_native._read_response_body(
            OversizedReadResponse(),
            declared_bytes=None,
            deadline=synoptic_native.time.monotonic() + 30.0,
        )


def test_oversized_declared_response_is_rejected_before_body_read(monkeypatch):
    query = synoptic_native._validate_query_payload(_payload())
    context = synoptic_native._query_context(query)

    class Response(io.BytesIO):
        status = 200
        headers = {
            "Content-Length": str(synoptic_native.MAX_RESPONSE_BYTES + 1),
            "Content-Type": "application/json",
        }

        def geturl(self):
            return self.request_url

        def read(self, *_args, **_kwargs):
            pytest.fail("oversized Synoptic-body werd gelezen")

    def open_request(request, **_kwargs):
        response = Response()
        response.request_url = request.full_url
        return response

    monkeypatch.setattr(synoptic_native, "_open_synoptic_request", open_request)
    with pytest.raises(ValueError, match="bytegrens"):
        synoptic_native._download_response(
            query, context=context, credential=SECRET
        )


def test_redirect_handler_never_creates_a_followup_request():
    assert synoptic_native._RejectRedirects().redirect_request(
        None,
        None,
        302,
        "Found",
        {},
        synoptic_native.SYNOPTIC_TIMESERIES_URL,
    ) is None


def test_network_opener_disables_environment_proxies(monkeypatch):
    handlers = []

    class Opener:
        def open(self, request, *, timeout):
            return (request, timeout)

    def build(*items):
        handlers.extend(items)
        return Opener()

    monkeypatch.setattr(synoptic_native, "build_opener", build)
    request = object()
    assert synoptic_native._open_synoptic_request(request, timeout=7) == (
        request,
        7,
    )
    proxy = next(
        item for item in handlers if isinstance(item, synoptic_native.ProxyHandler)
    )
    assert proxy.proxies == {}
    assert any(
        isinstance(item, synoptic_native._RejectRedirects) for item in handlers
    )


@pytest.mark.parametrize(
    ("raw", "message"),
    (
        (
            b'{"SUMMARY":{"NUMBER_OF_OBJECTS":0,"RESPONSE_CODE":1,'
            b'"RESPONSE_CODE":2,"RESPONSE_MESSAGE":"OK"},"STATION":[]}',
            "dubbele",
        ),
        (
            b'{"SUMMARY":{"NUMBER_OF_OBJECTS":0,"RESPONSE_CODE":1,'
            b'"RESPONSE_MESSAGE":"OK","X":NaN},"STATION":[]}',
            "niet-eindig",
        ),
        (b"\xff", "UTF-8 JSON"),
        (b"[]", "JSON-object"),
    ),
)
def test_ambiguous_nonfinite_or_invalid_json_leaves_exact_receipt_only(
    monkeypatch, tmp_path, raw, message,
):
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])
    with pytest.raises(ValueError, match=message):
        synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    receipt = store.load_receipt(
        synoptic_native._receipt_id(store, "synoptic-home-slot-001")
    )
    assert receipt is not None
    assert store.read_bytes(receipt.receipt_id) == raw


def test_structural_json_node_budget_is_enforced_after_raw_store(
    monkeypatch, tmp_path,
):
    raw = _json_bytes(stations=[{}, {}])
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])
    monkeypatch.setattr(synoptic_native, "MAX_JSON_NODES", 4)
    with pytest.raises(ValueError, match="structuurlimiet"):
        synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_code_two_ambiguity_is_captured_but_never_promoted_to_dry(
    monkeypatch, tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(_code_two_json())])
    result = synoptic_native.capture_synoptic_timeseries(
        _job(), raw_store=store
    )
    assert result["weather_state"] == "unknown"
    assert result["operational_admission_eligible"] is False
    assert result["provider_response_class"] == (
        "ambiguous_no_data_or_access"
    )
    assert result["coverage_status"] == "coverage_incomplete"
    assert result["area_coverage_evidence"] == "none"
    assert "zero_results" not in json.dumps(result, sort_keys=True)
    assert "station_count" not in result
    assert "dry" not in result


def test_code_two_message_cannot_be_promoted_to_zero_results_or_coverage(
    monkeypatch, tmp_path,
):
    raw = _code_two_message_json(
        "No stations found for this request, or you do not have access "
        "to the requested data."
    )
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    result = synoptic_native.capture_synoptic_timeseries(
        _job(), raw_store=store
    )

    assert result["provider_response_class"] == (
        "ambiguous_no_data_or_access"
    )
    assert result["area_coverage_evidence"] == "none"
    assert result["weather_state"] == "unknown"
    assert "zero_results" not in json.dumps(result, sort_keys=True)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1


def test_current_official_code_two_shape_without_object_count_is_accepted_unknown(
    monkeypatch, tmp_path,
):
    raw = json.dumps({
        "SUMMARY": {
            "RESPONSE_CODE": 2,
            "RESPONSE_MESSAGE": (
                "No stations found for this request, or your account does "
                "not have access to the requested station(s). Please contact "
                "support@synopticdata.com."
            ),
            "VERSION": None,
        }
    }, separators=(",", ":")).encode()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    result = synoptic_native.capture_synoptic_timeseries(
        _job(), raw_store=store
    )

    assert result["provider_response_class"] == (
        "ambiguous_no_data_or_access"
    )
    assert result["response_structure_status"] == (
        "verified_official_code_2_ambiguity"
    )
    assert result["weather_state"] == "unknown"
    assert result["semantic_class"] == "uninterpreted_synoptic_json_response"


@pytest.mark.parametrize(
    "message",
    (
        "HTTP 401 Unauthorized",
        "429 Too Many Requests; rate limit exceeded",
        "Invalid token",
        "Provider unavailable",
    ),
)
def test_code_two_provider_errors_remain_receipt_only(
    monkeypatch, tmp_path, message,
):
    raw = _code_two_message_json(message)
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    with pytest.raises(
        synoptic_native.SynopticNativeProviderResponseError
    ) as caught:
        synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    assert caught.value.response_class == "provider_error"
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    receipt = store.load_receipt(
        synoptic_native._receipt_id(store, "synoptic-home-slot-001")
    )
    assert receipt is not None
    assert store.read_bytes(receipt.receipt_id) == raw


def test_code_two_http_status_shape_is_provider_error_without_object_count(
    monkeypatch, tmp_path,
):
    raw = json.dumps({
        "SUMMARY": {
            "RESPONSE_CODE": 2,
            "RESPONSE_MESSAGE": (
                "Too many requests sent. Please wait for other requests "
                "to finish and resend."
            ),
            "VERSION": None,
            "HTTP_STATUS_CODE": 429,
        }
    }, separators=(",", ":")).encode()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])

    with pytest.raises(
        synoptic_native.SynopticNativeProviderResponseError
    ) as caught:
        synoptic_native.capture_synoptic_timeseries(
            _job(), raw_store=store
        )

    assert caught.value.http_status == 429
    assert caught.value.retryable is True
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_transient_provider_receipt_does_not_poison_same_job_retry(
    monkeypatch, tmp_path,
):
    synoptic_native._clear_runtime_caches_for_tests()
    transient = json.dumps({
        "SUMMARY": {
            "RESPONSE_CODE": 2,
            "RESPONSE_MESSAGE": "Too many requests sent.",
            "VERSION": None,
            "HTTP_STATUS_CODE": 429,
        }
    }, separators=(",", ":")).encode()
    success = _json_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    real_inspector = synoptic_native._inspect_response
    inspected_sha256 = []

    def count_inspection(payload, **kwargs):
        inspected_sha256.append(hashlib.sha256(payload).hexdigest())
        return real_inspector(payload, **kwargs)

    monkeypatch.setattr(
        synoptic_native,
        "_inspect_response",
        count_inspection,
    )
    calls = _install_download(
        monkeypatch,
        [_response(transient), _response(success, received_at=RECEIVED_AT + 5)],
    )

    with pytest.raises(synoptic_native.SynopticNativeProviderResponseError):
        synoptic_native.capture_synoptic_timeseries(
            _job("synoptic-provider-retry"), raw_store=store
        )
    assert len(calls) == 1
    first_receipt = store.load_receipt(
        synoptic_native._receipt_id(store, "synoptic-provider-retry", 1)
    )
    assert first_receipt is not None
    assert store.read_bytes(first_receipt.receipt_id) == transient

    result = synoptic_native.capture_synoptic_timeseries(
        _job("synoptic-provider-retry"), raw_store=store
    )

    assert len(calls) == 2
    assert inspected_sha256 == [
        hashlib.sha256(transient).hexdigest(),
        hashlib.sha256(success).hexdigest(),
    ]
    assert result["provider_response_class"] == "success"
    source = result["raw_provenance"]["raw_sources"]["response_json"]
    assert source["request_attempt"] == 2
    second_receipt = store.load_receipt(
        synoptic_native._receipt_id(store, "synoptic-provider-retry", 2)
    )
    assert second_receipt is not None
    assert store.read_bytes(second_receipt.receipt_id) == success
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1


def test_deterministic_provider_rejection_is_terminal_without_reparse_or_network(
    monkeypatch, tmp_path,
):
    synoptic_native._clear_runtime_caches_for_tests()
    raw = json.dumps({
        "SUMMARY": {
            "RESPONSE_CODE": 2,
            "RESPONSE_MESSAGE": (
                "Invalid token. Be sure to use a token generated from your "
                "API Key, and not the key itself."
            ),
            "VERSION": None,
            "HTTP_STATUS_CODE": 401,
        }
    }, separators=(",", ":")).encode()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    calls = _install_download(monkeypatch, [_response(raw)])
    real_inspector = synoptic_native._inspect_response
    inspection_count = 0

    def count_inspection(payload, **kwargs):
        nonlocal inspection_count
        inspection_count += 1
        return real_inspector(payload, **kwargs)

    monkeypatch.setattr(
        synoptic_native,
        "_inspect_response",
        count_inspection,
    )
    job = _job("synoptic-terminal-provider")

    with pytest.raises(
        synoptic_native.SynopticNativeProviderResponseError
    ) as first:
        synoptic_native.capture_synoptic_timeseries(job, raw_store=store)
    assert first.value.retryable is False
    assert len(calls) == 1
    assert inspection_count == 1

    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *_args, **_kwargs: pytest.fail("terminal job mocht geen netwerk doen"),
    )
    monkeypatch.setattr(
        synoptic_native,
        "_inspect_response",
        lambda *_args, **_kwargs: pytest.fail("terminal job mocht niet herparsen"),
    )
    monkeypatch.setattr(
        store,
        "read_receipt_bytes",
        lambda *_args, **_kwargs: pytest.fail("terminal job mocht raw niet herlezen"),
    )

    with pytest.raises(
        synoptic_native.SynopticNativeProviderResponseError
    ) as retry:
        synoptic_native.capture_synoptic_timeseries(job, raw_store=store)
    assert retry.value.retryable is False
    assert len(calls) == 1
    assert inspection_count == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_api_error_response_is_durable_but_not_a_complete_capture(
    monkeypatch, tmp_path,
):
    raw = json.dumps({
        "SUMMARY": {
            "NUMBER_OF_OBJECTS": 0,
            "RESPONSE_CODE": 400,
            "RESPONSE_MESSAGE": "Violates a rule of the API",
        }
    }).encode()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])
    with pytest.raises(ValueError, match="geen succesvolle"):
        synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_deterministic_structure_rejection_is_terminal_without_reparse_or_network(
    monkeypatch, tmp_path,
):
    synoptic_native._clear_runtime_caches_for_tests()
    raw = b'{"SUMMARY":{"RESPONSE_CODE":1,"RESPONSE_CODE":2}}'
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    calls = _install_download(monkeypatch, [_response(raw)])
    real_inspector = synoptic_native._inspect_response
    inspection_count = 0

    def count_inspection(payload, **kwargs):
        nonlocal inspection_count
        inspection_count += 1
        return real_inspector(payload, **kwargs)

    monkeypatch.setattr(
        synoptic_native,
        "_inspect_response",
        count_inspection,
    )
    job = _job("synoptic-terminal-structure")
    with pytest.raises(ValueError, match="dubbele"):
        synoptic_native.capture_synoptic_timeseries(job, raw_store=store)
    assert len(calls) == 1
    assert inspection_count == 1

    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *_args, **_kwargs: pytest.fail("terminal job mocht geen netwerk doen"),
    )
    monkeypatch.setattr(
        synoptic_native,
        "_inspect_response",
        lambda *_args, **_kwargs: pytest.fail("terminal job mocht niet herparsen"),
    )
    monkeypatch.setattr(
        store,
        "read_receipt_bytes",
        lambda *_args, **_kwargs: pytest.fail("terminal job mocht raw niet herlezen"),
    )
    with pytest.raises(ValueError, match="reeds compleet"):
        synoptic_native.capture_synoptic_timeseries(job, raw_store=store)
    assert len(calls) == 1
    assert inspection_count == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


@pytest.mark.parametrize(
    "retryable_error",
    (
        synoptic_native.SynopticNativeTransportError(
            "Synoptic netwerkcapture is mislukt zonder remote details"
        ),
        synoptic_native.SynopticNativeDeadlineError(
            "Synoptic job overschrijdt de absolute deadline"
        ),
    ),
)
def test_timeout_and_io_failure_remain_retryable_without_poisoning_job(
    monkeypatch, tmp_path, retryable_error,
):
    synoptic_native._clear_runtime_caches_for_tests()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    calls = []
    responses = iter((retryable_error, _response(_json_bytes())))

    def download(query, *, context, credential, deadline=None):
        calls.append((query, context, credential, deadline))
        value = next(responses)
        if isinstance(value, BaseException):
            raise value
        return value

    monkeypatch.setattr(synoptic_native, "_download_response", download)
    job = _job("synoptic-retryable-transport")
    with pytest.raises(type(retryable_error)):
        synoptic_native.capture_synoptic_timeseries(job, raw_store=store)
    assert store.stats()["receipt_count"] == 0

    result = synoptic_native.capture_synoptic_timeseries(job, raw_store=store)

    assert result["provider_response_class"] == "success"
    assert len(calls) == 2
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1


def test_receipt_only_retry_resumes_without_network_or_environment_secret(
    monkeypatch, tmp_path,
):
    raw = _json_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    calls = _install_download(monkeypatch, [_response(raw)])
    real_inspector = synoptic_native._inspect_response

    def crash_after_receipt(_payload, **_kwargs):
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        raise RuntimeError("gesimuleerde inspectiecrash")

    monkeypatch.setattr(synoptic_native, "_inspect_response", crash_after_receipt)
    with pytest.raises(RuntimeError, match="inspectiecrash"):
        synoptic_native.capture_synoptic_timeseries(
            _job("synoptic-resume-slot"), raw_store=store
        )
    assert len(calls) == 1

    monkeypatch.setattr(synoptic_native, "_inspect_response", real_inspector)
    monkeypatch.delenv(synoptic_native.SYNOPTIC_CREDENTIAL_ENV)
    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *_args, **_kwargs: pytest.fail("resume mocht geen netwerk gebruiken"),
    )
    result = synoptic_native.capture_synoptic_timeseries(
        _job("synoptic-resume-slot"), raw_store=store
    )
    assert result["capture_state"] == "captured_not_decoded"
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1


def test_complete_capture_retry_is_identical_and_network_free(
    monkeypatch, tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    calls = _install_download(monkeypatch, [_response(_json_bytes())])
    first = synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    assert len(calls) == 1
    monkeypatch.delenv(synoptic_native.SYNOPTIC_CREDENTIAL_ENV)
    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *_args, **_kwargs: pytest.fail("retry mocht geen netwerk gebruiken"),
    )
    retry = synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    assert retry == first


def test_complete_capture_retry_uses_one_bundle_read_and_no_raw_reparse(
    monkeypatch, tmp_path,
):
    synoptic_native._clear_runtime_caches_for_tests()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(_json_bytes())])
    real_inspector = synoptic_native._inspect_response
    inspection_count = 0

    def count_inspection(payload, **kwargs):
        nonlocal inspection_count
        inspection_count += 1
        return real_inspector(payload, **kwargs)

    monkeypatch.setattr(
        synoptic_native,
        "_inspect_response",
        count_inspection,
    )
    first = synoptic_native.capture_synoptic_timeseries(
        _job("synoptic-complete-cache"), raw_store=store
    )
    assert inspection_count == 1

    real_bundle = store.load_capture_bundle_bytes
    bundle_count = 0

    def count_bundle(capture_id):
        nonlocal bundle_count
        bundle_count += 1
        return real_bundle(capture_id)

    monkeypatch.setattr(store, "load_capture_bundle_bytes", count_bundle)
    monkeypatch.setattr(
        store,
        "read_receipt_bytes",
        lambda *_args, **_kwargs: pytest.fail(
            "complete retry mocht geen tweede rawread doen"
        ),
    )
    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *_args, **_kwargs: pytest.fail(
            "complete retry mocht geen netwerk doen"
        ),
    )

    retry = synoptic_native.capture_synoptic_timeseries(
        _job("synoptic-complete-cache"), raw_store=store
    )

    assert retry == first
    assert bundle_count == 1
    assert inspection_count == 1


def test_same_job_id_with_changed_query_fails_closed_without_network(
    monkeypatch, tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(_json_bytes())])
    synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    monkeypatch.setattr(
        synoptic_native,
        "_download_response",
        lambda *_args, **_kwargs: pytest.fail("conflict mocht geen netwerk gebruiken"),
    )
    changed = _job(payload=_payload(latitude=50.0))
    with pytest.raises(RuntimeError, match="verkeerde identiteit"):
        synoptic_native.capture_synoptic_timeseries(changed, raw_store=store)


def test_new_job_for_same_tracker_is_visible_new_query_and_deduplicates_bytes(
    monkeypatch, tmp_path,
):
    raw = _json_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    calls = _install_download(monkeypatch, [
        _response(raw, received_at=RECEIVED_AT),
        _response(raw, received_at=RECEIVED_AT + 300),
    ])
    first = synoptic_native.capture_synoptic_timeseries(
        _job("synoptic-slot-001"), raw_store=store
    )
    second = synoptic_native.capture_synoptic_timeseries(
        _job("synoptic-slot-002"), raw_store=store
    )
    assert len(calls) == 2
    assert first["query_scope"] == second["query_scope"] == "per_tracker"
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 2


def test_result_receipt_and_context_are_bounded_payload_free_and_secret_free(
    monkeypatch, tmp_path,
):
    raw = _json_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _set_secret(monkeypatch)
    _install_download(monkeypatch, [_response(raw)])
    result = synoptic_native.capture_synoptic_timeseries(_job(), raw_store=store)
    receipt_id = result["raw_provenance"]["raw_sources"]["response_json"][
        "raw_receipt_id"
    ]
    receipt = store.load_receipt(receipt_id)
    assert receipt is not None
    encoded = json.dumps(result, sort_keys=True).encode()
    persisted = json.dumps(receipt.to_metadata(), sort_keys=True).encode()
    assert len(encoded) < 32 * 1024
    assert len(persisted) < 32 * 1024
    assert raw not in encoded
    assert b"payload" not in encoded.lower()
    # The fixed public origin is part of the exact durable query context; the
    # credential-bearing request URL itself must never be persisted.
    assert synoptic_native.SYNOPTIC_ORIGIN.encode() in persisted
    assert b"?" not in persisted
    assert SECRET.encode() not in encoded
    assert SECRET.encode() not in persisted
