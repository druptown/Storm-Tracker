from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from storm_tracker_worker import public_source_raw_capture as native
from storm_tracker_worker import openmeteo_native as existing_openmeteo
from storm_tracker_worker.raw_artifacts import RawArtifactStore


@dataclass
class _Job:
    job_id: str
    payload: dict


class _Response:
    def __init__(
        self,
        payload: bytes,
        *,
        status: int = 200,
        content_type: str | None = "application/json",
        final_url: str | None = None,
        content_length: str | None = None,
    ) -> None:
        self._payload = payload
        self._offset = 0
        self.status = status
        self._final_url = final_url
        self.closed = False
        self.headers: dict[str, str] = {}
        if content_length is None:
            self.headers["Content-Length"] = str(len(payload))
        elif content_length:
            self.headers["Content-Length"] = content_length
        if content_type is not None:
            self.headers["Content-Type"] = content_type

    def read(self, size: int) -> bytes:
        result = self._payload[self._offset:self._offset + size]
        self._offset += len(result)
        return result

    def geturl(self) -> str:
        assert self._final_url is not None
        return self._final_url

    def close(self) -> None:
        self.closed = True


def _safe_id(prefix: str, seed: str) -> str:
    token = base64.b32encode(hashlib.sha256(seed.encode("ascii")).digest())
    encoded = token.decode("ascii").lower().rstrip("=")
    chunks = [encoded[offset:offset + 8] for offset in range(0, 52, 8)]
    return prefix + ":" + ":z:".join(chunks)


JOB_ID = _safe_id("acq", "public-source-job")
TARGET_KEY = _safe_id("aqt", "public-source-target")
REGION_KEY = _safe_id("aqr", "public-source-region")


def _region(latitude: float, longitude: float) -> dict:
    return {
        "contract": native.LOCATION_QUERY_CONTRACT,
        "latitude": latitude,
        "longitude": longitude,
        "query_key": REGION_KEY,
    }


def _openmeteo_batch(latitude: float = 50.85, longitude: float = 4.35) -> dict:
    return {
        "contract": "stv3-openmeteo-target-query-batch-v1",
        "operational_handler_compatible": False,
        "queries": [{
            "latitude": latitude,
            "longitude": longitude,
            "query_key": TARGET_KEY,
            "query_kind": "local",
        }],
        "target_resolution_required": True,
    }


def test_openmeteo_batch_repeats_elevation_for_every_coordinate() -> None:
    payload = _openmeteo_batch()
    payload["queries"].append({
        "latitude": 48.85,
        "longitude": 2.35,
        "query_key": _safe_id("aqt", "public-source-target-two"),
        "query_kind": "roaming",
    })

    parameters, query_count = native._parameters(
        native.HANDLER_SPECS_BY_WORKLOAD[native.OPENMETEO_WORKLOAD_ID],
        payload,
    )

    assert query_count == 2
    assert parameters["latitude"] == "50.85000,48.85000"
    assert parameters["longitude"] == "4.35000,2.35000"
    assert parameters["elevation"] == "nan,nan"


def _store(tmp_path: Path) -> RawArtifactStore:
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    return store


def _opener(payload: bytes, calls: list[dict], **response_kwargs):
    def open_response(request, *, timeout):
        calls.append({
            "headers": {
                key.lower(): value for key, value in request.header_items()
            },
            "timeout": timeout,
            "url": request.full_url,
        })
        return _Response(
            payload,
            final_url=request.full_url,
            **response_kwargs,
        )

    return open_response


CASES = (
    (
        native.OPENMETEO_WORKLOAD_ID,
        _openmeteo_batch(),
        native.OPENMETEO_HOST,
        native.OPENMETEO_PATH,
        {
            "cell_selection": ["nearest"],
            "current": ["precipitation"],
            "elevation": ["nan"],
            "forecast_hours": ["6"],
            "forecast_minutely_15": ["13"],
            "latitude": ["50.85000"],
            "longitude": ["4.35000"],
            "precipitation_unit": ["mm"],
            "timeformat": ["unixtime"],
            "timezone": ["UTC"],
            "wind_speed_unit": ["kmh"],
        },
    ),
    (
        native.OPENMETEO_ICON2I_WORKLOAD_ID,
        _region(41.90, 12.50),
        native.OPENMETEO_HOST,
        native.OPENMETEO_PATH,
        {
            "forecast_hours": ["6"],
            "hourly": ["precipitation,lightning_potential"],
            "latitude": ["41.90000"],
            "longitude": ["12.50000"],
            "models": ["italia_meteo_arpae_icon_2i"],
            "timezone": ["UTC"],
        },
    ),
    (
        native.GEOSPHERE_WORKLOAD_ID,
        _region(48.21, 16.37),
        native.GEOSPHERE_HOST,
        native.GEOSPHERE_PATH,
        {
            "lat_lon": ["48.21000,16.37000"],
            "output_format": ["geojson"],
            "parameters": ["rr"],
        },
    ),
    (
        native.ITALIAMETEO_CATALOG_WORKLOAD_ID,
        {},
        native.ITALIAMETEO_HOST,
        native.ITALIAMETEO_PATH,
        {},
    ),
    (
        native.METEOLUX_WORKLOAD_ID,
        _region(49.63, 6.21),
        native.METEOLUX_HOST,
        native.METEOLUX_PATH,
        {
            "langcode": ["en"],
            "lat": ["49.63000"],
            "long": ["6.21000"],
        },
    ),
)


@pytest.mark.parametrize(
    ("workload_id", "payload", "host", "path", "expected_query"),
    CASES,
)
def test_each_public_source_stores_success_bytes_unparsed_and_unverified(
    tmp_path: Path,
    workload_id: str,
    payload: dict,
    host: str,
    path: str,
    expected_query: dict[str, list[str]],
):
    # Deliberately not valid JSON: successful representation drift is raw
    # evidence, not a reason to lose the upstream bytes.
    upstream = b"<html>provider representation drift</html>"
    calls: list[dict] = []
    store = _store(tmp_path)
    result = native.capture_public_source(
        workload_id,
        _Job(JOB_ID, payload),
        raw_store=store,
        opener=_opener(upstream, calls, content_type="text/html; charset=utf-8"),
    )

    assert result["capture_state"] == "captured_not_decoded"
    assert result["decode_status"] == "not_decoded"
    assert result["semantic_status"] == "unverified"
    assert result["weather_state"] == "unknown"
    assert result["parse_attempted"] is False
    assert result["meteorological_claim"] is False
    assert result["operational_admission_eligible"] is False
    assert result["provider_rights_claim"] is False
    assert len(calls) == 1
    parsed = urlsplit(calls[0]["url"])
    assert parsed.scheme == "https"
    assert parsed.hostname == host
    assert parsed.path == path
    actual_query = parse_qs(parsed.query, keep_blank_values=True)
    for key, values in expected_query.items():
        assert actual_query[key] == values
    assert set(actual_query) >= set(expected_query)
    assert 0 < calls[0]["timeout"] <= native.HTTP_IO_TIMEOUT_SECONDS
    assert calls[0]["headers"]["accept-encoding"] == "identity"
    assert "authorization" not in calls[0]["headers"]
    assert "cookie" not in calls[0]["headers"]

    source = result["raw_provenance"]["sources"]["response_bytes"]
    assert store.read_bytes(source["receipt_id"]) == upstream
    receipt = store.load_receipt(source["receipt_id"])
    assert receipt is not None
    assert receipt.content_type == native.UNINTERPRETED_CONTENT_TYPE
    assert receipt.provenance["reported_media_type"] == "text/html"
    assert receipt.provenance["semantic_status"] == "unverified"
    assert receipt.provenance["parse_attempted"] is False
    assert receipt.provenance["provider_rights_claim"] is False
    assert receipt.provenance["access_class"] == "public_anonymous_no_secret"
    assert len(receipt.provenance["request_sha256"]) == 64
    assert len(receipt.provenance["transport_sha256"]) == 64
    assert len(receipt.provenance["response_headers_sha256"]) == 64
    persisted_metadata = json.dumps(
        {"provenance": receipt.provenance, "result": result},
        sort_keys=True,
    )
    assert "50.85000" not in persisted_metadata
    assert "48.21000" not in persisted_metadata
    assert "41.90000" not in persisted_metadata
    assert "49.63000" not in persisted_metadata
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1
    assert store.integrity_check()["healthy"] is True


def test_openmeteo_request_reuses_the_existing_full_variable_contract(tmp_path: Path):
    calls: list[dict] = []
    native.capture_openmeteo_region(
        _Job(JOB_ID, _openmeteo_batch()),
        raw_store=_store(tmp_path),
        opener=_opener(b"not-json", calls),
    )
    query = parse_qs(urlsplit(calls[0]["url"]).query)
    assert query["minutely_15"] == [
        ",".join(existing_openmeteo.MINUTELY_15)
    ]
    assert query["hourly"] == [",".join(existing_openmeteo.HOURLY)]
    assert native.OPENMETEO_MINUTELY_15 == existing_openmeteo.MINUTELY_15
    assert native.OPENMETEO_HOURLY == existing_openmeteo.HOURLY
    assert native.OPENMETEO_QUERY_BATCH_CONTRACT == (
        existing_openmeteo.QUERY_BATCH_CONTRACT
    )


def test_empty_success_body_is_still_an_exact_capture(tmp_path: Path):
    store = _store(tmp_path)
    result = native.capture_italiameteo_catalog(
        _Job(JOB_ID, {}),
        raw_store=store,
        opener=_opener(b"", [], status=204, content_type=None),
    )
    source = result["raw_provenance"]["sources"]["response_bytes"]
    assert source["size_bytes"] == 0
    assert store.read_bytes(source["receipt_id"]) == b""
    receipt = store.load_receipt(source["receipt_id"])
    assert receipt is not None
    assert receipt.provenance["response_status"] == 204
    assert receipt.provenance["reported_media_type"] == "unverified"


def test_partial_store_recovers_without_network_or_parsing(tmp_path: Path):
    store = _store(tmp_path)

    def fail(point: str) -> None:
        if point == "public_source.after_response_store":
            raise RuntimeError("simulated power loss")

    with pytest.raises(RuntimeError, match="power loss"):
        native.capture_geosphere_location(
            _Job(JOB_ID, _region(48.21, 16.37)),
            raw_store=store,
            opener=_opener(b"broken-json", []),
            _fault_injector=fail,
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0

    def no_network(*_args, **_kwargs):
        pytest.fail("duurzame raw receipt mag niet opnieuw worden opgehaald")

    recovered = native.capture_geosphere_location(
        _Job(JOB_ID, _region(48.21, 16.37)),
        raw_store=RawArtifactStore(store.root),
        opener=no_network,
    )
    assert recovered["semantic_status"] == "unverified"
    assert store.stats()["capture_count"] == 1


def test_complete_retry_is_byte_and_network_idempotent(tmp_path: Path):
    store = _store(tmp_path)
    job = _Job(JOB_ID, _region(49.63, 6.21))
    first = native.capture_meteolux_location(
        job, raw_store=store, opener=_opener(b"{malformed", [])
    )

    def no_network(*_args, **_kwargs):
        pytest.fail("complete capture mag niet refetchen")

    second = native.capture_meteolux_location(
        job, raw_store=RawArtifactStore(store.root), opener=no_network
    )
    assert second == first


def test_same_job_cannot_be_rebound_to_another_location(tmp_path: Path):
    store = _store(tmp_path)
    native.capture_meteolux_location(
        _Job(JOB_ID, _region(49.63, 6.21)),
        raw_store=store,
        opener=_opener(b"one", []),
    )
    with pytest.raises(native.PublicSourceRawCorruptionError):
        native.capture_meteolux_location(
            _Job(JOB_ID, _region(49.70, 6.20)),
            raw_store=RawArtifactStore(store.root),
            opener=lambda *_args, **_kwargs: pytest.fail("geen refetch verwacht"),
        )


@pytest.mark.parametrize(
    ("workload_id", "payload"),
    [
        (native.GEOSPHERE_WORKLOAD_ID, _region(50.0, 16.0)),
        (native.METEOLUX_WORKLOAD_ID, _region(49.63, 8.0)),
        (native.OPENMETEO_ICON2I_WORKLOAD_ID, _region(50.0, 12.0)),
        (native.ITALIAMETEO_CATALOG_WORKLOAD_ID, {"unexpected": True}),
        (
            native.OPENMETEO_WORKLOAD_ID,
            {**_openmeteo_batch(), "operational_handler_compatible": True},
        ),
    ],
)
def test_query_payloads_and_provider_coverage_are_fail_closed_before_network(
    tmp_path: Path, workload_id: str, payload: dict
):
    with pytest.raises(native.PublicSourceRawContractError):
        native.capture_public_source(
            workload_id,
            _Job(JOB_ID, payload),
            raw_store=_store(tmp_path),
            opener=lambda *_args, **_kwargs: pytest.fail("geen netwerk verwacht"),
        )


def test_redirect_query_drift_and_non_https_final_url_are_rejected(tmp_path: Path):
    payload = _region(48.21, 16.37)
    for suffix in ("&extra=1", None):
        store = _store(tmp_path / ("query" if suffix else "host"))

        def redirected(request, *, timeout):
            final = (
                request.full_url + suffix
                if suffix is not None
                else "http://dataset.api.hub.geosphere.at" + native.GEOSPHERE_PATH
            )
            return _Response(b"raw", final_url=final)

        with pytest.raises(native.PublicSourceRawContractError):
            native.capture_geosphere_location(
                _Job(JOB_ID, payload), raw_store=store, opener=redirected
            )
        assert store.stats()["receipt_count"] == 0


def test_response_transport_limits_fail_before_storage(tmp_path: Path):
    store = _store(tmp_path / "declared")

    def oversized_declared(request, *, timeout):
        return _Response(
            b"x",
            final_url=request.full_url,
            content_length=str(4 * 1024 * 1024 + 1),
        )

    with pytest.raises(native.PublicSourceRawCapacityError):
        native.capture_geosphere_location(
            _Job(JOB_ID, _region(48.21, 16.37)),
            raw_store=store,
            opener=oversized_declared,
        )
    assert store.stats()["receipt_count"] == 0

    greedy_store = _store(tmp_path / "greedy")

    def greedy(request, *, timeout):
        response = _Response(b"", final_url=request.full_url, content_length="")
        response.read = lambda requested: b"x" * (requested + 1)
        return response

    with pytest.raises(native.PublicSourceRawCapacityError):
        native.capture_geosphere_location(
            _Job(JOB_ID, _region(48.21, 16.37)),
            raw_store=greedy_store,
            opener=greedy,
        )
    assert greedy_store.stats()["receipt_count"] == 0


def test_transport_exception_cannot_expose_location(tmp_path: Path):
    def leaking(request, *, timeout):
        raise RuntimeError("failure at " + request.full_url)

    with pytest.raises(native.PublicSourceRawTransportError) as captured:
        native.capture_geosphere_location(
            _Job(JOB_ID, _region(48.21, 16.37)),
            raw_store=_store(tmp_path),
            opener=leaking,
        )
    assert "48.21000" not in str(captured.value)
    assert "16.37000" not in str(captured.value)
    assert captured.value.__suppress_context__ is True


def test_child_handler_contracts_bind_exact_catalog_budgets_and_roles():
    records = {
        item["workload_id"]: item for item in native.child_handler_contracts()
    }
    assert set(records) == {
        native.OPENMETEO_WORKLOAD_ID,
        native.OPENMETEO_ICON2I_WORKLOAD_ID,
        native.GEOSPHERE_WORKLOAD_ID,
        native.ITALIAMETEO_CATALOG_WORKLOAD_ID,
        native.METEOLUX_WORKLOAD_ID,
    }
    assert records[native.OPENMETEO_WORKLOAD_ID]["maximum_transport_bytes"] == (
        8 * 1024 * 1024
    )
    for workload_id, record in records.items():
        if workload_id != native.OPENMETEO_WORKLOAD_ID:
            assert record["maximum_transport_bytes"] == 4 * 1024 * 1024
        assert record["maximum_artifacts"] == 1
        assert record["maximum_wall_seconds"] == 60
        assert record["role"] == "response_bytes"
        assert record["receipt_fetch_suffix"] == ":response_bytes"
        assert record["allowed_content_types"] == [
            native.UNINTERPRETED_CONTENT_TYPE
        ]
        assert record["capture_state"] == "captured_not_decoded"
        assert record["decode_status"] == "not_decoded"


def test_public_endpoint_identities_match_the_existing_provider_contracts():
    assert native.OPENMETEO_HOST + native.OPENMETEO_PATH == (
        "api.open-meteo.com/v1/forecast"
    )
    assert native.GEOSPHERE_HOST + native.GEOSPHERE_PATH == (
        "dataset.api.hub.geosphere.at"
        "/v1/timeseries/forecast/nowcast-v1-15min-1km"
    )
    assert native.ITALIAMETEO_HOST + native.ITALIAMETEO_PATH == (
        "meteohub.agenziaitaliameteo.it"
        "/api/datasets/radar_sri_dpc/opendata"
    )
    assert native.METEOLUX_HOST + native.METEOLUX_PATH == (
        "metapi.ana.lu/api/v1/metapp/weather"
    )
