"""Adversarial contract tests for the unwired Netatmo native candidate."""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.parse import quote

import pytest

from storm_tracker_worker import netatmo_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


TOKEN = "netatmo-access-credential-TEST-123456789"
CONTRACT_FIXTURE = (
    Path(__file__).parent / "fixtures" / "netatmo_getpublicdata_contract.json"
)


def _job(
    job_id: str = "netatmo-capture-1",
    *,
    query_key: str = "local:heffen",
) -> SimpleNamespace:
    return SimpleNamespace(
        job_id=job_id,
        payload={
            "latitude": 51.047,
            "longitude": 4.418,
            "radius_km": 20.0,
            "query_key": query_key,
            "query_kind": "local",
        },
        execution_context={"netatmo_access_token": TOKEN},
    )


def _payload(*, status: str = "ok", body: list | None = None) -> bytes:
    if status == "ok" and body is None:
        return CONTRACT_FIXTURE.read_bytes()
    if body is None:
        body = [{
            "_id": "70:ee:50:29:59:56",
            "place": {"location": [4.418, 51.047]},
            "last_status_store": 1_722_000_001,
            "measures": {
                "02:00:00:aa:bb:cc": {
                    "rain_timeutc": 1_722_000_000,
                    "rain_live": 0.1,
                    "rain_60min": 0.1,
                    "res": {
                        "1721999999": [0.0],
                        "1722000000": [0.1],
                    },
                    "type": ["rain"],
                },
            },
        }]
    return json.dumps(
        {
            "body": body,
            "status": status,
            "time_exec": 0.0123,
            "time_server": 1_722_000_002,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _response(
    payload: bytes | None = None,
    *,
    attempt: int = 1,
    attested: bool = True,
) -> native.NetatmoHttpResponse:
    return native.NetatmoHttpResponse(
        payload=_payload() if payload is None else payload,
        content_type="application/json",
        received_at=1_722_000_010.25,
        transport_attempt=attempt,
        secret_scan_attestation=(
            native._SECRET_SCAN_ATTESTATION if attested else None
        ),
    )


@pytest.fixture
def store(tmp_path: Path) -> RawArtifactStore:
    result = RawArtifactStore(tmp_path / "raw")
    result.initialize()
    return result


def _capture(monkeypatch, store: RawArtifactStore, payload: bytes | None = None):
    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: _response(payload),
    )
    return native.capture_netatmo_public_data(_job(), raw_store=store)


def test_exact_response_is_durable_before_first_json_parse(
    monkeypatch,
    store,
):
    exact = b' {"status":"ok","body":[],"time_server":1722000002}\n'
    original = native._inspect_response
    observed = []

    def inspect(payload, *, deadline):
        receipt_id = native._receipt_id(store, _job().job_id)
        receipt, stored = store.read_receipt_bytes(receipt_id)
        observed.append((receipt.content_sha256, stored))
        return original(payload, deadline=deadline)

    monkeypatch.setattr(native, "_inspect_response", inspect)
    result = _capture(monkeypatch, store, exact)

    assert observed == [(hashlib.sha256(exact).hexdigest(), exact)]
    assert result["raw_provenance"]["raw_store_schema_version"] == 3
    assert Path(result["raw_provenance"]["raw_sources"]["response_json"][
        "raw_receipt_id"
    ]) is not None


def test_station_ids_and_upstream_timestamp_tokens_remain_exact(
    monkeypatch,
    store,
):
    result = _capture(monkeypatch, store)

    station = result["station_structures"][0]
    assert station["station_id"] == "70:ee:50:29:59:56"
    assert station["station_upstream_timestamp_tokens"] == [{
        "json_type": "integer",
        "source": "last_status_store",
        "value": 1_722_000_001,
    }]
    assert station["modules"][0]["module_id"] == "02:00:00:aa:bb:cc"
    assert station["modules"][0]["upstream_timestamp_tokens"] == [
        {
            "json_type": "integer",
            "source": "rain_timeutc",
            "value": 1_722_000_000,
        },
        {
            "json_type": "string",
            "source": "res_key",
            "value": "1721999999",
        },
        {
            "json_type": "string",
            "source": "res_key",
            "value": "1722000000",
        },
    ]
    assert result["response_timestamp_tokens"] == [{
        "json_type": "integer",
        "source": "time_server",
        "value": 1_722_000_002,
    }]


def test_capture_never_claims_weather_coverage_or_ground_truth(
    monkeypatch,
    store,
):
    result = _capture(monkeypatch, store)

    assert result["weather_state"] == "unknown"
    assert result["precipitation_state"] == "unknown"
    assert result["wet_claim"] is False
    assert result["dry_claim"] is False
    assert result["ground_confirmation_eligible"] is False
    assert result["operational_admission_eligible"] is False
    assert result["coverage_status"] == "unknown_not_proven_by_station_count"
    assert result["raw_provenance"]["product_timestamp"] is None


def test_empty_station_list_is_not_a_dry_or_coverage_claim(monkeypatch, store):
    result = _capture(monkeypatch, store, _payload(body=[]))

    assert result["station_count"] == 0
    assert result["weather_state"] == "unknown"
    assert result["dry_claim"] is False
    assert result["coverage_status"].startswith("unknown")


def test_exact_capture_retry_reads_bundle_and_never_uses_network(
    monkeypatch,
    store,
):
    first = _capture(monkeypatch, store)
    calls = []
    original = store.load_capture_bundle_bytes

    def counted(capture_id):
        calls.append(capture_id)
        return original(capture_id)

    monkeypatch.setattr(store, "load_capture_bundle_bytes", counted)
    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("exact retry mag geen netwerk gebruiken")
        ),
    )
    second = native.capture_netatmo_public_data(_job(), raw_store=store)

    assert second == first
    assert calls == [native._capture_id(store, _job().job_id)]


def test_crash_after_receipt_resumes_without_network(monkeypatch, store):
    real_commit = native._commit_capture
    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: _response(),
    )
    monkeypatch.setattr(
        native,
        "_commit_capture",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("simulated crash")
        ),
    )
    with pytest.raises(RuntimeError, match="simulated crash"):
        native.capture_netatmo_public_data(_job(), raw_store=store)

    monkeypatch.setattr(native, "_commit_capture", real_commit)
    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("receipt-resume mag geen netwerk gebruiken")
        ),
    )
    result = native.capture_netatmo_public_data(_job(), raw_store=store)
    assert result["capture_state"] == "captured_structural_only"


@pytest.mark.parametrize(
    "echo",
    [
        TOKEN,
        quote(TOKEN, safe=""),
        base64.b64encode(TOKEN.encode("ascii")).decode("ascii"),
        base64.b32encode(TOKEN.encode("ascii")).decode("ascii"),
        "-".join(base64.b64encode(TOKEN.encode("ascii")).decode("ascii")),
        "-".join(TOKEN),
        "".join(f"%{ord(character):02X}" for character in TOKEN),
        "".join(f"&#{ord(character)};" for character in TOKEN),
        "".join(
            chr(ord(character) + 0xFEE0)
            if 0x21 <= ord(character) <= 0x7E else character
            for character in TOKEN
        ),
        "".join(
            f"\\u{ord(character):04x}" for character in TOKEN
        ),
    ],
)
def test_adversarial_credential_echo_is_never_persisted(
    monkeypatch,
    store,
    echo,
):
    payload = json.dumps({"body": [], "status": "ok", "echo": echo}).encode()
    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: _response(payload, attested=False),
    )
    with pytest.raises(native.NetatmoNativeContractError) as raised:
        native.capture_netatmo_public_data(_job(), raw_store=store)

    assert TOKEN not in str(raised.value)
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["capture_count"] == 0
    all_bytes = b"".join(
        path.read_bytes() for path in store.root.rglob("*") if path.is_file()
    )
    assert TOKEN.encode() not in all_bytes


@pytest.mark.parametrize(
    "field",
    [
        "access_token",
        "refresh-token",
        "Authorization",
        "Set_Cookie",
        "api-key",
        "client_id",
        "application-key",
    ],
)
def test_auth_metadata_keys_are_rejected_before_storage(
    monkeypatch,
    store,
    field,
):
    payload = json.dumps({"body": [], "status": "ok", field: "opaque"}).encode()
    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: _response(payload, attested=False),
    )
    with pytest.raises(native.NetatmoNativeContractError, match="authmetadata"):
        native.capture_netatmo_public_data(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 0


def test_token_is_absent_from_result_provenance_cache_and_catalog(
    monkeypatch,
    store,
):
    result = _capture(monkeypatch, store)
    serialized = json.dumps(result, sort_keys=True)
    all_bytes = b"".join(
        path.read_bytes() for path in store.root.rglob("*") if path.is_file()
    )

    assert TOKEN not in serialized
    for variant in native._secret_variants(TOKEN):
        assert variant not in all_bytes
    assert "authorization" not in serialized.lower()
    assert "access_token" not in serialized.lower()
    assert "cookie" not in serialized.lower()


@pytest.mark.parametrize("identity_field", ["job_id", "query_key"])
def test_credential_cannot_be_smuggled_into_durable_identity(
    store,
    identity_field,
):
    job = _job()
    if identity_field == "job_id":
        job.job_id = f"capture:{TOKEN.lower()}"
    else:
        job.payload["query_key"] = f"local:{TOKEN.lower()}"
    with pytest.raises(native.NetatmoNativeContractError):
        native.capture_netatmo_public_data(job, raw_store=store)
    assert store.stats()["receipt_count"] == 0


def test_payload_cannot_contain_auth_or_unknown_fields(store):
    job = _job()
    job.payload["access_token"] = TOKEN
    with pytest.raises(native.NetatmoNativeContractError, match="vijf queryvelden"):
        native.capture_netatmo_public_data(job, raw_store=store)


def test_execution_context_must_contain_only_brokered_token(store):
    job = _job()
    job.execution_context["cookie"] = "forbidden"
    with pytest.raises(native.NetatmoNativeContractError, match="brokered"):
        native.capture_netatmo_public_data(job, raw_store=store)


def test_existing_parent_broker_effect_context_is_accepted(monkeypatch, store):
    job = _job()
    job.execution_context["effect_contract_id"] = (
        native.NETATMO_BROKER_EFFECT_CONTRACT
    )
    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: _response(),
    )
    result = native.capture_netatmo_public_data(job, raw_store=store)
    assert result["capture_state"] == "captured_structural_only"


def test_deterministic_structure_rejection_is_immutable_and_no_redownload(
    monkeypatch,
    store,
):
    invalid = b'{"body":"not-a-list","status":"ok"}'
    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: _response(invalid),
    )
    with pytest.raises(native.NetatmoNativeContractError):
        native.capture_netatmo_public_data(_job(), raw_store=store)
    verdicts = list((store.root / "netatmo_validation").glob("*.json"))
    assert len(verdicts) == 1
    before = verdicts[0].read_bytes()
    before_mtime = verdicts[0].stat().st_mtime_ns

    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("duurzame structurele afkeur mag niet herdownloaden")
        ),
    )
    with pytest.raises(native.NetatmoNativeContractError, match="reeds compleet"):
        native.capture_netatmo_public_data(_job(), raw_store=store)
    assert verdicts[0].read_bytes() == before
    assert verdicts[0].stat().st_mtime_ns == before_mtime
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_transport_failure_is_not_persisted_or_cached(monkeypatch, store):
    calls = []

    def fail(*_args, **_kwargs):
        calls.append("attempt")
        raise native.NetatmoNativeTransportError("safe temporary failure")

    monkeypatch.setattr(native, "_download_with_retries", fail)
    for _ in range(2):
        with pytest.raises(native.NetatmoNativeTransportError):
            native.capture_netatmo_public_data(_job(), raw_store=store)
    assert calls == ["attempt", "attempt"]
    assert store.stats()["receipt_count"] == 0
    validation = store.root / "netatmo_validation"
    assert not validation.exists() or not list(validation.glob("*.json"))


def test_bounded_transport_retry_records_only_success_attempt(monkeypatch, store):
    calls = []
    monkeypatch.setattr(native, "_bounded_backoff", lambda *_args: None)

    def one(query, *, credential, deadline, transport_attempt):
        calls.append((transport_attempt, credential))
        if transport_attempt < 3:
            raise native.NetatmoNativeTransportError("safe")
        return _response(attempt=transport_attempt)

    monkeypatch.setattr(native, "_download_once", one)
    result = native.capture_netatmo_public_data(_job(), raw_store=store)
    receipt = store.load_receipt(
        result["raw_provenance"]["raw_sources"]["response_json"]["raw_receipt_id"]
    )

    assert calls == [(1, TOKEN), (2, TOKEN), (3, TOKEN)]
    assert receipt is not None
    assert receipt.provenance["transport_attempt"] == 3
    assert TOKEN not in json.dumps(receipt.to_metadata())


def test_transport_retry_exhaustion_stores_nothing(monkeypatch, store):
    calls = []
    monkeypatch.setattr(native, "_bounded_backoff", lambda *_args: None)

    def fail(*_args, transport_attempt, **_kwargs):
        calls.append(transport_attempt)
        raise native.NetatmoNativeTransportError("safe")

    monkeypatch.setattr(native, "_download_once", fail)
    with pytest.raises(native.NetatmoNativeTransportError):
        native.capture_netatmo_public_data(_job(), raw_store=store)
    assert calls == [1, 2, 3]
    assert store.stats()["receipt_count"] == 0


def test_http_error_text_and_url_cannot_leak_token(monkeypatch):
    query = native._validate_query_payload(_job().payload, TOKEN)

    def fail(_request, *, timeout):
        raise HTTPError(
            f"https://example.invalid/{TOKEN}",
            401,
            f"Bearer {TOKEN}",
            {},
            None,
        )

    monkeypatch.setattr(native, "_open_public_request", fail)
    with pytest.raises(native.NetatmoNativeProviderResponseError) as raised:
        native._download_once(
            query,
            credential=TOKEN,
            deadline=time.monotonic() + 10,
            transport_attempt=1,
        )
    assert raised.value.http_status == 401
    assert raised.value.retryable is False
    assert TOKEN not in str(raised.value)


def test_request_has_exact_credential_free_url_and_header_only_token(monkeypatch):
    observed = {}
    payload = _payload(body=[])

    class Response:
        status = 200
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": str(len(payload)),
            "Set-Cookie": f"session={TOKEN}",
        }

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def getcode(self):
            return 200

        def geturl(self):
            return observed["request"].full_url

        def read(self, _size):
            if observed.get("read"):
                return b""
            observed["read"] = True
            return payload

    def open_request(request, *, timeout):
        observed["request"] = request
        observed["timeout"] = timeout
        return Response()

    monkeypatch.setattr(native, "_open_public_request", open_request)
    query = native._validate_query_payload(_job().payload, TOKEN)
    response = native._download_once(
        query,
        credential=TOKEN,
        deadline=time.monotonic() + 10,
        transport_attempt=1,
    )

    assert TOKEN not in observed["request"].full_url
    assert observed["request"].get_header("Authorization") == f"Bearer {TOKEN}"
    assert "cookie" not in {key.lower() for key in observed["request"].headers}
    assert response.payload == payload
    assert not hasattr(response, "headers")


def test_response_byte_content_and_json_structure_are_hard_bounded(monkeypatch):
    class Headers(dict):
        pass

    with pytest.raises(native.NetatmoNativeContractError, match="bytegrens"):
        native._declared_content_length(Headers({
            "Content-Length": str(native.MAX_RESPONSE_BYTES + 1)
        }))

    deep: object = "leaf"
    for _ in range(native.MAX_JSON_DEPTH + 2):
        deep = [deep]
    payload = json.dumps({"status": "ok", "body": [], "extra": deep}).encode()
    with pytest.raises(native.NetatmoNativeContractError, match="dieptelimiet"):
        native._inspect_response(payload, deadline=time.monotonic() + 10)


def test_duplicate_json_keys_fail_closed():
    payload = b'{"status":"ok","status":"ok","body":[]}'
    with pytest.raises(native.NetatmoNativeContractError, match="dubbele"):
        native._inspect_response(payload, deadline=time.monotonic() + 10)


def test_station_module_and_timestamp_counts_are_bounded():
    modules = {
        f"module-{index}": {} for index in range(native.MAX_MODULES_PER_STATION + 1)
    }
    payload = _payload(body=[{
        "_id": "station-a",
        "place": {"location": [4.0, 51.0]},
        "measures": modules,
    }])
    with pytest.raises(native.NetatmoNativeContractError, match="measures"):
        native._inspect_response(payload, deadline=time.monotonic() + 10)

    results = {str(index): [0] for index in range(
        native.MAX_TIMESTAMP_TOKENS_PER_MODULE + 1
    )}
    payload = _payload(body=[{
        "_id": "station-a",
        "place": {"location": [4.0, 51.0]},
        "measures": {"module-a": {"res": results}},
    }])
    with pytest.raises(native.NetatmoNativeContractError, match="timestampgrens"):
        native._inspect_response(payload, deadline=time.monotonic() + 10)


def test_duplicate_station_identity_fails_closed():
    station = {
        "_id": "same",
        "place": {"location": [4.0, 51.0]},
        "measures": {},
    }
    with pytest.raises(native.NetatmoNativeContractError, match="dubbele station"):
        native._inspect_response(
            _payload(body=[station, station]),
            deadline=time.monotonic() + 10,
        )


def test_deadline_failure_does_not_create_weather_capture(monkeypatch, store):
    monkeypatch.setattr(
        native,
        "_new_deadline",
        lambda: time.monotonic() - 1,
    )
    with pytest.raises(native.NetatmoNativeDeadlineError):
        native.capture_netatmo_public_data(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["capture_count"] == 0


def test_datum_boundary_crossing_fails_closed_without_rounding_geometry(store):
    job = _job()
    job.payload["longitude"] = 179.99
    with pytest.raises(native.NetatmoNativeContractError, match="datumgrens"):
        native.capture_netatmo_public_data(job, raw_store=store)


def test_validation_cache_unknown_file_and_capacity_fail_closed(
    monkeypatch,
    store,
):
    directory = native._validation_directory(store)
    (directory / "unknown.txt").write_text("not managed", encoding="utf-8")
    monkeypatch.setattr(native, "MAX_VALIDATION_VERDICTS", 1)
    with pytest.raises(native.NetatmoNativeContractError):
        _capture(monkeypatch, store)


def test_validation_cache_total_bytes_are_hard_bounded(monkeypatch, store):
    monkeypatch.setattr(native, "MAX_VALIDATION_TOTAL_BYTES", 1)
    with pytest.raises(native.NetatmoNativeContractError, match="totale bytegrens"):
        _capture(monkeypatch, store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_validation_cache_entry_count_is_hard_bounded(monkeypatch, store):
    monkeypatch.setattr(native, "MAX_VALIDATION_VERDICTS", 0)
    with pytest.raises(native.NetatmoNativeContractError, match="entrygrens"):
        _capture(monkeypatch, store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_corrupt_validation_verdict_fails_closed_without_network(
    monkeypatch,
    store,
):
    _capture(monkeypatch, store)
    verdict = next((store.root / "netatmo_validation").glob("*.json"))
    document = json.loads(verdict.read_text(encoding="utf-8"))
    document["body_sha256"] = "0" * 64
    verdict.write_text(json.dumps(document), encoding="utf-8")
    monkeypatch.setattr(
        native,
        "_download_with_retries",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("corrupt cache mag geen netwerkfallback veroorzaken")
        ),
    )
    with pytest.raises(native.NetatmoNativeContractError, match="identiteitsbewijs"):
        native.capture_netatmo_public_data(_job(), raw_store=store)


def test_file_lock_serializes_independent_store_instances(tmp_path):
    first_store = RawArtifactStore(tmp_path / "raw")
    first_store.initialize()
    second_store = RawArtifactStore(tmp_path / "raw")
    second_store.initialize()
    path_a = native._lock_path(first_store, "slot", "same")
    path_b = native._lock_path(second_store, "slot", "same")
    events = []
    first_has_lock = threading.Event()

    def first():
        with native._deadline_file_lock(path_a, deadline=time.monotonic() + 5):
            events.append("first-acquired")
            first_has_lock.set()
            time.sleep(0.15)
            events.append("first-release")

    def second():
        first_has_lock.wait(2)
        with native._deadline_file_lock(path_b, deadline=time.monotonic() + 5):
            events.append("second-acquired")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(first), executor.submit(second)]
        for future in futures:
            future.result()
    assert events == ["first-acquired", "first-release", "second-acquired"]


def test_file_lock_is_effective_across_real_processes(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    script = """
import sys, time
from pathlib import Path
from storm_tracker_worker.raw_artifacts import RawArtifactStore
from storm_tracker_worker import netatmo_native as native
store = RawArtifactStore(Path(sys.argv[1]))
store.initialize()
path = native._lock_path(store, 'slot', 'cross-process')
with native._deadline_file_lock(path, deadline=time.monotonic() + 5):
    print('locked', flush=True)
    time.sleep(0.7)
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(Path.cwd())
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(store.root)],
        cwd=Path.cwd(),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "locked"
        lock_path = native._lock_path(store, "slot", "cross-process")
        with pytest.raises(native.NetatmoNativeDeadlineError):
            with native._deadline_file_lock(
                lock_path,
                deadline=time.monotonic() + 0.15,
            ):
                raise AssertionError("proceslock werd niet gerespecteerd")
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0, (stdout, stderr)
        with native._deadline_file_lock(
            lock_path,
            deadline=time.monotonic() + 2,
        ):
            pass
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_unique_jobs_use_a_fixed_bounded_lock_shard_set(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    for index in range(native.MAX_LOCK_SHARDS + 17):
        path = native._lock_path(store, "slot", f"job-{index}")
        with native._deadline_file_lock(
            path,
            deadline=time.monotonic() + 2,
        ):
            pass
    entries = list((store.root / "netatmo_locks").glob("slot-*.lock"))
    assert 1 <= len(entries) <= native.MAX_LOCK_SHARDS


def test_query_context_is_complete_stable_and_has_no_circle_claim():
    query = native._validate_query_payload(_job().payload, TOKEN)
    context = native._query_context(query)
    serialized = json.dumps(context, sort_keys=True)

    assert context["method"] == "GET"
    assert context["path"] == "/api/getpublicdata"
    assert context["wire_geometry"] == "bounding_box_not_geodesic_circle"
    assert [item["name"] for item in context["parameters"]] == [
        "lat_ne",
        "lon_ne",
        "lat_sw",
        "lon_sw",
        "filter",
    ]
    assert TOKEN not in serialized
    assert "Bearer" not in serialized
