from __future__ import annotations

import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from storm_tracker_worker import kmi_appv4_raw_capture as appv4
from storm_tracker_worker.raw_artifacts import RawArtifactStore


JOB_ID = (
    "acq:aaaaaaaa:z:bbbbbbbb:z:cccccccc:z:dddddddd:z:eeeeeeee:"
    "z:ffffffff:z:gggg"
)
SECOND_JOB_ID = (
    "acq:hhhhhhhh:z:iiiiiiii:z:jjjjjjjj:z:kkkkkkkk:z:llllllll:"
    "z:mmmmmmmm:z:nnnn"
)
QUERY_KEY = (
    "aqr:22222222:z:33333333:z:44444444:z:55555555:z:66666666:"
    "z:77777777:z:abcd"
)
IMAGE_QUERY_SECRET = "never-persist-selected-image-query"
IMAGE_KEY_SECRET = "never-persist-selected-image-key"
PAST_TIME = "2026-08-14T11:55:00+00:00"
FUTURE_TIME = "2026-08-14T12:05:00+00:00"
PAST_TIMESTAMP = datetime.fromisoformat(PAST_TIME).timestamp()
REQUEST_AT = datetime(2026, 8, 14, 12, 0, tzinfo=timezone.utc).timestamp()
FORECAST_RECEIVED_AT = REQUEST_AT + 0.25
IMAGE_RECEIVED_AT = REQUEST_AT + 1.5
PAST_URI = (
    "https://app.meteo.be/rendered/animation/frame-past.png"
    f"?token={IMAGE_QUERY_SECRET}&k={IMAGE_KEY_SECRET}"
)
FUTURE_URI = "https://app.meteo.be/rendered/animation/frame-future.png"


def _payload(*, latitude: float = 51.05, longitude: float = 4.40) -> dict:
    return {
        "contract": appv4.PUBLIC_REGION_QUERY_CONTRACT,
        "latitude": latitude,
        "longitude": longitude,
        "query_key": QUERY_KEY,
    }


def _job(job_id: str = JOB_ID, **payload_overrides: object) -> SimpleNamespace:
    payload = _payload()
    payload.update(payload_overrides)
    return SimpleNamespace(job_id=job_id, payload=payload)


def _forecast_bytes(
    *,
    past_uri: str = PAST_URI,
    past_time: str = PAST_TIME,
    future_uri: str = FUTURE_URI,
    future_time: str = FUTURE_TIME,
) -> bytes:
    return json.dumps(
        {
            "animation": {
                "sequence": [
                    {"time": past_time, "uri": past_uri},
                    {"time": future_time, "uri": future_uri},
                ]
            },
            # Deliberately unrelated weather-shaped values.  The raw-only
            # handler must neither validate nor return them.
            "obs": {"temp": 18.7, "ww": 95},
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


class _Headers:
    def __init__(self, entries: list[tuple[str, str]]) -> None:
        self._entries = list(entries)

    def items(self):
        return list(self._entries)


class _Response:
    def __init__(
        self,
        payload: bytes,
        *,
        status: int = 200,
        content_type: str | None = "application/octet-stream",
        content_encoding: str | None = "identity",
        declared_length: int | None | object = ...,
        final_url: str | None = None,
        extra_headers: list[tuple[str, str]] | None = None,
    ) -> None:
        self.payload = payload
        self.status = status
        self._offset = 0
        self._final_url = final_url
        entries: list[tuple[str, str]] = []
        if declared_length is ...:
            entries.append(("Content-Length", str(len(payload))))
        elif declared_length is not None:
            entries.append(("Content-Length", str(declared_length)))
        if content_type is not None:
            entries.append(("Content-Type", content_type))
        if content_encoding is not None:
            entries.append(("Content-Encoding", content_encoding))
        entries.extend(extra_headers or ())
        self.headers = _Headers(entries)
        self.closed = False

    def read(self, amount: int) -> bytes:
        value = self.payload[self._offset : self._offset + amount]
        self._offset += len(value)
        return value

    def geturl(self) -> str:
        if self._final_url is None:
            raise AssertionError("opener did not bind the response URL")
        return self._final_url

    def close(self) -> None:
        self.closed = True


class _Opener:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = list(responses)
        self.calls: list[str] = []
        self.timeouts: list[float] = []

    def __call__(self, request, *, timeout: float):
        if not self.responses:
            raise AssertionError("unexpected network request")
        response = self.responses.pop(0)
        response._final_url = response._final_url or request.full_url
        self.calls.append(request.full_url)
        self.timeouts.append(timeout)
        assert request.get_method() == "GET"
        assert request.get_header("User-agent") == appv4.APP_USER_AGENT
        assert request.get_header("Accept-encoding") == "identity"
        return response


class _Clock:
    def __init__(self, *values: float) -> None:
        self.values = iter(values)

    def __call__(self) -> float:
        return next(self.values)


def _run_success(tmp_path, *, forecast: bytes | None = None, image: bytes = b"raw-png"):
    forecast_payload = _forecast_bytes() if forecast is None else forecast
    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([
        _Response(forecast_payload, content_type="application/json"),
        _Response(image, content_type="image/png"),
    ])
    result = appv4.capture_kmi_appv4_visual_raw(
        _job(),
        raw_store=store,
        opener=opener,
        _wall_clock=_Clock(
            REQUEST_AT, FORECAST_RECEIVED_AT, IMAGE_RECEIVED_AT
        ),
    )
    return store, opener, result, forecast_payload, image


def _bundle(store: RawArtifactStore, result: dict):
    loaded = store.load_capture_bundle_bytes(
        result["raw_provenance"]["raw_capture_id"]
    )
    assert loaded is not None
    return loaded


def test_exact_forecast_and_selected_image_are_one_atomic_raw_capture(tmp_path):
    store, opener, result, forecast_payload, image_payload = _run_success(tmp_path)
    capture, receipts, payloads = _bundle(store, result)

    assert len(opener.calls) == 2
    assert urlsplit(opener.calls[0]).hostname == "app.meteo.be"
    assert urlsplit(opener.calls[0]).path == "/services/appv4/"
    assert parse_qs(urlsplit(opener.calls[0]).query)["s"] == ["getForecasts"]
    assert opener.calls[1] == PAST_URI
    assert payloads == {
        appv4.FORECAST_ROLE: forecast_payload,
        appv4.IMAGE_ROLE: image_payload,
    }
    assert set(receipts) == {appv4.FORECAST_ROLE, appv4.IMAGE_ROLE}
    assert {member.role for member in capture.members} == {
        appv4.FORECAST_ROLE,
        appv4.IMAGE_ROLE,
    }
    assert capture.product_timestamp == PAST_TIMESTAMP
    assert store.stats()["object_count"] == 2
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1
    assert store.integrity_check()["healthy"] is True

    assert result["capture_state"] == "captured_not_decoded"
    assert result["decode_status"] == "not_attempted"
    assert result["meteorological_parse_attempted"] is False
    assert result["meteorological_claim"] is False
    assert result["operational_admission_eligible"] is False
    assert result["weather_state"] == "unknown"
    assert result["provider_rights_claim"] is False
    assert result["retention_right_claim"] is False
    assert result["rights_review_status"] == "contract_review_hold"
    assert "diagnostics" not in result


def test_official_kmi_cdn_frame_is_accepted_without_widening_host_scope(tmp_path):
    cdn_uri = "https://cdn.meteo.be/rendered/animation/frame-past.png"
    forecast = _forecast_bytes(past_uri=cdn_uri)
    store, opener, result, _forecast, _image = _run_success(
        tmp_path, forecast=forecast
    )

    assert opener.calls[1] == cdn_uri
    assert urlsplit(opener.calls[1]).hostname == "cdn.meteo.be"
    assert store.stats()["capture_count"] == 1
    assert result["capture_state"] == "captured_not_decoded"

    rejected = _forecast_bytes(
        past_uri="https://frames.cdn.meteo.be/frame.png",
        future_uri="https://frames.cdn.meteo.be/future.png",
    )
    with pytest.raises(appv4.KmiAppv4SelectionError):
        appv4.capture_kmi_appv4_visual_raw(
            _job(job_id=SECOND_JOB_ID),
            raw_store=store,
            opener=_Opener([_Response(rejected, content_type="application/json")]),
            _wall_clock=_Clock(REQUEST_AT, FORECAST_RECEIVED_AT),
        )


def test_empty_standard_response_header_value_is_preserved(tmp_path):
    forecast_payload = _forecast_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([
        _Response(
            forecast_payload,
            content_type="application/json",
            extra_headers=[("Expires", "")],
        ),
        _Response(b"raw-png", content_type="image/png"),
    ])

    result = appv4.capture_kmi_appv4_visual_raw(
        _job(),
        raw_store=store,
        opener=opener,
        _wall_clock=_Clock(
            REQUEST_AT, FORECAST_RECEIVED_AT, IMAGE_RECEIVED_AT
        ),
    )

    assert result["capture_state"] == "captured_not_decoded"
    assert store.stats()["capture_count"] == 1


def test_forecast_is_durable_before_minimal_link_selection(tmp_path, monkeypatch):
    forecast_payload = _forecast_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([
        _Response(forecast_payload, content_type="application/json"),
        _Response(b"png", content_type="image/png"),
    ])
    real_select = appv4._select_frame
    observations: list[dict[str, int]] = []

    def checked_select(payload: bytes, **kwargs):
        assert payload == forecast_payload
        observations.append(store.stats())
        assert store.stats()["object_count"] == 1
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        return real_select(payload, **kwargs)

    monkeypatch.setattr(appv4, "_select_frame", checked_select)
    appv4.capture_kmi_appv4_visual_raw(
        _job(),
        raw_store=store,
        opener=opener,
        _wall_clock=_Clock(
            REQUEST_AT, FORECAST_RECEIVED_AT, IMAGE_RECEIVED_AT
        ),
    )

    assert len(observations) >= 1


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"{",
        b"[]",
        b'{"animation":{"sequence":[]}}',
        b'{"animation":{"sequence":[{"time":"bad","uri":"x"}]}}',
        b"\xff\xfe\x00",
    ],
)
def test_empty_or_malformed_forecast_2xx_is_retained_without_success_manifest(
    tmp_path, payload,
):
    store = RawArtifactStore(tmp_path / hashlib.sha256(payload).hexdigest()[:12])
    opener = _Opener([_Response(payload, content_type="application/json")])

    with pytest.raises(appv4.KmiAppv4SelectionError):
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=opener,
            _wall_clock=_Clock(REQUEST_AT, FORECAST_RECEIVED_AT),
        )

    assert len(opener.calls) == 1
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    receipts = store.accounting_receipts()
    assert len(receipts) == 1
    assert receipts[0].product == appv4.FORECAST_PRODUCT
    assert store.read_bytes(receipts[0].receipt_id) == payload

    # A same-job retry replays the exact durable failure and performs no
    # second request that could silently bind another forecast.
    with pytest.raises(appv4.KmiAppv4SelectionError):
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=lambda *_args, **_kwargs: pytest.fail("network retry"),
        )
    assert store.stats()["capture_count"] == 0


def test_unallowlisted_selected_uri_stops_after_forecast_storage(tmp_path):
    forecast = _forecast_bytes(
        past_uri="https://attacker.example/frame.png?token=secret",
        future_uri="https://attacker.example/future.png?token=secret",
    )
    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([_Response(forecast, content_type="application/json")])

    with pytest.raises(appv4.KmiAppv4SelectionError):
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=opener,
            _wall_clock=_Clock(REQUEST_AT, FORECAST_RECEIVED_AT),
        )

    assert len(opener.calls) == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_malformed_image_2xx_is_exact_raw_and_not_decoded(tmp_path):
    malformed = b"this is not a PNG and that must not block raw storage"
    store, _opener, result, _forecast, image = _run_success(
        tmp_path, image=malformed
    )
    _capture, receipts, payloads = _bundle(store, result)

    assert image == malformed
    assert payloads[appv4.IMAGE_ROLE] == malformed
    assert receipts[appv4.IMAGE_ROLE].provenance[
        "meteorological_parse_attempted"
    ] is False
    assert result["decode_status"] == "not_attempted"


def test_empty_image_2xx_is_a_complete_two_role_transport_capture(tmp_path):
    store, _opener, result, _forecast, _image = _run_success(tmp_path, image=b"")
    capture, receipts, payloads = _bundle(store, result)

    assert payloads[appv4.IMAGE_ROLE] == b""
    assert receipts[appv4.IMAGE_ROLE].size_bytes == 0
    assert len(capture.members) == 2
    # The empty image object has a real exact SHA-256 object identity.
    assert receipts[appv4.IMAGE_ROLE].content_sha256 == hashlib.sha256(b"").hexdigest()


def test_secret_query_values_never_enter_metadata_result_or_catalog(tmp_path):
    store, opener, result, _forecast, _image = _run_success(tmp_path)
    capture, receipts, _payloads = _bundle(store, result)
    forecast_query = parse_qs(urlsplit(opener.calls[0]).query)
    request_key = forecast_query["k"][0]
    assert len(request_key) == 32

    serialized = json.dumps(
        {
            "capture": capture.to_metadata(),
            "handler": appv4.kmi_appv4_child_handler_contracts(),
            "receipts": {
                role: receipt.to_metadata()
                for role, receipt in receipts.items()
            },
            "recovery": appv4.kmi_appv4_recovery_binding_metadata(),
            "result": result,
        },
        sort_keys=True,
    )
    for forbidden in (
        IMAGE_QUERY_SECRET,
        IMAGE_KEY_SECRET,
        QUERY_KEY,
        request_key,
        "r9EnW374jkJ9acc",
        "?token=",
        "?k=",
        "/rendered/animation/",
    ):
        assert forbidden not in serialized

    catalog = (tmp_path / "raw" / "raw_artifacts.sqlite3").read_bytes()
    for forbidden in (
        IMAGE_QUERY_SECRET,
        IMAGE_KEY_SECRET,
        QUERY_KEY,
        request_key,
        "r9EnW374jkJ9acc",
    ):
        assert forbidden.encode("utf-8") not in catalog

    # Query-bearing source bytes remain exact in the content-addressed object;
    # redaction applies to evidence metadata, never to upstream raw payload.
    assert IMAGE_QUERY_SECRET.encode() in store.read_bytes(
        receipts[appv4.FORECAST_ROLE].receipt_id
    )


def test_transport_exception_never_reflects_credential_bearing_url(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    observed_url: list[str] = []

    def failing_opener(request, *, timeout):
        observed_url.append(request.full_url)
        raise RuntimeError(f"socket failure for {request.full_url}")

    with pytest.raises(appv4.KmiAppv4TransportError) as error:
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=failing_opener,
            _wall_clock=_Clock(REQUEST_AT),
        )

    key = parse_qs(urlsplit(observed_url[0]).query)["k"][0]
    assert key not in str(error.value)
    assert observed_url[0] not in str(error.value)
    assert store.stats()["receipt_count"] == 0


def test_redirect_or_final_url_drift_is_rejected_before_storage(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([
        _Response(
            _forecast_bytes(),
            content_type="application/json",
            final_url="https://app.meteo.be/redirected-without-query",
        )
    ])

    with pytest.raises(appv4.KmiAppv4ContractError, match="exacte request"):
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=opener,
            _wall_clock=_Clock(REQUEST_AT),
        )

    assert store.stats()["object_count"] == 0
    assert store.stats()["receipt_count"] == 0


def test_non_identity_transport_is_rejected_before_storage(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([
        _Response(
            _forecast_bytes(),
            content_type="application/json",
            content_encoding="gzip",
        )
    ])

    with pytest.raises(appv4.KmiAppv4ContractError, match="identity"):
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=opener,
            _wall_clock=_Clock(REQUEST_AT),
        )

    assert store.stats()["receipt_count"] == 0


def test_declared_image_over_role_limit_keeps_only_forecast_receipt(tmp_path):
    forecast = _forecast_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([
        _Response(forecast, content_type="application/json"),
        _Response(
            b"not-read",
            content_type="image/png",
            declared_length=appv4.IMAGE_MAX_BYTES + 1,
        ),
    ])

    with pytest.raises(appv4.KmiAppv4CapacityError, match="Content-Length"):
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=opener,
            _wall_clock=_Clock(REQUEST_AT, FORECAST_RECEIVED_AT),
        )

    assert len(opener.calls) == 2
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_content_length_mismatch_is_not_published(tmp_path):
    payload = _forecast_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([
        _Response(
            payload,
            content_type="application/json",
            declared_length=len(payload) + 1,
        )
    ])

    with pytest.raises(appv4.KmiAppv4ContractError, match="Content-Length"):
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=opener,
            _wall_clock=_Clock(REQUEST_AT),
        )

    assert store.stats()["receipt_count"] == 0


def test_crash_after_forecast_store_resumes_without_refetching_forecast(tmp_path):
    class Crash(RuntimeError):
        pass

    forecast = _forecast_bytes()
    image = b"png"
    store = RawArtifactStore(tmp_path / "raw")
    first = _Opener([_Response(forecast, content_type="application/json")])

    def crash(stage: str) -> None:
        if stage == "kmi_appv4.after_forecast_store":
            raise Crash(stage)

    with pytest.raises(Crash):
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=first,
            _wall_clock=_Clock(REQUEST_AT, FORECAST_RECEIVED_AT),
            _fault_injector=crash,
        )

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0

    second = _Opener([_Response(image, content_type="image/png")])
    result = appv4.capture_kmi_appv4_visual_raw(
        _job(),
        raw_store=store,
        opener=second,
        _wall_clock=_Clock(IMAGE_RECEIVED_AT),
    )
    assert second.calls == [PAST_URI]
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1
    assert result["raw_provenance"]["raw_member_count"] == 2


def test_crash_after_image_store_resumes_manifest_without_network(tmp_path):
    class Crash(RuntimeError):
        pass

    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([
        _Response(_forecast_bytes(), content_type="application/json"),
        _Response(b"png", content_type="image/png"),
    ])

    def crash(stage: str) -> None:
        if stage == "kmi_appv4.after_image_store":
            raise Crash(stage)

    with pytest.raises(Crash):
        appv4.capture_kmi_appv4_visual_raw(
            _job(),
            raw_store=store,
            opener=opener,
            _wall_clock=_Clock(
                REQUEST_AT, FORECAST_RECEIVED_AT, IMAGE_RECEIVED_AT
            ),
            _fault_injector=crash,
        )

    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 0
    result = appv4.capture_kmi_appv4_visual_raw(
        _job(),
        raw_store=store,
        opener=lambda *_args, **_kwargs: pytest.fail("unexpected network"),
    )
    assert store.stats()["capture_count"] == 1
    assert result["capture_state"] == "captured_not_decoded"


def test_complete_same_job_retry_is_idempotent_and_network_free(tmp_path):
    store, _opener, first, _forecast, _image = _run_success(tmp_path)
    retry = appv4.capture_kmi_appv4_visual_raw(
        _job(),
        raw_store=store,
        opener=lambda *_args, **_kwargs: pytest.fail("unexpected network"),
    )

    assert retry == first
    assert store.stats()["object_count"] == 2
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1


def test_same_job_location_authority_drift_fails_closed_without_network(tmp_path):
    store, _opener, _result_value, _forecast, _image = _run_success(tmp_path)

    with pytest.raises(appv4.KmiAppv4CorruptionError):
        appv4.capture_kmi_appv4_visual_raw(
            _job(latitude=50.85, longitude=4.35),
            raw_store=store,
            opener=lambda *_args, **_kwargs: pytest.fail("unexpected network"),
        )


def test_recovery_api_reproves_exact_bundle_and_rejects_payload_drift(tmp_path):
    store, _opener, result, _forecast, _image = _run_success(tmp_path)
    capture, receipts, payloads = _bundle(store, result)

    recovered = appv4.validate_kmi_appv4_capture_for_recovery(
        capture,
        receipts,
        payloads,
        job_id=JOB_ID,
        handler_payload=_payload(),
    )
    assert recovered == result

    drifted = dict(payloads)
    drifted[appv4.IMAGE_ROLE] += b"drift"
    with pytest.raises(appv4.KmiAppv4CorruptionError):
        appv4.validate_kmi_appv4_capture_for_recovery(
            capture,
            receipts,
            drifted,
            job_id=JOB_ID,
            handler_payload=_payload(),
        )


def test_self_contained_raw_recovery_and_shared_binding_reprove_every_byte(tmp_path):
    from storm_tracker_worker import acquisition_capture_recovery as shared_recovery

    store, _opener, result, _forecast, _image = _run_success(tmp_path)
    capture, receipts, payloads = _bundle(store, result)

    recovered = appv4.validate_kmi_appv4_capture_for_raw_recovery(
        capture,
        receipts,
        payloads,
        job_id=JOB_ID,
    )
    assert recovered == result
    binding = shared_recovery.RECOVERY_BINDINGS[appv4.HANDLER_ID]
    assert binding.job_identity_kind == "safe_native_alias_v1"
    assert binding.required_roles == (appv4.FORECAST_ROLE, appv4.IMAGE_ROLE)
    metadata = {}
    for member in binding.members:
        recovered_job_id, transport_sha256 = shared_recovery._validate_receipt(  # noqa: SLF001
            receipts[member.role],
            binding,
            member,
        )
        assert recovered_job_id == JOB_ID
        assert transport_sha256 == receipts[member.role].provenance[
            "transport_sha256"
        ]
        metadata[member.role] = transport_sha256
    assert shared_recovery._validate_capture(  # noqa: SLF001
        capture,
        binding=binding,
        receipts_by_role=receipts,
        metadata_by_role=metadata,
        payloads_by_role=payloads,
    ) == JOB_ID

    drifted = dict(payloads)
    drifted[appv4.FORECAST_ROLE] += b" "
    with pytest.raises(appv4.KmiAppv4CorruptionError):
        appv4.validate_kmi_appv4_capture_for_raw_recovery(
            capture,
            receipts,
            drifted,
            job_id=JOB_ID,
        )


def test_shared_kmi_contract_is_authority_gated_lazy_raw_only_and_inert(
    monkeypatch,
    tmp_path,
):
    from storm_tracker_worker import acquisition_activation as activation
    from storm_tracker_worker import acquisition_capture_recovery as shared_recovery
    from storm_tracker_worker import acquisition_handler_child as child
    from storm_tracker_worker import acquisition_query_authority as authority
    from storm_tracker_worker import terminal_raw_completion_router as terminal

    rule = activation.ACTIVATION_RULES[appv4.WORKLOAD_ID]
    assert rule.executable and rule.raw_capture_service_only
    assert rule.handler_id == appv4.HANDLER_ID
    assert rule.required_roles == (appv4.FORECAST_ROLE, appv4.IMAGE_ROLE)
    assert rule.handler_payload_contract == "public_region_query_v1"
    assert rule.authorization_requirement_ids == (
        appv4.RETENTION_REQUIREMENT_ID,
    )
    assert appv4.WORKLOAD_ID in activation.RAW_CAPTURE_WORKLOAD_IDS
    assert appv4.WORKLOAD_ID not in activation.DEFAULT_SHADOW_WORKLOAD_IDS

    spec = child._HANDLERS[appv4.HANDLER_ID]  # noqa: SLF001
    binding = shared_recovery.RECOVERY_BINDINGS[appv4.HANDLER_ID]
    assert child._recovery_binding(spec) == binding  # noqa: SLF001
    assert spec.safe_native_identity and spec.execution_guard_required
    assert spec.decode_status == "not_attempted"
    assert spec.capture_state == "captured_not_decoded"

    observed = {}

    def fake_capture(job, *, raw_store):
        observed["job_id"] = job.job_id
        observed["payload"] = job.payload
        observed["store"] = raw_store
        return {"technical": "only"}

    monkeypatch.setattr(appv4, "capture_kmi_appv4_visual_raw", fake_capture)
    store = RawArtifactStore(tmp_path / "child-raw")
    store.initialize()
    origin_job_id = "acquisition:" + "a" * 64
    result = child._run_native(  # noqa: SLF001
        spec,
        job_id=origin_job_id,
        store=store,
        handler_payload=_payload(),
    )
    assert result == {"technical": "only"}
    assert observed == {
        "job_id": authority.safe_native_job_id(origin_job_id),
        "payload": _payload(),
        "store": store,
    }

    terminal_binding = terminal.TERMINAL_RAW_CONSUMER_CATALOG[appv4.WORKLOAD_ID]
    assert terminal_binding.consumer_kind == "raw_manifest_audit"
    assert terminal_binding.implementation_id == "terminal_raw_reference_audit_v1"
    assert terminal_binding.as_dict()["meteorological_admission"] is False
    assert terminal_binding.as_dict()["runtime_wiring"] == "inert"


def test_frame_selection_is_bounded_timezone_aware_and_deterministic(tmp_path):
    forecast = json.dumps({
        "animation": {
            "sequence": [
                {"time": "not-a-time", "uri": "https://attacker.test/x"},
                {"time": "2026-08-14T11:50:00+00:00", "uri": FUTURE_URI},
                {"time": PAST_TIME, "uri": PAST_URI},
                {"time": "2026-08-14T12:10:00+00:00", "uri": FUTURE_URI},
            ]
        }
    }, separators=(",", ":")).encode()
    store, opener, result, _forecast, _image = _run_success(
        tmp_path, forecast=forecast
    )

    assert opener.calls[1] == PAST_URI
    assert result["product_timestamp"] == PAST_TIMESTAMP
    assert store.stats()["capture_count"] == 1


def test_region_payload_is_closed_and_coverage_bounded(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    no_network = lambda *_args, **_kwargs: pytest.fail("unexpected network")

    for bad_payload in (
        {},
        {**_payload(), "credential": "forbidden"},
        {**_payload(), "query_key": "unsafe"},
        {**_payload(), "latitude": 20.0},
        {**_payload(), "longitude": 30.0},
    ):
        with pytest.raises(appv4.KmiAppv4ContractError):
            appv4.capture_kmi_appv4_visual_raw(
                SimpleNamespace(job_id=SECOND_JOB_ID, payload=bad_payload),
                raw_store=store,
                opener=no_network,
            )

    # Payload rejection occurs before the raw store is even initialized.
    assert not (tmp_path / "raw").exists()


def test_drop_in_handler_and_recovery_metadata_are_copy_only_and_bounded():
    handlers = appv4.kmi_appv4_child_handler_contracts()
    handler = handlers[appv4.WORKLOAD_ID]
    recovery = appv4.kmi_appv4_recovery_binding_metadata()[appv4.HANDLER_ID]

    assert handler["handler_id"] == appv4.HANDLER_ID
    assert handler["capture_product"] == appv4.CAPTURE_PRODUCT
    assert handler["payload_contract"] == "public_region_query_v1"
    assert handler["safe_native_identity"] is True
    assert handler["maximum_artifacts"] == 2
    assert handler["maximum_transport_bytes"] == 12 * 1024 * 1024
    assert handler["required_roles"] == [
        appv4.FORECAST_ROLE,
        appv4.IMAGE_ROLE,
    ]
    assert handler["required_requirement_ids"] == [
        appv4.RETENTION_REQUIREMENT_ID
    ]
    assert appv4.RETENTION_REQUIREMENT_ID == (
        "worker_authority.kmi_appv4_raw_retention_v1"
    )
    assert recovery["job_identity_kind"] == "safe_native_alias_v1"
    assert [member["role"] for member in recovery["members"]] == [
        appv4.FORECAST_ROLE,
        appv4.IMAGE_ROLE,
    ]
    assert sum(
        member["maximum_transport_bytes"] for member in recovery["members"]
    ) == recovery["maximum_transport_bytes"]

    handler["required_roles"].clear()
    assert appv4.kmi_appv4_child_handler_contracts()[appv4.WORKLOAD_ID][
        "required_roles"
    ] == [appv4.FORECAST_ROLE, appv4.IMAGE_ROLE]

    serialized = json.dumps({"handler": handler, "recovery": recovery})
    assert "r9EnW374jkJ9acc" not in serialized
    assert "getForecasts&k=" not in serialized
    assert "latitude" not in serialized
    assert "longitude" not in serialized


def test_module_import_graph_is_light_and_raw_only():
    source_path = Path(appv4.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint({
        "PIL",
        "aiohttp",
        "homeassistant",
        "numpy",
        "paho",
        "pyproj",
    })
    assert "provider_radar" not in source_path.read_text(encoding="utf-8")


def test_missing_or_unexpected_media_type_does_not_create_semantic_admission(tmp_path):
    forecast = _forecast_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    opener = _Opener([
        _Response(forecast, content_type="text/html"),
        _Response(b"not-png", content_type=None),
    ])
    result = appv4.capture_kmi_appv4_visual_raw(
        _job(),
        raw_store=store,
        opener=opener,
        _wall_clock=_Clock(
            REQUEST_AT, FORECAST_RECEIVED_AT, IMAGE_RECEIVED_AT
        ),
    )
    _capture, receipts, _payloads = _bundle(store, result)

    assert receipts[appv4.FORECAST_ROLE].provenance["transport"][
        "reported_media_type"
    ] == "text/html"
    assert receipts[appv4.IMAGE_ROLE].provenance["transport"][
        "reported_media_type"
    ] == "unknown"
    assert result["semantic_status"] == "unverified_visualisation_bytes"
    assert result["operational_admission_eligible"] is False
