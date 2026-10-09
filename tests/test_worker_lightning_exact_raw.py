"""Exact-byte RAW contracts for EUMETSAT MTG LI and NOAA GOES GLM."""
from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, quote, urlsplit

import pytest

from storm_tracker_worker import lightning_exact_raw as raw
from storm_tracker_worker.raw_artifacts import (
    RawArtifactConflictError,
    RawArtifactLimits,
    RawArtifactStore,
)


JOB_ID = (
    "acq:aaaaaaaa:z:bbbbbbbb:z:cccccccc:z:dddddddd:z:eeeeeeee:"
    "z:ffffffff:z:gggg"
)
NOW = datetime(2026, 8, 14, 12, 34, tzinfo=timezone.utc)
TOKEN_TEXT = "ephemeral-token-material-0123456789"


class _Headers:
    def __init__(self, entries):
        self._entries = list(entries)

    def items(self):
        return list(self._entries)


class _Response:
    def __init__(
        self,
        payload: bytes,
        url: str,
        *,
        status: int = 200,
        headers=None,
        final_url: str | None = None,
    ) -> None:
        self._stream = io.BytesIO(payload)
        self.status = status
        self.headers = _Headers(headers or (
            ("Content-Length", str(len(payload))),
            ("Content-Type", "application/octet-stream"),
        ))
        self._url = final_url or url
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def geturl(self) -> str:
        return self._url

    def close(self) -> None:
        self.closed = True


class _FragmentingRead1Response:
    """Stream whose read1 exposes one transport fragment per call."""

    def __init__(self, payload: bytes) -> None:
        self._stream = io.BytesIO(payload)
        self.read_calls = 0
        self.read1_calls = 0

    def read(self, size: int = -1) -> bytes:
        self.read_calls += 1
        return self._stream.read(size)

    def read1(self, size: int = -1) -> bytes:
        self.read1_calls += 1
        return self._stream.read(min(size, 1))


class _Router:
    def __init__(self) -> None:
        self.routes: dict[str, list[dict]] = {}
        self.calls: list[dict] = []

    def add(
        self,
        url: str,
        payload: bytes,
        *,
        status: int = 200,
        headers=None,
        final_url: str | None = None,
    ) -> None:
        self.routes.setdefault(url, []).append({
            "payload": payload,
            "status": status,
            "headers": headers,
            "final_url": final_url,
        })

    def __call__(self, request, *, timeout: float):
        url = request.full_url
        self.calls.append({
            "body": request.data,
            "headers": dict(request.header_items()),
            "method": request.get_method(),
            "timeout": timeout,
            "url": url,
        })
        candidates = self.routes.get(url) or []
        if not candidates:
            raise AssertionError(f"unexpected request: {url}")
        route = candidates.pop(0)
        return _Response(
            route["payload"],
            url,
            status=route["status"],
            headers=route["headers"],
            final_url=route["final_url"],
        )


def _job(payload=None, *, job_id: str = JOB_ID):
    return SimpleNamespace(job_id=job_id, payload={} if payload is None else payload)


def test_body_reader_prefers_filled_read_over_fragmenting_read1():
    payload = b"x" * 257
    response = _FragmentingRead1Response(payload)

    assert raw._read_body(
        response,
        declared_length=None,
        maximum=len(payload),
        deadline=time.monotonic() + 1.0,
    ) == payload
    assert response.read_calls == 2
    assert response.read1_calls == 0


def _store(tmp_path: Path) -> RawArtifactStore:
    store = RawArtifactStore(
        tmp_path / "raw",
        limits=RawArtifactLimits(
            max_object_bytes=16 * 1024 * 1024,
            max_total_bytes=512 * 1024 * 1024,
            max_capture_members=32,
        ),
    )
    store.initialize()
    return store


def _eumetsat_url(title: str = "MTG-LI-CHK-BODY.nc") -> str:
    return (
        "https://api.eumetsat.int/data/download/1.0.0/collections/"
        "EO%3AEUM%3ADAT%3A0691/products/product-1/entry?name="
        + quote(title, safe="._-")
    )


def _eumetsat_search(
    *,
    href: str | None = None,
    title: str = "MTG-LI-CHK-BODY.nc",
) -> bytes:
    return json.dumps({
        "features": [{
            "id": "LI-PRODUCT-20260814T1230",
            "properties": {
                "date": "2026-08-14T12:20:00Z/2026-08-14T12:30:00Z",
                "links": {
                    "sip-entries": [{
                        "href": href or _eumetsat_url(title),
                        "title": title,
                    }],
                },
            },
        }],
    }, separators=(",", ":")).encode("utf-8")


def _eumetsat_router(search_payload: bytes, body_payload: bytes) -> _Router:
    router = _Router()
    search_url = raw._eumetsat_search_binding().url
    body_url = _eumetsat_url()
    router.add(
        search_url,
        search_payload,
        headers=(
            ("Content-Type", "application/geo+json"),
            ("Content-Length", str(len(search_payload))),
        ),
    )
    router.add(
        body_url,
        body_payload,
        headers=(
            ("Content-Type", "application/x-netcdf"),
            ("Content-Length", str(len(body_payload))),
        ),
    )
    return router


def _goes_stamp(moment: datetime) -> str:
    return moment.strftime("%Y%j%H%M%S") + "0"


def _goes_key(satellite: int, moment: datetime) -> str:
    start = _goes_stamp(moment)
    end = _goes_stamp(moment + timedelta(seconds=20))
    created = _goes_stamp(moment + timedelta(seconds=40))
    prefix = raw._goes_prefix(moment)
    return (
        f"{prefix}OR_GLM-L2-LCFA_G{satellite}_s{start}_e{end}_c{created}.nc"
    )


def _goes_listing(satellite: int, prefix: str, keys: list[str]) -> bytes:
    contents = "".join(
        "<Contents><Key>" + key + "</Key><Size>17</Size></Contents>"
        for key in keys
    )
    return (
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Name>noaa-goes{satellite}</Name><Prefix>{prefix}</Prefix>"
        f"<KeyCount>{len(keys)}</KeyCount><MaxKeys>1000</MaxKeys>"
        f"<IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
    ).encode("ascii")


def _goes_case(
    satellite: int = 18,
    *,
    malformed_object_at: int | None = None,
    count: int = 18,
):
    current_hour = NOW.replace(minute=0, second=0, microsecond=0)
    previous_hour = current_hour - timedelta(hours=1)
    previous = [
        _goes_key(satellite, previous_hour + timedelta(minutes=57 + index))
        for index in range(3)
    ]
    current = [
        _goes_key(satellite, current_hour + timedelta(minutes=index))
        for index in range(max(0, count - len(previous)))
    ]
    keys = previous + current
    selected = sorted(keys)[-raw.GOES_MAX_SELECTED_OBJECTS:]
    router = _Router()
    listing_payloads = {}
    for role, prefix, members in (
        ("listing_previous", raw._goes_prefix(previous_hour), previous),
        ("listing_current", raw._goes_prefix(current_hour), current),
    ):
        binding = raw._goes_listing_binding(satellite, prefix)
        payload = _goes_listing(satellite, prefix, members)
        listing_payloads[role] = payload
        router.add(
            binding.url,
            payload,
            headers=(
                ("Content-Length", str(len(payload))),
                ("Content-Type", "application/xml"),
            ),
        )
    object_payloads = {}
    for index, key in enumerate(selected):
        payload = (
            b"not-a-netcdf-but-still-exact"
            if malformed_object_at == index
            else b"exact-glm-native:" + key.encode("ascii")
        )
        object_payloads[key] = payload
        binding = raw._goes_object_binding(satellite, key)
        router.add(
            binding.url,
            payload,
            headers=(
                ("Content-Type", "application/x-netcdf"),
                ("Content-Length", str(len(payload))),
            ),
        )
    return router, listing_payloads, object_payloads, selected


def _receipt_bytes(store: RawArtifactStore, result: dict, role: str) -> bytes:
    receipt_id = result["raw_provenance"]["sources"][role]["receipt_id"]
    loaded = store.load_receipt_bytes(receipt_id)
    assert loaded is not None
    return loaded[1]


def test_handler_and_dynamic_recovery_metadata_are_drop_in_and_secret_free():
    child = raw.lightning_child_handler_contracts()
    recovery = raw.lightning_recovery_binding_metadata()
    oauth = raw.eumetsat_oauth_broker_contract()

    assert set(child) == {
        raw.EUMETSAT_WORKLOAD_ID,
        raw.GOES18_WORKLOAD_ID,
        raw.GOES19_WORKLOAD_ID,
    }
    assert child[raw.EUMETSAT_WORKLOAD_ID]["maximum_artifacts"] == 2
    for workload in (raw.GOES18_WORKLOAD_ID, raw.GOES19_WORKLOAD_ID):
        assert child[workload]["maximum_artifacts"] == 14
        assert child[workload]["dynamic_role_minimum"] == 1
        assert child[workload]["dynamic_role_maximum"] == 12
        assert child[workload]["meteorological_parse"] is False
    dynamic = recovery[raw.GOES18_HANDLER_ID]["members"][-1]
    assert dynamic["role"] is None
    assert dynamic["role_pattern"] == r"^netcdf_(?P<index>0[0-9]|1[01])$"
    assert dynamic["minimum_count"] == 1
    assert dynamic["maximum_count"] == 12
    assert oauth == {
        "child_ephemeral_env": raw.EUMETSAT_EPHEMERAL_TOKEN_ENV,
        "child_job_payload_secret_fields": [],
        "client_credentials_owner": "worker_parent_only",
        "consumer_key_secret_ref": "worker_secret.eumetsat_consumer_key",
        "consumer_secret_secret_ref": "worker_secret.eumetsat_consumer_secret",
        "contract": "stv3-eumetsat-parent-oauth-broker-v1",
        "ephemeral_env_must_be_removed_after_execution": True,
        "oauth_response_persisted": False,
        "result_contains_authentication_material": False,
    }
    serialized = json.dumps({"child": child, "recovery": recovery, "oauth": oauth})
    assert TOKEN_TEXT not in serialized


def test_eumetsat_exact_search_and_malformed_netcdf_are_one_atomic_capture(
    tmp_path: Path,
):
    search = _eumetsat_search()
    body = b"malformed-netcdf-is-still-upstream-native"
    router = _eumetsat_router(search, body)
    store = _store(tmp_path)
    token = raw.EphemeralBearerToken(TOKEN_TEXT)

    result = raw.capture_eumetsat_mtg_li_raw(
        _job(),
        raw_store=store,
        access_token_supplier=lambda: token,
        opener=router,
    )

    assert token.closed is True
    assert result["decode_status"] == "not_decoded"
    assert result["meteorological_parse_attempted"] is False
    assert result["semantic_status"] == "unverified"
    assert result["raw_provenance"]["raw_member_count"] == 2
    assert _receipt_bytes(store, result, "search_response") == search
    assert _receipt_bytes(store, result, "netcdf4") == body
    assert len(store.accounting_captures()) == 1
    body_call = router.calls[-1]
    assert body_call["headers"]["Authorization"] == "Bearer " + TOKEN_TEXT
    serialized_result = json.dumps(result, sort_keys=True)
    assert TOKEN_TEXT not in serialized_result
    assert _eumetsat_url() not in serialized_result
    for receipt in store.accounting_receipts():
        serialized = json.dumps(receipt.provenance, sort_keys=True)
        assert TOKEN_TEXT not in serialized
        assert _eumetsat_url() not in serialized
        assert receipt.content_type == "application/octet-stream"


def test_eumetsat_malformed_search_is_durable_before_parse_and_no_token_called(
    tmp_path: Path,
):
    payload = b"{definitely-not-json"
    router = _Router()
    router.add(raw._eumetsat_search_binding().url, payload)
    store = _store(tmp_path)
    supplier_calls = []

    with pytest.raises(raw.LightningRawContractError):
        raw.capture_eumetsat_mtg_li_raw(
            _job(),
            raw_store=store,
            access_token_supplier=lambda: supplier_calls.append(True),
            opener=router,
        )

    receipts = store.accounting_receipts()
    assert len(receipts) == 1
    assert store.load_receipt_bytes(receipts[0].receipt_id)[1] == payload
    assert store.accounting_captures() == ()
    assert supplier_calls == []


def test_eumetsat_partial_capture_has_no_manifest_then_resumes_without_network(
    tmp_path: Path,
):
    search = _eumetsat_search()
    body = b"exact-original-body"
    router = _eumetsat_router(search, body)
    store = _store(tmp_path)
    first_token = raw.EphemeralBearerToken(TOKEN_TEXT)

    def fault(point: str) -> None:
        if point == "eumetsat.after_netcdf_store":
            raise RuntimeError("injected crash before atomic manifest")

    with pytest.raises(RuntimeError, match="injected crash"):
        raw.capture_eumetsat_mtg_li_raw(
            _job(),
            raw_store=store,
            access_token_supplier=lambda: first_token,
            opener=router,
            _fault_injector=fault,
        )
    assert first_token.closed is True
    assert len(store.accounting_receipts()) == 2
    assert store.accounting_captures() == ()

    result = raw.capture_eumetsat_mtg_li_raw(
        _job(),
        raw_store=store,
        access_token_supplier=lambda: pytest.fail("token must not be requested"),
        opener=lambda *_args, **_kwargs: pytest.fail("network must not be used"),
    )
    assert result["raw_provenance"]["raw_member_count"] == 2
    first_capture = result["raw_provenance"]["raw_capture_id"]
    repeated = raw.capture_eumetsat_mtg_li_raw(
        _job(),
        raw_store=store,
        opener=lambda *_args, **_kwargs: pytest.fail("idempotent retry is local"),
    )
    assert repeated["raw_provenance"]["raw_capture_id"] == first_capture
    assert len(store.accounting_captures()) == 1


def test_eumetsat_url_authority_rejects_foreign_or_secret_link_after_raw_store(
    tmp_path: Path,
):
    for index, href in enumerate((
        "https://evil.invalid/data/download/file.nc",
        (
            "https://api.eumetsat.int/data/download/1.0.0/file.nc"
            "?access_token=secret"
        ),
    )):
        store = _store(tmp_path / str(index))
        search = _eumetsat_search(href=href)
        router = _Router()
        router.add(raw._eumetsat_search_binding().url, search)
        with pytest.raises(raw.LightningRawContractError):
            raw.capture_eumetsat_mtg_li_raw(
                _job(job_id=JOB_ID[:-4] + ("hhhh" if index == 0 else "iiii")),
                raw_store=store,
                access_token_supplier=lambda: pytest.fail("no token"),
                opener=router,
            )
        receipts = store.accounting_receipts()
        assert len(receipts) == 1
        assert store.load_receipt_bytes(receipts[0].receipt_id)[1] == search
        assert store.accounting_captures() == ()


def test_parent_oauth_broker_is_bounded_redacted_and_zeroes_owned_token():
    consumer_key = "consumer-user"
    consumer_secret = "parent-only-client-secret"
    response = json.dumps({
        "access_token": TOKEN_TEXT,
        "expires_in": 3600,
        "token_type": "bearer",
    }).encode("utf-8")
    router = _Router()
    token_url = "https://api.eumetsat.int/token"
    router.add(token_url, response, headers=(
        ("Content-Length", str(len(response))),
        ("Content-Type", "application/json"),
    ))

    token = raw.mint_eumetsat_access_token_parent(
        consumer_key, consumer_secret, opener=router
    )

    assert repr(token) == "EphemeralBearerToken(<redacted>)"
    assert TOKEN_TEXT not in repr(token)
    assert token.authorization_value() == "Bearer " + TOKEN_TEXT
    call = router.calls[0]
    expected = base64.b64encode(
        f"{consumer_key}:{consumer_secret}".encode("utf-8")
    ).decode("ascii")
    assert call["headers"]["Authorization"] == "Basic " + expected
    assert call["body"] == b"grant_type=client_credentials&validity_period=3600"
    token.close()
    assert token.closed is True
    with pytest.raises(raw.EumetsatOAuthError):
        token.authorization_value()


def test_job_payload_rejects_every_secret_before_transport(tmp_path: Path):
    store = _store(tmp_path)
    for field in (
        "consumer_key",
        "consumer_secret",
        "access_token",
        "authorization",
        "credential_ref",
    ):
        with pytest.raises(raw.LightningRawContractError):
            raw.capture_eumetsat_mtg_li_raw(
                _job({field: "secret"}),
                raw_store=store,
                access_token_supplier=lambda: pytest.fail("no token"),
                opener=lambda *_args, **_kwargs: pytest.fail("no network"),
            )
    assert store.accounting_receipts() == ()


def test_goes_two_exact_listings_and_deterministic_max_twelve_dynamic_members(
    tmp_path: Path,
):
    router, listings, objects, selected = _goes_case(18, count=18)
    store = _store(tmp_path)

    result = raw.capture_noaa_goes18_glm_raw(
        _job(), raw_store=store, now=NOW, opener=router
    )

    sources = result["raw_provenance"]["sources"]
    assert set(sources) == {
        "listing_previous",
        "listing_current",
        *(f"netcdf_{index:02d}" for index in range(12)),
    }
    assert result["raw_provenance"]["raw_member_count"] == 14
    assert _receipt_bytes(store, result, "listing_previous") == listings[
        "listing_previous"
    ]
    assert _receipt_bytes(store, result, "listing_current") == listings[
        "listing_current"
    ]
    for index, key in enumerate(selected):
        role = f"netcdf_{index:02d}"
        assert _receipt_bytes(store, result, role) == objects[key]
        receipt = store.load_receipt(sources[role]["receipt_id"])
        assert receipt.provenance["object_key_sha256"] == hashlib.sha256(
            key.encode("ascii")
        ).hexdigest()
    assert len(router.calls) == 14
    assert all("Authorization" not in call["headers"] for call in router.calls)
    assert result["meteorological_parse_attempted"] is False
    assert len(store.accounting_captures()) == 1


@pytest.mark.parametrize("satellite", [18, 19])
def test_goes_malformed_object_bytes_are_still_captured_without_decode(
    tmp_path: Path, satellite: int
):
    router, _listings, objects, selected = _goes_case(
        satellite, malformed_object_at=0, count=4
    )
    store = _store(tmp_path / str(satellite))

    result = raw.capture_noaa_goes_glm_raw(
        _job(job_id=JOB_ID[:-4] + ("hhaa" if satellite == 18 else "hhbb")),
        satellite=satellite,
        raw_store=store,
        now=NOW,
        opener=router,
    )

    assert _receipt_bytes(store, result, "netcdf_00") == objects[selected[0]]
    assert objects[selected[0]] == b"not-a-netcdf-but-still-exact"
    assert result["decode_status"] == "not_decoded"
    assert result["semantic_status"] == "unverified"


def test_goes_malformed_both_listings_are_durable_before_any_parse(
    tmp_path: Path,
):
    previous = raw._goes_prefix(NOW.replace(minute=0) - timedelta(hours=1))
    current = raw._goes_prefix(NOW.replace(minute=0))
    router = _Router()
    first = b"not xml one"
    second = b"not xml two"
    router.add(raw._goes_listing_binding(18, previous).url, first)
    router.add(raw._goes_listing_binding(18, current).url, second)
    store = _store(tmp_path)

    with pytest.raises(raw.LightningRawContractError):
        raw.capture_noaa_goes18_glm_raw(
            _job(), raw_store=store, now=NOW, opener=router
        )

    receipts = {item.provenance["role"]: item for item in store.accounting_receipts()}
    assert set(receipts) == {"listing_previous", "listing_current"}
    assert store.load_receipt_bytes(receipts["listing_previous"].receipt_id)[1] == first
    assert store.load_receipt_bytes(receipts["listing_current"].receipt_id)[1] == second
    assert store.accounting_captures() == ()
    assert len(router.calls) == 2


def test_goes_partial_dynamic_capture_has_no_manifest_then_resumes_exactly(
    tmp_path: Path,
):
    router, _listings, _objects, selected = _goes_case(19, count=7)
    store = _store(tmp_path)
    job = _job(job_id=JOB_ID[:-4] + "hhcc")

    def fault(point: str) -> None:
        if point == "goes.after_netcdf_00_store":
            raise RuntimeError("injected object-series crash")

    with pytest.raises(RuntimeError, match="object-series crash"):
        raw.capture_noaa_goes19_glm_raw(
            job,
            raw_store=store,
            now=NOW,
            opener=router,
            _fault_injector=fault,
        )
    assert len(store.accounting_receipts()) == 3
    assert store.accounting_captures() == ()

    result = raw.capture_noaa_goes19_glm_raw(
        job, raw_store=store, now=NOW + timedelta(hours=3), opener=router
    )
    assert result["raw_provenance"]["raw_member_count"] == 2 + len(selected)
    assert len(store.accounting_captures()) == 1
    calls_after_resume = len(router.calls)
    repeated = raw.capture_noaa_goes19_glm_raw(
        job,
        raw_store=store,
        now=NOW + timedelta(days=2),
        opener=lambda *_args, **_kwargs: pytest.fail("complete retry is local"),
    )
    assert repeated["raw_provenance"]["raw_capture_id"] == result[
        "raw_provenance"
    ]["raw_capture_id"]
    assert len(router.calls) == calls_after_resume


@pytest.mark.parametrize(
    ("headers", "final_url_suffix", "error"),
    [
        (
            (("Content-Length", "4"), ("Content-Encoding", "gzip")),
            "",
            raw.LightningRawContractError,
        ),
        (
            (("Content-Length", "4"),),
            "&redirected=1",
            raw.LightningRawContractError,
        ),
        (
            (("Content-Length", str(raw.EUMETSAT_SEARCH_MAX_BYTES + 1)),),
            "",
            raw.LightningRawCapacityError,
        ),
    ],
)
def test_transport_rejects_compression_redirect_and_declared_oversize_before_store(
    tmp_path: Path, headers, final_url_suffix: str, error
):
    binding = raw._eumetsat_search_binding()
    router = _Router()
    router.add(
        binding.url,
        b"data",
        headers=headers,
        final_url=(binding.url + final_url_suffix) if final_url_suffix else None,
    )
    store = _store(tmp_path)

    with pytest.raises(error):
        raw.capture_eumetsat_mtg_li_raw(
            _job(), raw_store=store, opener=router
        )
    assert store.accounting_receipts() == ()
    assert store.accounting_captures() == ()


def test_transport_rejects_duplicate_framing_and_oversized_header(tmp_path: Path):
    binding = raw._eumetsat_search_binding()
    for index, headers in enumerate((
        (("Content-Length", "2"), ("Content-Length", "2")),
        (("X-Evidence", "x" * (raw.MAX_HEADER_CHARS + 1)),),
    )):
        router = _Router()
        router.add(binding.url, b"{}", headers=headers)
        store = _store(tmp_path / str(index))
        with pytest.raises((raw.LightningRawContractError, raw.LightningRawCapacityError)):
            raw.capture_eumetsat_mtg_li_raw(
                _job(job_id=JOB_ID[:-4] + ("jjjj" if index == 0 else "kkkk")),
                raw_store=store,
                opener=router,
            )
        assert store.accounting_receipts() == ()


def test_truncated_goes_listing_is_durable_but_never_authorizes_members(
    tmp_path: Path,
):
    previous_hour = NOW.replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=1
    )
    current_hour = previous_hour + timedelta(hours=1)
    previous_prefix = raw._goes_prefix(previous_hour)
    current_prefix = raw._goes_prefix(current_hour)
    key = _goes_key(18, current_hour)
    first = _goes_listing(18, previous_prefix, [])
    second = _goes_listing(18, current_prefix, [key]).replace(
        b"<IsTruncated>false</IsTruncated>",
        b"<IsTruncated>true</IsTruncated>",
    )
    router = _Router()
    router.add(raw._goes_listing_binding(18, previous_prefix).url, first)
    router.add(raw._goes_listing_binding(18, current_prefix).url, second)
    store = _store(tmp_path)

    with pytest.raises(raw.LightningRawContractError):
        raw.capture_noaa_goes18_glm_raw(
            _job(), raw_store=store, now=NOW, opener=router
        )
    assert len(store.accounting_receipts()) == 2
    assert store.accounting_captures() == ()


def test_preexisting_wrong_role_receipt_is_visible_corruption_not_overwrite(
    tmp_path: Path,
):
    store = _store(tmp_path)
    store.initialize()
    store.store(
        provider=raw.EUMETSAT_PROVIDER,
        product=raw.EUMETSAT_SEARCH_PRODUCT,
        fetch_id=f"{JOB_ID}:search_response",
        content_type="application/octet-stream",
        received_at=1.0,
        upstream_id="preexisting-conflicting-upstream",
        provenance={
            "capture_stage": "before_decode",
            "collector": "untrusted-preexisting-writer",
            "collector_version": "wrong-v1",
            "representation": "upstream_native_bytes",
        },
        payload=b"conflicting bytes",
    )

    with pytest.raises(raw.LightningRawCorruptionError):
        raw.capture_eumetsat_mtg_li_raw(
            _job(),
            raw_store=store,
            opener=lambda *_args, **_kwargs: pytest.fail("must inspect durable conflict"),
        )
    receipt = store.accounting_receipts()[0]
    assert store.load_receipt_bytes(receipt.receipt_id)[1] == b"conflicting bytes"
    with pytest.raises(RawArtifactConflictError):
        store.store(
            provider=raw.EUMETSAT_PROVIDER,
            product=raw.EUMETSAT_SEARCH_PRODUCT,
            fetch_id=f"{JOB_ID}:search_response",
            content_type="application/octet-stream",
            received_at=2.0,
            upstream_id="other",
            provenance={
                "capture_stage": "before_decode",
                "collector": "untrusted-preexisting-writer",
                "collector_version": "wrong-v1",
                "representation": "upstream_native_bytes",
            },
            payload=b"replacement forbidden",
        )


def test_empty_successful_eumetsat_search_body_is_durable_not_dry(tmp_path: Path):
    router = _Router()
    router.add(
        raw._eumetsat_search_binding().url,
        b"",
        status=204,
        headers=(("Content-Length", "0"),),
    )
    store = _store(tmp_path)

    with pytest.raises(raw.LightningRawContractError):
        raw.capture_eumetsat_mtg_li_raw(_job(), raw_store=store, opener=router)
    receipt = store.accounting_receipts()[0]
    assert receipt.size_bytes == 0
    assert store.load_receipt_bytes(receipt.receipt_id)[1] == b""
    assert receipt.provenance["response_status"] == 204
    assert receipt.provenance["weather_state"] == "unknown"
    assert store.accounting_captures() == ()
