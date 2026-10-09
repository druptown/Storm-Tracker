from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from storm_tracker_worker import aemet_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactIntegrityError, RawArtifactStore


ROOT = Path(__file__).resolve().parents[1]
KEY = "aemet-application-key-0123456789"
DATA_URL = "https://opendata.aemet.es/opendata/sh/dataSelectionToken001"
METADATA_URL = "https://opendata.aemet.es/opendata/sh/metaSelectionToken001"
ENVELOPE = json.dumps(
    {
        "descripcion": "exito",
        "estado": 200,
        "datos": DATA_URL,
        "metadatos": METADATA_URL,
    },
    separators=(",", ":"),
).encode("utf-8")
DATA = b"II*\x00AEMET-EXACT-GEOTIFF-BYTES"
METADATA = b'{"format":"GeoTIFF","scale":"ESCALA"}'


def _job(job_id: str = "aemet-native-job", payload: dict | None = None):
    return SimpleNamespace(job_id=job_id, payload={} if payload is None else payload)


def _response(payload: bytes, content_type: str, received_at: float):
    return native.AemetHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
        content_length=len(payload),
        etag='"safe-etag"',
        last_modified="Sun, 02 Aug 2026 12:00:00 GMT",
    )


def _install(monkeypatch, *, envelope: bytes = ENVELOPE):
    calls = {role: 0 for role in native.REQUIRED_ROLES}
    monkeypatch.setattr(native, "_api_key_from_environment", lambda: KEY)

    def fetch_envelope(actual_key, *, deadline):
        assert actual_key == KEY
        assert deadline > time.monotonic()
        calls[native.REQUIRED_ROLES[0]] += 1
        return _response(envelope, "application/json", 1_785_700_001.0)

    def fetch_download(url, role, *, deadline):
        assert deadline > time.monotonic()
        calls[role] += 1
        if role == native.REQUIRED_ROLES[1]:
            assert url == DATA_URL
            return _response(DATA, "image/tiff", 1_785_700_002.0)
        assert role == native.REQUIRED_ROLES[2]
        assert url == METADATA_URL
        return _response(METADATA, "application/json", 1_785_700_003.0)

    monkeypatch.setattr(native, "_fetch_envelope", fetch_envelope)
    monkeypatch.setattr(native, "_fetch_download", fetch_download)
    return calls


def _capture(monkeypatch, tmp_path, *, job_id="aemet-native-job"):
    store = RawArtifactStore(tmp_path / "raw")
    calls = _install(monkeypatch)
    result = native.capture_latest_aemet_national_radar(
        _job(job_id), raw_store=store
    )
    return store, calls, result


def test_exact_three_member_capture_is_shadow_only_and_secret_free(monkeypatch, tmp_path):
    store, calls, result = _capture(monkeypatch, tmp_path)
    assert calls == {role: 1 for role in native.REQUIRED_ROLES}
    assert result["weather_state"] == "unknown"
    assert result["capture_state"] == "captured_not_decoded"
    assert result["operational_admission_eligible"] is False
    assert result["quantitative_rain_rate_eligible"] is False
    assert result["pixel_values_read"] is False
    semantics = result["raw_provenance"]["official_product_semantics"]
    assert semantics == {
        "quantity": "lowest_PPI_radar_reflectivity",
        "unit": "dBZ",
        "container": "GeoTIFF",
        "crs": "EPSG:4326",
        "per_file_ESCALA_required": True,
        "provenance_only": True,
    }

    capture_id = result["raw_provenance"]["raw_capture_id"]
    capture, receipts, payloads = store.load_capture_bundle_bytes(capture_id)
    assert tuple(sorted(receipts)) == tuple(sorted(native.REQUIRED_ROLES))
    assert payloads == {
        native.REQUIRED_ROLES[0]: ENVELOPE,
        native.REQUIRED_ROLES[1]: DATA,
        native.REQUIRED_ROLES[2]: METADATA,
    }
    assert capture.complete is True
    exported = json.dumps(result, sort_keys=True) + json.dumps(
        {role: receipt.to_metadata() for role, receipt in receipts.items()},
        sort_keys=True,
    )
    assert KEY not in exported
    assert DATA_URL not in exported
    assert METADATA_URL not in exported
    assert "api_key" not in capture.upstream_capture_id


def test_envelope_is_durable_before_first_parse(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch)
    original = native._inspect_envelope

    def inspect(payload):
        receipt_id = native._receipt_id(store, "parse-order-job", native.REQUIRED_ROLES[0])
        loaded = store.load_receipt_bytes(receipt_id)
        assert loaded is not None
        assert loaded[1] == payload == ENVELOPE
        return original(payload)

    monkeypatch.setattr(native, "_inspect_envelope", inspect)
    native.capture_latest_aemet_national_radar(
        _job("parse-order-job"), raw_store=store
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://opendata.aemet.es/opendata/sh/token12345678",
        "https://evil.example/opendata/sh/token12345678",
        "https://user@opendata.aemet.es/opendata/sh/token12345678",
        "https://opendata.aemet.es:443/opendata/sh/token12345678",
        "https://opendata.aemet.es/opendata/sh/token12345678?q=1",
        "https://opendata.aemet.es/opendata/sh/token12345678#fragment",
        "https://opendata.aemet.es/not-sh/token12345678",
        "https://opendata.aemet.es/opendata/sh/x",
    ],
)
def test_download_url_allowlist_fails_closed(url):
    with pytest.raises(native.AemetNativeContractError, match="allowlist"):
        native._validate_download_url(url)


@pytest.mark.parametrize(
    "mutation",
    [
        {"estado": 201},
        {"descripcion": ""},
        {"extra": True},
        {"datos": METADATA_URL},
        {"metadatos": "https://evil.example/opendata/sh/token12345678"},
    ],
)
def test_envelope_contract_drift_is_rejected_after_raw_store(monkeypatch, tmp_path, mutation):
    root = json.loads(ENVELOPE)
    root.update(mutation)
    payload = json.dumps(root, separators=(",", ":")).encode()
    store = RawArtifactStore(tmp_path / "raw")
    calls = _install(monkeypatch, envelope=payload)
    with pytest.raises(native.AemetNativeContractError):
        native.capture_latest_aemet_national_radar(_job(), raw_store=store)
    assert calls[native.REQUIRED_ROLES[0]] == 1
    loaded = store.load_receipt_bytes(
        native._receipt_id(store, "aemet-native-job", native.REQUIRED_ROLES[0])
    )
    assert loaded is not None and loaded[1] == payload
    assert calls[native.REQUIRED_ROLES[1]] == 0


def test_duplicate_envelope_key_is_rejected(monkeypatch, tmp_path):
    duplicate = (
        b'{"descripcion":"exito","estado":200,"estado":200,'
        b'"datos":"' + DATA_URL.encode() + b'","metadatos":"'
        + METADATA_URL.encode() + b'"}'
    )
    _install(monkeypatch, envelope=duplicate)
    with pytest.raises(native.AemetNativeContractError, match="dubbele"):
        native.capture_latest_aemet_national_radar(
            _job(), raw_store=RawArtifactStore(tmp_path / "raw")
        )


def test_secret_echo_is_rejected_before_any_raw_write(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    monkeypatch.setattr(native, "_api_key_from_environment", lambda: KEY)
    monkeypatch.setattr(
        native,
        "_fetch_envelope",
        lambda *_args, **_kwargs: _response(
            ENVELOPE + KEY.encode(), "application/json", 1_785_700_001.0
        ),
    )
    with pytest.raises(native.AemetNativeContractError, match="credentialmateriaal") as caught:
        native.capture_latest_aemet_national_radar(_job(), raw_store=store)
    assert KEY not in str(caught.value)
    assert store.load_receipt_bytes(
        native._receipt_id(store, "aemet-native-job", native.REQUIRED_ROLES[0])
    ) is None


def test_secret_echo_from_selected_download_is_rejected_before_member_write(
    monkeypatch, tmp_path
):
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch)

    def fetch_download(_url, role, *, deadline):
        assert deadline > time.monotonic()
        payload = KEY.encode() if role == native.REQUIRED_ROLES[1] else METADATA
        media_type = "image/tiff" if role == native.REQUIRED_ROLES[1] else "application/json"
        return _response(payload, media_type, 1_785_700_002.0)

    monkeypatch.setattr(native, "_fetch_download", fetch_download)
    with pytest.raises(native.AemetNativeContractError, match="credentialmateriaal"):
        native.capture_latest_aemet_national_radar(_job(), raw_store=store)
    assert store.load_receipt_bytes(
        native._receipt_id(store, "aemet-native-job", native.REQUIRED_ROLES[1])
    ) is None


def test_job_payload_cannot_carry_key_url_or_any_provider_option(monkeypatch, tmp_path):
    monkeypatch.setattr(
        native,
        "_api_key_from_environment",
        lambda: pytest.fail("credential resolution must not run"),
    )
    for payload in (
        {"api_key": KEY},
        {"url": DATA_URL},
        {"region": "national"},
    ):
        with pytest.raises(native.AemetNativeContractError, match="exact lege"):
            native.capture_latest_aemet_national_radar(
                _job("unsafe-payload", payload),
                raw_store=RawArtifactStore(tmp_path / "raw"),
            )


def test_job_id_cannot_embed_transport_secret(monkeypatch, tmp_path):
    monkeypatch.setattr(native, "_api_key_from_environment", lambda: KEY)
    monkeypatch.setattr(
        native, "_fetch_envelope", lambda *_a, **_k: pytest.fail("network")
    )
    with pytest.raises(native.AemetNativeContractError, match="credentialmateriaal"):
        native.capture_latest_aemet_national_radar(
            _job(f"capture-{KEY}"),
            raw_store=RawArtifactStore(tmp_path / "raw"),
        )


def test_injected_size_and_deadline_are_hard_bounds(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    monkeypatch.setattr(native, "_api_key_from_environment", lambda: KEY)
    too_large = b"x" * (native.MAX_ENVELOPE_BYTES + 1)
    monkeypatch.setattr(
        native,
        "_fetch_envelope",
        lambda *_args, **_kwargs: _response(
            too_large, "application/json", 1_785_700_001.0
        ),
    )
    with pytest.raises(native.AemetNativeContractError, match="bytegrens"):
        native.capture_latest_aemet_national_radar(_job(), raw_store=store)

    with pytest.raises(native.AemetNativeDeadlineError):
        native.capture_latest_aemet_national_radar(
            _job("expired"), raw_store=store, deadline_at=time.monotonic() - 1
        )


@pytest.mark.parametrize(
    ("checkpoint", "expected_calls"),
    [
        ("aemet.after_envelope_store", (0, 1, 1)),
        ("aemet.after_data_store", (0, 0, 1)),
        ("aemet.after_metadata_store", (0, 0, 0)),
        ("aemet.before_capture_commit", (0, 0, 0)),
    ],
)
def test_partial_attempt_resumes_without_refetch(monkeypatch, tmp_path, checkpoint, expected_calls):
    store = RawArtifactStore(tmp_path / "raw")
    _install(monkeypatch)

    class Crash(RuntimeError):
        pass

    def fault(stage):
        if stage == checkpoint:
            raise Crash(stage)

    with pytest.raises(Crash):
        native.capture_latest_aemet_national_radar(
            _job("resume-job"), raw_store=store, _fault_injector=fault
        )
    calls = _install(monkeypatch)
    result = native.capture_latest_aemet_national_radar(
        _job("resume-job"), raw_store=store
    )
    assert tuple(calls[role] for role in native.REQUIRED_ROLES) == expected_calls
    assert result["capture_state"] == "captured_not_decoded"


def _capture_hash_proof(store, result):
    capture_id = result["raw_provenance"]["raw_capture_id"]
    loaded = store.load_capture_bundle_bytes(capture_id)
    assert loaded is not None
    capture, receipts, payloads = loaded
    expected_payloads = {
        native.REQUIRED_ROLES[0]: ENVELOPE,
        native.REQUIRED_ROLES[1]: DATA,
        native.REQUIRED_ROLES[2]: METADATA,
    }
    assert payloads == expected_payloads
    provenance = result["raw_provenance"]
    assert provenance["raw_capture_manifest_sha256"] == capture.manifest_sha256
    assert provenance["raw_capture_receipt_sha256"] == (
        capture.capture_receipt_sha256
    )
    member_proofs = []
    for role in native.REQUIRED_ROLES:
        content_sha256 = hashlib.sha256(payloads[role]).hexdigest()
        assert receipts[role].content_sha256 == content_sha256
        member_proofs.append(
            (
                role,
                receipts[role].receipt_id,
                receipts[role].receipt_sha256,
                content_sha256,
            )
        )
    return {
        "capture_id": capture.capture_id,
        "capture_receipt_sha256": capture.capture_receipt_sha256,
        "manifest_sha256": capture.manifest_sha256,
        "members": tuple(member_proofs),
    }


def _growth_proof(store):
    stats = store.stats()
    return {
        name: stats[name]
        for name in (
            "object_count",
            "receipt_count",
            "capture_count",
            "catalog_bytes",
            "filesystem_bytes",
            "filesystem_object_entries",
            "filesystem_staging_entries",
            "unmanaged_file_count",
        )
    }


def _assert_credential_absent(raw, *, diagnostics, arguments, result=None):
    secret = KEY.encode("utf-8")
    assert secret not in diagnostics
    assert all(KEY not in argument for argument in arguments)
    if result is not None:
        assert KEY not in json.dumps(result, sort_keys=True)
    for path in raw.rglob("*"):
        if path.is_file():
            assert secret not in path.read_bytes(), path


@pytest.mark.parametrize(
    ("checkpoint", "pre_receipts", "pre_captures", "expected_calls"),
    [
        ("aemet.after_envelope_store", 1, 0, (0, 1, 1)),
        ("aemet.after_data_store", 2, 0, (0, 0, 1)),
        ("aemet.after_metadata_store", 3, 0, (0, 0, 0)),
        ("aemet.before_capture_commit", 3, 0, (0, 0, 0)),
        ("aemet.after_capture_commit", 3, 1, (0, 0, 0)),
    ],
)
def test_abrupt_process_crash_is_recoverable(
    monkeypatch,
    tmp_path,
    checkpoint,
    pre_receipts,
    pre_captures,
    expected_calls,
):
    raw = tmp_path / "raw"
    helper = Path(__file__).parent / "helpers" / "worker_capture_crash_child.py"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT)
    arguments = [sys.executable, str(helper), "aemet", str(raw), checkpoint]
    completed = subprocess.run(
        arguments,
        cwd=ROOT,
        env=environment,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)
    diagnostics = completed.stdout + completed.stderr
    _assert_credential_absent(raw, diagnostics=diagnostics, arguments=arguments)

    pre_recovery_store = RawArtifactStore(raw)
    pre_recovery_store.initialize()
    pre_recovery = pre_recovery_store.stats()
    assert pre_recovery["receipt_count"] == pre_receipts
    assert pre_recovery["capture_count"] == pre_captures

    calls = _install(monkeypatch)
    recovered_store = RawArtifactStore(raw)
    result = native.capture_latest_aemet_national_radar(
        _job("aemet-crash-job"), raw_store=recovered_store
    )
    assert result["capture_state"] == "captured_not_decoded"
    assert tuple(calls[role] for role in native.REQUIRED_ROLES) == expected_calls
    first_hashes = _capture_hash_proof(recovered_store, result)
    first_growth = _growth_proof(recovered_store)
    assert first_growth["receipt_count"] == 3
    assert first_growth["capture_count"] == 1

    monkeypatch.setattr(
        native,
        "_api_key_from_environment",
        lambda: pytest.fail("second replay resolved the credential"),
    )
    monkeypatch.setattr(
        native,
        "_fetch_envelope",
        lambda *_args, **_kwargs: pytest.fail("second replay fetched envelope"),
    )
    monkeypatch.setattr(
        native,
        "_fetch_download",
        lambda *_args, **_kwargs: pytest.fail("second replay fetched download"),
    )
    replay_store = RawArtifactStore(raw)
    replayed = native.capture_latest_aemet_national_radar(
        _job("aemet-crash-job"), raw_store=replay_store
    )
    assert replayed == result
    assert _capture_hash_proof(replay_store, replayed) == first_hashes
    assert _growth_proof(replay_store) == first_growth
    integrity = replay_store.integrity_check()
    assert integrity["healthy"] is True, integrity
    assert integrity["checked_objects"] == 3
    assert integrity["checked_receipts"] == 3
    assert integrity["checked_captures"] == 1
    _assert_credential_absent(
        raw,
        diagnostics=diagnostics,
        arguments=arguments,
        result=replayed,
    )


def test_complete_capture_resumes_without_secret_or_network(monkeypatch, tmp_path):
    store, _calls, first = _capture(monkeypatch, tmp_path, job_id="offline-job")
    monkeypatch.setattr(
        native, "_api_key_from_environment", lambda: pytest.fail("secret resolution")
    )
    monkeypatch.setattr(native, "_fetch_envelope", lambda *_a, **_k: pytest.fail("network"))
    monkeypatch.setattr(native, "_fetch_download", lambda *_a, **_k: pytest.fail("network"))
    second = native.capture_latest_aemet_national_radar(
        _job("offline-job"), raw_store=store
    )
    assert second == first


def test_raw_object_mutation_fails_closed_on_resume(monkeypatch, tmp_path):
    store, _calls, result = _capture(monkeypatch, tmp_path, job_id="mutation-job")
    capture, receipts, _payloads = store.load_capture_bundle_bytes(
        result["raw_provenance"]["raw_capture_id"]
    )
    assert capture.complete
    target = Path(receipts[native.REQUIRED_ROLES[1]].artifact_path)
    target.write_bytes(b"mutated")
    with pytest.raises(RawArtifactIntegrityError):
        native.capture_latest_aemet_national_radar(
            _job("mutation-job"), raw_store=store
        )


def test_thread_and_cross_process_locks_obey_deadline(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    assert native._CAPTURE_LOCK.acquire(timeout=1)
    try:
        with pytest.raises(native.AemetNativeDeadlineError):
            native.capture_latest_aemet_national_radar(
                _job("thread-locked"),
                raw_store=store,
                deadline_at=time.monotonic() + 0.05,
            )
    finally:
        native._CAPTURE_LOCK.release()

    helper = Path(__file__).parent / "helpers" / "aemet_native_lock_child.py"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT)
    child = subprocess.Popen(
        [sys.executable, str(helper), str(tmp_path / "raw")],
        cwd=ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "LOCKED"
        with pytest.raises(native.AemetNativeDeadlineError):
            native.capture_latest_aemet_national_radar(
                _job("process-locked"),
                raw_store=store,
                deadline_at=time.monotonic() + 0.1,
            )
    finally:
        child.terminate()
        child.wait(timeout=10)
        if child.stdout is not None:
            child.stdout.close()
        if child.stderr is not None:
            child.stderr.close()


def test_default_https_transport_uses_header_without_proxy_redirect_or_url_secret(monkeypatch):
    observed = {}

    class Response:
        status = 200
        url = native.API_URL
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(ENVELOPE)),
        }

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, size):
            payload, self.payload = getattr(self, "payload", ENVELOPE), b""
            return payload[:size]

    class Opener:
        def open(self, request, *, timeout):
            observed["request"] = request
            observed["timeout"] = timeout
            return Response()

    def build(*handlers):
        observed["handlers"] = handlers
        return Opener()

    monkeypatch.setattr(native, "build_opener", build)
    response = native._fetch_envelope(KEY, deadline=time.monotonic() + 2)
    request = observed["request"]
    assert request.full_url == native.API_URL
    assert KEY not in request.full_url
    assert request.get_header("Api_key") == KEY
    assert any(isinstance(item, native.ProxyHandler) for item in observed["handlers"])
    assert any(isinstance(item, native._RejectRedirects) for item in observed["handlers"])
    assert response.payload == ENVELOPE


def test_redirect_handler_fails_closed_without_reflecting_location_or_secret():
    handler = native._RejectRedirects()
    with pytest.raises(native.AemetNativeContractError, match="redirect") as caught:
        handler.redirect_request(
            None,
            None,
            302,
            "found",
            {"Location": f"https://evil.example/{KEY}"},
            f"https://evil.example/{KEY}",
        )
    assert KEY not in str(caught.value)


def test_candidate_is_absent_from_all_live_wiring():
    module_name = "storm_tracker_worker.aemet_native"
    handler_name = "capture_latest_aemet_national_radar"
    for relative in (
        "storm_tracker_worker/service.py",
        "storm_tracker_worker/provider_capture_registry.py",
        "storm_tracker_worker/autonomous_scheduler.py",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert module_name not in text
        assert handler_name not in text


def test_addon_exposes_aemet_transport_secret_without_payload_wiring():
    config = (ROOT / "storm_tracker_worker/config.yaml").read_text(
        encoding="utf-8"
    )
    runner = (ROOT / "storm_tracker_worker/run.sh").read_text(
        encoding="utf-8"
    )
    assert 'aemet_api_key: ""' in config
    assert 'aemet_api_key: "password?"' in config
    assert "export STV3_AEMET_API_KEY" in runner
    assert 'STV3_AEMET_API_KEY="$(bashio::config aemet_api_key)"' in runner
    assert "STV3_AEMET_API_KEY" not in json.dumps(
        _job().payload,
        sort_keys=True,
    )


@pytest.mark.skipif(
    os.environ.get("STV3_RUN_AEMET_NATIVE_LIVE") != "1"
    or not os.environ.get("STV3_AEMET_API_KEY"),
    reason="set STV3_RUN_AEMET_NATIVE_LIVE=1 and STV3_AEMET_API_KEY explicitly",
)
def test_optional_live_official_aemet_capture(tmp_path):
    result = native.capture_latest_aemet_national_radar(
        _job("aemet-live-golden"),
        raw_store=RawArtifactStore(tmp_path / "live"),
    )
    assert result["capture_state"] == "captured_not_decoded"
    assert result["raw_provenance"]["raw_member_count"] == 3
    assert result["operational_admission_eligible"] is False
