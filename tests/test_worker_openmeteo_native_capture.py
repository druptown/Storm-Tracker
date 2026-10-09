from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import parse_qs, urlsplit

import pytest

from storm_tracker_worker import acquisition_query_authority as query_authority
from storm_tracker_worker import openmeteo_native as native
from storm_tracker_worker import public_source_raw_capture as public_source
from storm_tracker_worker.openmeteo_native import (
    EXPECTED_HOURLY_UNITS,
    EXPECTED_MINUTELY_15_UNITS,
    FORECAST_HOURS,
    FORECAST_MINUTELY_15,
    HOURLY,
    MAX_HEADER_CHARS,
    MAX_QUERY_COUNT,
    MINUTELY_15,
    OPENMETEO_CAPTURE_PRODUCT,
    OPENMETEO_RESPONSE_PRODUCT,
    OpenMeteoNativeCapacityError,
    OpenMeteoNativeContractError,
    OpenMeteoNativeCorruptionError,
    OpenMeteoNativeTransportError,
    capture_openmeteo_forecast,
)
from storm_tracker_worker.raw_artifacts import (
    RawArtifactIntegrityError,
    RawArtifactStore,
)
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.openmeteo_operational_guidance import (
    decode_openmeteo_guidance,
)


ROOT = Path(__file__).resolve().parents[1]
CRASH_CHILD = Path(__file__).parent / "helpers" / "openmeteo_native_crash_child.py"
CRASH_TIMEOUT_SECONDS = 20


@dataclass
class _Job:
    job_id: str
    payload: dict


class _Response:
    def __init__(
        self, payload: bytes, *, content_type: str = "application/json",
        status: int = 200, final_url: str | None = None,
    ) -> None:
        self._payload = payload
        self._offset = 0
        self.status = status
        self.headers = {
            "Content-Length": str(len(payload)),
            "Content-Type": content_type,
        }
        self._final_url = final_url
        self.closed = False

    def read(self, size: int) -> bytes:
        chunk = self._payload[self._offset:self._offset + size]
        self._offset += len(chunk)
        return chunk

    def geturl(self) -> str:
        assert self._final_url is not None
        return self._final_url

    def close(self) -> None:
        self.closed = True


class _HeaderBag:
    def __init__(self, entries: list[tuple[str, str]]) -> None:
        self._entries = list(entries)

    def get(self, name: str):
        for key, value in self._entries:
            if key.lower() == name.lower():
                return value
        return None

    def items(self):
        return list(self._entries)


def _query(key: str, latitude: float, longitude: float, kind: str = "local"):
    return {
        "latitude": latitude,
        "longitude": longitude,
        "query_key": key,
        "query_kind": kind,
    }


def _authority_identity(prefix: str, seed: str) -> str:
    token = base64.b32encode(hashlib.sha256(seed.encode("ascii")).digest())
    encoded = token.decode("ascii").lower().rstrip("=")
    chunks = [encoded[offset:offset + 8] for offset in range(0, len(encoded), 8)]
    return prefix + ":" + ":z:".join(chunks)


_KEY_A = _authority_identity("aqt", "target-a")
_KEY_B = _authority_identity("aqt", "target-b")
_JOB_ID = _authority_identity("acq", "job-a")


def _job(*queries: dict) -> _Job:
    return _Job(
        _JOB_ID,
        {
            "contract": "stv3-openmeteo-target-query-batch-v1",
            "operational_handler_compatible": False,
            "queries": list(queries),
            "target_resolution_required": True,
        },
    )


def _location(latitude: float = 50.85, longitude: float = 4.35) -> dict:
    minutely_time = [1_800_000_000 + 900 * index for index in range(FORECAST_MINUTELY_15)]
    hourly_time = [1_800_000_000 + 3600 * index for index in range(FORECAST_HOURS)]
    minutely = {"time": minutely_time}
    minutely.update({name: [0.0] * FORECAST_MINUTELY_15 for name in MINUTELY_15})
    hourly = {"time": hourly_time}
    hourly.update({name: [0.0] * FORECAST_HOURS for name in HOURLY})
    minutely_units = dict(EXPECTED_MINUTELY_15_UNITS)
    hourly_units = dict(EXPECTED_HOURLY_UNITS)
    return {
        "latitude": latitude,
        "longitude": longitude,
        "elevation": 30.0,
        "generationtime_ms": 1.25,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "current": {
            "time": 1_800_000_000,
            "interval": 900,
            "precipitation": 0.0,
        },
        "current_units": {
            "time": "unixtime",
            "interval": "seconds",
            "precipitation": "mm",
        },
        "minutely_15": minutely,
        "minutely_15_units": minutely_units,
        "hourly": hourly,
        "hourly_units": hourly_units,
    }


def test_openmeteo_accepts_current_empty_dimensionless_lifted_index_unit():
    location = _location()
    location["hourly_units"]["lifted_index"] = ""
    payload = json.dumps(location, separators=(",", ":")).encode("utf-8")

    decoded = native.decode_verified_openmeteo_response(payload, query_count=1)

    assert decoded[0]["hourly_units"]["lifted_index"] == ""


def test_openmeteo_accepts_current_exact_multi_location_indexes():
    locations = [_location(), _location(48.85, 2.35)]
    locations[1]["location_id"] = 1
    payload = json.dumps(locations, separators=(",", ":")).encode("utf-8")

    decoded = native.decode_verified_openmeteo_response(payload, query_count=2)

    assert len(decoded) == 2
    assert decoded[1]["location_id"] == 1


def test_openmeteo_rejects_inconsistent_multi_location_indexes():
    locations = [_location(), _location(48.85, 2.35)]
    locations[1]["location_id"] = 7
    payload = json.dumps(locations, separators=(",", ":")).encode("utf-8")

    with pytest.raises(
        OpenMeteoNativeContractError,
        match="locatie-indexen wijken af",
    ):
        native.decode_verified_openmeteo_response(payload, query_count=2)


def test_exact_raw_capture_decodes_into_target_bound_operational_guidance(
    tmp_path: Path,
) -> None:
    location = _location()
    location["current"]["precipitation"] = 0.2
    location["minutely_15"]["precipitation"][2] = 0.4
    payload = json.dumps(location, separators=(",", ":")).encode("utf-8")
    store = RawArtifactStore(tmp_path / "raw")
    calls: list[dict] = []
    result = capture_openmeteo_forecast(
        _job(_query(_KEY_A, 50.85, 4.35)),
        raw_store=store,
        opener=_opener(payload, calls),
    )
    desired = parse_desired_state({
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:test",
        "authority_epoch": 2,
        "generation": 1,
        "issued_at": 1_800_000_000.0,
        "previous_state_sha256": None,
        "targets": [{
            "target_id": "home",
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": 50.85,
            "longitude": 4.35,
            "position_observed_at": 1_800_000_000.0,
            "location_accuracy_m": 10.0,
            "observation_radius_km": 250.0,
            "country_code": "BE",
        }],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    })

    guidance = decode_openmeteo_guidance(
        raw_store=store,
        capture_id=result["raw_provenance"]["raw_capture_id"],
        desired_state=desired,
    )

    assert len(guidance) == 1
    assert guidance[0].target_id == "home"
    assert guidance[0].current_rate_mm_h == 0.8
    assert guidance[0].first_wet_at == 1_800_001_800.0
    assert guidance[0].forecast_total_90m_mm == 0.4


def test_live_public_source_capture_decodes_into_target_bound_guidance(
    tmp_path: Path,
) -> None:
    """The active raw collector and the operational decoder share one contract."""

    location = _location()
    location["current"]["precipitation"] = 0.3
    location["minutely_15"]["precipitation"][1] = 0.2
    payload = json.dumps(location, separators=(",", ":")).encode("utf-8")
    store = RawArtifactStore(tmp_path / "raw")
    calls: list[dict] = []
    result = public_source.capture_openmeteo_region(
        _job(_query(_KEY_A, 50.85, 4.35)),
        raw_store=store,
        opener=_opener(payload, calls),
    )
    desired = parse_desired_state({
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:test",
        "authority_epoch": 2,
        "generation": 1,
        "issued_at": 1_800_000_000.0,
        "previous_state_sha256": None,
        "targets": [{
            "target_id": "home",
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": 50.85,
            "longitude": 4.35,
            "position_observed_at": 1_800_000_000.0,
            "location_accuracy_m": 10.0,
            "observation_radius_km": 250.0,
            "country_code": "BE",
        }],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    })

    guidance = decode_openmeteo_guidance(
        raw_store=store,
        capture_id=result["raw_provenance"]["raw_capture_id"],
        desired_state=desired,
    )

    assert len(guidance) == 1
    assert guidance[0].target_id == "home"
    assert guidance[0].current_rate_mm_h == 1.2
    assert guidance[0].first_wet_at == 1_800_000_900.0


def _opener(payload: bytes, calls: list[dict]):
    def open_response(request, *, timeout):
        calls.append({
            "headers": dict(request.header_items()),
            "timeout": timeout,
            "url": request.full_url,
        })
        return _Response(payload, final_url=request.full_url)

    return open_response


def _store(tmp_path: Path) -> RawArtifactStore:
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    return store


def test_query_and_job_identities_match_acquisition_authority_exactly():
    query_digest = hashlib.sha256(b"target-a").hexdigest()
    job_digest = hashlib.sha256(b"job-a").hexdigest()
    assert _KEY_A == query_authority._safe_query_key("aqt", query_digest)
    assert _JOB_ID == query_authority.safe_native_job_id(
        "acquisition:" + job_digest
    )


def test_attribution_authority_is_canonical_hashed_and_non_operational():
    canonical = native.OPENMETEO_ATTRIBUTION_POLICY_CANONICAL
    policy = json.loads(canonical)
    assert json.dumps(
        policy,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii") == canonical
    assert hashlib.sha256(canonical).hexdigest() == (
        native.OPENMETEO_ATTRIBUTION_POLICY_SHA256
    )
    assert native.OPENMETEO_ATTRIBUTION_POLICY_ID == (
        "oma1:" + native.OPENMETEO_ATTRIBUTION_POLICY_SHA256
    )
    assert policy["provider_credit"] == "Open-Meteo"
    assert policy["data_license_name"] == "CC BY 4.0"
    assert policy["free_api_commercial_use_authorized"] is False
    assert policy["personal_home_automation_scope"] == (
        "authorized_noncommercial_example"
    )
    assert policy["provider_coordinate_log_notice"] == (
        "Webserver logs may contain request coordinates."
    )
    assert policy["provider_log_retention_days"] == 90
    assert policy["provider_log_third_party_sharing"] is False
    assert policy["published_call_limits"] == {
        "day": 10_000,
        "hour": 5_000,
        "minute": 600,
        "month": 300_000,
        "provider_metering_rules_apply": True,
    }
    assert policy["legal_advice"] is False
    assert policy["retention_right"] == "not_established"
    serialized = json.dumps(policy, sort_keys=True).lower()
    assert "latitude" not in serialized
    assert "longitude" not in serialized
    assert "query_key" not in serialized


def test_documented_attribution_identity_matches_code():
    root = Path(__file__).resolve().parents[1]
    text = (root / "docs" / "WORKER_OPENMETEO_EXACT_RAW_SHADOW.md").read_text(
        encoding="utf-8"
    )
    assert native.OPENMETEO_ATTRIBUTION_POLICY_ID in text


def test_exact_response_is_durable_and_result_remains_unknown(tmp_path: Path):
    raw = json.dumps(
        [_location(), _location(48.85, 2.35)],
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("ascii")
    calls: list[dict] = []
    store = _store(tmp_path)

    result = capture_openmeteo_forecast(
        _job(
            _query(_KEY_A, 50.85, 4.35),
            _query(_KEY_B, 48.85, 2.35, "roaming"),
        ),
        raw_store=store,
        opener=_opener(raw, calls),
    )

    assert result["weather_state"] == "unknown"
    assert result["meteorological_claim"] is False
    assert result["model_skill_claim"] is False
    assert result["operational_admission_eligible"] is False
    assert result["structure"]["location_count"] == 2
    assert result["structure"]["variable_count"] == 19
    assert len(calls) == 1
    query = parse_qs(urlsplit(calls[0]["url"]).query)
    assert query["latitude"] == ["50.85000,48.85000"]
    assert query["longitude"] == ["4.35000,2.35000"]
    assert query["elevation"] == ["nan,nan"]
    assert query["cell_selection"] == ["nearest"]
    assert query["current"] == ["precipitation"]
    assert query["forecast_hours"] == [str(FORECAST_HOURS)]
    assert query["forecast_minutely_15"] == [str(FORECAST_MINUTELY_15)]
    assert query["hourly"] == [",".join(HOURLY)]
    assert query["minutely_15"] == [",".join(MINUTELY_15)]
    assert query["precipitation_unit"] == ["mm"]
    assert query["timeformat"] == ["unixtime"]
    assert query["timezone"] == ["UTC"]
    assert query["wind_speed_unit"] == ["kmh"]
    assert "apikey" not in query
    assert _KEY_A not in calls[0]["url"]
    assert 0 < calls[0]["timeout"] <= native.HTTP_IO_TIMEOUT_SECONDS
    assert "authorization" not in {
        name.lower() for name in calls[0]["headers"]
    }
    capture_id = store._capture_id(  # noqa: SLF001
        provider="openmeteo", product=OPENMETEO_CAPTURE_PRODUCT,
        fetch_id=f"{_job(_query(_KEY_A, 50.85, 4.35)).job_id}:capture",
    )
    bundle = store.load_capture_bundle_bytes(capture_id)
    assert bundle is not None
    capture, receipts, payloads = bundle
    assert payloads == {"response_json": raw}
    assert receipts["response_json"].product == OPENMETEO_RESPONSE_PRODUCT
    assert receipts["response_json"].provenance["attribution_policy"] == (
        json.loads(native.OPENMETEO_ATTRIBUTION_POLICY_CANONICAL)
    )
    assert receipts["response_json"].provenance[
        "attribution_policy_id"
    ] == native.OPENMETEO_ATTRIBUTION_POLICY_ID
    assert capture.upstream_capture_id.endswith(
        "attribution_policy_sha256="
        + native.OPENMETEO_ATTRIBUTION_POLICY_SHA256
    )
    assert result["attribution_policy_id"] == (
        native.OPENMETEO_ATTRIBUTION_POLICY_ID
    )
    assert capture.product_timestamp is None
    assert _KEY_A.encode() not in json.dumps(result, sort_keys=True).encode()


def test_single_location_object_and_restart_are_exactly_idempotent(tmp_path: Path):
    raw = json.dumps(_location(), separators=(",", ":")).encode()
    store = _store(tmp_path)
    job = _job(_query(_KEY_A, 50.85, 4.35))
    first_calls: list[dict] = []
    first = capture_openmeteo_forecast(
        job, raw_store=store, opener=_opener(raw, first_calls)
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("restart mag niet refetchen")

    second = capture_openmeteo_forecast(
        job, raw_store=RawArtifactStore(tmp_path / "raw"), opener=forbidden
    )
    assert first == second
    assert len(first_calls) == 1


def test_authority_order_is_preserved_even_when_query_keys_are_not_sorted(tmp_path: Path):
    raw = json.dumps(
        [_location(48.85, 2.35), _location()], separators=(",", ":")
    ).encode()
    calls: list[dict] = []
    result = capture_openmeteo_forecast(
        _job(
            _query(_KEY_B, 48.85, 2.35, "roaming"),
            _query(_KEY_A, 50.85, 4.35),
        ),
        raw_store=_store(tmp_path),
        opener=_opener(raw, calls),
    )
    query = parse_qs(urlsplit(calls[0]["url"]).query)
    assert query["latitude"] == ["48.85000,50.85000"]
    assert result["structure"]["location_count"] == 2


def test_crash_after_response_store_recovers_without_refetch(tmp_path: Path):
    raw = json.dumps(_location(), separators=(",", ":")).encode()
    store = _store(tmp_path)
    job = _job(_query(_KEY_A, 50.85, 4.35))

    def fail(point: str) -> None:
        if point == "openmeteo.after_response_store":
            raise RuntimeError("power loss")

    with pytest.raises(RuntimeError, match="power loss"):
        capture_openmeteo_forecast(
            job, raw_store=store, opener=_opener(raw, []),
            _fault_injector=fail,
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0

    result = capture_openmeteo_forecast(
        job,
        raw_store=RawArtifactStore(tmp_path / "raw"),
        opener=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("recovery mag niet refetchen")
        ),
    )
    assert result["structure"]["location_count"] == 1
    assert store.stats()["capture_count"] == 1


def test_crash_after_capture_commit_replays_without_network(tmp_path: Path):
    raw = json.dumps(_location(), separators=(",", ":")).encode()
    store = _store(tmp_path)
    job = _job(_query(_KEY_A, 50.85, 4.35))

    def fail(point: str) -> None:
        if point == "openmeteo.after_capture_commit":
            raise RuntimeError("power loss after capture")

    with pytest.raises(RuntimeError, match="power loss after capture"):
        capture_openmeteo_forecast(
            job,
            raw_store=store,
            opener=_opener(raw, []),
            _fault_injector=fail,
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1

    replay = capture_openmeteo_forecast(
        job,
        raw_store=RawArtifactStore(store.root),
        opener=lambda *_args, **_kwargs: pytest.fail("geen refetch verwacht"),
    )
    assert replay["structure"]["location_count"] == 1


def _catalog_counts(store: RawArtifactStore) -> tuple[int, int, int]:
    stats = store.stats()
    return (
        stats["object_count"],
        stats["receipt_count"],
        stats["capture_count"],
    )


@pytest.mark.parametrize(
    ("checkpoint", "crashed_capture_count"),
    [
        pytest.param(
            "openmeteo.after_response_store", 0,
            id="after_response_store-R1-C0",
        ),
        pytest.param(
            "openmeteo.before_capture_commit", 0,
            id="before_capture_commit-R1-C0",
        ),
        pytest.param(
            "openmeteo.after_capture_commit", 1,
            id="after_capture_commit-R1-C1",
        ),
    ],
)
def test_real_os_exit_recovery_matrix(
    tmp_path: Path, checkpoint: str, crashed_capture_count: int
):
    raw_root = tmp_path / checkpoint.replace(".", "_")
    command = [
        sys.executable,
        str(CRASH_CHILD),
        str(raw_root),
        checkpoint,
    ]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT)
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        capture_output=True,
        check=False,
        timeout=CRASH_TIMEOUT_SECONDS,
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    crashed_store = RawArtifactStore(raw_root)
    crashed_store.initialize()
    assert _catalog_counts(crashed_store) == (1, 1, crashed_capture_count)
    crashed_integrity = crashed_store.integrity_check()
    assert crashed_integrity["healthy"] is True, crashed_integrity

    job = _job(_query(_KEY_A, 50.85, 4.35))
    expected_receipt_id = native._receipt_id(crashed_store, job.job_id)
    expected_capture_id = native._capture_id(crashed_store, job.job_id)
    crashed_receipt = crashed_store.load_receipt(expected_receipt_id)
    assert crashed_receipt is not None
    assert crashed_store.read_bytes(expected_receipt_id) == json.dumps(
        _location(), separators=(",", ":")
    ).encode()
    receipt_identity = (
        crashed_receipt.receipt_id,
        crashed_receipt.content_sha256,
        crashed_receipt.receipt_sha256,
    )
    crashed_capture = crashed_store.load_capture(expected_capture_id)
    assert (crashed_capture is not None) is bool(crashed_capture_count)
    crashed_capture_identity = None
    if crashed_capture is not None:
        crashed_capture_identity = (
            crashed_capture.capture_id,
            crashed_capture.manifest_sha256,
            crashed_capture.capture_receipt_sha256,
        )

    refetches: list[object] = []

    def forbid_refetch(*args, **kwargs):
        refetches.append((args, kwargs))
        pytest.fail("durable Open-Meteo response_json role was refetched")

    recovered = capture_openmeteo_forecast(
        job,
        raw_store=RawArtifactStore(raw_root),
        opener=forbid_refetch,
    )
    assert refetches == []
    recovered_raw = recovered["raw_provenance"]
    recovered_source = recovered_raw["response"]
    assert (
        recovered_source["receipt_id"],
        recovered_source["content_sha256"],
        recovered_source["receipt_sha256"],
    ) == receipt_identity
    assert recovered_raw["raw_capture_id"] == expected_capture_id

    recovered_store = RawArtifactStore(raw_root)
    recovered_store.initialize()
    assert _catalog_counts(recovered_store) == (1, 1, 1)
    recovered_capture = recovered_store.load_capture(expected_capture_id)
    assert recovered_capture is not None
    capture_identity = (
        recovered_capture.capture_id,
        recovered_capture.manifest_sha256,
        recovered_capture.capture_receipt_sha256,
    )
    assert capture_identity == (
        recovered_raw["raw_capture_id"],
        recovered_raw["raw_capture_manifest_sha256"],
        recovered_raw["raw_capture_receipt_sha256"],
    )
    if crashed_capture_identity is not None:
        assert capture_identity == crashed_capture_identity
    recovered_integrity = recovered_store.integrity_check()
    assert recovered_integrity["healthy"] is True, recovered_integrity

    replay = capture_openmeteo_forecast(
        job,
        raw_store=RawArtifactStore(raw_root),
        opener=forbid_refetch,
    )
    assert refetches == []
    assert replay == recovered

    replay_store = RawArtifactStore(raw_root)
    replay_store.initialize()
    assert _catalog_counts(replay_store) == (1, 1, 1)
    replay_receipt = replay_store.load_receipt(expected_receipt_id)
    replay_capture = replay_store.load_capture(expected_capture_id)
    assert replay_receipt is not None
    assert replay_capture is not None
    assert (
        replay_receipt.receipt_id,
        replay_receipt.content_sha256,
        replay_receipt.receipt_sha256,
    ) == receipt_identity
    assert (
        replay_capture.capture_id,
        replay_capture.manifest_sha256,
        replay_capture.capture_receipt_sha256,
    ) == capture_identity
    replay_integrity = replay_store.integrity_check()
    assert replay_integrity["healthy"] is True, replay_integrity


def test_same_job_cannot_be_rebound_to_another_authority_query(tmp_path: Path):
    raw = json.dumps(_location(), separators=(",", ":")).encode()
    store = _store(tmp_path)
    capture_openmeteo_forecast(
        _job(_query(_KEY_A, 50.85, 4.35)),
        raw_store=store,
        opener=_opener(raw, []),
    )

    with pytest.raises(OpenMeteoNativeCorruptionError, match="receipt"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_B, 48.85, 2.35, "roaming")),
            raw_store=RawArtifactStore(store.root),
            opener=lambda *_args, **_kwargs: pytest.fail("geen refetch verwacht"),
        )


def test_physical_raw_tamper_is_detected_on_restart(tmp_path: Path):
    raw = json.dumps(_location(), separators=(",", ":")).encode()
    store = _store(tmp_path)
    result = capture_openmeteo_forecast(
        _job(_query(_KEY_A, 50.85, 4.35)),
        raw_store=store,
        opener=_opener(raw, []),
    )
    receipt_id = result["raw_provenance"]["response"]["receipt_id"]
    receipt = store.load_receipt(receipt_id)
    assert receipt is not None
    Path(receipt.artifact_path).write_bytes(b"x" * len(raw))

    with pytest.raises(RawArtifactIntegrityError):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=RawArtifactStore(store.root),
            opener=lambda *_args, **_kwargs: pytest.fail("geen refetch verwacht"),
        )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload.update(extra=True),
        lambda payload: payload.update(operational_handler_compatible=True),
        lambda payload: payload["queries"][0].update(target_id="home"),
        lambda payload: payload["queries"].append(dict(payload["queries"][0])),
    ],
)
def test_forged_or_privacy_broadening_payload_is_rejected(tmp_path: Path, mutator):
    job = _job(_query(_KEY_A, 50.85, 4.35))
    mutator(job.payload)
    with pytest.raises(OpenMeteoNativeContractError):
        capture_openmeteo_forecast(
            job, raw_store=_store(tmp_path),
            opener=lambda *_args, **_kwargs: pytest.fail("geen netwerk verwacht"),
        )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda job: setattr(job, "job_id", "acquisition:" + "a" * 64),
        lambda job: job.payload["queries"][0].update(
            query_key="aqt:" + "a" * 52
        ),
        lambda job: job.payload["queries"][0].update(query_kind=[]),
        lambda job: job.payload["queries"][0].update(latitude=10**4000),
    ],
)
def test_authority_projection_profile_rejects_near_misses(
    tmp_path: Path, mutator
):
    job = _job(_query(_KEY_A, 50.85, 4.35))
    mutator(job)
    with pytest.raises(OpenMeteoNativeContractError):
        capture_openmeteo_forecast(
            job,
            raw_store=_store(tmp_path),
            opener=lambda *_args, **_kwargs: pytest.fail("geen netwerk verwacht"),
        )


def test_full_authority_batch_cap_is_accepted_and_next_record_is_rejected(
    tmp_path: Path,
):
    queries = [
        _query(
            _authority_identity("aqt", f"target-{index}"),
            50.0 + index / 10_000,
            4.0 + index / 10_000,
        )
        for index in range(MAX_QUERY_COUNT)
    ]
    raw = json.dumps(
        [_location() for _ in queries], separators=(",", ":")
    ).encode()
    calls: list[dict] = []
    result = capture_openmeteo_forecast(
        _job(*queries),
        raw_store=_store(tmp_path),
        opener=_opener(raw, calls),
    )
    assert result["structure"]["location_count"] == MAX_QUERY_COUNT
    assert len(calls[0]["url"]) < native.MAX_URL_CHARS

    too_many = queries + [
        _query(_authority_identity("aqt", "one-too-many"), 50.1, 4.1)
    ]
    with pytest.raises(OpenMeteoNativeContractError, match="fail-closed"):
        capture_openmeteo_forecast(
            _job(*too_many),
            raw_store=_store(tmp_path / "too-many"),
            opener=lambda *_args, **_kwargs: pytest.fail("geen netwerk verwacht"),
        )


def test_response_location_count_and_cadence_fail_closed_after_durable_store(tmp_path: Path):
    payload = _location()
    payload["minutely_15"]["time"][2] += 1
    raw = json.dumps(payload, separators=(",", ":")).encode()
    store = _store(tmp_path)
    with pytest.raises(OpenMeteoNativeContractError, match="cadence"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=_opener(raw, []),
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_response_location_count_must_match_authority_batch(tmp_path: Path):
    raw = json.dumps([_location()], separators=(",", ":")).encode()
    store = _store(tmp_path)
    with pytest.raises(OpenMeteoNativeContractError, match="locatieaantal"):
        capture_openmeteo_forecast(
            _job(
                _query(_KEY_A, 50.85, 4.35),
                _query(_KEY_B, 48.85, 2.35, "roaming"),
            ),
            raw_store=store,
            opener=_opener(raw, []),
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_single_location_response_must_use_official_object_shape(tmp_path: Path):
    raw = json.dumps([_location()], separators=(",", ":")).encode()
    store = _store(tmp_path)
    with pytest.raises(OpenMeteoNativeContractError, match="enkelpuntsrespons"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=_opener(raw, []),
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_duplicate_json_key_fails_only_after_exact_bytes_are_durable(
    tmp_path: Path,
):
    valid = json.dumps(_location(), separators=(",", ":")).encode()
    raw = b'{"timezone":"UTC",' + valid[1:]
    store = _store(tmp_path)
    with pytest.raises(OpenMeteoNativeContractError, match="strikte JSON"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=_opener(raw, []),
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload["current"].update(time=1_800_000_000.5),
        lambda payload: payload["current"].update(interval=900.0),
        lambda payload: payload["minutely_15"].update(
            time=[value + 1 for value in payload["minutely_15"]["time"]]
        ),
        lambda payload: payload.update(timezone=[]),
        lambda payload: payload.update(timezone_abbreviation="CEST"),
        lambda payload: payload.update(utc_offset_seconds=False),
    ],
)
def test_official_utc_time_shape_drift_fails_after_durable_store(
    tmp_path: Path, mutator
):
    payload = _location()
    mutator(payload)
    raw = json.dumps(payload, separators=(",", ":")).encode()
    store = _store(tmp_path)
    with pytest.raises(OpenMeteoNativeContractError):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=_opener(raw, []),
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_unit_drift_fails_closed_after_durable_store(tmp_path: Path):
    payload = _location()
    payload["minutely_15_units"]["precipitation"] = "inch"
    raw = json.dumps(payload, separators=(",", ":")).encode()
    store = _store(tmp_path)
    with pytest.raises(OpenMeteoNativeContractError, match="units"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=_opener(raw, []),
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_wrong_content_type_and_redirect_are_rejected_before_storage(tmp_path: Path):
    raw = json.dumps(_location()).encode()
    store = _store(tmp_path)

    def wrong_type(request, *, timeout):
        return _Response(
            raw, content_type="text/html", final_url=request.full_url
        )

    with pytest.raises(OpenMeteoNativeContractError, match="geen JSON"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=wrong_type,
        )
    assert store.stats()["receipt_count"] == 0

    def redirected(_request, *, timeout):
        return _Response(
            raw, final_url="https://example.invalid/v1/forecast"
        )

    with pytest.raises(OpenMeteoNativeContractError, match="exacte request"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=redirected,
        )
    assert store.stats()["receipt_count"] == 0

    def same_origin_query_drift(request, *, timeout):
        return _Response(
            raw,
            final_url=request.full_url + "&forecast_hours=7",
        )

    with pytest.raises(OpenMeteoNativeContractError, match="exacte request"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=same_origin_query_drift,
        )
    assert store.stats()["receipt_count"] == 0


@pytest.mark.parametrize("kind", ["oversized", "duplicate"])
def test_header_caps_and_singleton_framing_fail_before_storage(
    tmp_path: Path, kind: str
):
    raw = json.dumps(_location()).encode()
    store = _store(tmp_path)

    def malformed_headers(request, *, timeout):
        response = _Response(raw, final_url=request.full_url)
        if kind == "oversized":
            response.headers = _HeaderBag([
                ("Content-Length", str(len(raw))),
                ("Content-Type", "application/json"),
                ("X-Padding", "x" * MAX_HEADER_CHARS),
            ])
        else:
            response.headers = _HeaderBag([
                ("Content-Length", str(len(raw))),
                ("Content-Length", str(len(raw))),
                ("Content-Type", "application/json"),
            ])
        return response

    expected = OpenMeteoNativeCapacityError if kind == "oversized" else OpenMeteoNativeContractError
    with pytest.raises(expected):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=malformed_headers,
        )
    assert store.stats()["receipt_count"] == 0


def test_transport_failure_cannot_expose_query_coordinates(tmp_path: Path):
    def leaking_transport(request, *, timeout):
        raise RuntimeError(f"upstream failed for {request.full_url}")

    with pytest.raises(OpenMeteoNativeTransportError) as captured:
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=_store(tmp_path),
            opener=leaking_transport,
        )
    assert "50.85000" not in str(captured.value)
    assert "4.35000" not in str(captured.value)
    assert captured.value.__suppress_context__ is True


def test_deadline_expiry_after_body_read_prevents_storage(
    tmp_path: Path, monkeypatch
):
    clock = [100.0]
    read_timeouts: list[float] = []
    monkeypatch.setattr(native.time, "monotonic", lambda: clock[0])
    raw = json.dumps(_location(), separators=(",", ":")).encode()

    def expiring_opener(request, *, timeout):
        response = _Response(raw, final_url=request.full_url)

        class Socket:
            def settimeout(self, value: float) -> None:
                read_timeouts.append(value)

        class Raw:
            _sock = Socket()

        class Stream:
            raw = Raw()

        response.fp = Stream()
        original = response.read

        def read(size: int) -> bytes:
            chunk = original(size)
            clock[0] = 102.0
            return chunk

        response.read = read
        return response

    store = _store(tmp_path)
    with pytest.raises(TimeoutError, match="deadline"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            deadline_at=101.0,
            opener=expiring_opener,
        )
    assert store.stats()["receipt_count"] == 0
    assert read_timeouts
    assert all(0 < value <= 1.0 for value in read_timeouts)


def test_unrepresentable_deadline_is_rejected_before_storage(tmp_path: Path):
    store = _store(tmp_path)
    with pytest.raises(OpenMeteoNativeContractError, match="deadline_at"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            deadline_at=10**4000,
            opener=lambda *_args, **_kwargs: pytest.fail("geen netwerk verwacht"),
        )
    assert store.stats()["receipt_count"] == 0


def test_deadline_after_durable_store_prevents_inspection_and_capture(
    tmp_path: Path, monkeypatch
):
    clock = [100.0]
    monkeypatch.setattr(native.time, "monotonic", lambda: clock[0])
    raw = json.dumps(_location(), separators=(",", ":")).encode()
    store = _store(tmp_path)
    original_store = store.store

    def delayed_store(**kwargs):
        receipt = original_store(**kwargs)
        clock[0] = 102.0
        return receipt

    monkeypatch.setattr(store, "store", delayed_store)
    monkeypatch.setattr(
        native,
        "_inspect",
        lambda *_args, **_kwargs: pytest.fail("geen inspectie na deadline"),
    )
    with pytest.raises(TimeoutError, match="deadline"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            deadline_at=101.0,
            opener=_opener(raw, []),
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


def test_recovery_deadline_expiry_does_not_publish_partial_capture(
    tmp_path: Path, monkeypatch
):
    raw = json.dumps(_location(), separators=(",", ":")).encode()
    store = _store(tmp_path)
    job = _job(_query(_KEY_A, 50.85, 4.35))

    def fail_after_store(point: str) -> None:
        if point == "openmeteo.after_response_store":
            raise RuntimeError("power loss")

    with pytest.raises(RuntimeError, match="power loss"):
        capture_openmeteo_forecast(
            job,
            raw_store=store,
            opener=_opener(raw, []),
            _fault_injector=fail_after_store,
        )
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0

    clock = [100.0]
    real_inspect = native._inspect

    def expiring_inspect(payload: bytes, *, query_count: int):
        result = real_inspect(payload, query_count=query_count)
        clock[0] = 102.0
        return result

    monkeypatch.setattr(native.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(native, "_inspect", expiring_inspect)
    with pytest.raises(TimeoutError, match="deadline"):
        capture_openmeteo_forecast(
            job,
            raw_store=RawArtifactStore(store.root),
            deadline_at=101.0,
            opener=lambda *_args, **_kwargs: pytest.fail("geen refetch verwacht"),
        )
    assert store.stats()["capture_count"] == 0


def test_tiny_chunk_attack_is_bounded_before_storage(tmp_path: Path):
    raw = json.dumps(_location()).encode()

    def tiny_chunks(request, *, timeout):
        response = _Response(raw, final_url=request.full_url)
        response.headers = {"Content-Type": "application/json"}
        response.read = lambda _size: b"x"
        return response

    store = _store(tmp_path)
    with pytest.raises(OpenMeteoNativeCapacityError, match="chunkgrens"):
        capture_openmeteo_forecast(
            _job(_query(_KEY_A, 50.85, 4.35)),
            raw_store=store,
            opener=tiny_chunks,
        )
    assert store.stats()["receipt_count"] == 0


def test_module_is_inert_and_not_live_wired():
    root = Path(__file__).resolve().parents[1]
    forbidden = (
        root / "storm_tracker_worker" / "service.py",
        root / "storm_tracker_worker" / "acquisition_activation.py",
        root / "storm_tracker_worker" / "acquisition_capture_recovery.py",
        root / "storm_tracker_worker" / "acquisition_handler_child.py",
        root / "storm_tracker_worker" / "terminal_raw_completion_router.py",
        root / "storm_tracker_worker" / "worker_first_service_composition.py",
        root / "custom_components" / "storm_tracker_v3" / "__init__.py",
    )
    for path in forbidden:
        text = path.read_text(encoding="utf-8")
        assert "openmeteo_native" not in text
        assert "capture_openmeteo_forecast" not in text
        assert "openmeteo_query_bound_forecast_capture" not in text
