"""Worker-owned Netatmo OAuth/query cache."""
import io
import json
import time
import urllib.error

import pytest

from storm_tracker_worker import netatmo


def test_worker_netatmo_rate_limits_same_20km_query(monkeypatch):
    worker = netatmo.NetatmoWorker()
    worker._client_id = "id"
    worker._client_secret = "secret"
    worker._refresh_token = "refresh"
    calls = []
    monkeypatch.setattr(worker, "_token", lambda now: "access")
    monkeypatch.setattr(
        worker,
        "_query",
        lambda token, lat, lon: calls.append((lat, lon)) or [{
            "obs_type": "rain",
            "station_id": "station-a",
            "lat": lat,
            "lon": lon,
            "timestamp": time.time(),
            "rain_mm": 0.0,
        }],
    )
    payload = {
        "query_key": "local:region-1",
        "query_kind": "local",
        "latitude": 51.0,
        "longitude": 4.0,
    }

    first = worker.fetch(payload)
    second = worker.fetch(payload)

    assert first["radius_km"] == 20.0
    assert first["cache"] == "fresh"
    assert second["cache"] == "rate_limited"
    assert calls == [(51.0, 4.0)]


def test_worker_netatmo_uses_last_good_after_provider_failure(monkeypatch):
    worker = netatmo.NetatmoWorker()
    worker._client_id = "id"
    worker._client_secret = "secret"
    worker._refresh_token = "refresh"
    monkeypatch.setattr(worker, "_token", lambda now: "access")
    monkeypatch.setattr(worker, "_query", lambda *_: [{"station_id": "ok"}])
    payload = {
        "query_key": "roaming:51.0,4.0",
        "query_kind": "roaming",
        "latitude": 51.0,
        "longitude": 4.0,
    }
    worker.fetch(payload)
    worker._cache[payload["query_key"]]["queried_at"] -= 6 * 60

    def fail(*_args):
        raise OSError("provider down")

    monkeypatch.setattr(worker, "_query", fail)
    result = worker.fetch(payload)

    assert result["cache"] == "last_good"
    assert result["station_count"] == 1


def test_worker_netatmo_separates_time_quarantine_from_current_stations(
    monkeypatch,
):
    worker = netatmo.NetatmoWorker()
    worker._client_id = "id"
    worker._client_secret = "secret"
    worker._refresh_token = "refresh"
    monkeypatch.setattr(worker, "_token", lambda now: "access")
    monkeypatch.setattr(worker, "_query", lambda *_: [
        {
            "station_id": "current",
            "usable_for_current_decision": True,
        },
        {
            "station_id": "future",
            "usable_for_current_decision": False,
            "time_quality": "future_clock_skew",
        },
    ])

    result = worker.fetch({
        "query_key": "local:time-quality",
        "latitude": 51.0,
        "longitude": 4.0,
    })

    assert result["station_count"] == 1
    assert [item["station_id"] for item in result["observations"]] == [
        "current",
    ]
    assert result["quarantined_observation_count"] == 1
    assert result["quarantined_observations"][0]["station_id"] == "future"


def test_worker_netatmo_requires_protected_credentials():
    worker = netatmo.NetatmoWorker()
    worker._client_id = ""
    worker._client_secret = ""
    worker._refresh_token = ""

    try:
        worker.fetch({
            "latitude": 51.0,
            "longitude": 4.0,
            "query_key": "local:test",
        })
    except RuntimeError as err:
        assert "credentials" in str(err)
    else:
        raise AssertionError("ontbrekende Netatmo-credentials aanvaard")


def test_worker_netatmo_rejects_credentials_in_mqtt_payload():
    worker = netatmo.NetatmoWorker()
    try:
        worker.fetch({
            "latitude": 51.0,
            "longitude": 4.0,
            "client_secret": "must-not-travel",
        })
    except ValueError as err:
        assert "MQTT-jobpayload" in str(err)
    else:
        raise AssertionError("secret-bearing MQTT job was accepted")


def test_worker_netatmo_restores_rotated_token_after_restart(
    monkeypatch, tmp_path
):
    state_path = tmp_path / "netatmo-token.json"
    monkeypatch.setenv("STV3_NETATMO_TOKEN_STATE", str(state_path))
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "secret")
    monkeypatch.setenv(
        "STV3_NETATMO_REFRESH_TOKEN",
        "configured-token",
    )
    first = netatmo.NetatmoWorker()
    first._refresh_token = "rotated-token"
    first._save_token_state()

    restarted = netatmo.NetatmoWorker()

    assert restarted._refresh_token == "rotated-token"


def test_worker_netatmo_does_not_reuse_rotated_state_for_new_credentials(
    monkeypatch, tmp_path
):
    state_path = tmp_path / "netatmo-token.json"
    monkeypatch.setenv("STV3_NETATMO_TOKEN_STATE", str(state_path))
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "first-secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "configured-token")
    first = netatmo.NetatmoWorker()
    first._refresh_token = "rotated-token"
    first._save_token_state()

    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "replacement-secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "replacement-token")
    restarted = netatmo.NetatmoWorker()

    assert restarted._refresh_token == "replacement-token"


def test_broker_fails_closed_when_rotated_token_cannot_be_persisted(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        "STV3_NETATMO_TOKEN_STATE",
        str(tmp_path / "netatmo-token.json"),
    )
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "configured-token")
    worker = netatmo.NetatmoWorker()
    calls = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        netatmo.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: calls.append("oauth") or Response(),
    )
    monkeypatch.setattr(
        netatmo,
        "_read_json_response",
        lambda *_args, **_kwargs: {
            "access_token": "new-access",
            "refresh_token": "rotated-token",
            "expires_in": 3600,
        },
    )

    def fail_replace(*_args):
        raise OSError("simulated persistence failure")

    monkeypatch.setattr(netatmo.os, "replace", fail_replace)
    deadline = time.time() + (
        netatmo.BROKER_REFRESH_DEADLINE_BUDGET_SECONDS + 10
    )
    with pytest.raises(RuntimeError, match="duurzaam"):
        worker.broker_access_token({}, deadline=deadline)
    assert worker._access_token == ""
    assert worker._token_state_durable is False
    assert calls == ["oauth"]

    with pytest.raises(RuntimeError, match="duurzaam"):
        worker.broker_access_token({}, deadline=deadline)
    assert calls == ["oauth"]


def test_broker_reserves_refresh_and_child_query_deadline_budget(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        "STV3_NETATMO_TOKEN_STATE",
        str(tmp_path / "netatmo-token.json"),
    )
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "refresh")
    worker = netatmo.NetatmoWorker()
    monkeypatch.setattr(
        worker,
        "_token",
        lambda _now: (_ for _ in ()).throw(
            AssertionError("OAuth mag niet starten")
        ),
    )
    with pytest.raises(TimeoutError, match="deadlinebudget"):
        worker.broker_access_token(
            {},
            deadline=(
                time.time()
                + netatmo.BROKER_REFRESH_DEADLINE_BUDGET_SECONDS
            ),
        )


def test_broker_rejects_corrupt_persisted_rotation_before_network(
    monkeypatch,
    tmp_path,
):
    state_path = tmp_path / "netatmo-token.json"
    state_path.write_text("{not-json", encoding="utf-8")
    monkeypatch.setenv("STV3_NETATMO_TOKEN_STATE", str(state_path))
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "refresh")
    worker = netatmo.NetatmoWorker()
    with pytest.raises(RuntimeError, match="onleesbaar"):
        worker.broker_access_token(
            {},
            deadline=time.time() + 120,
        )


def test_rotated_token_replace_is_directory_fsynced_before_return(
    monkeypatch,
    tmp_path,
):
    state_path = tmp_path / "netatmo-token.json"
    monkeypatch.setenv("STV3_NETATMO_TOKEN_STATE", str(state_path))
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "refresh")
    worker = netatmo.NetatmoWorker()
    events = []
    monkeypatch.setattr(
        worker,
        "_fsync_token_directory",
        lambda: events.append("directory_fsync"),
    )
    worker._refresh_token = "rotated"
    worker._save_token_state()
    events.append("returned")

    assert events == ["directory_fsync", "returned"]
    assert json.loads(state_path.read_text(encoding="utf-8"))[
        "refresh_token"
    ] == "rotated"


def test_oauth_http_error_never_echoes_provider_body_or_credentials(
    monkeypatch,
    tmp_path,
):
    secret = "secret-provider-body-value"
    monkeypatch.setenv(
        "STV3_NETATMO_TOKEN_STATE",
        str(tmp_path / "netatmo-token.json"),
    )
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "refresh-token")
    worker = netatmo.NetatmoWorker()
    error = urllib.error.HTTPError(
        netatmo.TOKEN_URL,
        401,
        "Unauthorized",
        {},
        io.BytesIO(
            json.dumps({
                "error": "invalid_grant",
                "error_description": secret,
                "refresh_token": "refresh-token",
            }).encode("utf-8")
        ),
    )
    monkeypatch.setattr(
        netatmo.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(error),
    )

    with pytest.raises(RuntimeError) as captured:
        worker.broker_access_token({}, deadline=time.time() + 120)

    assert str(captured.value) == "Netatmo OAuth HTTP 401"
    assert secret not in str(captured.value)
    assert "refresh-token" not in str(captured.value)


@pytest.mark.parametrize(
    "response",
    [
        {"refresh_token": "rotated", "expires_in": 3600},
        {
            "access_token": "access\nheader-injection",
            "refresh_token": "rotated",
            "expires_in": 3600,
        },
        {
            "access_token": "access",
            "refresh_token": "rotated",
            "expires_in": "NaN",
        },
        {
            "access_token": "access",
            "refresh_token": "rotated",
            "expires_in": netatmo.TOKEN_MAX_LIFETIME_SECONDS + 1,
        },
    ],
)
def test_invalid_oauth_response_never_rotates_durable_state(
    monkeypatch,
    tmp_path,
    response,
):
    state_path = tmp_path / "netatmo-token.json"
    monkeypatch.setenv("STV3_NETATMO_TOKEN_STATE", str(state_path))
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "configured-token")
    worker = netatmo.NetatmoWorker()

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        netatmo.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: Response(),
    )
    monkeypatch.setattr(
        netatmo,
        "_read_json_response",
        lambda *_args, **_kwargs: response,
    )

    with pytest.raises(RuntimeError, match="OAuth-response"):
        worker.broker_access_token({}, deadline=time.time() + 120)

    assert worker._refresh_token == "configured-token"
    assert worker._access_token == ""
    assert not state_path.exists()


@pytest.mark.parametrize(
    "state_text",
    [
        '{"credential_fingerprint":"' + ("a" * 64) + '",'
        '"refresh_token":"one","refresh_token":"two"}',
        '{"credential_fingerprint":"not-a-sha256",'
        '"refresh_token":"refresh"}',
        '{"credential_fingerprint":"' + ("a" * 64) + '",'
        '"refresh_token":"refresh","unknown":true}',
    ],
)
def test_broker_rejects_structurally_forged_token_state(
    monkeypatch,
    tmp_path,
    state_text,
):
    state_path = tmp_path / "netatmo-token.json"
    state_path.write_text(state_text, encoding="utf-8")
    monkeypatch.setenv("STV3_NETATMO_TOKEN_STATE", str(state_path))
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "refresh")

    worker = netatmo.NetatmoWorker()
    with pytest.raises(RuntimeError, match="onleesbaar"):
        worker.broker_access_token({}, deadline=time.time() + 120)


@pytest.mark.parametrize("deadline", [float("nan"), float("inf"), "bad"])
def test_broker_rejects_invalid_deadline_before_network(
    monkeypatch,
    tmp_path,
    deadline,
):
    monkeypatch.setenv(
        "STV3_NETATMO_TOKEN_STATE",
        str(tmp_path / "netatmo-token.json"),
    )
    monkeypatch.setenv("STV3_NETATMO_CLIENT_ID", "id")
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", "secret")
    monkeypatch.setenv("STV3_NETATMO_REFRESH_TOKEN", "refresh")
    worker = netatmo.NetatmoWorker()
    monkeypatch.setattr(
        netatmo.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("network must not be called")
        ),
    )

    with pytest.raises(TimeoutError, match="deadlinebudget"):
        worker.broker_access_token({}, deadline=deadline)
