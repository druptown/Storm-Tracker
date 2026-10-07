"""Native capture must remain scoped, secret-free and bounded before decode."""
import asyncio
import importlib

import pytest


def api():
    # Missing feature is a behavioral failure rather than collection error.
    spec = importlib.util.find_spec("storm_tracker_worker.thin_raw_archive")
    assert spec is not None, "native source capture is not implemented"
    return importlib.import_module(spec.name)


def context(m, provider="ground.netatmo.region", region="home"):
    return m.RawCaptureContext("request-" + region, provider, "weather", 10.0, 0,
                               (region,), ((region, 51.0, 4.0, None),), "test-code")


def test_parallel_contexts_and_to_thread_do_not_cross_bind():
    m = api()
    archive = m.RawArchiveBuffer()

    async def run(region):
        with m.capture_scope(context(m, region=region), archive):
            await asyncio.sleep(0)
            return await asyncio.to_thread(m.capture_received,
                "https://api.netatmo.com/api/getpublicdata",
                b'{"body":{"measure":{"rain":0}}}', received_at=12.0,
                content_type="application/json")

    async def both():
        return await asyncio.gather(run("home"), run("groningen"))

    ids = asyncio.run(both())
    assert len(set(ids)) == 2
    assert {r.context.region_revision_ids for r in archive.pending()} == {
        ("home",), ("groningen",)}
    assert {r.received_at for r in archive.pending()} == {12.0}
    assert m.capture_received("https://api.netatmo.com/api/getpublicdata",
                             b'{}', received_at=13.0) is None


def test_auth_response_and_secret_url_are_excluded():
    m = api()
    archive = m.RawArchiveBuffer()
    with m.capture_scope(context(m), archive):
        assert m.capture_received("https://api.netatmo.com/oauth2/token",
            b'{"access_token":"SUPERSECRET"}', received_at=12.0,
            content_type="application/json") is None
        assert m.capture_received("https://user:SUPERSECRET@api.netatmo.com/api/getpublicdata?access_token=SUPERSECRET",
            b'{"body":{"rain":0}}', received_at=12.0) is None
        assert m.capture_received("https://api.netatmo.com/api/getpublicdata",
            b'{"access_token":"SUPERSECRET"}', received_at=12.0,
            content_type="application/json") is None
    assert archive.pending() == ()
    assert "SUPERSECRET" not in repr(archive.status())
    assert archive.status()["excluded_count"] == 3


def test_buffer_overflow_records_gap_without_throwing_into_provider():
    m = api()
    archive = m.RawArchiveBuffer(maximum_bytes=64)
    body = b'{"body":{"rain":0}}'
    with m.capture_scope(context(m), archive):
        first = m.capture_received("https://api.netatmo.com/api/getpublicdata", body,
                                   received_at=12.0, content_type="application/json")
        second = m.capture_received("https://api.netatmo.com/api/getpublicdata", body,
                                    received_at=13.0, content_type="application/json")
        assert first and second and first != second
        # Same byte object, separate receipts, only one retained allocation.
        assert archive.status()["queued_bytes"] == len(body)
        assert m.capture_received("https://api.netatmo.com/api/getpublicdata",
            b'{"body":{"rain":"' + b'x' * 80 + b'"}}', received_at=14.0,
            content_type="application/json") is None
    assert archive.status()["dropped_count"] == 1
    assert archive.status()["status"] == "degraded"
    archive.ack((first,))
    assert archive.status()["queued_bytes"] == len(body)
    archive.ack((second,))
    assert archive.status()["queued_bytes"] == 0


def test_login_body_and_unknown_host_not_archived():
    m = api()
    archive = m.RawArchiveBuffer()
    with m.capture_scope(context(m), archive):
        assert m.capture_received("https://api.netatmo.com/api/getpublicdata",
            b'<html><form>Password:</form></html>', received_at=12.0,
            content_type="text/html") is None
        assert m.capture_received("https://evil.example/api/getpublicdata",
            b'{"body":{"rain":0}}', received_at=12.0) is None
    assert not archive.pending()
    assert archive.status()["status"] == "degraded"


def test_invalid_time_is_safe_gap_and_malformed_weather_is_retained():
    m = api()
    archive = m.RawArchiveBuffer()
    with m.capture_scope(context(m), archive):
        assert m.capture_received("https://api.netatmo.com/api/getpublicdata",
            b'{"body":', received_at=float("nan")) is None
        identifier = m.capture_received("https://api.netatmo.com/api/getpublicdata",
            b'{"body":', received_at=15.0, content_type="application/json")
    assert identifier
    assert archive.pending()[0].payload == b'{"body":'
    assert archive.pending()[0].product_valid_at is None


def test_netatmo_envelope_order_and_immutable_safe_source():
    m = api()
    archive = m.RawArchiveBuffer()
    with m.capture_scope(context(m), archive):
        identifier = m.capture_received("https://api.netatmo.com/api/getpublicdata",
            b'{"status":"ok","body":[]}', received_at=15.0,
            content_type="application/json")
    assert identifier
    with pytest.raises(TypeError):
        archive.pending()[0].safe_source["path"] = "/changed"


def test_unknown_error_json_not_a_weather_capture():
    m = api()
    archive = m.RawArchiveBuffer()
    with m.capture_scope(context(m, provider="metar"), archive):
        assert m.capture_received("https://aviationweather.gov/api/data/metar",
            b'{"error":"login required"}', received_at=15.0,
            content_type="application/json") is None
    assert archive.status()["status"] == "degraded"
