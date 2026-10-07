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


def test_knmi_signed_download_descriptor_is_not_archived_as_weather():
    m=api();archive=m.RawArchiveBuffer()
    with m.capture_scope(context(m,provider='knmi_radar'),archive):
        m.capture_received('https://api.dataplatform.knmi.nl/open-data/v1/datasets/test/files/file/url',
            b'{"temporaryDownloadUrl":"https://example.amazonaws.com/file?X-Amz-Signature=PRIVATE"}',received_at=15,
            content_type='application/json')
    assert archive.pending() == ()
    assert 'PRIVATE' not in repr(archive.status())


def test_reported_gap_can_be_acknowledged_without_losing_a_newer_gap():
    m=api();archive=m.RawArchiveBuffer()
    archive.mark_gap('first_gap')
    sequence=archive.status()['gap_sequence']
    archive.mark_gap('newer_gap')
    archive.ack_gaps(sequence)
    assert archive.status()['reason_codes'] == ('newer_gap',)
    archive.ack_gaps(archive.status()['gap_sequence'])
    assert archive.status()['status']=='current'
    assert archive.status()['dropped_count']==2


def test_knmi_signed_hdf_bytes_keep_safe_address_without_signature():
    m=api();archive=m.RawArchiveBuffer()
    with m.capture_scope(context(m,provider='knmi_radar'),archive):
        m.capture_received('https://knmi-radar.storage.googleapis.com/radar/RAD_NL25_RAC_MFBS_5min_202610072210.h5?X-Goog-Signature=PRIVATE',
                          b'\x89HDF\r\n\x1a\nweather',received_at=15)
    assert len(archive.pending())==1
    assert 'PRIVATE' not in str(archive.pending()[0].safe_source)


def test_bounded_reason_set_preserves_overflow_after_earlier_ack():
    m=api();archive=m.RawArchiveBuffer()
    for index in range(64): archive.mark_gap(f'gap_{index}')
    before=archive.status()['gap_sequence']
    archive.mark_gap('extra_gap')
    archive.ack_gaps(before)
    assert archive.status()['status']=='degraded'


def test_authenticated_weather_query_is_stripped_not_weather_discarded():
    m=api();archive=m.RawArchiveBuffer()
    with m.capture_scope(context(m),archive):
        identifier=m.capture_received('https://api.netatmo.com/api/getpublicdata?access_token=PRIVATE',
                                      b'{"body":[]}',received_at=15)
    assert identifier
    assert 'PRIVATE' not in str(archive.pending()[0].safe_source)
    assert 'access_token' not in str(archive.pending()[0].safe_source)


@pytest.mark.parametrize('host',['app.meteo.be','cdn.meteo.be'])
def test_kmi_actual_image_host_captured_only_as_image(host):
    m=api();archive=m.RawArchiveBuffer()
    with m.capture_scope(context(m,provider='kmi_appv4'),archive):
        identifier=m.capture_received(f'https://{host}/frame.png',b'\x89PNG\r\n\x1a\nweather',received_at=15,content_type='image/png')
        rejected=m.capture_received(f'https://{host}/login',b'{"login":"unknown"}',received_at=15,content_type='application/json')
    assert identifier
    assert rejected is None


@pytest.mark.parametrize('provider,host,path',[
    (provider,host,path) for provider,routes in api()._ROUTES.items()
    for host,path in routes
])
def test_declared_async_weather_route_never_admits_another_host(provider,host,path):
    m=api()
    assert m.safe_source_context(provider,f'https://{host}{path}')['allowed']
    assert not m.safe_source_context(provider,f'https://unknown.invalid{path}')['allowed']
