"""Transport-level byte contracts for runtime radar/provider HTTP bodies."""
from __future__ import annotations

import importlib
import sys

import pytest


class _AsyncContent:
    def __init__(self, chunks):
        self._chunks = tuple(chunks)
        self.iterated = 0
        self.requested_chunk_bytes = None

    async def iter_chunked(self, maximum_chunk_bytes):
        self.requested_chunk_bytes = maximum_chunk_bytes
        for chunk in self._chunks:
            self.iterated += 1
            yield chunk


class _AsyncResponse:
    def __init__(self, chunks, *, headers=None):
        self.headers = dict(headers or {})
        self.content = _AsyncContent(chunks)


def test_thin_runtime_imports_no_legacy_raw_or_acquisition_infrastructure():
    forbidden = (
        "storm_tracker_worker.raw_",
        "storm_tracker_worker.acquisition_",
        "storm_tracker_worker.completion_",
        "storm_tracker_worker.terminal_",
    )
    before = set(sys.modules)

    importlib.import_module("storm_tracker_worker.thin_mqtt_service")

    newly_loaded = set(sys.modules) - before
    assert not sorted(
        module for module in newly_loaded if module.startswith(forbidden)
    )


def _provider_modules(
    kmi_module,
    knmi_module,
    rainviewer_module,
    aemet_radar_module,
    dpc_radar_module,
    meteofrance_radar_module,
    noaa_goes_rrqpe_module,
):
    return (
        kmi_module,
        knmi_module,
        rainviewer_module,
        aemet_radar_module,
        dpc_radar_module,
        meteofrance_radar_module,
        noaa_goes_rrqpe_module,
    )


@pytest.mark.asyncio
async def test_runtime_readers_accept_missing_content_length(
    kmi_module,
    knmi_module,
    rainviewer_module,
    aemet_radar_module,
    dpc_radar_module,
    meteofrance_radar_module,
    noaa_goes_rrqpe_module,
):
    modules = _provider_modules(
        kmi_module,
        knmi_module,
        rainviewer_module,
        aemet_radar_module,
        dpc_radar_module,
        meteofrance_radar_module,
        noaa_goes_rrqpe_module,
    )
    for module in modules:
        response = _AsyncResponse((b"12", b"34"))
        assert await module._read_bounded_response(
            response,
            maximum_bytes=4,
        ) == b"1234"
        assert response.content.requested_chunk_bytes == 64 * 1024


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "chunks"),
    (
        ({"Content-Length": "not-a-number"}, (b"1234",)),
        ({"Content-Length": "-1"}, (b"1234",)),
        ({"Content-Length": "5"}, (b"1234",)),
    ),
)
async def test_runtime_readers_reject_invalid_declared_length_before_body(
    headers,
    chunks,
    kmi_module,
    knmi_module,
    rainviewer_module,
    aemet_radar_module,
    dpc_radar_module,
    meteofrance_radar_module,
    noaa_goes_rrqpe_module,
):
    modules = _provider_modules(
        kmi_module,
        knmi_module,
        rainviewer_module,
        aemet_radar_module,
        dpc_radar_module,
        meteofrance_radar_module,
        noaa_goes_rrqpe_module,
    )
    for module in modules:
        response = _AsyncResponse(chunks, headers=headers)
        with pytest.raises(ValueError):
            await module._read_bounded_response(
                response,
                maximum_bytes=4,
            )
        assert response.content.iterated == 0


@pytest.mark.asyncio
async def test_runtime_readers_reject_lying_content_length(
    kmi_module,
    knmi_module,
    rainviewer_module,
    aemet_radar_module,
    dpc_radar_module,
    meteofrance_radar_module,
    noaa_goes_rrqpe_module,
):
    modules = _provider_modules(
        kmi_module,
        knmi_module,
        rainviewer_module,
        aemet_radar_module,
        dpc_radar_module,
        meteofrance_radar_module,
        noaa_goes_rrqpe_module,
    )
    for module in modules:
        response = _AsyncResponse(
            (b"12", b"34"),
            headers={"Content-Length": "3"},
        )
        with pytest.raises(ValueError, match="payloadlengte"):
            await module._read_bounded_response(
                response,
                maximum_bytes=4,
            )


@pytest.mark.asyncio
async def test_runtime_readers_reject_oversized_stream_chunk(
    kmi_module,
    knmi_module,
    rainviewer_module,
    aemet_radar_module,
    dpc_radar_module,
    meteofrance_radar_module,
    noaa_goes_rrqpe_module,
):
    modules = _provider_modules(
        kmi_module,
        knmi_module,
        rainviewer_module,
        aemet_radar_module,
        dpc_radar_module,
        meteofrance_radar_module,
        noaa_goes_rrqpe_module,
    )
    for module in modules:
        response = _AsyncResponse((b"12345",))
        with pytest.raises(ValueError, match="contractlimiet"):
            await module._read_bounded_response(
                response,
                maximum_bytes=4,
            )


@pytest.mark.asyncio
async def test_runtime_readers_reject_transport_compression(
    kmi_module,
    knmi_module,
    rainviewer_module,
    aemet_radar_module,
    dpc_radar_module,
    meteofrance_radar_module,
    noaa_goes_rrqpe_module,
):
    modules = _provider_modules(
        kmi_module,
        knmi_module,
        rainviewer_module,
        aemet_radar_module,
        dpc_radar_module,
        meteofrance_radar_module,
        noaa_goes_rrqpe_module,
    )
    for module in modules:
        response = _AsyncResponse(
            (b"1234",),
            headers={"Content-Encoding": "gzip"},
        )
        with pytest.raises(ValueError, match="transportcompressie"):
            await module._read_bounded_response(
                response,
                maximum_bytes=4,
            )
        assert response.content.iterated == 0
