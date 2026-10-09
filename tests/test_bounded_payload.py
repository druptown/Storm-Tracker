from __future__ import annotations

import asyncio
import gzip
import hashlib
import io
from pathlib import Path
import zlib

import pytest

from storm_tracker_worker.tracking_core.engine.bounded_payload import (
    decompress_limited,
    iter_limited_async,
    read_limited_stream,
)


def test_worker_and_ha_modules_are_exact_mirrors():
    root = Path(__file__).parents[1]
    paths = (
        root / "custom_components/storm_tracker_v3/engine/bounded_payload.py",
        root / "storm_tracker_worker/tracking_core/engine/bounded_payload.py",
    )
    assert len({hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}) == 1


@pytest.mark.parametrize(
    ("codec", "encoded"),
    (
        ("gzip", gzip.compress(b"weather", mtime=0)),
        ("zlib", zlib.compress(b"weather")),
    ),
)
def test_exact_single_stream_round_trip(codec, encoded):
    assert decompress_limited(
        encoded,
        codec=codec,
        maximum_compressed_bytes=len(encoded),
        maximum_uncompressed_bytes=7,
    ) == b"weather"


def test_decompression_is_bounded_during_expansion():
    compressed = gzip.compress(b"x" * 100_000, mtime=0)
    with pytest.raises(ValueError, match="uitgepakte payload"):
        decompress_limited(
            compressed,
            codec="gzip",
            maximum_compressed_bytes=len(compressed),
            maximum_uncompressed_bytes=1024,
        )


@pytest.mark.parametrize(
    "payload",
    (
        gzip.compress(b"one", mtime=0) + gzip.compress(b"two", mtime=0),
        gzip.compress(b"one", mtime=0) + b"trailing",
    ),
)
def test_concatenated_or_trailing_gzip_is_rejected(payload):
    with pytest.raises(ValueError, match="extra leden|trailing"):
        decompress_limited(
            payload,
            codec="gzip",
            maximum_compressed_bytes=len(payload),
            maximum_uncompressed_bytes=100,
        )


def test_expansion_ratio_is_an_explicit_independent_contract():
    compressed = gzip.compress(b"repeated-" * 500, mtime=0)
    with pytest.raises(ValueError, match="uitgepakte payload"):
        decompress_limited(
            compressed,
            codec="gzip",
            maximum_compressed_bytes=len(compressed),
            maximum_uncompressed_bytes=100_000,
            maximum_expansion_ratio=2.0,
        )


def test_sync_stream_rejects_oversize_and_declared_length_mismatch():
    with pytest.raises(ValueError, match="overschrijdt"):
        read_limited_stream(io.BytesIO(b"12345"), maximum_bytes=4)
    with pytest.raises(ValueError, match="af van"):
        read_limited_stream(
            io.BytesIO(b"123"),
            maximum_bytes=4,
            declared_bytes=4,
            require_declared_match=True,
        )


def test_async_iterator_is_bounded_without_response_read_all():
    async def chunks():
        yield b"12"
        yield b"34"

    assert asyncio.run(iter_limited_async(
        chunks(),
        maximum_bytes=4,
        declared_bytes="4",
        require_declared_match=True,
    )) == b"1234"

    async def too_large():
        yield b"123"
        yield b"45"

    with pytest.raises(ValueError, match="overschrijdt"):
        asyncio.run(iter_limited_async(too_large(), maximum_bytes=4))
