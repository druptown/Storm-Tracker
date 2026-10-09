"""Pure, fail-closed helpers for bounded byte and compression handling.

The limits in this module are operational resource contracts.  They never
inspect meteorological values and therefore cannot reject weather because it
looks unusual.  Callers must supply limits that belong to the upstream
product contract; this module deliberately has no provider defaults.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
import math
from typing import Any, BinaryIO
import zlib


_READ_CHUNK_BYTES = 64 * 1024
_CODEC_WBITS = {
    "gzip": zlib.MAX_WBITS | 16,
    "zlib": zlib.MAX_WBITS,
}


def _limit(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} moet een positief geheel getal zijn")
    return value


def _declared_length(value: Any, maximum_bytes: int) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("opgegeven payloadlengte is ongeldig")
    try:
        declared = int(value)
    except (TypeError, ValueError) as err:
        raise ValueError("opgegeven payloadlengte is ongeldig") from err
    if declared < 0:
        raise ValueError("opgegeven payloadlengte is negatief")
    if declared > maximum_bytes:
        raise ValueError("opgegeven payloadlengte overschrijdt contractlimiet")
    return declared


def read_limited_stream(
    stream: BinaryIO,
    *,
    maximum_bytes: int,
    declared_bytes: Any = None,
    require_declared_match: bool = False,
) -> bytes:
    """Read a synchronous byte stream without ever accepting an oversized body."""

    maximum = _limit(maximum_bytes, "maximum_bytes")
    declared = _declared_length(declared_bytes, maximum)
    chunks: list[bytes] = []
    total = 0
    while total <= maximum:
        request_bytes = min(_READ_CHUNK_BYTES, maximum + 1 - total)
        chunk = stream.read(request_bytes)
        if not chunk:
            break
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise TypeError("payloadstream moet bytes opleveren")
        immutable = bytes(chunk)
        chunks.append(immutable)
        total += len(immutable)
        if total > maximum:
            raise ValueError("payload overschrijdt contractlimiet")
    payload = b"".join(chunks)
    if require_declared_match and declared is not None and len(payload) != declared:
        raise ValueError("payloadlengte wijkt af van de opgegeven lengte")
    return payload


async def iter_limited_async(
    chunks: AsyncIterator[bytes],
    *,
    maximum_bytes: int,
    declared_bytes: Any = None,
    require_declared_match: bool = False,
) -> bytes:
    """Collect an async byte iterator under the same explicit byte contract."""

    maximum = _limit(maximum_bytes, "maximum_bytes")
    declared = _declared_length(declared_bytes, maximum)
    parts: list[bytes] = []
    total = 0
    async for chunk in chunks:
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise TypeError("payloadstream moet bytes opleveren")
        immutable = bytes(chunk)
        total += len(immutable)
        if total > maximum:
            raise ValueError("payload overschrijdt contractlimiet")
        parts.append(immutable)
    payload = b"".join(parts)
    if require_declared_match and declared is not None and len(payload) != declared:
        raise ValueError("payloadlengte wijkt af van de opgegeven lengte")
    return payload


def decompress_limited(
    payload: bytes,
    *,
    codec: str,
    maximum_compressed_bytes: int,
    maximum_uncompressed_bytes: int,
    maximum_expansion_ratio: float | None = None,
) -> bytes:
    """Decode exactly one gzip/zlib stream with bounded output.

    Concatenated members and trailing bytes are rejected because they make
    hashing, provenance and product identity ambiguous.
    """

    compressed_limit = _limit(
        maximum_compressed_bytes,
        "maximum_compressed_bytes",
    )
    uncompressed_limit = _limit(
        maximum_uncompressed_bytes,
        "maximum_uncompressed_bytes",
    )
    if not isinstance(payload, bytes):
        raise TypeError("gecomprimeerde payload moet bytes zijn")
    if not payload:
        raise ValueError("gecomprimeerde payload is leeg")
    if len(payload) > compressed_limit:
        raise ValueError("gecomprimeerde payload overschrijdt contractlimiet")
    try:
        wbits = _CODEC_WBITS[codec]
    except KeyError as err:
        raise ValueError(f"niet-ondersteunde compressiecodec: {codec}") from err

    ratio_limit: float | None = None
    if maximum_expansion_ratio is not None:
        ratio_limit = float(maximum_expansion_ratio)
        if not math.isfinite(ratio_limit) or ratio_limit < 1.0:
            raise ValueError(
                "maximum_expansion_ratio moet eindig en minstens 1 zijn"
            )
    effective_limit = uncompressed_limit
    if ratio_limit is not None:
        effective_limit = min(
            effective_limit,
            max(1, math.floor(len(payload) * ratio_limit)),
        )

    decoder = zlib.decompressobj(wbits)
    output = bytearray()
    pending = payload
    try:
        while pending:
            remaining = effective_limit - len(output)
            decoded = decoder.decompress(pending, remaining + 1)
            output.extend(decoded)
            if len(output) > effective_limit:
                raise ValueError(
                    "uitgepakte payload overschrijdt contractlimiet"
                )
            pending = decoder.unconsumed_tail
            if decoder.eof:
                break
            if not pending:
                break
        remaining = effective_limit - len(output)
        output.extend(decoder.flush(remaining + 1))
    except zlib.error as err:
        raise ValueError("gecomprimeerde payload is ongeldig") from err

    if len(output) > effective_limit:
        raise ValueError("uitgepakte payload overschrijdt contractlimiet")
    if not decoder.eof:
        raise ValueError("gecomprimeerde payload is onvolledig")
    if decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError(
            "gecomprimeerde payload bevat extra leden of trailing data"
        )
    return bytes(output)


__all__ = [
    "decompress_limited",
    "iter_limited_async",
    "read_limited_stream",
]
