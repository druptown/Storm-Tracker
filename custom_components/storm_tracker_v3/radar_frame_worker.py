"""Thin HA adapter for regional raster construction in the worker."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
import uuid

from .engine.radar_frame import RadarFrame
from .engine.bounded_payload import decompress_limited, read_limited_stream

ARTIFACT_ROOT = Path("/share/storm_tracker_worker")
MAX_ARTIFACT_BYTES = 32 * 1024 * 1024


def _write_input(overlays: dict, regions: dict) -> dict:
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    value = {
        "artifact_type": "radar_overlays",
        "overlays": overlays,
        "regions": regions,
    }
    encoded = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    if len(encoded) > MAX_ARTIFACT_BYTES:
        raise ValueError("Radarinvoer voor worker is te groot")
    compressed = gzip.compress(encoded, compresslevel=3, mtime=0)
    name = f"radar-input-{uuid.uuid4()}.json.gz"
    target = ARTIFACT_ROOT / name
    with tempfile.NamedTemporaryFile(
        dir=ARTIFACT_ROOT, prefix=".radar-input-", delete=False
    ) as temporary:
        temporary.write(compressed)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    temporary_path.replace(target)
    return {
        "artifact_path": str(target),
        "artifact_bytes": len(compressed),
        "artifact_sha256": hashlib.sha256(compressed).hexdigest(),
    }


def _read_output(metadata: dict) -> tuple[dict, dict]:
    # Use the same strict path/size/hash envelope, but decode this artifact's
    # own schema because it contains regional frames rather than observations.
    raw_path = metadata.get("artifact_path")
    root = ARTIFACT_ROOT.resolve()
    path = Path(str(raw_path)).resolve()
    if root not in path.parents:
        raise ValueError("Rasterworkerartifact staat buiten gedeelde map")
    try:
        expected_bytes = int(metadata.get("artifact_bytes", -1))
    except (TypeError, ValueError) as err:
        raise ValueError(
            "Rasterworkerartifact heeft een ongeldige grootte"
        ) from err
    if (
        expected_bytes <= 0
        or expected_bytes > MAX_ARTIFACT_BYTES
        or path.stat().st_size != expected_bytes
    ):
        raise ValueError("Rasterworkerartifact faalt grootte- of hashcontrole")
    with path.open("rb") as stream:
        compressed = read_limited_stream(
            stream,
            maximum_bytes=MAX_ARTIFACT_BYTES,
            declared_bytes=expected_bytes,
            require_declared_match=True,
        )
    if (
        hashlib.sha256(compressed).hexdigest()
        != metadata.get("artifact_sha256")
    ):
        raise ValueError("Rasterworkerartifact faalt grootte- of hashcontrole")
    decoded = decompress_limited(
        compressed,
        codec="gzip",
        maximum_compressed_bytes=MAX_ARTIFACT_BYTES,
        maximum_uncompressed_bytes=MAX_ARTIFACT_BYTES,
    )
    value = json.loads(decoded)
    if value.get("artifact_type") != "regional_radar_frames":
        raise ValueError("Rasterworkerartifact heeft verkeerd type")
    frames, overlays = {}, {}
    for engine_id, result in dict(value.get("regions") or {}).items():
        frame = RadarFrame.from_dict(result["frame"])
        if frame.height * frame.width > 640_000 or len(frame.cells) > 640_000:
            raise ValueError("Rasterworkerresultaat overschrijdt limiet")
        frames[engine_id] = frame
        overlays[engine_id] = dict(result["overlay"])
    return frames, overlays


class RadarFrameWorker:
    def __init__(self, hass) -> None:
        self.hass = hass
        self.executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="storm_tracker_raster_artifact"
        )

    async def async_process(self, overlays: dict, regions: dict) -> tuple[dict, dict]:
        from .worker_bridge import async_request_worker_job

        loop = asyncio.get_running_loop()
        metadata = await loop.run_in_executor(
            self.executor, _write_input, overlays, regions
        )
        result = await async_request_worker_job(
            self.hass, "radar_frame_operational", metadata, timeout=35
        )
        return await loop.run_in_executor(self.executor, _read_output, result)

    async def async_process_provider_artifacts(
        self, selections: dict
    ) -> tuple[dict, dict]:
        """Normalize directly from existing provider artifacts in the worker."""
        from .worker_bridge import async_request_worker_job

        result = await async_request_worker_job(
            self.hass,
            "radar_frame_from_provider_artifacts",
            {"selections": selections},
            timeout=35,
        )
        return await asyncio.get_running_loop().run_in_executor(
            self.executor, _read_output, result
        )

    async def async_run_io(self, function):
        """Run the small persistence tail outside HA's shared executor."""
        return await asyncio.get_running_loop().run_in_executor(
            self.executor, function
        )

    def close(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)
