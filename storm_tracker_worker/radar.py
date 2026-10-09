"""Radarjobs die volledig buiten Home Assistant Core draaien."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
import re
import tarfile
import threading
import time
from urllib.request import Request, urlopen

import h5py
import numpy as np
from pyproj import CRS, Transformer

from .artifacts import write_json_artifact
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)
from .tracking_core.engine.bounded_payload import read_limited_stream

RV_LATEST_URL = (
    "https://opendata.dwd.de/weather/radar/composite/rv/"
    "composite_rv_LATEST.tar"
)
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_FRAME_AGE_SECONDS = 15 * 60
MAX_ODIM_BYTES = 40 * 1024 * 1024
MAX_RV_ARCHIVE_MEMBERS = 64

DWD_RV_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
DWD_RV_RAW_ARTIFACT_ROOT = DWD_RV_RAW_SHARE_ROOT / "raw" / "dwd_rv"
DWD_RV_ARCHIVE_PRODUCT = "dwd_open_data_composite_rv_tar"
DWD_RV_CAPTURE_PRODUCT = "dwd_open_data_composite_rv_capture"
DWD_RV_RAW_COLLECTOR = "dwd_open_data_composite_rv"
DWD_RV_RAW_COLLECTOR_VERSION = "worker-dwd-rv-exact-raw-v1"
DWD_RV_PARSER_VERSION = "dwd-rv-odim-hdf5-legacy-components-v1"
DWD_RV_RAW_SEMANTIC_CLASS = "quantitative_precipitation_accumulation"
DWD_RV_RAW_QUANTITY = "precipitation_accumulation"
DWD_RV_RAW_UNIT = "mm"
DWD_RV_ACCUMULATION_MINUTES = 5.0
DWD_RV_FORECAST_LEAD_MINUTES = 0
DWD_RV_PROVENANCE_GRADE = "exact_raw"
DWD_RV_PROVENANCE_SCOPE = "upstream_bytes_only"
DWD_RV_ADMISSION_STATUS = "exact_raw_decode_uncertified"
DWD_RV_LEGACY_DECODER_QUALITY = 0.98
DWD_RV_LEGACY_SEMANTIC_CLASS = "legacy_categorical_components"
DWD_RV_LEGACY_INTENSITY_SEMANTICS = (
    "legacy_threshold_category_from_5_minute_accumulation"
)
DWD_RV_PRODUCTION_MEMBER_RE = re.compile(
    r"^composite_rv_(?P<date>\d{8})_(?P<time>\d{4})_000-hd5$"
)
DWD_RV_COMPAT_MEMBER_RE = re.compile(
    r"^composite_rv_[A-Za-z0-9_.-]{1,220}_000-hd5$"
)
_DWD_RV_RAW_STORE: RawArtifactStore | None = None
_DWD_RV_RAW_STORE_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class _DwdRvHttpResponse:
    """Exact DWD response bytes and receipt time, before any tar decode."""

    payload: bytes
    content_type: str
    received_at: float


@dataclass(frozen=True, slots=True)
class _DwdRvArchiveFrame:
    """One bounded +000 member selected without writing it to disk."""

    member_name: str
    payload: bytes
    filename_timestamp: float | None


def _text(value) -> str:
    return value.decode("ascii") if isinstance(value, bytes) else str(value)


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def _contains(area: dict, lat: float, lon: float) -> bool:
    return _haversine(
        float(area["center_lat"]),
        float(area["center_lon"]),
        lat,
        lon,
    ) <= float(area["horizon_km"])


def _intensity(rain_rate: float) -> int:
    if rain_rate < 0.1:
        return 0
    for level, threshold in enumerate((0.1, 0.5, 1, 2, 5, 10, 25), start=1):
        if rain_rate < threshold:
            return max(1, level - 1)
    return 8


def _pixel_components(
    values, *, maximum_components: int | None = None
) -> list[list[tuple[int, int]]]:
    remaining = {
        (int(row), int(column)) for row, column in np.argwhere(values > 0)
    }
    components = []
    while remaining:
        start = min(remaining)
        remaining.remove(start)
        pending = deque((start,))
        pixels = []
        while pending:
            row, column = pending.popleft()
            pixels.append((row, column))
            for neighbour in (
                (row - 1, column),
                (row + 1, column),
                (row, column - 1),
                (row, column + 1),
            ):
                if neighbour in remaining:
                    remaining.remove(neighbour)
                    pending.append(neighbour)
        components.append(pixels)
        if (
            maximum_components is not None
            and len(components) > int(maximum_components)
        ):
            raise ValueError("radarraster overschrijdt componentbudget")
    return sorted(components, key=min)


def _signed_grid_area(ring) -> float:
    return sum(
        col1 * row2 - col2 * row1
        for (row1, col1), (row2, col2) in zip(ring, ring[1:])
    ) / 2.0


def _simplify_grid_ring(ring) -> tuple[tuple[int, int], ...]:
    simplified = []
    for index, current in enumerate(ring[:-1]):
        previous = ring[index - 1 if index else -2]
        following = ring[(index + 1) % (len(ring) - 1)]
        if (
            previous[0] == current[0] == following[0]
            or previous[1] == current[1] == following[1]
        ):
            continue
        simplified.append(current)
    if len(simplified) < 3:
        simplified = list(ring[:-1])
    simplified.append(simplified[0])
    return tuple(simplified)


def _point_in_grid_ring(point, ring) -> bool:
    row, col = point
    inside = False
    previous_row, previous_col = ring[-1]
    for current_row, current_col in ring:
        if (current_row > row) != (previous_row > row):
            crossing_col = (
                (previous_col - current_col)
                * (row - current_row)
                / (previous_row - current_row)
                + current_col
            )
            if col < crossing_col:
                inside = not inside
        previous_row, previous_col = current_row, current_col
    return inside


def _trace_grid_loops(pixels):
    edges = set()
    for row, column in pixels:
        for edge in (
            ((row, column), (row, column + 1)),
            ((row, column + 1), (row + 1, column + 1)),
            ((row + 1, column + 1), (row + 1, column)),
            ((row + 1, column), (row, column)),
        ):
            reverse = (edge[1], edge[0])
            if reverse in edges:
                edges.remove(reverse)
            else:
                edges.add(edge)
    unused = set(edges)
    loops = []
    direction_index = {
        (0, 1): 0,
        (1, 0): 1,
        (0, -1): 2,
        (-1, 0): 3,
    }
    turn_rank = {1: 0, 0: 1, 3: 2, 2: 3}
    while unused:
        first = min(unused)
        unused.remove(first)
        ring = [first[0], first[1]]
        while ring[-1] != ring[0]:
            candidates = [
                edge for edge in unused if edge[0] == ring[-1]
            ]
            if not candidates:
                ring = []
                break
            previous = (
                ring[-1][0] - ring[-2][0],
                ring[-1][1] - ring[-2][1],
            )
            previous_index = direction_index[previous]

            def candidate_rank(edge):
                following = (
                    edge[1][0] - edge[0][0],
                    edge[1][1] - edge[0][1],
                )
                delta = (
                    direction_index[following] - previous_index
                ) % 4
                return turn_rank[delta], edge

            edge = min(candidates, key=candidate_rank)
            unused.remove(edge)
            ring.append(edge[1])
            if len(ring) > len(edges) + 1:
                ring = []
                break
        if len(ring) >= 4:
            loops.append(_simplify_grid_ring(ring))
    return loops


def _pixel_fallback_polygons(pixels):
    return tuple(
        ((
            (row, column),
            (row, column + 1),
            (row + 1, column + 1),
            (row + 1, column),
            (row, column),
        ),)
        for row, column in sorted(pixels)
    )


def _has_ambiguous_diagonal_contact(pixels) -> bool:
    occupied = {
        (int(row), int(column)) for row, column in pixels
    }
    for row, column in occupied:
        for row_delta, column_delta in ((1, 1), (1, -1)):
            diagonal = (row + row_delta, column + column_delta)
            if diagonal not in occupied:
                continue
            bridges = (
                (row + row_delta, column),
                (row, column + column_delta),
            )
            if not any(bridge in occupied for bridge in bridges):
                return True
    return False


def _grid_polygons(pixels, *, fail_closed: bool = False):
    pixels = tuple(
        (int(row), int(column)) for row, column in pixels
    )
    if fail_closed and _has_ambiguous_diagonal_contact(pixels):
        return ()
    loops = _trace_grid_loops(pixels)
    signed = [(_signed_grid_area(ring), ring) for ring in loops]
    if (
        not signed
        or abs(sum(area for area, _ in signed) - len(pixels)) > 1e-9
    ):
        return () if fail_closed else _pixel_fallback_polygons(pixels)
    outers = [(area, ring) for area, ring in signed if area > 0.0]
    holes = [(area, ring) for area, ring in signed if area < 0.0]
    if not outers or (fail_closed and len(outers) != 1):
        return () if fail_closed else _pixel_fallback_polygons(pixels)
    outers.sort(key=lambda item: (min(item[1]), -item[0]))
    polygons = [[ring] for _, ring in outers]
    for _, hole in sorted(holes, key=lambda item: min(item[1])):
        containers = [
            (outer_area, index)
            for index, (outer_area, outer) in enumerate(outers)
            if _point_in_grid_ring(hole[0], outer)
        ]
        if not containers:
            return () if fail_closed else _pixel_fallback_polygons(pixels)
        _, container_index = min(containers)
        polygons[container_index].append(hole)
    return tuple(tuple(rings) for rings in polygons)


def _grid_boundary(pixels) -> list[tuple[int, int]]:
    polygons = _grid_polygons(pixels)
    if not polygons:
        return []
    largest = max(
        (polygon[0] for polygon in polygons),
        key=lambda ring: abs(_signed_grid_area(ring)),
    )
    return list(largest)


def _download_rv_response(url: str = RV_LATEST_URL) -> _DwdRvHttpResponse:
    """Read one exact, bounded RV response without inspecting its body."""
    request = Request(
        url,
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/1.16",
        },
    )
    with urlopen(request, timeout=20) as response:
        headers = getattr(response, "headers", None)
        encoding = (
            str(headers.get("Content-Encoding") or "identity")
            if headers is not None
            else "identity"
        )
        if encoding.lower() != "identity":
            raise ValueError("onverwachte DWD-transportcompressie")
        declared = headers.get("Content-Length") if headers is not None else None
        payload = read_limited_stream(
            response,
            maximum_bytes=MAX_ARCHIVE_BYTES,
            declared_bytes=declared,
            require_declared_match=declared is not None,
        )
        received_at = time.time()
        content_type = (
            str(headers.get("Content-Type") or "")
            if headers is not None
            else ""
        )
        content_type = content_type.split(";", 1)[0].strip().lower()
        return _DwdRvHttpResponse(
            payload=payload,
            content_type=content_type or "application/x-tar",
            received_at=received_at,
        )


def _download(url: str = RV_LATEST_URL) -> bytes:
    """Compatibility wrapper returning only the bounded RV response body."""
    return _download_rv_response(url).payload


def _download_bounded(url: str, max_bytes: int) -> bytes:
    """Download één publiek radarbestand met een harde bovengrens."""
    if not url.startswith("https://"):
        raise ValueError("Radarworker accepteert uitsluitend HTTPS")
    limit = min(MAX_ODIM_BYTES, max(1, int(max_bytes)))
    request = Request(
        url,
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/0.3",
        },
    )
    with urlopen(request, timeout=25) as response:
        headers = getattr(response, "headers", None)
        declared = headers.get("Content-Length") if headers is not None else None
        return read_limited_stream(
            response,
            maximum_bytes=limit,
            declared_bytes=declared,
            require_declared_match=declared is not None,
        )


def parse_odim_rainfall(
    payload: bytes,
    areas: tuple[dict, ...],
    *,
    source: str,
    quality: float,
    max_age_seconds: float,
    sample_stride: int = 4,
    accumulation_minutes: float | None = None,
    now: float | None = None,
) -> dict:
    """Decodeer een nationaal ODIM-HDF5-raster buiten Home Assistant."""
    with h5py.File(io.BytesIO(payload), "r") as dataset:
        data = np.asarray(dataset["dataset1/data1/data"])
        data_what = dataset["dataset1/data1/what"].attrs
        frame_what = dataset["dataset1/what"].attrs
        where = dataset["where"].attrs
        timestamp = datetime.strptime(
            _text(frame_what["enddate"]) + _text(frame_what["endtime"]),
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=timezone.utc).timestamp()
        reference_now = (
            datetime.now(timezone.utc).timestamp() if now is None else now
        )
        if reference_now - timestamp > max_age_seconds:
            raise ValueError(f"{source}-frame is te oud")

        raw = data.astype(np.float64)
        decoded = (
            raw * float(data_what["gain"]) + float(data_what["offset"])
        )
        rate = (
            decoded
            if accumulation_minutes is None
            else decoded * (60.0 / accumulation_minutes)
        )
        valid = (
            (raw != float(data_what["nodata"]))
            & (raw != float(data_what["undetect"]))
            & (rate >= 0.1)
        )
        xscale = float(where["xscale"])
        yscale = float(where["yscale"])
        transformer = Transformer.from_crs(
            CRS.from_user_input(_text(where["projdef"])),
            "EPSG:4326",
            always_xy=True,
        )
        inverse = Transformer.from_crs(
            "EPSG:4326",
            CRS.from_user_input(_text(where["projdef"])),
            always_xy=True,
        )
        if "UL_lon" in where and "UL_lat" in where:
            ul_x, ul_y = inverse.transform(
                float(where["UL_lon"]), float(where["UL_lat"])
            )
        else:
            ul_x, ul_y = 0.0, int(where["ysize"]) * yscale
        intensity_grid = np.zeros(data.shape, dtype=np.uint8)
        for row, column in np.argwhere(valid):
            intensity_grid[row, column] = _intensity(float(rate[row, column]))

        def corner_to_latlon(row, column):
            lon, lat = transformer.transform(
                ul_x + column * xscale,
                ul_y - row * yscale,
            )
            return round(float(lat), 5), round(float(lon), 5)

        observations = []
        frame_id = f"{source}:{timestamp:.0f}"
        pixel_area_km2 = xscale * yscale / 1_000_000.0
        for index, pixels in enumerate(_pixel_components(intensity_grid)):
            centroid_row = sum(row + 0.5 for row, _ in pixels) / len(pixels)
            centroid_column = (
                sum(column + 0.5 for _, column in pixels) / len(pixels)
            )
            lon, lat = transformer.transform(
                ul_x + centroid_column * xscale,
                ul_y - centroid_row * yscale,
            )
            if areas and not any(_contains(area, lat, lon) for area in areas):
                continue
            polygons = tuple(
                tuple(
                    tuple(
                        corner_to_latlon(float(row), float(column))
                        for row, column in ring
                    )
                    for ring in polygon
                )
                for polygon in _grid_polygons(pixels)
            )
            boundary = polygons[0][0] if polygons else ()
            component_id = f"{frame_id}:c{index}"
            area_km2 = len(pixels) * pixel_area_km2
            observations.append({
                "obs_type": "radar",
                "lat": float(lat),
                "lon": float(lon),
                "timestamp": timestamp,
                "intensity": max(
                    int(intensity_grid[row, column])
                    for row, column in pixels
                ),
                "area_km2": area_km2,
                "quality": float(quality),
                "footprint_points": boundary,
                "footprint_polygons": polygons,
                "radar_cell_id": component_id,
                "parent_system_id": component_id,
                "parent_area_km2": area_km2,
                "parent_footprint_points": boundary,
                "parent_footprint_polygons": polygons,
                "source": source,
            })

        overlay_runs = []
        max_run = max(1, int(sample_stride) * 4)
        for row in range(intensity_grid.shape[0]):
            column = 0
            while column < intensity_grid.shape[1]:
                intensity = int(intensity_grid[row, column])
                if intensity <= 0:
                    column += 1
                    continue
                end = column + 1
                while (
                    end < min(intensity_grid.shape[1], column + max_run)
                    and int(intensity_grid[row, end]) == intensity
                ):
                    end += 1
                center_lat, center_lon = corner_to_latlon(
                    row + 0.5, (column + end) / 2.0
                )
                if not areas or any(
                    _contains(area, center_lat, center_lon) for area in areas
                ):
                    overlay_runs.append({
                        "intensity": intensity,
                        "ring": (
                            corner_to_latlon(row, column),
                            corner_to_latlon(row, end),
                            corner_to_latlon(row + 1, end),
                            corner_to_latlon(row + 1, column),
                        ),
                    })
                column = end

    return {
        "provider": source,
        "frame_timestamp": timestamp,
        "observations": observations,
        "overlay": {
            "source": source,
            "timestamp": timestamp,
            "runs": overlay_runs,
        },
    }


def odim_hdf5_fetch(job) -> dict:
    """Download en decodeer één begrensd nationaal ODIM-HDF5-product."""
    payload = job.payload
    source = str(payload["source"])
    if source not in {"met_office_radar"}:
        raise ValueError("ODIM-provider is niet toegestaan")
    result = parse_odim_rainfall(
        _download_bounded(
            str(payload["url"]),
            int(payload.get("max_file_bytes", MAX_ODIM_BYTES)),
        ),
        tuple(payload.get("areas") or ()),
        source=source,
        quality=float(payload["quality"]),
        max_age_seconds=float(payload["max_age_seconds"]),
        sample_stride=int(payload.get("sample_stride", 4)),
        accumulation_minutes=payload.get("accumulation_minutes"),
    )
    artifact = write_json_artifact(job.job_id, result)
    return {
        **artifact,
        "provider": source,
        "frame_timestamp": result["frame_timestamp"],
        "observation_count": len(result["observations"]),
    }


def _dwd_rv_raw_store(
    injected: RawArtifactStore | None = None,
) -> RawArtifactStore:
    """Return the worker-bounded DWD store, injectable for isolated tests."""
    if injected is not None:
        injected.initialize()
        return injected
    share_root = DWD_RV_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = DWD_RV_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("DWD raw-artifactpad ontsnapt uit worker-share")
    global _DWD_RV_RAW_STORE
    with _DWD_RV_RAW_STORE_LOCK:
        if _DWD_RV_RAW_STORE is None:
            candidate = RawArtifactStore(raw_root)
            candidate.initialize()
            _DWD_RV_RAW_STORE = candidate
        return _DWD_RV_RAW_STORE


def _dwd_rv_native_provenance() -> dict[str, object]:
    """Describe the requested upstream product without claiming decode success."""
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": DWD_RV_RAW_COLLECTOR,
        "collector_version": DWD_RV_RAW_COLLECTOR_VERSION,
        "parser_version": DWD_RV_PARSER_VERSION,
        "product": DWD_RV_ARCHIVE_PRODUCT,
        "declared_semantic_class": DWD_RV_RAW_SEMANTIC_CLASS,
        "declared_quantity": DWD_RV_RAW_QUANTITY,
        "declared_unit": DWD_RV_RAW_UNIT,
        "declared_accumulation_minutes": DWD_RV_ACCUMULATION_MINUTES,
        "requested_forecast_lead_minutes": DWD_RV_FORECAST_LEAD_MINUTES,
        "quantitative_semantics_verified_after_decode": False,
    }


def _dwd_rv_filename_timestamp(member_name: str) -> float | None:
    match = DWD_RV_PRODUCTION_MEMBER_RE.fullmatch(member_name)
    if match is None:
        return None
    return datetime.strptime(
        match.group("date") + match.group("time"), "%Y%m%d%H%M"
    ).replace(tzinfo=timezone.utc).timestamp()


def _read_rv_archive_frame(
    payload: bytes,
    *,
    require_production_name: bool,
) -> _DwdRvArchiveFrame:
    """Select and bounded-read exactly one direct-child RV +000 member."""
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:") as archive:
        archive_members = archive.getmembers()
        if len(archive_members) > MAX_RV_ARCHIVE_MEMBERS:
            raise ValueError("DWD RV-archief overschrijdt ledenlimiet")
        candidates = []
        for member in archive_members:
            name = str(member.name)
            if not name.endswith("_000-hd5"):
                continue
            if "/" in name or "\\" in name or not member.isfile():
                raise ValueError("DWD RV +000-lid heeft een onveilig type of pad")
            if require_production_name:
                if DWD_RV_PRODUCTION_MEMBER_RE.fullmatch(name) is not None:
                    candidates.append(member)
            elif DWD_RV_COMPAT_MEMBER_RE.fullmatch(name) is not None:
                candidates.append(member)
        if not candidates:
            raise ValueError("DWD RV-archief bevat geen actueel HDF5-frame")
        if len(candidates) != 1:
            raise ValueError("DWD RV-archief bevat meerdere +000-frames")
        member = candidates[0]
        if member.size <= 0 or member.size > MAX_ODIM_BYTES:
            raise ValueError("DWD RV-frame overschrijdt productlimiet")
        stream = archive.extractfile(member)
        if stream is None:
            raise ValueError("DWD RV-frame kan niet worden gelezen")
        frame = read_limited_stream(
            stream,
            maximum_bytes=MAX_ODIM_BYTES,
            declared_bytes=member.size,
            require_declared_match=True,
        )
        return _DwdRvArchiveFrame(
            member_name=str(member.name),
            payload=frame,
            filename_timestamp=_dwd_rv_filename_timestamp(str(member.name)),
        )


def _dwd_rv_raw_source_reference(
    receipt: RawArtifactReceipt,
    *,
    member_name: str,
    product_timestamp: float,
) -> dict[str, object]:
    provenance = receipt.provenance
    return {
        "role": "rv_tar",
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "raw_receipt_sha256": receipt.receipt_sha256,
        "fetch_id": receipt.fetch_id,
        "upstream_record_id": receipt.upstream_id,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "archive_member_name": member_name,
        "product_timestamp": product_timestamp,
        "parser_version": provenance["parser_version"],
        "collector_version": provenance["collector_version"],
        "provenance_grade": DWD_RV_PROVENANCE_GRADE,
        "provenance_scope": DWD_RV_PROVENANCE_SCOPE,
        "semantic_class": DWD_RV_RAW_SEMANTIC_CLASS,
        "quantity": DWD_RV_RAW_QUANTITY,
        "unit": DWD_RV_RAW_UNIT,
        "accumulation_minutes": DWD_RV_ACCUMULATION_MINUTES,
        "forecast_lead_minutes": DWD_RV_FORECAST_LEAD_MINUTES,
        "quantitative_semantics_eligible": True,
    }


def _dwd_rv_capture_provenance(
    capture: RawArtifactCaptureReceipt,
    *,
    receipt: RawArtifactReceipt,
    member_name: str,
) -> dict[str, object]:
    product_timestamp = capture.product_timestamp
    if product_timestamp is None:  # pragma: no cover - caller invariant
        raise RuntimeError("DWD RV-capture mist producttijd")
    return {
        "raw_store_schema_version": capture.raw_store_schema_version,
        "raw_capture_id": capture.capture_id,
        "raw_capture_manifest_sha256": capture.manifest_sha256,
        "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
        "raw_capture_fetch_id": capture.fetch_id,
        "upstream_capture_id": capture.upstream_capture_id,
        "archive_member_name": member_name,
        "product_timestamp": product_timestamp,
        "collector_version": DWD_RV_RAW_COLLECTOR_VERSION,
        "parser_versions": {"rv_tar": DWD_RV_PARSER_VERSION},
        "provenance_grade": DWD_RV_PROVENANCE_GRADE,
        "provenance_scope": DWD_RV_PROVENANCE_SCOPE,
        "raw_sources": {
            "rv_tar": _dwd_rv_raw_source_reference(
                receipt,
                member_name=member_name,
                product_timestamp=product_timestamp,
            )
        },
    }


def parse_rv_archive(
    payload: bytes,
    areas: tuple[dict, ...],
    *,
    now: float | None = None,
    require_production_name: bool = False,
) -> dict:
    """Decodeer één actueel DWD-frame naar overdraagbare radarobjecten."""
    archive_frame = _read_rv_archive_frame(
        payload,
        require_production_name=require_production_name,
    )
    frame = archive_frame.payload

    with h5py.File(io.BytesIO(frame), "r") as dataset:
        data = np.asarray(dataset["dataset1/data1/data"])
        data_what = dataset["dataset1/data1/what"].attrs
        frame_what = dataset["dataset1/what"].attrs
        where = dataset["where"].attrs
        timestamp = datetime.strptime(
            _text(frame_what["enddate"]) + _text(frame_what["endtime"]),
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=timezone.utc).timestamp()
        if (
            archive_frame.filename_timestamp is not None
            and abs(timestamp - archive_frame.filename_timestamp) > 1.0
        ):
            raise ValueError(
                "DWD RV-producttijd verschilt tussen tar-lid en HDF5"
            )
        reference_now = datetime.now(timezone.utc).timestamp() if now is None else now
        if reference_now - timestamp > MAX_FRAME_AGE_SECONDS:
            raise ValueError("DWD RV-frame is ouder dan 15 minuten")

        raw = data.astype(np.float64)
        rate = (
            raw * float(data_what["gain"]) + float(data_what["offset"])
        ) * 12.0
        valid = (
            (raw != float(data_what["nodata"]))
            & (raw != float(data_what["undetect"]))
            & (rate >= 0.1)
        )
        xscale = float(where["xscale"])
        yscale = float(where["yscale"])
        ysize = int(where["ysize"])
        transformer = Transformer.from_crs(
            CRS.from_user_input(_text(where["projdef"])),
            "EPSG:4326",
            always_xy=True,
        )
        inverse = Transformer.from_crs(
            "EPSG:4326",
            CRS.from_user_input(_text(where["projdef"])),
            always_xy=True,
        )
        if "UL_lon" in where and "UL_lat" in where:
            ul_x, ul_y = inverse.transform(
                float(where["UL_lon"]), float(where["UL_lat"])
            )
        else:
            ul_x, ul_y = 0.0, ysize * yscale
        intensity_grid = np.zeros(data.shape, dtype=np.uint8)
        for row, column in np.argwhere(valid):
            intensity_grid[row, column] = _intensity(float(rate[row, column]))

        def corner_to_latlon(row, column):
            lon, lat = transformer.transform(
                ul_x + column * xscale,
                ul_y - row * yscale,
            )
            return round(float(lat), 5), round(float(lon), 5)

        observations = []
        overlay_runs = []
        frame_id = f"dwd_radolan:{timestamp:.0f}"
        pixel_area_km2 = xscale * yscale / 1_000_000.0
        for index, pixels in enumerate(_pixel_components(intensity_grid)):
            centroid_row = sum(row + 0.5 for row, _ in pixels) / len(pixels)
            centroid_column = sum(column + 0.5 for _, column in pixels) / len(pixels)
            lon, lat = transformer.transform(
                ul_x + centroid_column * xscale,
                ul_y - centroid_row * yscale,
            )
            if areas and not any(_contains(area, lat, lon) for area in areas):
                continue
            polygons = tuple(
                tuple(
                    tuple(
                        corner_to_latlon(float(row), float(column))
                        for row, column in ring
                    )
                    for ring in polygon
                )
                for polygon in _grid_polygons(pixels)
            )
            boundary = polygons[0][0] if polygons else ()
            component_id = f"{frame_id}:c{index}"
            observations.append(
                {
                    "obs_type": "radar",
                    "lat": float(lat),
                    "lon": float(lon),
                    "timestamp": timestamp,
                    "intensity": max(
                        int(intensity_grid[row, column])
                        for row, column in pixels
                    ),
                    "area_km2": len(pixels) * pixel_area_km2,
                    "quality": 0.98,
                    "footprint_points": boundary,
                    "footprint_polygons": polygons,
                    "radar_cell_id": component_id,
                    "parent_system_id": component_id,
                    "parent_area_km2": len(pixels) * pixel_area_km2,
                    "parent_footprint_points": boundary,
                    "parent_footprint_polygons": polygons,
                    "source": "dwd_radolan",
                }
            )

        for row in range(intensity_grid.shape[0]):
            column = 0
            while column < intensity_grid.shape[1]:
                intensity = int(intensity_grid[row, column])
                if intensity <= 0:
                    column += 1
                    continue
                end = column + 1
                while (
                    end < min(intensity_grid.shape[1], column + 16)
                    and int(intensity_grid[row, end]) == intensity
                ):
                    end += 1
                center_lat, center_lon = corner_to_latlon(
                    row + 0.5, (column + end) / 2
                )
                if not areas or any(
                    _contains(area, center_lat, center_lon) for area in areas
                ):
                    overlay_runs.append(
                        {
                            "intensity": intensity,
                            "ring": (
                                corner_to_latlon(row, column),
                                corner_to_latlon(row, end),
                                corner_to_latlon(row + 1, end),
                                corner_to_latlon(row + 1, column),
                            ),
                        }
                    )
                column = end

    return {
        "provider": "dwd_radolan",
        "archive_member_name": archive_frame.member_name,
        "frame_timestamp": timestamp,
        "observations": observations,
        "overlay": {
            "source": "dwd_radolan",
            "timestamp": timestamp,
            "runs": overlay_runs,
        },
    }


def dwd_radolan_fetch(
    job,
    *,
    raw_store: RawArtifactStore | None = None,
) -> dict:
    """Capture exact RV tar bytes, then decode a legacy diagnostic artifact."""
    job_id = str(job.job_id)
    areas = tuple(job.payload.get("areas") or ())
    store = _dwd_rv_raw_store(raw_store)
    response = _download_rv_response()
    archive_sha256 = hashlib.sha256(response.payload).hexdigest()
    receipt = store.store(
        provider="dwd_radolan",
        product=DWD_RV_ARCHIVE_PRODUCT,
        fetch_id=f"{job_id}:rv_tar",
        content_type=response.content_type,
        received_at=response.received_at,
        upstream_id=(
            f"{DWD_RV_ARCHIVE_PRODUCT}|lead_minutes=0"
            f"|content_sha256={archive_sha256}"
        ),
        provenance=_dwd_rv_native_provenance(),
        payload=response.payload,
    )

    # Nothing above this line opens the tar or HDF5 payload. A malformed
    # response therefore retains its exact receipt but can never become a
    # complete capture or a decoded weather result.
    result = parse_rv_archive(
        response.payload,
        areas,
        require_production_name=True,
    )
    timestamp = float(result["frame_timestamp"])
    member_name = str(result["archive_member_name"])
    upstream_capture_id = (
        f"{DWD_RV_CAPTURE_PRODUCT}|member={member_name}"
        f"|product_timestamp={timestamp:.6f}"
        f"|content_sha256={archive_sha256}"
    )
    capture = store.commit_capture(
        provider="dwd_radolan",
        product=DWD_RV_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=upstream_capture_id,
        product_timestamp=timestamp,
        members={"rv_tar": receipt.receipt_id},
    )
    raw_provenance = _dwd_rv_capture_provenance(
        capture,
        receipt=receipt,
        member_name=member_name,
    )

    legacy_fields = {
        "semantic_class": DWD_RV_LEGACY_SEMANTIC_CLASS,
        "intensity_semantics": DWD_RV_LEGACY_INTENSITY_SEMANTICS,
        "quantitative_semantics_eligible": False,
        "operational_use": "legacy_diagnostic_only",
        "operational_admission_eligible": False,
        "admission_status": DWD_RV_ADMISSION_STATUS,
        "legacy_decoder_quality_constant": DWD_RV_LEGACY_DECODER_QUALITY,
        "legacy_decoder_quality_operational": False,
        "quality_semantics": "legacy_constant_not_provider_quality",
    }
    for observation in result["observations"]:
        observation.update(legacy_fields)
    result["overlay"].update(legacy_fields)
    result.update({
        "product": DWD_RV_ARCHIVE_PRODUCT,
        "semantic_class": DWD_RV_RAW_SEMANTIC_CLASS,
        "quantity": DWD_RV_RAW_QUANTITY,
        "unit": DWD_RV_RAW_UNIT,
        "accumulation_minutes": DWD_RV_ACCUMULATION_MINUTES,
        "forecast_lead_minutes": DWD_RV_FORECAST_LEAD_MINUTES,
        "quantitative_semantics_eligible": True,
        "operational_use": "raw_capture_with_legacy_diagnostic_decode",
        "operational_admission_eligible": False,
        "admission_status": DWD_RV_ADMISSION_STATUS,
        "decoded_semantic_class": DWD_RV_LEGACY_SEMANTIC_CLASS,
        "decoded_quantitative_semantics_eligible": False,
        "legacy_decoder_quality_constant": DWD_RV_LEGACY_DECODER_QUALITY,
        "legacy_decoder_quality_operational": False,
        "provenance_grade": DWD_RV_PROVENANCE_GRADE,
        "provenance_scope": DWD_RV_PROVENANCE_SCOPE,
        "raw_provenance": raw_provenance,
        "diagnostics": {
            "archive_member_name": member_name,
            "raw_capture_id": capture.capture_id,
            "raw_receipt_id": receipt.receipt_id,
            "decode_status": "legacy_non_operational",
        },
    })
    cell_ids = sorted(
        observation["radar_cell_id"]
        for observation in result["observations"]
    )
    cell_digest = hashlib.sha256(
        json.dumps(cell_ids, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    artifact = write_json_artifact(job_id, result)
    return {
        **artifact,
        "provider": "dwd_radolan",
        "product": DWD_RV_ARCHIVE_PRODUCT,
        "semantic_class": DWD_RV_RAW_SEMANTIC_CLASS,
        "quantity": DWD_RV_RAW_QUANTITY,
        "unit": DWD_RV_RAW_UNIT,
        "accumulation_minutes": DWD_RV_ACCUMULATION_MINUTES,
        "forecast_lead_minutes": DWD_RV_FORECAST_LEAD_MINUTES,
        "quantitative_semantics_eligible": True,
        "operational_use": "raw_capture_with_legacy_diagnostic_decode",
        "operational_admission_eligible": False,
        "admission_status": DWD_RV_ADMISSION_STATUS,
        "decoded_semantic_class": DWD_RV_LEGACY_SEMANTIC_CLASS,
        "decoded_quantitative_semantics_eligible": False,
        "legacy_decoder_quality_constant": DWD_RV_LEGACY_DECODER_QUALITY,
        "legacy_decoder_quality_operational": False,
        "provenance_grade": DWD_RV_PROVENANCE_GRADE,
        "provenance_scope": DWD_RV_PROVENANCE_SCOPE,
        "raw_provenance": raw_provenance,
        "frame_timestamp": result["frame_timestamp"],
        "observation_count": len(result["observations"]),
        "cell_ids_sha256": cell_digest,
        "diagnostics": result["diagnostics"],
    }
