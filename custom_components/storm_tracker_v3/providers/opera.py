"""Storm Tracker V3 — providers/opera.py v0.3.0

Provider: OPERA radar composiet via EUMETNET MeteoGate S3

Verantwoordelijkheden:
  - Meest recente OPERA DBZH composiet betrouwbaar opsporen via S3 paginering
  - HDF5 download delen via module-level cache (één download voor meerdere engines)
  - Lokale bbox slice op basis van center + radius (niet heel Europa)
  - Productleeftijd valideren (max 15 min oud)
  - Poll-overlap voorkomen via asyncio.Lock
  - HDF5 parsing non-blocking via async_add_executor_job

Datakwaliteit:
  - 1km² resolutie (OPERA CIRRUS composiet)
  - Gecorrigeerd Europees composiet van ~180 radarstations
  - Quality score per pixel (0-1)
  - Open S3 bucket — geen API key vereist
  - Europese dekking, update elke 5 minuten

S3 bucket: s3://openradar-24h (open access)
Endpoint:  https://s3.waw3-1.cloudferro.com

Dependencies: h5py, numpy, pyproj (in manifest.json requirements)

Versiegeschiedenis:
  v0.3.0 — adaptieve kernsegmentatie voor buitensporig grote, door lichte
            neerslag verbonden componenten
  v0.2.0 — S3 paginering via IsTruncated/NextContinuationToken;
            volledige S3-key bewaren; datum uit bestandsnaam afleiden;
            productleeftijd validatie (max 15 min);
            lokale bbox op basis van center + radius (niet heel Europa);
            module-level gedeelde downloadcache + asyncio.Lock;
            poll-overlap voorkomen via per-provider Lock
  v0.1.0 — eerste versie; HDF5 parsing correct maar heel Europa verwerkt;
            S3 discovery niet betrouwbaar; geen caching; geen poll-lock
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import io
import logging
import math
import re
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

import aiohttp

from ..engine.observation import Observation, ObservationType
from .raster_components import extract_intensity_runs

_LOGGER = logging.getLogger(__name__)

# ── Constanten ────────────────────────────────────────────────────────────────

S3_ENDPOINT    = "https://s3.waw3-1.cloudferro.com"
S3_BUCKET      = "openradar-24h"
TIMEOUT_S      = 30
MAX_PRODUCT_AGE_S = 15 * 60   # 15 minuten — verwerp oudere producten

# Filterparameters (uit PoC defaults, bewezen correct)
# Live OPERA CIRRUS data can report qi_total=0.0 for genuine precipitation.
# Treat quality as diagnostic metadata instead of a hard rejection criterion.
MIN_DBZ      = 8.0
MIN_QUALITY  = 0.0
MIN_PIXELS   = 5
CONNECTIVITY = 8
MAX_DIAGNOSTIC_CELLS = 40
MAX_UNSPLIT_CELL_PIXELS = 3_000
SPLIT_CORE_THRESHOLDS_DBZ = (12.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0)
SPLIT_GROWTH_PIXELS = 5

# Werkelijk OPERA dekkingsgebied (uit API metadata)
OPERA_LON_MIN = -22.635361
OPERA_LON_MAX =  29.797720
OPERA_LAT_MIN =  28.018790
OPERA_LAT_MAX =  70.605194

# ── Module-level gedeelde downloadcache ───────────────────────────────────────
# Eén OPERA bestand gedeeld door alle actieve OperaProvider instanties.
# Slaat (s3_key → bytes) op. Wordt geinvalideerd zodra een nieuwer bestand
# beschikbaar is.

_cache_lock:    asyncio.Lock   = asyncio.Lock()
_cached_key:    Optional[str]  = None
_cached_data:   Optional[bytes] = None
_cache_ts:      float          = 0.0   # unix timestamp van de gecachede data


@asynccontextmanager
async def _session_scope(existing: Optional[aiohttp.ClientSession]):
    """Reuse Home Assistant's managed session; own one only in standalone use."""
    if existing is not None:
        yield existing
        return
    async with aiohttp.ClientSession() as session:
        yield session


# ── Dataklassen ───────────────────────────────────────────────────────────────

@dataclass
class Grid:
    """OPERA rasterprojectie metadata uit HDF5 /where attribuut."""
    projdef: str
    xsize:   int
    ysize:   int
    xscale:  float
    yscale:  float


@dataclass
class OperaCell:
    """Eén gedetecteerde radarcel uit het OPERA composiet."""
    centroid_lat: float
    centroid_lon: float
    area_km2:     float
    max_dbz:      float
    mean_dbz:     float
    mean_quality: float
    pixelcount:   int
    footprint_points: tuple[tuple[float, float], ...] = ()
    parent_component: int = 0
    child_component: int = 0
    parent_area_km2: float = 0.0
    parent_footprint_points: tuple[tuple[float, float], ...] = ()


@dataclass
class ComponentGroup:
    """Eén oorspronkelijke radarecho met één of meer lokale kernen."""
    parent_pixels: object
    child_pixels: list


# ── Geometrie helpers ─────────────────────────────────────────────────────────

def _text_attr(value: object) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance used only for compact provider diagnostics."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _point_in_ring(lat: float, lon: float, ring) -> bool:
    """Even-odd test voor een (lat, lon)-bronpixel binnen een celring."""
    inside = False
    points = tuple(ring or ())
    if len(points) < 4:
        return False
    previous_lat, previous_lon = points[-1]
    for current_lat, current_lon in points:
        crosses = (current_lat > lat) != (previous_lat > lat)
        if crosses:
            boundary_lon = (
                (previous_lon - current_lon) * (lat - current_lat)
                / (previous_lat - current_lat) + current_lon
            )
            if lon < boundary_lon:
                inside = not inside
        previous_lat, previous_lon = current_lat, current_lon
    return inside


def _edge_points(bbox: tuple, samples: int = 33):
    import numpy as np
    min_lon, min_lat, max_lon, max_lat = bbox
    lon = np.linspace(min_lon, max_lon, samples)
    lat = np.linspace(min_lat, max_lat, samples)
    lons = np.concatenate((lon, lon, np.full(samples, min_lon), np.full(samples, max_lon)))
    lats = np.concatenate((np.full(samples, min_lat), np.full(samples, max_lat), lat, lat))
    return lons, lats


def _crop_window(grid: Grid, bbox: tuple) -> tuple:
    """
    Bereken HDF5 slice (row0, row1, col0, col1) voor een WGS84 bbox.
    OPERA CIRRUS projectie: bovenrand y=0, y daalt zuidwaarts.
    """
    import numpy as np
    from pyproj import CRS, Transformer

    forward = Transformer.from_crs(
        CRS.from_epsg(4326),
        CRS.from_user_input(grid.projdef),
        always_xy=True
    )
    lons, lats = _edge_points(bbox)
    xs, ys = forward.transform(lons, lats)

    col0 = max(0, int(np.floor(np.min(xs) / grid.xscale)))
    col1 = min(grid.xsize, int(np.ceil(np.max(xs) / grid.xscale)))
    row0 = max(0, int(np.floor(-np.max(ys) / grid.yscale)))
    row1 = min(grid.ysize, int(np.ceil(-np.min(ys) / grid.yscale)))

    if row0 >= row1 or col0 >= col1:
        raise ValueError(f"BBox {bbox} valt buiten het OPERA-raster")
    return row0, row1, col0, col1


def _neighbors(row: int, col: int, height: int, width: int) -> Iterator[tuple]:
    for dr, dc in ((-1,0),(1,0),(0,-1),(0,1),(-1,-1),(-1,1),(1,-1),(1,1)):
        rr, cc = row + dr, col + dc
        if 0 <= rr < height and 0 <= cc < width:
            yield rr, cc


def _label_components(mask, min_pixels: int) -> list:
    """8-connected component labeling zonder SciPy."""
    import numpy as np
    height, width = mask.shape
    visited = np.zeros(mask.shape, dtype=np.bool_)
    components = []
    for start_row, start_col in np.argwhere(mask):
        sr, sc = int(start_row), int(start_col)
        if visited[sr, sc]:
            continue
        visited[sr, sc] = True
        queue = deque([(sr, sc)])
        pixels = []
        while queue:
            row, col = queue.popleft()
            pixels.append((row, col))
            for rr, cc in _neighbors(row, col, height, width):
                if mask[rr, cc] and not visited[rr, cc]:
                    visited[rr, cc] = True
                    queue.append((rr, cc))
        if len(pixels) >= min_pixels:
            components.append(np.asarray(pixels, dtype=np.int32))
    return components


def _grow_seed_components(
    support_mask, seeds: list, max_steps: int
) -> list:
    """Grow separate rain cores into weak rain without merging the cores."""
    import numpy as np

    height, width = support_mask.shape
    owner = np.full(support_mask.shape, -1, dtype=np.int32)
    distance = np.full(support_mask.shape, max_steps + 1, dtype=np.int16)
    queue = deque()

    for seed_id, seed in enumerate(seeds):
        for row, col in seed:
            rr, cc = int(row), int(col)
            owner[rr, cc] = seed_id
            distance[rr, cc] = 0
            queue.append((rr, cc))

    while queue:
        row, col = queue.popleft()
        current_distance = int(distance[row, col])
        if current_distance >= max_steps:
            continue
        for rr, cc in _neighbors(row, col, height, width):
            if not support_mask[rr, cc] or owner[rr, cc] >= 0:
                continue
            owner[rr, cc] = owner[row, col]
            distance[rr, cc] = current_distance + 1
            queue.append((rr, cc))

    owned_pixels = np.argwhere(owner >= 0).astype(np.int32)
    if not len(owned_pixels):
        return []
    owned_ids = owner[owned_pixels[:, 0], owned_pixels[:, 1]]
    return [
        owned_pixels[owned_ids == seed_id]
        for seed_id in range(len(seeds))
        if np.count_nonzero(owned_ids == seed_id) >= MIN_PIXELS
    ]


def _segment_component_groups(mask, radar, min_pixels: int) -> list[ComponentGroup]:
    """Split only implausibly large echoes using adaptive strong-rain cores.

    Ordinary and light-rain components keep the original 8 dBZ sensitivity.
    A component larger than ``MAX_UNSPLIT_CELL_PIXELS`` is progressively
    thresholded until compact cores emerge. Each core may reclaim only a small
    halo of the original weak-rain mask, preventing thin drizzle/noise bridges
    from joining unrelated systems again.
    """
    import numpy as np

    result = []
    for component in _label_components(mask, min_pixels):
        if len(component) <= MAX_UNSPLIT_CELL_PIXELS:
            result.append(ComponentGroup(component, [component]))
            continue

        component_mask = np.zeros(mask.shape, dtype=np.bool_)
        component_mask[component[:, 0], component[:, 1]] = True
        seeds = []
        for threshold in SPLIT_CORE_THRESHOLDS_DBZ:
            candidates = _label_components(
                component_mask & (radar >= threshold), min_pixels
            )
            if candidates and max(map(len, candidates)) <= MAX_UNSPLIT_CELL_PIXELS:
                seeds = candidates
                break

        if not seeds:
            # A broad but uniformly light field has no defensible strong cores;
            # retain it rather than silently losing precipitation data.
            result.append(ComponentGroup(component, [component]))
            continue

        grown = _grow_seed_components(
            component_mask, seeds, SPLIT_GROWTH_PIXELS
        )
        result.append(ComponentGroup(component, grown or [component]))

    return result


def _segment_components(mask, radar, min_pixels: int) -> list:
    """Compatibele vlakke lijst van lokale cellen voor bestaande callers."""
    return [
        child
        for group in _segment_component_groups(mask, radar, min_pixels)
        for child in group.child_pixels
    ]


def _boundary_ring(pixels, window, grid, inverse) -> tuple[tuple[float, float], ...]:
    """Bouw een gevalideerde buitenrand uit de echte OPERA-rasterpixels."""
    row0, _, col0, _ = window
    edges = set()
    for local_row, local_col in pixels:
        row, col = int(local_row), int(local_col)
        for edge in (
            ((row, col), (row, col + 1)),
            ((row, col + 1), (row + 1, col + 1)),
            ((row + 1, col + 1), (row + 1, col)),
            ((row + 1, col), (row, col)),
        ):
            reverse = (edge[1], edge[0])
            if reverse in edges:
                edges.remove(reverse)
            else:
                edges.add(edge)
    if not edges:
        return ()

    unused = set(edges)
    loops = []
    while unused:
        first = min(unused)
        unused.remove(first)
        ring = [first[0], first[1]]
        while ring[-1] != ring[0]:
            candidates = sorted(edge for edge in unused if edge[0] == ring[-1])
            if not candidates:
                ring = []
                break
            edge = candidates[0]
            unused.remove(edge)
            ring.append(edge[1])
            if len(ring) > len(edges) + 1:
                ring = []
                break
        if len(ring) >= 4:
            loops.append(ring)
    if len(loops) != 1:
        return ()

    ring = loops[0]
    projected = [
        ((col0 + col) * grid.xscale, -(row0 + row) * grid.yscale)
        for row, col in ring
    ]
    area = abs(sum(
        x1 * y2 - x2 * y1
        for (x1, y1), (x2, y2) in zip(projected, projected[1:])
    )) / 2.0
    expected = len(pixels) * grid.xscale * grid.yscale
    if expected <= 0 or not 0.98 <= area / expected <= 1.02:
        return ()

    xs, ys = zip(*projected)
    lons, lats = inverse.transform(xs, ys)
    return tuple(
        (round(float(lat), 5), round(float(lon), 5))
        for lat, lon in zip(lats, lons)
    )


def _analyze_components(
    components, radar, quality, window, grid, component_metadata=None
) -> list[OperaCell]:
    import numpy as np
    from pyproj import CRS, Transformer

    row0, _, col0, _ = window
    inverse = Transformer.from_crs(
        CRS.from_user_input(grid.projdef),
        CRS.from_epsg(4326),
        always_xy=True
    )
    cells = []
    for component_index, pixels in enumerate(components):
        rows, cols = pixels[:, 0], pixels[:, 1]
        values    = radar[rows, cols]
        qualities = quality[rows, cols]
        global_row = row0 + float(rows.mean())
        global_col = col0 + float(cols.mean())
        x = (global_col + 0.5) * grid.xscale
        y = -(global_row + 0.5) * grid.yscale
        lon, lat = inverse.transform(x, y)

        # Kies één echte celpixel per ongeveer 8x8 km rasterblok. Dit volgt
        # ook onregelmatige cellen veel beter dan een centroid of bounding box.
        footprint_points = _boundary_ring(pixels, window, grid, inverse)

        metadata = (
            component_metadata[component_index]
            if component_metadata is not None else {}
        )
        cells.append(OperaCell(
            centroid_lat = round(float(lat), 5),
            centroid_lon = round(float(lon), 5),
            area_km2     = round(len(pixels) * grid.xscale * grid.yscale / 1_000_000, 3),
            max_dbz      = round(float(values.max()), 2),
            mean_dbz     = round(float(values.mean()), 2),
            mean_quality = round(float(qualities.mean()), 3),
            pixelcount   = len(pixels),
            footprint_points = footprint_points,
            **metadata,
        ))
    return sorted(cells, key=lambda c: c.area_km2, reverse=True)


def _parse_hdf5_slice(
    data: bytes, bbox: tuple, overlay_out: list | None = None
) -> tuple[list[OperaCell], str]:
    """
    Synchrone HDF5 parsing van een lokale bbox slice.
    Wordt via async_add_executor_job buiten de event loop uitgevoerd.

    Onderscheid:
      - geografisch buiten OPERA grid → ValueError
      - binnen grid maar nodata/undetect → gefilterd door valid mask
      - lage quality → gefilterd door quality threshold
      - voldoende quality en dBZ → radarcel
    """
    import h5py
    import numpy as np

    with h5py.File(io.BytesIO(data), "r") as h5:
        where = h5["where"].attrs
        grid  = Grid(
            projdef = _text_attr(where["projdef"]),
            xsize   = int(where["xsize"]),
            ysize   = int(where["ysize"]),
            xscale  = float(where["xscale"]),
            yscale  = float(where["yscale"]),
        )
        window = _crop_window(grid, bbox)
        row0, row1, col0, col1 = window
        _LOGGER.debug("OPERA HDF5 slice: rows=%d:%d cols=%d:%d (%.0f×%.0f km)",
                      row0, row1, col0, col1,
                      (col1 - col0) * grid.xscale / 1000,
                      (row1 - row0) * grid.yscale / 1000)

        radar   = h5["dataset1/data1/data"][row0:row1, col0:col1]
        quality_group = h5["dataset1/data1/quality1"]
        quality = quality_group["data"][row0:row1, col0:col1]
        attrs   = h5["dataset1/data1/what"].attrs
        nodata, undetect = float(attrs["nodata"]), float(attrs["undetect"])
        gain, offset     = float(attrs["gain"]), float(attrs["offset"])
        radar   = radar * gain + offset

        quality_what = quality_group.get("what")
        quality_nodata = float(
            quality_what.attrs.get("nodata", -9999000.0)
            if quality_what is not None else -9999000.0
        )
        valid   = (
            np.isfinite(radar) & np.isfinite(quality)
            & (radar != nodata * gain + offset)
            & (radar != undetect * gain + offset)
            & (quality != quality_nodata)
        )
        mask       = valid & (radar >= MIN_DBZ)
        if overlay_out is not None:
            from pyproj import CRS, Transformer
            inverse = Transformer.from_crs(
                CRS.from_user_input(grid.projdef), CRS.from_epsg(4326),
                always_xy=True,
            )

            def corner_to_latlon(row, column):
                lon, lat = inverse.transform(
                    (col0 + column) * grid.xscale,
                    -(row0 + row) * grid.yscale,
                )
                return round(float(lat), 5), round(float(lon), 5)

            intensity_grid = np.zeros(radar.shape, dtype=np.uint8)
            intensity_grid[mask & (radar < 15)] = 1
            intensity_grid[mask & (radar >= 15) & (radar < 20)] = 2
            intensity_grid[mask & (radar >= 20) & (radar < 25)] = 3
            intensity_grid[mask & (radar >= 25) & (radar < 30)] = 4
            intensity_grid[mask & (radar >= 30) & (radar < 35)] = 5
            intensity_grid[mask & (radar >= 35) & (radar < 40)] = 6
            intensity_grid[mask & (radar >= 40) & (radar < 50)] = 7
            intensity_grid[mask & (radar >= 50)] = 8
            overlay_out.append({
                "source": "opera", "timestamp": 0.0,
                "runs": extract_intensity_runs(
                    intensity_grid, corner_to_latlon
                ),
            })
        groups = _segment_component_groups(mask, radar, MIN_PIXELS)
        cells = []
        for parent_index, group in enumerate(groups):
            parent = _analyze_components(
                [group.parent_pixels], radar, quality, window, grid
            )[0]
            metadata = [
                {
                    "parent_component": parent_index,
                    "child_component": child_index,
                    "parent_area_km2": parent.area_km2,
                    "parent_footprint_points": parent.footprint_points,
                }
                for child_index, _ in enumerate(group.child_pixels)
            ]
            cells.extend(_analyze_components(
                group.child_pixels,
                radar,
                quality,
                window,
                grid,
                component_metadata=metadata,
            ))
        cells.sort(key=lambda cell: cell.area_km2, reverse=True)

        date = _text_attr(h5["what"].attrs.get("date", ""))
        time_ = _text_attr(h5["what"].attrs.get("time", ""))
        timestamp = f"{date}T{time_}Z" if date and time_ else ""
        if overlay_out:
            try:
                overlay_out[0]["timestamp"] = datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                ).timestamp()
            except ValueError:
                overlay_out[0]["timestamp"] = time.time()
        return cells, timestamp


# ── S3 discovery helpers ──────────────────────────────────────────────────────

_KEY_PATTERN = re.compile(
    r"((?:\d{4}/\d{2}/\d{2})/OPERA/COMP/OPERA@(\d{8})T(\d{4})@0@DBZH\.h5)"
)


def _extract_product_ts(s3_key: str) -> Optional[datetime]:
    """Extraheer UTC datetime uit de S3-key bestandsnaam."""
    m = _KEY_PATTERN.search(s3_key)
    if not m:
        return None
    try:
        return datetime.strptime(
            f"{m.group(2)}{m.group(3)}", "%Y%m%d%H%M"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _s3_path_from_key(s3_key: str) -> str:
    """Construeer de volledige S3 URL uit de key (datum uit bestandsnaam)."""
    return f"{S3_ENDPOINT}/{S3_BUCKET}/{s3_key}"


async def _list_opera_files(session: aiohttp.ClientSession) -> list[str]:
    """
    Lijst OPERA DBZH bestanden op via S3 XML listing met paginering.
    Gebruikt IsTruncated + NextContinuationToken voor grote listings.
    Zoekt in de bucket voor vandaag EN gisteren (dagovergang om 00:00 UTC).
    Returns: gesorteerde lijst van volledige S3 keys (nieuwste eerst).
    """
    now = datetime.now(timezone.utc)
    dates_to_check = [
        now.strftime("%Y/%m/%d"),
        (now - timedelta(days=1)).strftime("%Y/%m/%d"),
    ]

    all_keys: list[str] = []

    for date_prefix in dates_to_check:
        prefix = f"{date_prefix}/OPERA/COMP/OPERA@"
        continuation_token = None

        while True:
            params = {
                "list-type": "2",
                "prefix":    prefix,
                "max-keys":  "1000",
            }
            if continuation_token:
                params["continuation-token"] = continuation_token

            url = f"{S3_ENDPOINT}/{S3_BUCKET}/"
            try:
                async with session.get(url, params=params,
                                       timeout=aiohttp.ClientTimeout(total=TIMEOUT_S)) as resp:
                    if resp.status != 200:
                        _LOGGER.warning("OPERA S3 listing mislukt: %d", resp.status)
                        break
                    text = await resp.text()
            except Exception:
                _LOGGER.debug("OPERA S3 listing fout", exc_info=True)
                break

            # Extraheer keys
            keys = _KEY_PATTERN.findall(text)
            all_keys.extend(k[0] for k in keys)

            # Paginering
            is_truncated = "<IsTruncated>true</IsTruncated>" in text
            if not is_truncated:
                break
            token_match = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", text)
            if not token_match:
                break
            continuation_token = token_match.group(1)

    # Dedupliceren en sorteren (nieuwste eerst)
    return sorted(set(all_keys), reverse=True)


async def _find_latest_valid_key(session: aiohttp.ClientSession) -> Optional[str]:
    """
    Zoek de meest recente OPERA DBZH key die:
    1. De nieuwste is (op timestamp in bestandsnaam)
    2. Niet ouder is dan MAX_PRODUCT_AGE_S
    """
    # Fast path: OPERA filenames are deterministic at five-minute intervals.
    # Probe the expected recent objects directly. This avoids relying on a
    # broad S3 ListObjects response, which has occasionally returned an older
    # day on the Home Assistant host even while fresh objects were available.
    head = getattr(session, "head", None)
    if head is not None:
        now = datetime.now(timezone.utc)
        rounded = now.replace(
            minute=(now.minute // 5) * 5, second=0, microsecond=0
        )
        probe_count = MAX_PRODUCT_AGE_S // 300 + 2
        for step in range(probe_count):
            product_ts = rounded - timedelta(minutes=5 * step)
            key = (
                f"{product_ts:%Y/%m/%d}/OPERA/COMP/"
                f"OPERA@{product_ts:%Y%m%dT%H%M}@0@DBZH.h5"
            )
            try:
                async with head(
                    _s3_path_from_key(key),
                    timeout=aiohttp.ClientTimeout(total=TIMEOUT_S),
                ) as response:
                    if response.status == 200:
                        _LOGGER.debug(
                            "OPERA: direct gevonden %s (leeftijd %.1f min)",
                            key, (now - product_ts).total_seconds() / 60,
                        )
                        return key
                    if response.status not in (403, 404):
                        _LOGGER.debug(
                            "OPERA HEAD gaf HTTP %d voor %s", response.status, key
                        )
            except Exception:
                _LOGGER.debug("OPERA HEAD-probe mislukt voor %s", key, exc_info=True)
                break

    # Compatibility fallback for S3 implementations without usable HEAD.
    keys = await _list_opera_files(session)
    if not keys:
        _LOGGER.debug("OPERA: geen bestanden gevonden op S3")
        return None

    now = datetime.now(timezone.utc)
    for key in keys:
        product_ts = _extract_product_ts(key)
        if product_ts is None:
            continue
        age_s = (now - product_ts).total_seconds()
        if age_s < 0:
            # Toekomst — sla over (klokafwijking)
            continue
        if age_s > MAX_PRODUCT_AGE_S:
            _LOGGER.warning(
                "OPERA: nieuwste product is %.0f min oud (max %d min) — %s",
                age_s / 60, MAX_PRODUCT_AGE_S // 60, key
            )
            return None
        _LOGGER.debug("OPERA: geselecteerd %s (leeftijd %.1f min)", key, age_s / 60)
        return key

    return None


# ── Provider ──────────────────────────────────────────────────────────────────

class OperaProvider:
    """
    OPERA DBZH composiet provider.

    Download: één bestand gedeeld via module-level cache.
    Verwerking: lokale slice op basis van center + radius.
    Validatie: productleeftijd ≤ 15 min.
    Synchronisatie: asyncio.Lock voorkomt poll-overlap.
    """

    plugin_id    = "opera"
    priority     = 90
    capabilities = frozenset()   # wordt ingevuld bij Fase 3 migratie

    def __init__(self, lat: float, lon: float, radius_km: float = 200.0,
                 session: Optional[aiohttp.ClientSession] = None) -> None:
        self._lat      = lat
        self._lon      = lon
        self._radius   = radius_km
        self._session  = session
        self._lock     = asyncio.Lock()
        self._callback = None
        self._last_key: Optional[str] = None
        self._last_log_ts: float = 0.0
        self._healthy = False
        self._last_error: Optional[str] = None
        self._last_success_ts: Optional[float] = None
        self._last_product_ts: Optional[float] = None
        self._last_cells: list[dict] = []
        self._last_observations: list[Observation] = []
        self.overlay = None
        self._raw_overlay = None

    @property
    def healthy(self) -> bool:
        """True only after a fresh product was downloaded/cached and parsed."""
        return self._healthy

    def apply_accepted_overlay(self, observations: list[Observation]) -> None:
        """Beperk bronpixels tot cellen die de OPERA-verificatie aanvaardde."""
        if not self._raw_overlay:
            self.overlay = None
            return
        accepted = []
        for observation in observations:
            ring = (
                observation.footprint_points
                or observation.parent_footprint_points
            )
            accepted.append((observation, ring))
        runs = []
        for run in self._raw_overlay.get("runs", ()):
            lat = sum(point[0] for point in run["ring"]) / 4
            lon = sum(point[1] for point in run["ring"]) / 4
            if any(
                _point_in_ring(lat, lon, ring)
                or (
                    not ring and _haversine_km(
                        lat, lon, observation.lat, observation.lon
                    ) <= max(2.0, math.sqrt(max(0.0, observation.area_km2) / math.pi))
                )
                for observation, ring in accepted
            ):
                runs.append(run)
        self.overlay = {**self._raw_overlay, "runs": runs}

    @property
    def diagnostics(self) -> dict:
        bbox = self._bbox()
        return {
            "healthy": self._healthy,
            "last_error": self._last_error,
            "last_success_ts": self._last_success_ts,
            "last_product_ts": self._last_product_ts,
            "last_key": self._last_key,
            "min_dbz": MIN_DBZ,
            "min_pixels": MIN_PIXELS,
            "segmentation": {
                "max_unsplit_pixels": MAX_UNSPLIT_CELL_PIXELS,
                "core_thresholds_dbz": list(SPLIT_CORE_THRESHOLDS_DBZ),
                "growth_pixels": SPLIT_GROWTH_PIXELS,
            },
            "quality_filter_enabled": "cross_source",
            "radius_km": self._radius,
            "coverage_complete": self.coverage_complete,
            "bbox": {
                "lon_min": round(bbox[0], 4),
                "lat_min": round(bbox[1], 4),
                "lon_max": round(bbox[2], 4),
                "lat_max": round(bbox[3], 4),
            },
            "cells": self._last_cells,
        }

    def set_callback(self, cb) -> None:
        self._callback = cb

    def start(self) -> None:
        _LOGGER.info("OperaProvider gestart voor (%.4f,%.4f) r=%.0fkm",
                     self._lat, self._lon, self._radius)

    def stop(self) -> None:
        pass

    def _bbox(self) -> tuple:
        """
        Lokale bbox op basis van center + radius.
        Begrensd door het werkelijke OPERA dekkingsgebied.
        """
        deg_lat = self._radius / 111.32
        deg_lon = self._radius / (111.32 * math.cos(math.radians(self._lat)))
        return (
            max(OPERA_LON_MIN, self._lon - deg_lon),
            max(OPERA_LAT_MIN, self._lat - deg_lat),
            min(OPERA_LON_MAX, self._lon + deg_lon),
            min(OPERA_LAT_MAX, self._lat + deg_lat),
        )

    @property
    def coverage_complete(self) -> bool:
        """Return whether OPERA covers the complete requested radius."""
        deg_lat = self._radius / 111.32
        deg_lon = self._radius / (111.32 * math.cos(math.radians(self._lat)))
        return (
            self._lon - deg_lon >= OPERA_LON_MIN
            and self._lon + deg_lon <= OPERA_LON_MAX
            and self._lat - deg_lat >= OPERA_LAT_MIN
            and self._lat + deg_lat <= OPERA_LAT_MAX
        )

    async def fetch_observations(self, hass=None) -> list[Observation]:
        """
        Haal OPERA observaties op voor de lokale bbox.
        Lock voorkomt gelijktijdige polls.
        """
        if self._lock.locked():
            _LOGGER.debug("OperaProvider: poll overgeslagen (vorige nog bezig)")
            return []

        async with self._lock:
            return await self._fetch_inner(hass)

    async def _fetch_inner(self, hass) -> list[Observation]:
        global _cached_key, _cached_data, _cache_ts

        try:
            async with _session_scope(self._session) as session:
                # Zoek meest recente geldige S3 key
                key = await _find_latest_valid_key(session)
                if not key:
                    self._healthy = False
                    self._last_error = "no fresh OPERA product available"
                    return []

                # Avoid repeating HDF5/component work when the five-minute poll
                # fires before a new OPERA product becomes available.
                if key == self._last_key and self._healthy:
                    return list(self._last_observations)

                # Download — gebruik cache als het dezelfde key is
                async with _cache_lock:
                    if key != _cached_key:
                        data = await self._download(session, key)
                        if not data:
                            return []
                        _cached_key  = key
                        _cached_data = data
                        _cache_ts    = time.time()
                        _LOGGER.info("OPERA cache bijgewerkt: %s (%.1f MB)",
                                     key, len(data) / 1_048_576)
                    else:
                        data = _cached_data
                        _LOGGER.debug("OPERA cache hit: %s", key)

            # Verwerk lokale slice (buiten event loop)
            bbox = self._bbox()
            overlays = []
            if hass:
                cells, ts = await hass.async_add_executor_job(
                    _parse_hdf5_slice, data, bbox, overlays
                )
            else:
                cells, ts = _parse_hdf5_slice(data, bbox, overlays)

            raw_overlay = overlays[0] if overlays else None
            if raw_overlay:
                raw_overlay["runs"] = [
                    run for run in raw_overlay["runs"]
                    if _haversine_km(
                        self._lat, self._lon,
                        sum(point[0] for point in run["ring"]) / 4,
                        sum(point[1] for point in run["ring"]) / 4,
                    ) <= self._radius
                ]
            self._raw_overlay = raw_overlay
            self.overlay = raw_overlay

            # The projected crop is rectangular and its corners can extend far
            # beyond the configured circular monitoring radius. Enforce the
            # actual radius before observations enter the WeatherSystem Engine.
            cells = [
                cell for cell in cells
                if _haversine_km(
                    self._lat, self._lon,
                    cell.centroid_lat, cell.centroid_lon,
                ) <= self._radius
            ]

            obs = self._cells_to_observations(cells, ts)
            self._last_observations = list(obs)
            self._last_cells = [
                {
                    "lat": cell.centroid_lat,
                    "lon": cell.centroid_lon,
                    "distance_km": round(
                        _haversine_km(
                            self._lat, self._lon,
                            cell.centroid_lat, cell.centroid_lon,
                        ), 1
                    ),
                    "area_km2": cell.area_km2,
                    "max_dbz": cell.max_dbz,
                    "mean_dbz": cell.mean_dbz,
                    "quality": cell.mean_quality,
                    "pixels": cell.pixelcount,
                    "footprint_points": len(cell.footprint_points),
                    "parent_component": cell.parent_component,
                    "parent_area_km2": cell.parent_area_km2,
                    "parent_footprint_points": len(cell.parent_footprint_points),
                }
                for cell in cells[:MAX_DIAGNOSTIC_CELLS]
            ]
            product_dt = _extract_product_ts(key)
            self._last_key = key
            self._healthy = True
            self._last_error = None
            self._last_success_ts = time.time()
            self._last_product_ts = product_dt.timestamp() if product_dt else None
            _LOGGER.info(
                "OPERA: %d cellen → %d observaties | bbox=(%.1f,%.1f,%.1f,%.1f)",
                len(cells), len(obs), *bbox
            )
            return obs

        except Exception:
            self._healthy = False
            self._last_error = "fetch or parse failed"
            _LOGGER.exception("OperaProvider: fout bij ophalen data")
            return []

    async def _download(self, session: aiohttp.ClientSession, key: str) -> Optional[bytes]:
        url = _s3_path_from_key(key)
        t0  = time.time()
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT_S)) as resp:
                if resp.status != 200:
                    _LOGGER.warning("OPERA download mislukt: %d voor %s", resp.status, url)
                    return None
                data = await resp.read()
                _LOGGER.debug("OPERA download: %.1f MB in %.1fs",
                              len(data) / 1_048_576, time.time() - t0)
                return data
        except Exception:
            _LOGGER.exception("OPERA download fout voor %s", url)
            return None

    def _cells_to_observations(self, cells: list[OperaCell], timestamp_str: str) -> list[Observation]:
        try:
            ts = datetime.fromisoformat(
                timestamp_str.replace("Z", "+00:00")
            ).timestamp() if timestamp_str else time.time()
        except ValueError:
            ts = time.time()

        obs = []
        for cell in cells:
            # dBZ → intensiteit 0-8
            # <10=0, 10-14=1, 15-19=2, 20-24=3, 25-29=4, 30-34=5, 35-39=6, 40-49=7, >=50=8
            intensity = min(8, max(0, int((cell.max_dbz - 10) / 5)))
            obs.append(Observation(
                obs_type  = ObservationType.RADAR,
                lat       = cell.centroid_lat,
                lon       = cell.centroid_lon,
                timestamp = ts,
                intensity = intensity,
                max_dbz   = cell.max_dbz,
                mean_dbz  = cell.mean_dbz,
                area_km2  = cell.area_km2,
                quality   = cell.mean_quality,
                footprint_points = cell.footprint_points,
                radar_cell_id = (
                    f"opera:{timestamp_str}:p{cell.parent_component}:"
                    f"c{cell.child_component}"
                ),
                parent_system_id = (
                    f"opera:{timestamp_str}:p{cell.parent_component}"
                ),
                parent_area_km2 = cell.parent_area_km2,
                parent_footprint_points = cell.parent_footprint_points,
                source    = "opera",
            ))
        return obs


class OperaProviderFactory:
    """
    Factory voor OperaProvider.
    Europees dekkingsgebied (werkelijk OPERA extent + buffer).

    Versiegeschiedenis:
      v0.2.0 — werkelijk OPERA extent uit API metadata; buffer 100km
      v0.1.0 — eerste versie
    """
    LAT_MIN   = OPERA_LAT_MIN
    LAT_MAX   = OPERA_LAT_MAX
    LON_MIN   = OPERA_LON_MIN
    LON_MAX   = OPERA_LON_MAX
    BUFFER_KM = 100.0

    @classmethod
    def supports(cls, center_lat: float, center_lon: float, radius_km: float) -> bool:
        nearest_lat = max(cls.LAT_MIN, min(center_lat, cls.LAT_MAX))
        nearest_lon = max(cls.LON_MIN, min(center_lon, cls.LON_MAX))
        dlat_km = abs(center_lat - nearest_lat) * 111.32
        dlon_km = abs(center_lon - nearest_lon) * 111.32 * math.cos(math.radians(center_lat))
        dist_km = math.sqrt(dlat_km ** 2 + dlon_km ** 2)
        return dist_km <= cls.BUFFER_KM

    def create(self, hass, center_lat: float, center_lon: float, radius_km: float):
        if not self.supports(center_lat, center_lon, radius_km):
            return None
        return OperaProvider(center_lat, center_lon, radius_km)
