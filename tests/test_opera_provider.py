"""Storm Tracker V3 — tests/test_opera_provider.py v0.1.0

Tests voor OperaProvider/OperaProviderFactory-gedrag dat niet in
test_opera.py (pure HDF5/S3-parsing) past:

  - _bbox() clipping tegen het werkelijke OPERA-dekkingsgebied
  - OperaProviderFactory.supports()/create()
  - _download(): succes, non-200, exception
  - Gedeelde module-level downloadcache (Blokker 1 acceptatiecriterium:
    "twee RegionEngines gebruiken dezelfde gedownloade OPERA-file")
  - Poll-overlap-lock in fetch_observations()
  - _cells_to_observations(): intensiteits-bucketing + timestamp-parsing

De HDF5-parsing zelf (_parse_hdf5_slice) en de S3-discovery-helpers
worden hier gemockt/gestubd — die logica wordt al apart getest in
test_opera.py. Dit bestand test de PLUMBING eromheen.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

import pytest


# ── Isolatie van de module-level cache tussen tests ───────────────────────

@pytest.fixture(autouse=True)
def _reset_opera_cache(opera_module):
    """
    opera.py houdt een GEDEELDE module-level cache bij (_cached_key/_cached_data/
    _cache_ts). Omdat opera_module sessie-breed hergebruikt wordt, resetten we
    die state voor en na elke test in dit bestand, anders lekt de cache-status
    van de ene test naar de andere.
    """
    opera_module._cached_key = None
    opera_module._cached_data = None
    opera_module._cache_ts = 0.0
    yield
    opera_module._cached_key = None
    opera_module._cached_data = None
    opera_module._cache_ts = 0.0


class _FakeHass:
    """Minimale hass-stub: async_add_executor_job voert de functie synchroon uit."""
    async def async_add_executor_job(self, func, *args):
        return func(*args)


class _DummySessionCtx:
    """Vervangt `async with aiohttp.ClientSession() as session:` door een no-op."""
    async def __aenter__(self):
        return "dummy-session"

    async def __aexit__(self, *a):
        return False


def _patch_dummy_session(monkeypatch, opera_module):
    monkeypatch.setattr(opera_module.aiohttp, "ClientSession", lambda *a, **kw: _DummySessionCtx())


async def _async_return(value):
    return value


# ── _bbox() clipping tegen OPERA-dekkingsgebied ───────────────────────────

def test_bbox_normal_case_is_symmetric_around_center(opera_module):
    p = opera_module.OperaProvider(lat=51.026, lon=4.478, radius_km=100.0)
    lon_min, lat_min, lon_max, lat_max = p._bbox()
    # Ver van alle grenzen -> geen clipping, dus symmetrisch rond het centrum
    assert lon_min < 4.478 < lon_max
    assert lat_min < 51.026 < lat_max
    assert abs((lon_max - 4.478) - (4.478 - lon_min)) < 1e-6
    assert abs((lat_max - 51.026) - (51.026 - lat_min)) < 1e-6


def test_diagnostics_expose_effective_radius_and_bbox(opera_module):
    provider = opera_module.OperaProvider(51.0498, 4.4186, radius_km=300.0)
    diagnostics = provider.diagnostics
    assert diagnostics["radius_km"] == 300.0
    assert diagnostics["bbox"]["lat_min"] < 51.0498 < diagnostics["bbox"]["lat_max"]
    assert diagnostics["bbox"]["lon_min"] < 4.4186 < diagnostics["bbox"]["lon_max"]
    assert diagnostics["cells"] == []


def test_diagnostic_distance_uses_great_circle_distance(opera_module):
    distance = opera_module._haversine_km(51.0498, 4.4186, 52.3676, 4.9041)
    assert distance == pytest.approx(150.5, abs=1.0)


def test_bbox_clips_against_real_opera_extent_near_top_edge(opera_module):
    """Een tracker vlak bij de noordgrens van OPERA moet geclipt worden op OPERA_LAT_MAX."""
    p = opera_module.OperaProvider(lat=70.5, lon=10.0, radius_km=200.0)
    _, lat_min, _, lat_max = p._bbox()
    assert lat_max == pytest.approx(opera_module.OPERA_LAT_MAX)
    assert lat_max <= opera_module.OPERA_LAT_MAX + 1e-9


def test_bbox_clips_against_real_opera_extent_near_west_edge(opera_module):
    """Een tracker vlak bij de westgrens moet geclipt worden op OPERA_LON_MIN."""
    p = opera_module.OperaProvider(lat=45.0, lon=-22.5, radius_km=200.0)
    lon_min, _, _, _ = p._bbox()
    assert lon_min == pytest.approx(opera_module.OPERA_LON_MIN)


# ── OperaProviderFactory ───────────────────────────────────────────────────

def test_factory_supports_inside_opera_extent(opera_module):
    assert opera_module.OperaProviderFactory.supports(51.026, 4.478, 200.0) is True


def test_factory_supports_just_outside_extent_within_buffer(opera_module):
    """Net buiten OPERA_LAT_MAX, maar binnen de BUFFER_KM=100 -> nog steeds ondersteund."""
    just_outside_lat = opera_module.OPERA_LAT_MAX + 0.3  # ~33km buiten de grens
    assert opera_module.OperaProviderFactory.supports(just_outside_lat, 10.0, 200.0) is True


def test_factory_rejects_far_outside_extent(opera_module):
    assert opera_module.OperaProviderFactory.supports(-33.9, 18.4, 200.0) is False  # Kaapstad


def test_factory_create_returns_none_when_unsupported(opera_module):
    factory = opera_module.OperaProviderFactory()
    assert factory.create(hass=None, center_lat=-33.9, center_lon=18.4, radius_km=200.0) is None


def test_factory_create_returns_provider_when_supported(opera_module):
    factory = opera_module.OperaProviderFactory()
    provider = factory.create(hass=None, center_lat=51.026, center_lon=4.478, radius_km=150.0)
    assert isinstance(provider, opera_module.OperaProvider)
    assert provider._radius == 150.0


# ── _download() ────────────────────────────────────────────────────────────

class _FakeGetResponse:
    def __init__(self, status, body=b""):
        self.status = status
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def read(self):
        return self._body


class _FakeGetSession:
    def __init__(self, response=None, raise_exc=None):
        self._response = response
        self._raise_exc = raise_exc

    def get(self, url, timeout=None):
        if self._raise_exc:
            raise self._raise_exc
        return self._response


def test_download_success_returns_bytes(opera_module):
    p = opera_module.OperaProvider(51.026, 4.478)
    session = _FakeGetSession(response=_FakeGetResponse(200, b"hdf5-bytes-hier"))
    result = asyncio.run(
        p._download(session, "2026/07/12/OPERA/COMP/OPERA@20260712T1200@0@DBZH.h5")
    )
    assert result == b"hdf5-bytes-hier"


def test_download_non_200_returns_none(opera_module):
    p = opera_module.OperaProvider(51.026, 4.478)
    session = _FakeGetSession(response=_FakeGetResponse(403))
    result = asyncio.run(p._download(session, "some/key.h5"))
    assert result is None


def test_download_exception_returns_none(opera_module):
    p = opera_module.OperaProvider(51.026, 4.478)
    session = _FakeGetSession(raise_exc=ConnectionError("netwerk weg"))
    result = asyncio.run(p._download(session, "some/key.h5"))
    assert result is None


# ── Gedeelde module-level downloadcache ───────────────────────────────────

def test_shared_cache_reused_across_two_provider_instances(opera_module, monkeypatch):
    """
    Blokker 1 acceptatiecriterium: 'Twee RegionEngines gebruiken dezelfde
    gedownloade OPERA-file.' Simuleert twee OperaProvider-instanties
    (representatief voor twee RegionEngines) die om dezelfde S3-key vragen
    -> _download() mag maar ÉÉN keer werkelijk aangeroepen worden.
    """
    key = "2026/07/12/OPERA/COMP/OPERA@20260712T1200@0@DBZH.h5"
    download_calls = {"n": 0}

    async def _fake_find_key(session):
        return key

    async def _fake_download(self, session, k):
        download_calls["n"] += 1
        return b"gedeelde-fake-hdf5-data"

    def _fake_parse(data, bbox):
        return [], "2026-07-12T12:00:00Z"

    _patch_dummy_session(monkeypatch, opera_module)
    monkeypatch.setattr(opera_module, "_find_latest_valid_key", _fake_find_key)
    monkeypatch.setattr(opera_module.OperaProvider, "_download", _fake_download)
    monkeypatch.setattr(opera_module, "_parse_hdf5_slice", _fake_parse)

    provider_a = opera_module.OperaProvider(51.026, 4.478)  # "RegionEngine A"
    provider_b = opera_module.OperaProvider(50.850, 4.350)  # "RegionEngine B", andere locatie

    asyncio.run(provider_a.fetch_observations(hass=None))
    asyncio.run(provider_b.fetch_observations(hass=None))

    assert download_calls["n"] == 1, (
        f"verwacht 1 download voor 2 providers met dezelfde key, kreeg {download_calls['n']}"
    )
    assert opera_module._cached_key == key


def test_fetch_filters_cells_outside_true_circular_radius(opera_module, monkeypatch):
    key = "2026/07/12/OPERA/COMP/OPERA@20260712T1200@0@DBZH.h5"

    async def _fake_find_key(session):
        return key

    async def _fake_download(self, session, k):
        return b"fake-hdf5"

    def _cell(lat, lon):
        return opera_module.OperaCell(
            centroid_lat=lat, centroid_lon=lon, area_km2=10,
            max_dbz=20, mean_dbz=15, mean_quality=0, pixelcount=10,
        )

    def _fake_parse(data, bbox, overlay_out=None):
        return [_cell(51.5, 4.5), _cell(55.0, 10.0)], "2026-07-12T12:00:00Z"

    _patch_dummy_session(monkeypatch, opera_module)
    monkeypatch.setattr(opera_module, "_find_latest_valid_key", _fake_find_key)
    monkeypatch.setattr(opera_module.OperaProvider, "_download", _fake_download)
    monkeypatch.setattr(opera_module, "_parse_hdf5_slice", _fake_parse)

    provider = opera_module.OperaProvider(51.0, 4.4, radius_km=100.0)
    observations = asyncio.run(provider.fetch_observations(hass=None))
    assert len(observations) == 1
    assert observations[0].lat == 51.5
    assert all(cell["distance_km"] <= 100 for cell in provider.diagnostics["cells"])


def test_cache_invalidated_when_newer_key_appears(opera_module, monkeypatch):
    """Zodra _find_latest_valid_key een NIEUWE key teruggeeft, moet er opnieuw gedownload worden."""
    keys = iter([
        "2026/07/12/OPERA/COMP/OPERA@20260712T1200@0@DBZH.h5",
        "2026/07/12/OPERA/COMP/OPERA@20260712T1205@0@DBZH.h5",
    ])
    download_calls = {"n": 0}

    async def _fake_find_key(session):
        return next(keys)

    async def _fake_download(self, session, k):
        download_calls["n"] += 1
        return f"data-voor-{k}".encode()

    def _fake_parse(data, bbox):
        return [], "2026-07-12T12:05:00Z"

    _patch_dummy_session(monkeypatch, opera_module)
    monkeypatch.setattr(opera_module, "_find_latest_valid_key", _fake_find_key)
    monkeypatch.setattr(opera_module.OperaProvider, "_download", _fake_download)
    monkeypatch.setattr(opera_module, "_parse_hdf5_slice", _fake_parse)

    provider = opera_module.OperaProvider(51.026, 4.478)
    asyncio.run(provider.fetch_observations(hass=None))
    asyncio.run(provider.fetch_observations(hass=None))

    assert download_calls["n"] == 2, "een nieuwe key moet een nieuwe download triggeren"


# ── Poll-overlap-lock ───────────────────────────────────────────────────────

def test_concurrent_fetch_skips_second_call_while_first_in_progress(opera_module, monkeypatch):
    """
    Twee gelijktijdige fetch_observations()-calls: de tweede moet meteen []
    teruggeven zolang de eerste nog bezig is (poll-overlap-bescherming).
    """
    call_order = []

    async def _slow_fetch_inner(self, hass):
        call_order.append("start")
        await asyncio.sleep(0.05)
        call_order.append("end")
        return ["marker-echte-observaties"]

    monkeypatch.setattr(opera_module.OperaProvider, "_fetch_inner", _slow_fetch_inner)
    provider = opera_module.OperaProvider(51.026, 4.478)

    async def _run():
        return await asyncio.gather(
            provider.fetch_observations(hass=None),
            provider.fetch_observations(hass=None),
        )

    result_a, result_b = asyncio.run(_run())
    results = [result_a, result_b]

    assert [] in results, "de tweede, overlappende poll moet leeg teruggegeven worden"
    assert ["marker-echte-observaties"] in results
    assert call_order == ["start", "end"], "_fetch_inner mag maar 1x werkelijk lopen"


# ── _cells_to_observations(): intensiteit + timestamp ──────────────────────

def _make_cell(opera_module, max_dbz, area_km2=10.0):
    return opera_module.OperaCell(
        centroid_lat=51.0, centroid_lon=4.5, area_km2=area_km2,
        max_dbz=max_dbz, mean_dbz=max_dbz, mean_quality=0.9, pixelcount=10,
    )


def test_cells_preserve_parent_and_child_identity(opera_module):
    provider = opera_module.OperaProvider(51.0, 4.5)
    cells = [
        opera_module.OperaCell(
            centroid_lat=48.8, centroid_lon=-3.2, area_km2=300.0,
            max_dbz=45.0, mean_dbz=30.0, mean_quality=0.4,
            pixelcount=300, footprint_points=((48.8, -3.2),),
            parent_component=2, child_component=1,
            parent_area_km2=46_627.0,
            parent_footprint_points=((48.4, -4.5), (48.8, -1.4)),
        )
    ]

    observations = provider._cells_to_observations(
        cells, "20260713T120500Z"
    )

    assert observations[0].radar_cell_id.endswith(":p2:c1")
    assert observations[0].parent_system_id.endswith(":p2")
    assert observations[0].parent_area_km2 == 46_627.0
    assert len(observations[0].parent_footprint_points) == 2
    assert observations[0].max_dbz == 45.0
    assert observations[0].mean_dbz == 30.0


@pytest.mark.parametrize("dbz,expected_intensity", [
    (5.0, 0),
    (9.9, 0),
    (10.0, 0),   # zie opmerking hieronder — wijkt af van de docstring in opera.py
    (14.9, 0),
    (15.0, 1),
    (19.9, 1),
    (20.0, 2),
    (24.9, 2),
    (29.9, 3),
    (30.0, 4),
    (34.9, 4),
    (35.0, 5),
    (39.9, 5),
    (40.0, 6),
    (44.9, 6),
    (45.0, 7),
    (49.9, 7),
    (50.0, 8),
    (75.0, 8),
])
def test_intensity_bucket_boundaries(opera_module, dbz, expected_intensity):
    """
    BELANGRIJK — bevinding om met Wim te bespreken: de docstring in
    `_cells_to_observations` claimt buckets "<10=0, 10-14=1, 15-19=2, ...",
    maar de FORMULE (`min(8, max(0, int((max_dbz-10)/5)))`) geeft in
    werkelijkheid: alles <15 dBZ -> intensiteit 0 (dus 10-14 dBZ valt NIET
    in bucket 1 zoals de docstring beweert), en de buckets daarna liggen
    each 5 dBZ breed vanaf 15. Deze test documenteert het WERKELIJKE
    gedrag van de code, niet de docstring-tekst. Of dit een bug is
    (docstring correct, formule fout) of alleen een verouderde docstring
    is, moet je zelf beslissen — ik heb de formule niet aangepast.
    """
    p = opera_module.OperaProvider(51.026, 4.478)
    cell = _make_cell(opera_module, dbz)
    obs = p._cells_to_observations([cell], "2026-07-12T12:00:00Z")
    assert obs[0].intensity == expected_intensity


def test_cells_to_observations_parses_iso_timestamp(opera_module):
    p = opera_module.OperaProvider(51.026, 4.478)
    cell = _make_cell(opera_module, 35.0)
    obs = p._cells_to_observations([cell], "2026-07-12T12:00:00Z")
    expected_ts = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    assert obs[0].timestamp == pytest.approx(expected_ts)


def test_cells_to_observations_falls_back_to_now_on_bad_timestamp(opera_module):
    p = opera_module.OperaProvider(51.026, 4.478)
    cell = _make_cell(opera_module, 35.0)
    before = time.time()
    obs = p._cells_to_observations([cell], "niet-een-geldige-timestamp")
    after = time.time()
    assert before <= obs[0].timestamp <= after


def test_cells_to_observations_preserves_area_and_source(opera_module):
    p = opera_module.OperaProvider(51.026, 4.478)
    cell = _make_cell(opera_module, 35.0, area_km2=42.5)
    cell.footprint_points = ((50.9, 4.4), (51.1, 4.6))
    obs = p._cells_to_observations([cell], "2026-07-12T12:00:00Z")
    assert obs[0].area_km2 == 42.5
    assert obs[0].source == "opera"
    assert obs[0].quality == 0.9
    assert obs[0].obs_type == opera_module.ObservationType.RADAR
    assert obs[0].footprint_points == cell.footprint_points
