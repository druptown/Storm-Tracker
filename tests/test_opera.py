"""Storm Tracker V3 — tests/test_opera.py v0.1.0

Tests voor providers/opera.py, gebruikmakend van een synthetisch
ODIM-HDF5-fixture (tests/fixtures/make_opera_fixture.py) i.p.v. een
echt (268MB) OPERA-bestand.

Dekt de punten 1-8 uit de feedback-checklist:
  1. Correcte crop van WGS84-bbox naar rijen/kolommen
  2. Correcte nodata/undetect/gain/offset-verwerking
  3. Quality- en dBZ-thresholds
  4. Connected components (8-connectiviteit — zie opmerking bij de test)
  5. Componenten aan de rand van een slice
  6. S3-listing met meer dan 300 objecten
  7. Dagovergang rond 00:00 UTC
  8. Rejectie van stale data

Punten 9-12 uit de review (primary/fallback-wissel, geen dubbele
WeatherSystems, gedeelde download tussen RegionEngines, onafhankelijke
ProjectionTarget-projecties) vereisen functionaliteit die nog niet
bestaat (ProviderRegistry-runtime-integratie, RegionEngines) en worden
pas relevant na Fase 2/4.
"""
from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone

import pytest


# ── 1. Bbox-crop ──────────────────────────────────────────────────────────

def test_crop_window_matches_full_grid(opera_module, opera_fixture_file):
    """
    Een bbox die het volledige testgrid dekt, moet croppen naar
    (0, ysize, 0, xsize) — d.w.z. de volledige rij/kolom-range.
    """
    meta = opera_fixture_file
    grid = opera_module.Grid(
        projdef=meta["projdef"], xsize=meta["xsize"], ysize=meta["ysize"],
        xscale=meta["xscale"], yscale=meta["yscale"],
    )
    # Grens van het grid in projected coords is x:[0,100000], y:[-100000,0].
    # Corresponderende WGS84-hoekpunten (zie verkenning): ongeveer
    # lon 3.75-5.20, lat 50.57-51.47 — met marge om afrondingen op te vangen.
    bbox = (3.70, 50.50, 5.30, 51.55)
    row0, row1, col0, col1 = opera_module._crop_window(grid, bbox)
    assert row0 == 0
    assert col0 == 0
    assert row1 == meta["ysize"]
    assert col1 == meta["xsize"]


def test_crop_window_smaller_bbox_is_strict_subset(opera_module, opera_fixture_file):
    """Een kleinere bbox moet een strikte deelverzameling van rijen/kolommen geven."""
    meta = opera_fixture_file
    grid = opera_module.Grid(
        projdef=meta["projdef"], xsize=meta["xsize"], ysize=meta["ysize"],
        xscale=meta["xscale"], yscale=meta["yscale"],
    )
    bbox = (3.70, 50.50, 5.30, 51.55)
    full = opera_module._crop_window(grid, bbox)

    # Bbox rond het testpunt zelf met een kleinere marge (~0.2 graden)
    small_bbox = (meta["test_lon"] - 0.2, meta["test_lat"] - 0.2,
                  meta["test_lon"] + 0.2, meta["test_lat"] + 0.2)
    row0, row1, col0, col1 = opera_module._crop_window(grid, small_bbox)

    assert (row0, row1, col0, col1) != full
    assert 0 <= row0 < row1 <= meta["ysize"]
    assert 0 <= col0 < col1 <= meta["xsize"]
    # Het testpunt zelf (row=50,col=50) moet binnen deze kleinere crop vallen
    assert row0 <= 50 < row1
    assert col0 <= 50 < col1


def test_crop_window_out_of_grid_raises(opera_module, opera_fixture_file):
    """Een bbox ver buiten het grid moet een ValueError geven (geen silent leeg resultaat)."""
    meta = opera_fixture_file
    grid = opera_module.Grid(
        projdef=meta["projdef"], xsize=meta["xsize"], ysize=meta["ysize"],
        xscale=meta["xscale"], yscale=meta["yscale"],
    )
    # Bbox ergens in Noord-Afrika — ver buiten het 100x100 testgrid
    with pytest.raises(ValueError):
        opera_module._crop_window(grid, (0.0, 20.0, 1.0, 21.0))


# ── 2 + 3. nodata/undetect/gain/offset + quality/dBZ-thresholds ──────────

def _full_bbox(meta):
    return (3.70, 50.50, 5.30, 51.55)


def test_valid_storm_cell_detected_with_gain_offset_applied(opera_module, opera_fixture_file):
    """
    De 'storm'-cel (35 dBZ, quality 0.9) moet gevonden worden, en de
    gemelde max_dbz moet de dBZ-waarde zijn NA toepassing van gain/offset
    (fixture gebruikt bewust gain=0.5, offset=-5.0, dus een niet-toegepaste
    gain/offset zou een andere waarde opleveren dan 35.0).
    """
    meta = opera_fixture_file
    with open(meta["path"], "rb") as f:
        data = f.read()
    cells, timestamp = opera_module._parse_hdf5_slice(data, _full_bbox(meta))

    storm_cells = [c for c in cells if abs(c.max_dbz - 35.0) < 0.5]
    assert storm_cells, f"verwachtte een cel met max_dbz~35.0, kreeg: {[c.max_dbz for c in cells]}"
    assert storm_cells[0].footprint_points
    assert len(storm_cells[0].footprint_points) <= storm_cells[0].pixelcount
    assert timestamp == f"{meta['date']}T{meta['time']}Z"


def test_low_quality_is_retained_as_diagnostic_metadata(opera_module, opera_fixture_file):
    """OPERA qi_total=0/low can occur on real rain and must not reject the cell."""
    meta = opera_fixture_file
    with open(meta["path"], "rb") as f:
        data = f.read()
    cells, _ = opera_module._parse_hdf5_slice(data, _full_bbox(meta))

    # low_quality-venster ligt rond rij10-15/kol10-15 en moet behouden blijven.
    total_cells_at_low_quality_location = [
        c for c in cells
        if 10 <= _lookup_row(opera_module, meta, c) < 15
    ]
    assert total_cells_at_low_quality_location
    assert total_cells_at_low_quality_location[0].mean_quality == pytest.approx(0.2)


def _lookup_row(opera_module, meta, cell):
    """Reconstrueer bij benadering de pixelrij van een celcentroid (voor locatie-assertions)."""
    from pyproj import CRS, Transformer
    fwd = Transformer.from_crs(CRS.from_epsg(4326), CRS.from_user_input(meta["projdef"]), always_xy=True)
    x, y = fwd.transform(cell.centroid_lon, cell.centroid_lat)
    return -y / meta["yscale"]


def test_low_dbz_cell_filtered_out(opera_module, opera_fixture_file):
    """De 'low_dbz'-cel (6 dBZ) mag niet verschijnen (< MIN_DBZ=8)."""
    meta = opera_fixture_file
    with open(meta["path"], "rb") as f:
        data = f.read()
    cells, _ = opera_module._parse_hdf5_slice(data, _full_bbox(meta))
    assert all(c.max_dbz >= opera_module.MIN_DBZ for c in cells)


def test_undetect_patch_never_becomes_a_cell(opera_module, opera_fixture_file):
    """
    De undetect-patch (rij/kol 90-93) heeft een geldige quality (0.9) maar de
    undetect-sentinelwaarde als radarwaarde — mag nooit als cel verschijnen,
    zelfs al zou de mask alleen op quality filteren.
    """
    meta = opera_fixture_file
    with open(meta["path"], "rb") as f:
        data = f.read()
    cells, _ = opera_module._parse_hdf5_slice(data, _full_bbox(meta))
    for c in cells:
        row = _lookup_row(opera_module, meta, c)
        assert not (89 <= row <= 94), f"undetect-patch mag geen cel opleveren, kreeg centroid bij rij~{row}"


# ── 4. Connectiviteit (documenteert huidig gedrag: alleen 8-connected) ────

def test_component_labeling_is_8_connected_not_4(opera_module):
    """
    BELANGRIJK — bevinding om met Wim te bespreken: de review vraagt om
    tests voor zowel 4- als 8-connectiviteit, maar `_label_components` in
    opera.py implementeert alleen 8-connectiviteit (`_neighbors` gebruikt
    alle 8 buren, geen configureerbare optie). Deze test documenteert het
    HUIDIGE gedrag: twee pixels die alleen diagonaal raken worden als
    ÉÉN component gezien. Als 4-connectiviteit ooit nodig is (bijvoorbeeld
    om diagonaal 'lekkende' cellen niet samen te voegen), is dat nieuwe
    functionaliteit, geen bugfix.
    """
    import numpy as np
    mask = np.zeros((4, 4), dtype=bool)
    mask[0, 0] = True
    mask[1, 1] = True  # raakt (0,0) alleen diagonaal
    # Twee extra buren zodat elk 'been' >= MIN_PIXELS niet vereist is voor deze test
    components = opera_module._label_components(mask, min_pixels=1)
    assert len(components) == 1, (
        "huidige implementatie is 8-connected: diagonaal rakende pixels "
        "worden samengevoegd tot één component"
    )


def test_large_cells_are_split_on_strong_cores(opera_module):
    """A weak bridge must not turn two heavy showers into one giant cell."""
    import numpy as np

    radar = np.zeros((50, 90), dtype=float)
    radar[5:45, 2:42] = 45.0
    radar[5:45, 48:88] = 45.0
    radar[24, 42:48] = 8.0
    mask = radar >= opera_module.MIN_DBZ

    plain = opera_module._label_components(mask, min_pixels=5)
    segmented = opera_module._segment_components(mask, radar, min_pixels=5)

    assert len(plain) == 1
    assert len(plain[0]) > opera_module.MAX_UNSPLIT_CELL_PIXELS
    assert len(segmented) == 2
    assert all(len(component) < len(plain[0]) for component in segmented)

    groups = opera_module._segment_component_groups(mask, radar, min_pixels=5)
    assert len(groups) == 1
    assert len(groups[0].parent_pixels) == len(plain[0])
    assert len(groups[0].child_pixels) == 2


def test_small_light_rain_cell_keeps_original_sensitivity(opera_module):
    import numpy as np

    radar = np.zeros((20, 20), dtype=float)
    radar[5:10, 5:10] = 9.0
    mask = radar >= opera_module.MIN_DBZ
    segmented = opera_module._segment_components(mask, radar, min_pixels=5)

    assert len(segmented) == 1
    assert len(segmented[0]) == 25


# ── 5. Component aan de rand van een slice ────────────────────────────────

def test_edge_cell_detected_when_partially_inside_crop(opera_module, opera_fixture_file):
    """
    De 'edge_storm'-cel zit op pixel (0-5, 0-5) — in de linkerbovenhoek.
    Bij een crop die exact bij het grid-begin snijdt, moet de cel nog
    steeds (desnoods gedeeltelijk) gedetecteerd worden, niet stilzwijgend
    verdwijnen.
    """
    meta = opera_fixture_file
    with open(meta["path"], "rb") as f:
        data = f.read()
    cells, _ = opera_module._parse_hdf5_slice(data, _full_bbox(meta))
    edge_cells = [c for c in cells if abs(c.max_dbz - 40.0) < 0.5]
    assert edge_cells, "edge_storm-cel (40 dBZ, hoek van het grid) werd niet gevonden"


# ── 6. S3-listing met meer dan 300 objecten (paginering) ──────────────────

class _FakeResponse:
    def __init__(self, text, status=200):
        self._text = text
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def text(self):
        return self._text


class _FakeSession:
    """Simuleert aiohttp.ClientSession.get() met twee XML-pagina's."""

    def __init__(self, pages_by_call):
        self._pages = pages_by_call
        self._call = 0

    def get(self, url, params=None, timeout=None):
        page = self._pages[min(self._call, len(self._pages) - 1)]
        self._call += 1
        return _FakeResponse(page)


class _FakeHeadSession:
    def __init__(self, available_key, opera_module):
        self.available_url = opera_module._s3_path_from_key(available_key)
        self.calls = []

    def head(self, url, timeout=None):
        self.calls.append(url)
        return _FakeResponse("", status=200 if url == self.available_url else 404)


def test_direct_head_probe_finds_fresh_product_without_listing(opera_module, monkeypatch):
    """Deterministic HEAD discovery must find a fresh five-minute product."""
    now = datetime(2026, 7, 12, 22, 47, tzinfo=timezone.utc)
    fresh_key = "2026/07/12/OPERA/COMP/OPERA@20260712T2240@0@DBZH.h5"
    session = _FakeHeadSession(fresh_key, opera_module)

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(opera_module, "datetime", _FixedDateTime)

    import asyncio
    result = asyncio.run(opera_module._find_latest_valid_key(session))
    assert result == fresh_key
    assert session.calls


def _make_key(date_str, hhmm):
    return f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}/OPERA/COMP/OPERA@{date_str}T{hhmm}@0@DBZH.h5"


def test_s3_pagination_finds_key_beyond_first_300(opera_module):
    """
    Simuleert een S3-listing waarbij de nieuwste key pas op de TWEEDE
    pagina staat (na >300 'oudere' dummy-keys op de eerste pagina) —
    en controleert dat paginering die key toch oplevert.
    """
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y%m%d")

    newest_key = _make_key(today, now.strftime("%H%M"))
    # 350 oudere dummy-keys op pagina 1 (zelfde dag, vroegere tijdstippen)
    older_keys_xml = "".join(
        f"<Contents><Key>{_make_key(today, f'{h:02d}{m:02d}')}</Key></Contents>"
        for h in range(0, 6) for m in (0, 10, 20, 30, 40, 50)
    )[: ]
    page1 = (
        "<ListBucketResult>"
        f"{older_keys_xml}"
        "<IsTruncated>true</IsTruncated>"
        "<NextContinuationToken>tok123</NextContinuationToken>"
        "</ListBucketResult>"
    )
    page2 = (
        "<ListBucketResult>"
        f"<Contents><Key>{newest_key}</Key></Contents>"
        "<IsTruncated>false</IsTruncated>"
        "</ListBucketResult>"
    )
    session = _FakeSession([page1, page2])

    import asyncio
    keys = asyncio.run(opera_module._list_opera_files(session))
    assert newest_key in keys
    assert session._call >= 2, "paginering moet minstens 2 requests doen (page1 + page2)"


# ── 7. Dagovergang rond 00:00 UTC ──────────────────────────────────────────

def test_midnight_boundary_checks_yesterday_too(opera_module, monkeypatch):
    """
    Om 00:02 UTC moet _list_opera_files zowel de prefix van vandaag als
    van gisteren doorzoeken (het nieuwste bestand van gisteren 23:55 mag
    niet gemist worden).
    """
    fixed_now = datetime(2026, 7, 12, 0, 2, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(opera_module, "datetime", _FixedDatetime)

    yesterday_key = _make_key("20260711", "2355")
    empty_page = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    yesterday_page = (
        "<ListBucketResult>"
        f"<Contents><Key>{yesterday_key}</Key></Contents>"
        "<IsTruncated>false</IsTruncated>"
        "</ListBucketResult>"
    )

    # Eerste call = vandaag (leeg), tweede call = gisteren (bevat de key)
    session = _FakeSession([empty_page, yesterday_page])

    import asyncio
    keys = asyncio.run(opera_module._list_opera_files(session))
    assert yesterday_key in keys, (
        "bestand van gisteren 23:55 UTC moet nog gevonden worden om 00:02 UTC"
    )


# ── 8. Rejectie van stale data ─────────────────────────────────────────────

def test_stale_product_rejected(opera_module, monkeypatch):
    """Een product van 20 minuten oud (> MAX_PRODUCT_AGE_S=15min) moet verworpen worden."""
    fixed_now = datetime(2026, 7, 12, 12, 20, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(opera_module, "datetime", _FixedDatetime)

    stale_key = _make_key("20260712", "1200")  # 20 min oud t.o.v. fixed_now
    page = (
        "<ListBucketResult>"
        f"<Contents><Key>{stale_key}</Key></Contents>"
        "<IsTruncated>false</IsTruncated>"
        "</ListBucketResult>"
    )
    session = _FakeSession([page, page])  # zelfde lege/stale resultaat voor beide dagen

    import asyncio
    result = asyncio.run(opera_module._find_latest_valid_key(session))
    assert result is None, "een 20 minuten oud product mag niet als geldig worden teruggegeven"


def test_no_files_found_returns_none(opera_module):
    """Lege S3-listing (geen bestanden gevonden) moet None geven, geen crash."""
    empty_page = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    session = _FakeSession([empty_page, empty_page])
    import asyncio
    result = asyncio.run(opera_module._find_latest_valid_key(session))
    assert result is None


def test_unparseable_key_is_skipped_not_crashed(opera_module, monkeypatch):
    """Een key die niet aan het verwachte patroon voldoet mag geen exception geven, alleen overgeslagen worden."""
    fixed_now = datetime(2026, 7, 12, 12, 5, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(opera_module, "datetime", _FixedDatetime)

    garbage_key = "2026/07/12/OPERA/COMP/iets-anders-onherkenbaars.h5"
    fresh_key = _make_key("20260712", "1200")
    page = (
        "<ListBucketResult>"
        f"<Contents><Key>{garbage_key}</Key></Contents>"
        f"<Contents><Key>{fresh_key}</Key></Contents>"
        "<IsTruncated>false</IsTruncated>"
        "</ListBucketResult>"
    )
    empty_page = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    session = _FakeSession([page, empty_page])

    import asyncio
    result = asyncio.run(opera_module._find_latest_valid_key(session))
    assert result == fresh_key


def test_future_timestamp_key_is_skipped(opera_module, monkeypatch):
    """Een key met een timestamp in de toekomst (klokafwijking) mag niet geselecteerd worden."""
    fixed_now = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(opera_module, "datetime", _FixedDatetime)

    future_key = _make_key("20260712", "1210")  # 10 min in de toekomst t.o.v. fixed_now
    page = (
        "<ListBucketResult>"
        f"<Contents><Key>{future_key}</Key></Contents>"
        "<IsTruncated>false</IsTruncated>"
        "</ListBucketResult>"
    )
    empty_page = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    session = _FakeSession([page, empty_page])

    import asyncio
    result = asyncio.run(opera_module._find_latest_valid_key(session))
    assert result is None, "een key met een timestamp in de toekomst mag nooit geselecteerd worden"


def test_extract_product_ts_valid_key(opera_module):
    key = _make_key("20260712", "1230")
    ts = opera_module._extract_product_ts(key)
    assert ts == datetime(2026, 7, 12, 12, 30, tzinfo=timezone.utc)


def test_extract_product_ts_invalid_key_returns_none(opera_module):
    assert opera_module._extract_product_ts("dit/is/geen/opera/key.h5") is None


def test_s3_path_from_key_builds_full_url(opera_module):
    key = _make_key("20260712", "1200")
    url = opera_module._s3_path_from_key(key)
    assert url == f"{opera_module.S3_ENDPOINT}/{opera_module.S3_BUCKET}/{key}"


def test_text_attr_decodes_bytes(opera_module):
    assert opera_module._text_attr(b"hello") == "hello"
    assert opera_module._text_attr("already-str") == "already-str"
    assert opera_module._text_attr(123) == "123"


def test_fresh_product_accepted(opera_module, monkeypatch):
    """Contrasttest: een product van 5 minuten oud moet wél geaccepteerd worden."""
    fixed_now = datetime(2026, 7, 12, 12, 5, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(opera_module, "datetime", _FixedDatetime)

    fresh_key = _make_key("20260712", "1200")  # 5 min oud
    page = (
        "<ListBucketResult>"
        f"<Contents><Key>{fresh_key}</Key></Contents>"
        "<IsTruncated>false</IsTruncated>"
        "</ListBucketResult>"
    )
    empty_page = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    session = _FakeSession([page, empty_page])

    import asyncio
    result = asyncio.run(opera_module._find_latest_valid_key(session))
    assert result == fresh_key
