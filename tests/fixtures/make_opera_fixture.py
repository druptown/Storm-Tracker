"""Storm Tracker V3 — tests/fixtures/make_opera_fixture.py

Bouwt een klein, synthetisch ODIM-HDF5-bestand met dezelfde structuur als
een echt OPERA DBZH-composiet, maar met een handvol pixels in plaats van
4.400 x 3.800 — zodat tests deterministisch en snel blijven.

Structuur (identiek aan een echt product, kleiner grid):
    /where            attrs: projdef, xsize, ysize, xscale, yscale
    /what             attrs: date, time
    /dataset1/data1/data              — ruwe radarwaarden (vóór gain/offset)
    /dataset1/data1/quality1/data     — kwaliteit per pixel (0.0-1.0)
    /dataset1/data1/what              attrs: nodata, undetect, gain, offset

Projectie: LAEA gecentreerd op TEST_LAT/TEST_LON, met x_0/y_0 zo gekozen
dat het testpunt exact op pixel (row=50, col=50) van het 100x100-grid valt
(xscale=yscale=1000m -> grid dekt 100km x 100km, testpunt in het midden).
Dit maakt alle verwachte pixel-locaties met de hand na te rekenen.

gain=0.5, offset=-5.0 (bewust ≠ 1/0) zodat een test die gain/offset niet
toepast, meetbaar faalt.
"""
from __future__ import annotations

import numpy as np
import h5py

TEST_LAT = 51.026
TEST_LON = 4.478

XSIZE = YSIZE = 100
XSCALE = YSCALE = 1000.0  # meter per pixel

PROJDEF = (
    f"+proj=laea +lat_0={TEST_LAT} +lon_0={TEST_LON} "
    f"+x_0=50000 +y_0=-50000 +units=m +ellps=WGS84"
)

NODATA_RAW   = -9999000.0
UNDETECT_RAW = -8888000.0
GAIN         = 0.5
OFFSET       = -5.0

DATE = "20260712"
TIME = "120000"


def _dbz_to_raw(dbz: float) -> float:
    """Inverse van (raw * gain + offset) -> gebruikt om testwaarden te plaatsen."""
    return (dbz - OFFSET) / GAIN


def build_fixture(path: str) -> dict:
    """
    Bouwt het synthetische ODIM-bestand op `path`.

    Legt op vaste, met de hand na te rekenen pixellocaties:
      - "storm"        : compacte cel, ruim boven dbz/quality-drempels (moet gevonden worden)
      - "low_quality"   : zelfde dBZ als storm, maar quality te laag (moet gefilterd worden)
      - "low_dbz"       : voldoende quality, maar dBZ te laag (moet gefilterd worden)
      - "too_small"     : voldoende dBZ/quality, maar < MIN_PIXELS (moet gefilterd worden)
      - "undetect_patch": undetect-sentinelwaarde (moet als ongeldig gemaskeerd worden)
      - "edge_storm"    : cel die deels buiten een opzettelijk kleinere crop-bbox valt

    Returns: dict met alle metadata die tests nodig hebben om verwachtingen
    te berekenen (grid-parameters + pixel-vensters per synthetische cel).
    """
    radar   = np.full((YSIZE, XSIZE), NODATA_RAW, dtype=np.float64)
    quality = np.zeros((YSIZE, XSIZE), dtype=np.float64)

    def _fill(row0, row1, col0, col1, dbz, qual):
        radar[row0:row1, col0:col1]   = _dbz_to_raw(dbz)
        quality[row0:row1, col0:col1] = qual

    cells = {
        # 5x5 = 25 pixels, ruim boven MIN_PIXELS(9)/MIN_DBZ(10)/MIN_QUALITY(0.5)
        "storm":         dict(window=(40, 45, 60, 65), dbz=35.0, quality=0.9),
        # Zelfde grootte/dBZ, maar kwaliteit te laag -> moet verdwijnen
        "low_quality":    dict(window=(10, 15, 10, 15), dbz=35.0, quality=0.2),
        # Zelfde grootte/kwaliteit, maar dBZ te laag -> moet verdwijnen
        "low_dbz":        dict(window=(70, 75, 70, 75), dbz=6.0,  quality=0.9),
        # 2x2 = 4 pixels (< MIN_PIXELS) ondanks goede dBZ/kwaliteit -> moet verdwijnen
        "too_small":      dict(window=(20, 22, 80, 82), dbz=35.0, quality=0.9),
        # Cel die precies op de rand van de (kleinere) test-cropbbox ligt
        "edge_storm":     dict(window=(0, 5, 0, 5),     dbz=40.0, quality=0.95),
    }
    for cell in cells.values():
        r0, r1, c0, c1 = cell["window"]
        _fill(r0, r1, c0, c1, cell["dbz"], cell["quality"])

    # Undetect-patch: expliciete undetect-sentinelwaarde, geldige quality
    radar[90:93, 90:93]   = UNDETECT_RAW
    quality[90:93, 90:93] = 0.9

    with h5py.File(path, "w") as h5:
        where = h5.create_group("where")
        where.attrs["projdef"] = PROJDEF
        where.attrs["xsize"]   = XSIZE
        where.attrs["ysize"]   = YSIZE
        where.attrs["xscale"]  = XSCALE
        where.attrs["yscale"]  = YSCALE

        what = h5.create_group("what")
        what.attrs["date"] = DATE
        what.attrs["time"] = TIME

        ds1 = h5.create_group("dataset1/data1")
        ds1.create_dataset("data", data=radar)
        q1 = ds1.create_group("quality1")
        q1.create_dataset("data", data=quality)
        ds1_what = ds1.create_group("what")
        ds1_what.attrs["nodata"]   = NODATA_RAW
        ds1_what.attrs["undetect"] = UNDETECT_RAW
        ds1_what.attrs["gain"]     = GAIN
        ds1_what.attrs["offset"]   = OFFSET

    return {
        "test_lat": TEST_LAT,
        "test_lon": TEST_LON,
        "xsize": XSIZE, "ysize": YSIZE,
        "xscale": XSCALE, "yscale": YSCALE,
        "projdef": PROJDEF,
        "cells": cells,
        "date": DATE, "time": TIME,
        "gain": GAIN, "offset": OFFSET,
        "nodata": NODATA_RAW, "undetect": UNDETECT_RAW,
    }


if __name__ == "__main__":
    import sys
    meta = build_fixture(sys.argv[1] if len(sys.argv) > 1 else "/tmp/opera_fixture.h5")
    print(meta)
