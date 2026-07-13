"""Storm Tracker V3 — engine/observation.py v0.1.0

Generiek Observation-model.

Providers leveren uitsluitend Observation-objecten — geen storms,
geen clustering, geen logica. De Observation Fusion Engine (voorheen
Strike Engine) en Storm Engine doen al het verdere werk.

Observatie-types:
  LIGHTNING  — blikseminslag (Blitzortung): puntvormig, hoge precisie
  RADAR      — radarpixel (KMI/RainViewer): vlakdekkend, neerslagintensiteit
  RAIN       — regenverificatie (Netatmo): grondstation-meting

Elk type heeft zijn eigen velden. De Storm Engine beslist per type
hoe een observatie het Storm-object beïnvloedt:
  - LIGHTNING: clustering, history, regressie (richting/snelheid)
  - RADAR:     hull uitbreiden, intensiteit, bevestiging van bui-aanwezigheid
  - RAIN:      betrouwbaarheid verhogen/verlagen (nooit een nieuwe storm aanmaken)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ObservationType(Enum):
    LIGHTNING = "lightning"   # Blitzortung: puntvormige inslag
    RADAR     = "radar"       # KMI/RainViewer: neerslagpixel
    RAIN      = "rain"        # Netatmo: grondstations-regenbevestiging


@dataclass(slots=True)
class Observation:
    """
    Eén ruwe observatie van een willekeurige provider.

    Providers vullen alleen de velden in die relevant zijn voor hun type.
    Lege/None velden worden door de Storm Engine genegeerd.
    """
    # Verplicht voor alle types
    obs_type:   ObservationType
    lat:        float
    lon:        float
    timestamp:  float          # Unix timestamp

    # LIGHTNING-specifiek
    # (geen extra velden — lat/lon/timestamp is volledig voor bliksem)

    # RADAR-specifiek
    intensity:  Optional[int]  = None   # 1-8 pixelwaarde (KMI-schaal)
    area_km2:   Optional[float] = None  # oppervlakte van het radarpixelcluster
    quality:    Optional[float] = None  # 0-1 bronkwaliteit, indien beschikbaar
    # Compacte puntenwolk op de werkelijk bezette radarcel. OPERA bewaart
    # maximaal ongeveer één representatief punt per 8x8 km rasterblok.
    footprint_points: tuple[tuple[float, float], ...] = ()

    # RAIN-specifiek (Netatmo grondstation)
    rain_mm:     Optional[float] = None   # mm/u gemeten (rain_live)
    rain_5min:   Optional[float] = None   # mm/u gemiddeld over 5 min
    station_id:  Optional[str]   = None   # voor deduplicatie

    # Wind (Netatmo windmodule)
    wind_speed:  Optional[float] = None   # km/u
    wind_angle:  Optional[float] = None   # graden (meteorologisch: vanwaar)
    gust_speed:  Optional[float] = None   # km/u windstoot

    # Atmosferisch (Netatmo hoofdmodule)
    pressure:    Optional[float] = None   # mbar absolute druk
    temperature: Optional[float] = None   # °C
    humidity:    Optional[float] = None   # %RH

    # Bron-label (voor logging en debugging)
    source:      str = "unknown"
