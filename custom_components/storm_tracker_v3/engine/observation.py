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

from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
import math
from typing import Any, Optional, TypeAlias


GeoPoint: TypeAlias = tuple[float, float]
LinearRing: TypeAlias = tuple[GeoPoint, ...]
FootprintPolygon: TypeAlias = tuple[LinearRing, ...]
FootprintMultiPolygon: TypeAlias = tuple[FootprintPolygon, ...]


def immutable_footprint_points(
    value: object,
    *,
    field_name: str = "footprint_points",
) -> tuple[GeoPoint, ...]:
    """Maak een legacy puntenpad immutable zonder ongeldige punten te verbergen."""
    if value is None:
        return ()
    try:
        raw_points = tuple(value)  # type: ignore[arg-type]
    except TypeError as err:
        raise ValueError(f"{field_name} moet een puntenreeks zijn") from err
    points: list[GeoPoint] = []
    for raw_point in raw_points:
        try:
            latitude, longitude = raw_point
        except (TypeError, ValueError) as err:
            raise ValueError(
                f"{field_name} bevat geen geldig (lat, lon)-punt"
            ) from err
        if isinstance(latitude, bool) or isinstance(longitude, bool):
            raise ValueError(f"{field_name} bevat een booleaanse coordinaat")
        try:
            point = (float(latitude), float(longitude))
        except (TypeError, ValueError, OverflowError) as err:
            raise ValueError(
                f"{field_name} bevat geen numerieke coordinaat"
            ) from err
        if (
            not math.isfinite(point[0])
            or not math.isfinite(point[1])
            or not -90.0 <= point[0] <= 90.0
            or not -180.0 <= point[1] <= 180.0
        ):
            raise ValueError(f"{field_name} bevat een ongeldige coordinaat")
        points.append(point)
    return tuple(points)


def immutable_footprint_polygons(
    value: object,
    *,
    field_name: str = "footprint_polygons",
) -> FootprintMultiPolygon:
    """Valideer en materialiseer een volledige immutable MultiPolygon."""
    if value is None:
        return ()
    try:
        raw_polygons = tuple(value)  # type: ignore[arg-type]
    except TypeError as err:
        raise ValueError(f"{field_name} moet een multipolygon zijn") from err
    polygons: list[FootprintPolygon] = []
    for raw_polygon in raw_polygons:
        try:
            raw_rings = tuple(raw_polygon)
        except TypeError as err:
            raise ValueError(
                f"{field_name} bevat geen geldige polygon"
            ) from err
        if not raw_rings:
            raise ValueError(f"{field_name} bevat een polygon zonder ringen")
        rings: list[LinearRing] = []
        for raw_ring in raw_rings:
            ring = immutable_footprint_points(
                raw_ring,
                field_name=field_name,
            )
            open_ring = (
                ring[:-1]
                if len(ring) > 1 and ring[0] == ring[-1]
                else ring
            )
            if len(set(open_ring)) < 3:
                raise ValueError(
                    f"{field_name} bevat een ring met minder dan drie punten"
                )
            rings.append(ring)
        polygons.append(tuple(rings))
    return tuple(polygons)


def footprint_vertices(
    polygons: FootprintMultiPolygon,
    legacy_points: tuple[GeoPoint, ...] = (),
) -> tuple[GeoPoint, ...]:
    """Geef alle ringpunten; gebruik legacy alleen als topology ontbreekt."""
    if polygons:
        return tuple(
            point
            for polygon in polygons
            for ring in polygon
            for point in ring
        )
    return tuple(legacy_points)


def footprint_geometry(
    polygons: FootprintMultiPolygon,
    legacy_points: tuple[GeoPoint, ...] = (),
    *,
    fallback_point: GeoPoint | None = None,
) -> object:
    """Geef de rijkste beschikbare geometrie zonder ringen samen te voegen."""
    if polygons:
        return polygons
    if legacy_points:
        return legacy_points
    return (fallback_point,) if fallback_point is not None else ()


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
    received_at: Optional[float] = None

    # LIGHTNING-specifiek
    # (geen extra velden — lat/lon/timestamp is volledig voor bliksem)

    # RADAR-specifiek
    intensity:  Optional[int]  = None   # 1-8 pixelwaarde (KMI-schaal)
    max_dbz:    Optional[float] = None  # exacte piekreflectiviteit indien beschikbaar
    mean_dbz:   Optional[float] = None  # gemiddelde reflectiviteit van de cel
    area_km2:   Optional[float] = None  # oppervlakte van het radarpixelcluster
    quality:    Optional[float] = None  # 0-1 bronkwaliteit, indien beschikbaar
    # Compacte puntenwolk op de werkelijk bezette radarcel. OPERA bewaart
    # maximaal ongeveer één representatief punt per 8x8 km rasterblok.
    footprint_points: tuple[tuple[float, float], ...] = ()
    # Autoritatieve topologie: MultiPolygon -> Polygon -> outer/holes -> punt.
    # De vlakke puntenreeks blijft alleen als achterwaartse compatibiliteit.
    footprint_polygons: FootprintMultiPolygon = ()
    # Radarhiërarchie. Een bron kan één brede neerslagband opsplitsen in
    # meerdere lokale cellen. ``parent_system_id`` groepeert die cellen per
    # radarframe; de WeatherSystem Engine bewaart vervolgens de kinderen.
    radar_cell_id: Optional[str] = None
    parent_system_id: Optional[str] = None
    parent_area_km2: Optional[float] = None
    parent_footprint_points: tuple[tuple[float, float], ...] = ()
    parent_footprint_polygons: FootprintMultiPolygon = ()

    # RAIN-specifiek (Netatmo grondstation)
    rain_mm:     Optional[float] = None   # mm/u gemeten (rain_live)
    rain_5min:   Optional[float] = None   # mm/u gemiddeld over 5 min
    rain_60min:  Optional[float] = None   # accumulatie afgelopen uur
    station_id:  Optional[str]   = None   # voor deduplicatie
    rain_capable: bool = False            # station heeft werkelijk een regenmodule
    precipitation_present: Optional[bool] = None
    quantitative_precipitation: bool = True

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
    raw_text: Optional[str] = None
    raw_payload: Optional[dict[str, Any]] = None
    measurement_kind: Optional[str] = None
    measurement_unit: Optional[str] = None
    interval_minutes: Optional[float] = None
    parser_version: Optional[str] = None
    # Optionele, secretvrije ketenreferenties. De concrete raw bytes blijven
    # buiten Observation; deze velden verwijzen er alleen verliesloos naar.
    artifact_id: Optional[str] = None
    raw_artifact_receipt_id: Optional[str] = None
    upstream_record_id: Optional[str] = None
    contribution_id: Optional[str] = None
    provenance_grade: Optional[str] = None
    provenance: Optional[dict[str, Any]] = None
    interpretation_reason: Optional[str] = None
    quality_flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        self.footprint_points = immutable_footprint_points(
            self.footprint_points,
            field_name="footprint_points",
        )
        self.footprint_polygons = immutable_footprint_polygons(
            self.footprint_polygons,
            field_name="footprint_polygons",
        )
        self.parent_footprint_points = immutable_footprint_points(
            self.parent_footprint_points,
            field_name="parent_footprint_points",
        )
        self.parent_footprint_polygons = immutable_footprint_polygons(
            self.parent_footprint_polygons,
            field_name="parent_footprint_polygons",
        )
        if self.footprint_polygons and not self.footprint_points:
            self.footprint_points = self.footprint_polygons[0][0]
        if (
            self.parent_footprint_polygons
            and not self.parent_footprint_points
        ):
            self.parent_footprint_points = (
                self.parent_footprint_polygons[0][0]
            )
        if self.provenance is not None:
            if not isinstance(self.provenance, dict):
                raise TypeError("provenance moet een object of None zijn")
            self.provenance = deepcopy(self.provenance)


def observation_is_wet(observation: Observation) -> bool:
    """Gebruik expliciete aanwezigheid vóór een kwantitatieve regenwaarde."""
    if observation.precipitation_present is not None:
        return bool(observation.precipitation_present)
    return float(observation.rain_mm or 0.0) >= 0.1
