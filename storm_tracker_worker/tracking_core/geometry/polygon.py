"""Bolgeometrie voor geografische footprints.

De publieke helpers accepteren vier verliesloze vormen:

* een legacy ring/punt/lijn als ``[(lat, lon), ...]``;
* een Polygon als ``[outer_ring, hole_ring, ...]``;
* een MultiPolygon als ``[[outer_ring, ...], [outer_ring, ...], ...]``;
* een GeoJSON ``Polygon`` of ``MultiPolygon`` mapping. Alleen in GeoJSON is
  de positievolgorde conform de standaard ``(lon, lat)``.

Polygonranden zijn de kortste grootcirkelbogen tussen opeenvolgende punten.
Daardoor blijven afstand en containment correct rond de antimeridiaan en op
hoge breedte. Geometrie die niet eenduidig of ongeldig is, geeft ``None``;
er worden nooit stilzwijgend ongeldige punten verwijderd.
"""
from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

EARTH_RADIUS_KM = 6371.0088
_ANGULAR_EPSILON = 1e-12
_BOUNDARY_EPSILON_KM = 1e-6
_HEMISPHERE_EPSILON = 1e-10
_MAX_GEOMETRY_ITEMS = 100_000
_MAX_POLYGONS = 4_096
_MAX_RINGS_PER_POLYGON = 256
_MAX_RING_POINTS = 4_096

_Position = tuple[float, float]
_Vector = tuple[float, float, float]
_Point2D = tuple[float, float]


@dataclass(frozen=True)
class _PreparedRing:
    positions: tuple[_Position, ...]
    vectors: tuple[_Vector, ...]
    projected: tuple[_Point2D, ...]


@dataclass(frozen=True)
class _PreparedPolygon:
    reference: _Vector
    east: _Vector
    north: _Vector
    outer: _PreparedRing
    holes: tuple[_PreparedRing, ...]


@dataclass(frozen=True)
class _PreparedGeometry:
    polygons: tuple[_PreparedPolygon, ...] = ()
    path_positions: tuple[_Position, ...] = ()
    path_vectors: tuple[_Vector, ...] = ()


def closest_point_on_polygon(
    target_lat: float,
    target_lon: float,
    polygon: object,
) -> tuple[float, float, float, bool] | None:
    """Geef ``(afstand_km, lat, lon, binnen)`` voor een footprint.

    Een punt op een buiten- of binnenring behoort tot de geometrische grens
    en levert dus afstand nul en ``binnen=True``. Een punt in een hole ligt
    buiten de footprint en krijgt de afstand tot de dichtstbijzijnde holerand.

    De historische punt- en lijnsemantiek blijft uitsluitend voor de vlakke
    legacy invoervorm bestaan. Een GeoJSON/nested Polygon moet geldige ringen
    met minstens drie verschillende posities bevatten.
    """
    target = _normalise_position((target_lat, target_lon), lon_first=False)
    if target is None:
        return None
    prepared = _prepare_geometry(polygon)
    if prepared is None:
        return None

    target_vector = _to_vector(*target)
    if prepared.path_vectors:
        if len(prepared.path_vectors) == 1:
            distance = (
                _central_angle(target_vector, prepared.path_vectors[0])
                * EARTH_RADIUS_KM
            )
            position = prepared.path_positions[0]
            return distance, position[0], position[1], False
        nearest = _closest_on_arc(
            target_vector,
            prepared.path_vectors[0],
            prepared.path_vectors[1],
        )
        if nearest is None:
            return None
        angular_distance, vector = nearest
        latitude, longitude = _position_for_vector(
            vector,
            prepared.path_positions,
            prepared.path_vectors,
        )
        return (
            angular_distance * EARTH_RADIUS_KM,
            latitude,
            longitude,
            False,
        )

    minimum_distance = math.inf
    closest_vector: _Vector | None = None
    for part in prepared.polygons:
        part_minimum = math.inf
        part_closest: _Vector | None = None
        on_boundary = False
        for ring in (part.outer, *part.holes):
            nearest = _closest_on_ring(target_vector, ring.vectors)
            if nearest is None:
                return None
            angular_distance, vector = nearest
            distance_km = angular_distance * EARTH_RADIUS_KM
            if distance_km < part_minimum:
                part_minimum = distance_km
                part_closest = vector
            if distance_km <= _BOUNDARY_EPSILON_KM:
                on_boundary = True

        if on_boundary or _point_in_polygon(target_vector, part):
            return 0.0, target[0], target[1], True
        if part_minimum < minimum_distance:
            minimum_distance = part_minimum
            closest_vector = part_closest

    if closest_vector is None or not math.isfinite(minimum_distance):
        return None
    latitude, longitude = _from_prepared_geometry(
        closest_vector,
        prepared,
    )
    return minimum_distance, latitude, longitude, False


def point_to_polygon_distance_km(
    target_lat: float,
    target_lon: float,
    polygon: object,
) -> float | None:
    """Geef de minimale bolafstand tot de volledige footprint in kilometer."""
    result = closest_point_on_polygon(target_lat, target_lon, polygon)
    return result[0] if result is not None else None


def _prepare_geometry(raw_geometry: object) -> _PreparedGeometry | None:
    parsed = _parse_geometry(raw_geometry)
    if parsed is None:
        return None
    geometry_type, value = parsed

    if geometry_type == "path":
        positions = _normalise_path(value)
        if not positions or len(positions) > 2:
            return None
        vectors = tuple(_to_vector(*position) for position in positions)
        if (
            len(vectors) == 2
            and _central_angle(vectors[0], vectors[1])
            >= math.pi - _ANGULAR_EPSILON
        ):
            return None
        return _PreparedGeometry(
            path_positions=positions,
            path_vectors=vectors,
        )

    if len(value) > _MAX_POLYGONS:
        return None
    polygons: list[_PreparedPolygon] = []
    total_points = 0
    for raw_polygon in value:
        if not raw_polygon or len(raw_polygon) > _MAX_RINGS_PER_POLYGON:
            return None
        rings: list[tuple[_Position, ...]] = []
        for raw_ring in raw_polygon:
            ring = _normalise_ring(raw_ring)
            if ring is None:
                return None
            total_points += len(ring)
            if total_points > _MAX_GEOMETRY_ITEMS:
                return None
            rings.append(ring)
        prepared = _prepare_polygon(rings)
        if prepared is None:
            return None
        polygons.append(prepared)
    return (
        _PreparedGeometry(polygons=tuple(polygons))
        if polygons
        else None
    )


def _parse_geometry(
    raw_geometry: object,
) -> tuple[str, Any] | None:
    """Parseer vorm én positievolgorde zonder ongeldige data te herstellen."""
    lon_first = False
    expected_type: str | None = None
    raw_coordinates = raw_geometry
    if isinstance(raw_geometry, Mapping):
        expected_type = raw_geometry.get("type")
        if expected_type not in ("Polygon", "MultiPolygon"):
            return None
        if "coordinates" not in raw_geometry:
            return None
        raw_coordinates = raw_geometry["coordinates"]
        lon_first = True

    tree = _materialise_tree(raw_coordinates)
    if tree is None:
        return None

    if expected_type == "Polygon":
        if not _is_polygon_shape(tree):
            return None
        polygon = _convert_polygon(tree, lon_first=lon_first)
        return ("polygons", (polygon,)) if polygon is not None else None
    if expected_type == "MultiPolygon":
        if not _is_multipolygon_shape(tree):
            return None
        polygons = tuple(
            _convert_polygon(raw_polygon, lon_first=lon_first)
            for raw_polygon in tree
        )
        return (
            ("polygons", polygons)
            if all(polygon is not None for polygon in polygons)
            else None
        )

    if _is_ring_shape(tree):
        positions = _convert_ring(tree, lon_first=False)
        if positions is None:
            return None
        if len(positions) <= 2:
            return "path", positions
        return "polygons", ((positions,),)
    if _is_polygon_shape(tree):
        polygon = _convert_polygon(tree, lon_first=False)
        return ("polygons", (polygon,)) if polygon is not None else None
    if _is_multipolygon_shape(tree):
        polygons = tuple(
            _convert_polygon(raw_polygon, lon_first=False)
            for raw_polygon in tree
        )
        return (
            ("polygons", polygons)
            if all(polygon is not None for polygon in polygons)
            else None
        )
    return None


def _materialise_tree(value: object) -> tuple[Any, ...] | None:
    """Maak maximaal vier geografische nestingsniveaus materialiseerbaar."""
    budget = [_MAX_GEOMETRY_ITEMS]

    def visit(item: object, depth: int) -> Any:
        if isinstance(item, Mapping):
            raise ValueError
        if isinstance(item, (str, bytes, bytearray)):
            return item
        try:
            iterator = iter(item)  # type: ignore[arg-type]
        except TypeError:
            return item
        if depth >= 4:
            raise ValueError
        result = []
        for child in iterator:
            budget[0] -= 1
            if budget[0] < 0:
                raise ValueError
            result.append(visit(child, depth + 1))
        return tuple(result)

    try:
        tree = visit(value, 0)
    except (TypeError, ValueError):
        return None
    return tree if isinstance(tree, tuple) else None


def _is_position_shape(value: object) -> bool:
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and not isinstance(value[0], (tuple, Mapping))
        and not isinstance(value[1], (tuple, Mapping))
    )


def _is_ring_shape(value: object) -> bool:
    return (
        isinstance(value, tuple)
        and bool(value)
        and all(_is_position_shape(position) for position in value)
    )


def _is_polygon_shape(value: object) -> bool:
    return (
        isinstance(value, tuple)
        and bool(value)
        and all(_is_ring_shape(ring) for ring in value)
    )


def _is_multipolygon_shape(value: object) -> bool:
    return (
        isinstance(value, tuple)
        and bool(value)
        and all(_is_polygon_shape(polygon) for polygon in value)
    )


def _convert_ring(
    raw_ring: tuple[Any, ...],
    *,
    lon_first: bool,
) -> tuple[_Position, ...] | None:
    positions = tuple(
        _normalise_position(raw_position, lon_first=lon_first)
        for raw_position in raw_ring
    )
    return (
        tuple(position for position in positions if position is not None)
        if all(position is not None for position in positions)
        else None
    )


def _convert_polygon(
    raw_polygon: tuple[Any, ...],
    *,
    lon_first: bool,
) -> tuple[tuple[_Position, ...], ...] | None:
    rings = tuple(
        _convert_ring(raw_ring, lon_first=lon_first)
        for raw_ring in raw_polygon
    )
    return (
        tuple(ring for ring in rings if ring is not None)
        if all(ring is not None for ring in rings)
        else None
    )


def _normalise_position(
    raw_position: object,
    *,
    lon_first: bool,
) -> _Position | None:
    try:
        first, second = raw_position  # type: ignore[misc]
    except (TypeError, ValueError):
        return None
    if isinstance(first, bool) or isinstance(second, bool):
        return None
    try:
        latitude = float(second if lon_first else first)
        longitude = float(first if lon_first else second)
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        not math.isfinite(latitude)
        or not math.isfinite(longitude)
        or not -90.0 <= latitude <= 90.0
        or not -180.0 <= longitude <= 180.0
    ):
        return None
    # Alleen de dubbele antimeridiaanrepresentatie canoniseren. Een modulo
    # op ieder geldig getal zou anders de exacte bronfloat onnodig wijzigen.
    if longitude == 180.0:
        longitude = -180.0
    return latitude, longitude


def _normalise_path(
    raw_positions: Iterable[_Position],
) -> tuple[_Position, ...]:
    positions: list[_Position] = []
    for position in raw_positions:
        if not positions or position != positions[-1]:
            positions.append(position)
    return tuple(positions)


def _normalise_ring(
    raw_positions: Iterable[_Position],
) -> tuple[_Position, ...] | None:
    positions = list(_normalise_path(raw_positions))
    if len(positions) > 1 and positions[0] == positions[-1]:
        positions.pop()
    if (
        len(positions) < 3
        or len(positions) > _MAX_RING_POINTS
        or len(set(positions)) != len(positions)
    ):
        return None
    return tuple(positions)


def _prepare_polygon(
    rings: list[tuple[_Position, ...]],
) -> _PreparedPolygon | None:
    outer_vectors = tuple(_to_vector(*position) for position in rings[0])
    reference = _normalise_vector(
        (
            sum(vector[0] for vector in outer_vectors),
            sum(vector[1] for vector in outer_vectors),
            sum(vector[2] for vector in outer_vectors),
        )
    )
    if reference is None:
        return None
    east, north = _tangent_basis(reference)

    prepared_rings: list[_PreparedRing] = []
    for positions in rings:
        vectors = tuple(_to_vector(*position) for position in positions)
        projected: list[_Point2D] = []
        for vector in vectors:
            denominator = _dot(reference, vector)
            if denominator <= _HEMISPHERE_EPSILON:
                return None
            projected.append(
                (
                    _dot(east, vector) / denominator,
                    _dot(north, vector) / denominator,
                )
            )
        if any(
            _central_angle(vectors[index], vectors[(index + 1) % len(vectors)])
            >= math.pi - _ANGULAR_EPSILON
            for index in range(len(vectors))
        ):
            return None
        projected_tuple = tuple(projected)
        if not _is_simple_ring(projected_tuple):
            return None
        prepared_rings.append(
            _PreparedRing(
                positions=positions,
                vectors=vectors,
                projected=projected_tuple,
            )
        )

    outer = prepared_rings[0]
    holes = prepared_rings[1:]
    for hole in holes:
        if (
            not _point_in_ring_strict(hole.projected[0], outer.projected)
            or _rings_intersect(hole.projected, outer.projected)
        ):
            return None
    for first_index, first in enumerate(holes):
        for second in holes[first_index + 1 :]:
            if (
                _rings_intersect(first.projected, second.projected)
                or _point_in_ring_strict(
                    first.projected[0],
                    second.projected,
                )
                or _point_in_ring_strict(
                    second.projected[0],
                    first.projected,
                )
            ):
                return None

    return _PreparedPolygon(
        reference=reference,
        east=east,
        north=north,
        outer=outer,
        holes=tuple(holes),
    )


def _point_in_polygon(target: _Vector, polygon: _PreparedPolygon) -> bool:
    denominator = _dot(polygon.reference, target)
    if denominator <= _HEMISPHERE_EPSILON:
        return False
    projected = (
        _dot(polygon.east, target) / denominator,
        _dot(polygon.north, target) / denominator,
    )
    return (
        _point_in_ring_strict(projected, polygon.outer.projected)
        and not any(
            _point_in_ring_strict(projected, hole.projected)
            for hole in polygon.holes
        )
    )


def _closest_on_ring(
    target: _Vector,
    ring: tuple[_Vector, ...],
) -> tuple[float, _Vector] | None:
    best: tuple[float, _Vector] | None = None
    for index, start in enumerate(ring):
        candidate = _closest_on_arc(
            target,
            start,
            ring[(index + 1) % len(ring)],
        )
        if candidate is None:
            return None
        if best is None or candidate[0] < best[0]:
            best = candidate
    return best


def _closest_on_arc(
    target: _Vector,
    start: _Vector,
    end: _Vector,
) -> tuple[float, _Vector] | None:
    arc_length = _central_angle(start, end)
    if arc_length >= math.pi - _ANGULAR_EPSILON:
        return None
    candidates: list[_Vector] = [start, end]
    normal = _normalise_vector(_cross(start, end))
    if normal is not None:
        projection = (
            target[0] - _dot(target, normal) * normal[0],
            target[1] - _dot(target, normal) * normal[1],
            target[2] - _dot(target, normal) * normal[2],
        )
        projected = _normalise_vector(projection)
        if projected is not None:
            for candidate in (projected, _scale(projected, -1.0)):
                covered = (
                    _central_angle(start, candidate)
                    + _central_angle(candidate, end)
                )
                if abs(covered - arc_length) <= 1e-9:
                    candidates.append(candidate)
    closest = min(
        candidates,
        key=lambda candidate: _central_angle(target, candidate),
    )
    return _central_angle(target, closest), closest


def _position_for_vector(
    vector: _Vector,
    positions: tuple[_Position, ...],
    vectors: tuple[_Vector, ...],
) -> _Position:
    """Behoud de exacte bronpositie wanneer een boogeindpunt het dichtst is."""
    for position, known_vector in zip(positions, vectors, strict=True):
        if vector is known_vector:
            return position
    return _from_vector(vector)


def _from_prepared_geometry(
    vector: _Vector,
    geometry: _PreparedGeometry,
) -> _Position:
    for polygon in geometry.polygons:
        for ring in (polygon.outer, *polygon.holes):
            for position, known_vector in zip(
                ring.positions,
                ring.vectors,
                strict=True,
            ):
                if vector is known_vector:
                    return position
    return _from_vector(vector)


def _is_simple_ring(points: tuple[_Point2D, ...]) -> bool:
    scale = max(
        1.0,
        max(
            (
                abs(coordinate)
                for point in points
                for coordinate in point
            ),
            default=0.0,
        ),
    )
    area_twice = sum(
        points[index][0] * points[(index + 1) % len(points)][1]
        - points[(index + 1) % len(points)][0] * points[index][1]
        for index in range(len(points))
    )
    if abs(area_twice) <= 1e-14 * scale * scale:
        return False
    segment_count = len(points)
    for first in range(segment_count):
        first_end = (first + 1) % segment_count
        for second in range(first + 1, segment_count):
            second_end = (second + 1) % segment_count
            if (
                first == second
                or first_end == second
                or second_end == first
            ):
                continue
            if _segments_intersect(
                points[first],
                points[first_end],
                points[second],
                points[second_end],
            ):
                return False
    return True


def _rings_intersect(
    first: tuple[_Point2D, ...],
    second: tuple[_Point2D, ...],
) -> bool:
    return any(
        _segments_intersect(
            first[first_index],
            first[(first_index + 1) % len(first)],
            second[second_index],
            second[(second_index + 1) % len(second)],
        )
        for first_index in range(len(first))
        for second_index in range(len(second))
    )


def _segments_intersect(
    first_start: _Point2D,
    first_end: _Point2D,
    second_start: _Point2D,
    second_end: _Point2D,
) -> bool:
    scale = max(
        1.0,
        *(abs(value) for point in (
            first_start,
            first_end,
            second_start,
            second_end,
        ) for value in point),
    )
    epsilon = 1e-12 * scale * scale
    orientations = (
        _orientation(first_start, first_end, second_start),
        _orientation(first_start, first_end, second_end),
        _orientation(second_start, second_end, first_start),
        _orientation(second_start, second_end, first_end),
    )
    if (
        orientations[0] * orientations[1] < -epsilon * epsilon
        and orientations[2] * orientations[3] < -epsilon * epsilon
    ):
        return True
    return (
        abs(orientations[0]) <= epsilon
        and _point_on_segment(second_start, first_start, first_end, epsilon)
        or abs(orientations[1]) <= epsilon
        and _point_on_segment(second_end, first_start, first_end, epsilon)
        or abs(orientations[2]) <= epsilon
        and _point_on_segment(first_start, second_start, second_end, epsilon)
        or abs(orientations[3]) <= epsilon
        and _point_on_segment(first_end, second_start, second_end, epsilon)
    )


def _point_in_ring_strict(
    point: _Point2D,
    ring: tuple[_Point2D, ...],
) -> bool:
    x, y = point
    inside = False
    previous_x, previous_y = ring[-1]
    for current_x, current_y in ring:
        if _point_on_segment(
            point,
            (previous_x, previous_y),
            (current_x, current_y),
            1e-12,
        ):
            return False
        if (current_y > y) != (previous_y > y):
            crossing_x = current_x + (
                (previous_x - current_x)
                * (y - current_y)
                / (previous_y - current_y)
            )
            if crossing_x > x:
                inside = not inside
        previous_x, previous_y = current_x, current_y
    return inside


def _point_on_segment(
    point: _Point2D,
    start: _Point2D,
    end: _Point2D,
    epsilon: float,
) -> bool:
    return (
        abs(_orientation(start, end, point)) <= epsilon
        and min(start[0], end[0]) - epsilon
        <= point[0]
        <= max(start[0], end[0]) + epsilon
        and min(start[1], end[1]) - epsilon
        <= point[1]
        <= max(start[1], end[1]) + epsilon
    )


def _orientation(
    start: _Point2D,
    end: _Point2D,
    point: _Point2D,
) -> float:
    return (
        (end[0] - start[0]) * (point[1] - start[1])
        - (end[1] - start[1]) * (point[0] - start[0])
    )


def _to_vector(latitude: float, longitude: float) -> _Vector:
    latitude_radians = math.radians(latitude)
    longitude_radians = math.radians(longitude)
    cosine = math.cos(latitude_radians)
    return (
        cosine * math.cos(longitude_radians),
        cosine * math.sin(longitude_radians),
        math.sin(latitude_radians),
    )


def _from_vector(vector: _Vector) -> _Position:
    normalised = _normalise_vector(vector)
    if normalised is None:
        raise ValueError("nulvector heeft geen geografische positie")
    latitude = math.degrees(
        math.atan2(
            normalised[2],
            math.hypot(normalised[0], normalised[1]),
        )
    )
    longitude = math.degrees(math.atan2(normalised[1], normalised[0]))
    longitude = (longitude + 180.0) % 360.0 - 180.0
    return latitude, longitude


def _tangent_basis(reference: _Vector) -> tuple[_Vector, _Vector]:
    axis: _Vector = (
        (0.0, 0.0, 1.0)
        if abs(reference[2]) < 0.9
        else (1.0, 0.0, 0.0)
    )
    east = _normalise_vector(_cross(axis, reference))
    if east is None:
        raise ValueError("kan geen tangentieel assenstelsel maken")
    north = _cross(reference, east)
    return east, north


def _central_angle(first: _Vector, second: _Vector) -> float:
    return math.atan2(
        _norm(_cross(first, second)),
        max(-1.0, min(1.0, _dot(first, second))),
    )


def _normalise_vector(vector: _Vector) -> _Vector | None:
    length = _norm(vector)
    if not math.isfinite(length) or length <= _ANGULAR_EPSILON:
        return None
    return (
        vector[0] / length,
        vector[1] / length,
        vector[2] / length,
    )


def _dot(first: _Vector, second: _Vector) -> float:
    return (
        first[0] * second[0]
        + first[1] * second[1]
        + first[2] * second[2]
    )


def _cross(first: _Vector, second: _Vector) -> _Vector:
    return (
        first[1] * second[2] - first[2] * second[1],
        first[2] * second[0] - first[0] * second[2],
        first[0] * second[1] - first[1] * second[0],
    )


def _scale(vector: _Vector, factor: float) -> _Vector:
    return (
        vector[0] * factor,
        vector[1] * factor,
        vector[2] * factor,
    )


def _norm(vector: _Vector) -> float:
    return math.sqrt(_dot(vector, vector))
