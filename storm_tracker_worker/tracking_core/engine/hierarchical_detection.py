"""Deterministic rain-shield and embedded-core detection for shadow tracking.

Input is an already normalized scalar raster on an :class:`AnalysisGrid`.
Provider decoding, reprojection and physical-unit conversion are deliberately
outside this module.  Every meteorological threshold, minimum area, centroid
method and measurement uncertainty is supplied by the caller.
"""
from __future__ import annotations

import hashlib
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable
from .analysis_grid import AnalysisGrid
from .cell_track import CellDetection


HIERARCHICAL_DETECTOR_VERSION = "stv3-hierarchical-cell-detector-v1"
DEADLINE_CHECK_INTERVAL = 1_024
Pixel = tuple[int, int]


class HierarchicalDetectionDeadlineError(TimeoutError):
    """The detector reached the caller-owned absolute monotonic deadline."""


class HierarchicalDetectionCapacityError(RuntimeError):
    """The complete lossless result would exceed an explicit object budget."""


@dataclass(slots=True)
class _CooperativeDetectionBudget:
    deadline_at: float | None
    monotonic: Callable[[], float]
    max_detection_objects: int | None
    work_since_check: int = 0
    detection_objects: int = 0

    def check(self, *, work: int = 0, force: bool = False) -> None:
        self.work_since_check += work
        if self.deadline_at is None:
            return
        if not force and self.work_since_check < DEADLINE_CHECK_INTERVAL:
            return
        self.work_since_check = 0
        now = self.monotonic()
        if (
            isinstance(now, bool)
            or not isinstance(now, (int, float))
            or not math.isfinite(float(now))
        ):
            raise ValueError("monotonic moet een eindig tijdstip teruggeven")
        if float(now) >= self.deadline_at:
            raise HierarchicalDetectionDeadlineError(
                "hierarchische detectiedeadline verstreken"
            )

    def admit_component(self) -> None:
        if (
            self.max_detection_objects is not None
            and self.detection_objects >= self.max_detection_objects
        ):
            raise HierarchicalDetectionCapacityError(
                "hiërarchische detectie overschrijdt het objectbudget"
            )
        self.detection_objects += 1


@dataclass(frozen=True, slots=True)
class NormalizedRasterFrame:
    """One physical scalar field after provider-specific normalization."""

    frame_id: str
    valid_at: float
    source: str
    quantity: str
    unit: str
    grid: AnalysisGrid
    values: tuple[tuple[float | None, ...], ...]

    def __post_init__(self) -> None:
        for label, value in (
            ("frame_id", self.frame_id),
            ("source", self.source),
            ("quantity", self.quantity),
            ("unit", self.unit),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"NormalizedRasterFrame vereist {label}")
        if not math.isfinite(self.valid_at):
            raise ValueError("NormalizedRasterFrame valid_at moet eindig zijn")
        if not isinstance(self.grid, AnalysisGrid):
            raise TypeError("NormalizedRasterFrame vereist een AnalysisGrid")
        if not isinstance(self.values, tuple) or any(
            not isinstance(row, tuple) for row in self.values
        ):
            raise TypeError("rasterwaarden moeten immutable tuples zijn")
        self.grid.validate_raster_shape(self.values)
        for row in self.values:
            for value in row:
                if value is not None and not math.isfinite(value):
                    raise ValueError(
                        "genormaliseerde rasterwaarden moeten eindig of None zijn"
                    )


@dataclass(frozen=True, slots=True)
class HierarchicalDetectorConfig:
    """All detector choices that can affect object identity.

    The minimum areas are classification thresholds only.  They must never
    remove a connected precipitation component from the returned frame.
    """

    shield_threshold: float
    core_threshold: float
    minimum_shield_area_m2: float
    minimum_core_area_m2: float
    shield_position_sigma_m: float
    core_position_sigma_m: float
    connectivity: int
    centroid_method: str

    def __post_init__(self) -> None:
        for label, value in (
            ("shield_threshold", self.shield_threshold),
            ("core_threshold", self.core_threshold),
            ("minimum_shield_area_m2", self.minimum_shield_area_m2),
            ("minimum_core_area_m2", self.minimum_core_area_m2),
            ("shield_position_sigma_m", self.shield_position_sigma_m),
            ("core_position_sigma_m", self.core_position_sigma_m),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{label} moet eindig zijn")
        if self.core_threshold <= self.shield_threshold:
            raise ValueError("core_threshold moet hoger zijn dan shield_threshold")
        if self.minimum_shield_area_m2 < 0.0 or self.minimum_core_area_m2 < 0.0:
            raise ValueError("minimumoppervlakken moeten niet-negatief zijn")
        if (
            self.shield_position_sigma_m <= 0.0
            or self.core_position_sigma_m <= 0.0
        ):
            raise ValueError("positie-onzekerheden moeten positief zijn")
        if self.connectivity not in (4, 8):
            raise ValueError("connectivity moet expliciet 4 of 8 zijn")
        if self.centroid_method not in ("geometric", "value_weighted"):
            raise ValueError(
                "centroid_method moet geometric of value_weighted zijn"
            )


@dataclass(frozen=True, slots=True)
class HierarchicalCellDetection:
    """One auditable object plus the exact pixels that formed it."""

    detection: CellDetection
    object_kind: str
    parent_detection_id: str | None
    footprint_pixels: tuple[Pixel, ...]
    area_m2: float
    geometric_centroid_x_m: float
    geometric_centroid_y_m: float
    value_weighted_centroid_x_m: float | None
    value_weighted_centroid_y_m: float | None
    mean_value: float
    maximum_value: float
    threshold: float
    centroid_method: str
    minimum_area_m2: float = 0.0
    low_confidence_fragment: bool = False

    def __post_init__(self) -> None:
        if self.object_kind not in ("rain_shield", "embedded_core"):
            raise ValueError("onbekend hiërarchisch objecttype")
        if not isinstance(self.footprint_pixels, tuple) or not self.footprint_pixels:
            raise ValueError("celdetectie vereist immutable footprintpixels")
        if len(self.footprint_pixels) != len(set(self.footprint_pixels)) or any(
            isinstance(row, bool)
            or not isinstance(row, int)
            or row < 0
            or isinstance(column, bool)
            or not isinstance(column, int)
            or column < 0
            for row, column in self.footprint_pixels
        ):
            raise ValueError("footprintpixels moeten unieke geldige indices zijn")
        if not math.isfinite(self.area_m2) or self.area_m2 <= 0.0:
            raise ValueError("detectieoppervlak moet positief en eindig zijn")
        scalars = (
            self.geometric_centroid_x_m,
            self.geometric_centroid_y_m,
            self.mean_value,
            self.maximum_value,
            self.threshold,
        )
        if not all(math.isfinite(value) for value in scalars):
            raise ValueError("detectiekenmerken moeten eindig zijn")
        if self.mean_value > self.maximum_value:
            raise ValueError("mean_value mag maximum_value niet overschrijden")
        if (
            not math.isfinite(self.minimum_area_m2)
            or self.minimum_area_m2 < 0.0
        ):
            raise ValueError("minimum_area_m2 moet niet-negatief en eindig zijn")
        if self.low_confidence_fragment != (
            self.area_m2 < self.minimum_area_m2
        ):
            raise ValueError(
                "low_confidence_fragment moet exact de minimumarea-audit volgen"
            )
        weighted = (
            self.value_weighted_centroid_x_m,
            self.value_weighted_centroid_y_m,
        )
        if (weighted[0] is None) != (weighted[1] is None) or any(
            value is not None and not math.isfinite(value) for value in weighted
        ):
            raise ValueError("value-weighted centroid moet volledig of None zijn")
        expected_x, expected_y = (
            weighted
            if self.centroid_method == "value_weighted"
            else (
                self.geometric_centroid_x_m,
                self.geometric_centroid_y_m,
            )
        )
        if expected_x is None or expected_y is None:
            raise ValueError("geselecteerde centroidmethode is onbepaald")
        if not math.isclose(
            self.detection.x_m,
            expected_x,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ) or not math.isclose(
            self.detection.y_m,
            expected_y,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ):
            raise ValueError("CellDetection-positie verschilt van gekozen centroid")
        if self.object_kind == "embedded_core" and not self.parent_detection_id:
            raise ValueError("embedded_core vereist een parent rain_shield")
        if self.object_kind == "rain_shield" and self.parent_detection_id is not None:
            raise ValueError("rain_shield mag geen parent detectie hebben")


@dataclass(frozen=True, slots=True)
class HierarchicalDetectionFrame:
    frame_id: str
    valid_at: float
    analysis_grid_id: str
    quantity: str
    unit: str
    detections: tuple[HierarchicalCellDetection, ...]
    detector_version: str = HIERARCHICAL_DETECTOR_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("frame_id", self.frame_id),
            ("analysis_grid_id", self.analysis_grid_id),
            ("quantity", self.quantity),
            ("unit", self.unit),
            ("detector_version", self.detector_version),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"HierarchicalDetectionFrame vereist {label}")
        if not math.isfinite(self.valid_at):
            raise ValueError("detectieframe valid_at moet eindig zijn")
        if not isinstance(self.detections, tuple):
            raise TypeError("detecties moeten een immutable tuple zijn")
        identifiers = tuple(
            item.detection.detection_id for item in self.detections
        )
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("detectieframe bevat dubbele detection-ID's")
        if any(
            item.detection.valid_at != self.valid_at
            or item.detection.analysis_grid_id != self.analysis_grid_id
            for item in self.detections
        ):
            raise ValueError("detecties moeten tijd en grid van hun frame delen")

        shields = {
            item.detection.detection_id: item
            for item in self.detections
            if item.object_kind == "rain_shield"
        }
        occupied_shield_pixels: set[Pixel] = set()
        occupied_core_pixels: set[Pixel] = set()
        for shield in shields.values():
            footprint = set(shield.footprint_pixels)
            if occupied_shield_pixels & footprint:
                raise ValueError("rain-shieldfootprints mogen niet overlappen")
            occupied_shield_pixels.update(footprint)
        for core in (
            item for item in self.detections
            if item.object_kind == "embedded_core"
        ):
            parent = shields.get(core.parent_detection_id or "")
            if parent is None:
                raise ValueError("embedded core verwijst niet naar een frame-shield")
            footprint = set(core.footprint_pixels)
            if not footprint.issubset(parent.footprint_pixels):
                raise ValueError("embedded core ligt niet volledig binnen zijn shield")
            if occupied_core_pixels & footprint:
                raise ValueError("embedded-corefootprints mogen niet overlappen")
            occupied_core_pixels.update(footprint)

    @property
    def rain_shields(self) -> tuple[HierarchicalCellDetection, ...]:
        return tuple(
            item for item in self.detections if item.object_kind == "rain_shield"
        )

    @property
    def embedded_cores(self) -> tuple[HierarchicalCellDetection, ...]:
        return tuple(
            item for item in self.detections if item.object_kind == "embedded_core"
        )

    @property
    def low_confidence_fragments(self) -> tuple[HierarchicalCellDetection, ...]:
        """Return every retained object below the configured minimum area."""

        return tuple(
            item for item in self.detections if item.low_confidence_fragment
        )


def detect_hierarchical_cells(
    frame: NormalizedRasterFrame,
    config: HierarchicalDetectorConfig,
    *,
    deadline_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    max_detection_objects: int | None = None,
) -> HierarchicalDetectionFrame:
    """Detect all connected shields and cores within explicit resource bounds.

    Minimum-area settings remain classification-only.  A deadline or capacity
    failure raises before a frame is returned; it can never produce a partial
    frame or be interpreted as an absence of precipitation.
    """

    if not isinstance(frame, NormalizedRasterFrame):
        raise TypeError("detectie vereist een NormalizedRasterFrame")
    if not isinstance(config, HierarchicalDetectorConfig):
        raise TypeError("detectie vereist een HierarchicalDetectorConfig")
    if not callable(monotonic):
        raise TypeError("monotonic moet aanroepbaar zijn")
    if deadline_at is not None and (
        isinstance(deadline_at, bool)
        or not isinstance(deadline_at, (int, float))
        or not math.isfinite(float(deadline_at))
    ):
        raise ValueError("deadline_at moet een eindig absoluut tijdstip zijn")
    if max_detection_objects is not None and (
        isinstance(max_detection_objects, bool)
        or not isinstance(max_detection_objects, int)
        or max_detection_objects < 1
    ):
        raise ValueError("max_detection_objects moet positief geheel zijn")

    budget = _CooperativeDetectionBudget(
        deadline_at=(None if deadline_at is None else float(deadline_at)),
        monotonic=monotonic,
        max_detection_objects=max_detection_objects,
    )
    budget.check(force=True)

    shield_components = _connected_components(
        frame.values,
        threshold=config.shield_threshold,
        connectivity=config.connectivity,
        budget=budget,
    )
    results: list[HierarchicalCellDetection] = []
    for shield_pixels in shield_components:
        shield = _build_detection(
            frame,
            pixels=shield_pixels,
            object_kind="rain_shield",
            parent_detection_id=None,
            threshold=config.shield_threshold,
            minimum_area_m2=config.minimum_shield_area_m2,
            position_sigma_m=config.shield_position_sigma_m,
            centroid_method=config.centroid_method,
            budget=budget,
        )
        results.append(shield)

        shield_set = frozenset(shield_pixels)
        core_components = _connected_components(
            frame.values,
            threshold=config.core_threshold,
            connectivity=config.connectivity,
            allowed_pixels=shield_set,
            budget=budget,
        )
        for core_pixels in core_components:
            results.append(
                _build_detection(
                    frame,
                    pixels=core_pixels,
                    object_kind="embedded_core",
                    parent_detection_id=shield.detection.detection_id,
                    threshold=config.core_threshold,
                    minimum_area_m2=config.minimum_core_area_m2,
                    position_sigma_m=config.core_position_sigma_m,
                    centroid_method=config.centroid_method,
                    budget=budget,
                )
            )

    budget.check(force=True)
    result = HierarchicalDetectionFrame(
        frame_id=frame.frame_id,
        valid_at=frame.valid_at,
        analysis_grid_id=frame.grid.grid_id,
        quantity=frame.quantity,
        unit=frame.unit,
        detections=tuple(results),
    )
    budget.check(force=True)
    return result


def _connected_components(
    values: tuple[tuple[float | None, ...], ...],
    *,
    threshold: float,
    connectivity: int,
    budget: _CooperativeDetectionBudget,
    allowed_pixels: frozenset[Pixel] | None = None,
) -> tuple[tuple[Pixel, ...], ...]:
    height = len(values)
    width = len(values[0])
    active: set[Pixel] = set()
    candidates = (
        ((row, column) for row in range(height) for column in range(width))
        if allowed_pixels is None
        else iter(sorted(allowed_pixels))
    )
    for row, column in candidates:
        budget.check(work=1)
        value = values[row][column]
        if value is not None and value >= threshold:
            active.add((row, column))
    offsets = (
        ((-1, 0), (0, -1), (0, 1), (1, 0))
        if connectivity == 4
        else (
            (-1, -1),
            (-1, 0),
            (-1, 1),
            (0, -1),
            (0, 1),
            (1, -1),
            (1, 0),
            (1, 1),
        )
    )
    components: list[tuple[Pixel, ...]] = []
    while active:
        budget.check(force=True)
        seed = min(active)
        budget.check(force=True)
        budget.admit_component()
        active.remove(seed)
        queue: deque[Pixel] = deque((seed,))
        component: list[Pixel] = []
        while queue:
            budget.check(work=1)
            pixel = queue.popleft()
            component.append(pixel)
            row, column = pixel
            for row_offset, column_offset in offsets:
                budget.check(work=1)
                neighbour = (row + row_offset, column + column_offset)
                if neighbour in active:
                    active.remove(neighbour)
                    queue.append(neighbour)
        budget.check(force=True)
        components.append(tuple(sorted(component)))
        budget.check(force=True)
    return tuple(components)


def _build_detection(
    frame: NormalizedRasterFrame,
    *,
    pixels: tuple[Pixel, ...],
    object_kind: str,
    parent_detection_id: str | None,
    threshold: float,
    minimum_area_m2: float,
    position_sigma_m: float,
    centroid_method: str,
    budget: _CooperativeDetectionBudget,
) -> HierarchicalCellDetection:
    geometric_x_sum = 0.0
    geometric_y_sum = 0.0
    weighted_x_sum = 0.0
    weighted_y_sum = 0.0
    total_value = 0.0
    all_nonnegative = True
    maximum_value = -math.inf
    for row, column in pixels:
        budget.check(work=1)
        centre_x, centre_y = frame.grid.pixel_center(row, column)
        value = float(frame.values[row][column])
        geometric_x_sum += centre_x
        geometric_y_sum += centre_y
        weighted_x_sum += centre_x * value
        weighted_y_sum += centre_y * value
        total_value += value
        all_nonnegative = all_nonnegative and value >= 0.0
        maximum_value = max(maximum_value, value)
    geometric_x = geometric_x_sum / len(pixels)
    geometric_y = geometric_y_sum / len(pixels)
    if total_value > 0.0 and all_nonnegative:
        weighted_x = weighted_x_sum / total_value
        weighted_y = weighted_y_sum / total_value
    else:
        weighted_x = None
        weighted_y = None
    if centroid_method == "value_weighted":
        if weighted_x is None or weighted_y is None:
            raise ValueError(
                "value_weighted centroid is onbepaald bij niet-positieve som"
            )
        selected_x, selected_y = weighted_x, weighted_y
    else:
        selected_x, selected_y = geometric_x, geometric_y

    digest_builder = hashlib.sha256()
    digest_builder.update(
        f"{frame.frame_id}\0{object_kind}\0{parent_detection_id or ''}\0".encode(
            "utf-8"
        )
    )
    for index, (row, column) in enumerate(pixels):
        budget.check(work=1)
        if index:
            digest_builder.update(b";")
        digest_builder.update(f"{row},{column}".encode("utf-8"))
    digest = digest_builder.hexdigest()[:20]
    detection_id = f"{frame.frame_id}:{object_kind}:{digest}"
    return HierarchicalCellDetection(
        detection=CellDetection(
            detection_id=detection_id,
            valid_at=frame.valid_at,
            analysis_grid_id=frame.grid.grid_id,
            x_m=selected_x,
            y_m=selected_y,
            position_sigma_m=position_sigma_m,
            source=frame.source,
        ),
        object_kind=object_kind,
        parent_detection_id=parent_detection_id,
        footprint_pixels=pixels,
        area_m2=len(pixels) * frame.grid.pixel_area_m2,
        geometric_centroid_x_m=geometric_x,
        geometric_centroid_y_m=geometric_y,
        value_weighted_centroid_x_m=weighted_x,
        value_weighted_centroid_y_m=weighted_y,
        mean_value=total_value / len(pixels),
        maximum_value=maximum_value,
        threshold=threshold,
        centroid_method=centroid_method,
        minimum_area_m2=minimum_area_m2,
        low_confidence_fragment=(
            len(pixels) * frame.grid.pixel_area_m2 < minimum_area_m2
        ),
    )
