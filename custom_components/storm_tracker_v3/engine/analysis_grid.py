"""Explicit metric analysis-grid contract for shadow cell tracking.

The grid is axis-aligned in a caller-selected projected CRS.  ``origin_x_m``
and ``origin_y_m`` describe the north-west *outer edge* of pixel ``(0, 0)``.
Columns increase eastward and rows increase southward.  Resolution describes
sampling support; it is deliberately never presented as measurement accuracy.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


ANALYSIS_GRID_CONTRACT_VERSION = "stv3-metric-analysis-grid-v1"


@dataclass(frozen=True, slots=True)
class AnalysisGrid:
    """Immutable, axis-aligned grid in metres.

    A projected CRS identifier is mandatory, but this class intentionally does
    not perform reprojection.  Providers must transform into the chosen CRS
    before constructing a normalized raster.
    """

    grid_id: str
    crs: str
    origin_x_m: float
    origin_y_m: float
    x_resolution_m: float
    y_resolution_m: float
    width: int
    height: int
    coordinate_unit: str = "m"
    axis_order: str = "x_east_y_north"
    row_order: str = "north_to_south"
    contract_version: str = ANALYSIS_GRID_CONTRACT_VERSION

    def __post_init__(self) -> None:
        for label, value in (("grid_id", self.grid_id), ("crs", self.crs)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"AnalysisGrid vereist een niet-lege {label}")
        for label, value in (
            ("origin_x_m", self.origin_x_m),
            ("origin_y_m", self.origin_y_m),
            ("x_resolution_m", self.x_resolution_m),
            ("y_resolution_m", self.y_resolution_m),
        ):
            if not math.isfinite(value):
                raise ValueError(f"AnalysisGrid {label} moet eindig zijn")
        if self.x_resolution_m <= 0.0 or self.y_resolution_m <= 0.0:
            raise ValueError("AnalysisGrid-resoluties moeten positief zijn")
        if (
            isinstance(self.width, bool)
            or not isinstance(self.width, int)
            or self.width <= 0
            or isinstance(self.height, bool)
            or not isinstance(self.height, int)
            or self.height <= 0
        ):
            raise ValueError("AnalysisGrid width en height moeten positief zijn")
        if self.coordinate_unit != "m":
            raise ValueError("AnalysisGrid accepteert uitsluitend metrische SI-data")
        if self.axis_order != "x_east_y_north":
            raise ValueError("AnalysisGrid vereist x-oost/y-noord-asvolgorde")
        if self.row_order != "north_to_south":
            raise ValueError("AnalysisGrid vereist noord-naar-zuid-rijvolgorde")

    @property
    def pixel_area_m2(self) -> float:
        return self.x_resolution_m * self.y_resolution_m

    @property
    def east_edge_m(self) -> float:
        return self.origin_x_m + self.width * self.x_resolution_m

    @property
    def south_edge_m(self) -> float:
        return self.origin_y_m - self.height * self.y_resolution_m

    @property
    def extent_edges_m(self) -> tuple[float, float, float, float]:
        """Return ``(west, south, east, north)`` outer pixel edges."""

        return (
            self.origin_x_m,
            self.south_edge_m,
            self.east_edge_m,
            self.origin_y_m,
        )

    def pixel_center(self, row: int, column: int) -> tuple[float, float]:
        """Return the exact metric centre of one in-bounds pixel."""

        self._validate_index(row, column)
        return (
            self.origin_x_m + (column + 0.5) * self.x_resolution_m,
            self.origin_y_m - (row + 0.5) * self.y_resolution_m,
        )

    def fractional_center_index(self, x_m: float, y_m: float) -> tuple[float, float]:
        """Return fractional ``(row, column)`` in the pixel-centre convention."""

        self._validate_xy(x_m, y_m)
        return (
            (self.origin_y_m - y_m) / self.y_resolution_m - 0.5,
            (x_m - self.origin_x_m) / self.x_resolution_m - 0.5,
        )

    def point_to_pixel(self, x_m: float, y_m: float) -> tuple[int, int]:
        """Map a point through half-open pixel edges, failing outside the grid.

        The west and north outer edges are included; east and south are
        excluded.  That convention makes adjacent grids/pixels unambiguous.
        """

        self._validate_xy(x_m, y_m)
        column = math.floor((x_m - self.origin_x_m) / self.x_resolution_m)
        row = math.floor((self.origin_y_m - y_m) / self.y_resolution_m)
        if row < 0 or row >= self.height or column < 0 or column >= self.width:
            raise ValueError("metrisch punt ligt buiten de AnalysisGrid")
        return row, column

    def round_trip_error_m(self, row: int, column: int) -> float:
        """Measure centre -> fractional-index roundtrip numerical error."""

        x_m, y_m = self.pixel_center(row, column)
        fractional_row, fractional_column = self.fractional_center_index(x_m, y_m)
        return math.hypot(
            (fractional_column - column) * self.x_resolution_m,
            (fractional_row - row) * self.y_resolution_m,
        )

    def assert_round_trip(
        self,
        *,
        tolerance_m: float = 1e-6,
    ) -> None:
        """Check corners and centre without inventing an accuracy threshold."""

        if not math.isfinite(tolerance_m) or tolerance_m < 0.0:
            raise ValueError("tolerance_m moet niet-negatief en eindig zijn")
        checks = {
            (0, 0),
            (0, self.width - 1),
            (self.height - 1, 0),
            (self.height - 1, self.width - 1),
            (self.height // 2, self.width // 2),
        }
        maximum = max(self.round_trip_error_m(*item) for item in checks)
        if maximum > tolerance_m:
            raise ValueError(
                f"AnalysisGrid-roundtripfout {maximum!r} m overschrijdt "
                f"caller-tolerantie {tolerance_m!r} m"
            )

    def assert_resolution_at_most(
        self,
        *,
        x_m: float,
        y_m: float,
    ) -> None:
        """Fail if sampling is coarser than caller-supplied limits."""

        if (
            not math.isfinite(x_m)
            or x_m <= 0.0
            or not math.isfinite(y_m)
            or y_m <= 0.0
        ):
            raise ValueError("resolutielimieten moeten positief en eindig zijn")
        if self.x_resolution_m > x_m or self.y_resolution_m > y_m:
            raise ValueError(
                "AnalysisGrid is grover dan de caller-supplied resolutielimiet"
            )

    def validate_raster_shape(self, rows: Sequence[Sequence[object]]) -> None:
        if len(rows) != self.height:
            raise ValueError(
                f"rasterhoogte {len(rows)} verschilt van gridhoogte {self.height}"
            )
        for row_number, row in enumerate(rows):
            if len(row) != self.width:
                raise ValueError(
                    f"rasterrij {row_number} heeft breedte {len(row)}; "
                    f"verwacht {self.width}"
                )

    def same_geometry(self, other: "AnalysisGrid") -> bool:
        """Compare every geometric field, not just a human-readable ID."""

        return isinstance(other, AnalysisGrid) and (
            self.grid_id,
            self.crs,
            self.origin_x_m,
            self.origin_y_m,
            self.x_resolution_m,
            self.y_resolution_m,
            self.width,
            self.height,
            self.coordinate_unit,
            self.axis_order,
            self.row_order,
        ) == (
            other.grid_id,
            other.crs,
            other.origin_x_m,
            other.origin_y_m,
            other.x_resolution_m,
            other.y_resolution_m,
            other.width,
            other.height,
            other.coordinate_unit,
            other.axis_order,
            other.row_order,
        )

    def assert_same_geometry(self, other: "AnalysisGrid") -> None:
        if not self.same_geometry(other):
            raise ValueError("AnalysisGrid-geometrie komt niet exact overeen")

    def _validate_index(self, row: int, column: int) -> None:
        if (
            isinstance(row, bool)
            or not isinstance(row, int)
            or isinstance(column, bool)
            or not isinstance(column, int)
            or row < 0
            or row >= self.height
            or column < 0
            or column >= self.width
        ):
            raise IndexError("pixelindex ligt buiten de AnalysisGrid")

    @staticmethod
    def _validate_xy(x_m: float, y_m: float) -> None:
        if not math.isfinite(x_m) or not math.isfinite(y_m):
            raise ValueError("metrische gridcoordinaten moeten eindig zijn")
