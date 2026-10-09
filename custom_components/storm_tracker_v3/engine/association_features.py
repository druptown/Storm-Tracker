"""Complete optical-flow-aware feature table for cell association.

Every previous/current pair is retained.  Features are measurements and
uncalibrated ranking terms only; this module neither gates candidates nor
pretends that a Gaussian geometric term is an identity probability.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

from .hierarchical_detection import HierarchicalCellDetection
from .motion_prediction import Covariance2D, MotionPrediction
from .track_motion_prior import TrackMotionPrior


ASSOCIATION_FEATURE_VERSION = "stv3-cell-association-features-v1"


@dataclass(frozen=True, slots=True)
class AssociationFeatureVector:
    previous_detection_id: str
    current_detection_id: str
    previous_source: str
    current_source: str
    source_changed: bool
    previous_object_kind: str
    current_object_kind: str
    same_object_kind: bool
    elapsed_seconds: float
    observed_displacement_x_m: float
    observed_displacement_y_m: float
    observed_interval_speed_kmh: float
    predicted_x_m: float
    predicted_y_m: float
    residual_x_m: float
    residual_y_m: float
    residual_distance_m: float
    innovation_covariance_m2: Covariance2D
    mahalanobis_squared: float
    gaussian_motion_nll: float
    area_ratio: float
    absolute_log_area_ratio: float
    mean_value_difference: float
    maximum_value_difference: float
    footprint_iou_unadvected: float
    flow_sample_id: str
    flow_method: str


@dataclass(frozen=True, slots=True)
class AssociationFeatureTable:
    analysis_grid_id: str
    previous_valid_at: float
    current_valid_at: float
    previous_detection_ids: tuple[str, ...]
    current_detection_ids: tuple[str, ...]
    features: tuple[AssociationFeatureVector, ...]
    algorithm_version: str = ASSOCIATION_FEATURE_VERSION

    @property
    def expected_feature_count(self) -> int:
        return len(self.previous_detection_ids) * len(self.current_detection_ids)

    @property
    def is_complete(self) -> bool:
        return len(self.features) == self.expected_feature_count


def build_association_feature_table(
    previous_detections: Iterable[HierarchicalCellDetection],
    current_detections: Iterable[HierarchicalCellDetection],
    predictions: Iterable[MotionPrediction | TrackMotionPrior],
) -> AssociationFeatureTable:
    """Enrich the complete cross product without deleting extreme candidates."""

    previous = _validated_detection_frame(previous_detections, "vorige")
    current = _validated_detection_frame(current_detections, "huidige")
    previous_time = previous[0].detection.valid_at
    current_time = current[0].detection.valid_at
    if current_time <= previous_time:
        raise ValueError("huidig detectieframe moet meteorologisch later zijn")
    grid_id = previous[0].detection.analysis_grid_id
    if current[0].detection.analysis_grid_id != grid_id:
        raise ValueError("detectieframes moeten hetzelfde analysis grid delen")

    prediction_values = tuple(predictions)
    prediction_map = {item.detection_id: item for item in prediction_values}
    if len(prediction_map) != len(prediction_values):
        raise ValueError("dubbele motion prediction voor een detectie")
    expected_previous_ids = {
        item.detection.detection_id for item in previous
    }
    if set(prediction_map) != expected_previous_ids:
        raise ValueError(
            "motion predictions moeten exact alle vorige detecties afdekken"
        )
    for prediction in prediction_values:
        if prediction.analysis_grid_id != grid_id:
            raise ValueError("motion prediction gebruikt een ander analysis grid")
        if not math.isclose(
            prediction.source_valid_at,
            previous_time,
            rel_tol=0.0,
            abs_tol=1e-6,
        ) or not math.isclose(
            prediction.target_valid_at,
            current_time,
            rel_tol=0.0,
            abs_tol=1e-6,
        ):
            raise ValueError("motion prediction heeft niet de frametijden")

    feature_vectors: list[AssociationFeatureVector] = []
    elapsed = current_time - previous_time
    for old in previous:
        prediction = prediction_map[old.detection.detection_id]
        if not math.isclose(
            prediction.source_x_m,
            old.detection.x_m,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ) or not math.isclose(
            prediction.source_y_m,
            old.detection.y_m,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ):
            raise ValueError(
                "motion prediction startpositie verschilt van vorige detectie"
            )
        for new in current:
            dx = new.detection.x_m - old.detection.x_m
            dy = new.detection.y_m - old.detection.y_m
            residual_x = new.detection.x_m - prediction.predicted_x_m
            residual_y = new.detection.y_m - prediction.predicted_y_m
            current_variance = new.detection.position_sigma_m**2
            innovation = (
                (
                    prediction.predicted_position_covariance_m2[0][0]
                    + current_variance,
                    prediction.predicted_position_covariance_m2[0][1],
                ),
                (
                    prediction.predicted_position_covariance_m2[1][0],
                    prediction.predicted_position_covariance_m2[1][1]
                    + current_variance,
                ),
            )
            mahalanobis, determinant = _mahalanobis_squared(
                residual_x,
                residual_y,
                innovation,
            )
            old_pixels = frozenset(old.footprint_pixels)
            new_pixels = frozenset(new.footprint_pixels)
            intersection = len(old_pixels & new_pixels)
            union = len(old_pixels | new_pixels)
            footprint_iou = intersection / union if union else 0.0
            area_ratio = new.area_m2 / old.area_m2
            feature_vectors.append(
                AssociationFeatureVector(
                    previous_detection_id=old.detection.detection_id,
                    current_detection_id=new.detection.detection_id,
                    previous_source=old.detection.source,
                    current_source=new.detection.source,
                    source_changed=(
                        old.detection.source != new.detection.source
                    ),
                    previous_object_kind=old.object_kind,
                    current_object_kind=new.object_kind,
                    same_object_kind=old.object_kind == new.object_kind,
                    elapsed_seconds=elapsed,
                    observed_displacement_x_m=dx,
                    observed_displacement_y_m=dy,
                    observed_interval_speed_kmh=(
                        math.hypot(dx, dy) / elapsed * 3.6
                    ),
                    predicted_x_m=prediction.predicted_x_m,
                    predicted_y_m=prediction.predicted_y_m,
                    residual_x_m=residual_x,
                    residual_y_m=residual_y,
                    residual_distance_m=math.hypot(residual_x, residual_y),
                    innovation_covariance_m2=innovation,
                    mahalanobis_squared=mahalanobis,
                    gaussian_motion_nll=(
                        math.log(2.0 * math.pi)
                        + 0.5 * math.log(determinant)
                        + 0.5 * mahalanobis
                    ),
                    area_ratio=area_ratio,
                    absolute_log_area_ratio=abs(math.log(area_ratio)),
                    mean_value_difference=new.mean_value - old.mean_value,
                    maximum_value_difference=(
                        new.maximum_value - old.maximum_value
                    ),
                    footprint_iou_unadvected=footprint_iou,
                    flow_sample_id=prediction.sample_id,
                    flow_method=prediction.method,
                )
            )

    table = AssociationFeatureTable(
        analysis_grid_id=grid_id,
        previous_valid_at=previous_time,
        current_valid_at=current_time,
        previous_detection_ids=tuple(
            item.detection.detection_id for item in previous
        ),
        current_detection_ids=tuple(
            item.detection.detection_id for item in current
        ),
        features=tuple(feature_vectors),
    )
    if not table.is_complete:
        raise AssertionError("associationfeatures verloren een kandidaat")
    return table


def _validated_detection_frame(
    values: Iterable[HierarchicalCellDetection],
    label: str,
) -> tuple[HierarchicalCellDetection, ...]:
    detections = tuple(values)
    if not detections:
        raise ValueError(f"het {label} detectieframe mag niet leeg zijn")
    if any(not isinstance(item, HierarchicalCellDetection) for item in detections):
        raise TypeError(
            f"het {label} frame vereist HierarchicalCellDetection-objecten"
        )
    identifiers = [item.detection.detection_id for item in detections]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError(f"het {label} frame bevat dubbele detection_id")
    times = {item.detection.valid_at for item in detections}
    if len(times) != 1:
        raise ValueError(f"het {label} frame bevat meerdere tijden")
    grids = {item.detection.analysis_grid_id for item in detections}
    if len(grids) != 1:
        raise ValueError(f"het {label} frame bevat meerdere grids")
    return tuple(
        sorted(detections, key=lambda item: item.detection.detection_id)
    )


def _mahalanobis_squared(
    x: float,
    y: float,
    covariance: Covariance2D,
) -> tuple[float, float]:
    xx, xy = covariance[0]
    yx, yy = covariance[1]
    determinant = xx * yy - xy * yx
    if not math.isfinite(determinant) or determinant <= 0.0:
        raise ValueError("innovation covariance moet positief definiet zijn")
    inverse_xx = yy / determinant
    inverse_xy = -xy / determinant
    inverse_yx = -yx / determinant
    inverse_yy = xx / determinant
    squared = (
        x * (inverse_xx * x + inverse_xy * y)
        + y * (inverse_yx * x + inverse_yy * y)
    )
    if not math.isfinite(squared):
        raise ValueError("Mahalanobis-afstand overschrijdt numeriek bereik")
    return max(0.0, squared), determinant
