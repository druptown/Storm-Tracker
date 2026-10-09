"""Reproducible read-only inventory and optional normalized-cache shadow replay."""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
import time

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from storm_tracker_worker.calibration_replay import (
    build_hierarchical_replay_from_raster_view,
    build_normalized_cache_inventory,
    build_normalized_raster_replay_view,
    load_normalized_frame_cache,
    run_bounded_shadow_assignment,
)
from storm_tracker_worker.tracking_core.engine.global_assignment import (
    AssignmentEdgeCost,
)
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
)
from storm_tracker_worker.tracking_core.engine.motion_prediction import (
    OpticalFlowSample,
)
from storm_tracker_worker.tracking_core.engine.motion_identity_evidence import (
    MotionEvidenceRecord,
)
from storm_tracker_worker.tracking_core.engine.pyramidal_sparse_flow import (
    SPARSE_PYRAMIDAL_FLOW_VERSION,
    SparseDetectionAggregationConfig,
    SparsePyramidalConfig,
    SparseReplayFlowSampleFactory,
    aggregate_sparse_detection_flow,
    estimate_sparse_pyramidal_motion,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inventariseer uitsluitend read-only de genormaliseerde "
            "pysteps_frame_cache en voer optioneel een offline shadowassignment "
            "uit. De cache is geen raw providerdata."
        )
    )
    parser.add_argument("database", type=Path)
    parser.add_argument("--region")
    parser.add_argument("--source")
    parser.add_argument("--min-frames", type=int, default=20)
    parser.add_argument("--shadow", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--shield-threshold", type=float, default=1.0)
    parser.add_argument("--core-threshold", type=float, default=5.0)
    parser.add_argument("--minimum-shield-area-km2", type=float, default=10.0)
    parser.add_argument("--minimum-core-area-km2", type=float, default=2.0)
    parser.add_argument("--shield-position-sigma-m", type=float, default=1_000.0)
    parser.add_argument("--core-position-sigma-m", type=float, default=750.0)
    parser.add_argument("--connectivity", type=int, choices=(4, 8), default=8)
    parser.add_argument(
        "--centroid-method",
        choices=("geometric", "value_weighted"),
        default="value_weighted",
    )
    parser.add_argument("--maximum-grid-cells", type=int, default=700_000)
    parser.add_argument(
        "--object-kind",
        choices=("rain_shield", "embedded_core", "all"),
        default="rain_shield",
    )
    parser.add_argument(
        "--prior-mode",
        choices=("constant", "last-two", "sparse-pyramidal"),
        default="constant",
        help=(
            "constant draait de bestaande caller-prior; last-two draait "
            "dezelfde detecties zowel met constant als met een stateful "
            "exacte laatste-twee-posities-prior; sparse-pyramidal draait "
            "alle drie op exact hetzelfde begrensde analysebeeld"
        ),
    )
    parser.add_argument("--prior-vx-m-s", type=float, default=0.0)
    parser.add_argument("--prior-vy-m-s", type=float, default=0.0)
    parser.add_argument("--velocity-sigma-m-s", type=float, default=25.0)
    parser.add_argument("--residual-position-sigma-m", type=float, default=5_000.0)
    parser.add_argument(
        "--last-two-process-position-sigma-m",
        type=float,
        default=5_000.0,
    )
    parser.add_argument(
        "--last-two-process-velocity-sigma-m-s",
        type=float,
        default=5.0,
    )
    parser.add_argument("--motion-cost-weight", type=float, default=0.5)
    parser.add_argument("--area-cost-weight", type=float, default=2.0)
    parser.add_argument("--nonoverlap-cost-weight", type=float, default=1.0)
    parser.add_argument("--death-cost", type=float, default=6.0)
    parser.add_argument("--birth-cost", type=float, default=6.0)
    parser.add_argument("--solution-limit", type=int, default=3)
    parser.add_argument("--maximum-top-k-detections", type=int, default=8)
    parser.add_argument("--maximum-complete-edges", type=int, default=2_000)
    parser.add_argument("--ambiguity-cost-delta", type=float, default=0.5)
    parser.add_argument("--jump-speed-kmh", type=float, default=200.0)
    parser.add_argument(
        "--sparse-analysis-maximum-pixels",
        type=int,
        default=20_000,
    )
    parser.add_argument(
        "--sparse-maximum-input-pixels",
        type=int,
        default=20_000,
    )
    parser.add_argument("--sparse-max-features", type=int, default=32)
    parser.add_argument(
        "--sparse-minimum-feature-distance-pixels",
        type=float,
        default=4.0,
    )
    parser.add_argument(
        "--sparse-feature-window-radius-pixels",
        type=int,
        default=2,
    )
    parser.add_argument(
        "--sparse-minimum-feature-valid-fraction",
        type=float,
        default=0.8,
    )
    parser.add_argument(
        "--sparse-minimum-feature-eigenvalue",
        type=float,
        default=0.01,
    )
    parser.add_argument("--sparse-pyramid-levels", type=int, default=4)
    parser.add_argument(
        "--sparse-minimum-pyramid-valid-fraction",
        type=float,
        default=0.5,
    )
    parser.add_argument(
        "--sparse-tracking-window-radius-pixels",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--sparse-minimum-tracking-samples",
        type=int,
        default=20,
    )
    parser.add_argument("--sparse-maximum-iterations", type=int, default=12)
    parser.add_argument(
        "--sparse-convergence-threshold-pixels",
        type=float,
        default=0.001,
    )
    parser.add_argument(
        "--sparse-minimum-tracking-eigenvalue",
        type=float,
        default=0.00001,
    )
    parser.add_argument(
        "--sparse-maximum-condition-number",
        type=float,
        default=100_000_000.0,
    )
    parser.add_argument("--sparse-regularization", type=float, default=1e-8)
    parser.add_argument(
        "--sparse-residual-variance-floor",
        type=float,
        default=1e-5,
    )
    parser.add_argument(
        "--sparse-process-noise-x-sigma-m-s",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--sparse-process-noise-y-sigma-m-s",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--sparse-aggregation-minimum-usable-vectors",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--sparse-aggregation-minimum-usable-fraction",
        type=float,
        default=0.25,
    )
    parser.add_argument(
        "--sparse-aggregation-include-iteration-limit",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--sparse-aggregation-residual-weight-floor",
        type=float,
        default=0.001,
    )
    parser.add_argument(
        "--sparse-aggregation-velocity-floor-x-sigma-m-s",
        type=float,
        default=5.0,
    )
    parser.add_argument(
        "--sparse-aggregation-velocity-floor-y-sigma-m-s",
        type=float,
        default=5.0,
    )
    parser.add_argument(
        "--sparse-aggregation-residual-position-x-sigma-m",
        type=float,
        default=10_000.0,
    )
    parser.add_argument(
        "--sparse-aggregation-residual-position-y-sigma-m",
        type=float,
        default=10_000.0,
    )
    parser.add_argument(
        "--sparse-aggregation-method",
        choices=(
            "conservative_mean",
            "inverse_residual_weighted_mean",
        ),
        default="inverse_residual_weighted_mean",
    )
    parser.add_argument(
        "--sparse-unavailable-velocity-sigma-m-s",
        type=float,
        default=100.0,
    )
    parser.add_argument(
        "--sparse-unavailable-residual-position-sigma-m",
        type=float,
        default=100_000.0,
    )
    parser.add_argument(
        "--sparse-maximum-frame-pairs",
        type=int,
        default=40,
    )
    parser.add_argument(
        "--sparse-maximum-total-feature-tracks",
        type=int,
        default=2_000,
    )
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_cli_policy(args: argparse.Namespace) -> None:
    if args.min_frames < 2:
        raise ValueError("--min-frames moet minstens twee zijn")
    for label in ("prior_vx_m_s", "prior_vy_m_s"):
        if not math.isfinite(getattr(args, label)):
            raise ValueError(
                f"--{label.replace('_', '-')} moet eindig zijn"
            )
    for label in (
        "velocity_sigma_m_s",
        "residual_position_sigma_m",
        "last_two_process_position_sigma_m",
        "last_two_process_velocity_sigma_m_s",
        "motion_cost_weight",
        "area_cost_weight",
        "nonoverlap_cost_weight",
        "death_cost",
        "birth_cost",
        "ambiguity_cost_delta",
        "jump_speed_kmh",
    ):
        value = getattr(args, label)
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(f"--{label.replace('_', '-')} moet niet-negatief zijn")
    if args.jump_speed_kmh <= 0.0:
        raise ValueError("--jump-speed-kmh moet positief zijn")
    if (
        args.prior_mode == "last-two"
        and args.velocity_sigma_m_s == 0.0
        and args.residual_position_sigma_m == 0.0
    ):
        raise ValueError(
            "last-two cold start vereist positieve caller-onzekerheid"
        )
    for label in (
        "sparse_analysis_maximum_pixels",
        "sparse_maximum_input_pixels",
        "sparse_max_features",
        "sparse_feature_window_radius_pixels",
        "sparse_pyramid_levels",
        "sparse_tracking_window_radius_pixels",
        "sparse_minimum_tracking_samples",
        "sparse_maximum_iterations",
        "sparse_aggregation_minimum_usable_vectors",
        "sparse_maximum_frame_pairs",
        "sparse_maximum_total_feature_tracks",
    ):
        value = getattr(args, label)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(
                f"--{label.replace('_', '-')} moet positief geheel zijn"
            )
    for label in (
        "sparse_minimum_feature_distance_pixels",
        "sparse_minimum_feature_valid_fraction",
        "sparse_minimum_feature_eigenvalue",
        "sparse_minimum_pyramid_valid_fraction",
        "sparse_convergence_threshold_pixels",
        "sparse_minimum_tracking_eigenvalue",
        "sparse_maximum_condition_number",
        "sparse_regularization",
        "sparse_residual_variance_floor",
        "sparse_process_noise_x_sigma_m_s",
        "sparse_process_noise_y_sigma_m_s",
        "sparse_aggregation_minimum_usable_fraction",
        "sparse_aggregation_residual_weight_floor",
        "sparse_aggregation_velocity_floor_x_sigma_m_s",
        "sparse_aggregation_velocity_floor_y_sigma_m_s",
        "sparse_aggregation_residual_position_x_sigma_m",
        "sparse_aggregation_residual_position_y_sigma_m",
        "sparse_unavailable_velocity_sigma_m_s",
        "sparse_unavailable_residual_position_sigma_m",
    ):
        value = getattr(args, label)
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(
                f"--{label.replace('_', '-')} moet niet-negatief zijn"
            )
    if (
        args.sparse_analysis_maximum_pixels
        > args.sparse_maximum_input_pixels
    ):
        raise ValueError(
            "--sparse-analysis-maximum-pixels mag "
            "--sparse-maximum-input-pixels niet overschrijden"
        )
    if (
        args.sparse_unavailable_velocity_sigma_m_s <= 0.0
        or args.sparse_unavailable_residual_position_sigma_m <= 0.0
    ):
        raise ValueError(
            "sparse unavailable fallback vereist expliciete brede "
            "positieve onzekerheid"
        )


def _assignment_summary(assignment, *, include_prior_audits: bool) -> dict:
    summary = assignment.to_dict()
    summary["metrics"].pop("tracks", None)
    if not include_prior_audits:
        summary.pop("motion_prior_audits", None)
        for assessment in summary["identity_frame_assessments"]:
            assessment.pop("candidates", None)
    summary["frame_audits"] = [
        {
            "previous_valid_at": item.previous_valid_at,
            "current_valid_at": item.current_valid_at,
            "previous_detection_count": item.previous_detection_count,
            "current_detection_count": item.current_detection_count,
            "complete_feature_count": item.complete_feature_count,
            "solver": item.solver,
            "solution_count": item.solution_count,
            "best_total_cost": item.best_total_cost,
            "alternative_cost_gap": item.alternative_cost_gap,
            "ambiguous": item.ambiguous,
            "matches": item.match_count,
            "births": item.birth_count,
            "deaths": item.death_count,
            "identity_indeterminate_matches": (
                item.identity_assessment.indeterminate_match_count
                if item.identity_assessment is not None
                else 0
            ),
            "identity_supported_unvalidated_matches": (
                item.identity_assessment.supported_unvalidated_match_count
                if item.identity_assessment is not None
                else 0
            ),
            "motion_unavailable_matches": (
                item.identity_assessment.motion_unavailable_match_count
                if item.identity_assessment is not None
                else 0
            ),
            "all_candidates_retained": (
                item.identity_assessment.all_candidates_retained
                if item.identity_assessment is not None
                else True
            ),
            "identity_confirmation_claimed": False,
        }
        for item in assignment.frame_audits
    ]
    return summary


def _prior_mode_comparison(
    replay,
    object_kind: str,
    constant_assignment,
    last_two_assignment,
    constant_summary: dict,
    last_two_summary: dict,
) -> dict:
    if (
        constant_assignment.detection_evidence_sha256
        != last_two_assignment.detection_evidence_sha256
    ):
        raise AssertionError(
            "priorvergelijking gebruikte niet exact dezelfde detecties"
        )
    constant_tracks = dict(constant_assignment.assignments)
    last_two_tracks = dict(last_two_assignment.assignments)
    first_divergence = None
    for frame_index, frame in enumerate(replay.frames):
        if object_kind == "rain_shield":
            detections = frame.rain_shields
        elif object_kind == "embedded_core":
            detections = frame.embedded_cores
        else:
            detections = frame.detections
        differing = sorted(
            item.detection.detection_id
            for item in detections
            if (
                constant_tracks.get(item.detection.detection_id)
                != last_two_tracks.get(item.detection.detection_id)
            )
        )
        if differing:
            first_divergence = {
                "frame_index": frame_index,
                "valid_at": frame.valid_at,
                "detection_ids": differing,
            }
            break

    fields = (
        "track_count",
        "match_count",
        "birth_count",
        "death_count",
        "right_censored_track_count",
        "chosen_speed_median_kmh",
        "chosen_speed_p95_kmh",
        "chosen_speed_maximum_kmh",
    )
    constant_metrics = {
        field: constant_summary[field] for field in fields
    }
    constant_metrics["jump_count"] = (
        constant_summary["metrics"]["jump_count"]
    )
    last_two_metrics = {
        field: last_two_summary[field] for field in fields
    }
    last_two_metrics["jump_count"] = (
        last_two_summary["metrics"]["jump_count"]
    )
    numeric_delta = {}
    for field in (*fields, "jump_count"):
        before = constant_metrics[field]
        after = last_two_metrics[field]
        numeric_delta[field] = (
            after - before
            if before is not None and after is not None
            else None
        )
    feedback_observation = (
        "The first assignment-state divergence is reported, but no external "
        "truth exists to label either branch correct. Because last-two feeds "
        "earlier assignments into later priors, an erroneous branch can "
        "propagate."
        if first_divergence is not None
        else
        "No assignment-state divergence was observed in this replay. The "
        "last-two feedback path can still propagate an earlier error in other "
        "sequences; external truth is unavailable."
    )
    return {
        "same_detection_evidence": True,
        "detection_evidence_sha256":
            constant_assignment.detection_evidence_sha256,
        "constant_metrics": constant_metrics,
        "last_two_metrics": last_two_metrics,
        "last_two_minus_constant": numeric_delta,
        "first_assignment_state_divergence": first_divergence,
        "feedback_poisoning_observation": feedback_observation,
        "ground_truth_available": False,
        "comparison_is_meteorological_validation": False,
    }


def _selected_detections(frame, object_kind: str):
    if object_kind == "rain_shield":
        return frame.rain_shields
    if object_kind == "embedded_core":
        return frame.embedded_cores
    return frame.detections


def _numeric_summary(values) -> dict:
    finite = sorted(
        float(value)
        for value in values
        if value is not None and math.isfinite(float(value))
    )
    if not finite:
        return {
            "count": 0,
            "minimum": None,
            "median": None,
            "p95": None,
            "maximum": None,
        }
    rank = max(0, math.ceil(0.95 * len(finite)) - 1)
    return {
        "count": len(finite),
        "minimum": finite[0],
        "median": statistics.median(finite),
        "p95": finite[rank],
        "maximum": finite[-1],
    }


def _stable_fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _sparse_configs(args):
    flow = SparsePyramidalConfig(
        maximum_input_pixels=args.sparse_maximum_input_pixels,
        max_features=args.sparse_max_features,
        minimum_feature_distance_pixels=(
            args.sparse_minimum_feature_distance_pixels
        ),
        feature_window_radius_pixels=(
            args.sparse_feature_window_radius_pixels
        ),
        minimum_feature_valid_fraction=(
            args.sparse_minimum_feature_valid_fraction
        ),
        minimum_feature_eigenvalue=(
            args.sparse_minimum_feature_eigenvalue
        ),
        pyramid_levels=args.sparse_pyramid_levels,
        minimum_pyramid_valid_fraction=(
            args.sparse_minimum_pyramid_valid_fraction
        ),
        tracking_window_radius_pixels=(
            args.sparse_tracking_window_radius_pixels
        ),
        minimum_tracking_samples=args.sparse_minimum_tracking_samples,
        maximum_iterations=args.sparse_maximum_iterations,
        convergence_threshold_pixels=(
            args.sparse_convergence_threshold_pixels
        ),
        minimum_tracking_eigenvalue=(
            args.sparse_minimum_tracking_eigenvalue
        ),
        maximum_condition_number=args.sparse_maximum_condition_number,
        regularization=args.sparse_regularization,
        residual_variance_floor=args.sparse_residual_variance_floor,
        velocity_process_noise_covariance_m2_s2=(
            (args.sparse_process_noise_x_sigma_m_s**2, 0.0),
            (0.0, args.sparse_process_noise_y_sigma_m_s**2),
        ),
    )
    aggregation = SparseDetectionAggregationConfig(
        minimum_usable_vectors=(
            args.sparse_aggregation_minimum_usable_vectors
        ),
        minimum_usable_fraction=(
            args.sparse_aggregation_minimum_usable_fraction
        ),
        include_iteration_limit=(
            args.sparse_aggregation_include_iteration_limit
        ),
        residual_weight_floor=(
            args.sparse_aggregation_residual_weight_floor
        ),
        velocity_covariance_floor_m2_s2=(
            (
                args.sparse_aggregation_velocity_floor_x_sigma_m_s**2,
                0.0,
            ),
            (
                0.0,
                args.sparse_aggregation_velocity_floor_y_sigma_m_s**2,
            ),
        ),
        residual_position_covariance_m2=(
            (
                args.sparse_aggregation_residual_position_x_sigma_m**2,
                0.0,
            ),
            (
                0.0,
                args.sparse_aggregation_residual_position_y_sigma_m**2,
            ),
        ),
        method=args.sparse_aggregation_method,
    )
    return flow, aggregation


class _AuditedSparseReplayFactory:
    def __init__(
        self,
        *,
        sequence,
        aggregation_config,
        unavailable_velocity_sigma_m_s: float,
        unavailable_residual_position_sigma_m: float,
    ) -> None:
        self.sequence = sequence
        self.aggregation_config = aggregation_config
        self.records = []
        self._evidence_by_key = {}
        self._evidence_by_previous_detection_id = {}
        velocity_variance = unavailable_velocity_sigma_m_s**2
        residual_variance = unavailable_residual_position_sigma_m**2

        def unavailable(detection, current_frame, aggregation):
            pair = sequence.pair_for_current_frame(current_frame.frame_id)
            sample_token = {
                "pair_checksum": pair.checksum,
                "detection_id": detection.detection.detection_id,
                "aggregation_status": aggregation.status,
                "velocity_variance": velocity_variance,
                "residual_variance": residual_variance,
            }
            return OpticalFlowSample(
                sample_id=(
                    "caller-broad-sparse-unavailable:"
                    + _stable_fingerprint(sample_token)
                ),
                analysis_grid_id=detection.detection.analysis_grid_id,
                reference_valid_at=detection.detection.valid_at,
                vx_m_s=0.0,
                vy_m_s=0.0,
                velocity_covariance_m2_s2=(
                    (velocity_variance, 0.0),
                    (0.0, velocity_variance),
                ),
                residual_covariance_m2=(
                    (residual_variance, 0.0),
                    (0.0, residual_variance),
                ),
                method=(
                    "caller_explicit_broad_sparse_cold_start_or_"
                    "unavailable_v1"
                ),
                support_frame_ids=(
                    pair.previous_frame_id,
                    pair.current_frame_id,
                ),
            )

        self._factory = SparseReplayFlowSampleFactory(
            sequence=sequence,
            aggregation_config=aggregation_config,
            unavailable_factory=unavailable,
        )

    def __call__(self, detection, current_frame):
        pair = self.sequence.pair_for_current_frame(current_frame.frame_id)
        aggregation = aggregate_sparse_detection_flow(
            pair,
            detection,
            self.aggregation_config,
        )
        sample = self._factory(detection, current_frame)
        evidence = MotionEvidenceRecord(
            previous_detection_id=detection.detection.detection_id,
            target_valid_at=current_frame.valid_at,
            status=(
                "available"
                if aggregation.sample is not None
                else "unavailable"
            ),
            reason=aggregation.status,
            method=sample.method,
            support_frame_ids=sample.support_frame_ids,
            selected_vectors=aggregation.selected_vectors_in_footprint,
            usable_vectors=aggregation.usable_vectors,
            usable_fraction=aggregation.usable_fraction,
        )
        key = (
            detection.detection.detection_id,
            current_frame.frame_id,
        )
        if key in self._evidence_by_key:
            raise AssertionError("dubbele sparse motion-evidence replaycall")
        self._evidence_by_key[key] = evidence
        previous_id = detection.detection.detection_id
        if previous_id in self._evidence_by_previous_detection_id:
            raise AssertionError(
                "detectie kreeg meer dan één sparse targetframe"
            )
        self._evidence_by_previous_detection_id[previous_id] = evidence
        self.records.append({
            "previous_frame_id": pair.previous_frame_id,
            "current_frame_id": pair.current_frame_id,
            "previous_valid_at": pair.previous_valid_at,
            "current_valid_at": pair.current_valid_at,
            "detection_id": detection.detection.detection_id,
            "aggregation_status": aggregation.status,
            "global_candidate_features":
                aggregation.global_candidate_features,
            "global_selected_features":
                aggregation.global_selected_features,
            "selected_vectors_in_footprint":
                aggregation.selected_vectors_in_footprint,
            "selected_vector_coverage_fraction":
                aggregation.selected_vector_coverage_fraction,
            "usable_vectors": aggregation.usable_vectors,
            "usable_fraction": aggregation.usable_fraction,
            "mean_forward_residual_rms":
                aggregation.mean_forward_residual_rms,
            "fallback_used": aggregation.sample is None,
            "motion_evidence_status": evidence.status,
            "motion_evidence_reason": evidence.reason,
            "motion_evidence_fingerprint": evidence.fingerprint,
            "identity_claim_permitted": False,
            "sample_method": sample.method,
            "sample_vx_m_s": sample.vx_m_s,
            "sample_vy_m_s": sample.vy_m_s,
            "sample_velocity_covariance_m2_s2":
                sample.velocity_covariance_m2_s2,
            "support_frame_ids": list(sample.support_frame_ids),
        })
        return sample

    def evidence_for(self, detection, current_frame, prediction):
        key = (
            detection.detection.detection_id,
            current_frame.frame_id,
        )
        try:
            evidence = self._evidence_by_key[key]
        except KeyError as err:
            raise AssertionError(
                "sparse motion-evidence ontbreekt na sampleconstructie"
            ) from err
        if (
            evidence.method != prediction.method
            or not math.isclose(
                evidence.target_valid_at,
                prediction.target_valid_at,
                rel_tol=0.0,
                abs_tol=1e-6,
            )
        ):
            raise AssertionError(
                "sparse motion-evidence en prediction zijn inconsistent"
            )
        return evidence

    def evidence_for_previous_detection_id(self, detection_id):
        try:
            return self._evidence_by_previous_detection_id[detection_id]
        except KeyError as err:
            raise AssertionError(
                "edge-costconstructie mist sparse motion-evidence"
            ) from err


def _sparse_flow_audit(sequence, factory) -> dict:
    vectors = [
        vector
        for pair in sequence.pairs
        for vector in pair.vectors
    ]
    available = [item for item in vectors if item.usable]
    status_counts = Counter(item.status for item in vectors)
    failure_reasons = Counter(
        item.failure_reason
        for item in vectors
        if item.failure_reason is not None
    )
    covariance_xx = [
        item.velocity_covariance_m2_s2[0][0]
        for item in available
        if item.velocity_covariance_m2_s2 is not None
    ]
    covariance_yy = [
        item.velocity_covariance_m2_s2[1][1]
        for item in available
        if item.velocity_covariance_m2_s2 is not None
    ]
    aggregation_statuses = Counter(
        item["aggregation_status"] for item in factory.records
    )
    motion_evidence_statuses = Counter(
        item["motion_evidence_status"] for item in factory.records
    )
    return {
        "algorithm_version": sequence.algorithm_version,
        "sequence_checksum": sequence.checksum,
        "config_fingerprint": sequence.config_fingerprint,
        "frame_ids": list(sequence.frame_ids),
        "pair_count": len(sequence.pairs),
        "feature_candidate_count": sum(
            item.feature_audit.candidate_count for item in sequence.pairs
        ),
        "feature_selected_count": sum(
            item.feature_audit.selected_count for item in sequence.pairs
        ),
        "vector_count": len(vectors),
        "vector_usable_count": len(available),
        "vector_failure_count": len(vectors) - len(available),
        "vector_status_counts": dict(sorted(status_counts.items())),
        "vector_failure_reason_counts": dict(sorted(failure_reasons.items())),
        "feature_candidate_coverage_fraction": _numeric_summary(
            item.feature_audit.candidate_coverage_fraction
            for item in sequence.pairs
        ),
        "forward_residual_rms": _numeric_summary(
            item.forward_residual_rms for item in available
        ),
        "feature_quality": _numeric_summary(
            item.feature_quality for item in vectors
        ),
        "structure_quality": _numeric_summary(
            item.structure_quality for item in available
        ),
        "velocity_covariance_xx_m2_s2":
            _numeric_summary(covariance_xx),
        "velocity_covariance_yy_m2_s2":
            _numeric_summary(covariance_yy),
        "vx_m_s": _numeric_summary(item.vx_m_s for item in available),
        "vy_m_s": _numeric_summary(item.vy_m_s for item in available),
        "aggregation_call_count": len(factory.records),
        "aggregation_status_counts":
            dict(sorted(aggregation_statuses.items())),
        "motion_evidence_status_counts":
            dict(sorted(motion_evidence_statuses.items())),
        "aggregation_fallback_count": sum(
            item["fallback_used"] for item in factory.records
        ),
        "identity_claim_permitted_count": sum(
            item["identity_claim_permitted"] for item in factory.records
        ),
        "aggregation_selected_vector_coverage_fraction": _numeric_summary(
            item["selected_vector_coverage_fraction"]
            for item in factory.records
        ),
        "aggregation_usable_fraction": _numeric_summary(
            item["usable_fraction"] for item in factory.records
        ),
        "aggregation_mean_forward_residual_rms": _numeric_summary(
            item["mean_forward_residual_rms"]
            for item in factory.records
        ),
        "association_flow_samples": factory.records,
        "causality_contract": {
            "pair_support": "only_previous_and_current_target_frame",
            "future_frames_beyond_target_used": False,
            "maximum_future_frame_offset": 0,
        },
        "speed_cap_kmh": None,
        "smoothing": False,
        "candidate_rejection_by_speed": False,
    }


def _all_mode_comparison(
    replay,
    object_kind: str,
    assignments,
    summaries,
) -> dict:
    hashes = {
        mode: value.detection_evidence_sha256
        for mode, value in assignments.items()
    }
    if len(set(hashes.values())) != 1:
        raise AssertionError(
            "algoritmevergelijking gebruikte niet exact dezelfde detecties"
        )

    def first_divergence(first_mode, second_mode):
        first_tracks = dict(assignments[first_mode].assignments)
        second_tracks = dict(assignments[second_mode].assignments)
        for frame_index, frame in enumerate(replay.frames):
            differing = sorted(
                item.detection.detection_id
                for item in _selected_detections(frame, object_kind)
                if (
                    first_tracks.get(item.detection.detection_id)
                    != second_tracks.get(item.detection.detection_id)
                )
            )
            if differing:
                return {
                    "frame_index": frame_index,
                    "valid_at": frame.valid_at,
                    "detection_ids": differing,
                }
        return None

    fields = (
        "track_count",
        "match_count",
        "birth_count",
        "death_count",
        "right_censored_track_count",
        "chosen_speed_median_kmh",
        "chosen_speed_p95_kmh",
        "chosen_speed_maximum_kmh",
    )
    metrics = {}
    for mode, summary in summaries.items():
        metrics[mode] = {field: summary[field] for field in fields}
        metrics[mode]["jump_count"] = summary["metrics"]["jump_count"]
        metrics[mode]["missed_detection_slots"] = (
            summary["metrics"]["missed_detection_slots"]
        )
    modes = tuple(assignments)
    pairwise = {}
    for first_index, first_mode in enumerate(modes):
        for second_mode in modes[first_index + 1:]:
            pairwise[f"{first_mode}_vs_{second_mode}"] = first_divergence(
                first_mode,
                second_mode,
            )
    return {
        "same_detection_evidence": True,
        "detection_evidence_sha256": next(iter(hashes.values())),
        "detection_evidence_sha256_by_mode": hashes,
        "metrics_by_mode": metrics,
        "pairwise_first_assignment_divergence": pairwise,
        "ground_truth_available": False,
        "comparison_is_meteorological_validation": False,
        "correct_algorithm_claimed": None,
    }


def main() -> int:
    args = _arguments()
    _validate_cli_policy(args)
    database = args.database.expanduser().resolve(strict=True)
    before_sha256 = _sha256(database)
    corpus = load_normalized_frame_cache(
        database,
        region_id=args.region,
        source=args.source,
    )
    inventories = build_normalized_cache_inventory(
        corpus,
        minimum_sequence_frames=args.min_frames,
    )
    report: dict = {
        "mode": "read_only_calibration_replay",
        "database": str(database),
        "database_sha256_before": before_sha256,
        "connection_mode": corpus.connection_mode,
        "data_kind": corpus.data_kind,
        "operational_state_mutated": False,
        "normalized_cache_issue_count": len(corpus.issues),
        "normalized_cache_inventories": [
            inventory.to_dict() for inventory in inventories
        ],
    }
    if args.shadow:
        shadow_started = time.perf_counter()
        qualifying = [
            sequence
            for inventory in inventories
            for sequence in inventory.qualifying_sequences
        ]
        if not qualifying:
            raise ValueError(
                "geen genormaliseerde sequence voldoet aan --min-frames"
            )
        sequence = max(
            qualifying,
            key=lambda item: (item.frame_count, item.ends_at, item.sequence_id),
        )
        detector_config = HierarchicalDetectorConfig(
            shield_threshold=args.shield_threshold,
            core_threshold=args.core_threshold,
            minimum_shield_area_m2=args.minimum_shield_area_km2 * 1_000_000.0,
            minimum_core_area_m2=args.minimum_core_area_km2 * 1_000_000.0,
            shield_position_sigma_m=args.shield_position_sigma_m,
            core_position_sigma_m=args.core_position_sigma_m,
            connectivity=args.connectivity,
            centroid_method=args.centroid_method,
        )
        sparse_requested = args.prior_mode == "sparse-pyramidal"
        frame_pair_count = max(0, sequence.frame_count - 1)
        if (
            sparse_requested
            and frame_pair_count > args.sparse_maximum_frame_pairs
        ):
            raise ValueError(
                "sparse replay overschrijdt --sparse-maximum-frame-pairs"
            )
        if (
            sparse_requested
            and frame_pair_count * args.sparse_max_features
            > args.sparse_maximum_total_feature_tracks
        ):
            raise ValueError(
                "sparse replay overschrijdt "
                "--sparse-maximum-total-feature-tracks"
            )
        view_started = time.perf_counter()
        raster_view = build_normalized_raster_replay_view(
            sequence,
            maximum_source_grid_cells=args.maximum_grid_cells,
            maximum_analysis_pixels=(
                args.sparse_analysis_maximum_pixels
                if sparse_requested
                else args.maximum_grid_cells
            ),
        )
        view_runtime = time.perf_counter() - view_started
        detection_started = time.perf_counter()
        replay = build_hierarchical_replay_from_raster_view(
            raster_view,
            detector_config=detector_config,
        )
        detection_runtime = time.perf_counter() - detection_started
        velocity_variance = args.velocity_sigma_m_s**2
        residual_variance = args.residual_position_sigma_m**2
        process_position_variance = (
            args.last_two_process_position_sigma_m**2
        )
        process_velocity_variance = (
            args.last_two_process_velocity_sigma_m_s**2
        )
        last_two_process_noise = (
            (process_position_variance, 0.0, 0.0, 0.0),
            (0.0, process_position_variance, 0.0, 0.0),
            (0.0, 0.0, process_velocity_variance, 0.0),
            (0.0, 0.0, 0.0, process_velocity_variance),
        )
        detector_policy = {
            "shield_threshold": args.shield_threshold,
            "core_threshold": args.core_threshold,
            "minimum_shield_area_km2": args.minimum_shield_area_km2,
            "minimum_core_area_km2": args.minimum_core_area_km2,
            "shield_position_sigma_m": args.shield_position_sigma_m,
            "core_position_sigma_m": args.core_position_sigma_m,
            "connectivity": args.connectivity,
            "centroid_method": args.centroid_method,
        }
        feature_cost_policy = {
            "motion_cost_weight": args.motion_cost_weight,
            "area_cost_weight": args.area_cost_weight,
            "nonoverlap_cost_weight": args.nonoverlap_cost_weight,
            "death_cost": args.death_cost,
            "birth_cost": args.birth_cost,
            "unavailable_motion_cost_policy": (
                "zero_weight_keep_complete_feature_and_candidate"
            ),
        }
        assignment_resource_policy = {
            "solution_limit": args.solution_limit,
            "maximum_top_k_detections":
                args.maximum_top_k_detections,
            "maximum_complete_edges": args.maximum_complete_edges,
            "ambiguity_cost_delta": args.ambiguity_cost_delta,
            "jump_speed_kmh": args.jump_speed_kmh,
        }
        base_cost_model_id = (
            "cli-explicit-motion-area-iou-v3:"
            + _stable_fingerprint(feature_cost_policy)[:16]
        )
        sparse_cost_model_id = (
            "cli-explicit-evidence-aware-motion-area-iou-v3:"
            + _stable_fingerprint({
                **feature_cost_policy,
                "motion_evidence_required_for_motion_component": True,
            })[:16]
        )

        def make_flow(method):
            def flow(old, current_frame):
                return OpticalFlowSample(
                    sample_id=(
                        f"cli-prior:{method}:"
                        f"{old.detection.detection_id}"
                    ),
                    analysis_grid_id=old.detection.analysis_grid_id,
                    reference_valid_at=old.detection.valid_at,
                    vx_m_s=args.prior_vx_m_s,
                    vy_m_s=args.prior_vy_m_s,
                    velocity_covariance_m2_s2=(
                        (velocity_variance, 0.0),
                        (0.0, velocity_variance),
                    ),
                    residual_covariance_m2=(
                        (residual_variance, 0.0),
                        (0.0, residual_variance),
                    ),
                    method=method,
                    support_frame_ids=(old.detection.detection_id,),
                )

            return flow

        def edge(feature, *, prior_mode):
            motion_component = (
                args.motion_cost_weight * feature.mahalanobis_squared
            )
            mode_cost_model_id = base_cost_model_id
            if prior_mode == "sparse-pyramidal":
                evidence = (
                    sparse_factory.evidence_for_previous_detection_id(
                        feature.previous_detection_id
                    )
                )
                mode_cost_model_id = sparse_cost_model_id
                if evidence.status != "available":
                    # Missing evidence is neutral, not weak evidence.  The
                    # measured feature remains in the complete candidate audit
                    # and no edge is rejected.
                    motion_component = 0.0
            components = (
                (
                    (
                        "mahalanobis_squared_available_motion"
                        if prior_mode == "sparse-pyramidal"
                        else "mahalanobis_squared"
                    ),
                    motion_component,
                ),
                (
                    "absolute_log_area_ratio",
                    args.area_cost_weight * feature.absolute_log_area_ratio,
                ),
                (
                    "unadvected_nonoverlap",
                    args.nonoverlap_cost_weight
                    * (1.0 - feature.footprint_iou_unadvected),
                ),
            )
            return AssignmentEdgeCost(
                previous_detection_id=feature.previous_detection_id,
                current_detection_id=feature.current_detection_id,
                cost=sum(value for _name, value in components),
                cost_model_id=mode_cost_model_id,
                components=components,
            )

        sparse_flow_config = None
        sparse_aggregation_config = None
        sparse_flow_sequence = None
        sparse_factory = None
        sparse_flow_runtime = None
        sparse_policy = None
        if sparse_requested:
            (
                sparse_flow_config,
                sparse_aggregation_config,
            ) = _sparse_configs(args)
            if (
                raster_view.transform.output_pixel_count
                > sparse_flow_config.maximum_input_pixels
            ):
                raise ValueError(
                    "begrensd analysebeeld overschrijdt sparse "
                    "maximum_input_pixels"
                )
            flow_started = time.perf_counter()
            sparse_flow_sequence = estimate_sparse_pyramidal_motion(
                raster_view.frames,
                sparse_flow_config,
            )
            sparse_flow_runtime = time.perf_counter() - flow_started
            sparse_factory = _AuditedSparseReplayFactory(
                sequence=sparse_flow_sequence,
                aggregation_config=sparse_aggregation_config,
                unavailable_velocity_sigma_m_s=(
                    args.sparse_unavailable_velocity_sigma_m_s
                ),
                unavailable_residual_position_sigma_m=(
                    args.sparse_unavailable_residual_position_sigma_m
                ),
            )
            sparse_policy = {
                "algorithm_version": SPARSE_PYRAMIDAL_FLOW_VERSION,
                "flow_config": asdict(sparse_flow_config),
                "flow_config_fingerprint":
                    sparse_flow_config.fingerprint,
                "aggregation_config": asdict(
                    sparse_aggregation_config
                ),
                "aggregation_config_fingerprint":
                    sparse_aggregation_config.fingerprint,
                "unavailable_fallback": {
                    "method": (
                        "caller_explicit_broad_sparse_cold_start_or_"
                        "unavailable_v1"
                    ),
                    "vx_m_s": 0.0,
                    "vy_m_s": 0.0,
                    "velocity_sigma_m_s":
                        args.sparse_unavailable_velocity_sigma_m_s,
                    "residual_position_sigma_m":
                        args.sparse_unavailable_residual_position_sigma_m,
                    "silent_zero_motion": False,
                    "semantics": (
                        "uncertainty_placeholder_not_measured_zero_motion"
                    ),
                    "motion_evidence_status": "unavailable",
                    "identity_claim_permitted": False,
                    "motion_cost_contribution": 0.0,
                    "measured_motion_feature_retained": True,
                },
                "resource_caps": {
                    "analysis_maximum_pixels":
                        args.sparse_analysis_maximum_pixels,
                    "maximum_input_pixels":
                        args.sparse_maximum_input_pixels,
                    "maximum_frame_pairs":
                        args.sparse_maximum_frame_pairs,
                    "maximum_total_feature_tracks":
                        args.sparse_maximum_total_feature_tracks,
                },
                "analysis_transform_fingerprint":
                    raster_view.transform.fingerprint,
                "speed_cap_kmh": None,
                "smoothing": False,
                "candidate_rejection_by_speed": False,
                "assignment_semantics": (
                    "one_best_hypothesis_not_confirmed_identity"
                ),
            }

        assignment_runtime = {}
        policy_ids = {}

        def run_assignment(prior_mode):
            last_two = prior_mode == "last-two"
            if prior_mode == "sparse-pyramidal":
                flow_factory = sparse_factory
                motion_evidence_factory = sparse_factory.evidence_for
                mode_cost_model_id = sparse_cost_model_id
                mode_policy = sparse_policy
            else:
                flow_factory = make_flow(
                    "cold_start_cli_caller_supplied_constant_prior"
                    if last_two
                    else "cli_caller_supplied_constant_prior"
                )
                motion_evidence_factory = None
                mode_cost_model_id = base_cost_model_id
                mode_policy = {
                    "prior_vx_m_s": args.prior_vx_m_s,
                    "prior_vy_m_s": args.prior_vy_m_s,
                    "velocity_sigma_m_s": args.velocity_sigma_m_s,
                    "residual_position_sigma_m":
                        args.residual_position_sigma_m,
                    "last_two_process_position_sigma_m": (
                        args.last_two_process_position_sigma_m
                        if last_two else None
                    ),
                    "last_two_process_velocity_sigma_m_s": (
                        args.last_two_process_velocity_sigma_m_s
                        if last_two else None
                    ),
                    "speed_cap_kmh": None,
                    "smoothing": False,
                    "candidate_rejection_by_speed": False,
                }
            policy_payload = {
                "schema": "stv3-offline-replay-policy-v4",
                "prior_mode": prior_mode,
                "data_kind": corpus.data_kind,
                "sequence_id": sequence.sequence_id,
                "analysis_transform":
                    raster_view.transform.to_dict(),
                "detector": detector_policy,
                "feature_cost": feature_cost_policy,
                "cost_model_id": mode_cost_model_id,
                "assignment_resources": assignment_resource_policy,
                "motion": mode_policy,
            }
            policy_id = (
                f"cli-explicit-offline-policy-v4:{prior_mode}:"
                f"{_stable_fingerprint(policy_payload)}"
            )
            policy_ids[prior_mode] = policy_id
            started = time.perf_counter()
            result = run_bounded_shadow_assignment(
                replay,
                object_kind=args.object_kind,
                flow_sample_factory=flow_factory,
                edge_cost_factory=lambda feature: edge(
                    feature,
                    prior_mode=prior_mode,
                ),
                death_cost=args.death_cost,
                birth_cost=args.birth_cost,
                policy_id=policy_id,
                solution_limit=args.solution_limit,
                maximum_top_k_current_detections=(
                    args.maximum_top_k_detections
                ),
                maximum_complete_edges_per_pair=(
                    args.maximum_complete_edges
                ),
                ambiguity_cost_delta=args.ambiguity_cost_delta,
                jump_speed_threshold_kmh=args.jump_speed_kmh,
                motion_prior_mode=prior_mode,
                last_two_process_noise_covariance=(
                    last_two_process_noise if last_two else None
                ),
                motion_evidence_factory=motion_evidence_factory,
            )
            assignment_runtime[prior_mode] = (
                time.perf_counter() - started
            )
            return result

        constant_assignment = run_assignment("constant")
        assignments_by_mode = {
            "constant": constant_assignment,
        }
        if args.prior_mode in ("last-two", "sparse-pyramidal"):
            assignments_by_mode["last-two"] = run_assignment("last-two")
        if sparse_requested:
            assignments_by_mode["sparse-pyramidal"] = run_assignment(
                "sparse-pyramidal"
            )
        assignment_summaries = {
            mode: _assignment_summary(
                value,
                include_prior_audits=not args.compact,
            )
            for mode, value in assignments_by_mode.items()
        }
        assignment = assignments_by_mode[args.prior_mode]
        assignment_summary = assignment_summaries[args.prior_mode]
        comparison = (
            _prior_mode_comparison(
                replay,
                args.object_kind,
                assignments_by_mode["constant"],
                assignments_by_mode["last-two"],
                assignment_summaries["constant"],
                assignment_summaries["last-two"],
            )
            if args.prior_mode == "last-two"
            else (
                _all_mode_comparison(
                    replay,
                    args.object_kind,
                    assignments_by_mode,
                    assignment_summaries,
                )
                if sparse_requested
                else None
            )
        )
        sparse_flow_audit = (
            _sparse_flow_audit(sparse_flow_sequence, sparse_factory)
            if sparse_requested
            else None
        )
        shadow_runtime = time.perf_counter() - shadow_started
        report.update({
            "selected_sequence": sequence.to_dict(),
            "normalized_raster_replay_view": raster_view.to_dict(),
            "detector_config": detector_policy,
            "assignment_policy": {
                "status": "exploratory_uncalibrated_not_operational",
                "selected_prior_mode": args.prior_mode,
                "policy_ids_by_mode": policy_ids,
                "cost_model_id": (
                    assignment.cost_model_ids[0]
                    if len(assignment.cost_model_ids) == 1
                    else None
                ),
                "cost_model_ids_by_mode": {
                    mode: list(value.cost_model_ids)
                    for mode, value in assignments_by_mode.items()
                },
                "unavailable_motion_cost_policy": (
                    "zero_weight_keep_complete_feature_and_candidate"
                ),
                "prior_vx_m_s": args.prior_vx_m_s,
                "prior_vy_m_s": args.prior_vy_m_s,
                "velocity_sigma_m_s": args.velocity_sigma_m_s,
                "residual_position_sigma_m":
                    args.residual_position_sigma_m,
                "last_two_process_position_sigma_m":
                    args.last_two_process_position_sigma_m,
                "last_two_process_velocity_sigma_m_s":
                    args.last_two_process_velocity_sigma_m_s,
                "last_two_speed_cap_kmh": None,
                "last_two_smoothing": False,
                "last_two_candidate_rejection": False,
                "motion_cost_weight": args.motion_cost_weight,
                "area_cost_weight": args.area_cost_weight,
                "nonoverlap_cost_weight": args.nonoverlap_cost_weight,
                "death_cost": args.death_cost,
                "birth_cost": args.birth_cost,
                "solution_limit": args.solution_limit,
                "maximum_top_k_detections":
                    args.maximum_top_k_detections,
                "maximum_complete_edges": args.maximum_complete_edges,
                "ambiguity_cost_delta": args.ambiguity_cost_delta,
                "jump_speed_kmh": args.jump_speed_kmh,
                "sparse_pyramidal": sparse_policy,
            },
            "hierarchical_replay": replay.to_dict(),
            "shadow_assignment": assignment_summary,
            "shadow_assignments": assignment_summaries,
            "shadow_assignment_comparison": comparison,
            "sparse_pyramidal_flow_audit": sparse_flow_audit,
            "runtime_seconds": {
                "raster_view": view_runtime,
                "hierarchical_detection": detection_runtime,
                "sparse_flow": sparse_flow_runtime,
                "assignment_by_mode": assignment_runtime,
                "shadow_total": shadow_runtime,
            },
            "scientific_limits": [
                "not_historical_live_identity_replay",
                "identity_switches_and_hota_unavailable_without_truth",
                "normalized_cache_is_not_raw_provider_data",
                "cache_has_no_provider_nodata_mask",
                "flow_covariance_and_costs_are_caller_supplied",
                "last_two_recursively_depends_on_earlier_assignments",
                "prior_comparison_is_not_ground_truth_validation",
                (
                    "sparse_flow_is_bounded_downscaled_analysis_view_"
                    "not_native_full_resolution"
                    if (
                        sparse_requested
                        and not raster_view.transform.
                        lossless_cache_value_expansion
                    )
                    else "native_cache_value_expansion_used"
                ),
                "no_algorithm_is_declared_correct_without_external_truth",
            ],
        })
    after_sha256 = _sha256(database)
    report["database_sha256_after"] = after_sha256
    report["database_unchanged"] = before_sha256 == after_sha256
    if not report["database_unchanged"]:
        raise RuntimeError("databasehash veranderde tijdens read-only analyse")
    text = json.dumps(
        report,
        separators=(",", ":") if args.compact else None,
        indent=None if args.compact else 2,
        sort_keys=True,
    )
    if args.output is None:
        print(text)
    else:
        destination = args.output.expanduser().resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text + "\n", encoding="utf-8")
        print(destination)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
