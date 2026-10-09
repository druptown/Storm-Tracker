"""Fail-closed shadow adapter for the real pySTEPS STEPS and LINDA methods.

This module contains no implementation or approximation of STEPS or LINDA.
When an exact, caller-pinned pySTEPS runtime is available it invokes
``pysteps.nowcasts.steps.forecast`` or ``pysteps.nowcasts.linda.forecast``.
Otherwise it returns an ineligible, output-free result.

The module is pure at import time: NumPy and pySTEPS are loaded only inside a
shadow run.  Nothing here is wired to Home Assistant state or worker admission.
"""
from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import json
import math
import platform
import warnings
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Mapping

from .analysis_grid import AnalysisGrid
from .nowcast_baselines import (
    EULERIAN_PERSISTENCE_VERSION,
    TWO_FRAME_TRANSLATION_VERSION,
    MetricRasterField,
)


PYSTEPS_CHALLENGER_CONTRACT_VERSION = "stv3-pysteps-shadow-challenger-v1"
PYSTEPS_STEPS_ENTRYPOINT = "pysteps.nowcasts.steps.forecast"
PYSTEPS_LINDA_ENTRYPOINT = "pysteps.nowcasts.linda.forecast"
PYSTEPS_DB_TRANSFORM_ENTRYPOINT = "pysteps.utils.transformation.dB_transform"
_TIME_TOLERANCE_SECONDS = 1e-6
_SHA256_LENGTH = 64


class PystepsAlgorithm(str, Enum):
    STEPS = "pysteps_steps"
    LINDA = "pysteps_linda"


class ShadowPartition(str, Enum):
    DEVELOPMENT = "development"
    HELD_OUT = "held_out"


class ChallengerRunStatus(str, Enum):
    COMPLETED = "completed"
    RUNTIME_UNAVAILABLE = "runtime_unavailable"
    RUNTIME_VERSION_MISMATCH = "runtime_version_mismatch"
    INPUT_REJECTED = "input_rejected"
    RESOURCE_REJECTED = "resource_rejected"
    EXECUTION_FAILED = "execution_failed"


class MetricDirection(str, Enum):
    LOWER_IS_BETTER = "lower_is_better"
    HIGHER_IS_BETTER = "higher_is_better"


class MetricGateStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True, slots=True)
class PystepsFieldProvenance:
    """Immutable origin and normalization chain for one physical input field."""

    field_id: str
    provider_id: str
    product_id: str
    observation_id: str
    source_artifact_sha256: str
    decoder_version: str
    quantity_derivation_version: str
    quality_control_version: str
    reprojection_version: str

    def __post_init__(self) -> None:
        for label, value in (
            ("field_id", self.field_id),
            ("provider_id", self.provider_id),
            ("product_id", self.product_id),
            ("observation_id", self.observation_id),
            ("decoder_version", self.decoder_version),
            ("quantity_derivation_version", self.quantity_derivation_version),
            ("quality_control_version", self.quality_control_version),
            ("reprojection_version", self.reprojection_version),
        ):
            _identifier(value, label)
        _sha256(self.source_artifact_sha256, "source_artifact_sha256")


@dataclass(frozen=True, slots=True)
class DenseMotionField:
    """Dense physical motion on the same metric analysis grid as the rain."""

    motion_id: str
    grid: AnalysisGrid
    reference_valid_at: float
    vx_m_s: tuple[tuple[float, ...], ...]
    vy_m_s: tuple[tuple[float, ...], ...]
    input_field_ids: tuple[str, ...]
    algorithm_version: str
    algorithm_config_sha256: str
    implementation_sha256: str
    quality_control_version: str

    def __post_init__(self) -> None:
        _identifier(self.motion_id, "motion_id")
        _identifier(self.algorithm_version, "algorithm_version")
        _identifier(self.quality_control_version, "quality_control_version")
        _sha256(self.algorithm_config_sha256, "algorithm_config_sha256")
        _sha256(self.implementation_sha256, "implementation_sha256")
        if not isinstance(self.grid, AnalysisGrid):
            raise TypeError("motion grid must be AnalysisGrid")
        _finite(self.reference_valid_at, "reference_valid_at")
        _matrix(self.vx_m_s, self.grid, "vx_m_s")
        _matrix(self.vy_m_s, self.grid, "vy_m_s")
        for name, matrix in (("vx_m_s", self.vx_m_s), ("vy_m_s", self.vy_m_s)):
            if any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                for row in matrix
                for value in row
            ):
                raise ValueError(f"{name} must contain finite physical velocity")
        _unique_ids(self.input_field_ids, "motion input_field_ids")


@dataclass(frozen=True, slots=True)
class PystepsChallengerConfig:
    """Fully explicit algorithm, RNG and resource contract for one run."""

    algorithm: PystepsAlgorithm
    expected_pysteps_version: str
    seed: int
    lead_steps: tuple[float, ...]
    ensemble_members: int
    num_workers: int = 1
    rain_threshold_mm_h: float = 0.1
    max_grid_cells: int = 160_000
    max_output_values: int = 20_000_000
    steps_ar_order: int = 2
    steps_cascade_levels: int = 6
    steps_zerovalue_db: float = -15.0
    steps_noise_method: str = "nonparametric"
    steps_velocity_perturbation: str | None = "bps"
    steps_mask_method: str | None = "incremental"
    steps_fft_method: str = "numpy"
    linda_ari_order: int = 1
    linda_feature_method: str = "blob"
    linda_max_num_features: int = 25
    linda_kernel_type: str = "anisotropic"
    linda_add_perturbations: bool = True
    linda_perturbation_thresholds_mm_h: tuple[float, float] = (0.5, 1.0)
    linda_velocity_perturbation: str | None = "bps"
    contract_version: str = PYSTEPS_CHALLENGER_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.algorithm, PystepsAlgorithm):
            raise TypeError("algorithm must be PystepsAlgorithm")
        _identifier(self.expected_pysteps_version, "expected_pysteps_version")
        _identifier(self.contract_version, "contract_version")
        if (
            isinstance(self.seed, bool)
            or not isinstance(self.seed, int)
            or not 0 <= self.seed <= 2**32 - 1
        ):
            raise ValueError("seed must be an explicit unsigned 32-bit integer")
        if (
            not isinstance(self.lead_steps, tuple)
            or not self.lead_steps
            or any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0.0
                for value in self.lead_steps
            )
            or any(
                right <= left
                for left, right in zip(self.lead_steps, self.lead_steps[1:])
            )
        ):
            raise ValueError("lead_steps must be finite, positive and increasing")
        _positive_int(self.ensemble_members, "ensemble_members", minimum=2)
        if self.num_workers != 1:
            raise ValueError(
                "shadow reproducibility contract currently requires num_workers=1"
            )
        _positive(self.rain_threshold_mm_h, "rain_threshold_mm_h")
        _positive_int(self.max_grid_cells, "max_grid_cells")
        _positive_int(self.max_output_values, "max_output_values")
        if self.steps_ar_order != 2:
            raise ValueError("STEPS shadow contract is preregistered for AR(2)")
        _positive_int(self.steps_cascade_levels, "steps_cascade_levels", minimum=2)
        _finite(self.steps_zerovalue_db, "steps_zerovalue_db")
        if self.steps_zerovalue_db >= 10.0 * math.log10(
            self.rain_threshold_mm_h
        ):
            raise ValueError("STEPS zerovalue must lie below its rain threshold")
        if self.steps_noise_method not in {
            "parametric",
            "nonparametric",
            "ssft",
            "nested",
        }:
            raise ValueError("unsupported explicit STEPS noise method")
        if self.steps_velocity_perturbation not in {"bps", None}:
            raise ValueError("unsupported STEPS velocity perturbation")
        if self.steps_mask_method not in {
            "obs",
            "sprog",
            "incremental",
            None,
        }:
            raise ValueError("unsupported STEPS mask method")
        if self.steps_fft_method not in {"numpy", "pyfftw"}:
            raise ValueError("unsupported explicit STEPS FFT method")
        if self.linda_ari_order not in {1, 2}:
            raise ValueError("LINDA ARI order must be 1 or 2")
        if self.linda_feature_method not in {"blob", "domain", "shitomasi"}:
            raise ValueError("unsupported LINDA feature method")
        _positive_int(
            self.linda_max_num_features,
            "linda_max_num_features",
        )
        if self.linda_kernel_type not in {"anisotropic", "isotropic"}:
            raise ValueError("unsupported LINDA kernel type")
        if self.linda_add_perturbations is not True:
            raise ValueError(
                "ensemble challenger requires LINDA perturbations enabled"
            )
        if (
            not isinstance(self.linda_perturbation_thresholds_mm_h, tuple)
            or len(self.linda_perturbation_thresholds_mm_h) != 2
        ):
            raise ValueError("LINDA perturbation thresholds require two values")
        for value in self.linda_perturbation_thresholds_mm_h:
            _positive(value, "LINDA perturbation threshold")
        if (
            self.linda_perturbation_thresholds_mm_h[1]
            <= self.linda_perturbation_thresholds_mm_h[0]
        ):
            raise ValueError("LINDA perturbation thresholds must increase")
        if self.linda_velocity_perturbation not in {"bps", None}:
            raise ValueError("unsupported LINDA velocity perturbation")

    @property
    def required_input_frames(self) -> int:
        if self.algorithm is PystepsAlgorithm.STEPS:
            return self.steps_ar_order + 1
        return self.linda_ari_order + 2

    @property
    def entrypoint(self) -> str:
        if self.algorithm is PystepsAlgorithm.STEPS:
            return PYSTEPS_STEPS_ENTRYPOINT
        return PYSTEPS_LINDA_ENTRYPOINT


@dataclass(frozen=True, slots=True)
class PystepsShadowRequest:
    run_id: str
    event_id: str
    partition: ShadowPartition
    fields: tuple[MetricRasterField, ...]
    field_provenance: tuple[PystepsFieldProvenance, ...]
    motion: DenseMotionField
    config: PystepsChallengerConfig

    def __post_init__(self) -> None:
        _identifier(self.run_id, "run_id")
        _identifier(self.event_id, "event_id")
        if not isinstance(self.partition, ShadowPartition):
            raise TypeError("partition must be ShadowPartition")
        if not isinstance(self.config, PystepsChallengerConfig):
            raise TypeError("config must be PystepsChallengerConfig")
        if (
            not isinstance(self.fields, tuple)
            or len(self.fields) != self.config.required_input_frames
            or any(not isinstance(field, MetricRasterField) for field in self.fields)
        ):
            raise ValueError(
                "request requires the exact algorithm-specific input frame count"
            )
        if len({field.field_id for field in self.fields}) != len(self.fields):
            raise ValueError("input field IDs must be unique")
        if (
            not isinstance(self.field_provenance, tuple)
            or any(
                not isinstance(item, PystepsFieldProvenance)
                for item in self.field_provenance
            )
            or tuple(item.field_id for item in self.field_provenance)
            != tuple(field.field_id for field in self.fields)
        ):
            raise ValueError(
                "field provenance must bind every input field in exact order"
            )
        first = self.fields[0]
        for field in self.fields:
            first.grid.assert_same_geometry(field.grid)
            if field.quantity != "rain_rate" or field.unit != "mm h-1":
                raise ValueError(
                    "pySTEPS challenger requires linear rain_rate in mm h-1"
                )
            if not all(all(row) for row in field.valid_mask):
                raise ValueError(
                    "pySTEPS challenger refuses incomplete/nodata domains"
                )
            if any(value < 0.0 for row in field.values for value in row):
                raise ValueError("rain-rate inputs cannot be negative")
        timestamps = tuple(field.valid_at for field in self.fields)
        if any(right <= left for left, right in zip(timestamps, timestamps[1:])):
            raise ValueError("input timestamps must strictly increase")
        intervals = tuple(
            right - left for left, right in zip(timestamps, timestamps[1:])
        )
        if any(
            not math.isclose(
                interval,
                intervals[0],
                rel_tol=0.0,
                abs_tol=_TIME_TOLERANCE_SECONDS,
            )
            for interval in intervals[1:]
        ):
            raise ValueError("pySTEPS input frames must be regularly spaced")
        if not isinstance(self.motion, DenseMotionField):
            raise TypeError("motion must be DenseMotionField")
        first.grid.assert_same_geometry(self.motion.grid)
        if not math.isclose(
            self.motion.reference_valid_at,
            self.fields[-1].valid_at,
            rel_tol=0.0,
            abs_tol=_TIME_TOLERANCE_SECONDS,
        ):
            raise ValueError("motion reference must equal newest field time")
        if self.motion.input_field_ids != tuple(
            field.field_id for field in self.fields
        ):
            raise ValueError("motion provenance must bind the exact input sequence")

    @property
    def timestep_seconds(self) -> float:
        return self.fields[1].valid_at - self.fields[0].valid_at


@dataclass(frozen=True, slots=True)
class PystepsRuntime:
    """Resolved runtime. Test doubles are explicit and never skill-eligible."""

    pysteps_version: str
    package_versions: tuple[tuple[str, str], ...]
    numpy: object
    steps_forecast: Callable
    linda_forecast: Callable
    db_transform: Callable
    backend_kind: str
    python_version: str
    python_implementation: str
    platform_system: str
    platform_machine: str

    def __post_init__(self) -> None:
        _identifier(self.pysteps_version, "pysteps_version")
        for label, value in (
            ("python_version", self.python_version),
            ("python_implementation", self.python_implementation),
            ("platform_system", self.platform_system),
            ("platform_machine", self.platform_machine),
        ):
            _identifier(value, label)
        if self.backend_kind not in {"official_pysteps", "contract_test_double"}:
            raise ValueError("unknown pySTEPS runtime kind")
        if any(
            not callable(value)
            for value in (
                self.steps_forecast,
                self.linda_forecast,
                self.db_transform,
            )
        ):
            raise TypeError("pySTEPS runtime entrypoints must be callable")
        if (
            not isinstance(self.package_versions, tuple)
            or any(
                not isinstance(item, tuple)
                or len(item) != 2
                or not all(isinstance(value, str) and value for value in item)
                for item in self.package_versions
            )
        ):
            raise ValueError("package_versions must be immutable name/version pairs")
        if len({name for name, _version in self.package_versions}) != len(
            self.package_versions
        ):
            raise ValueError("package_versions cannot contain duplicate packages")
        if self.package_versions != tuple(sorted(self.package_versions)):
            raise ValueError("package_versions must be sorted reproducibly")
        if self.official:
            installed = dict(self.package_versions)
            if (
                installed.get("pysteps") != self.pysteps_version
                or "numpy" not in installed
            ):
                raise ValueError(
                    "official runtime provenance requires pySTEPS and NumPy versions"
                )

    @property
    def official(self) -> bool:
        return self.backend_kind == "official_pysteps"


@dataclass(frozen=True, slots=True)
class PystepsEnsembleForecast:
    ensemble_id: str
    algorithm: PystepsAlgorithm
    grid: AnalysisGrid
    reference_valid_at: float
    timestep_seconds: float
    lead_steps: tuple[float, ...]
    member_ids: tuple[str, ...]
    values_mm_h: tuple[
        tuple[tuple[tuple[float, ...], ...], ...],
        ...,
    ]
    output_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.ensemble_id, "ensemble_id")
        if not isinstance(self.algorithm, PystepsAlgorithm):
            raise TypeError("forecast algorithm must be PystepsAlgorithm")
        if not isinstance(self.grid, AnalysisGrid):
            raise TypeError("forecast grid must be AnalysisGrid")
        _finite(self.reference_valid_at, "reference_valid_at")
        _positive(self.timestep_seconds, "timestep_seconds")
        _unique_ids(self.member_ids, "member_ids")
        if len(self.member_ids) < 2 or len(self.values_mm_h) != len(
            self.member_ids
        ):
            raise ValueError("ensemble member axis mismatch")
        if any(len(member) != len(self.lead_steps) for member in self.values_mm_h):
            raise ValueError("ensemble lead-time axis mismatch")
        for member in self.values_mm_h:
            for field in member:
                _matrix(field, self.grid, "ensemble field")
                if any(
                    not math.isfinite(value) or value < 0.0
                    for row in field
                    for value in row
                ):
                    raise ValueError(
                        "ensemble rain-rate values must be finite and nonnegative"
                    )
        _sha256(self.output_sha256, "output_sha256")


@dataclass(frozen=True, slots=True)
class PystepsChallengerResult:
    run_id: str
    event_id: str
    partition: ShadowPartition
    status: ChallengerRunStatus
    algorithm: PystepsAlgorithm
    entrypoint: str
    request_sha256: str
    config_sha256: str
    seed: int
    seed_sha256: str
    expected_pysteps_version: str
    actual_pysteps_version: str | None
    runtime_fingerprint_sha256: str | None
    genuine_pysteps_runtime: bool
    skill_evidence_eligible: bool
    failure_code: str | None
    failure_class: str | None
    warning_classes: tuple[str, ...]
    forecast: PystepsEnsembleForecast | None
    contract_version: str = PYSTEPS_CHALLENGER_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _identifier(self.run_id, "run_id")
        _identifier(self.event_id, "event_id")
        if not isinstance(self.partition, ShadowPartition):
            raise TypeError("result partition must be ShadowPartition")
        if not isinstance(self.status, ChallengerRunStatus):
            raise TypeError("result status must be ChallengerRunStatus")
        if not isinstance(self.algorithm, PystepsAlgorithm):
            raise TypeError("result algorithm must be PystepsAlgorithm")
        _identifier(self.entrypoint, "entrypoint")
        _sha256(self.request_sha256, "request_sha256")
        _sha256(self.config_sha256, "config_sha256")
        _sha256(self.seed_sha256, "seed_sha256")
        if self.runtime_fingerprint_sha256 is not None:
            _sha256(
                self.runtime_fingerprint_sha256,
                "runtime_fingerprint_sha256",
            )
        if self.status is ChallengerRunStatus.COMPLETED:
            if self.forecast is None or self.failure_code is not None:
                raise ValueError("completed challenger requires only forecast output")
        elif self.forecast is not None:
            raise ValueError("failed/unavailable challenger cannot carry output")
        if self.skill_evidence_eligible and (
            self.status is not ChallengerRunStatus.COMPLETED
            or not self.genuine_pysteps_runtime
            or self.partition is not ShadowPartition.HELD_OUT
        ):
            raise ValueError(
                "only genuine completed held-out pySTEPS runs can be eligible"
            )


@dataclass(frozen=True, slots=True)
class MetricRequirement:
    metric_id: str
    direction: MetricDirection
    allowed_degradation: float

    def __post_init__(self) -> None:
        _identifier(self.metric_id, "metric_id")
        if not isinstance(self.direction, MetricDirection):
            raise TypeError("metric direction must be MetricDirection")
        if (
            not isinstance(self.allowed_degradation, (int, float))
            or isinstance(self.allowed_degradation, bool)
            or not math.isfinite(self.allowed_degradation)
            or self.allowed_degradation < 0.0
        ):
            raise ValueError("allowed_degradation must be explicit and nonnegative")


@dataclass(frozen=True, slots=True)
class HeldOutMetricGateConfig:
    gate_id: str
    split_manifest_sha256: str
    frozen_candidate_config_sha256: str
    allowed_runtime_fingerprint_sha256: tuple[str, ...]
    baseline_algorithm_versions: tuple[str, ...]
    verification_algorithm_versions: tuple[str, ...]
    verification_config_sha256: str
    minimum_independent_events: int
    requirements: tuple[MetricRequirement, ...]

    def __post_init__(self) -> None:
        _identifier(self.gate_id, "gate_id")
        _sha256(self.split_manifest_sha256, "split_manifest_sha256")
        _sha256(
            self.frozen_candidate_config_sha256,
            "frozen_candidate_config_sha256",
        )
        if (
            not isinstance(self.allowed_runtime_fingerprint_sha256, tuple)
            or not self.allowed_runtime_fingerprint_sha256
        ):
            raise ValueError("metric gate requires allowed runtime fingerprints")
        for item in self.allowed_runtime_fingerprint_sha256:
            _sha256(item, "allowed_runtime_fingerprint_sha256")
        if len(set(self.allowed_runtime_fingerprint_sha256)) != len(
            self.allowed_runtime_fingerprint_sha256
        ):
            raise ValueError("allowed runtime fingerprints must be unique")
        _unique_ids(
            self.baseline_algorithm_versions,
            "baseline_algorithm_versions",
        )
        _unique_ids(
            self.verification_algorithm_versions,
            "verification_algorithm_versions",
        )
        _sha256(self.verification_config_sha256, "verification_config_sha256")
        _positive_int(
            self.minimum_independent_events,
            "minimum_independent_events",
            minimum=2,
        )
        if (
            not isinstance(self.requirements, tuple)
            or not self.requirements
            or any(
                not isinstance(item, MetricRequirement)
                for item in self.requirements
            )
            or len({item.metric_id for item in self.requirements})
            != len(self.requirements)
        ):
            raise ValueError("metric gate requires unique typed requirements")


@dataclass(frozen=True, slots=True)
class HeldOutMetricEvidence:
    metric_id: str
    partition: ShadowPartition
    split_manifest_sha256: str
    candidate_config_sha256: str
    runtime_fingerprint_sha256: str
    baseline_algorithm_version: str
    verification_algorithm_version: str
    verification_config_sha256: str
    independent_event_count: int
    candidate_minus_baseline_point: float
    candidate_minus_baseline_ci_lower: float
    candidate_minus_baseline_ci_upper: float
    genuine_pysteps_runtime: bool

    def __post_init__(self) -> None:
        for label, value in (
            ("metric_id", self.metric_id),
            ("baseline_algorithm_version", self.baseline_algorithm_version),
            ("verification_algorithm_version", self.verification_algorithm_version),
        ):
            _identifier(value, label)
        if not isinstance(self.partition, ShadowPartition):
            raise TypeError("metric evidence partition must be ShadowPartition")
        _sha256(self.split_manifest_sha256, "split_manifest_sha256")
        _sha256(self.candidate_config_sha256, "candidate_config_sha256")
        _sha256(
            self.runtime_fingerprint_sha256,
            "runtime_fingerprint_sha256",
        )
        _sha256(self.verification_config_sha256, "verification_config_sha256")
        _positive_int(
            self.independent_event_count,
            "independent_event_count",
        )
        for label, value in (
            ("candidate_minus_baseline_point", self.candidate_minus_baseline_point),
            (
                "candidate_minus_baseline_ci_lower",
                self.candidate_minus_baseline_ci_lower,
            ),
            (
                "candidate_minus_baseline_ci_upper",
                self.candidate_minus_baseline_ci_upper,
            ),
        ):
            _finite(value, label)
        if (
            self.candidate_minus_baseline_ci_lower
            > self.candidate_minus_baseline_ci_upper
            or not self.candidate_minus_baseline_ci_lower
            <= self.candidate_minus_baseline_point
            <= self.candidate_minus_baseline_ci_upper
        ):
            raise ValueError("metric point must lie inside ordered confidence bounds")
        if not isinstance(self.genuine_pysteps_runtime, bool):
            raise TypeError("genuine_pysteps_runtime must be boolean")


@dataclass(frozen=True, slots=True)
class HeldOutMetricGateDecision:
    gate_id: str
    status: MetricGateStatus
    passed_metric_ids: tuple[str, ...]
    failed_metric_ids: tuple[str, ...]
    indeterminate_reasons: tuple[str, ...]


def load_official_pysteps_runtime() -> PystepsRuntime:
    """Load only official module entrypoints and record dependency versions."""

    pysteps = importlib.import_module("pysteps")
    numpy = importlib.import_module("numpy")
    steps_module = importlib.import_module("pysteps.nowcasts.steps")
    linda_module = importlib.import_module("pysteps.nowcasts.linda")
    transform_module = importlib.import_module("pysteps.utils.transformation")
    steps_forecast = getattr(steps_module, "forecast")
    linda_forecast = getattr(linda_module, "forecast")
    db_transform = getattr(transform_module, "dB_transform")
    if (
        getattr(steps_forecast, "__module__", "") != "pysteps.nowcasts.steps"
        or getattr(linda_forecast, "__module__", "") != "pysteps.nowcasts.linda"
        or getattr(db_transform, "__module__", "")
        != "pysteps.utils.transformation"
    ):
        raise RuntimeError("resolved pySTEPS entrypoint identity mismatch")
    packages = []
    for name in (
        "pysteps",
        "numpy",
        "scipy",
        "scikit-image",
        "opencv-python",
        "opencv-python-headless",
        "dask",
        "pyfftw",
        "jsonschema",
        "matplotlib",
        "netCDF4",
        "pillow",
        "pyproj",
    ):
        try:
            packages.append((name, importlib.metadata.version(name)))
        except importlib.metadata.PackageNotFoundError:
            continue
    version = str(getattr(pysteps, "__version__", "")).strip()
    return PystepsRuntime(
        pysteps_version=version,
        package_versions=tuple(sorted(packages)),
        numpy=numpy,
        steps_forecast=steps_forecast,
        linda_forecast=linda_forecast,
        db_transform=db_transform,
        backend_kind="official_pysteps",
        python_version=platform.python_version(),
        python_implementation=platform.python_implementation(),
        platform_system=platform.system() or "unknown",
        platform_machine=platform.machine() or "unknown",
    )


def run_pysteps_challenger(
    request: PystepsShadowRequest,
    *,
    runtime_loader: Callable[[], PystepsRuntime] = load_official_pysteps_runtime,
) -> PystepsChallengerResult:
    """Run a genuine method when available, otherwise return no forecast."""

    if not isinstance(request, PystepsShadowRequest):
        raise TypeError("request must be PystepsShadowRequest")
    request_sha = _request_sha256(request)
    config_sha = _config_sha256(request.config)
    seed_sha = _sha256_text(str(request.config.seed))
    common = {
        "run_id": request.run_id,
        "event_id": request.event_id,
        "partition": request.partition,
        "algorithm": request.config.algorithm,
        "entrypoint": request.config.entrypoint,
        "request_sha256": request_sha,
        "config_sha256": config_sha,
        "seed": request.config.seed,
        "seed_sha256": seed_sha,
        "expected_pysteps_version": request.config.expected_pysteps_version,
    }
    cell_count = request.fields[0].grid.width * request.fields[0].grid.height
    output_values = (
        cell_count
        * len(request.config.lead_steps)
        * request.config.ensemble_members
    )
    if (
        cell_count > request.config.max_grid_cells
        or output_values > request.config.max_output_values
    ):
        return _failed_result(
            common,
            ChallengerRunStatus.RESOURCE_REJECTED,
            failure_code="explicit_resource_contract_exceeded",
        )
    if not math.isclose(
        request.fields[0].grid.x_resolution_m,
        request.fields[0].grid.y_resolution_m,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        return _failed_result(
            common,
            ChallengerRunStatus.INPUT_REJECTED,
            failure_code="pysteps_requires_scalar_isotropic_kmperpixel",
        )
    try:
        runtime = runtime_loader()
    except (ImportError, ModuleNotFoundError) as err:
        return _failed_result(
            common,
            ChallengerRunStatus.RUNTIME_UNAVAILABLE,
            failure_code="pysteps_runtime_unavailable",
            failure_class=type(err).__name__,
        )
    except Exception as err:
        return _failed_result(
            common,
            ChallengerRunStatus.EXECUTION_FAILED,
            failure_code="pysteps_runtime_resolution_failed",
            failure_class=type(err).__name__,
        )
    if not isinstance(runtime, PystepsRuntime):
        return _failed_result(
            common,
            ChallengerRunStatus.EXECUTION_FAILED,
            failure_code="invalid_runtime_loader_contract",
            failure_class=type(runtime).__name__,
        )
    runtime_fingerprint = _runtime_fingerprint(runtime)
    if runtime.pysteps_version != request.config.expected_pysteps_version:
        return _failed_result(
            common,
            ChallengerRunStatus.RUNTIME_VERSION_MISMATCH,
            failure_code="exact_pysteps_version_mismatch",
            actual_pysteps_version=runtime.pysteps_version,
            runtime_fingerprint_sha256=runtime_fingerprint,
            genuine_pysteps_runtime=runtime.official,
        )

    try:
        forecast, warning_classes = _execute(request, runtime)
    except Exception as err:
        return _failed_result(
            common,
            ChallengerRunStatus.EXECUTION_FAILED,
            failure_code="pysteps_execution_failed",
            failure_class=type(err).__name__,
            actual_pysteps_version=runtime.pysteps_version,
            runtime_fingerprint_sha256=runtime_fingerprint,
            genuine_pysteps_runtime=runtime.official,
        )
    if warning_classes:
        return _failed_result(
            common,
            ChallengerRunStatus.EXECUTION_FAILED,
            failure_code="pysteps_runtime_warning_fail_closed",
            actual_pysteps_version=runtime.pysteps_version,
            runtime_fingerprint_sha256=runtime_fingerprint,
            genuine_pysteps_runtime=runtime.official,
            warning_classes=warning_classes,
        )
    return PystepsChallengerResult(
        **common,
        status=ChallengerRunStatus.COMPLETED,
        actual_pysteps_version=runtime.pysteps_version,
        runtime_fingerprint_sha256=runtime_fingerprint,
        genuine_pysteps_runtime=runtime.official,
        skill_evidence_eligible=(
            runtime.official
            and request.partition is ShadowPartition.HELD_OUT
        ),
        failure_code=None,
        failure_class=None,
        warning_classes=(),
        forecast=forecast,
    )


def evaluate_held_out_metric_gate(
    config: HeldOutMetricGateConfig,
    evidence: tuple[HeldOutMetricEvidence, ...],
) -> HeldOutMetricGateDecision:
    """Apply preregistered confidence-bound gates without inventing defaults."""

    if not isinstance(config, HeldOutMetricGateConfig):
        raise TypeError("config must be HeldOutMetricGateConfig")
    if (
        not isinstance(evidence, tuple)
        or any(not isinstance(item, HeldOutMetricEvidence) for item in evidence)
    ):
        raise TypeError("evidence must be typed immutable metric evidence")
    by_id = {item.metric_id: item for item in evidence}
    if len(by_id) != len(evidence):
        raise ValueError("metric evidence IDs must be unique")
    required = {item.metric_id for item in config.requirements}
    reasons: list[str] = []
    missing = sorted(required - set(by_id))
    if missing:
        reasons.append("missing_metrics:" + ",".join(missing))
    passed: list[str] = []
    failed: list[str] = []
    for requirement in config.requirements:
        item = by_id.get(requirement.metric_id)
        if item is None:
            continue
        if item.partition is not ShadowPartition.HELD_OUT:
            reasons.append(f"{item.metric_id}:not_held_out")
            continue
        if (
            item.split_manifest_sha256 != config.split_manifest_sha256
            or item.candidate_config_sha256
            != config.frozen_candidate_config_sha256
        ):
            reasons.append(f"{item.metric_id}:provenance_mismatch")
            continue
        if (
            item.runtime_fingerprint_sha256
            not in config.allowed_runtime_fingerprint_sha256
        ):
            reasons.append(f"{item.metric_id}:runtime_not_preregistered")
            continue
        if (
            item.baseline_algorithm_version
            not in config.baseline_algorithm_versions
        ):
            reasons.append(f"{item.metric_id}:baseline_not_preregistered")
            continue
        if (
            item.verification_algorithm_version
            not in config.verification_algorithm_versions
            or item.verification_config_sha256
            != config.verification_config_sha256
        ):
            reasons.append(f"{item.metric_id}:verification_not_preregistered")
            continue
        if item.independent_event_count < config.minimum_independent_events:
            reasons.append(f"{item.metric_id}:insufficient_independent_events")
            continue
        if not item.genuine_pysteps_runtime:
            reasons.append(f"{item.metric_id}:not_genuine_pysteps_runtime")
            continue
        if requirement.direction is MetricDirection.LOWER_IS_BETTER:
            metric_passed = (
                item.candidate_minus_baseline_ci_upper
                <= requirement.allowed_degradation
            )
        else:
            metric_passed = (
                item.candidate_minus_baseline_ci_lower
                >= -requirement.allowed_degradation
            )
        (passed if metric_passed else failed).append(item.metric_id)
    if reasons:
        status = MetricGateStatus.INDETERMINATE
    elif failed:
        status = MetricGateStatus.FAILED
    elif len(passed) == len(config.requirements):
        status = MetricGateStatus.PASSED
    else:
        status = MetricGateStatus.INDETERMINATE
        reasons.append("incomplete_metric_decision")
    return HeldOutMetricGateDecision(
        gate_id=config.gate_id,
        status=status,
        passed_metric_ids=tuple(sorted(passed)),
        failed_metric_ids=tuple(sorted(failed)),
        indeterminate_reasons=tuple(sorted(reasons)),
    )


def default_baseline_versions() -> tuple[str, str]:
    """Return the two existing deterministic comparison versions."""

    return (
        EULERIAN_PERSISTENCE_VERSION,
        TWO_FRAME_TRANSLATION_VERSION,
    )


def _execute(
    request: PystepsShadowRequest,
    runtime: PystepsRuntime,
) -> tuple[PystepsEnsembleForecast, tuple[str, ...]]:
    np = runtime.numpy
    config = request.config
    rain = np.asarray(
        [field.values for field in request.fields],
        dtype=float,
    )
    timestep = request.timestep_seconds
    grid = request.fields[0].grid
    velocity = np.stack(
        (
            np.asarray(request.motion.vx_m_s, dtype=float)
            * timestep
            / grid.x_resolution_m,
            -np.asarray(request.motion.vy_m_s, dtype=float)
            * timestep
            / grid.y_resolution_m,
        )
    )
    if not bool(np.all(np.isfinite(velocity))):
        raise ValueError("converted pySTEPS velocity is not finite")
    kmperpixel = grid.x_resolution_m / 1000.0
    timestep_minutes = timestep / 60.0
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        if config.algorithm is PystepsAlgorithm.STEPS:
            metadata = {
                "unit": "mm/h",
                "transform": None,
                "threshold": config.rain_threshold_mm_h,
                "zerovalue": 0.0,
            }
            transformed, transformed_metadata = runtime.db_transform(
                rain,
                metadata,
                threshold=config.rain_threshold_mm_h,
                zerovalue=config.steps_zerovalue_db,
            )
            if not bool(np.all(np.isfinite(transformed))):
                raise ValueError("STEPS dB input transformation is not finite")
            threshold_db = float(transformed_metadata["threshold"])
            raw = runtime.steps_forecast(
                transformed,
                velocity,
                list(config.lead_steps),
                n_ens_members=config.ensemble_members,
                n_cascade_levels=config.steps_cascade_levels,
                precip_thr=threshold_db,
                kmperpixel=kmperpixel,
                timestep=timestep_minutes,
                noise_method=config.steps_noise_method,
                vel_pert_method=config.steps_velocity_perturbation,
                mask_method=config.steps_mask_method,
                seed=config.seed,
                num_workers=config.num_workers,
                fft_method=config.steps_fft_method,
                return_output=True,
            )
            linear, _ = runtime.db_transform(
                raw,
                transformed_metadata,
                inverse=True,
            )
        else:
            linear = runtime.linda_forecast(
                rain,
                velocity,
                list(config.lead_steps),
                feature_method=config.linda_feature_method,
                max_num_features=config.linda_max_num_features,
                ari_order=config.linda_ari_order,
                kernel_type=config.linda_kernel_type,
                add_perturbations=config.linda_add_perturbations,
                pert_thrs=config.linda_perturbation_thresholds_mm_h,
                n_ens_members=config.ensemble_members,
                vel_pert_method=config.linda_velocity_perturbation,
                kmperpixel=kmperpixel,
                timestep=timestep_minutes,
                seed=config.seed,
                num_workers=config.num_workers,
                use_multiprocessing=False,
                return_output=True,
            )
    expected_shape = (
        config.ensemble_members,
        len(config.lead_steps),
        grid.height,
        grid.width,
    )
    array = np.asarray(linear, dtype=float)
    if tuple(array.shape) != expected_shape:
        raise ValueError(
            f"pySTEPS output shape {tuple(array.shape)!r} != {expected_shape!r}"
        )
    if not bool(np.all(np.isfinite(array))) or bool(np.any(array < 0.0)):
        raise ValueError("pySTEPS output is not finite nonnegative rain rate")
    values = tuple(
        tuple(
            tuple(
                tuple(float(value) for value in row)
                for row in field
            )
            for field in member
        )
        for member in array.tolist()
    )
    output_sha = _canonical_sha256(
        {
            "algorithm": config.algorithm.value,
            "lead_steps": list(config.lead_steps),
            "values_mm_h": values,
        }
    )
    forecast = PystepsEnsembleForecast(
        ensemble_id=f"{request.run_id}:ensemble",
        algorithm=config.algorithm,
        grid=grid,
        reference_valid_at=request.fields[-1].valid_at,
        timestep_seconds=timestep,
        lead_steps=config.lead_steps,
        member_ids=tuple(
            f"{request.run_id}:member:{index:04d}"
            for index in range(config.ensemble_members)
        ),
        values_mm_h=values,
        output_sha256=output_sha,
    )
    return forecast, tuple(
        sorted({type(item.message).__name__ for item in caught})
    )


def _failed_result(
    common: Mapping[str, object],
    status: ChallengerRunStatus,
    *,
    failure_code: str,
    failure_class: str | None = None,
    actual_pysteps_version: str | None = None,
    runtime_fingerprint_sha256: str | None = None,
    genuine_pysteps_runtime: bool = False,
    warning_classes: tuple[str, ...] = (),
) -> PystepsChallengerResult:
    return PystepsChallengerResult(
        **dict(common),
        status=status,
        actual_pysteps_version=actual_pysteps_version,
        runtime_fingerprint_sha256=runtime_fingerprint_sha256,
        genuine_pysteps_runtime=genuine_pysteps_runtime,
        skill_evidence_eligible=False,
        failure_code=failure_code,
        failure_class=failure_class,
        warning_classes=warning_classes,
        forecast=None,
    )


def _request_sha256(request: PystepsShadowRequest) -> str:
    return _canonical_sha256(
        {
            "run_id": request.run_id,
            "event_id": request.event_id,
            "partition": request.partition.value,
            "config": _config_projection(request.config),
            "fields": [
                {
                    "field_id": field.field_id,
                    "valid_at": field.valid_at,
                    "quantity": field.quantity,
                    "unit": field.unit,
                    "grid": _grid_projection(field.grid),
                    "values": field.values,
                    "valid_mask": field.valid_mask,
                }
                for field in request.fields
            ],
            "motion": {
                "motion_id": request.motion.motion_id,
                "reference_valid_at": request.motion.reference_valid_at,
                "grid": _grid_projection(request.motion.grid),
                "vx_m_s": request.motion.vx_m_s,
                "vy_m_s": request.motion.vy_m_s,
                "input_field_ids": request.motion.input_field_ids,
                "algorithm_version": request.motion.algorithm_version,
                "algorithm_config_sha256": (
                    request.motion.algorithm_config_sha256
                ),
                "implementation_sha256": request.motion.implementation_sha256,
                "quality_control_version": (
                    request.motion.quality_control_version
                ),
            },
            "field_provenance": [
                {
                    "field_id": item.field_id,
                    "provider_id": item.provider_id,
                    "product_id": item.product_id,
                    "observation_id": item.observation_id,
                    "source_artifact_sha256": item.source_artifact_sha256,
                    "decoder_version": item.decoder_version,
                    "quantity_derivation_version": (
                        item.quantity_derivation_version
                    ),
                    "quality_control_version": item.quality_control_version,
                    "reprojection_version": item.reprojection_version,
                }
                for item in request.field_provenance
            ],
        }
    )


def _config_sha256(config: PystepsChallengerConfig) -> str:
    return _canonical_sha256(_config_projection(config))


def _config_projection(config: PystepsChallengerConfig) -> dict:
    return {
        name: (
            value.value if isinstance(value, Enum) else value
        )
        for name, value in (
            ("algorithm", config.algorithm),
            ("expected_pysteps_version", config.expected_pysteps_version),
            ("seed", config.seed),
            ("lead_steps", config.lead_steps),
            ("ensemble_members", config.ensemble_members),
            ("num_workers", config.num_workers),
            ("rain_threshold_mm_h", config.rain_threshold_mm_h),
            ("max_grid_cells", config.max_grid_cells),
            ("max_output_values", config.max_output_values),
            ("steps_ar_order", config.steps_ar_order),
            ("steps_cascade_levels", config.steps_cascade_levels),
            ("steps_zerovalue_db", config.steps_zerovalue_db),
            ("steps_noise_method", config.steps_noise_method),
            (
                "steps_velocity_perturbation",
                config.steps_velocity_perturbation,
            ),
            ("steps_mask_method", config.steps_mask_method),
            ("steps_fft_method", config.steps_fft_method),
            ("linda_ari_order", config.linda_ari_order),
            ("linda_feature_method", config.linda_feature_method),
            ("linda_max_num_features", config.linda_max_num_features),
            ("linda_kernel_type", config.linda_kernel_type),
            ("linda_add_perturbations", config.linda_add_perturbations),
            (
                "linda_perturbation_thresholds_mm_h",
                config.linda_perturbation_thresholds_mm_h,
            ),
            (
                "linda_velocity_perturbation",
                config.linda_velocity_perturbation,
            ),
            ("contract_version", config.contract_version),
        )
    }


def _grid_projection(grid: AnalysisGrid) -> dict:
    return {
        "grid_id": grid.grid_id,
        "crs": grid.crs,
        "origin_x_m": grid.origin_x_m,
        "origin_y_m": grid.origin_y_m,
        "x_resolution_m": grid.x_resolution_m,
        "y_resolution_m": grid.y_resolution_m,
        "width": grid.width,
        "height": grid.height,
        "coordinate_unit": grid.coordinate_unit,
        "axis_order": grid.axis_order,
        "row_order": grid.row_order,
        "contract_version": grid.contract_version,
    }


def _runtime_fingerprint(runtime: PystepsRuntime) -> str:
    return _canonical_sha256(
        {
            "pysteps_version": runtime.pysteps_version,
            "package_versions": runtime.package_versions,
            "backend_kind": runtime.backend_kind,
            "python_version": runtime.python_version,
            "python_implementation": runtime.python_implementation,
            "platform_system": runtime.platform_system,
            "platform_machine": runtime.platform_machine,
            "entrypoint_identities": (
                (
                    PYSTEPS_STEPS_ENTRYPOINT,
                    getattr(runtime.steps_forecast, "__module__", ""),
                    getattr(runtime.steps_forecast, "__qualname__", ""),
                ),
                (
                    PYSTEPS_LINDA_ENTRYPOINT,
                    getattr(runtime.linda_forecast, "__module__", ""),
                    getattr(runtime.linda_forecast, "__qualname__", ""),
                ),
                (
                    PYSTEPS_DB_TRANSFORM_ENTRYPOINT,
                    getattr(runtime.db_transform, "__module__", ""),
                    getattr(runtime.db_transform, "__qualname__", ""),
                ),
            ),
        }
    )


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _matrix(value: object, grid: AnalysisGrid, label: str) -> None:
    if not isinstance(value, tuple) or any(
        not isinstance(row, tuple) for row in value
    ):
        raise TypeError(f"{label} must use immutable tuples")
    grid.validate_raster_shape(value)


def _identifier(value: object, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")


def _finite(value: object, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{label} must be finite")


def _positive(value: object, label: str) -> None:
    _finite(value, label)
    if float(value) <= 0.0:
        raise ValueError(f"{label} must be positive")


def _positive_int(value: object, label: str, *, minimum: int = 1) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
    ):
        raise ValueError(f"{label} must be an integer >= {minimum}")


def _unique_ids(values: object, label: str) -> None:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(value, str) or not value for value in values)
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{label} must contain unique non-empty IDs")


def _sha256(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be lowercase SHA-256")
