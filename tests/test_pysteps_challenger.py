from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import warnings

import numpy as np
import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.nowcast_baselines import (
    EULERIAN_PERSISTENCE_VERSION,
    MetricRasterField,
)
from storm_tracker_worker.tracking_core.engine.pysteps_challenger import (
    ChallengerRunStatus,
    DenseMotionField,
    HeldOutMetricEvidence,
    HeldOutMetricGateConfig,
    MetricDirection,
    MetricGateStatus,
    MetricRequirement,
    PystepsAlgorithm,
    PystepsChallengerConfig,
    PystepsFieldProvenance,
    PystepsRuntime,
    PystepsShadowRequest,
    ShadowPartition,
    evaluate_held_out_metric_gate,
    run_pysteps_challenger,
)


REPO = Path(__file__).resolve().parent.parent
PYSTEPS_VERSION = "1.21.1"
RUNTIME_SHA = "1" * 64
VERIFICATION_CONFIG_SHA = "2" * 64
SPLIT_SHA = "3" * 64


def _grid(
    *,
    width: int = 3,
    height: int = 2,
    y_resolution_m: float = 1_000.0,
) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id="shadow-grid",
        crs="EPSG:31370",
        origin_x_m=100_000.0,
        origin_y_m=200_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=y_resolution_m,
        width=width,
        height=height,
    )


def _field(index: int, grid: AnalysisGrid) -> MetricRasterField:
    return MetricRasterField(
        field_id=f"rain-{index}",
        valid_at=1_000.0 + index * 300.0,
        quantity="rain_rate",
        unit="mm h-1",
        grid=grid,
        values=tuple(
            tuple(0.05 + index + row + column / 10 for column in range(grid.width))
            for row in range(grid.height)
        ),
        valid_mask=tuple(
            tuple(True for _column in range(grid.width))
            for _row in range(grid.height)
        ),
    )


def _request(
    algorithm: PystepsAlgorithm = PystepsAlgorithm.STEPS,
    *,
    partition: ShadowPartition = ShadowPartition.DEVELOPMENT,
    grid: AnalysisGrid | None = None,
    config_changes: dict | None = None,
) -> PystepsShadowRequest:
    grid = grid or _grid()
    config = PystepsChallengerConfig(
        algorithm=algorithm,
        expected_pysteps_version=PYSTEPS_VERSION,
        seed=17,
        lead_steps=(1.0, 2.0),
        ensemble_members=3,
    )
    if config_changes:
        config = replace(config, **config_changes)
    fields = tuple(
        _field(index, grid) for index in range(config.required_input_frames)
    )
    provenance = tuple(
        PystepsFieldProvenance(
            field_id=field.field_id,
            provider_id="test-provider",
            product_id="quantitative-rain-rate",
            observation_id=f"observation-{index}",
            source_artifact_sha256=f"{index + 4:x}" * 64,
            decoder_version="decoder-v1",
            quantity_derivation_version="physical-rain-rate-v1",
            quality_control_version="qc-v1",
            reprojection_version="nearest-grid-v1",
        )
        for index, field in enumerate(fields)
    )
    values_x = tuple(
        tuple(10.0 for _column in range(grid.width))
        for _row in range(grid.height)
    )
    values_y = tuple(
        tuple(5.0 for _column in range(grid.width))
        for _row in range(grid.height)
    )
    motion = DenseMotionField(
        motion_id="dense-motion-1",
        grid=grid,
        reference_valid_at=fields[-1].valid_at,
        vx_m_s=values_x,
        vy_m_s=values_y,
        input_field_ids=tuple(field.field_id for field in fields),
        algorithm_version="dense-motion-v1",
        algorithm_config_sha256="a" * 64,
        implementation_sha256="b" * 64,
        quality_control_version="motion-qc-v1",
    )
    return PystepsShadowRequest(
        run_id="shadow-run-1",
        event_id="event-1",
        partition=partition,
        fields=fields,
        field_provenance=provenance,
        motion=motion,
        config=config,
    )


class _RuntimeHarness:
    def __init__(self) -> None:
        self.steps_calls: list[tuple] = []
        self.linda_calls: list[tuple] = []
        self.db_calls: list[tuple] = []
        self.emit_warning = False
        self.bad_shape = False

    def db_transform(
        self,
        values,
        metadata=None,
        *,
        threshold=None,
        zerovalue=None,
        inverse=False,
    ):
        array = np.asarray(values, dtype=float)
        self.db_calls.append((array.copy(), metadata, threshold, zerovalue, inverse))
        if inverse:
            if not metadata or metadata.get("transform") != "dB":
                raise ValueError("inverse requires forward-transform metadata")
            return np.power(10.0, array / 10.0), dict(metadata)
        transformed_metadata = dict(metadata or {})
        transformed_metadata.update(
            {
                "transform": "dB",
                "threshold": 10.0 * np.log10(threshold),
                "zerovalue": zerovalue,
            }
        )
        transformed = np.where(
            array >= threshold,
            10.0 * np.log10(array),
            zerovalue,
        )
        return transformed, transformed_metadata

    def steps(self, precip, velocity, timesteps, **kwargs):
        self.steps_calls.append(
            (np.asarray(precip).copy(), np.asarray(velocity).copy(), timesteps, kwargs)
        )
        if self.emit_warning:
            warnings.warn("synthetic warning", UserWarning)
        members = kwargs["n_ens_members"]
        output = np.stack(
            [
                np.stack(
                    [np.asarray(precip[-1]) + member + lead / 10.0 for lead in timesteps]
                )
                for member in range(members)
            ]
        )
        return output[:, :, :, :-1] if self.bad_shape else output

    def linda(self, precip, velocity, timesteps, **kwargs):
        self.linda_calls.append(
            (np.asarray(precip).copy(), np.asarray(velocity).copy(), timesteps, kwargs)
        )
        return np.stack(
            [
                np.stack(
                    [np.asarray(precip[-1]) + member + lead / 10.0 for lead in timesteps]
                )
                for member in range(kwargs["n_ens_members"])
            ]
        )

    def runtime(self, *, version: str = PYSTEPS_VERSION) -> PystepsRuntime:
        return PystepsRuntime(
            pysteps_version=version,
            package_versions=(
                ("numpy", np.__version__),
                ("pysteps", version),
            ),
            numpy=np,
            steps_forecast=self.steps,
            linda_forecast=self.linda,
            db_transform=self.db_transform,
            backend_kind="contract_test_double",
            python_version="3.13.0",
            python_implementation="CPython",
            platform_system="Linux",
            platform_machine="x86_64",
        )


def test_steps_calls_exact_api_contract_and_is_seed_reproducible() -> None:
    request = _request()
    harness = _RuntimeHarness()

    first = run_pysteps_challenger(request, runtime_loader=harness.runtime)
    second = run_pysteps_challenger(request, runtime_loader=harness.runtime)

    assert first.status is ChallengerRunStatus.COMPLETED
    assert first.forecast is not None
    assert first.forecast.output_sha256 == second.forecast.output_sha256
    assert first.genuine_pysteps_runtime is False
    assert first.skill_evidence_eligible is False
    precip, velocity, timesteps, kwargs = harness.steps_calls[0]
    assert precip.shape == (3, 2, 3)
    assert timesteps == [1.0, 2.0]
    np.testing.assert_allclose(velocity[0], 3.0)
    np.testing.assert_allclose(velocity[1], -1.5)
    assert kwargs == {
        "n_ens_members": 3,
        "n_cascade_levels": 6,
        "precip_thr": -10.0,
        "kmperpixel": 1.0,
        "timestep": 5.0,
        "noise_method": "nonparametric",
        "vel_pert_method": "bps",
        "mask_method": "incremental",
        "seed": 17,
        "num_workers": 1,
        "fft_method": "numpy",
        "return_output": True,
    }
    assert harness.db_calls[-1][1]["transform"] == "dB"
    assert harness.db_calls[-1][-1] is True


def test_linda_receives_linear_rain_rate_and_explicit_parameters() -> None:
    request = _request(PystepsAlgorithm.LINDA)
    harness = _RuntimeHarness()

    result = run_pysteps_challenger(request, runtime_loader=harness.runtime)

    assert result.status is ChallengerRunStatus.COMPLETED
    assert not harness.db_calls
    precip, velocity, timesteps, kwargs = harness.linda_calls[0]
    np.testing.assert_allclose(precip, np.asarray([f.values for f in request.fields]))
    np.testing.assert_allclose(velocity[0], 3.0)
    np.testing.assert_allclose(velocity[1], -1.5)
    assert timesteps == [1.0, 2.0]
    assert kwargs["ari_order"] == 1
    assert kwargs["feature_method"] == "blob"
    assert kwargs["add_perturbations"] is True
    assert kwargs["seed"] == 17
    assert kwargs["use_multiprocessing"] is False


def test_unavailable_or_wrong_runtime_returns_no_forecast() -> None:
    request = _request()

    def unavailable():
        raise ModuleNotFoundError("pysteps")

    missing = run_pysteps_challenger(request, runtime_loader=unavailable)
    assert missing.status is ChallengerRunStatus.RUNTIME_UNAVAILABLE
    assert missing.forecast is None
    assert missing.failure_code == "pysteps_runtime_unavailable"

    harness = _RuntimeHarness()
    mismatch = run_pysteps_challenger(
        request,
        runtime_loader=lambda: harness.runtime(version="1.20.0"),
    )
    assert mismatch.status is ChallengerRunStatus.RUNTIME_VERSION_MISMATCH
    assert mismatch.forecast is None
    assert not harness.steps_calls


@pytest.mark.parametrize("fault", ["warning", "shape"])
def test_backend_warning_or_invalid_shape_fails_without_partial_output(
    fault: str,
) -> None:
    harness = _RuntimeHarness()
    harness.emit_warning = fault == "warning"
    harness.bad_shape = fault == "shape"

    result = run_pysteps_challenger(
        _request(),
        runtime_loader=harness.runtime,
    )

    assert result.status is ChallengerRunStatus.EXECUTION_FAILED
    assert result.forecast is None
    if fault == "warning":
        assert result.warning_classes == ("UserWarning",)


def test_input_and_resource_contracts_fail_before_runtime() -> None:
    called = False

    def loader():
        nonlocal called
        called = True
        return _RuntimeHarness().runtime()

    resource_request = _request(config_changes={"max_output_values": 35})
    resource = run_pysteps_challenger(resource_request, runtime_loader=loader)
    assert resource.status is ChallengerRunStatus.RESOURCE_REJECTED
    assert called is False

    anisotropic = run_pysteps_challenger(
        _request(grid=_grid(y_resolution_m=2_000.0)),
        runtime_loader=loader,
    )
    assert anisotropic.status is ChallengerRunStatus.INPUT_REJECTED
    assert called is False


def test_request_rejects_nodata_and_unbound_provenance() -> None:
    request = _request()
    first = request.fields[0]
    mask = tuple(
        tuple(not (row == 0 and column == 0) for column in range(first.grid.width))
        for row in range(first.grid.height)
    )
    with pytest.raises(ValueError, match="incomplete/nodata"):
        replace(
            request,
            fields=(replace(first, valid_mask=mask), *request.fields[1:]),
        )
    with pytest.raises(ValueError, match="provenance"):
        replace(request, field_provenance=request.field_provenance[:-1])


def _gate() -> HeldOutMetricGateConfig:
    return HeldOutMetricGateConfig(
        gate_id="held-out-pysteps-gate-v1",
        split_manifest_sha256=SPLIT_SHA,
        frozen_candidate_config_sha256="4" * 64,
        allowed_runtime_fingerprint_sha256=(RUNTIME_SHA,),
        baseline_algorithm_versions=(EULERIAN_PERSISTENCE_VERSION,),
        verification_algorithm_versions=(
            "event-weighted-ensemble-crps-rank-v1",
            "masked-full-window-fss-v1",
        ),
        verification_config_sha256=VERIFICATION_CONFIG_SHA,
        minimum_independent_events=10,
        requirements=(
            MetricRequirement(
                metric_id="crps_mm_h",
                direction=MetricDirection.LOWER_IS_BETTER,
                allowed_degradation=0.02,
            ),
            MetricRequirement(
                metric_id="fss_1mm_5km",
                direction=MetricDirection.HIGHER_IS_BETTER,
                allowed_degradation=0.01,
            ),
        ),
    )


def _evidence(
    metric_id: str,
    *,
    verification_version: str,
    point: float,
    lower: float,
    upper: float,
) -> HeldOutMetricEvidence:
    return HeldOutMetricEvidence(
        metric_id=metric_id,
        partition=ShadowPartition.HELD_OUT,
        split_manifest_sha256=SPLIT_SHA,
        candidate_config_sha256="4" * 64,
        runtime_fingerprint_sha256=RUNTIME_SHA,
        baseline_algorithm_version=EULERIAN_PERSISTENCE_VERSION,
        verification_algorithm_version=verification_version,
        verification_config_sha256=VERIFICATION_CONFIG_SHA,
        independent_event_count=12,
        candidate_minus_baseline_point=point,
        candidate_minus_baseline_ci_lower=lower,
        candidate_minus_baseline_ci_upper=upper,
        genuine_pysteps_runtime=True,
    )


def test_held_out_gate_uses_confidence_bounds_and_preregistered_provenance() -> None:
    crps = _evidence(
        "crps_mm_h",
        verification_version="event-weighted-ensemble-crps-rank-v1",
        point=-0.02,
        lower=-0.04,
        upper=0.01,
    )
    fss = _evidence(
        "fss_1mm_5km",
        verification_version="masked-full-window-fss-v1",
        point=0.02,
        lower=0.0,
        upper=0.04,
    )
    passed = evaluate_held_out_metric_gate(_gate(), (crps, fss))
    assert passed.status is MetricGateStatus.PASSED

    failed = evaluate_held_out_metric_gate(
        _gate(),
        (crps, replace(fss, candidate_minus_baseline_point=-0.02,
                       candidate_minus_baseline_ci_lower=-0.03,
                       candidate_minus_baseline_ci_upper=-0.015)),
    )
    assert failed.status is MetricGateStatus.FAILED
    assert failed.failed_metric_ids == ("fss_1mm_5km",)

    indeterminate = evaluate_held_out_metric_gate(
        _gate(),
        (replace(crps, runtime_fingerprint_sha256="9" * 64), fss),
    )
    assert indeterminate.status is MetricGateStatus.INDETERMINATE
    assert "crps_mm_h:runtime_not_preregistered" in (
        indeterminate.indeterminate_reasons
    )


def test_worker_mirror_is_exact_and_challenger_is_not_activated() -> None:
    integration = (
        REPO
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "pysteps_challenger.py"
    )
    worker = (
        REPO
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "pysteps_challenger.py"
    )
    assert integration.read_bytes() == worker.read_bytes()
    assert "pysteps" not in (
        REPO / "storm_tracker_worker" / "requirements.txt"
    ).read_text(encoding="utf-8").lower()
    assert "pysteps_challenger" not in (
        REPO / "storm_tracker_worker" / "service.py"
    ).read_text(encoding="utf-8")
