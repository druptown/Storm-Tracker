"""Certified quantitative radar processing used by the worker-only runtime."""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import logging
import math
from pathlib import Path
import threading
from typing import Callable

from pyproj import CRS, Transformer

from .analysis_grid_transform import (
    AnalysisGridTransformConfig,
    NativeFieldCandidateBinding,
    native_candidate_content_sha256,
)
from .canonical_evidence_store import CanonicalEvidenceStore
from .canonical_radar_field_bridge import (
    ProviderDerivedArtifactAuthority,
    default_radar_semantics_certificates,
    derive_canonical_radar_field,
)
from .desired_state import DesiredState
from .dwd_rv_analysis_grid_certificate import (
    issue_dwd_rv_native_grid_certificate,
)
from .dwd_rv_quantitative import (
    DwdRvDerivedArtifactStore,
    decode_dwd_rv_capture,
)
from .multi_source_radar_facts import (
    VerifiedRadarFieldSource,
    build_radar_target_contribution_batch,
)
from .multi_source_radar_snapshot import (
    RadarTargetObservation,
    ReflectivityEchoTargetObservation,
    radar_observations_from_radar_batch,
)
from .raw_artifacts import RawArtifactStore
from .tracking_core.engine.analysis_grid import AnalysisGrid


_LOGGER = logging.getLogger(__name__)


QUANTITATIVE_WORKLOAD_IDS = frozenset({
    "radar.dwd.radolan_rv",
    "radar.knmi.rtcor",
    "radar.metoffice.uk_rate",
    "radar.opera.dbzh",
    "satellite.noaa.err",
})


class WorkerOnlyRadarProcessingError(RuntimeError):
    """Verified radar evidence could not produce an admissible candidate."""


@dataclass(frozen=True, slots=True)
class WorkerOnlyRadarProcessingResult:
    """Target samples plus the same verified field for shadow cell tracking."""

    observations: tuple[RadarTargetObservation, ...]
    verified_source: VerifiedRadarFieldSource | None

    def __post_init__(self) -> None:
        if not isinstance(self.observations, tuple):
            raise TypeError("radar observations moeten een tuple zijn")
        if self.verified_source is not None and not isinstance(
            self.verified_source,
            VerifiedRadarFieldSource,
        ):
            raise TypeError("verified_source heeft het verkeerde type")


_SOURCE_CACHE_MAX = len(QUANTITATIVE_WORKLOAD_IDS) * 2
_SOURCE_CACHE_LOCK = threading.RLock()
_SOURCE_CACHE: OrderedDict[tuple[object, ...], VerifiedRadarFieldSource] = (
    OrderedDict()
)


def _identity(prefix: str, *parts: str) -> str:
    material = "\x00".join(parts).encode("utf-8")
    return prefix + hashlib.sha256(material).hexdigest()


def _source_cache_key(
    *,
    workload_id: str,
    capture_id: str,
    raw_store: RawArtifactStore,
    artifact_root: str | Path,
    evidence_path: str | Path,
    projection: tuple[float, ...] | None = None,
    handoff_root: str | Path | None = None,
) -> tuple[object, ...]:
    """Bind a decoded field cache entry to its exact local authorities."""

    return (
        workload_id,
        capture_id,
        id(raw_store),
        str(Path(artifact_root).resolve()),
        str(Path(evidence_path).resolve()),
        None if handoff_root is None else str(Path(handoff_root).resolve()),
        projection,
    )


def _cached_source(
    cache_key: tuple[object, ...],
) -> VerifiedRadarFieldSource | None:
    with _SOURCE_CACHE_LOCK:
        source = _SOURCE_CACHE.get(cache_key)
        if source is not None:
            _SOURCE_CACHE.move_to_end(cache_key)
        return source


def _remember_source(
    cache_key: tuple[object, ...],
    source: VerifiedRadarFieldSource,
) -> None:
    with _SOURCE_CACHE_LOCK:
        _SOURCE_CACHE[cache_key] = source
        _SOURCE_CACHE.move_to_end(cache_key)
        while len(_SOURCE_CACHE) > _SOURCE_CACHE_MAX:
            _SOURCE_CACHE.popitem(last=False)


def _positive_observations(
    desired_state: DesiredState,
    source: VerifiedRadarFieldSource,
) -> tuple[RadarTargetObservation, ...]:
    # Sampling is deliberately repeated for every desired-state authority.
    # Only the expensive, target-independent decoded field is reused.
    batch = build_radar_target_contribution_batch(desired_state, (source,))
    return radar_observations_from_radar_batch(batch)


def decode_dwd_positive_observations(
    *,
    desired_state: DesiredState,
    capture_id: str,
    raw_store: RawArtifactStore,
    artifact_root: str | Path,
    evidence_path: str | Path,
    handoff_root: str | Path | None,
    evaluated_at: float,
    clock: Callable[[], float],
) -> tuple[RadarTargetObservation, ...]:
    """Decode one DWD RV capture and sample only certified positive target rain."""

    if not isinstance(desired_state, DesiredState):
        raise TypeError("desired_state heeft het verkeerde type")
    if not isinstance(raw_store, RawArtifactStore):
        raise TypeError("raw_store heeft het verkeerde type")
    source_capture_id = capture_id
    source_capture = raw_store.load_capture(capture_id)
    if source_capture is None:
        raise WorkerOnlyRadarProcessingError(
            "dwd_capture_receipt_unavailable"
        )
    cache_key = _source_cache_key(
        workload_id="radar.dwd.radolan_rv",
        capture_id=source_capture_id,
        raw_store=raw_store,
        artifact_root=artifact_root,
        evidence_path=evidence_path,
        handoff_root=handoff_root,
    )
    cached = _cached_source(cache_key)
    if cached is not None:
        return _positive_observations(desired_state, cached)
    processing_store = raw_store
    if (
        source_capture.provider == "dwd"
        and source_capture.product
        == "dwd_open_data_composite_rv_raw_capture"
    ):
        if handoff_root is None:
            raise WorkerOnlyRadarProcessingError(
                "dwd_capture_handoff_failed"
            )
        from .dwd_rv_capture_handoff import adapt_dwd_rv_raw_capture

        processing_store = RawArtifactStore(Path(handoff_root))
        try:
            handoff = adapt_dwd_rv_raw_capture(
                source_capture_id=capture_id,
                source_raw_store=raw_store,
                handoff_raw_store=processing_store,
            )
        except Exception:
            raise WorkerOnlyRadarProcessingError(
                "dwd_capture_handoff_failed"
            ) from None
        capture_id = handoff.target_capture_id
    elif (
        source_capture.provider != "dwd_radolan"
        or source_capture.product != "dwd_open_data_composite_rv_capture"
    ):
        raise WorkerOnlyRadarProcessingError(
            "dwd_capture_handoff_failed"
        )

    artifacts = DwdRvDerivedArtifactStore(Path(artifact_root))
    evidence = CanonicalEvidenceStore(
        Path(evidence_path),
        raw_artifact_reader=processing_store,
        clock=clock,
    )
    decoder_job_id = _identity(
        "decoder-job-",
        capture_id,
        desired_state.state_sha256,
    )
    attempt_id = _identity(
        "decoder-attempt-",
        decoder_job_id,
        capture_id,
    )
    decoded = decode_dwd_rv_capture(
        capture_id=capture_id,
        decoder_job_id=decoder_job_id,
        attempt_id=attempt_id,
        raw_store=processing_store,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=evaluated_at,
        clock=clock,
    )
    if decoded.outcome != "decoded_complete" or decoded.artifact is None:
        raise WorkerOnlyRadarProcessingError(
            "dwd_quantitative_decode_withheld"
        )
    authority = ProviderDerivedArtifactAuthority({"dwd_rv_v1": artifacts})
    bridged = derive_canonical_radar_field(
        evidence,
        authority,
        run_id=decoded.evidence_receipt.run_id,
        certificates=default_radar_semantics_certificates(),
    )
    if not bridged.is_candidate or bridged.candidate is None:
        raise WorkerOnlyRadarProcessingError(
            "dwd_canonical_candidate_withheld"
        )
    field = bridged.candidate
    width = field.native_grid.width
    height = field.native_grid.height
    coverage = field.native_coverage
    target_grid = AnalysisGrid(
        grid_id="worker-only-dwd-rv-native-grid",
        crs=field.native_crs,
        origin_x_m=coverage.minimum_x,
        origin_y_m=coverage.maximum_y,
        x_resolution_m=(coverage.maximum_x - coverage.minimum_x) / width,
        y_resolution_m=(coverage.maximum_y - coverage.minimum_y) / height,
        width=width,
        height=height,
    )
    certificate = issue_dwd_rv_native_grid_certificate(
        binding=NativeFieldCandidateBinding(
            candidate=field,
            candidate_content_sha256=native_candidate_content_sha256(field),
            semantics_certificate_sha256=field.semantics_certificate_sha256,
        ),
        target_grid=target_grid,
        config=AnalysisGridTransformConfig(
            max_source_pixels=max(2_000_000, width * height),
            max_target_pixels=max(2_000_000, width * height),
        ),
    )
    receipt = processing_store.load_capture(capture_id)
    if receipt is None or not receipt.members:
        raise WorkerOnlyRadarProcessingError("dwd_capture_receipt_unavailable")
    source = VerifiedRadarFieldSource(
        field=field,
        received_at=max(member.received_at for member in receipt.members),
        native_grid_certificate=certificate,
    )
    _remember_source(cache_key, source)
    return _positive_observations(desired_state, source)


def _verified_source(
    *,
    desired_state: DesiredState,
    capture_id: str,
    raw_store: RawArtifactStore,
    evidence: CanonicalEvidenceStore,
    artifacts: object,
    artifact_authority_key: str,
    run_id: str,
    cache_key: tuple[object, ...],
) -> tuple[RadarTargetObservation, ...]:
    authority = ProviderDerivedArtifactAuthority({
        artifact_authority_key: artifacts,
    })
    bridged = derive_canonical_radar_field(
        evidence,
        authority,
        run_id=run_id,
        certificates=default_radar_semantics_certificates(),
    )
    if not bridged.is_candidate or bridged.candidate is None:
        raise WorkerOnlyRadarProcessingError(
            f"{artifact_authority_key}_canonical_candidate_withheld"
        )
    receipt = raw_store.load_capture(capture_id)
    if receipt is None or not receipt.members:
        raise WorkerOnlyRadarProcessingError("capture_receipt_unavailable")
    source = VerifiedRadarFieldSource(
        field=bridged.candidate,
        received_at=max(member.received_at for member in receipt.members),
    )
    _remember_source(cache_key, source)
    return _positive_observations(desired_state, source)


def decode_knmi_positive_observations(
    *,
    desired_state: DesiredState,
    capture_id: str,
    raw_store: RawArtifactStore,
    artifact_root: str | Path,
    evidence_path: str | Path,
    evaluated_at: float,
    clock: Callable[[], float],
) -> tuple[RadarTargetObservation, ...]:
    """Decode RTCOR while retaining its explicit target-grid withholding."""

    from .knmi_rtcor_quantitative import (
        KnmiRtcorDerivedArtifactStore,
        decode_knmi_rtcor_capture,
    )

    if raw_store.load_capture(capture_id) is None:
        raise WorkerOnlyRadarProcessingError("capture_receipt_unavailable")
    cache_key = _source_cache_key(
        workload_id="radar.knmi.rtcor",
        capture_id=capture_id,
        raw_store=raw_store,
        artifact_root=artifact_root,
        evidence_path=evidence_path,
    )
    cached = _cached_source(cache_key)
    if cached is not None:
        return _positive_observations(desired_state, cached)
    artifacts = KnmiRtcorDerivedArtifactStore(Path(artifact_root))
    evidence = CanonicalEvidenceStore(
        Path(evidence_path),
        raw_artifact_reader=raw_store,
        clock=clock,
    )
    job_id = _identity("decoder-job-", capture_id, desired_state.state_sha256)
    decoded = decode_knmi_rtcor_capture(
        capture_id=capture_id,
        decoder_job_id=job_id,
        attempt_id=_identity("decoder-attempt-", job_id, capture_id),
        raw_store=raw_store,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=evaluated_at,
        clock=clock,
    )
    if decoded.outcome != "decoded_complete" or decoded.artifact is None:
        raise WorkerOnlyRadarProcessingError("knmi_quantitative_decode_withheld")
    return _verified_source(
        desired_state=desired_state,
        capture_id=capture_id,
        raw_store=raw_store,
        evidence=evidence,
        artifacts=artifacts,
        artifact_authority_key="knmi_rtcor_v1",
        run_id=decoded.evidence_receipt.run_id,
        cache_key=cache_key,
    )


def decode_met_office_positive_observations(
    *,
    desired_state: DesiredState,
    capture_id: str,
    raw_store: RawArtifactStore,
    artifact_root: str | Path,
    evidence_path: str | Path,
    evaluated_at: float,
    clock: Callable[[], float],
) -> tuple[RadarTargetObservation, ...]:
    """Decode and target-sample the certified Met Office RATE field."""

    from .met_office_rate_quantitative import (
        MetOfficeRateDerivedArtifactStore,
        MetOfficeRateRawAuthority,
        decode_met_office_rate_capture,
        open_met_office_rate_evidence_store,
    )

    if raw_store.load_capture(capture_id) is None:
        raise WorkerOnlyRadarProcessingError("capture_receipt_unavailable")
    cache_key = _source_cache_key(
        workload_id="radar.metoffice.uk_rate",
        capture_id=capture_id,
        raw_store=raw_store,
        artifact_root=artifact_root,
        evidence_path=evidence_path,
    )
    cached = _cached_source(cache_key)
    if cached is not None:
        return _positive_observations(desired_state, cached)
    raw_authority = MetOfficeRateRawAuthority(raw_store)
    artifacts = MetOfficeRateDerivedArtifactStore(Path(artifact_root))
    evidence = open_met_office_rate_evidence_store(
        Path(evidence_path),
        raw_authority=raw_authority,
        clock=clock,
    )
    job_id = _identity("decoder-job-", capture_id, desired_state.state_sha256)
    decoded = decode_met_office_rate_capture(
        capture_id=capture_id,
        decoder_job_id=job_id,
        attempt_id=_identity("decoder-attempt-", job_id, capture_id),
        raw_authority=raw_authority,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=evaluated_at,
    )
    if decoded.outcome != "decoded_complete" or decoded.artifact is None:
        raise WorkerOnlyRadarProcessingError(
            "met_office_quantitative_decode_withheld"
        )
    return _verified_source(
        desired_state=desired_state,
        capture_id=capture_id,
        raw_store=raw_store,
        evidence=evidence,
        artifacts=artifacts,
        artifact_authority_key="met_office_rate_v1",
        run_id=decoded.evidence_receipt.run_id,
        cache_key=cache_key,
    )


def _opera_region(desired_state: DesiredState):
    """Return one bounded OPERA window covering every available target."""

    from .opera_dbzh_quantitative import OperaWgs84Region

    targets = tuple(item for item in desired_state.targets if item.available)
    if not targets:
        return None
    padding = 0.03
    return OperaWgs84Region(
        minimum_latitude=max(-89.99, min(item.latitude for item in targets) - padding),
        minimum_longitude=max(-179.99, min(item.longitude for item in targets) - padding),
        maximum_latitude=min(89.99, max(item.latitude for item in targets) + padding),
        maximum_longitude=min(179.99, max(item.longitude for item in targets) + padding),
    )


def _opera_echo_observations_from_artifact(
    desired_state: DesiredState,
    artifact,
    *,
    received_at: float,
    source_receipt_sha256: str,
    certificate_sha256: str,
    process_receipt_sha256: str,
) -> tuple[RadarTargetObservation, ...]:
    """Sample significant OPERA echoes while withholding rain-rate semantics."""

    from . import opera_native as native

    grid = artifact.metadata["grid"]
    capture = artifact.metadata["capture"]
    row_start = int(grid["row_start"])
    column_start = int(grid["column_start"])
    rows, columns = artifact.detected_dbz.shape
    transformer = Transformer.from_crs(
        CRS.from_epsg(4326),
        CRS.from_user_input(native.OPERA_PROJDEF),
        always_xy=True,
    )
    observations: list[RadarTargetObservation] = []
    for target in desired_state.targets:
        if not target.available:
            continue
        x_metres, y_metres = transformer.transform(
            float(target.longitude), float(target.latitude)
        )
        if not math.isfinite(x_metres) or not math.isfinite(y_metres):
            continue
        row = math.floor(-y_metres / native.OPERA_GRID_SCALE_METRES) - row_start
        column = math.floor(x_metres / native.OPERA_GRID_SCALE_METRES) - column_start
        if not (0 <= row < rows and 0 <= column < columns):
            continue
        if (
            int(artifact.coverage_mask[row, column]) != 1
            or int(artifact.dbzh_detected_mask[row, column]) != 1
            or int(artifact.qi_valid_mask[row, column]) != 1
        ):
            continue
        reflectivity_dbz = float(artifact.detected_dbz[row, column])
        quality_index = float(artifact.qi_raw[row, column])
        # The legacy OPERA implementation used 8 dBZ as the first meaningful
        # echo class.  We preserve that detection boundary, but deliberately do
        # not convert reflectivity into surface rain rate.
        if reflectivity_dbz < 8.0:
            continue
        observations.append(ReflectivityEchoTargetObservation(
            source_id="radar.opera.dbzh",
            target_id=target.target_id,
            reflectivity_dbz=reflectivity_dbz,
            quality_index=quality_index,
            product_valid_at=float(artifact.metadata["product"]["nominal_timestamp"]),
            received_at=float(received_at),
            source_receipt_sha256=source_receipt_sha256,
            certificate_sha256=certificate_sha256,
            process_receipt_sha256=process_receipt_sha256,
        ))
    return tuple(sorted(observations, key=lambda item: item.target_id))


def decode_opera_echo_observations(
    *,
    desired_state: DesiredState,
    capture_id: str,
    raw_store: RawArtifactStore,
    artifact_root: str | Path,
    evidence_path: str | Path,
    evaluated_at: float,
    clock: Callable[[], float],
) -> tuple[RadarTargetObservation, ...]:
    """Decode OPERA DBZH/QI and expose only significant reflectivity echoes."""

    from .opera_dbzh_quantitative import (
        DECODER_CONFIG_SHA256,
        OperaDbzhDerivedArtifactStore,
        OperaDbzhRawAuthority,
        decode_opera_dbzh_region,
        open_opera_evidence_store,
    )

    region = _opera_region(desired_state)
    if region is None:
        return ()
    capture = raw_store.load_capture(capture_id)
    if capture is None or not capture.members:
        raise WorkerOnlyRadarProcessingError("capture_receipt_unavailable")
    raw_authority = OperaDbzhRawAuthority(raw_store)
    artifacts = OperaDbzhDerivedArtifactStore(Path(artifact_root))
    evidence = open_opera_evidence_store(
        Path(evidence_path), raw_authority=raw_authority, clock=clock
    )
    job_id = _identity("decoder-job-", capture_id, desired_state.state_sha256)
    decoded = decode_opera_dbzh_region(
        capture_id=capture_id,
        decoder_job_id=job_id,
        attempt_id=_identity("decoder-attempt-", job_id, capture_id),
        region=region,
        raw_authority=raw_authority,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=evaluated_at,
    )
    if decoded.outcome != "decoded_complete" or decoded.artifact is None:
        _LOGGER.warning(
            "OPERA reflectivity decode withheld: outcome=%s reason=%s",
            decoded.outcome,
            decoded.reason_code,
        )
        raise WorkerOnlyRadarProcessingError("opera_reflectivity_decode_withheld")
    artifact = artifacts.read_decoded(
        decoded.artifact, checkpoint=lambda _stage: None
    )
    return _opera_echo_observations_from_artifact(
        desired_state,
        artifact,
        received_at=max(member.received_at for member in capture.members),
        source_receipt_sha256=capture.capture_receipt_sha256,
        certificate_sha256=DECODER_CONFIG_SHA256,
        process_receipt_sha256=decoded.evidence_receipt.receipt_sha256,
    )


def _noaa_region(desired_state: DesiredState):
    from .noaa_err_quantitative import (
        GRID_EAST,
        GRID_NORTH,
        GRID_RESOLUTION_DEGREES,
        GRID_SOUTH,
        GRID_WEST,
        NoaaErrRegion,
    )

    targets = tuple(item for item in desired_state.targets if item.available)
    if not targets:
        return None
    padding = GRID_RESOLUTION_DEGREES
    minimum_latitude = max(
        GRID_SOUTH,
        min(item.latitude for item in targets) - padding,
    )
    maximum_latitude = min(
        GRID_NORTH,
        max(item.latitude for item in targets) + padding,
    )
    minimum_longitude = max(
        GRID_WEST,
        min(item.longitude for item in targets) - padding,
    )
    maximum_longitude = min(
        GRID_EAST,
        max(item.longitude for item in targets) + padding,
    )
    return NoaaErrRegion(
        minimum_latitude=minimum_latitude,
        minimum_longitude=minimum_longitude,
        maximum_latitude=maximum_latitude,
        maximum_longitude=maximum_longitude,
    )


def decode_noaa_positive_observations(
    *,
    desired_state: DesiredState,
    capture_id: str,
    raw_store: RawArtifactStore,
    artifact_root: str | Path,
    evidence_path: str | Path,
    evaluated_at: float,
    clock: Callable[[], float],
) -> tuple[RadarTargetObservation, ...]:
    """Decode a bounded target region from NOAA Enterprise Rain Rate."""

    from .noaa_err_quantitative import (
        NoaaErrDerivedArtifactStore,
        decode_noaa_err_region,
    )

    region = _noaa_region(desired_state)
    if region is None:
        return ()
    if raw_store.load_capture(capture_id) is None:
        raise WorkerOnlyRadarProcessingError("capture_receipt_unavailable")
    projection = (
        float(region.minimum_latitude),
        float(region.minimum_longitude),
        float(region.maximum_latitude),
        float(region.maximum_longitude),
    )
    cache_key = _source_cache_key(
        workload_id="satellite.noaa.err",
        capture_id=capture_id,
        raw_store=raw_store,
        artifact_root=artifact_root,
        evidence_path=evidence_path,
        projection=projection,
    )
    cached = _cached_source(cache_key)
    if cached is not None:
        return _positive_observations(desired_state, cached)
    artifacts = NoaaErrDerivedArtifactStore(Path(artifact_root))
    evidence = CanonicalEvidenceStore(
        Path(evidence_path),
        raw_artifact_reader=raw_store,
        clock=clock,
    )
    job_id = _identity("decoder-job-", capture_id, desired_state.state_sha256)
    decoded = decode_noaa_err_region(
        capture_id=capture_id,
        decoder_job_id=job_id,
        attempt_id=_identity("decoder-attempt-", job_id, capture_id),
        region=region,
        raw_store=raw_store,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=evaluated_at,
        clock=clock,
    )
    if decoded.outcome != "decoded_complete" or decoded.artifact is None:
        raise WorkerOnlyRadarProcessingError("noaa_quantitative_decode_withheld")
    return _verified_source(
        desired_state=desired_state,
        capture_id=capture_id,
        raw_store=raw_store,
        evidence=evidence,
        artifacts=artifacts,
        artifact_authority_key="noaa_err_v3",
        run_id=decoded.evidence_receipt.run_id,
        cache_key=cache_key,
    )


def decode_worker_only_positive_observations(
    *,
    workload_id: str,
    desired_state: DesiredState,
    capture_id: str,
    raw_store: RawArtifactStore,
    artifact_root: str | Path,
    evidence_path: str | Path,
    handoff_root: str | Path | None = None,
    evaluated_at: float,
    clock: Callable[[], float],
) -> tuple[RadarTargetObservation, ...]:
    """Route one certified quantitative workload without semantic guessing."""

    processors = {
        "radar.dwd.radolan_rv": decode_dwd_positive_observations,
        "radar.knmi.rtcor": decode_knmi_positive_observations,
        "radar.metoffice.uk_rate": decode_met_office_positive_observations,
        "radar.opera.dbzh": decode_opera_echo_observations,
        "satellite.noaa.err": decode_noaa_positive_observations,
    }
    processor = processors.get(workload_id)
    if processor is None:
        raise WorkerOnlyRadarProcessingError(
            "quantitative_workload_not_certified"
        )
    safe_name = workload_id.replace(".", "-")
    arguments = dict(
        desired_state=desired_state,
        capture_id=capture_id,
        raw_store=raw_store,
        artifact_root=Path(artifact_root) / safe_name,
        evidence_path=Path(evidence_path) / f"{safe_name}.sqlite3",
        evaluated_at=evaluated_at,
        clock=clock,
    )
    if workload_id == "radar.dwd.radolan_rv":
        arguments["handoff_root"] = handoff_root
    return processor(**arguments)


def _routed_cached_source(
    *,
    workload_id: str,
    desired_state: DesiredState,
    capture_id: str,
    raw_store: RawArtifactStore,
    artifact_root: str | Path,
    evidence_path: str | Path,
    handoff_root: str | Path | None,
) -> VerifiedRadarFieldSource | None:
    """Reload only the exact cache address populated by the routed decoder."""

    safe_name = workload_id.replace(".", "-")
    routed_artifact_root = Path(artifact_root) / safe_name
    routed_evidence_path = Path(evidence_path) / f"{safe_name}.sqlite3"
    projection = None
    routed_handoff_root = None
    if workload_id == "satellite.noaa.err":
        region = _noaa_region(desired_state)
        if region is None:
            return None
        projection = (
            float(region.minimum_latitude),
            float(region.minimum_longitude),
            float(region.maximum_latitude),
            float(region.maximum_longitude),
        )
    elif workload_id == "radar.dwd.radolan_rv":
        routed_handoff_root = handoff_root
    key = _source_cache_key(
        workload_id=workload_id,
        capture_id=capture_id,
        raw_store=raw_store,
        artifact_root=routed_artifact_root,
        evidence_path=routed_evidence_path,
        projection=projection,
        handoff_root=routed_handoff_root,
    )
    return _cached_source(key)


def decode_worker_only_radar_result(
    *,
    workload_id: str,
    desired_state: DesiredState,
    capture_id: str,
    raw_store: RawArtifactStore,
    artifact_root: str | Path,
    evidence_path: str | Path,
    handoff_root: str | Path | None = None,
    evaluated_at: float,
    clock: Callable[[], float],
) -> WorkerOnlyRadarProcessingResult:
    """Decode once and retain the verified field for bounded shadow tracking."""

    observations = decode_worker_only_positive_observations(
        workload_id=workload_id,
        desired_state=desired_state,
        capture_id=capture_id,
        raw_store=raw_store,
        artifact_root=artifact_root,
        evidence_path=evidence_path,
        handoff_root=handoff_root,
        evaluated_at=evaluated_at,
        clock=clock,
    )
    source = _routed_cached_source(
        workload_id=workload_id,
        desired_state=desired_state,
        capture_id=capture_id,
        raw_store=raw_store,
        artifact_root=artifact_root,
        evidence_path=evidence_path,
        handoff_root=handoff_root,
    )
    if (
        workload_id != "radar.opera.dbzh"
        and source is None
        and any(item.available for item in desired_state.targets)
    ):
        raise WorkerOnlyRadarProcessingError(
            "quantitative_verified_source_unavailable"
        )
    return WorkerOnlyRadarProcessingResult(observations, source)


__all__ = [
    "QUANTITATIVE_WORKLOAD_IDS",
    "WorkerOnlyRadarProcessingError",
    "WorkerOnlyRadarProcessingResult",
    "decode_dwd_positive_observations",
    "decode_knmi_positive_observations",
    "decode_met_office_positive_observations",
    "decode_opera_echo_observations",
    "decode_noaa_positive_observations",
    "decode_worker_only_positive_observations",
    "decode_worker_only_radar_result",
]
