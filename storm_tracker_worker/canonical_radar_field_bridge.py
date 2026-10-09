"""Fail-closed bridge from canonical decoder evidence to radar field candidates.

This module is deliberately worker-only, shadow-only and unwired.  It accepts
only a ``run_id`` from :class:`CanonicalEvidenceStore`, so the decoder receipt
and its complete schema-3 raw capture are reverified before a derived artifact
is opened.  A provider artifact store then verifies the opaque handle, complete
artifact SHA-256 and the provider-specific closed artifact format.

The result is still *not* operational admission.  It is an explicit native
meteorological field candidate that retains temporal support, native geometry,
nodata/undetect masks, native QC planes and the complete evidence chain.  No
reprojection, resampling, thresholding, wet/dry classification, confidence or
provider fallback occurs here.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
from pathlib import Path
import re
import stat
import time
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable

import numpy as np

from .canonical_evidence_store import (
    CanonicalEvidenceError,
    CanonicalEvidenceStore,
    CoverageSpec,
    DecoderRunReceipt,
    NativeGridSpec,
)
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.nowcast_baselines import MetricRasterField


RADAR_FIELD_BRIDGE_ALGORITHM = "canonical-radar-field-bridge-shadow-v1"
RADAR_FIELD_ADMISSION_STATUS = "candidate_only_unadmitted"
DEFAULT_BRIDGE_DEADLINE_SECONDS = 90.0
HARD_MAX_NATIVE_PIXELS = 4_000_000
HARD_MAX_ARRAY_BYTES = 256 * 1024 * 1024
HARD_MAX_CERTIFICATES = 32
HARD_MAX_AUXILIARY_PLANES = 32
HARD_MAX_METRIC_FIELD_PIXELS = 1_000_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^run-[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[a-z][a-z0-9_.:-]{0,127}$")


class RadarFieldBridgeError(RuntimeError):
    """Base error for invalid caller configuration or resource bounds."""


class RadarFieldBridgeDeadlineError(RadarFieldBridgeError):
    """The absolute monotonic bridge deadline expired."""


class RadarFieldBridgeCapacityError(RadarFieldBridgeError):
    """A hard field, byte, certificate or materialization bound was exceeded."""


class RadarArtifactAuthorityError(RadarFieldBridgeError):
    """A derived artifact was absent, unsafe or failed verification."""


class RadarFieldIssueState(str, Enum):
    WITHHELD = "withheld"
    INVALID = "invalid"


class RadarTemporalSemantics(str, Enum):
    """The exact physical time meaning retained by the output rate."""

    INSTANTANEOUS_RATE_ESTIMATE = "instantaneous_rate_estimate"
    INTERVAL_MEAN_RATE = "interval_mean_rate_from_exact_accumulation"
    NOMINAL_RATE_WITH_INPUT_WINDOW = "nominal_rate_with_nonuniform_input_window"


class AnalysisGridMapping(str, Enum):
    """Whether native geometry can be represented without reprojection."""

    NONE = "native_only_no_exact_metric_analysis_mapping"
    METRIC_BBOX_EDGES = "native_metric_bbox_edges_north_to_south"


@dataclass(frozen=True, slots=True)
class RadarFieldSemanticsCertificate:
    """Exact provider/product/decoder/config semantics allowlist entry."""

    certificate_id: str
    provider: str
    product: str
    decoder_identity: str
    decoder_version: str
    decoder_config_sha256: str
    artifact_schema: str
    artifact_handle_prefix: str
    artifact_authority_key: str
    receipt_quantity: str
    receipt_unit: str
    output_quantity: str
    output_unit: str
    temporal_semantics: RadarTemporalSemantics
    exact_accumulation_seconds: int | None
    analysis_grid_mapping: AnalysisGridMapping
    qc_policy: str
    primary_sources: tuple[str, ...]

    def __post_init__(self) -> None:
        for label, value in (
            ("certificate_id", self.certificate_id),
            ("provider", self.provider),
            ("product", self.product),
            ("decoder_identity", self.decoder_identity),
            ("decoder_version", self.decoder_version),
            ("artifact_schema", self.artifact_schema),
            ("artifact_authority_key", self.artifact_authority_key),
            ("receipt_quantity", self.receipt_quantity),
            ("receipt_unit", self.receipt_unit),
            ("output_quantity", self.output_quantity),
            ("output_unit", self.output_unit),
            ("qc_policy", self.qc_policy),
        ):
            if not isinstance(value, str) or not value or len(value) > 256:
                raise ValueError(f"semantics certificate {label} is invalid")
        if _SAFE_ID_RE.fullmatch(self.certificate_id) is None:
            raise ValueError("semantics certificate_id is not a safe code")
        if _SHA256_RE.fullmatch(self.decoder_config_sha256) is None:
            raise ValueError("semantics decoder_config_sha256 is invalid")
        if (
            not isinstance(self.artifact_handle_prefix, str)
            or not self.artifact_handle_prefix.startswith("artifact:")
            or not self.artifact_handle_prefix.endswith(":")
            or len(self.artifact_handle_prefix) > 256
        ):
            raise ValueError("semantics artifact handle prefix is invalid")
        if not isinstance(self.temporal_semantics, RadarTemporalSemantics):
            raise TypeError("temporal_semantics must be RadarTemporalSemantics")
        if not isinstance(self.analysis_grid_mapping, AnalysisGridMapping):
            raise TypeError("analysis_grid_mapping must be AnalysisGridMapping")
        if self.temporal_semantics is RadarTemporalSemantics.INTERVAL_MEAN_RATE:
            if (
                isinstance(self.exact_accumulation_seconds, bool)
                or not isinstance(self.exact_accumulation_seconds, int)
                or self.exact_accumulation_seconds <= 0
                or self.exact_accumulation_seconds > 86_400
            ):
                raise ValueError("interval-mean certificate needs exact seconds")
        elif self.exact_accumulation_seconds is not None:
            raise ValueError("direct-rate certificate cannot claim accumulation")
        if self.output_unit != "mm/h":
            raise ValueError("radar field bridge output unit must be mm/h")
        if (
            not isinstance(self.primary_sources, tuple)
            or not self.primary_sources
            or len(self.primary_sources) > 8
            or any(
                not isinstance(item, str)
                or not item.startswith("https://")
                or len(item) > 1024
                for item in self.primary_sources
            )
        ):
            raise ValueError("semantics primary_sources are invalid")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "analysis_grid_mapping": self.analysis_grid_mapping.value,
            "artifact_authority_key": self.artifact_authority_key,
            "artifact_handle_prefix": self.artifact_handle_prefix,
            "artifact_schema": self.artifact_schema,
            "certificate_id": self.certificate_id,
            "decoder_config_sha256": self.decoder_config_sha256,
            "decoder_identity": self.decoder_identity,
            "decoder_version": self.decoder_version,
            "exact_accumulation_seconds": self.exact_accumulation_seconds,
            "output_quantity": self.output_quantity,
            "output_unit": self.output_unit,
            "primary_sources": list(self.primary_sources),
            "product": self.product,
            "provider": self.provider,
            "qc_policy": self.qc_policy,
            "receipt_quantity": self.receipt_quantity,
            "receipt_unit": self.receipt_unit,
            "temporal_semantics": self.temporal_semantics.value,
        }

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_mapping())).hexdigest()

    def matches(self, receipt: DecoderRunReceipt) -> bool:
        run = receipt.run
        return (
            run.provider == self.provider
            and run.product == self.product
            and run.decoder_identity == self.decoder_identity
            and run.decoder_version == self.decoder_version
            and run.config_sha256 == self.decoder_config_sha256
            and run.quantity == self.receipt_quantity
            and run.unit == self.receipt_unit
            and run.non_operational_candidate_artifact is not None
            and run.non_operational_candidate_artifact.handle
            == self.artifact_handle_prefix
            + run.non_operational_candidate_artifact.sha256
        )


@dataclass(frozen=True, slots=True)
class NativeRasterPlane:
    """One retained native array plane with an explicit physical meaning."""

    name: str
    semantics: str
    values: np.ndarray

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name or len(self.name) > 128:
            raise ValueError("native plane name is invalid")
        if (
            not isinstance(self.semantics, str)
            or not self.semantics
            or len(self.semantics) > 512
        ):
            raise ValueError("native plane semantics are invalid")
        if not isinstance(self.values, np.ndarray) or self.values.ndim != 2:
            raise TypeError("native plane must be a two-dimensional ndarray")
        if self.values.flags.writeable:
            raise ValueError("native plane must be read-only")


@dataclass(frozen=True, slots=True)
class VerifiedProviderRaster:
    """Provider reader output after full artifact hash and format validation."""

    artifact_schema: str
    artifact_metadata: Mapping[str, Any]
    source_values: np.ndarray
    valid_mask: np.ndarray
    nodata_mask: np.ndarray
    undetect_mask: np.ndarray
    auxiliary_planes: tuple[NativeRasterPlane, ...]
    support_start: float
    support_end: float
    representative_at: float
    source_quantity: str
    source_unit: str
    quality_state: str
    analysis_grid: AnalysisGrid | None


@dataclass(frozen=True, slots=True)
class RadarFieldIssue:
    state: RadarFieldIssueState
    code: str
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.state, RadarFieldIssueState):
            raise TypeError("radar issue state is invalid")
        if _SAFE_ID_RE.fullmatch(self.code) is None:
            raise ValueError("radar issue code is invalid")
        if not isinstance(self.detail, str) or not self.detail or len(self.detail) > 1024:
            raise ValueError("radar issue detail is invalid")


@dataclass(frozen=True, slots=True)
class NativeRainRateFieldCandidate:
    """Unadmitted native field with complete provenance and explicit masks."""

    candidate_id: str
    canonical_run_id: str
    canonical_commit_id: str
    canonical_receipt_sha256: str
    canonical_result_sha256: str
    capture_id: str
    capture_receipt_sha256: str
    capture_manifest_sha256: str
    artifact_handle: str
    artifact_sha256: str
    artifact_metadata_sha256: str
    semantics_certificate_id: str
    semantics_certificate_sha256: str
    provider: str
    product: str
    decoder_identity: str
    decoder_version: str
    decoder_config_sha256: str
    source_quantity: str
    source_unit: str
    source_values: np.ndarray
    output_quantity: str
    output_unit: str
    rain_rate_mm_h: np.ndarray
    valid_mask: np.ndarray
    nodata_mask: np.ndarray
    undetect_mask: np.ndarray
    auxiliary_planes: tuple[NativeRasterPlane, ...]
    temporal_semantics: RadarTemporalSemantics
    support_start: float
    support_end: float
    valid_at: float
    conversion: str
    quality_state: str
    native_crs: str
    native_grid: NativeGridSpec
    native_coverage: CoverageSpec
    analysis_grid: AnalysisGrid | None
    metric_raster_field: MetricRasterField | None
    primary_sources: tuple[str, ...]
    admission_status: str = RADAR_FIELD_ADMISSION_STATUS
    reprojection_applied: bool = False
    resampling_applied: bool = False
    thresholding_applied: bool = False
    wet_dry_classification_applied: bool = False
    algorithm_version: str = RADAR_FIELD_BRIDGE_ALGORITHM

    def __post_init__(self) -> None:
        if self.admission_status != RADAR_FIELD_ADMISSION_STATUS:
            raise ValueError("radar field candidate cannot grant admission")
        if any(
            (
                self.reprojection_applied,
                self.resampling_applied,
                self.thresholding_applied,
                self.wet_dry_classification_applied,
            )
        ):
            raise ValueError("radar field candidate contains a forbidden operation")
        shape = (self.native_grid.height, self.native_grid.width)
        for label, array in (
            ("source_values", self.source_values),
            ("rain_rate_mm_h", self.rain_rate_mm_h),
            ("valid_mask", self.valid_mask),
            ("nodata_mask", self.nodata_mask),
            ("undetect_mask", self.undetect_mask),
        ):
            if not isinstance(array, np.ndarray) or array.shape != shape:
                raise ValueError(f"{label} does not match native grid")
            if array.flags.writeable:
                raise ValueError(f"{label} must be read-only")
        if not np.array_equal(np.isnan(self.rain_rate_mm_h), ~self.valid_mask):
            raise ValueError("invalid rain-rate cells must be NaN, never dry zero")
        if not np.isfinite(self.rain_rate_mm_h[self.valid_mask]).all():
            raise ValueError("valid rain rates must be finite")
        if np.any(self.rain_rate_mm_h[self.valid_mask] < 0.0):
            raise ValueError("valid rain rates cannot be negative")
        if not np.array_equal(
            self.valid_mask,
            ~(self.nodata_mask | self.undetect_mask),
        ):
            raise ValueError("valid/nodata/undetect masks are not exhaustive")
        if np.any(self.nodata_mask & self.undetect_mask):
            raise ValueError("nodata and undetect masks overlap")
        for plane in self.auxiliary_planes:
            if plane.values.shape != shape:
                raise ValueError("auxiliary plane does not match native grid")
        if self.metric_raster_field is not None:
            if self.analysis_grid is None:
                raise ValueError("MetricRasterField requires exact AnalysisGrid")
            self.analysis_grid.assert_same_geometry(self.metric_raster_field.grid)
            if self.metric_raster_field.field_id != self.candidate_id:
                raise ValueError("MetricRasterField identity differs from candidate")
        for timestamp in (self.support_start, self.support_end, self.valid_at):
            if not math.isfinite(timestamp):
                raise ValueError("candidate time is not finite")
        if self.support_end < self.support_start:
            raise ValueError("candidate time support is inverted")
        if not self.support_start <= self.valid_at <= self.support_end:
            raise ValueError("candidate valid_at lies outside retained support")

    def value_or_none(self, row: int, column: int) -> float | None:
        if (
            isinstance(row, bool)
            or not isinstance(row, int)
            or isinstance(column, bool)
            or not isinstance(column, int)
            or row < 0
            or row >= self.native_grid.height
            or column < 0
            or column >= self.native_grid.width
        ):
            raise IndexError("native candidate pixel is out of bounds")
        if not self.valid_mask[row, column]:
            return None
        return float(self.rain_rate_mm_h[row, column])


@dataclass(frozen=True, slots=True)
class RadarFieldBridgeResult:
    run_id: str
    candidate: NativeRainRateFieldCandidate | None
    issues: tuple[RadarFieldIssue, ...]

    @property
    def is_candidate(self) -> bool:
        return self.candidate is not None and not self.issues


@runtime_checkable
class RadarDerivedArtifactAuthority(Protocol):
    """Bounded artifact reader used by the bridge."""

    def load_verified(
        self,
        receipt: DecoderRunReceipt,
        certificate: RadarFieldSemanticsCertificate,
        *,
        deadline_at: float,
        monotonic: Callable[[], float],
    ) -> VerifiedProviderRaster: ...


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _check_deadline(deadline_at: float, monotonic: Callable[[], float]) -> None:
    current = float(monotonic())
    if not math.isfinite(current) or current >= deadline_at:
        raise RadarFieldBridgeDeadlineError("radar field bridge deadline expired")


def _deadline(
    deadline_at: float | None,
    monotonic: Callable[[], float],
) -> float:
    current = float(monotonic())
    if not math.isfinite(current):
        raise ValueError("monotonic clock must be finite")
    if deadline_at is None:
        return current + DEFAULT_BRIDGE_DEADLINE_SECONDS
    if isinstance(deadline_at, bool) or not isinstance(deadline_at, (int, float)):
        raise ValueError("deadline_at must be finite")
    result = float(deadline_at)
    if not math.isfinite(result):
        raise ValueError("deadline_at must be finite")
    return result


def _safe_object_size(
    path: Path,
    *,
    maximum_bytes: int,
    checkpoint: Callable[[], None],
) -> int:
    checkpoint()
    try:
        info = path.lstat()
    except OSError as exc:
        raise RadarArtifactAuthorityError("derived artifact object is missing") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or not 0 < info.st_size <= maximum_bytes
    ):
        raise RadarArtifactAuthorityError("derived artifact object is unsafe or oversized")
    checkpoint()
    return int(info.st_size)


def _readonly(array: np.ndarray, *, dtype: np.dtype[Any] | type | None = None) -> np.ndarray:
    result = np.asarray(array, dtype=dtype)
    if result.flags.writeable:
        result = result.view()
        result.setflags(write=False)
    return result


def _readonly_owned(array: np.ndarray, *, dtype: np.dtype[Any] | type) -> np.ndarray:
    result = np.array(array, dtype=dtype, copy=True, order="C")
    result.setflags(write=False)
    return result


def _mask(array: np.ndarray, name: str) -> np.ndarray:
    value = np.asarray(array)
    if value.ndim != 2 or not np.all((value == 0) | (value == 1)):
        raise RadarArtifactAuthorityError(f"artifact {name} is not a binary mask")
    return _readonly_owned(value, dtype=np.bool_)


def _plane(name: str, semantics: str, values: np.ndarray) -> NativeRasterPlane:
    return NativeRasterPlane(name=name, semantics=semantics, values=_readonly(values))


class ProviderDerivedArtifactAuthority:
    """Concrete adapters around the four isolated content-addressed stores.

    Stores are keyed by the semantics certificate's ``artifact_authority_key``.
    The imports are lazy so merely importing this shadow bridge does not load
    HDF5/NetCDF decoder modules into a production worker process.
    """

    def __init__(self, stores: Mapping[str, object]) -> None:
        if not isinstance(stores, Mapping) or len(stores) > HARD_MAX_CERTIFICATES:
            raise ValueError("artifact authority store mapping is invalid")
        self._stores = dict(stores)

    def load_verified(
        self,
        receipt: DecoderRunReceipt,
        certificate: RadarFieldSemanticsCertificate,
        *,
        deadline_at: float,
        monotonic: Callable[[], float],
    ) -> VerifiedProviderRaster:
        def checkpoint() -> None:
            _check_deadline(deadline_at, monotonic)

        checkpoint()
        store = self._stores.get(certificate.artifact_authority_key)
        if store is None:
            raise RadarArtifactAuthorityError("derived artifact authority is unavailable")
        if certificate.artifact_authority_key == "knmi_rtcor_v1":
            result = self._read_knmi(store, receipt, checkpoint)
        elif certificate.artifact_authority_key == "noaa_err_v3":
            result = self._read_noaa(store, receipt, checkpoint)
        elif certificate.artifact_authority_key == "met_office_rate_v1":
            result = self._read_met_office(store, receipt, checkpoint)
        elif certificate.artifact_authority_key == "dwd_rv_v1":
            result = self._read_dwd(store, receipt, checkpoint)
        else:
            raise RadarArtifactAuthorityError("artifact authority key is unsupported")
        checkpoint()
        return result

    @staticmethod
    def _artifact_reference(receipt: DecoderRunReceipt) -> tuple[str, str]:
        reference = receipt.run.non_operational_candidate_artifact
        if reference is None:
            raise RadarArtifactAuthorityError("canonical receipt has no artifact")
        return reference.handle, reference.sha256

    @classmethod
    def _read_knmi(
        cls,
        store: object,
        receipt: DecoderRunReceipt,
        checkpoint: Callable[[], None],
    ) -> VerifiedProviderRaster:
        from . import knmi_rtcor_quantitative as provider

        if not isinstance(store, provider.KnmiRtcorDerivedArtifactStore):
            raise TypeError("KNMI artifact store has the wrong type")
        handle, digest = cls._artifact_reference(receipt)
        size = _safe_object_size(
            store.objects_path / f"{digest}.krtq",
            maximum_bytes=provider.MAX_ARTIFACT_BYTES,
            checkpoint=checkpoint,
        )
        artifact = store.read_decoded(provider.KnmiRtcorArtifactRef(handle, digest, size))
        checkpoint()
        metadata = artifact.metadata
        product = metadata["product"]
        missing = _mask(artifact.precipitation_missing_mask, "KNMI missing")
        outside = _mask(
            artifact.precipitation_out_of_image_mask,
            "KNMI out_of_image",
        )
        nodata = _readonly_owned(missing | outside, dtype=np.bool_)
        valid = _mask(artifact.precipitation_valid_mask, "KNMI valid")
        undetect = _readonly_owned(np.zeros(valid.shape, dtype=np.bool_), dtype=np.bool_)
        auxiliary = (
            _plane("precipitation_raw", "native_uint16_with_sentinels", artifact.precipitation_raw),
            _plane("precipitation_missing_mask", "one_is_missing_data", missing),
            _plane("precipitation_out_of_image_mask", "one_is_outside_product_domain", outside),
            _plane("quality_raw", "native_uint8_with_sentinels", artifact.quality_raw),
            _plane("quality_index", "upstream_native_dimensionless_index", artifact.quality_index),
            _plane("quality_valid_mask", "one_is_valid_quality", artifact.quality_valid_mask),
            _plane("quality_missing_mask", "one_is_missing_quality", artifact.quality_missing_mask),
            _plane("quality_out_of_image_mask", "one_is_outside_quality_domain", artifact.quality_out_of_image_mask),
            _plane("adjustment_raw", "native_uint8_with_sentinels", artifact.adjustment_raw),
            _plane("adjustment_db", "already_applied_upstream_diagnostic_db", artifact.adjustment_db),
            _plane("adjustment_valid_mask", "one_is_valid_adjustment", artifact.adjustment_valid_mask),
            _plane("adjustment_missing_mask", "one_is_missing_adjustment", artifact.adjustment_missing_mask),
            _plane("adjustment_out_of_image_mask", "one_is_outside_adjustment_domain", artifact.adjustment_out_of_image_mask),
        )
        return VerifiedProviderRaster(
            artifact_schema=provider.ARTIFACT_SCHEMA,
            artifact_metadata=metadata,
            source_values=_readonly(artifact.precipitation_mm),
            valid_mask=valid,
            nodata_mask=nodata,
            undetect_mask=undetect,
            auxiliary_planes=auxiliary,
            support_start=float(product["interval_start"]),
            support_end=float(product["interval_end"]),
            representative_at=float(product["product_time"]),
            source_quantity="precipitation_accumulation_5_minute",
            source_unit="mm",
            quality_state="native_quality_index_retained_not_interpreted",
            analysis_grid=None,
        )

    @classmethod
    def _read_noaa(
        cls,
        store: object,
        receipt: DecoderRunReceipt,
        checkpoint: Callable[[], None],
    ) -> VerifiedProviderRaster:
        from . import noaa_err_quantitative as provider

        if not isinstance(store, provider.NoaaErrDerivedArtifactStore):
            raise TypeError("NOAA artifact store has the wrong type")
        handle, digest = cls._artifact_reference(receipt)
        size = _safe_object_size(
            store.objects_path / f"{digest}.neqr",
            maximum_bytes=provider.MAX_ARTIFACT_BYTES,
            checkpoint=checkpoint,
        )
        artifact = store.read_decoded(
            provider.NoaaErrArtifactRef(handle, digest, size),
            checkpoint=lambda _stage: checkpoint(),
        )
        metadata = artifact.metadata
        product = metadata["product"]
        valid = _mask(artifact.rate_valid_mask, "NOAA valid")
        nodata = _mask(artifact.rate_missing_mask, "NOAA missing")
        undetect = _readonly_owned(np.zeros(valid.shape, dtype=np.bool_), dtype=np.bool_)
        auxiliary = (
            _plane("rrqpe_raw", "native_int16_scaled_by_0.1", artifact.rrqpe_raw),
            _plane("dqf_raw", "native_cf_flag_mask_value", artifact.dqf_raw),
            _plane("dqf_missing_mask", "one_is_missing_dqf", artifact.dqf_missing_mask),
            _plane("dqf_bad_quality_mask", "native_bad_quality_bit", artifact.dqf_bad_quality_mask),
            _plane("dqf_degraded_mask", "native_viewing_or_latitude_degraded_bit", artifact.dqf_degraded_mask),
            _plane("quality_optimal_mask", "native_dqf_equals_zero_only", artifact.quality_optimal_mask),
        )
        return VerifiedProviderRaster(
            artifact_schema=provider.ARTIFACT_SCHEMA,
            artifact_metadata=metadata,
            source_values=_readonly(artifact.precipitation_rate_mm_h),
            valid_mask=valid,
            nodata_mask=nodata,
            undetect_mask=undetect,
            auxiliary_planes=auxiliary,
            support_start=float(product["product_start"]),
            support_end=float(product["product_end"]),
            representative_at=float(product["product_start"]),
            source_quantity="instantaneous_precipitation_rate",
            source_unit="mm/h",
            quality_state="native_dqf_flags_retained_not_applied",
            analysis_grid=None,
        )

    @classmethod
    def _read_met_office(
        cls,
        store: object,
        receipt: DecoderRunReceipt,
        checkpoint: Callable[[], None],
    ) -> VerifiedProviderRaster:
        from . import met_office_rate_quantitative as provider

        if not isinstance(store, provider.MetOfficeRateDerivedArtifactStore):
            raise TypeError("Met Office artifact store has the wrong type")
        handle, digest = cls._artifact_reference(receipt)
        size = _safe_object_size(
            store.objects_path / f"{digest}.morq",
            maximum_bytes=provider.MAX_ARTIFACT_BYTES,
            checkpoint=checkpoint,
        )
        artifact = store.read_decoded(
            provider.MetOfficeRateArtifactRef(handle, digest, size),
            checkpoint=lambda _stage: checkpoint(),
        )
        metadata = artifact.metadata
        product = metadata["product"]
        grid = metadata["grid"]
        coverage = grid["coverage"]
        analysis = AnalysisGrid(
            grid_id=f"native:{receipt.run.provider}:{receipt.run.native_grid.definition_sha256}",  # type: ignore[union-attr]
            crs=str(grid["projection"]),
            origin_x_m=float(coverage["minimum_x"]),
            origin_y_m=float(coverage["maximum_y"]),
            x_resolution_m=float(grid["xscale_metres"]),
            y_resolution_m=float(grid["yscale_metres"]),
            width=int(grid["columns"]),
            height=int(grid["rows"]),
        )
        valid = _mask(artifact.rate_valid_mask, "Met Office valid")
        nodata = _mask(artifact.rate_nodata_mask, "Met Office nodata")
        undetect = _mask(artifact.rate_undetect_mask, "Met Office undetect")
        return VerifiedProviderRaster(
            artifact_schema=provider.ARTIFACT_SCHEMA,
            artifact_metadata=metadata,
            source_values=_readonly(artifact.rate_mm_h),
            valid_mask=valid,
            nodata_mask=nodata,
            undetect_mask=undetect,
            auxiliary_planes=(
                _plane("rate_encoded", "native_odim_rate_float32", artifact.rate_encoded),
            ),
            support_start=float(product["input_window_start_timestamp"]),
            support_end=float(product["input_window_end_timestamp"]),
            representative_at=float(product["nominal_timestamp"]),
            source_quantity="surface_precipitation_rate_estimate",
            source_unit="mm/h",
            quality_state="unavailable:odim_quality_dataset_absent",
            analysis_grid=analysis,
        )

    @classmethod
    def _read_dwd(
        cls,
        store: object,
        receipt: DecoderRunReceipt,
        checkpoint: Callable[[], None],
    ) -> VerifiedProviderRaster:
        from . import dwd_rv_quantitative as provider

        if not isinstance(store, provider.DwdRvDerivedArtifactStore):
            raise TypeError("DWD artifact store has the wrong type")
        handle, digest = cls._artifact_reference(receipt)
        size = _safe_object_size(
            store.objects_path / f"{digest}.drvq",
            maximum_bytes=provider.MAX_ARTIFACT_BYTES,
            checkpoint=checkpoint,
        )
        artifact = store.read_decoded(provider.DwdRvArtifactRef(handle, digest, size))
        checkpoint()
        metadata = artifact.metadata
        product = metadata["product"]
        valid = _mask(artifact.valid_mask, "DWD valid")
        nodata = _mask(artifact.nodata_mask, "DWD nodata")
        undetect = _mask(artifact.undetect_mask, "DWD undetect")
        return VerifiedProviderRaster(
            artifact_schema=provider.ARTIFACT_SCHEMA,
            artifact_metadata=metadata,
            source_values=_readonly(artifact.accumulation_mm),
            valid_mask=valid,
            nodata_mask=nodata,
            undetect_mask=undetect,
            auxiliary_planes=(
                _plane("raw_values", "native_uint32_with_sentinels", artifact.raw_values),
            ),
            support_start=float(product["interval_start"]),
            support_end=float(product["interval_end"]),
            representative_at=float(product["product_time"]),
            source_quantity="precipitation_accumulation_5_minute",
            source_unit="mm",
            quality_state="unavailable:no_native_quality_plane",
            # The artifact's bbox is reconstructed from projected WGS84
            # corners with a documented 0.25 m tolerance.  It is excellent
            # native coverage evidence, but not an exact outer-edge origin
            # for AnalysisGrid; inventing one here would be a geometry guess.
            analysis_grid=None,
        )


def default_radar_semantics_certificates() -> tuple[RadarFieldSemanticsCertificate, ...]:
    """Build the closed allowlist from the exact decoder constants.

    Lazy imports keep this bridge absent from normal worker startup.  The
    returned certificate hashes change whenever a decoder/config identity
    changes, forcing explicit review instead of silently accepting drift.
    """

    from . import dwd_rv_quantitative as dwd
    from . import knmi_rtcor_quantitative as knmi
    from . import met_office_rate_quantitative as met_office
    from . import noaa_err_native as noaa_native
    from . import noaa_err_quantitative as noaa

    return (
        RadarFieldSemanticsCertificate(
            certificate_id="knmi.rtcor.accumulation-to-interval-mean-rate.v1",
            provider=knmi.KNMI_PROVIDER,
            product=knmi.KNMI_CAPTURE_PRODUCT,
            decoder_identity=knmi.DECODER_IDENTITY,
            decoder_version=knmi.DECODER_VERSION,
            decoder_config_sha256=knmi.DECODER_CONFIG_SHA256,
            artifact_schema=knmi.ARTIFACT_SCHEMA,
            artifact_handle_prefix=knmi.ARTIFACT_HANDLE_PREFIX,
            artifact_authority_key="knmi_rtcor_v1",
            receipt_quantity="precipitation_accumulation_5_minute",
            receipt_unit="mm",
            output_quantity="interval_mean_precipitation_rate",
            output_unit="mm/h",
            temporal_semantics=RadarTemporalSemantics.INTERVAL_MEAN_RATE,
            exact_accumulation_seconds=knmi.EXPECTED_ACCUMULATION_SECONDS,
            analysis_grid_mapping=AnalysisGridMapping.NONE,
            qc_policy="retain_native_quality_index_without_interpretation",
            primary_sources=(
                "https://essd.copernicus.org/articles/17/4715/2025/",
                "https://cdn.knmi.nl/system/data_center_publications/files/000/072/301/original/hdftag38.pdf?1739444428=",
            ),
        ),
        RadarFieldSemanticsCertificate(
            certificate_id="noaa.err.instantaneous-rate.v1",
            provider="noaa_enterprise_rainrate",
            product=noaa_native.NOAA_ERR_CAPTURE_PRODUCT,
            decoder_identity=noaa.DECODER_IDENTITY,
            decoder_version=noaa.DECODER_VERSION,
            decoder_config_sha256=noaa.DECODER_CONFIG_SHA256,
            artifact_schema=noaa.ARTIFACT_SCHEMA,
            artifact_handle_prefix=noaa.ARTIFACT_HANDLE_PREFIX,
            artifact_authority_key="noaa_err_v3",
            receipt_quantity="instantaneous_precipitation_rate",
            receipt_unit="mm/h",
            output_quantity="instantaneous_precipitation_rate",
            output_unit="mm/h",
            temporal_semantics=RadarTemporalSemantics.INSTANTANEOUS_RATE_ESTIMATE,
            exact_accumulation_seconds=None,
            analysis_grid_mapping=AnalysisGridMapping.NONE,
            qc_policy="retain_native_dqf_bits_without_quality_score",
            primary_sources=(
                "https://www.star.nesdis.noaa.gov/smcd/emb/ff/SCaMPR.php",
                "https://www.star.nesdis.noaa.gov/smcd/emb/ff/index.php",
            ),
        ),
        RadarFieldSemanticsCertificate(
            certificate_id="met-office.odim-rate.v1",
            provider=met_office.PROVIDER,
            product=met_office.CAPTURE_PRODUCT,
            decoder_identity=met_office.DECODER_IDENTITY,
            decoder_version=met_office.DECODER_VERSION,
            decoder_config_sha256=met_office.DECODER_CONFIG_SHA256,
            artifact_schema=met_office.ARTIFACT_SCHEMA,
            artifact_handle_prefix=met_office.ARTIFACT_HANDLE_PREFIX,
            artifact_authority_key="met_office_rate_v1",
            receipt_quantity="surface_precipitation_rate_estimate",
            receipt_unit="mm/h",
            output_quantity="surface_precipitation_rate_estimate",
            output_unit="mm/h",
            temporal_semantics=RadarTemporalSemantics.NOMINAL_RATE_WITH_INPUT_WINDOW,
            exact_accumulation_seconds=None,
            analysis_grid_mapping=AnalysisGridMapping.METRIC_BBOX_EDGES,
            qc_policy="quality_unavailable_must_remain_visible",
            primary_sources=(
                "https://registry.opendata.aws/met-office-uk-radar-observations/",
                "https://www.eumetnet.eu/wp-content/uploads/2017/01/OPERA_hdf_description_2014.pdf",
            ),
        ),
        RadarFieldSemanticsCertificate(
            certificate_id="dwd.rv.accumulation-to-interval-mean-rate.v1",
            provider=dwd.DWD_RV_PROVIDER,
            product=dwd.DWD_RV_CAPTURE_PRODUCT,
            decoder_identity=dwd.DECODER_IDENTITY,
            decoder_version=dwd.DECODER_VERSION,
            decoder_config_sha256=dwd.DECODER_CONFIG_SHA256,
            artifact_schema=dwd.ARTIFACT_SCHEMA,
            artifact_handle_prefix=dwd.ARTIFACT_HANDLE_PREFIX,
            artifact_authority_key="dwd_rv_v1",
            receipt_quantity="precipitation_accumulation_5_minute",
            receipt_unit="mm",
            output_quantity="interval_mean_precipitation_rate",
            output_unit="mm/h",
            temporal_semantics=RadarTemporalSemantics.INTERVAL_MEAN_RATE,
            exact_accumulation_seconds=dwd.EXPECTED_ACCUMULATION_SECONDS,
            analysis_grid_mapping=AnalysisGridMapping.NONE,
            qc_policy="quality_unavailable_must_remain_visible",
            primary_sources=(
                "https://opendata.dwd.de/weather/radar/composite/rv/",
                "https://www.dwd.de/DE/leistungen/radolan/radolan.html",
            ),
        ),
    )


def _issue(state: RadarFieldIssueState, code: str, detail: str) -> RadarFieldIssue:
    return RadarFieldIssue(state=state, code=code, detail=detail)


def _select_certificate(
    receipt: DecoderRunReceipt,
    certificates: Sequence[RadarFieldSemanticsCertificate],
) -> tuple[RadarFieldSemanticsCertificate | None, tuple[RadarFieldIssue, ...]]:
    run = receipt.run
    if run.provider == "opera" or run.decoder_identity.startswith("opera-"):
        return None, (
            _issue(
                RadarFieldIssueState.WITHHELD,
                "reflectivity_requires_uncertified_zr",
                "OPERA DBZH is reflectivity; no certified Z-R conversion exists for this bridge",
            ),
        )
    matches = tuple(item for item in certificates if item.matches(receipt))
    if len(matches) == 1:
        return matches[0], ()
    if len(matches) > 1:
        return None, (
            _issue(
                RadarFieldIssueState.INVALID,
                "semantics_certificate_ambiguous",
                "multiple exact semantics certificates match the decoder receipt",
            ),
        )
    identity_matches = tuple(
        item
        for item in certificates
        if item.provider == run.provider and item.product == run.product
    )
    drift = []
    for item in identity_matches[:4]:
        if item.decoder_identity != run.decoder_identity:
            drift.append("decoder_identity")
        if item.decoder_version != run.decoder_version:
            drift.append("decoder_version")
        if item.decoder_config_sha256 != run.config_sha256:
            drift.append("decoder_config_sha256")
        if item.receipt_quantity != run.quantity:
            drift.append("quantity")
        if item.receipt_unit != run.unit:
            drift.append("unit")
    detail = (
        "no certificate exists for this provider/product"
        if not identity_matches
        else "certificate mismatch fields: " + ",".join(sorted(set(drift)))
    )
    return None, (
        _issue(
            RadarFieldIssueState.WITHHELD,
            "semantics_certificate_missing_or_drifted",
            detail,
        ),
    )


def _metadata_capture(metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    capture = metadata.get("capture")
    if not isinstance(capture, Mapping):
        raise RadarArtifactAuthorityError("artifact capture metadata is missing")
    return capture


def _metadata_grid(metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    grid = metadata.get("grid")
    if not isinstance(grid, Mapping):
        raise RadarArtifactAuthorityError("artifact grid metadata is missing")
    return grid


def _numeric_equal(left: object, right: float) -> bool:
    return (
        not isinstance(left, bool)
        and isinstance(left, (int, float))
        and math.isfinite(float(left))
        and float(left) == float(right)
    )


def _validate_artifact_binding(
    receipt: DecoderRunReceipt,
    certificate: RadarFieldSemanticsCertificate,
    artifact: VerifiedProviderRaster,
) -> tuple[RadarFieldIssue, ...]:
    issues: list[RadarFieldIssue] = []
    run = receipt.run
    metadata = artifact.artifact_metadata
    if artifact.artifact_schema != certificate.artifact_schema:
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_schema_mismatch",
            "provider artifact schema differs from the semantics certificate",
        ))
    decoder = metadata.get("decoder")
    expected_decoder = {
        "config_sha256": run.config_sha256,
        "identity": run.decoder_identity,
        "version": run.decoder_version,
    }
    if not isinstance(decoder, Mapping) or dict(decoder) != expected_decoder:
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_decoder_binding_mismatch",
            "artifact decoder identity/version/config differs from canonical evidence",
        ))
    try:
        capture = _metadata_capture(metadata)
        manifest_value = capture.get(
            "capture_manifest_sha256", capture.get("manifest_sha256")
        )
        if (
            capture.get("capture_id") != run.capture_id
            or capture.get("capture_receipt_sha256") != run.capture_receipt_sha256
            or manifest_value != receipt.capture_manifest_sha256
        ):
            issues.append(_issue(
                RadarFieldIssueState.INVALID,
                "artifact_capture_binding_mismatch",
                "artifact capture/receipt/manifest differs from raw-reverified canonical evidence",
            ))
        if "provider" in capture and capture.get("provider") != run.provider:
            issues.append(_issue(
                RadarFieldIssueState.INVALID,
                "artifact_provider_binding_mismatch",
                "artifact provider differs from canonical evidence",
            ))
        if "product" in capture and capture.get("product") != run.product:
            issues.append(_issue(
                RadarFieldIssueState.INVALID,
                "artifact_product_binding_mismatch",
                "artifact product differs from canonical evidence",
            ))
        if "product_timestamp" in capture and not _numeric_equal(
            capture.get("product_timestamp"), float(run.product_time)  # type: ignore[arg-type]
        ):
            issues.append(_issue(
                RadarFieldIssueState.INVALID,
                "artifact_product_time_binding_mismatch",
                "artifact product timestamp differs from canonical evidence",
            ))
    except (TypeError, ValueError):
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_capture_metadata_invalid",
            "artifact capture metadata cannot be validated",
        ))
    if run.native_grid is None or run.coverage is None or run.native_crs is None:
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "canonical_native_geometry_missing",
            "decoded_complete receipt lacks native CRS/grid/coverage",
        ))
        return tuple(issues)
    try:
        grid = _metadata_grid(metadata)
        width = grid.get("width", grid.get("columns"))
        height = grid.get("height", grid.get("rows"))
        grid_sha = grid.get("definition_sha256", grid.get("grid_definition_sha256"))
        coverage_sha = grid.get("coverage_definition_sha256")
        artifact_crs = grid.get(
            "projection",
            grid.get("projdef", grid.get("proj4", grid.get("native_crs"))),
        )
        if (
            width != run.native_grid.width
            or height != run.native_grid.height
            or grid_sha != run.native_grid.definition_sha256
            or coverage_sha != run.coverage.definition_sha256
            or artifact_crs != run.native_crs
        ):
            issues.append(_issue(
                RadarFieldIssueState.INVALID,
                "artifact_grid_binding_mismatch",
                "artifact CRS/dimensions/grid/coverage identity differs from canonical evidence",
            ))
        coverage = grid.get("coverage")
        if isinstance(coverage, Mapping):
            actual = (
                coverage.get("minimum_x"),
                coverage.get("minimum_y"),
                coverage.get("maximum_x"),
                coverage.get("maximum_y"),
            )
        else:
            actual = (
                grid.get("minimum_longitude"),
                grid.get("minimum_latitude"),
                grid.get("maximum_longitude"),
                grid.get("maximum_latitude"),
            )
        expected = (
            run.coverage.minimum_x,
            run.coverage.minimum_y,
            run.coverage.maximum_x,
            run.coverage.maximum_y,
        )
        if any(not _numeric_equal(value, target) for value, target in zip(actual, expected)):
            issues.append(_issue(
                RadarFieldIssueState.INVALID,
                "artifact_coverage_bounds_mismatch",
                "artifact native bounds differ from canonical evidence",
            ))
    except (TypeError, ValueError):
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_grid_metadata_invalid",
            "artifact grid metadata cannot be validated",
        ))
    if (
        artifact.source_quantity != certificate.receipt_quantity
        or artifact.source_unit != certificate.receipt_unit
    ):
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_quantity_or_unit_mismatch",
            "artifact physical quantity/unit differs from the exact certificate",
        ))
    if artifact.representative_at != run.product_time:
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_representative_time_mismatch",
            "artifact representative time differs from canonical product time",
        ))
    if (
        not all(math.isfinite(value) for value in (
            artifact.support_start,
            artifact.support_end,
            artifact.representative_at,
        ))
        or artifact.support_end < artifact.support_start
        or not artifact.support_start
        <= artifact.representative_at
        <= artifact.support_end
    ):
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_temporal_support_invalid",
            "artifact time support is non-finite, inverted or excludes its valid time",
        ))
    if certificate.temporal_semantics is RadarTemporalSemantics.INTERVAL_MEAN_RATE:
        expected_seconds = certificate.exact_accumulation_seconds
        if artifact.support_end - artifact.support_start != expected_seconds:
            issues.append(_issue(
                RadarFieldIssueState.WITHHELD,
                "accumulation_interval_not_exact",
                "accumulation cannot be converted because exact temporal support differs",
            ))
    if certificate.analysis_grid_mapping is AnalysisGridMapping.METRIC_BBOX_EDGES:
        if artifact.analysis_grid is None:
            issues.append(_issue(
                RadarFieldIssueState.WITHHELD,
                "exact_metric_analysis_grid_missing",
                "certificate requires an exact native metric grid mapping",
            ))
        else:
            expected_edges = (
                run.coverage.minimum_x,
                run.coverage.minimum_y,
                run.coverage.maximum_x,
                run.coverage.maximum_y,
            )
            if artifact.analysis_grid.extent_edges_m != expected_edges:
                issues.append(_issue(
                    RadarFieldIssueState.INVALID,
                    "analysis_grid_extent_mismatch",
                    "AnalysisGrid edges differ from the canonical native coverage",
                ))
            if (
                artifact.analysis_grid.crs != run.native_crs
                or artifact.analysis_grid.width != run.native_grid.width
                or artifact.analysis_grid.height != run.native_grid.height
            ):
                issues.append(_issue(
                    RadarFieldIssueState.INVALID,
                    "analysis_grid_crs_or_shape_mismatch",
                    "AnalysisGrid CRS or dimensions differ from canonical native geometry",
                ))
    elif artifact.analysis_grid is not None:
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "uncertified_analysis_grid_mapping",
            "artifact reader supplied AnalysisGrid without a certified exact mapping",
        ))
    return tuple(issues)


def _validate_arrays(
    receipt: DecoderRunReceipt,
    artifact: VerifiedProviderRaster,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, tuple[RadarFieldIssue, ...]]:
    run = receipt.run
    assert run.native_grid is not None
    shape = (run.native_grid.height, run.native_grid.width)
    issues: list[RadarFieldIssue] = []
    arrays = (
        artifact.source_values,
        artifact.valid_mask,
        artifact.nodata_mask,
        artifact.undetect_mask,
        *(plane.values for plane in artifact.auxiliary_planes),
    )
    if len(artifact.auxiliary_planes) > HARD_MAX_AUXILIARY_PLANES:
        raise RadarFieldBridgeCapacityError("auxiliary plane bound exceeded")
    if run.native_grid.width * run.native_grid.height > HARD_MAX_NATIVE_PIXELS:
        raise RadarFieldBridgeCapacityError("native raster pixel bound exceeded")
    total_bytes = 0
    seen_arrays: set[int] = set()
    for array in arrays:
        if not isinstance(array, np.ndarray) or array.ndim != 2 or array.shape != shape:
            issues.append(_issue(
                RadarFieldIssueState.INVALID,
                "artifact_array_shape_mismatch",
                "one or more artifact arrays differ from canonical native dimensions",
            ))
            break
        identity = id(array)
        if identity not in seen_arrays:
            seen_arrays.add(identity)
            total_bytes += int(array.nbytes)
    if total_bytes > HARD_MAX_ARRAY_BYTES:
        raise RadarFieldBridgeCapacityError("artifact array byte bound exceeded")
    if issues:
        empty = _readonly_owned(np.zeros(shape, dtype=np.bool_), dtype=np.bool_)
        return empty, empty, empty, tuple(issues)
    try:
        valid = _mask(artifact.valid_mask, "valid")
        nodata = _mask(artifact.nodata_mask, "nodata")
        undetect = _mask(artifact.undetect_mask, "undetect")
    except RadarArtifactAuthorityError as exc:
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_mask_invalid",
            str(exc),
        ))
        empty = _readonly_owned(np.zeros(shape, dtype=np.bool_), dtype=np.bool_)
        return empty, empty, empty, tuple(issues)
    if np.any(nodata & undetect) or not np.array_equal(valid, ~(nodata | undetect)):
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_masks_not_exhaustive",
            "valid, nodata and undetect masks are overlapping or incomplete",
        ))
    source = np.asarray(artifact.source_values)
    if (
        source.dtype.kind not in "iuf"
        or not np.isfinite(source[valid]).all()
        or np.any(source[valid] < 0)
    ):
        issues.append(_issue(
            RadarFieldIssueState.INVALID,
            "artifact_physical_values_invalid",
            "valid source values must be finite and non-negative",
        ))
    return valid, nodata, undetect, tuple(issues)


def _candidate_id(
    receipt: DecoderRunReceipt,
    certificate: RadarFieldSemanticsCertificate,
    artifact_metadata_sha256: str,
) -> str:
    digest = hashlib.sha256(_canonical_json({
        "algorithm": RADAR_FIELD_BRIDGE_ALGORITHM,
        "artifact_metadata_sha256": artifact_metadata_sha256,
        "artifact_sha256": receipt.run.non_operational_candidate_artifact.sha256,  # type: ignore[union-attr]
        "certificate_sha256": certificate.fingerprint,
        "canonical_receipt_sha256": receipt.receipt_sha256,
    })).hexdigest()
    return f"radar-field-{digest}"


def _metric_field(
    *,
    candidate_id: str,
    valid_at: float,
    quantity: str,
    grid: AnalysisGrid | None,
    rates: np.ndarray,
    valid: np.ndarray,
    materialize: bool,
    max_pixels: int,
) -> MetricRasterField | None:
    if not materialize or grid is None:
        return None
    pixels = grid.width * grid.height
    if (
        isinstance(max_pixels, bool)
        or not isinstance(max_pixels, int)
        or max_pixels < 1
        or max_pixels > HARD_MAX_METRIC_FIELD_PIXELS
    ):
        raise RadarFieldBridgeCapacityError("MetricRasterField pixel bound is invalid")
    if pixels > max_pixels:
        raise RadarFieldBridgeCapacityError("MetricRasterField materialization bound exceeded")
    values = tuple(
        tuple(float(value) if bool(is_valid) else 0.0 for value, is_valid in zip(row, mask_row))
        for row, mask_row in zip(rates, valid)
    )
    mask = tuple(tuple(bool(value) for value in row) for row in valid)
    return MetricRasterField(
        field_id=candidate_id,
        valid_at=valid_at,
        quantity=quantity,
        unit="mm/h",
        grid=grid,
        values=values,
        valid_mask=mask,
    )


def derive_canonical_radar_field(
    evidence_store: CanonicalEvidenceStore,
    artifact_authority: RadarDerivedArtifactAuthority,
    *,
    run_id: str,
    certificates: Sequence[RadarFieldSemanticsCertificate] | None = None,
    materialize_metric_field: bool = False,
    max_metric_field_pixels: int = 100_000,
    deadline_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> RadarFieldBridgeResult:
    """Derive one unadmitted field only through raw-reverified store loading."""

    if not isinstance(evidence_store, CanonicalEvidenceStore):
        raise TypeError("evidence_store must be CanonicalEvidenceStore")
    if not isinstance(artifact_authority, RadarDerivedArtifactAuthority):
        raise TypeError("artifact_authority must implement RadarDerivedArtifactAuthority")
    if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
        raise ValueError("run_id is invalid")
    active_deadline = _deadline(deadline_at, monotonic)
    _check_deadline(active_deadline, monotonic)
    selected_certificates = (
        default_radar_semantics_certificates()
        if certificates is None
        else tuple(certificates)
    )
    if (
        len(selected_certificates) > HARD_MAX_CERTIFICATES
        or any(
            not isinstance(item, RadarFieldSemanticsCertificate)
            for item in selected_certificates
        )
    ):
        raise RadarFieldBridgeCapacityError("semantics certificate set is invalid")
    _check_deadline(active_deadline, monotonic)
    try:
        receipt = evidence_store.load(run_id)
    except (CanonicalEvidenceError, OSError, TypeError, ValueError) as exc:
        return RadarFieldBridgeResult(
            run_id=run_id,
            candidate=None,
            issues=(
                _issue(
                    RadarFieldIssueState.INVALID,
                    "canonical_raw_reverification_failed",
                    f"canonical evidence/raw authority failed verification: {type(exc).__name__}",
                ),
            ),
        )
    _check_deadline(active_deadline, monotonic)
    if receipt is None:
        return RadarFieldBridgeResult(
            run_id=run_id,
            candidate=None,
            issues=(
                _issue(
                    RadarFieldIssueState.WITHHELD,
                    "canonical_decoder_run_missing",
                    "canonical decoder run does not exist",
                ),
            ),
        )
    run = receipt.run
    if run.outcome != "decoded_complete":
        return RadarFieldBridgeResult(
            run_id=run_id,
            candidate=None,
            issues=(
                _issue(
                    RadarFieldIssueState.WITHHELD,
                    "decoder_outcome_not_complete",
                    f"decoder outcome {run.outcome} retained with reason {run.reason_code}",
                ),
            ),
        )
    certificate, certificate_issues = _select_certificate(
        receipt, selected_certificates
    )
    if certificate is None:
        return RadarFieldBridgeResult(run_id, None, certificate_issues)
    if (
        run.non_operational_candidate_artifact is None
        or run.native_grid is None
        or run.coverage is None
        or run.native_crs is None
        or run.product_time is None
    ):
        return RadarFieldBridgeResult(
            run_id,
            None,
            (
                _issue(
                    RadarFieldIssueState.INVALID,
                    "decoded_complete_contract_incomplete",
                    "decoded_complete lacks artifact, time or native geometry",
                ),
            ),
        )
    try:
        artifact = artifact_authority.load_verified(
            receipt,
            certificate,
            deadline_at=active_deadline,
            monotonic=monotonic,
        )
    except RadarFieldBridgeDeadlineError:
        raise
    except (RadarFieldBridgeError, RuntimeError, OSError, TypeError, ValueError) as exc:
        return RadarFieldBridgeResult(
            run_id,
            None,
            (
                _issue(
                    RadarFieldIssueState.INVALID,
                    "derived_artifact_verification_failed",
                    f"derived artifact failed verification: {type(exc).__name__}",
                ),
            ),
        )
    _check_deadline(active_deadline, monotonic)
    binding_issues = _validate_artifact_binding(receipt, certificate, artifact)
    valid, nodata, undetect, array_issues = _validate_arrays(receipt, artifact)
    issues = binding_issues + array_issues
    if issues:
        return RadarFieldBridgeResult(run_id, None, issues)

    source = _readonly(artifact.source_values)
    rates = np.array(source, dtype=np.float64, copy=True, order="C")
    if certificate.temporal_semantics is RadarTemporalSemantics.INTERVAL_MEAN_RATE:
        assert certificate.exact_accumulation_seconds is not None
        rates *= 3600.0 / certificate.exact_accumulation_seconds
        conversion = (
            "exact_interval_mean_rate=accumulation_mm*3600/"
            f"{certificate.exact_accumulation_seconds}"
        )
    else:
        conversion = "identity_mm_h"
    rates[~valid] = np.nan
    rates.setflags(write=False)
    _check_deadline(active_deadline, monotonic)

    metadata_sha = hashlib.sha256(
        _canonical_json(artifact.artifact_metadata)
    ).hexdigest()
    candidate_id = _candidate_id(receipt, certificate, metadata_sha)
    metric = _metric_field(
        candidate_id=candidate_id,
        valid_at=artifact.representative_at,
        quantity=certificate.output_quantity,
        grid=artifact.analysis_grid,
        rates=rates,
        valid=valid,
        materialize=materialize_metric_field,
        max_pixels=max_metric_field_pixels,
    )
    candidate = NativeRainRateFieldCandidate(
        candidate_id=candidate_id,
        canonical_run_id=receipt.run_id,
        canonical_commit_id=receipt.commit_id,
        canonical_receipt_sha256=receipt.receipt_sha256,
        canonical_result_sha256=receipt.result_sha256,
        capture_id=run.capture_id,
        capture_receipt_sha256=run.capture_receipt_sha256,
        capture_manifest_sha256=receipt.capture_manifest_sha256,
        artifact_handle=run.non_operational_candidate_artifact.handle,
        artifact_sha256=run.non_operational_candidate_artifact.sha256,
        artifact_metadata_sha256=metadata_sha,
        semantics_certificate_id=certificate.certificate_id,
        semantics_certificate_sha256=certificate.fingerprint,
        provider=run.provider,
        product=run.product,
        decoder_identity=run.decoder_identity,
        decoder_version=run.decoder_version,
        decoder_config_sha256=run.config_sha256,
        source_quantity=certificate.receipt_quantity,
        source_unit=certificate.receipt_unit,
        source_values=source,
        output_quantity=certificate.output_quantity,
        output_unit=certificate.output_unit,
        rain_rate_mm_h=rates,
        valid_mask=valid,
        nodata_mask=nodata,
        undetect_mask=undetect,
        auxiliary_planes=artifact.auxiliary_planes,
        temporal_semantics=certificate.temporal_semantics,
        support_start=artifact.support_start,
        support_end=artifact.support_end,
        valid_at=artifact.representative_at,
        conversion=conversion,
        quality_state=artifact.quality_state,
        native_crs=run.native_crs,
        native_grid=run.native_grid,
        native_coverage=run.coverage,
        analysis_grid=artifact.analysis_grid,
        metric_raster_field=metric,
        primary_sources=certificate.primary_sources,
    )
    _check_deadline(active_deadline, monotonic)
    return RadarFieldBridgeResult(run_id=run_id, candidate=candidate, issues=())


__all__ = [
    "AnalysisGridMapping",
    "DEFAULT_BRIDGE_DEADLINE_SECONDS",
    "HARD_MAX_ARRAY_BYTES",
    "HARD_MAX_METRIC_FIELD_PIXELS",
    "HARD_MAX_NATIVE_PIXELS",
    "NativeRainRateFieldCandidate",
    "NativeRasterPlane",
    "ProviderDerivedArtifactAuthority",
    "RADAR_FIELD_ADMISSION_STATUS",
    "RADAR_FIELD_BRIDGE_ALGORITHM",
    "RadarArtifactAuthorityError",
    "RadarDerivedArtifactAuthority",
    "RadarFieldBridgeCapacityError",
    "RadarFieldBridgeDeadlineError",
    "RadarFieldBridgeError",
    "RadarFieldBridgeResult",
    "RadarFieldIssue",
    "RadarFieldIssueState",
    "RadarFieldSemanticsCertificate",
    "RadarTemporalSemantics",
    "VerifiedProviderRaster",
    "default_radar_semantics_certificates",
    "derive_canonical_radar_field",
]
