"""Official, fail-closed DWD RV DE1200 native-grid certificate issuer.

This module is deliberately worker-only, shadow-only and unwired.  It does not
reproject anything.  It can certify only the exact DE1200 pixel-edge mapping in
the already decoded DWD native CRS, after the complete bridge candidate and its
semantics certificate have been checksum-bound by the caller.

The authority chain is deliberately split:

* DWD defines the WGS84 DE1200 projection, dimensions, one-kilometre cells and
  the centre of the first north-west pixel;
* ODIM_H5 2.3 defines the cartesian array order and the four corner attributes
  as the outer corners of the respective outer pixels;
* the DWD quantitative decoder accepts only that ODIM profile and reconstructs
  the four projected outer edges before committing canonical evidence.

No generic CRS inference, coordinate lookup or operational admission is
available here.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any, Mapping

import numpy as np

from . import dwd_rv_quantitative as dwd
from .analysis_grid_transform import (
    AffineGridTransformCertificate,
    AnalysisGridTransformConfig,
    NativeFieldCandidateBinding,
    QualityPlaneResamplingMethod,
    QualityPlaneTransformRule,
    TransformEvidenceClass,
    native_candidate_content_sha256,
)
from .canonical_radar_field_bridge import (
    RADAR_FIELD_ADMISSION_STATUS,
    RADAR_FIELD_BRIDGE_ALGORITHM,
    NativeRainRateFieldCandidate,
    RadarTemporalSemantics,
    default_radar_semantics_certificates,
)
from .tracking_core.engine.analysis_grid import AnalysisGrid


DWD_RV_GRID_CERTIFICATE_VERSION = "dwd-rv-de1200-native-grid-certificate-v1"
DWD_RV_GRID_ADAPTER_ID = "dwd.rv.de1200.native-affine"
DWD_RV_GRID_ADAPTER_VERSION = "v1"
DWD_RV_FORMAT_URL = (
    "https://www.dwd.de/DE/leistungen/radarprodukte/"
    "formatbeschreibung_rv.pdf?__blob=publicationFile&v=3"
)
DWD_RV_HDF5_PROFILE_URL = (
    "https://www.dwd.de/DE/leistungen/radarprodukte/"
    "radarkomposit_rv_hdf5.pdf?__blob=publicationFile&v=3"
)
ODIM_H5_V23_URL = (
    "https://www.eumetnet.eu/wp-content/uploads/2019/01/ODIM_H5_v23.pdf"
)

SOURCE_WIDTH = 1100
SOURCE_HEIGHT = 1200
SOURCE_X_RESOLUTION_M = 1000.0
SOURCE_Y_RESOLUTION_M = 1000.0
SOURCE_WEST_EDGE_M = -500.0
SOURCE_NORTH_EDGE_M = 500.0
SOURCE_EAST_EDGE_M = 1_099_500.0
SOURCE_SOUTH_EDGE_M = -1_199_500.0

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_PROJECTION_TEXT = {"proj": "stere", "units": "m"}
_EXPECTED_PROJECTION_FLAGS = {"no_defs"}
_EXPECTED_PROJECTION_NUMERIC = {
    "lat_ts": (60.0, 1e-9),
    "lat_0": (90.0, 1e-9),
    "lon_0": (10.0, 1e-9),
    "x_0": (543196.83521776402, 1e-6),
    "y_0": (3622588.8619310018, 1e-6),
    "a": (6378137.0, 1e-9),
    "b": (6356752.3142451802, 1e-6),
}


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


DWD_RV_GRID_AUTHORITY: Mapping[str, Any] = {
    "authority": "DWD RV format v1.01 and EUMETNET OPERA ODIM_H5 2.3",
    "certificate_version": DWD_RV_GRID_CERTIFICATE_VERSION,
    "facts": {
        "array_order": "upper_left_row_major_north_to_south_west_to_east",
        "corner_semantics": "outer_corners_of_outer_pixels",
        "earth_model": "WGS84_ellipsoid",
        "first_north_west_pixel_centre_projected_m": [0.0, 0.0],
        "height": SOURCE_HEIGHT,
        "outer_edges_projected_m": {
            "east": SOURCE_EAST_EDGE_M,
            "north": SOURCE_NORTH_EDGE_M,
            "south": SOURCE_SOUTH_EDGE_M,
            "west": SOURCE_WEST_EDGE_M,
        },
        "pixel_size_m": [SOURCE_X_RESOLUTION_M, SOURCE_Y_RESOLUTION_M],
        "width": SOURCE_WIDTH,
    },
    "primary_sources": [
        DWD_RV_FORMAT_URL,
        DWD_RV_HDF5_PROFILE_URL,
        ODIM_H5_V23_URL,
    ],
    "schema": "stv3-dwd-rv-grid-authority-v1",
}
DWD_RV_GRID_EVIDENCE_SHA256 = hashlib.sha256(
    _canonical_json(DWD_RV_GRID_AUTHORITY)
).hexdigest()


class DwdRvGridCertificateError(ValueError):
    """The candidate cannot be certified as the official DE1200 edge map."""


def _fail(reason: str) -> None:
    raise DwdRvGridCertificateError(reason)


def _validate_projection(projdef: str) -> None:
    if not isinstance(projdef, str) or not projdef or len(projdef) > 512:
        _fail("native CRS is absent or unbounded")
    tokens: dict[str, str | None] = {}
    for token in projdef.split():
        if not token.startswith("+") or len(token) < 2:
            _fail("native CRS contains an invalid token")
        raw = token[1:]
        key, separator, value = raw.partition("=")
        if not key or key in tokens:
            _fail("native CRS contains a duplicate or empty key")
        tokens[key] = value if separator else None
    expected_keys = (
        set(_EXPECTED_PROJECTION_TEXT)
        | set(_EXPECTED_PROJECTION_NUMERIC)
        | _EXPECTED_PROJECTION_FLAGS
    )
    if set(tokens) != expected_keys:
        _fail("native CRS differs from the closed DWD WGS84 profile")
    if any(tokens[key] != value for key, value in _EXPECTED_PROJECTION_TEXT.items()):
        _fail("native CRS text parameters differ from DWD")
    if any(tokens[key] is not None for key in _EXPECTED_PROJECTION_FLAGS):
        _fail("native CRS flags differ from DWD")
    for key, (expected, tolerance) in _EXPECTED_PROJECTION_NUMERIC.items():
        try:
            actual = float(str(tokens[key]))
        except ValueError:
            _fail(f"native CRS parameter {key} is not numeric")
        if not math.isfinite(actual) or not math.isclose(
            actual, expected, rel_tol=0.0, abs_tol=tolerance
        ):
            _fail(f"native CRS parameter {key} differs from DWD")


def _official_semantics_certificate():
    matches = tuple(
        item
        for item in default_radar_semantics_certificates()
        if item.provider == dwd.DWD_RV_PROVIDER
        and item.product == dwd.DWD_RV_CAPTURE_PRODUCT
    )
    if len(matches) != 1:
        raise RuntimeError("closed DWD semantics certificate is absent or ambiguous")
    return matches[0]


def _validate_candidate(
    binding: NativeFieldCandidateBinding,
    target_grid: AnalysisGrid,
    config: AnalysisGridTransformConfig,
) -> NativeRainRateFieldCandidate:
    if not isinstance(binding, NativeFieldCandidateBinding):
        raise TypeError("binding must be NativeFieldCandidateBinding")
    if not isinstance(target_grid, AnalysisGrid):
        raise TypeError("target_grid must be AnalysisGrid")
    if not isinstance(config, AnalysisGridTransformConfig):
        raise TypeError("config must be AnalysisGridTransformConfig")
    candidate = binding.candidate
    actual_content_sha = native_candidate_content_sha256(candidate)
    if actual_content_sha != binding.candidate_content_sha256:
        _fail("candidate content differs from the caller binding")
    if binding.semantics_certificate_sha256 != candidate.semantics_certificate_sha256:
        _fail("candidate semantics differs from the caller binding")

    semantics = _official_semantics_certificate()
    exact_pairs = (
        (candidate.admission_status, RADAR_FIELD_ADMISSION_STATUS),
        (candidate.algorithm_version, RADAR_FIELD_BRIDGE_ALGORITHM),
        (candidate.provider, dwd.DWD_RV_PROVIDER),
        (candidate.product, dwd.DWD_RV_CAPTURE_PRODUCT),
        (candidate.decoder_identity, dwd.DECODER_IDENTITY),
        (candidate.decoder_version, dwd.DECODER_VERSION),
        (candidate.decoder_config_sha256, dwd.DECODER_CONFIG_SHA256),
        (candidate.semantics_certificate_id, semantics.certificate_id),
        (candidate.semantics_certificate_sha256, semantics.fingerprint),
        (candidate.source_quantity, "precipitation_accumulation_5_minute"),
        (candidate.source_unit, "mm"),
        (candidate.output_quantity, "interval_mean_precipitation_rate"),
        (candidate.output_unit, "mm/h"),
        (candidate.temporal_semantics, RadarTemporalSemantics.INTERVAL_MEAN_RATE),
        (candidate.conversion, "exact_interval_mean_rate=accumulation_mm*3600/300"),
        (candidate.quality_state, "unavailable:no_native_quality_plane"),
        (candidate.primary_sources, semantics.primary_sources),
        (candidate.native_grid.kind, "regular"),
        (candidate.native_grid.axis_order, "yx"),
        (candidate.native_grid.width, SOURCE_WIDTH),
        (candidate.native_grid.height, SOURCE_HEIGHT),
        (candidate.native_coverage.kind, "native_bbox"),
        (candidate.native_coverage.minimum_x, SOURCE_WEST_EDGE_M),
        (candidate.native_coverage.minimum_y, SOURCE_SOUTH_EDGE_M),
        (candidate.native_coverage.maximum_x, SOURCE_EAST_EDGE_M),
        (candidate.native_coverage.maximum_y, SOURCE_NORTH_EDGE_M),
        (target_grid.crs, candidate.native_crs),
    )
    if any(actual != expected for actual, expected in exact_pairs):
        _fail("candidate identity, semantics, geometry or target CRS drifted")
    if candidate.support_end - candidate.support_start != 300.0:
        _fail("DWD accumulation support is not exactly 300 seconds")
    if candidate.valid_at != candidate.support_end:
        _fail("DWD representative time is not the accumulation interval end")
    _validate_projection(candidate.native_crs)
    if (
        candidate.source_values.dtype != np.dtype("<f8")
        or candidate.rain_rate_mm_h.dtype != np.dtype("<f8")
        or candidate.valid_mask.dtype != np.dtype("bool")
        or candidate.nodata_mask.dtype != np.dtype("bool")
        or candidate.undetect_mask.dtype != np.dtype("bool")
    ):
        _fail("candidate array dtypes differ from the reviewed DWD bridge")
    if len(candidate.auxiliary_planes) != 1:
        _fail("DWD auxiliary-plane contract drifted")
    plane = candidate.auxiliary_planes[0]
    if (
        plane.name != "raw_values"
        or plane.semantics != "native_uint32_with_sentinels"
        or plane.values.dtype != np.dtype("<u4")
    ):
        _fail("DWD native raw-value plane contract drifted")
    if (
        not isinstance(candidate.native_grid.definition_sha256, str)
        or _SHA256_RE.fullmatch(candidate.native_grid.definition_sha256) is None
        or not isinstance(candidate.native_coverage.definition_sha256, str)
        or _SHA256_RE.fullmatch(candidate.native_coverage.definition_sha256) is None
    ):
        _fail("candidate grid receipts are not SHA-256 bound")
    return candidate


def issue_dwd_rv_native_grid_certificate(
    *,
    binding: NativeFieldCandidateBinding,
    target_grid: AnalysisGrid,
    config: AnalysisGridTransformConfig,
) -> AffineGridTransformCertificate:
    """Issue one content-bound certificate for DWD's unchanged native CRS.

    A target grid may crop, refine or coarsen, but it must use the byte-exact
    native CRS string already bound by the DWD decoder candidate.  Reprojection
    is intentionally impossible through this issuer.
    """

    candidate = _validate_candidate(binding, target_grid, config)
    plane = candidate.auxiliary_planes[0]
    quality_rules = (
        QualityPlaneTransformRule(
            plane_name=plane.name,
            plane_semantics_sha256=hashlib.sha256(
                plane.semantics.encode("utf-8")
            ).hexdigest(),
            source_dtype=plane.values.dtype.str,
            method=QualityPlaneResamplingMethod.EXACT_AREA_CONSENSUS,
        ),
    )
    identity_sha = hashlib.sha256(_canonical_json({
        "candidate_content_sha256": binding.candidate_content_sha256,
        "evidence_sha256": DWD_RV_GRID_EVIDENCE_SHA256,
        "grid_definition_sha256": candidate.native_grid.definition_sha256,
        "coverage_definition_sha256": (
            candidate.native_coverage.definition_sha256
        ),
        "semantics_certificate_sha256": candidate.semantics_certificate_sha256,
        "target_crs": target_grid.crs,
        "transform_config_sha256": config.fingerprint,
    })).hexdigest()
    return AffineGridTransformCertificate(
        certificate_id=f"dwd.rv.de1200.pixel-edges.v1:{identity_sha}",
        adapter_id=DWD_RV_GRID_ADAPTER_ID,
        adapter_version=DWD_RV_GRID_ADAPTER_VERSION,
        transform_config_sha256=config.fingerprint,
        evidence_class=TransformEvidenceClass.OFFICIAL_EXACT_GRID_ANCHORS,
        evidence_sha256=DWD_RV_GRID_EVIDENCE_SHA256,
        evidence_authority=str(DWD_RV_GRID_AUTHORITY["authority"]),
        primary_sources=(
            DWD_RV_FORMAT_URL,
            DWD_RV_HDF5_PROFILE_URL,
            ODIM_H5_V23_URL,
        ),
        source_provider=candidate.provider,
        source_product=candidate.product,
        source_semantics_certificate_id=candidate.semantics_certificate_id,
        source_semantics_certificate_sha256=(
            candidate.semantics_certificate_sha256
        ),
        source_candidate_algorithm=candidate.algorithm_version,
        source_crs=candidate.native_crs,
        source_grid_definition_sha256=candidate.native_grid.definition_sha256,
        source_coverage_definition_sha256=(
            candidate.native_coverage.definition_sha256
        ),
        source_width=SOURCE_WIDTH,
        source_height=SOURCE_HEIGHT,
        source_array_axis_order="yx",
        target_crs=target_grid.crs,
        source_west_edge_target_m=SOURCE_WEST_EDGE_M,
        source_north_edge_target_m=SOURCE_NORTH_EDGE_M,
        source_x_resolution_target_m=SOURCE_X_RESOLUTION_M,
        source_y_resolution_target_m=SOURCE_Y_RESOLUTION_M,
        quality_plane_rules=quality_rules,
    )


__all__ = [
    "DWD_RV_FORMAT_URL",
    "DWD_RV_HDF5_PROFILE_URL",
    "DWD_RV_GRID_ADAPTER_ID",
    "DWD_RV_GRID_ADAPTER_VERSION",
    "DWD_RV_GRID_AUTHORITY",
    "DWD_RV_GRID_CERTIFICATE_VERSION",
    "DWD_RV_GRID_EVIDENCE_SHA256",
    "DwdRvGridCertificateError",
    "ODIM_H5_V23_URL",
    "SOURCE_EAST_EDGE_M",
    "SOURCE_HEIGHT",
    "SOURCE_NORTH_EDGE_M",
    "SOURCE_SOUTH_EDGE_M",
    "SOURCE_WEST_EDGE_M",
    "SOURCE_WIDTH",
    "SOURCE_X_RESOLUTION_M",
    "SOURCE_Y_RESOLUTION_M",
    "issue_dwd_rv_native_grid_certificate",
]
