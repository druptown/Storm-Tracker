"""Closed JSON codec for the immutable CellTrack engine state algebra.

No module, class or callable name is accepted from data.  The decoder uses a
fixed in-process type table and validates every reconstructed dataclass through
its normal constructor.  This small shared boundary lets both the scientific
replay store and the worker-only restart store use the same byte semantics.
"""
from __future__ import annotations

from dataclasses import fields, is_dataclass
import math
from typing import Any

from .advected_footprint_evidence import AdvectedFootprintEvidence
from .analysis_grid import AnalysisGrid
from .cell_track import CellDetection
from .cell_track_engine import CellTrackEngineState
from .cell_track_lifecycle import (
    CellTrackLedger,
    CellTrackLifecycleEvent,
    ImmutableCellTrack,
)
from .hierarchical_detection import (
    HierarchicalCellDetection,
    HierarchicalDetectionFrame,
    NormalizedRasterFrame,
)
from .multi_hypothesis_identity import (
    IdentityAssignmentHypothesis,
    LocalCellDeformationCandidate,
    MultiHypothesisIdentitySnapshot,
    MultiScanBranch,
    MultiScanBranchStep,
    MultiScanIdentityPosterior,
    MultiScanLineageAlternative,
    MultiScanTrackPath,
    SystemTranslationEvidence,
)
from .recursive_track_estimator import RecursiveTrackEstimate
from .weather_system_grouping import (
    DescriptiveSystemMotion,
    WeatherSystemConnectionEvidence,
    WeatherSystemGroupingPolicy,
    WeatherSystemGroupingSnapshot,
    WeatherSystemMembershipEvent,
    WeatherSystemPredecessorCandidate,
    WeatherSystemSnapshot,
    WeatherSystemTrackInput,
)


MAX_STATE_CODEC_DEPTH = 96
MAX_STATE_CODEC_NODES = 5_000_000
MAX_STATE_CODEC_STRING_BYTES = 8_192


class CellTrackStateCodecError(RuntimeError):
    """Encoded state is unsupported, invalid or internally inconsistent."""


class CellTrackStateCodecCapacityError(CellTrackStateCodecError):
    """Encoded state exceeds a fixed structural or scalar bound."""


_STATE_CODEC_TYPES = {
    item.__name__: item
    for item in (
        AnalysisGrid,
        AdvectedFootprintEvidence,
        CellDetection,
        CellTrackEngineState,
        CellTrackLedger,
        CellTrackLifecycleEvent,
        DescriptiveSystemMotion,
        HierarchicalCellDetection,
        HierarchicalDetectionFrame,
        ImmutableCellTrack,
        IdentityAssignmentHypothesis,
        LocalCellDeformationCandidate,
        MultiHypothesisIdentitySnapshot,
        MultiScanBranch,
        MultiScanBranchStep,
        MultiScanIdentityPosterior,
        MultiScanLineageAlternative,
        MultiScanTrackPath,
        NormalizedRasterFrame,
        RecursiveTrackEstimate,
        SystemTranslationEvidence,
        WeatherSystemConnectionEvidence,
        WeatherSystemGroupingPolicy,
        WeatherSystemGroupingSnapshot,
        WeatherSystemMembershipEvent,
        WeatherSystemPredecessorCandidate,
        WeatherSystemSnapshot,
        WeatherSystemTrackInput,
    )
}


def encode_cell_track_state_value(
    value: Any,
    *,
    _depth: int = 0,
    _nodes: list[int] | None = None,
) -> Any:
    """Encode only the closed CellTrack state algebra to JSON primitives."""

    if _nodes is None:
        _nodes = [0]
    _nodes[0] += 1
    if _nodes[0] > MAX_STATE_CODEC_NODES or _depth > MAX_STATE_CODEC_DEPTH:
        raise CellTrackStateCodecCapacityError(
            "state codec structural bound exceeded"
        )
    if value is None or type(value) is bool:
        return value
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_STATE_CODEC_STRING_BYTES or "\0" in value:
            raise CellTrackStateCodecCapacityError(
                "state codec string bound exceeded"
            )
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        if not -(2**63) <= value <= 2**63 - 1:
            raise CellTrackStateCodecCapacityError(
                "state codec integer bound exceeded"
            )
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CellTrackStateCodecError(
                "state codec refuses non-finite values"
            )
        return value
    if isinstance(value, tuple):
        return [
            encode_cell_track_state_value(
                item,
                _depth=_depth + 1,
                _nodes=_nodes,
            )
            for item in value
        ]
    value_type = type(value)
    if is_dataclass(value) and value_type.__name__ in _STATE_CODEC_TYPES:
        if _STATE_CODEC_TYPES[value_type.__name__] is not value_type:
            raise CellTrackStateCodecError("state codec type-name collision")
        return {
            "fields": {
                item.name: encode_cell_track_state_value(
                    getattr(value, item.name),
                    _depth=_depth + 1,
                    _nodes=_nodes,
                )
                for item in fields(value)
            },
            "type": value_type.__name__,
        }
    raise CellTrackStateCodecError(
        f"state codec refuses unsupported type {value_type.__name__}"
    )


def decode_cell_track_state_value(
    value: Any,
    *,
    _depth: int = 0,
    _nodes: list[int] | None = None,
) -> Any:
    """Decode the closed algebra without data-driven imports or callables."""

    if _nodes is None:
        _nodes = [0]
    _nodes[0] += 1
    if _nodes[0] > MAX_STATE_CODEC_NODES or _depth > MAX_STATE_CODEC_DEPTH:
        raise CellTrackStateCodecCapacityError(
            "state codec structural bound exceeded"
        )
    if value is None or type(value) is bool:
        return value
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_STATE_CODEC_STRING_BYTES or "\0" in value:
            raise CellTrackStateCodecCapacityError(
                "state codec string bound exceeded"
            )
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        if not -(2**63) <= value <= 2**63 - 1:
            raise CellTrackStateCodecCapacityError(
                "state codec integer bound exceeded"
            )
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CellTrackStateCodecError(
                "state codec contains non-finite value"
            )
        return value
    if isinstance(value, list):
        return tuple(
            decode_cell_track_state_value(
                item,
                _depth=_depth + 1,
                _nodes=_nodes,
            )
            for item in value
        )
    if not isinstance(value, dict) or set(value) != {"fields", "type"}:
        raise CellTrackStateCodecError(
            "state codec mapping has an unknown schema"
        )
    type_name = value["type"]
    field_values = value["fields"]
    if not isinstance(type_name, str) or type_name not in _STATE_CODEC_TYPES:
        raise CellTrackStateCodecError("state codec type is not admitted")
    value_type = _STATE_CODEC_TYPES[type_name]
    expected_fields = tuple(item.name for item in fields(value_type))
    if not isinstance(field_values, dict) or set(field_values) != set(expected_fields):
        raise CellTrackStateCodecError("state codec fields are not exact")
    decoded = {
        name: decode_cell_track_state_value(
            field_values[name],
            _depth=_depth + 1,
            _nodes=_nodes,
        )
        for name in expected_fields
    }
    try:
        return value_type(**decoded)
    except (KeyError, TypeError, ValueError) as exc:
        raise CellTrackStateCodecError(
            "state codec object failed validation"
        ) from exc


__all__ = [
    "CellTrackStateCodecCapacityError",
    "CellTrackStateCodecError",
    "MAX_STATE_CODEC_DEPTH",
    "MAX_STATE_CODEC_NODES",
    "MAX_STATE_CODEC_STRING_BYTES",
    "decode_cell_track_state_value",
    "encode_cell_track_state_value",
]
