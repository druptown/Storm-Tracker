"""Execute the existing deterministic nowcast baselines from immutable bytes.

The runner is intentionally narrow.  It parses the same bounded replay pack
twice, executes every adjacent frame pair through the real
:mod:`tracking_core.engine.nowcast_baselines` functions, and requires
byte-identical canonical sequences.  It does not run a service, use the
network, mutate Home Assistant, publish a forecast or grant
scientific/meteorological admission.

This closes execution evidence for two roadmap baselines only: Eulerian
persistence and two-frame linear translation.  Legacy, deterministic pySTEPS
and the last released vNext remain explicit blockers in every receipt.
"""
from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping

from .offline_replay_acceptance import (
    ArtifactRole,
    BaselineId,
    BaselineReplayEvidence,
    OfflineReplayInput,
    ScienceComponentSource,
    ScienceStage,
    StageReplayEvidence,
    audit_local_science_component_sources,
    offline_replay_input_from_mapping,
)
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.nowcast_baselines import (
    EULERIAN_PERSISTENCE_VERSION,
    TWO_FRAME_TRANSLATION_VERSION,
    BaselineForecastField,
    ConstantMotionVector,
    MetricRasterField,
    derive_two_frame_constant_motion,
    eulerian_persistence,
    lagrangian_constant_motion,
)


OFFLINE_NOWCAST_REPLAY_CONTRACT = "stv3-offline-nowcast-baseline-replay-v1"
OFFLINE_NOWCAST_RECEIPT_CONTRACT = "stv3-offline-nowcast-primary-repeat-v1"
CENTROID_MOTION_VERSION = "stv3-offline-intensity-centroid-motion-v1"
FIXTURE_KIND = "synthetic_known_motion_contract"

MINIMUM_FRAMES = 20
MAXIMUM_FRAMES = 64
MAXIMUM_GRID_PIXELS = 65_536
MAXIMUM_REPLAY_PACK_BYTES = 4 * 1024 * 1024
MAXIMUM_FRAME_PAYLOAD_BYTES = 2 * 1024 * 1024
MAXIMUM_TOTAL_FRAME_BYTES = 16 * 1024 * 1024
MAXIMUM_CANONICAL_OUTPUT_BYTES = 16 * 1024 * 1024
MAXIMUM_LEAD_SECONDS = 7_200.0

EXECUTED_BASELINES = (
    BaselineId.EULERIAN_PERSISTENCE,
    BaselineId.TWO_FRAME_TRANSLATION,
)
REMAINING_BASELINES = tuple(
    sorted(
        set(BaselineId) - set(EXECUTED_BASELINES),
        key=lambda item: item.value,
    )
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class OfflineNowcastReplayError(RuntimeError):
    """The replay byte contract is malformed, unbound or not executable."""


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_mapping(value: Any) -> str:
    return _sha_bytes(_canonical_json_bytes(value))


def _identifier(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or "\x00" in value
        or len(value) > 512
    ):
        raise ValueError(f"{field} is invalid")
    return value


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} requires lowercase SHA-256")
    return value


def _finite(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field} must be finite")
    return result


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be an object")
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{field} has unexpected/missing keys")


def _parse_json_bytes(body: bytes, *, field: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key in {field}: {key}")
            result[key] = value
        return result

    try:
        return json.loads(body, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OfflineNowcastReplayError(f"{field} is invalid JSON") from exc


def _read_regular(path: Path) -> bytes:
    if not path.is_absolute() or path.is_symlink():
        raise OfflineNowcastReplayError("replay pack path is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise OfflineNowcastReplayError("replay pack cannot be opened") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > MAXIMUM_REPLAY_PACK_BYTES
        ):
            raise OfflineNowcastReplayError("replay pack type/size is invalid")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(
                descriptor,
                min(1024 * 1024, MAXIMUM_REPLAY_PACK_BYTES + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAXIMUM_REPLAY_PACK_BYTES:
                raise OfflineNowcastReplayError("replay pack exceeds byte bound")
        after = os.fstat(descriptor)
        if (
            (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            )
            != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            )
            or total != before.st_size
        ):
            raise OfflineNowcastReplayError("replay pack changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class _DecodedFrame:
    binding_id: str
    event_id: str
    valid_at: float
    payload_sha256: str
    field: MetricRasterField


@dataclass(frozen=True, slots=True)
class _Execution:
    replay_input: OfflineReplayInput
    component_source: ScienceComponentSource
    output_mapping: Mapping[str, Any]
    output_bytes: bytes
    eulerian_mapping: Mapping[str, Any]
    translation_mapping: Mapping[str, Any]
    preregistration_source_ids: Mapping[BaselineId, str]


@dataclass(frozen=True, slots=True)
class OfflineNowcastReplayReceipt:
    replay_pack_sha256: str
    input_contract_sha256: str
    component_source: ScienceComponentSource
    stage_evidence: StageReplayEvidence
    baseline_evidence: tuple[BaselineReplayEvidence, ...]
    primary_output_json: str
    primary_output_sha256: str
    reproduction_output_sha256: str
    primary_repeat_passed: bool
    remaining_baselines: tuple[BaselineId, ...]
    blocking_reasons: tuple[str, ...]
    contract_version: str = OFFLINE_NOWCAST_RECEIPT_CONTRACT
    fixture_kind: str = FIXTURE_KIND
    real_meteorological_case: bool = False
    offline_science_gate_passed: bool = False
    meteorological_admission: bool = False
    live_wiring_allowed: bool = False
    dashboard_publication_allowed: bool = False
    home_assistant_state_mutated: bool = False

    def __post_init__(self) -> None:
        if self.contract_version != OFFLINE_NOWCAST_RECEIPT_CONTRACT:
            raise ValueError("unsupported nowcast receipt contract")
        if self.fixture_kind != FIXTURE_KIND:
            raise ValueError("unsupported fixture kind")
        for field, value in (
            ("replay_pack_sha256", self.replay_pack_sha256),
            ("input_contract_sha256", self.input_contract_sha256),
            ("primary_output_sha256", self.primary_output_sha256),
            ("reproduction_output_sha256", self.reproduction_output_sha256),
        ):
            _sha256(value, field)
        if not isinstance(self.component_source, ScienceComponentSource):
            raise TypeError("component_source requires ScienceComponentSource")
        if self.component_source.stage is not ScienceStage.NOWCAST_BASELINES:
            raise ValueError("component source is not the nowcast baseline stage")
        if not isinstance(self.stage_evidence, StageReplayEvidence):
            raise TypeError("stage_evidence requires StageReplayEvidence")
        if self.stage_evidence.stage is not ScienceStage.NOWCAST_BASELINES:
            raise ValueError("stage evidence is not the nowcast baseline stage")
        if (
            not isinstance(self.baseline_evidence, tuple)
            or any(
                not isinstance(item, BaselineReplayEvidence)
                for item in self.baseline_evidence
            )
            or tuple(item.baseline_id for item in self.baseline_evidence)
            != EXECUTED_BASELINES
        ):
            raise ValueError("receipt must contain the exact two executed baselines")
        if self.remaining_baselines != REMAINING_BASELINES:
            raise ValueError("receipt must retain the exact remaining baselines")
        if not isinstance(self.primary_output_json, str):
            raise TypeError("primary_output_json must be text")
        try:
            output_bytes = self.primary_output_json.encode("ascii")
        except UnicodeEncodeError as exc:
            raise ValueError("primary output must be canonical ASCII JSON") from exc
        if len(output_bytes) > MAXIMUM_CANONICAL_OUTPUT_BYTES:
            raise ValueError("primary output exceeds hard byte bound")
        parsed = _mapping(
            _parse_json_bytes(output_bytes, field="primary output"),
            "primary output",
        )
        if _canonical_json_bytes(parsed) != output_bytes:
            raise ValueError("primary output JSON is not canonical")
        if _sha_bytes(output_bytes) != self.primary_output_sha256:
            raise ValueError("primary output SHA differs")
        _exact_keys(
            parsed,
            {
                "baseline_outputs",
                "component_source",
                "event_ids",
                "execution_contract",
                "fixture_kind",
                "frame_count",
                "input_contract_sha256",
                "real_meteorological_case",
                "replay_pack_sha256",
            },
            "primary output",
        )
        if (
            parsed["execution_contract"] != OFFLINE_NOWCAST_REPLAY_CONTRACT
            or parsed["fixture_kind"] != FIXTURE_KIND
            or parsed["real_meteorological_case"] is not False
            or parsed["replay_pack_sha256"] != self.replay_pack_sha256
            or parsed["input_contract_sha256"] != self.input_contract_sha256
            or parsed["component_source"] != self.component_source.to_mapping()
            or parsed["event_ids"] != list(self.stage_evidence.event_ids)
            or parsed["frame_count"] != self.stage_evidence.frame_count
        ):
            raise ValueError("primary output provenance differs from receipt")
        if (
            self.stage_evidence.implementation_version
            != self.component_source.implementation_version
            or self.stage_evidence.implementation_sha256
            != self.component_source.source_sha256
            or self.stage_evidence.input_contract_sha256
            != self.input_contract_sha256
            or self.stage_evidence.primary_output_sha256
            != self.primary_output_sha256
            or self.stage_evidence.reproduction_output_sha256
            != self.reproduction_output_sha256
            or self.stage_evidence.completed != self.primary_repeat_passed
            or not self.stage_evidence.shadow_only
            or self.stage_evidence.operational_state_mutated
        ):
            raise ValueError("stage evidence provenance differs from receipt")
        expected_issues = () if self.primary_repeat_passed else (
            "primary_repeat_mismatch",
        )
        if self.stage_evidence.issue_codes != expected_issues:
            raise ValueError("stage evidence issues differ from repeat state")
        baseline_outputs = _mapping(
            parsed["baseline_outputs"],
            "primary output baseline_outputs",
        )
        _exact_keys(
            baseline_outputs,
            {baseline.value for baseline in EXECUTED_BASELINES},
            "primary output baseline_outputs",
        )
        versions = {
            BaselineId.EULERIAN_PERSISTENCE: EULERIAN_PERSISTENCE_VERSION,
            BaselineId.TWO_FRAME_TRANSLATION: TWO_FRAME_TRANSLATION_VERSION,
        }
        expected_forecasts = (
            self.stage_evidence.frame_count - len(self.stage_evidence.event_ids)
        )
        for evidence in self.baseline_evidence:
            baseline_output = _mapping(
                baseline_outputs[evidence.baseline_id.value],
                f"primary output {evidence.baseline_id.value}",
            )
            forecasts = baseline_output.get("forecasts")
            if (
                baseline_output.get("baseline_id") != evidence.baseline_id.value
                or baseline_output.get("algorithm_version")
                != versions[evidence.baseline_id]
                or baseline_output.get("preregistration_source_id")
                != evidence.preregistration_source_id
                or not isinstance(forecasts, list)
                or baseline_output.get("forecast_count") != len(forecasts)
                or len(forecasts) != expected_forecasts
                or evidence.implementation_version != versions[evidence.baseline_id]
                or evidence.implementation_sha256
                != self.component_source.source_sha256
                or evidence.input_contract_sha256 != self.input_contract_sha256
                or evidence.primary_output_sha256 != _sha_mapping(baseline_output)
                or evidence.event_ids != self.stage_evidence.event_ids
                or evidence.frame_count != self.stage_evidence.frame_count
                or not evidence.candidate_independent
            ):
                raise ValueError("baseline evidence provenance differs from output")
            baseline_matches = (
                evidence.primary_output_sha256
                == evidence.reproduction_output_sha256
            )
            if (
                evidence.completed != baseline_matches
                or evidence.issue_codes
                != (() if baseline_matches else ("primary_repeat_mismatch",))
                or (self.primary_repeat_passed and not baseline_matches)
            ):
                raise ValueError("baseline evidence issues differ from repeat state")
        for field, value in (
            ("primary_repeat_passed", self.primary_repeat_passed),
            ("real_meteorological_case", self.real_meteorological_case),
            ("offline_science_gate_passed", self.offline_science_gate_passed),
            ("meteorological_admission", self.meteorological_admission),
            ("live_wiring_allowed", self.live_wiring_allowed),
            (
                "dashboard_publication_allowed",
                self.dashboard_publication_allowed,
            ),
            ("home_assistant_state_mutated", self.home_assistant_state_mutated),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{field} must be bool")
        if self.primary_repeat_passed != (
            self.primary_output_sha256 == self.reproduction_output_sha256
        ):
            raise ValueError("primary/repeat pass state differs from hashes")
        if any(
            (
                self.real_meteorological_case,
                self.offline_science_gate_passed,
                self.meteorological_admission,
                self.live_wiring_allowed,
                self.dashboard_publication_allowed,
                self.home_assistant_state_mutated,
            )
        ):
            raise ValueError("partial synthetic runner may never grant live/science authority")
        expected_blockers = tuple(
            f"baseline_not_executed:{baseline.value}"
            for baseline in self.remaining_baselines
        )
        extra = () if self.primary_repeat_passed else ("primary_repeat_mismatch",)
        if self.blocking_reasons != tuple(sorted((*expected_blockers, *extra))):
            raise ValueError("receipt blockers differ from exact partial-run state")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "baseline_evidence": [
                item.to_mapping() for item in self.baseline_evidence
            ],
            "blocking_reasons": list(self.blocking_reasons),
            "component_source": self.component_source.to_mapping(),
            "contract_version": self.contract_version,
            "dashboard_publication_allowed": self.dashboard_publication_allowed,
            "fixture_kind": self.fixture_kind,
            "home_assistant_state_mutated": self.home_assistant_state_mutated,
            "input_contract_sha256": self.input_contract_sha256,
            "live_wiring_allowed": self.live_wiring_allowed,
            "meteorological_admission": self.meteorological_admission,
            "offline_science_gate_passed": self.offline_science_gate_passed,
            "primary_output": json.loads(self.primary_output_json),
            "primary_output_sha256": self.primary_output_sha256,
            "primary_repeat_passed": self.primary_repeat_passed,
            "real_meteorological_case": self.real_meteorological_case,
            "remaining_baselines": [item.value for item in self.remaining_baselines],
            "replay_pack_sha256": self.replay_pack_sha256,
            "reproduction_output_sha256": self.reproduction_output_sha256,
            "stage_evidence": self.stage_evidence.to_mapping(),
        }

    @property
    def sha256(self) -> str:
        return _sha_mapping(self.payload_mapping())

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self.payload_mapping(),
            "nowcast_replay_receipt_sha256": self.sha256,
        }


def run_nowcast_baseline_primary_repeat(
    replay_pack: bytes,
    *,
    package_directory: str | Path | None = None,
) -> OfflineNowcastReplayReceipt:
    """Parse and execute the exact same bytes twice, then compare output bytes."""

    if not isinstance(replay_pack, bytes):
        raise TypeError("replay_pack must be immutable bytes")
    if not 0 < len(replay_pack) <= MAXIMUM_REPLAY_PACK_BYTES:
        raise OfflineNowcastReplayError("replay pack byte length is invalid")
    pack_sha = _sha_bytes(replay_pack)
    component_source = next(
        item
        for item in audit_local_science_component_sources(package_directory)
        if item.stage is ScienceStage.NOWCAST_BASELINES
    )
    primary = _execute_once(replay_pack, pack_sha, component_source)
    reproduction = _execute_once(replay_pack, pack_sha, component_source)
    if primary.replay_input != reproduction.replay_input:
        raise OfflineNowcastReplayError("input contract changed between executions")
    primary_sha = _sha_bytes(primary.output_bytes)
    reproduction_sha = _sha_bytes(reproduction.output_bytes)
    matches = primary.output_bytes == reproduction.output_bytes
    issue_codes = () if matches else ("primary_repeat_mismatch",)
    input_contract_sha = primary.replay_input.sha256
    event_ids = primary.replay_input.event_ids
    frame_count = len(primary.replay_input.frames)
    stage = StageReplayEvidence(
        stage=ScienceStage.NOWCAST_BASELINES,
        implementation_version=component_source.implementation_version,
        implementation_sha256=component_source.source_sha256,
        input_contract_sha256=input_contract_sha,
        primary_output_sha256=primary_sha,
        reproduction_output_sha256=reproduction_sha,
        event_ids=event_ids,
        frame_count=frame_count,
        completed=matches,
        shadow_only=True,
        operational_state_mutated=False,
        issue_codes=issue_codes,
    )
    primary_baseline_mappings = (
        (BaselineId.EULERIAN_PERSISTENCE, primary.eulerian_mapping),
        (BaselineId.TWO_FRAME_TRANSLATION, primary.translation_mapping),
    )
    repeat_baseline_mappings = {
        BaselineId.EULERIAN_PERSISTENCE: reproduction.eulerian_mapping,
        BaselineId.TWO_FRAME_TRANSLATION: reproduction.translation_mapping,
    }
    if primary.preregistration_source_ids != reproduction.preregistration_source_ids:
        raise OfflineNowcastReplayError(
            "baseline preregistration bindings changed between executions"
        )
    versions = {
        BaselineId.EULERIAN_PERSISTENCE: EULERIAN_PERSISTENCE_VERSION,
        BaselineId.TWO_FRAME_TRANSLATION: TWO_FRAME_TRANSLATION_VERSION,
    }
    baselines = tuple(
        BaselineReplayEvidence(
            baseline_id=baseline_id,
            preregistration_source_id=primary.preregistration_source_ids[baseline_id],
            implementation_id=f"storm-tracker-{baseline_id.value}",
            implementation_version=versions[baseline_id],
            implementation_sha256=component_source.source_sha256,
            input_contract_sha256=input_contract_sha,
            primary_output_sha256=_sha_mapping(mapping),
            reproduction_output_sha256=_sha_mapping(
                repeat_baseline_mappings[baseline_id]
            ),
            event_ids=event_ids,
            frame_count=frame_count,
            completed=(mapping == repeat_baseline_mappings[baseline_id]),
            candidate_independent=True,
            issue_codes=(
                ()
                if mapping == repeat_baseline_mappings[baseline_id]
                else ("primary_repeat_mismatch",)
            ),
        )
        for baseline_id, mapping in primary_baseline_mappings
    )
    blockers = [
        f"baseline_not_executed:{baseline.value}"
        for baseline in REMAINING_BASELINES
    ]
    if not matches:
        blockers.append("primary_repeat_mismatch")
    return OfflineNowcastReplayReceipt(
        replay_pack_sha256=pack_sha,
        input_contract_sha256=input_contract_sha,
        component_source=component_source,
        stage_evidence=stage,
        baseline_evidence=baselines,
        primary_output_json=primary.output_bytes.decode("ascii"),
        primary_output_sha256=primary_sha,
        reproduction_output_sha256=reproduction_sha,
        primary_repeat_passed=matches,
        remaining_baselines=REMAINING_BASELINES,
        blocking_reasons=tuple(sorted(blockers)),
    )


def run_nowcast_baseline_primary_repeat_file(
    path: str | Path,
    *,
    package_directory: str | Path | None = None,
) -> OfflineNowcastReplayReceipt:
    """Read one stable local replay pack and execute the offline runner."""

    absolute = Path(os.path.abspath(os.fspath(path)))
    return run_nowcast_baseline_primary_repeat(
        _read_regular(absolute),
        package_directory=package_directory,
    )


def _execute_once(
    body: bytes,
    pack_sha: str,
    component_source: ScienceComponentSource,
) -> _Execution:
    root = _mapping(_parse_json_bytes(body, field="replay pack"), "replay pack")
    _exact_keys(
        root,
        {
            "contract_version",
            "baseline_preregistration_source_ids",
            "fixture_kind",
            "frames",
            "grid",
            "lead_time_seconds",
            "motion_algorithm_version",
            "offline_replay_input",
            "quantity",
            "unit",
        },
        "replay pack",
    )
    if root["contract_version"] != OFFLINE_NOWCAST_REPLAY_CONTRACT:
        raise ValueError("unsupported nowcast replay contract")
    if root["fixture_kind"] != FIXTURE_KIND:
        raise ValueError("runner accepts only the explicit synthetic fixture kind")
    source_ids_value = _mapping(
        root["baseline_preregistration_source_ids"],
        "baseline_preregistration_source_ids",
    )
    _exact_keys(
        source_ids_value,
        {baseline.value for baseline in EXECUTED_BASELINES},
        "baseline_preregistration_source_ids",
    )
    preregistration_source_ids = {
        baseline: _identifier(
            source_ids_value[baseline.value],
            f"baseline_preregistration_source_ids.{baseline.value}",
        )
        for baseline in EXECUTED_BASELINES
    }
    if len(set(preregistration_source_ids.values())) != len(EXECUTED_BASELINES):
        raise ValueError("each executed baseline requires a distinct source id")
    replay_input = offline_replay_input_from_mapping(
        _mapping(root["offline_replay_input"], "offline_replay_input")
    )
    if len(replay_input.frames) < MINIMUM_FRAMES:
        raise ValueError("nowcast replay requires at least twenty frames")
    if len(replay_input.frames) > MAXIMUM_FRAMES:
        raise ValueError("nowcast replay exceeds frame bound")
    grid = _grid_from_mapping(_mapping(root["grid"], "grid"))
    if grid.width * grid.height > MAXIMUM_GRID_PIXELS:
        raise ValueError("nowcast replay grid exceeds pixel bound")
    quantity = _identifier(root["quantity"], "quantity")
    unit = _identifier(root["unit"], "unit")
    if quantity != "rain_rate" or unit != "mm h-1":
        raise ValueError("runner requires explicit rain_rate in mm h-1")
    lead_seconds = _finite(root["lead_time_seconds"], "lead_time_seconds")
    if not 0.0 < lead_seconds <= MAXIMUM_LEAD_SECONDS:
        raise ValueError("lead_time_seconds is outside the bounded horizon")
    motion_version = _identifier(
        root["motion_algorithm_version"],
        "motion_algorithm_version",
    )
    if motion_version != CENTROID_MOTION_VERSION:
        raise ValueError("unsupported centroid motion algorithm version")
    raw_frames = root["frames"]
    if (
        not isinstance(raw_frames, list)
        or len(raw_frames) != len(replay_input.frames)
    ):
        raise ValueError("frame payload count differs from input contract")
    decoded = []
    total_payload_bytes = 0
    for raw, binding in zip(raw_frames, replay_input.frames, strict=True):
        frame, payload_bytes = _decode_frame(
            _mapping(raw, "frame"),
            binding,
            grid,
            quantity,
            unit,
        )
        total_payload_bytes += payload_bytes
        if total_payload_bytes > MAXIMUM_TOTAL_FRAME_BYTES:
            raise ValueError("total decoded frame bytes exceed bound")
        decoded.append(frame)
    frames = tuple(decoded)
    raw_manifest = next(
        artifact
        for artifact in replay_input.artifacts
        if artifact.role is ArtifactRole.RAW_INPUT_MANIFEST
    )
    if raw_manifest.sha256 != _sha_mapping(
        [frame.to_mapping() for frame in replay_input.frames]
    ):
        raise ValueError("raw input manifest differs from frame bindings")
    frames_by_event: dict[str, list[_DecodedFrame]] = {
        event_id: [] for event_id in replay_input.event_ids
    }
    for frame in frames:
        frames_by_event[frame.event_id].append(frame)
    eulerian_forecasts: list[dict[str, Any]] = []
    translation_forecasts: list[dict[str, Any]] = []
    sequence_index = 0
    for event_id in replay_input.event_ids:
        event_frames = frames_by_event[event_id]
        if len(event_frames) < 2:
            raise ValueError("each event requires at least two ordered frames")
        if any(
            current.valid_at <= previous.valid_at
            for previous, current in zip(event_frames, event_frames[1:])
        ):
            raise ValueError(
                "timestamps must be strictly increasing within each event"
            )
        for pair_index, (previous, current) in enumerate(
            zip(event_frames, event_frames[1:]),
            start=1,
        ):
            previous_x, previous_y = _intensity_centroid(previous.field)
            current_x, current_y = _intensity_centroid(current.field)
            receipt_suffix = f"{pack_sha[:16]}-{sequence_index:04d}"
            motion = derive_two_frame_constant_motion(
                motion_id=f"offline-centroid-motion-{receipt_suffix}",
                analysis_grid_id=grid.grid_id,
                previous_field_id=previous.field.field_id,
                current_field_id=current.field.field_id,
                previous_valid_at=previous.valid_at,
                current_valid_at=current.valid_at,
                previous_x_m=previous_x,
                previous_y_m=previous_y,
                current_x_m=current_x,
                current_y_m=current_y,
                method="intensity_weighted_centroid_translation",
                algorithm_version=motion_version,
            )
            target_valid_at = current.valid_at + lead_seconds
            eulerian = eulerian_persistence(
                current.field,
                target_valid_at=target_valid_at,
                forecast_field_id=f"offline-eulerian-{receipt_suffix}",
            )
            translation = lagrangian_constant_motion(
                current.field,
                motion,
                target_valid_at=target_valid_at,
                forecast_field_id=f"offline-translation-{receipt_suffix}",
            )
            pair_context = {
                "event_id": event_id,
                "pair_index": pair_index,
                "support_frame_ids": [
                    previous.binding_id,
                    current.binding_id,
                ],
            }
            eulerian_forecasts.append(
                {**pair_context, "forecast": _forecast_mapping(eulerian)}
            )
            translation_forecasts.append(
                {
                    **pair_context,
                    "forecast": _forecast_mapping(translation),
                    "motion": _motion_mapping(motion),
                }
            )
            sequence_index += 1
    eulerian_mapping = {
        "algorithm_version": EULERIAN_PERSISTENCE_VERSION,
        "baseline_id": BaselineId.EULERIAN_PERSISTENCE.value,
        "forecast_count": len(eulerian_forecasts),
        "forecasts": eulerian_forecasts,
        "preregistration_source_id": preregistration_source_ids[
            BaselineId.EULERIAN_PERSISTENCE
        ],
    }
    translation_mapping = {
        "algorithm_version": TWO_FRAME_TRANSLATION_VERSION,
        "baseline_id": BaselineId.TWO_FRAME_TRANSLATION.value,
        "forecast_count": len(translation_forecasts),
        "forecasts": translation_forecasts,
        "motion_algorithm_version": motion_version,
        "preregistration_source_id": preregistration_source_ids[
            BaselineId.TWO_FRAME_TRANSLATION
        ],
    }
    output = {
        "baseline_outputs": {
            BaselineId.EULERIAN_PERSISTENCE.value: eulerian_mapping,
            BaselineId.TWO_FRAME_TRANSLATION.value: translation_mapping,
        },
        "component_source": component_source.to_mapping(),
        "event_ids": list(replay_input.event_ids),
        "execution_contract": OFFLINE_NOWCAST_REPLAY_CONTRACT,
        "fixture_kind": FIXTURE_KIND,
        "frame_count": len(frames),
        "input_contract_sha256": replay_input.sha256,
        "real_meteorological_case": False,
        "replay_pack_sha256": pack_sha,
    }
    output_bytes = _canonical_json_bytes(output)
    if len(output_bytes) > MAXIMUM_CANONICAL_OUTPUT_BYTES:
        raise OfflineNowcastReplayError(
            "canonical nowcast output exceeds hard byte bound"
        )
    return _Execution(
        replay_input=replay_input,
        component_source=component_source,
        output_mapping=output,
        output_bytes=output_bytes,
        eulerian_mapping=eulerian_mapping,
        translation_mapping=translation_mapping,
        preregistration_source_ids=preregistration_source_ids,
    )


def _grid_from_mapping(value: Mapping[str, Any]) -> AnalysisGrid:
    _exact_keys(
        value,
        {
            "crs",
            "grid_id",
            "height",
            "origin_x_m",
            "origin_y_m",
            "width",
            "x_resolution_m",
            "y_resolution_m",
        },
        "grid",
    )
    width = _positive_integer(value["width"], "grid width")
    height = _positive_integer(value["height"], "grid height")
    return AnalysisGrid(
        grid_id=_identifier(value["grid_id"], "grid_id"),
        crs=_identifier(value["crs"], "crs"),
        origin_x_m=_finite(value["origin_x_m"], "origin_x_m"),
        origin_y_m=_finite(value["origin_y_m"], "origin_y_m"),
        x_resolution_m=_finite(value["x_resolution_m"], "x_resolution_m"),
        y_resolution_m=_finite(value["y_resolution_m"], "y_resolution_m"),
        width=width,
        height=height,
    )


def _decode_frame(
    value: Mapping[str, Any],
    binding: Any,
    grid: AnalysisGrid,
    quantity: str,
    unit: str,
) -> tuple[_DecodedFrame, int]:
    _exact_keys(
        value,
        {"frame_id", "payload_base64", "payload_sha256"},
        "frame",
    )
    frame_id = _identifier(value["frame_id"], "frame_id")
    if frame_id != binding.frame_id:
        raise ValueError("frame payload order/identity differs from input contract")
    payload_sha = _sha256(value["payload_sha256"], "payload_sha256")
    encoded = value["payload_base64"]
    if (
        not isinstance(encoded, str)
        or len(encoded) > 4 * MAXIMUM_FRAME_PAYLOAD_BYTES
    ):
        raise ValueError("payload_base64 is invalid or too large")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("payload_base64 is invalid") from exc
    if not 0 < len(payload) <= MAXIMUM_FRAME_PAYLOAD_BYTES:
        raise ValueError("decoded frame payload size is invalid")
    if _sha_bytes(payload) != payload_sha or payload_sha != binding.raw_frame_sha256:
        raise ValueError("frame payload SHA differs from input contract")
    decoded = _mapping(_parse_json_bytes(payload, field="frame payload"), "frame payload")
    _exact_keys(decoded, {"valid_mask", "values"}, "frame payload")
    if _canonical_json_bytes(decoded) != payload:
        raise ValueError("frame payload must be canonical JSON bytes")
    values = _numeric_matrix(decoded["values"], grid, "values")
    valid_mask = _boolean_matrix(decoded["valid_mask"], grid, "valid_mask")
    field = MetricRasterField(
        field_id=frame_id,
        valid_at=binding.valid_time_utc,
        quantity=quantity,
        unit=unit,
        grid=grid,
        values=values,
        valid_mask=valid_mask,
    )
    return (
        _DecodedFrame(
            binding_id=frame_id,
            event_id=binding.event_id,
            valid_at=binding.valid_time_utc,
            payload_sha256=payload_sha,
            field=field,
        ),
        len(payload),
    )


def _numeric_matrix(
    value: object,
    grid: AnalysisGrid,
    field: str,
) -> tuple[tuple[float, ...], ...]:
    if not isinstance(value, list) or len(value) != grid.height:
        raise ValueError(f"{field} height differs from grid")
    rows = []
    for row in value:
        if not isinstance(row, list) or len(row) != grid.width:
            raise ValueError(f"{field} width differs from grid")
        numeric = tuple(_finite(item, field) for item in row)
        if any(item < 0.0 for item in numeric):
            raise ValueError("rain rate values must be non-negative")
        rows.append(numeric)
    return tuple(rows)


def _boolean_matrix(
    value: object,
    grid: AnalysisGrid,
    field: str,
) -> tuple[tuple[bool, ...], ...]:
    if not isinstance(value, list) or len(value) != grid.height:
        raise ValueError(f"{field} height differs from grid")
    rows = []
    for row in value:
        if (
            not isinstance(row, list)
            or len(row) != grid.width
            or any(not isinstance(item, bool) for item in row)
        ):
            raise ValueError(f"{field} must match grid and contain booleans")
        rows.append(tuple(row))
    return tuple(rows)


def _intensity_centroid(field: MetricRasterField) -> tuple[float, float]:
    total = 0.0
    weighted_x = 0.0
    weighted_y = 0.0
    for row in range(field.grid.height):
        for column in range(field.grid.width):
            if not field.valid_mask[row][column]:
                continue
            weight = float(field.values[row][column])
            if weight <= 0.0:
                continue
            x_m, y_m = field.grid.pixel_center(row, column)
            total += weight
            weighted_x += weight * x_m
            weighted_y += weight * y_m
    if total <= 0.0 or not all(
        math.isfinite(value) for value in (total, weighted_x, weighted_y)
    ):
        raise ValueError("latest two frames require finite positive rain support")
    return weighted_x / total, weighted_y / total


def _grid_mapping(grid: AnalysisGrid) -> dict[str, Any]:
    return {
        "axis_order": grid.axis_order,
        "contract_version": grid.contract_version,
        "coordinate_unit": grid.coordinate_unit,
        "crs": grid.crs,
        "grid_id": grid.grid_id,
        "height": grid.height,
        "origin_x_m": float(grid.origin_x_m),
        "origin_y_m": float(grid.origin_y_m),
        "row_order": grid.row_order,
        "width": grid.width,
        "x_resolution_m": float(grid.x_resolution_m),
        "y_resolution_m": float(grid.y_resolution_m),
    }


def _motion_mapping(motion: ConstantMotionVector) -> dict[str, Any]:
    return {
        "algorithm_version": motion.algorithm_version,
        "analysis_grid_id": motion.analysis_grid_id,
        "method": motion.method,
        "motion_id": motion.motion_id,
        "observed_interval_seconds": float(motion.observed_interval_seconds),
        "reference_valid_at": float(motion.reference_valid_at),
        "speed_kmh": float(motion.speed_kmh),
        "support_field_ids": list(motion.support_field_ids),
        "support_valid_at": [float(item) for item in motion.support_valid_at],
        "vx_m_s": float(motion.vx_m_s),
        "vy_m_s": float(motion.vy_m_s),
    }


def _forecast_mapping(forecast: BaselineForecastField) -> dict[str, Any]:
    return {
        "algorithm_version": forecast.algorithm_version,
        "contract_version": forecast.contract_version,
        "forecast_field_id": forecast.forecast_field_id,
        "grid": _grid_mapping(forecast.grid),
        "input_field_ids": list(forecast.input_field_ids),
        "interpolation": forecast.interpolation,
        "lead_time_seconds": float(forecast.lead_time_seconds),
        "method": forecast.method.value,
        "motion_id": forecast.motion_id,
        "quantity": forecast.quantity,
        "reference_valid_at": float(forecast.reference_valid_at),
        "target_valid_at": float(forecast.target_valid_at),
        "unit": forecast.unit,
        "valid_mask": [list(row) for row in forecast.valid_mask],
        "values": [[float(item) for item in row] for row in forecast.values],
    }
