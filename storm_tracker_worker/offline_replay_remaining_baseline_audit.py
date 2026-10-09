"""Fail-closed executability audit for the three unrun roadmap baselines.

This module deliberately does *not* turn related source code, a test double or
an unbound local archive into baseline output.  It first validates and executes
the existing Eulerian/translation primary-repeat runner from the exact replay
bytes.  It then content-addresses the real related source files and records the
capabilities that the immutable replay contract lacks for:

* the last released vNext;
* the production legacy engine; and
* official pySTEPS deterministic extrapolation.

The result is an executability-preflight receipt, not scientific execution
evidence for those three baselines.  It never grants admission, publication or
live wiring and it never mutates Home Assistant or worker state.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping

from .offline_replay_acceptance import BaselineId
from .offline_replay_nowcast_runner import (
    EXECUTED_BASELINES,
    MAXIMUM_REPLAY_PACK_BYTES,
    REMAINING_BASELINES,
    run_nowcast_baseline_primary_repeat,
)
from .tracking_core.engine.pysteps_challenger import PystepsAlgorithm


REMAINING_BASELINE_AUDIT_CONTRACT = (
    "stv3-offline-replay-remaining-baseline-executability-v1"
)
AUDIT_KIND = "executability_preflight_only"
MAXIMUM_SOURCE_BYTES = 2 * 1024 * 1024
MAXIMUM_CANONICAL_RECEIPT_BYTES = 2 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_SOURCE_SPECS: Mapping[
    BaselineId,
    tuple[tuple[str, str, tuple[str, ...]], ...],
] = {
    BaselineId.LAST_RELEASED_VNEXT: (
        (
            "storm_tracker_worker/tracking_core/engine/nowcast_baselines.py",
            "current_vnext_source_not_release_authority",
            (
                "eulerian_persistence",
                "lagrangian_constant_motion",
            ),
        ),
    ),
    BaselineId.LEGACY_ENGINE: (
        (
            "custom_components/storm_tracker_v3/engine/nowcast.py",
            "legacy_nowcast_entrypoint",
            ("build_precipitation_status",),
        ),
        (
            "custom_components/storm_tracker_v3/engine/storm.py",
            "legacy_storm_model",
            ("Storm",),
        ),
        (
            "custom_components/storm_tracker_v3/engine/storm_engine.py",
            "legacy_storm_engine",
            ("StormEngine",),
        ),
        (
            "custom_components/storm_tracker_v3/engine/trajectory.py",
            "legacy_trajectory_support",
            ("fit_adaptive_trajectory",),
        ),
    ),
    BaselineId.PYSTEPS_DETERMINISTIC: (
        (
            "storm_tracker_worker/tracking_core/engine/pysteps_challenger.py",
            "stochastic_pysteps_shadow_adapter_not_deterministic_baseline",
            (
                "PystepsAlgorithm",
                "run_pysteps_challenger",
            ),
        ),
        (
            "storm_tracker_worker/requirements.txt",
            "worker_runtime_dependency_declaration",
            (),
        ),
    ),
}

_REQUIRED_CAPABILITIES: Mapping[BaselineId, tuple[str, ...]] = {
    BaselineId.LAST_RELEASED_VNEXT: (
        "last_released_vnext_executable_artifact_binding",
        "last_released_vnext_preregistration_binding",
        "last_released_vnext_release_authority",
        "metric_raster_frame_sequence",
    ),
    BaselineId.LEGACY_ENGINE: (
        "legacy_deterministic_clock_contract",
        "legacy_raster_to_storm_adapter_preregistration",
        "legacy_storm_state_sequence",
        "legacy_target_contract",
        "metric_raster_frame_sequence",
    ),
    BaselineId.PYSTEPS_DETERMINISTIC: (
        "metric_raster_frame_sequence",
        "official_pysteps_deterministic_entrypoint",
        "official_pysteps_runtime_distribution",
        "pysteps_dense_motion_contract",
        "pysteps_deterministic_adapter",
        "pysteps_deterministic_preregistration_binding",
        "worker_pysteps_runtime_dependency",
    ),
}

_BASE_BLOCKERS: Mapping[BaselineId, tuple[str, ...]] = {
    BaselineId.LAST_RELEASED_VNEXT: (
        "last_released_vnext.executable_artifact_not_bound_to_replay",
        "last_released_vnext.preregistration_not_bound_to_replay",
        "last_released_vnext.release_authority_not_bound_to_replay",
    ),
    BaselineId.LEGACY_ENGINE: (
        "legacy_engine.deterministic_clock_contract_missing",
        "legacy_engine.raster_to_storm_adapter_not_preregistered",
        "legacy_engine.storm_state_sequence_missing",
        "legacy_engine.target_contract_missing",
    ),
    BaselineId.PYSTEPS_DETERMINISTIC: (
        "pysteps_deterministic_extrapolation.adapter_not_exposed",
        "pysteps_deterministic_extrapolation.dense_motion_contract_missing",
        "pysteps_deterministic_extrapolation.official_entrypoint_not_audited",
        "pysteps_deterministic_extrapolation.preregistration_not_bound_to_replay",
        "pysteps_deterministic_extrapolation.worker_runtime_dependency_not_declared",
    ),
}


class RemainingBaselineAuditError(RuntimeError):
    """The local executability preflight cannot produce trustworthy evidence."""


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


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} requires lowercase SHA-256")
    return value


def _identifier(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or "\x00" in value
        or len(value) > 512
    ):
        raise ValueError(f"{field} is invalid")
    return value


def _sorted_unique(values: tuple[str, ...], field: str) -> None:
    if (
        not isinstance(values, tuple)
        or any(not isinstance(item, str) or not item for item in values)
        or tuple(sorted(set(values))) != values
    ):
        raise ValueError(f"{field} must be sorted unique strings")


def _parse_object(body: bytes, field: str) -> Mapping[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key in {field}: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(body, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RemainingBaselineAuditError(f"{field} is invalid JSON") from exc
    if not isinstance(value, Mapping):
        raise RemainingBaselineAuditError(f"{field} root must be an object")
    return value


def _stable_regular_file(path: Path, *, maximum_bytes: int) -> bytes:
    if not path.is_absolute() or path.is_symlink():
        raise RemainingBaselineAuditError("audited path is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RemainingBaselineAuditError("audited file cannot be opened") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > maximum_bytes
        ):
            raise RemainingBaselineAuditError("audited file type/size is invalid")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(
                descriptor,
                min(1024 * 1024, maximum_bytes + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise RemainingBaselineAuditError("audited file exceeds byte bound")
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
            raise RemainingBaselineAuditError("audited file changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class AuditedSourceFile:
    relative_path: str
    role: str
    sha256: str
    size_bytes: int
    required_symbols: tuple[str, ...]

    def __post_init__(self) -> None:
        path = Path(self.relative_path)
        if (
            path.is_absolute()
            or not path.parts
            or ".." in path.parts
            or "\\" in self.relative_path
        ):
            raise ValueError("relative_path is invalid")
        _identifier(self.role, "role")
        _sha256(self.sha256, "sha256")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes <= 0
            or self.size_bytes > MAXIMUM_SOURCE_BYTES
        ):
            raise ValueError("size_bytes is invalid")
        _sorted_unique(self.required_symbols, "required_symbols")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "required_symbols": list(self.required_symbols),
            "role": self.role,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class RuntimeFact:
    fact_id: str
    value: str

    def __post_init__(self) -> None:
        _identifier(self.fact_id, "fact_id")
        _identifier(self.value, "runtime fact value")

    def to_mapping(self) -> dict[str, str]:
        return {"fact_id": self.fact_id, "value": self.value}


@dataclass(frozen=True, slots=True)
class RemainingBaselineDecision:
    baseline_id: BaselineId
    source_files: tuple[AuditedSourceFile, ...]
    source_bundle_sha256: str
    required_capability_ids: tuple[str, ...]
    available_capability_ids: tuple[str, ...]
    missing_capability_ids: tuple[str, ...]
    blocker_codes: tuple[str, ...]
    runtime_facts: tuple[RuntimeFact, ...]
    related_source_code_present: bool = True
    required_executable_path_present: bool = False
    existing_code_invoked: bool = False
    execution_call_count: int = 0
    completed: bool = False
    primary_output_sha256: str | None = None
    reproduction_output_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.baseline_id, BaselineId):
            raise TypeError("baseline_id must be BaselineId")
        if self.baseline_id not in REMAINING_BASELINES:
            raise ValueError("decision baseline is not a remaining roadmap baseline")
        if (
            not isinstance(self.source_files, tuple)
            or not self.source_files
            or any(not isinstance(item, AuditedSourceFile) for item in self.source_files)
            or tuple(sorted(self.source_files, key=lambda item: item.relative_path))
            != self.source_files
        ):
            raise ValueError("source_files must be nonempty and path-sorted")
        if len({item.relative_path for item in self.source_files}) != len(
            self.source_files
        ):
            raise ValueError("source_files contain duplicate paths")
        _sha256(self.source_bundle_sha256, "source_bundle_sha256")
        if self.source_bundle_sha256 != _sha_mapping(
            [item.to_mapping() for item in self.source_files]
        ):
            raise ValueError("source bundle SHA differs from audited files")
        for field, values in (
            ("required_capability_ids", self.required_capability_ids),
            ("available_capability_ids", self.available_capability_ids),
            ("missing_capability_ids", self.missing_capability_ids),
            ("blocker_codes", self.blocker_codes),
        ):
            _sorted_unique(values, field)
        expected_missing = tuple(
            sorted(
                set(self.required_capability_ids)
                - set(self.available_capability_ids)
            )
        )
        if self.missing_capability_ids != expected_missing:
            raise ValueError("missing capabilities differ from available capabilities")
        if not self.missing_capability_ids or not self.blocker_codes:
            raise ValueError("an unexecuted baseline requires explicit blockers")
        if (
            not isinstance(self.runtime_facts, tuple)
            or any(not isinstance(item, RuntimeFact) for item in self.runtime_facts)
            or tuple(sorted(self.runtime_facts, key=lambda item: item.fact_id))
            != self.runtime_facts
            or len({item.fact_id for item in self.runtime_facts})
            != len(self.runtime_facts)
        ):
            raise ValueError("runtime_facts must be uniquely fact-id sorted")
        for field, value in (
            ("related_source_code_present", self.related_source_code_present),
            ("required_executable_path_present", self.required_executable_path_present),
            ("existing_code_invoked", self.existing_code_invoked),
            ("completed", self.completed),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{field} must be bool")
        if (
            isinstance(self.execution_call_count, bool)
            or not isinstance(self.execution_call_count, int)
        ):
            raise TypeError("execution_call_count must be int")
        if (
            not self.related_source_code_present
            or self.required_executable_path_present
            or self.existing_code_invoked
            or self.execution_call_count != 0
            or self.completed
            or self.primary_output_sha256 is not None
            or self.reproduction_output_sha256 is not None
        ):
            raise ValueError("preflight decision may not claim remaining-baseline execution")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "available_capability_ids": list(self.available_capability_ids),
            "baseline_id": self.baseline_id.value,
            "blocker_codes": list(self.blocker_codes),
            "completed": self.completed,
            "execution_call_count": self.execution_call_count,
            "existing_code_invoked": self.existing_code_invoked,
            "missing_capability_ids": list(self.missing_capability_ids),
            "primary_output_sha256": self.primary_output_sha256,
            "related_source_code_present": self.related_source_code_present,
            "reproduction_output_sha256": self.reproduction_output_sha256,
            "required_capability_ids": list(self.required_capability_ids),
            "required_executable_path_present": self.required_executable_path_present,
            "runtime_facts": [item.to_mapping() for item in self.runtime_facts],
            "source_bundle_sha256": self.source_bundle_sha256,
            "source_files": [item.to_mapping() for item in self.source_files],
        }


@dataclass(frozen=True, slots=True)
class RemainingBaselineAuditReceipt:
    replay_pack_sha256: str
    input_contract_sha256: str
    validated_nowcast_receipt_sha256: str
    validated_primary_output_sha256: str
    validated_reproduction_output_sha256: str
    validated_primary_repeat_passed: bool
    validated_executed_baselines: tuple[BaselineId, ...]
    replay_root_members: tuple[str, ...]
    replay_capability_ids: tuple[str, ...]
    decisions: tuple[RemainingBaselineDecision, ...]
    blocking_reasons: tuple[str, ...]
    auditor_source_sha256: str
    auditor_source_size_bytes: int
    contract_version: str = REMAINING_BASELINE_AUDIT_CONTRACT
    audit_kind: str = AUDIT_KIND
    closed_remaining_baselines: tuple[BaselineId, ...] = ()
    unclosed_remaining_baselines: tuple[BaselineId, ...] = REMAINING_BASELINES
    remaining_baseline_execution_call_count: int = 0
    offline_science_gate_passed: bool = False
    meteorological_admission: bool = False
    live_wiring_allowed: bool = False
    dashboard_publication_allowed: bool = False
    home_assistant_state_mutated: bool = False

    def __post_init__(self) -> None:
        if self.contract_version != REMAINING_BASELINE_AUDIT_CONTRACT:
            raise ValueError("unsupported remaining-baseline audit contract")
        if self.audit_kind != AUDIT_KIND:
            raise ValueError("unsupported audit kind")
        for field, value in (
            ("replay_pack_sha256", self.replay_pack_sha256),
            ("input_contract_sha256", self.input_contract_sha256),
            ("validated_nowcast_receipt_sha256", self.validated_nowcast_receipt_sha256),
            ("validated_primary_output_sha256", self.validated_primary_output_sha256),
            (
                "validated_reproduction_output_sha256",
                self.validated_reproduction_output_sha256,
            ),
            ("auditor_source_sha256", self.auditor_source_sha256),
        ):
            _sha256(value, field)
        if (
            isinstance(self.auditor_source_size_bytes, bool)
            or not isinstance(self.auditor_source_size_bytes, int)
            or not 0 < self.auditor_source_size_bytes <= MAXIMUM_SOURCE_BYTES
        ):
            raise ValueError("auditor source size is invalid")
        if not isinstance(self.validated_primary_repeat_passed, bool):
            raise TypeError("validated_primary_repeat_passed must be bool")
        if (
            not isinstance(self.validated_executed_baselines, tuple)
            or any(
                not isinstance(item, BaselineId)
                for item in self.validated_executed_baselines
            )
            or self.validated_executed_baselines != EXECUTED_BASELINES
        ):
            raise ValueError("validated baseline set differs from real runner")
        _sorted_unique(self.replay_root_members, "replay_root_members")
        _sorted_unique(self.replay_capability_ids, "replay_capability_ids")
        if (
            not isinstance(self.decisions, tuple)
            or any(
                not isinstance(item, RemainingBaselineDecision)
                for item in self.decisions
            )
            or tuple(item.baseline_id for item in self.decisions)
            != REMAINING_BASELINES
        ):
            raise ValueError("receipt requires the exact remaining baseline decisions")
        if not isinstance(self.closed_remaining_baselines, tuple):
            raise TypeError("closed_remaining_baselines must be tuple")
        if self.closed_remaining_baselines:
            raise ValueError("executability preflight cannot close an unexecuted baseline")
        if (
            not isinstance(self.unclosed_remaining_baselines, tuple)
            or any(
                not isinstance(item, BaselineId)
                for item in self.unclosed_remaining_baselines
            )
            or self.unclosed_remaining_baselines != REMAINING_BASELINES
        ):
            raise ValueError("all unexecuted baselines must remain unclosed")
        if (
            isinstance(self.remaining_baseline_execution_call_count, bool)
            or not isinstance(self.remaining_baseline_execution_call_count, int)
        ):
            raise TypeError("remaining baseline execution call count must be int")
        if self.remaining_baseline_execution_call_count != 0:
            raise ValueError("receipt may not claim remaining-baseline calls")
        expected_blockers = {
            code for decision in self.decisions for code in decision.blocker_codes
        }
        if not self.validated_primary_repeat_passed:
            expected_blockers.add("validated_nowcast_primary_repeat_mismatch")
        if self.blocking_reasons != tuple(sorted(expected_blockers)):
            raise ValueError("blocking reasons differ from exact decisions")
        for field, value in (
            ("offline_science_gate_passed", self.offline_science_gate_passed),
            ("meteorological_admission", self.meteorological_admission),
            ("live_wiring_allowed", self.live_wiring_allowed),
            ("dashboard_publication_allowed", self.dashboard_publication_allowed),
            ("home_assistant_state_mutated", self.home_assistant_state_mutated),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{field} must be bool")
            if value:
                raise ValueError("executability preflight may never grant live/science authority")
        if len(_canonical_json_bytes(self.payload_mapping())) > MAXIMUM_CANONICAL_RECEIPT_BYTES:
            raise ValueError("canonical remaining-baseline receipt exceeds byte bound")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "audit_kind": self.audit_kind,
            "auditor_source_sha256": self.auditor_source_sha256,
            "auditor_source_size_bytes": self.auditor_source_size_bytes,
            "blocking_reasons": list(self.blocking_reasons),
            "closed_remaining_baselines": [
                item.value for item in self.closed_remaining_baselines
            ],
            "contract_version": self.contract_version,
            "dashboard_publication_allowed": self.dashboard_publication_allowed,
            "decisions": [item.to_mapping() for item in self.decisions],
            "home_assistant_state_mutated": self.home_assistant_state_mutated,
            "input_contract_sha256": self.input_contract_sha256,
            "live_wiring_allowed": self.live_wiring_allowed,
            "meteorological_admission": self.meteorological_admission,
            "offline_science_gate_passed": self.offline_science_gate_passed,
            "remaining_baseline_execution_call_count": (
                self.remaining_baseline_execution_call_count
            ),
            "replay_capability_ids": list(self.replay_capability_ids),
            "replay_pack_sha256": self.replay_pack_sha256,
            "replay_root_members": list(self.replay_root_members),
            "unclosed_remaining_baselines": [
                item.value for item in self.unclosed_remaining_baselines
            ],
            "validated_executed_baselines": [
                item.value for item in self.validated_executed_baselines
            ],
            "validated_nowcast_receipt_sha256": (
                self.validated_nowcast_receipt_sha256
            ),
            "validated_primary_output_sha256": self.validated_primary_output_sha256,
            "validated_primary_repeat_passed": self.validated_primary_repeat_passed,
            "validated_reproduction_output_sha256": (
                self.validated_reproduction_output_sha256
            ),
        }

    @property
    def sha256(self) -> str:
        return _sha_mapping(self.payload_mapping())

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self.payload_mapping(),
            "remaining_baseline_audit_sha256": self.sha256,
        }


def audit_remaining_baseline_executability(
    replay_pack: bytes,
    *,
    repository_directory: str | Path | None = None,
    package_directory: str | Path | None = None,
) -> RemainingBaselineAuditReceipt:
    """Audit, but never execute, the three remaining roadmap baselines."""

    if not isinstance(replay_pack, bytes):
        raise TypeError("replay_pack must be immutable bytes")
    if not 0 < len(replay_pack) <= MAXIMUM_REPLAY_PACK_BYTES:
        raise RemainingBaselineAuditError("replay pack byte length is invalid")
    repository_root = _repository_root(repository_directory)
    module_repository_root = Path(
        os.path.abspath(os.fspath(Path(__file__).parent.parent))
    )
    if os.path.normcase(os.fspath(repository_root)) != os.path.normcase(
        os.fspath(module_repository_root)
    ):
        raise RemainingBaselineAuditError(
            "repository root differs from the imported auditor checkout"
        )
    expected_package_root = repository_root / "storm_tracker_worker"
    supplied_package_root = Path(
        os.path.abspath(
            os.fspath(
                package_directory
                if package_directory is not None
                else expected_package_root
            )
        )
    )
    if os.path.normcase(os.fspath(supplied_package_root)) != os.path.normcase(
        os.fspath(expected_package_root)
    ):
        raise RemainingBaselineAuditError(
            "worker package differs from the audited repository checkout"
        )
    root = _parse_object(replay_pack, "replay pack")
    validated = run_nowcast_baseline_primary_repeat(
        replay_pack,
        package_directory=expected_package_root,
    )
    source_groups = {
        baseline: tuple(
            sorted(
                (
                    _audit_source_file(
                        repository_root,
                        relative_path,
                        role,
                        symbols,
                    )
                    for relative_path, role, symbols in specs
                ),
                key=lambda item: item.relative_path,
            )
        )
        for baseline, specs in _SOURCE_SPECS.items()
    }
    requirements_body = next(
        _stable_regular_file(
            repository_root / item.relative_path,
            maximum_bytes=MAXIMUM_SOURCE_BYTES,
        )
        for item in source_groups[BaselineId.PYSTEPS_DETERMINISTIC]
        if item.relative_path == "storm_tracker_worker/requirements.txt"
    )
    requirements_evidence = next(
        item
        for item in source_groups[BaselineId.PYSTEPS_DETERMINISTIC]
        if item.relative_path == "storm_tracker_worker/requirements.txt"
    )
    if _sha_bytes(requirements_body) != requirements_evidence.sha256:
        raise RemainingBaselineAuditError(
            "worker requirements changed between source audit and capability audit"
        )
    worker_declares_pysteps = _requirements_declare_pysteps(requirements_body)
    pysteps_version = _pysteps_distribution_version()
    adapter_algorithms = tuple(
        sorted(item.value for item in PystepsAlgorithm)
    )
    replay_capabilities = _replay_capabilities(root)
    decisions = tuple(
        _decision(
            baseline,
            source_groups[baseline],
            replay_capabilities,
            worker_declares_pysteps=worker_declares_pysteps,
            pysteps_version=pysteps_version,
            adapter_algorithms=adapter_algorithms,
        )
        for baseline in REMAINING_BASELINES
    )
    blockers = {
        code for decision in decisions for code in decision.blocker_codes
    }
    if not validated.primary_repeat_passed:
        blockers.add("validated_nowcast_primary_repeat_mismatch")
    auditor_body = _stable_regular_file(
        Path(os.path.abspath(os.fspath(__file__))),
        maximum_bytes=MAXIMUM_SOURCE_BYTES,
    )
    return RemainingBaselineAuditReceipt(
        replay_pack_sha256=_sha_bytes(replay_pack),
        input_contract_sha256=validated.input_contract_sha256,
        validated_nowcast_receipt_sha256=validated.sha256,
        validated_primary_output_sha256=validated.primary_output_sha256,
        validated_reproduction_output_sha256=(
            validated.reproduction_output_sha256
        ),
        validated_primary_repeat_passed=validated.primary_repeat_passed,
        validated_executed_baselines=EXECUTED_BASELINES,
        replay_root_members=tuple(sorted(root)),
        replay_capability_ids=replay_capabilities,
        decisions=decisions,
        blocking_reasons=tuple(sorted(blockers)),
        auditor_source_sha256=_sha_bytes(auditor_body),
        auditor_source_size_bytes=len(auditor_body),
    )


def audit_remaining_baseline_executability_file(
    path: str | Path,
    *,
    repository_directory: str | Path | None = None,
    package_directory: str | Path | None = None,
) -> RemainingBaselineAuditReceipt:
    """Read one stable replay pack and produce an audit-only receipt."""

    absolute = Path(os.path.abspath(os.fspath(path)))
    body = _stable_regular_file(
        absolute,
        maximum_bytes=MAXIMUM_REPLAY_PACK_BYTES,
    )
    return audit_remaining_baseline_executability(
        body,
        repository_directory=repository_directory,
        package_directory=package_directory,
    )


def _repository_root(value: str | Path | None) -> Path:
    root = Path(
        os.path.abspath(
            os.fspath(
                value if value is not None else Path(__file__).parent.parent
            )
        )
    )
    if root.is_symlink() or not root.is_dir():
        raise RemainingBaselineAuditError("repository root is invalid")
    return root


def _audit_source_file(
    root: Path,
    relative_path: str,
    role: str,
    required_symbols: tuple[str, ...],
) -> AuditedSourceFile:
    target = root.joinpath(*relative_path.split("/"))
    try:
        common = os.path.commonpath((os.fspath(root), os.fspath(target)))
    except ValueError as exc:
        raise RemainingBaselineAuditError("audited source escapes repository") from exc
    if os.path.normcase(common) != os.path.normcase(os.fspath(root)):
        raise RemainingBaselineAuditError("audited source escapes repository")
    cursor = root
    for part in relative_path.split("/")[:-1]:
        cursor = cursor / part
        if cursor.is_symlink():
            raise RemainingBaselineAuditError(
                "audited source traverses a symbolic-link directory"
            )
    body = _stable_regular_file(target, maximum_bytes=MAXIMUM_SOURCE_BYTES)
    sorted_symbols = tuple(sorted(required_symbols))
    if sorted_symbols:
        try:
            tree = ast.parse(body.decode("utf-8-sig"), filename=relative_path)
        except (UnicodeDecodeError, SyntaxError) as exc:
            raise RemainingBaselineAuditError(
                "audited Python source cannot be parsed"
            ) from exc
        defined = {
            node.name
            for node in tree.body
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        }
        if not set(sorted_symbols).issubset(defined):
            raise RemainingBaselineAuditError(
                "audited source lacks required existing symbol"
            )
    return AuditedSourceFile(
        relative_path=relative_path,
        role=role,
        sha256=_sha_bytes(body),
        size_bytes=len(body),
        required_symbols=sorted_symbols,
    )


def _requirements_declare_pysteps(body: bytes) -> bool:
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise RemainingBaselineAuditError("worker requirements are not UTF-8") from exc
    for line in text.splitlines():
        candidate = line.split("#", 1)[0].strip().lower()
        if not candidate:
            continue
        name = re.split(r"[<>=!~\[;\s]", candidate, maxsplit=1)[0]
        if name.replace("_", "-") == "pysteps":
            return True
    return False


def _pysteps_distribution_version() -> str | None:
    try:
        value = importlib.metadata.version("pysteps")
    except importlib.metadata.PackageNotFoundError:
        return None
    except Exception as exc:
        raise RemainingBaselineAuditError(
            f"pySTEPS metadata lookup failed:{type(exc).__name__[:80]}"
        ) from exc
    return _identifier(value, "pysteps distribution version")


def _replay_capabilities(root: Mapping[str, Any]) -> tuple[str, ...]:
    capabilities = {
        "held_out_golden_input",
        "metric_raster_frame_sequence",
    }
    source_ids = root.get("baseline_preregistration_source_ids")
    if isinstance(source_ids, Mapping):
        if BaselineId.EULERIAN_PERSISTENCE.value in source_ids:
            capabilities.add("eulerian_preregistration_binding")
        if BaselineId.TWO_FRAME_TRANSLATION.value in source_ids:
            capabilities.add("two_frame_translation_preregistration_binding")
    return tuple(sorted(capabilities))


def _decision(
    baseline: BaselineId,
    sources: tuple[AuditedSourceFile, ...],
    replay_capabilities: tuple[str, ...],
    *,
    worker_declares_pysteps: bool,
    pysteps_version: str | None,
    adapter_algorithms: tuple[str, ...],
) -> RemainingBaselineDecision:
    required = _REQUIRED_CAPABILITIES[baseline]
    available = tuple(sorted(set(required) & set(replay_capabilities)))
    blockers = set(_BASE_BLOCKERS[baseline])
    facts: list[RuntimeFact] = []
    if baseline is BaselineId.LAST_RELEASED_VNEXT:
        facts.extend(
            (
                RuntimeFact("audited_source_authority", "current_tree_only"),
                RuntimeFact("release_artifact_bound_to_replay", "false"),
                RuntimeFact("release_deployment_authority_bound_to_replay", "false"),
            )
        )
    elif baseline is BaselineId.LEGACY_ENGINE:
        facts.extend(
            (
                RuntimeFact("audited_legacy_entrypoint", "build_precipitation_status"),
                RuntimeFact("input_model", "storm_objects_and_target_coordinates"),
                RuntimeFact("replay_payload_model", "metric_raster_fields"),
            )
        )
    elif baseline is BaselineId.PYSTEPS_DETERMINISTIC:
        facts.extend(
            (
                RuntimeFact(
                    "official_pysteps_distribution_version",
                    pysteps_version if pysteps_version is not None else "absent",
                ),
                RuntimeFact(
                    "worker_requirements_declare_pysteps",
                    "true" if worker_declares_pysteps else "false",
                ),
                RuntimeFact(
                    "worker_shadow_adapter_algorithm_ids",
                    ",".join(adapter_algorithms),
                ),
            )
        )
        if worker_declares_pysteps:
            available = tuple(
                sorted((*available, "worker_pysteps_runtime_dependency"))
            )
            blockers.discard(
                "pysteps_deterministic_extrapolation.worker_runtime_dependency_not_declared"
            )
        if pysteps_version is None:
            blockers.add(
                "pysteps_deterministic_extrapolation.official_runtime_distribution_absent"
            )
        else:
            available = tuple(
                sorted((*available, "official_pysteps_runtime_distribution"))
            )
    else:  # pragma: no cover - guarded by the exact enum tuple above
        raise RemainingBaselineAuditError("unsupported remaining baseline")
    missing = tuple(sorted(set(required) - set(available)))
    facts_tuple = tuple(sorted(facts, key=lambda item: item.fact_id))
    source_bundle_sha = _sha_mapping([item.to_mapping() for item in sources])
    return RemainingBaselineDecision(
        baseline_id=baseline,
        source_files=sources,
        source_bundle_sha256=source_bundle_sha,
        required_capability_ids=required,
        available_capability_ids=available,
        missing_capability_ids=missing,
        blocker_codes=tuple(sorted(blockers)),
        runtime_facts=facts_tuple,
    )
