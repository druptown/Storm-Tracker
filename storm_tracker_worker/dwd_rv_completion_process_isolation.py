"""Hard, crash-reconciled process boundary for the inert DWD completion chain.

The lifecycle completion outbox remains the only queue.  A fresh spawned child
opens only the exact stores bound by a checksummed parent authority, executes
the DWD raw -> quantitative -> canonical -> grid -> cells-shadow chain and
atomically commits both output and hard-process receipts.  The parent reloads
that durable row before interpreting every child terminal outcome.

This module is deliberately absent from service.py, MQTT, Home Assistant,
snapshot publication and admission wiring.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import errno
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Any, Literal, Mapping

from .analysis_grid_transform import (
    AnalysisGridResamplingMethod,
    AnalysisGridTransformConfig,
    analysis_grid_sha256,
)
from .canonical_evidence_store import CanonicalEvidenceStore
from .canonical_radar_field_bridge import RadarFieldBridgeDeadlineError
from .dwd_rv_completion_consumer import (
    CONSUMER_ID,
    STREAM_ID,
    DwdRvCompletionCancelled,
    DwdRvCompletionCapacityError,
    DwdRvCompletionConflict,
    DwdRvCompletionDeadlineError,
    DwdRvCompletionIntegrityError,
    DwdRvCompletionOutputLimits,
    DwdRvCompletionOutputReceipt,
    DwdRvCompletionOutputStore,
    DwdRvShadowChainProfile,
    _LeaseBinding,
    _execute_chain,
    _validate_lease,
)
from .dwd_rv_quantitative import (
    DwdRvDecodeDeadlineError,
    DwdRvDerivedArtifactStore,
)
from .evidence_to_cells_shadow import (
    EVIDENCE_TO_CELLS_SHADOW_ALGORITHM,
    EvidenceToCellsShadowConfig,
    EvidenceToCellsShadowDeadlineError,
)
from .hard_decoder_isolation import (
    DecoderEffectContract,
    DecoderProcessResult,
    DecoderResourceLimits,
    HardDecoderExecutor,
)
from .raw_artifacts import RawArtifactLimits, RawArtifactStore
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.hierarchical_detection import (
    HIERARCHICAL_DETECTOR_VERSION,
    HierarchicalDetectorConfig,
)
from .acquisition_evidence_lifecycle import CompletionLease


PROCESS_AUTHORITY_SCHEMA = "stv3-dwd-rv-hard-process-authority-v1"
PROCESS_REQUEST_SCHEMA = "stv3-dwd-rv-hard-process-request-v1"
PROCESS_CHILD_RESULT_SCHEMA = "stv3-dwd-rv-hard-process-child-result-v1"
PROCESS_RESULT_SCHEMA = "stv3-dwd-rv-hard-process-parent-result-v2"
PROCESS_TARGET = (
    "storm_tracker_worker.dwd_rv_completion_process_isolation:"
    "_dwd_completion_child_entry"
)
PROCESS_EFFECT_CONTRACT = DecoderEffectContract(
    contract_id="dwd-rv-completion.transactional-receipts.v1",
    hard_kill_safe=True,
    durable_effects="transactional",
    spawns_descendants=False,
)

_SHA = re.compile(r"^[0-9a-f]{64}$")
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_SECRET_KEY = re.compile(
    r"(?:authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"password|passwd|credential|private[_-]?key)",
    re.IGNORECASE,
)
_MAX_DIAGNOSTIC_BYTES = 4096


class DwdRvProcessIsolationError(RuntimeError):
    """A bounded child could not produce a durable exact output."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise DwdRvCompletionIntegrityError(f"{label} is not SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise DwdRvCompletionIntegrityError(f"{label} is invalid")
    return value


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise DwdRvCompletionIntegrityError(f"{label} schema differs")
    if any(not isinstance(key, str) for key in value):
        raise DwdRvCompletionIntegrityError(f"{label} has a non-text key")
    return value


def _assert_secret_free(value: Any) -> None:
    stack = [value]
    nodes = 0
    while stack:
        current = stack.pop()
        nodes += 1
        if nodes > 262_144:
            raise DwdRvCompletionCapacityError("process authority node cap exceeded")
        if isinstance(current, Mapping):
            for key, nested in current.items():
                if not isinstance(key, str) or _SECRET_KEY.search(key):
                    raise DwdRvCompletionIntegrityError(
                        "process authority contains a credential-like key"
                    )
                stack.append(nested)
        elif isinstance(current, (list, tuple)):
            stack.extend(current)


def _safe_existing_path(value: object, label: str, *, directory: bool) -> Path:
    if not isinstance(value, str) or not value or "\0" in value or len(value) > 1024:
        raise DwdRvCompletionIntegrityError(f"{label} path is invalid")
    path = Path(value)
    if not path.is_absolute() or path.is_symlink() or not path.exists():
        raise DwdRvCompletionIntegrityError(f"{label} path is unsafe")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise DwdRvCompletionIntegrityError(f"{label} path is not normalized")
    mode = path.lstat().st_mode
    if directory and not stat.S_ISDIR(mode):
        raise DwdRvCompletionIntegrityError(f"{label} is not a directory")
    if not directory and not stat.S_ISREG(mode):
        raise DwdRvCompletionIntegrityError(f"{label} is not a file")
    return path


def _effect_mapping() -> dict[str, Any]:
    return {
        "contract_id": PROCESS_EFFECT_CONTRACT.contract_id,
        "durable_effects": PROCESS_EFFECT_CONTRACT.durable_effects,
        "hard_kill_safe": PROCESS_EFFECT_CONTRACT.hard_kill_safe,
        "spawns_descendants": PROCESS_EFFECT_CONTRACT.spawns_descendants,
    }


PROCESS_EFFECT_CONTRACT_SHA256 = _digest(_effect_mapping())


@dataclass(frozen=True, slots=True)
class DwdRvCompletionIsolationLimits:
    wall_seconds: float = 90.0
    cpu_seconds: float = 60.0
    rss_bytes: int = 1536 * 1024 * 1024
    max_request_bytes: int = 1024 * 1024
    max_result_bytes: int = 16 * 1024

    def __post_init__(self) -> None:
        for label, value in (
            ("wall_seconds", self.wall_seconds),
            ("cpu_seconds", self.cpu_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not 0.05 <= float(value) <= 3600.0
            ):
                raise ValueError(f"{label} is outside its hard boundary")
        for label, value, minimum, maximum in (
            ("rss_bytes", self.rss_bytes, 32 * 1024 * 1024, 64 * 1024**3),
            ("max_request_bytes", self.max_request_bytes, 1024, 16 * 1024**2),
            ("max_result_bytes", self.max_result_bytes, 1024, 256 * 1024**2),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its hard boundary")

    def to_mapping(self) -> dict[str, int | float]:
        return {
            "cpu_seconds": float(self.cpu_seconds),
            "max_request_bytes": self.max_request_bytes,
            "max_result_bytes": self.max_result_bytes,
            "rss_bytes": self.rss_bytes,
            "wall_seconds": float(self.wall_seconds),
        }

    @property
    def fingerprint(self) -> str:
        return _digest({
            **self.to_mapping(),
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "schema": "stv3-dwd-rv-hard-process-limits-v1",
        })

    def decoder_limits(self) -> DecoderResourceLimits:
        return DecoderResourceLimits(
            wall_seconds=float(self.wall_seconds),
            cpu_seconds=float(self.cpu_seconds),
            memory_bytes=self.rss_bytes,
            max_request_bytes=self.max_request_bytes,
            max_result_bytes=self.max_result_bytes,
        )


def _grid_from_mapping(value: object) -> AnalysisGrid:
    mapping = _exact(value, {
        "axis_order", "contract_version", "coordinate_unit", "crs", "grid_id",
        "height", "origin_x_m", "origin_y_m", "row_order", "width",
        "x_resolution_m", "y_resolution_m",
    }, "analysis grid")
    return AnalysisGrid(**dict(mapping))


def _profile_from_mapping(value: object) -> DwdRvShadowChainProfile:
    mapping = _exact(value, {
        "detector", "pipeline", "schema", "target_grid",
        "target_grid_sha256", "transform",
    }, "DWD chain profile")
    if mapping["schema"] != "stv3-dwd-rv-shadow-chain-profile-v1":
        raise DwdRvCompletionIntegrityError("DWD chain profile contract differs")
    grid = _grid_from_mapping(mapping["target_grid"])
    if analysis_grid_sha256(grid) != _sha(
        mapping["target_grid_sha256"], "target grid SHA-256"
    ):
        raise DwdRvCompletionIntegrityError("target grid checksum differs")
    transform_value = _exact(mapping["transform"], {
        "area_fraction_tolerance", "max_array_bytes", "max_overlap_contributions",
        "max_source_pixels", "max_target_pixels", "resampling_method",
        "roundtrip_tolerance_m",
    }, "transform profile")
    try:
        transform = AnalysisGridTransformConfig(
            resampling_method=AnalysisGridResamplingMethod(
                transform_value["resampling_method"]
            ),
            roundtrip_tolerance_m=transform_value["roundtrip_tolerance_m"],
            area_fraction_tolerance=transform_value["area_fraction_tolerance"],
            max_source_pixels=transform_value["max_source_pixels"],
            max_target_pixels=transform_value["max_target_pixels"],
            max_array_bytes=transform_value["max_array_bytes"],
            max_overlap_contributions=transform_value[
                "max_overlap_contributions"
            ],
        )
    except (TypeError, ValueError) as exc:
        raise DwdRvCompletionIntegrityError("transform profile is invalid") from exc
    detector_value = _exact(mapping["detector"], {
        "centroid_method", "connectivity", "core_position_sigma_m",
        "core_threshold", "detector_version", "minimum_core_area_m2",
        "minimum_shield_area_m2", "shield_position_sigma_m", "shield_threshold",
    }, "detector profile")
    if detector_value["detector_version"] != HIERARCHICAL_DETECTOR_VERSION:
        raise DwdRvCompletionIntegrityError("detector version differs")
    detector = HierarchicalDetectorConfig(**{
        key: detector_value[key]
        for key in detector_value
        if key != "detector_version"
    })
    pipeline_value = _exact(mapping["pipeline"], {
        "algorithm_version", "max_detection_objects", "max_receipt_bytes",
        "max_runtime_seconds", "max_target_pixels",
    }, "pipeline profile")
    if pipeline_value["algorithm_version"] != EVIDENCE_TO_CELLS_SHADOW_ALGORITHM:
        raise DwdRvCompletionIntegrityError("pipeline algorithm differs")
    pipeline = EvidenceToCellsShadowConfig(**{
        key: pipeline_value[key]
        for key in pipeline_value
        if key != "algorithm_version"
    })
    profile = DwdRvShadowChainProfile(
        target_grid=grid,
        transform_config=transform,
        detector_config=detector,
        pipeline_config=pipeline,
    )
    if profile.to_mapping() != dict(mapping):
        raise DwdRvCompletionIntegrityError("DWD chain profile roundtrip differs")
    return profile


@dataclass(frozen=True, slots=True)
class _LoadedAuthority:
    authority_sha256: str
    handoff_raw_root: Path
    handoff_raw_limits: RawArtifactLimits
    artifact_root: Path
    artifact_limits: dict[str, int]
    evidence_path: Path
    evidence_max_database_bytes: int
    output_path: Path
    output_limits: DwdRvCompletionOutputLimits
    profile: DwdRvShadowChainProfile
    isolation_limits: DwdRvCompletionIsolationLimits


def _load_authority(value: object, expected_sha256: str) -> _LoadedAuthority:
    mapping = _exact(value, {
        "consumer_id", "isolation_limits", "profile", "profile_sha256",
        "schema", "stores", "stream_id",
    }, "process authority")
    _assert_secret_free(mapping)
    expected = _sha(expected_sha256, "process authority SHA-256")
    if _digest(mapping) != expected:
        raise DwdRvCompletionIntegrityError("process authority checksum differs")
    if (
        mapping["schema"] != PROCESS_AUTHORITY_SCHEMA
        or mapping["consumer_id"] != CONSUMER_ID
        or mapping["stream_id"] != STREAM_ID
    ):
        raise DwdRvCompletionIntegrityError("process authority route differs")
    profile = _profile_from_mapping(mapping["profile"])
    if profile.fingerprint != _sha(mapping["profile_sha256"], "profile SHA-256"):
        raise DwdRvCompletionIntegrityError("process profile checksum differs")
    limit_value = _exact(mapping["isolation_limits"], {
        "cpu_seconds", "max_request_bytes", "max_result_bytes", "rss_bytes",
        "wall_seconds",
    }, "isolation limits")
    limits = DwdRvCompletionIsolationLimits(**dict(limit_value))
    if limits.to_mapping() != dict(limit_value):
        raise DwdRvCompletionIntegrityError("isolation limits roundtrip differs")
    stores = _exact(mapping["stores"], {
        "canonical_evidence", "derived_artifacts", "handoff_raw", "outputs",
    }, "process stores")
    raw_value = _exact(stores["handoff_raw"], {"limits", "root"}, "raw store")
    raw_limits = RawArtifactLimits(**dict(_exact(
        raw_value["limits"], set(RawArtifactLimits.__dataclass_fields__),
        "raw limits",
    )))
    artifact_value = _exact(
        stores["derived_artifacts"],
        {"max_objects", "max_staging_objects", "max_total_bytes", "root"},
        "artifact store",
    )
    artifact_limits = {
        key: artifact_value[key]
        for key in ("max_objects", "max_staging_objects", "max_total_bytes")
    }
    if any(isinstance(value, bool) or not isinstance(value, int) for value in artifact_limits.values()):
        raise DwdRvCompletionIntegrityError("artifact limits are invalid")
    evidence_value = _exact(
        stores["canonical_evidence"], {"max_database_bytes", "path"},
        "canonical evidence store",
    )
    output_value = _exact(stores["outputs"], {"limits", "path"}, "output store")
    output_limits = DwdRvCompletionOutputLimits(**dict(_exact(
        output_value["limits"], set(DwdRvCompletionOutputLimits.__dataclass_fields__),
        "output limits",
    )))
    evidence_max = evidence_value["max_database_bytes"]
    if isinstance(evidence_max, bool) or not isinstance(evidence_max, int):
        raise DwdRvCompletionIntegrityError("evidence database limit is invalid")
    return _LoadedAuthority(
        authority_sha256=expected,
        handoff_raw_root=_safe_existing_path(
            raw_value["root"], "handoff raw", directory=True
        ),
        handoff_raw_limits=raw_limits,
        artifact_root=_safe_existing_path(
            artifact_value["root"], "derived artifacts", directory=True
        ),
        artifact_limits=artifact_limits,
        evidence_path=_safe_existing_path(
            evidence_value["path"], "canonical evidence", directory=False
        ),
        evidence_max_database_bytes=evidence_max,
        output_path=_safe_existing_path(
            output_value["path"], "completion output", directory=False
        ),
        output_limits=output_limits,
        profile=profile,
        isolation_limits=limits,
    )


def _lease_mapping(lease: CompletionLease) -> dict[str, Any]:
    return {
        "acquisition_job_id": lease.acquisition_job_id,
        "attempt_count": lease.attempt_count,
        "authority_sha256": lease.authority_sha256,
        "consumer_id": lease.consumer_id,
        "content_sha256": lease.content_sha256,
        "desired_state_sha256": lease.desired_state_sha256,
        "event_id": lease.event_id,
        "event_sha256": lease.event_sha256,
        "lease_expires_at": lease.lease_expires_at,
        "producer_receipt_sha256": lease.producer_receipt_sha256,
        "proof": json.loads(_canonical(lease.proof)),
        "size_bytes": lease.size_bytes,
        "stream_id": lease.stream_id,
        "stream_sequence": lease.stream_sequence,
    }


def _binding_mapping(binding: _LeaseBinding) -> dict[str, Any]:
    return {
        "capture_manifest_sha256": binding.capture_manifest_sha256,
        "capture_receipt_sha256": binding.capture_receipt_sha256,
        "evaluated_at": binding.evaluated_at,
        "target_capture_id": binding.target_capture_id,
    }


def _lease_from_mapping(
    value: object,
    raw_store: RawArtifactStore,
) -> CompletionLease:
    mapping = _exact(value, {
        "acquisition_job_id", "attempt_count", "authority_sha256", "consumer_id",
        "content_sha256", "desired_state_sha256", "event_id", "event_sha256",
        "lease_expires_at", "producer_receipt_sha256", "proof", "size_bytes",
        "stream_id", "stream_sequence",
    }, "isolated lease")
    proof = mapping["proof"]
    if not isinstance(proof, dict):
        raise DwdRvCompletionIntegrityError("isolated lease proof is invalid")
    _assert_secret_free(proof)
    binding_proof = proof.get("handoff_capture")
    if not isinstance(binding_proof, Mapping):
        raise DwdRvCompletionIntegrityError("handoff capture proof is absent")
    capture_id = _identifier(binding_proof.get("capture_id"), "capture ID")
    bundle = raw_store.load_capture_bundle_bytes(capture_id)
    if bundle is None or set(bundle[2]) != {"rv_tar"}:
        raise DwdRvCompletionIntegrityError("isolated raw payload is unavailable")
    payload = bundle[2]["rv_tar"]
    sequence = mapping["stream_sequence"]
    attempt = mapping["attempt_count"]
    size = mapping["size_bytes"]
    for item, label in (
        (sequence, "stream sequence"),
        (attempt, "attempt count"),
        (size, "payload size"),
    ):
        if isinstance(item, bool) or not isinstance(item, int) or item < 1:
            raise DwdRvCompletionIntegrityError(f"isolated {label} is invalid")
    expires = mapping["lease_expires_at"]
    if (
        isinstance(expires, bool)
        or not isinstance(expires, (int, float))
        or not math.isfinite(float(expires))
    ):
        raise DwdRvCompletionIntegrityError("isolated lease expiry is invalid")
    return CompletionLease(
        event_id=_identifier(mapping["event_id"], "event ID"),
        event_sha256=_sha(mapping["event_sha256"], "event SHA-256"),
        producer_receipt_sha256=_sha(
            mapping["producer_receipt_sha256"], "producer receipt SHA-256"
        ),
        acquisition_job_id=_identifier(
            mapping["acquisition_job_id"], "acquisition job ID"
        ),
        consumer_id=_identifier(mapping["consumer_id"], "consumer ID"),
        stream_id=_identifier(mapping["stream_id"], "stream ID"),
        stream_sequence=sequence,
        desired_state_sha256=_sha(
            mapping["desired_state_sha256"], "desired state SHA-256"
        ),
        authority_sha256=_sha(mapping["authority_sha256"], "authority SHA-256"),
        content_sha256=_sha(mapping["content_sha256"], "content SHA-256"),
        size_bytes=size,
        attempt_count=attempt,
        lease_token="capability-not-forwarded-to-child",
        lease_expires_at=float(expires),
        proof=proof,
        payload=payload,
    )


def _child_envelope(
    status: Literal[
        "success", "deadline", "resource", "corrupt_contract", "crash"
    ],
    *,
    authority_sha256: str,
    code: str,
    output: DwdRvCompletionOutputReceipt | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "authority_sha256": authority_sha256,
        "code": code,
        "schema": PROCESS_CHILD_RESULT_SCHEMA,
        "status": status,
    }
    if output is not None:
        value["event_id"] = output.event_id
        value["output_sha256"] = output.output_sha256
        value["output_receipt_sha256"] = output.receipt_sha256
        value["process_receipt_sha256"] = output.process_receipt_sha256
    return value


def _dwd_completion_child_entry(request: Mapping[str, Any]) -> dict[str, Any]:
    """Fixed spawn target; no exception text, payload, credential or path returns."""

    authority_sha = "0" * 64
    try:
        exact = _exact(request, {
            "authority", "authority_sha256", "binding", "lease", "request_sha256",
            "schema",
        }, "process request")
        if exact["schema"] != PROCESS_REQUEST_SCHEMA:
            raise DwdRvCompletionIntegrityError("process request contract differs")
        request_body = dict(exact)
        request_sha = request_body.pop("request_sha256")
        if _digest(request_body) != _sha(request_sha, "process request SHA-256"):
            raise DwdRvCompletionIntegrityError("process request checksum differs")
        authority_sha = _sha(exact["authority_sha256"], "authority SHA-256")
        authority = _load_authority(exact["authority"], authority_sha)
        raw_store = RawArtifactStore(
            authority.handoff_raw_root,
            limits=authority.handoff_raw_limits,
        )
        raw_store.initialize()
        artifact_store = DwdRvDerivedArtifactStore(
            authority.artifact_root,
            **authority.artifact_limits,
        )
        evidence_store = CanonicalEvidenceStore(
            authority.evidence_path,
            raw_artifact_reader=raw_store,
            max_database_bytes=authority.evidence_max_database_bytes,
        )
        output_store = DwdRvCompletionOutputStore(
            authority.output_path,
            limits=authority.output_limits,
        )
        lease = _lease_from_mapping(exact["lease"], raw_store)
        actual_binding = _validate_lease(lease, raw_store)
        expected_binding = _exact(exact["binding"], {
            "capture_manifest_sha256", "capture_receipt_sha256", "evaluated_at",
            "target_capture_id",
        }, "lease binding")
        if _binding_mapping(actual_binding) != dict(expected_binding):
            raise DwdRvCompletionIntegrityError("parent/child lease binding differs")
        process_isolation = {
            "authority_sha256": authority.authority_sha256,
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "limits_sha256": authority.isolation_limits.fingerprint,
            "state": "enforced_before_target_start",
        }
        deadline_at = time.monotonic() + min(
            float(authority.isolation_limits.wall_seconds),
            float(authority.profile.pipeline_config.max_runtime_seconds),
        )
        document = _execute_chain(
            lease=lease,
            binding=actual_binding,
            raw_store=raw_store,
            artifact_store=artifact_store,
            evidence_store=evidence_store,
            profile=authority.profile,
            process_isolation=process_isolation,
            deadline_at=deadline_at,
            monotonic=time.monotonic,
            fault=lambda _point: None,
        )
        output = output_store.commit(output=document)
        return _child_envelope(
            "success",
            authority_sha256=authority.authority_sha256,
            code="hard_process_receipts_committed",
            output=output,
        )
    except MemoryError:
        raise
    except OSError as exc:
        if exc.errno == errno.ENOMEM:
            raise
        return _child_envelope(
            "crash", authority_sha256=authority_sha, code="child_store_failed"
        )
    except (
        DwdRvCompletionDeadlineError,
        DwdRvDecodeDeadlineError,
        RadarFieldBridgeDeadlineError,
        EvidenceToCellsShadowDeadlineError,
    ):
        return _child_envelope(
            "deadline",
            authority_sha256=authority_sha,
            code="child_chain_deadline",
        )
    except DwdRvCompletionCapacityError:
        return _child_envelope(
            "resource",
            authority_sha256=authority_sha,
            code="child_chain_capacity",
        )
    except (
        DwdRvCompletionConflict,
        DwdRvCompletionIntegrityError,
        ValueError,
        TypeError,
    ):
        return _child_envelope(
            "corrupt_contract",
            authority_sha256=authority_sha,
            code="authority_request_or_store_invalid",
        )
    except Exception:
        return _child_envelope(
            "crash", authority_sha256=authority_sha, code="dwd_child_failed"
        )


ProcessStatus = Literal[
    "committed", "deadline", "resource", "cancelled", "busy", "boundary",
    "corrupt_contract", "crash",
]


@dataclass(frozen=True, slots=True)
class DwdRvCompletionProcessResult:
    status: ProcessStatus
    execution_outcome: str
    detail_code: str
    authority_sha256: str
    durable_disposition: Literal["committed", "not_committed", "unknown"]
    output_receipt: DwdRvCompletionOutputReceipt | None
    isolation_enforced: bool
    child_exit_code: int | None
    elapsed_seconds: float
    child_cpu_seconds: float | None = None
    child_peak_rss_bytes: int | None = None
    child_peak_boundary_bytes: int | None = None
    meteorological_admission_granted: bool = False
    operational_snapshot_claim_generated: bool = False
    schema: str = PROCESS_RESULT_SCHEMA

    def __post_init__(self) -> None:
        _sha(self.authority_sha256, "process-result authority SHA-256")
        if self.status not in {
            "committed", "deadline", "resource", "cancelled", "busy",
            "boundary", "corrupt_contract", "crash",
        }:
            raise ValueError("process result status is invalid")
        if self.durable_disposition not in {"committed", "not_committed", "unknown"}:
            raise ValueError("process durable disposition is invalid")
        if (self.output_receipt is not None) != (
            self.durable_disposition == "committed"
        ):
            raise ValueError("process output/disposition differs")
        if type(self.isolation_enforced) is not bool:
            raise TypeError("isolation_enforced must be boolean")
        if (
            not isinstance(self.execution_outcome, str)
            or not self.execution_outcome
            or not isinstance(self.detail_code, str)
            or not self.detail_code
        ):
            raise ValueError("process terminal code is invalid")
        if not math.isfinite(self.elapsed_seconds) or self.elapsed_seconds < 0.0:
            raise ValueError("process elapsed time is invalid")
        if self.child_cpu_seconds is not None and (
            isinstance(self.child_cpu_seconds, bool)
            or not isinstance(self.child_cpu_seconds, (int, float))
            or not math.isfinite(float(self.child_cpu_seconds))
            or float(self.child_cpu_seconds) < 0.0
        ):
            raise ValueError("child CPU time is invalid")
        if self.child_peak_rss_bytes is not None and (
            isinstance(self.child_peak_rss_bytes, bool)
            or not isinstance(self.child_peak_rss_bytes, int)
            or self.child_peak_rss_bytes < 0
        ):
            raise ValueError("child peak RSS is invalid")
        if self.child_peak_boundary_bytes is not None and (
            isinstance(self.child_peak_boundary_bytes, bool)
            or not isinstance(self.child_peak_boundary_bytes, int)
            or self.child_peak_boundary_bytes < 0
        ):
            raise ValueError("child peak boundary memory is invalid")
        if (
            self.meteorological_admission_granted
            or self.operational_snapshot_claim_generated
            or self.schema != PROCESS_RESULT_SCHEMA
        ):
            raise ValueError("process result contains an operational claim")
        if len(_canonical(self.to_mapping())) > _MAX_DIAGNOSTIC_BYTES:
            raise ValueError("process diagnostics exceed bound")

    @property
    def committed(self) -> bool:
        return self.status == "committed" and self.output_receipt is not None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "authority_sha256": self.authority_sha256,
            "child_exit_code": self.child_exit_code,
            "child_cpu_seconds": self.child_cpu_seconds,
            "child_peak_rss_bytes": self.child_peak_rss_bytes,
            "child_peak_boundary_bytes": self.child_peak_boundary_bytes,
            "detail_code": self.detail_code,
            "durable_disposition": self.durable_disposition,
            "elapsed_seconds": self.elapsed_seconds,
            "execution_outcome": self.execution_outcome,
            "isolation_enforced": self.isolation_enforced,
            "meteorological_admission_granted": False,
            "operational_snapshot_claim_generated": False,
            "output_identity": (
                None
                if self.output_receipt is None
                else {
                    "event_id": self.output_receipt.event_id,
                    "output_receipt_sha256": self.output_receipt.receipt_sha256,
                    "output_sha256": self.output_receipt.output_sha256,
                    "process_receipt_sha256": (
                        self.output_receipt.process_receipt_sha256
                    ),
                }
            ),
            "schema": self.schema,
            "status": self.status,
        }


def _status(result: DecoderProcessResult) -> ProcessStatus:
    if result.outcome == "deadline_exceeded":
        return "deadline"
    if result.outcome in {
        "cpu_limit_exceeded", "memory_limit_exceeded", "result_too_large",
    }:
        return "resource"
    if result.outcome == "cancelled":
        return "cancelled"
    if result.outcome == "busy":
        return "busy"
    if result.outcome in {
        "unsupported_platform", "boundary_setup_failed", "unsafe_effect_contract",
    }:
        return "boundary"
    if result.outcome in {
        "invalid_request", "result_not_json", "protocol_error",
    }:
        return "corrupt_contract"
    return "crash"


class DwdRvCompletionIsolatedRunner:
    """One-shot hard child with exact output/process receipt reconciliation."""

    def __init__(
        self,
        *,
        handoff_raw_store: RawArtifactStore,
        artifact_store: DwdRvDerivedArtifactStore,
        evidence_store: CanonicalEvidenceStore,
        output_store: DwdRvCompletionOutputStore,
        profile: DwdRvShadowChainProfile,
        limits: DwdRvCompletionIsolationLimits = DwdRvCompletionIsolationLimits(),
        context: multiprocessing.context.BaseContext | None = None,
    ) -> None:
        if not isinstance(handoff_raw_store, RawArtifactStore):
            raise TypeError("handoff_raw_store has wrong type")
        if not isinstance(artifact_store, DwdRvDerivedArtifactStore):
            raise TypeError("artifact_store has wrong type")
        if not isinstance(evidence_store, CanonicalEvidenceStore):
            raise TypeError("evidence_store has wrong type")
        if not isinstance(output_store, DwdRvCompletionOutputStore):
            raise TypeError("output_store has wrong type")
        if not isinstance(profile, DwdRvShadowChainProfile):
            raise TypeError("profile has wrong type")
        if not isinstance(limits, DwdRvCompletionIsolationLimits):
            raise TypeError("limits has wrong type")
        self.handoff_raw_store = handoff_raw_store
        self.artifact_store = artifact_store
        self.evidence_store = evidence_store
        self.output_store = output_store
        self.profile = profile
        self.limits = limits
        self._authority = self._authority_mapping()
        self.authority_sha256 = _digest(self._authority)
        _load_authority(self._authority, self.authority_sha256)
        self._executor = HardDecoderExecutor(
            limits=limits.decoder_limits(),
            context=context,
        )

    @property
    def minimum_lease_seconds(self) -> float:
        return float(self.limits.wall_seconds) + 1.0

    def _authority_mapping(self) -> dict[str, Any]:
        for path, label, directory in (
            (self.handoff_raw_store.root, "handoff raw", True),
            (self.artifact_store.root, "derived artifacts", True),
            (self.evidence_store.path, "canonical evidence", False),
            (self.output_store.path, "completion output", False),
        ):
            _safe_existing_path(str(path), label, directory=directory)
        return {
            "consumer_id": CONSUMER_ID,
            "isolation_limits": self.limits.to_mapping(),
            "profile": self.profile.to_mapping(),
            "profile_sha256": self.profile.fingerprint,
            "schema": PROCESS_AUTHORITY_SCHEMA,
            "stores": {
                "canonical_evidence": {
                    "max_database_bytes": self.evidence_store._max_database_bytes,
                    "path": str(self.evidence_store.path),
                },
                "derived_artifacts": {
                    "max_objects": self.artifact_store.max_objects,
                    "max_staging_objects": self.artifact_store.max_staging_objects,
                    "max_total_bytes": self.artifact_store.max_total_bytes,
                    "root": str(self.artifact_store.root),
                },
                "handoff_raw": {
                    "limits": asdict(self.handoff_raw_store.limits),
                    "root": str(self.handoff_raw_store.root),
                },
                "outputs": {
                    "limits": asdict(self.output_store._limits),
                    "path": str(self.output_store.path),
                },
            },
            "stream_id": STREAM_ID,
        }

    def assert_parent_binding(
        self,
        *,
        handoff_raw_store: RawArtifactStore,
        artifact_store: DwdRvDerivedArtifactStore,
        evidence_store: CanonicalEvidenceStore,
        output_store: DwdRvCompletionOutputStore,
        profile: DwdRvShadowChainProfile,
    ) -> None:
        if (
            handoff_raw_store.root != self.handoff_raw_store.root
            or artifact_store.root != self.artifact_store.root
            or evidence_store.path != self.evidence_store.path
            or output_store.path != self.output_store.path
            or profile.fingerprint != self.profile.fingerprint
            or self._authority_mapping() != self._authority
        ):
            raise DwdRvCompletionConflict(
                "isolated runner differs from parent DWD stores/profile"
            )

    def _load_exact(self, lease: CompletionLease) -> DwdRvCompletionOutputReceipt | None:
        receipt = self.output_store.load(lease.event_id)
        if receipt is None:
            return None
        if (
            receipt.event_sha256 != lease.event_sha256
            or receipt.producer_receipt_sha256 != lease.producer_receipt_sha256
            or receipt.desired_state_sha256 != lease.desired_state_sha256
            or receipt.authority_sha256 != lease.authority_sha256
            or receipt.consumer_id != lease.consumer_id
            or receipt.stream_id != lease.stream_id
            or receipt.profile_sha256 != self.profile.fingerprint
            or receipt.isolation_authority_sha256 != self.authority_sha256
            or receipt.isolation_effect_contract_sha256
            != PROCESS_EFFECT_CONTRACT_SHA256
            or receipt.isolation_limits_sha256 != self.limits.fingerprint
            or receipt.fact_revision_sha256 is not None
        ):
            raise DwdRvCompletionConflict(
                "durable isolated output differs from completion authority"
            )
        self.output_store.verify_for_completion(
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            desired_state_sha256=lease.desired_state_sha256,
            authority_sha256=lease.authority_sha256,
            consumer_id=lease.consumer_id,
            stream_id=lease.stream_id,
            durable_output_receipt_sha256=receipt.receipt_sha256,
            durable_process_receipt_sha256=receipt.process_receipt_sha256,
            process_isolation_authority_sha256=self.authority_sha256,
            output_sha256=receipt.output_sha256,
            disposition=receipt.disposition,
            staged_fact_revision=None,
        )
        return receipt

    def _terminal(
        self,
        *,
        status: ProcessStatus,
        execution_outcome: str,
        detail_code: str,
        started: float,
        receipt: DwdRvCompletionOutputReceipt | None = None,
        isolation_enforced: bool = False,
        child_exit_code: int | None = None,
        child_cpu_seconds: float | None = None,
        child_peak_rss_bytes: int | None = None,
        child_peak_boundary_bytes: int | None = None,
        elapsed_seconds: float | None = None,
        durable_unknown: bool = False,
    ) -> DwdRvCompletionProcessResult:
        return DwdRvCompletionProcessResult(
            status=status,
            execution_outcome=execution_outcome,
            detail_code=detail_code,
            authority_sha256=self.authority_sha256,
            durable_disposition=(
                "unknown" if durable_unknown else (
                    "committed" if receipt is not None else "not_committed"
                )
            ),
            output_receipt=receipt,
            isolation_enforced=isolation_enforced,
            child_exit_code=child_exit_code,
            child_cpu_seconds=child_cpu_seconds,
            child_peak_rss_bytes=child_peak_rss_bytes,
            child_peak_boundary_bytes=child_peak_boundary_bytes,
            elapsed_seconds=(
                max(0.0, time.monotonic() - started)
                if elapsed_seconds is None
                else elapsed_seconds
            ),
        )

    def execute(
        self,
        *,
        lease: CompletionLease,
        binding: _LeaseBinding,
        cancel_event: threading.Event,
    ) -> DwdRvCompletionProcessResult:
        started = time.monotonic()
        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        if not isinstance(binding, _LeaseBinding):
            raise TypeError("binding must be _LeaseBinding")
        if not callable(getattr(cancel_event, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        existing = self._load_exact(lease)
        if existing is not None:
            return self._terminal(
                status="committed",
                execution_outcome="durable_duplicate",
                detail_code="exact_process_receipt_reconciled_before_spawn",
                started=started,
                receipt=existing,
                isolation_enforced=True,
            )
        request_body = {
            "authority": self._authority,
            "authority_sha256": self.authority_sha256,
            "binding": _binding_mapping(binding),
            "lease": _lease_mapping(lease),
            "schema": PROCESS_REQUEST_SCHEMA,
        }
        request = {**request_body, "request_sha256": _digest(request_body)}
        decoder = self._executor.execute(
            target=PROCESS_TARGET,
            request=request,
            effect_contract=PROCESS_EFFECT_CONTRACT,
            cancel_event=cancel_event,
        )
        isolation_enforced = decoder.outcome not in {
            "busy", "unsupported_platform", "boundary_setup_failed",
            "unsafe_effect_contract", "invalid_request",
        } and not (
            decoder.outcome == "cancelled"
            and decoder.detail_code == "cancelled_before_spawn"
        )
        durable_unknown = False
        try:
            receipt = self._load_exact(lease)
        except (DwdRvCompletionConflict, DwdRvCompletionIntegrityError):
            receipt = None
            durable_unknown = True
        if receipt is not None:
            return self._terminal(
                status="committed",
                execution_outcome=decoder.outcome,
                detail_code="durable_process_receipt_reconciled_after_child",
                started=started,
                receipt=receipt,
                isolation_enforced=True,
                child_exit_code=decoder.child_exit_code,
                child_cpu_seconds=decoder.child_cpu_seconds,
                child_peak_rss_bytes=decoder.child_peak_rss_bytes,
                child_peak_boundary_bytes=decoder.child_peak_boundary_bytes,
                elapsed_seconds=decoder.elapsed_seconds,
            )
        if decoder.outcome != "succeeded":
            status = _status(decoder)
            return self._terminal(
                status=status,
                execution_outcome=decoder.outcome,
                detail_code={
                    "deadline": "hard_parent_wall_deadline",
                    "resource": "hard_cpu_rss_or_output_limit",
                    "cancelled": "parent_cancelled_and_tree_killed",
                    "busy": "global_hard_process_slot_busy",
                    "boundary": "kernel_boundary_unavailable",
                    "corrupt_contract": "child_protocol_or_request_invalid",
                    "crash": "child_process_crashed",
                    "committed": "durable_process_receipt_committed",
                }[status],
                started=started,
                isolation_enforced=isolation_enforced,
                child_exit_code=decoder.child_exit_code,
                child_cpu_seconds=decoder.child_cpu_seconds,
                child_peak_rss_bytes=decoder.child_peak_rss_bytes,
                child_peak_boundary_bytes=decoder.child_peak_boundary_bytes,
                elapsed_seconds=decoder.elapsed_seconds,
                durable_unknown=durable_unknown,
            )
        envelope = decoder.result
        if (
            not isinstance(envelope, dict)
            or envelope.get("schema") != PROCESS_CHILD_RESULT_SCHEMA
            or envelope.get("authority_sha256") != self.authority_sha256
        ):
            return self._terminal(
                status="corrupt_contract",
                execution_outcome="succeeded",
                detail_code="child_envelope_invalid_without_durable_receipt",
                started=started,
                isolation_enforced=isolation_enforced,
                child_exit_code=decoder.child_exit_code,
                child_cpu_seconds=decoder.child_cpu_seconds,
                child_peak_rss_bytes=decoder.child_peak_rss_bytes,
                child_peak_boundary_bytes=decoder.child_peak_boundary_bytes,
                elapsed_seconds=decoder.elapsed_seconds,
                durable_unknown=durable_unknown,
            )
        child_status = envelope.get("status")
        status: ProcessStatus = {
            "deadline": "deadline",
            "resource": "resource",
            "corrupt_contract": "corrupt_contract",
        }.get(child_status, "crash")
        return self._terminal(
            status=status,
            execution_outcome="succeeded",
            detail_code={
                "deadline": "child_chain_deadline_without_durable_receipt",
                "resource": "child_chain_capacity_without_durable_receipt",
                "corrupt_contract": (
                    "child_contract_invalid_without_durable_receipt"
                ),
                "crash": "child_returned_without_durable_process_receipt",
            }[status],
            started=started,
            isolation_enforced=isolation_enforced,
            child_exit_code=decoder.child_exit_code,
            child_cpu_seconds=decoder.child_cpu_seconds,
            child_peak_rss_bytes=decoder.child_peak_rss_bytes,
            child_peak_boundary_bytes=decoder.child_peak_boundary_bytes,
            elapsed_seconds=decoder.elapsed_seconds,
            durable_unknown=durable_unknown,
        )

    @staticmethod
    def raise_for_terminal(result: DwdRvCompletionProcessResult) -> None:
        if result.committed:
            return
        if result.status == "deadline":
            raise DwdRvCompletionDeadlineError(result.detail_code)
        if result.status == "cancelled":
            raise DwdRvCompletionCancelled(result.detail_code)
        if result.status == "resource":
            raise DwdRvCompletionCapacityError(result.detail_code)
        if result.status == "corrupt_contract" or result.durable_disposition == "unknown":
            raise DwdRvCompletionIntegrityError(result.detail_code)
        raise DwdRvProcessIsolationError(result.detail_code)


__all__ = [
    "DwdRvCompletionIsolatedRunner",
    "DwdRvCompletionIsolationLimits",
    "DwdRvCompletionProcessResult",
    "DwdRvProcessIsolationError",
    "PROCESS_AUTHORITY_SCHEMA",
    "PROCESS_EFFECT_CONTRACT",
    "PROCESS_EFFECT_CONTRACT_SHA256",
    "PROCESS_RESULT_SCHEMA",
]
