"""Certified, durable and deliberately never-live shadow-to-fact composition.

This module joins existing worker authorities without becoming a new source of
meteorological truth.  It verifies a recovered raw-capture reference, reloads
the corresponding canonical decoder receipt, invokes the existing isolated
science runner and, only after exact cross-binding, calls the existing
science-to-fact bridge.

The current production science receipt contract is structurally shadow-only
and the existing bridge admits only its conspicuous synthetic-test authority.
Consequently every real result produced here is durable but withheld.  A
synthetic authority may stage a synthetic fact for tests, but the receipt below
still forbids dashboard projection, publication and live service wiring.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Protocol, runtime_checkable

from .acquisition_capture_recovery import (
    RECOVERY_BINDING_CATALOG,
    RawRecoveryBinding,
    RecoveredCaptureEvidence,
    recovery_binding_sha256,
)
from .canonical_evidence_store import CanonicalEvidenceStore, DecoderRunReceipt
from .desired_state import DesiredState, DesiredStateStore
from .science_operational_fact_bridge import (
    DurableScienceShadowFactAuthority,
    ScienceOperationalFactBridge,
    ScienceOperationalFactBridgeResult,
    ScienceOperationalFactRequest,
    ScienceTargetFootprint,
    SyntheticScienceTestFileAuthority,
)
from .science_shadow_process_isolation import (
    ScienceShadowIsolatedRunner,
    ScienceShadowProcessResult,
)
from .tracking_core.engine.cell_track_engine import CellTrackLineageHypothesis


CERTIFIED_SHADOW_FACT_PIPELINE_CONTRACT = (
    "stv3-certified-shadow-fact-pipeline-receipt-v1"
)
CERTIFIED_RAW_RECOVERY_REFERENCE_CONTRACT = (
    "stv3-certified-shadow-raw-recovery-reference-v1"
)
CERTIFIED_STAGE_HASHES_CONTRACT = "stv3-certified-shadow-stage-hashes-v1"
CERTIFIED_SHADOW_FACT_STORE_CONTRACT = "stv3-certified-shadow-fact-store-v1"
CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM = (
    "stv3-certified-shadow-fact-pipeline-v1"
)
CERTIFIED_SHADOW_FACT_STORE_SCHEMA_VERSION = 1

MAX_PIPELINE_RECEIPT_BYTES = 64 * 1024
MAX_PIPELINE_DATABASE_BYTES = 64 * 1024 * 1024
MAX_PIPELINE_RECEIPTS = 8_192
MAX_PIPELINE_REASON_CODES = 64
MAX_PIPELINE_STAGE_HASHES = 40

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_REASON_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_SECRET_RE = re.compile(
    r"(?:authorization|password|passwd|secret|credential|api[_-]?key|"
    r"application[_-]?key|access[_-]?token|refresh[_-]?token)",
    re.IGNORECASE,
)

_VERIFIABLE_STAGES = frozenset({
    "analysis_grid",
    "canonical_decoder_normalizer",
    "cell_detection",
    "cell_tracking",
    "fact_staging_boundary",
    "native_radar_field",
    "raw_recovery",
})

_STAGE_HASH_KEYS = frozenset({
    "analysis_field_content",
    "analysis_lineage",
    "bridge_request",
    "bridge_result",
    "canonical_candidate_artifact",
    "canonical_capture_manifest",
    "canonical_receipt",
    "canonical_revision",
    "canonical_result",
    "certificate_reference_set",
    "decoder_config",
    "detector_config",
    "evidence_content",
    "native_candidate_content",
    "native_lineage",
    "pipeline_config",
    "radar_semantics_certificate",
    "raw_capacity_commit",
    "raw_capture_receipt",
    "raw_content",
    "raw_recovery_binding",
    "raw_recovery_reference",
    "raw_role_receipt",
    "science_process_authority",
    "science_process_metadata",
    "science_provenance",
    "science_receipt",
    "science_request",
    "synthetic_fact_binding",
    "synthetic_fact_revision",
    "synthetic_snapshot",
    "target_grid",
    "transform_certificate",
    "transform_config",
    "transform_receipt",
})

_REQUIRED_PROVENANCE_HASHES = {
    "analysis_field_content": "analysis_field_content_sha256",
    "analysis_lineage": "analysis_lineage_sha256",
    "detector_config": "detector_config_sha256",
    "evidence_content": "evidence_content_sha256",
    "native_candidate_content": "native_candidate_content_sha256",
    "native_lineage": "native_lineage_sha256",
    "pipeline_config": "pipeline_config_sha256",
    "radar_semantics_certificate": "radar_semantics_certificate_sha256",
    "target_grid": "target_grid_sha256",
    "transform_certificate": "transform_certificate_sha256",
    "transform_config": "transform_config_sha256",
    "transform_receipt": "transform_receipt_sha256",
}


class CertifiedShadowFactPipelineError(RuntimeError):
    """The certified shadow/fact boundary failed closed."""


class CertifiedShadowFactPipelineConflict(CertifiedShadowFactPipelineError):
    """An idempotency identity was reused for different immutable inputs."""


class CertifiedShadowFactPipelineCapacityError(CertifiedShadowFactPipelineError):
    """A configured durable byte or row boundary was reached."""


class CertifiedShadowFactPipelineCorruption(CertifiedShadowFactPipelineError):
    """The durable pipeline receipt authority is corrupt or incompatible."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, RecursionError) as error:
        raise CertifiedShadowFactPipelineError(
            "pipeline value is not canonical JSON"
        ) from error


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise CertifiedShadowFactPipelineError(
            f"{label} is not a lowercase SHA-256"
        )
    return value


def _identifier(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or _SAFE_ID_RE.fullmatch(value) is None
        or "://" in value
        or _SECRET_RE.search(value) is not None
    ):
        raise CertifiedShadowFactPipelineError(
            f"{label} is not a safe bounded identifier"
        )
    return value


def _counter(value: object, label: str, *, minimum: int = 0) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= 2**63 - 1
    ):
        raise CertifiedShadowFactPipelineError(f"{label} is outside bounds")
    return value


def _reasons(values: Iterable[str]) -> tuple[str, ...]:
    result = tuple(sorted(set(values)))
    if not result or len(result) > MAX_PIPELINE_REASON_CODES:
        raise CertifiedShadowFactPipelineError("pipeline reasons exceed bounds")
    if any(not isinstance(item, str) or _REASON_RE.fullmatch(item) is None for item in result):
        raise CertifiedShadowFactPipelineError("pipeline reason code is invalid")
    return result


def _stages(values: Iterable[str]) -> tuple[str, ...]:
    result = tuple(sorted(set(values)))
    if not set(result) <= _VERIFIABLE_STAGES:
        raise CertifiedShadowFactPipelineError("pipeline stage inventory differs")
    return result


def _reject_secret_material(value: Any) -> None:
    stack: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > 8_192 or depth > 32:
            raise CertifiedShadowFactPipelineError(
                "pipeline receipt exceeds structural bounds"
            )
        if isinstance(current, Mapping):
            for key, item in current.items():
                if not isinstance(key, str) or _SECRET_RE.search(key) is not None:
                    raise CertifiedShadowFactPipelineError(
                        "pipeline receipt contains secret material"
                    )
                stack.append((item, depth + 1))
        elif isinstance(current, (tuple, list)):
            stack.extend((item, depth + 1) for item in current)
        elif isinstance(current, str) and _SECRET_RE.search(current) is not None:
            raise CertifiedShadowFactPipelineError(
                "pipeline receipt contains secret material"
            )


def _science_provenance_sha256(value: Mapping[str, Any] | None) -> str:
    return _digest({
        "contract": "stv3-science-receipt-provenance-v1",
        "value": None if value is None else dict(value),
    })


@dataclass(frozen=True, slots=True)
class CertifiedRawRecoveryReference:
    """Path-free fingerprint of one already reconciled raw capture."""

    reference_sha256: str
    handler_id: str
    recovery_binding_sha256: str
    capture_id: str
    actual_bytes: int
    actual_entries: int
    content_sha256: str
    role_receipt_sha256: str
    capture_receipt_sha256: str
    capacity_commit_sha256: str
    capacity_was_already_committed: bool
    contract: str = CERTIFIED_RAW_RECOVERY_REFERENCE_CONTRACT

    def __post_init__(self) -> None:
        _sha(self.reference_sha256, "raw reference_sha256")
        _identifier(self.handler_id, "raw handler_id")
        _identifier(self.capture_id, "raw capture_id")
        for label, value in (
            ("recovery_binding_sha256", self.recovery_binding_sha256),
            ("content_sha256", self.content_sha256),
            ("role_receipt_sha256", self.role_receipt_sha256),
            ("capture_receipt_sha256", self.capture_receipt_sha256),
            ("capacity_commit_sha256", self.capacity_commit_sha256),
        ):
            _sha(value, label)
        _counter(self.actual_bytes, "raw actual_bytes")
        _counter(self.actual_entries, "raw actual_entries", minimum=1)
        if type(self.capacity_was_already_committed) is not bool:
            raise TypeError("capacity_was_already_committed must be boolean")
        if self.contract != CERTIFIED_RAW_RECOVERY_REFERENCE_CONTRACT:
            raise CertifiedShadowFactPipelineError("raw reference contract differs")
        if self.reference_sha256 != _digest(self.payload_mapping()):
            raise CertifiedShadowFactPipelineError(
                "raw recovery reference checksum differs"
            )

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "actual_bytes": self.actual_bytes,
            "actual_entries": self.actual_entries,
            "capacity_commit_sha256": self.capacity_commit_sha256,
            "capacity_was_already_committed": self.capacity_was_already_committed,
            "capture_id": self.capture_id,
            "capture_receipt_sha256": self.capture_receipt_sha256,
            "content_sha256": self.content_sha256,
            "contract": self.contract,
            "handler_id": self.handler_id,
            "recovery_binding_sha256": self.recovery_binding_sha256,
            "role_receipt_sha256": self.role_receipt_sha256,
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "reference_sha256": self.reference_sha256}

    @classmethod
    def create(
        cls,
        *,
        binding: RawRecoveryBinding,
        evidence: RecoveredCaptureEvidence,
    ) -> "CertifiedRawRecoveryReference":
        if not isinstance(binding, RawRecoveryBinding):
            raise TypeError("binding must be RawRecoveryBinding")
        if not isinstance(evidence, RecoveredCaptureEvidence):
            raise TypeError("evidence must be RecoveredCaptureEvidence")
        catalogued = RECOVERY_BINDING_CATALOG.get(binding.handler_id)
        if catalogued != binding:
            raise CertifiedShadowFactPipelineError(
                "raw recovery binding is not the exact closed catalog entry"
            )
        if not binding.minimum_artifacts <= evidence.actual_entries <= binding.maximum_artifacts:
            raise CertifiedShadowFactPipelineError(
                "recovery entry count differs from its closed binding"
            )
        if evidence.actual_bytes > binding.maximum_transport_bytes:
            raise CertifiedShadowFactPipelineError(
                "recovery bytes exceed the closed transport bound"
            )
        values = {
            "handler_id": binding.handler_id,
            "recovery_binding_sha256": recovery_binding_sha256(binding),
            "capture_id": evidence.capture_id,
            "actual_bytes": evidence.actual_bytes,
            "actual_entries": evidence.actual_entries,
            "content_sha256": evidence.content_sha256,
            "role_receipt_sha256": evidence.receipt_sha256,
            "capture_receipt_sha256": evidence.capture_sha256,
            "capacity_commit_sha256": evidence.capacity_commit_sha256,
            "capacity_was_already_committed": evidence.already_committed,
        }
        temporary = object.__new__(cls)
        for name, value in values.items():
            object.__setattr__(temporary, name, value)
        object.__setattr__(temporary, "reference_sha256", "0" * 64)
        object.__setattr__(
            temporary,
            "contract",
            CERTIFIED_RAW_RECOVERY_REFERENCE_CONTRACT,
        )
        reference_sha256 = _digest(temporary.payload_mapping())
        return cls(reference_sha256=reference_sha256, **values)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
    ) -> "CertifiedRawRecoveryReference":
        expected = {
            "actual_bytes",
            "actual_entries",
            "capacity_commit_sha256",
            "capacity_was_already_committed",
            "capture_id",
            "capture_receipt_sha256",
            "content_sha256",
            "contract",
            "handler_id",
            "recovery_binding_sha256",
            "reference_sha256",
            "role_receipt_sha256",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise CertifiedShadowFactPipelineError(
                "raw recovery reference schema is not exact"
            )
        return cls(**dict(value))


@dataclass(frozen=True, slots=True)
class CertifiedPipelineStageHashes:
    """Closed, name-sorted exact output hashes for every reached stage."""

    items: tuple[tuple[str, str], ...]
    contract: str = CERTIFIED_STAGE_HASHES_CONTRACT

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple) or len(self.items) > MAX_PIPELINE_STAGE_HASHES:
            raise CertifiedShadowFactPipelineError("stage hashes exceed bounds")
        if self.items != tuple(sorted(self.items)):
            raise CertifiedShadowFactPipelineError("stage hashes are not canonical")
        names = tuple(name for name, _value in self.items)
        if len(names) != len(set(names)) or not set(names) <= _STAGE_HASH_KEYS:
            raise CertifiedShadowFactPipelineError("stage hash inventory differs")
        for name, value in self.items:
            if not isinstance(name, str):
                raise CertifiedShadowFactPipelineError("stage hash name is invalid")
            _sha(value, f"stage hash {name}")
        if self.contract != CERTIFIED_STAGE_HASHES_CONTRACT:
            raise CertifiedShadowFactPipelineError("stage hash contract differs")

    @classmethod
    def create(cls, values: Mapping[str, str]) -> "CertifiedPipelineStageHashes":
        return cls(items=tuple(sorted(values.items())))

    def to_mapping(self) -> dict[str, Any]:
        return {"contract": self.contract, "items": dict(self.items)}

    @property
    def fingerprint(self) -> str:
        return _digest(self.to_mapping())

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
    ) -> "CertifiedPipelineStageHashes":
        if not isinstance(value, Mapping) or set(value) != {"contract", "items"}:
            raise CertifiedShadowFactPipelineError("stage hash schema is not exact")
        raw = value["items"]
        if not isinstance(raw, Mapping):
            raise CertifiedShadowFactPipelineError("stage hash items are invalid")
        return cls(
            items=tuple(sorted((str(name), item) for name, item in raw.items())),
            contract=value["contract"],
        )


@dataclass(frozen=True, slots=True)
class CertifiedShadowFactPipelineReceipt:
    """Durable inert or explicitly synthetic-test terminal receipt."""

    receipt_sha256: str
    request_sha256: str
    attempt_id: str
    canonical_run_id: str
    desired_state_sha256: str
    authority_epoch: int
    mode: str
    status: str
    reason_codes: tuple[str, ...]
    verified_stages: tuple[str, ...]
    missing_stages: tuple[str, ...]
    stage_hashes: CertifiedPipelineStageHashes
    exact_output_sha256: str
    science_process_invoked: bool
    science_process_verified: bool
    stage_coverage_complete: bool
    synthetic_test_only: bool
    synthetic_test_fact_staged: bool
    meteorological_admission_granted: bool = False
    created_ready_fact: bool = False
    operational_snapshot_claim_generated: bool = False
    dashboard_projection_generated: bool = False
    publication_authorized: bool = False
    live_service_wiring: bool = False
    algorithm_version: str = CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM
    contract: str = CERTIFIED_SHADOW_FACT_PIPELINE_CONTRACT

    def __post_init__(self) -> None:
        _sha(self.receipt_sha256, "pipeline receipt_sha256")
        _sha(self.request_sha256, "pipeline request_sha256")
        _identifier(self.attempt_id, "pipeline attempt_id")
        if not isinstance(self.canonical_run_id, str) or re.fullmatch(
            r"run-[0-9a-f]{64}", self.canonical_run_id
        ) is None:
            raise CertifiedShadowFactPipelineError("canonical run ID is invalid")
        _sha(self.desired_state_sha256, "desired_state_sha256")
        _counter(self.authority_epoch, "authority_epoch", minimum=1)
        if self.mode not in {"real_shadow", "synthetic_test"}:
            raise CertifiedShadowFactPipelineError("pipeline mode differs")
        if self.status not in {
            "deadline_exceeded",
            "inert",
            "process_failed",
            "rejected",
            "synthetic_test_result",
            "withheld",
        }:
            raise CertifiedShadowFactPipelineError("pipeline status differs")
        _reasons(self.reason_codes)
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise CertifiedShadowFactPipelineError("reason codes are not canonical")
        if self.verified_stages != _stages(self.verified_stages):
            raise CertifiedShadowFactPipelineError("verified stages are not canonical")
        if self.missing_stages != _stages(self.missing_stages):
            raise CertifiedShadowFactPipelineError("missing stages are not canonical")
        if set(self.verified_stages) & set(self.missing_stages):
            raise CertifiedShadowFactPipelineError("pipeline stage sets overlap")
        if set(self.verified_stages) | set(self.missing_stages) != _VERIFIABLE_STAGES:
            raise CertifiedShadowFactPipelineError("pipeline stage coverage is not exact")
        if not isinstance(self.stage_hashes, CertifiedPipelineStageHashes):
            raise TypeError("stage_hashes has the wrong type")
        _sha(self.exact_output_sha256, "exact_output_sha256")
        if self.exact_output_sha256 != self.stage_hashes.fingerprint:
            raise CertifiedShadowFactPipelineError("exact output hash differs")
        for label in (
            "science_process_invoked",
            "science_process_verified",
            "stage_coverage_complete",
            "synthetic_test_only",
            "synthetic_test_fact_staged",
            "meteorological_admission_granted",
            "created_ready_fact",
            "operational_snapshot_claim_generated",
            "dashboard_projection_generated",
            "publication_authorized",
            "live_service_wiring",
        ):
            if type(getattr(self, label)) is not bool:
                raise TypeError(f"{label} must be boolean")
        if self.stage_coverage_complete != (not self.missing_stages):
            raise CertifiedShadowFactPipelineError("stage coverage flag differs")
        if self.science_process_verified and not self.science_process_invoked:
            raise CertifiedShadowFactPipelineError("verified process was not invoked")
        if self.synthetic_test_only != (self.mode == "synthetic_test"):
            raise CertifiedShadowFactPipelineError("synthetic mode flag differs")
        if self.synthetic_test_fact_staged and (
            not self.synthetic_test_only or self.status != "synthetic_test_result"
        ):
            raise CertifiedShadowFactPipelineError(
                "synthetic fact staging escaped its test-only result"
            )
        if any((
            self.meteorological_admission_granted,
            self.created_ready_fact,
            self.operational_snapshot_claim_generated,
            self.dashboard_projection_generated,
            self.publication_authorized,
            self.live_service_wiring,
        )):
            raise CertifiedShadowFactPipelineError(
                "pipeline receipt contains a forbidden live claim"
            )
        if (
            self.algorithm_version != CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM
            or self.contract != CERTIFIED_SHADOW_FACT_PIPELINE_CONTRACT
        ):
            raise CertifiedShadowFactPipelineError(
                "pipeline receipt contract or algorithm differs"
            )
        payload = self.payload_mapping()
        _reject_secret_material(payload)
        if len(_canonical(payload)) > MAX_PIPELINE_RECEIPT_BYTES:
            raise CertifiedShadowFactPipelineCapacityError(
                "pipeline receipt exceeds its byte bound"
            )
        if self.receipt_sha256 != _digest(payload):
            raise CertifiedShadowFactPipelineError("pipeline receipt checksum differs")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "algorithm_version": self.algorithm_version,
            "attempt_id": self.attempt_id,
            "authority_epoch": self.authority_epoch,
            "canonical_run_id": self.canonical_run_id,
            "contract": self.contract,
            "created_ready_fact": self.created_ready_fact,
            "dashboard_projection_generated": self.dashboard_projection_generated,
            "desired_state_sha256": self.desired_state_sha256,
            "exact_output_sha256": self.exact_output_sha256,
            "live_service_wiring": self.live_service_wiring,
            "meteorological_admission_granted": (
                self.meteorological_admission_granted
            ),
            "missing_stages": list(self.missing_stages),
            "mode": self.mode,
            "operational_snapshot_claim_generated": (
                self.operational_snapshot_claim_generated
            ),
            "publication_authorized": self.publication_authorized,
            "reason_codes": list(self.reason_codes),
            "request_sha256": self.request_sha256,
            "science_process_invoked": self.science_process_invoked,
            "science_process_verified": self.science_process_verified,
            "stage_coverage_complete": self.stage_coverage_complete,
            "stage_hashes": self.stage_hashes.to_mapping(),
            "status": self.status,
            "synthetic_test_fact_staged": self.synthetic_test_fact_staged,
            "synthetic_test_only": self.synthetic_test_only,
            "verified_stages": list(self.verified_stages),
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        attempt_id: str,
        canonical_run_id: str,
        desired_state_sha256: str,
        authority_epoch: int,
        mode: str,
        status: str,
        reason_codes: Iterable[str],
        verified_stages: Iterable[str],
        stage_hashes: Mapping[str, str],
        science_process_invoked: bool,
        science_process_verified: bool,
        synthetic_test_fact_staged: bool,
    ) -> "CertifiedShadowFactPipelineReceipt":
        verified = _stages(verified_stages)
        missing = tuple(sorted(_VERIFIABLE_STAGES - set(verified)))
        hashes = CertifiedPipelineStageHashes.create(stage_hashes)
        values = {
            "request_sha256": request_sha256,
            "attempt_id": attempt_id,
            "canonical_run_id": canonical_run_id,
            "desired_state_sha256": desired_state_sha256,
            "authority_epoch": authority_epoch,
            "mode": mode,
            "status": status,
            "reason_codes": _reasons(reason_codes),
            "verified_stages": verified,
            "missing_stages": missing,
            "stage_hashes": hashes,
            "exact_output_sha256": hashes.fingerprint,
            "science_process_invoked": science_process_invoked,
            "science_process_verified": science_process_verified,
            "stage_coverage_complete": not missing,
            "synthetic_test_only": mode == "synthetic_test",
            "synthetic_test_fact_staged": synthetic_test_fact_staged,
        }
        temporary = object.__new__(cls)
        for name, value in values.items():
            object.__setattr__(temporary, name, value)
        object.__setattr__(temporary, "receipt_sha256", "0" * 64)
        for name in (
            "meteorological_admission_granted",
            "created_ready_fact",
            "operational_snapshot_claim_generated",
            "dashboard_projection_generated",
            "publication_authorized",
            "live_service_wiring",
        ):
            object.__setattr__(temporary, name, False)
        object.__setattr__(
            temporary,
            "algorithm_version",
            CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM,
        )
        object.__setattr__(temporary, "contract", CERTIFIED_SHADOW_FACT_PIPELINE_CONTRACT)
        receipt_sha256 = _digest(temporary.payload_mapping())
        return cls(receipt_sha256=receipt_sha256, **values)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
    ) -> "CertifiedShadowFactPipelineReceipt":
        expected = {
            "algorithm_version",
            "attempt_id",
            "authority_epoch",
            "canonical_run_id",
            "contract",
            "created_ready_fact",
            "dashboard_projection_generated",
            "desired_state_sha256",
            "exact_output_sha256",
            "live_service_wiring",
            "meteorological_admission_granted",
            "missing_stages",
            "mode",
            "operational_snapshot_claim_generated",
            "publication_authorized",
            "reason_codes",
            "receipt_sha256",
            "request_sha256",
            "science_process_invoked",
            "science_process_verified",
            "stage_coverage_complete",
            "stage_hashes",
            "status",
            "synthetic_test_fact_staged",
            "synthetic_test_only",
            "verified_stages",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise CertifiedShadowFactPipelineCorruption(
                "pipeline receipt schema is not exact"
            )
        try:
            return cls(
                receipt_sha256=value["receipt_sha256"],
                request_sha256=value["request_sha256"],
                attempt_id=value["attempt_id"],
                canonical_run_id=value["canonical_run_id"],
                desired_state_sha256=value["desired_state_sha256"],
                authority_epoch=value["authority_epoch"],
                mode=value["mode"],
                status=value["status"],
                reason_codes=tuple(value["reason_codes"]),
                verified_stages=tuple(value["verified_stages"]),
                missing_stages=tuple(value["missing_stages"]),
                stage_hashes=CertifiedPipelineStageHashes.from_mapping(
                    value["stage_hashes"]
                ),
                exact_output_sha256=value["exact_output_sha256"],
                science_process_invoked=value["science_process_invoked"],
                science_process_verified=value["science_process_verified"],
                stage_coverage_complete=value["stage_coverage_complete"],
                synthetic_test_only=value["synthetic_test_only"],
                synthetic_test_fact_staged=value["synthetic_test_fact_staged"],
                meteorological_admission_granted=value[
                    "meteorological_admission_granted"
                ],
                created_ready_fact=value["created_ready_fact"],
                operational_snapshot_claim_generated=value[
                    "operational_snapshot_claim_generated"
                ],
                dashboard_projection_generated=value[
                    "dashboard_projection_generated"
                ],
                publication_authorized=value["publication_authorized"],
                live_service_wiring=value["live_service_wiring"],
                algorithm_version=value["algorithm_version"],
                contract=value["contract"],
            )
        except (KeyError, TypeError, ValueError, CertifiedShadowFactPipelineError) as error:
            if isinstance(error, CertifiedShadowFactPipelineCorruption):
                raise
            raise CertifiedShadowFactPipelineCorruption(
                "pipeline receipt values are invalid"
            ) from error


@dataclass(frozen=True, slots=True)
class CertifiedShadowFactPipelineResult:
    receipt: CertifiedShadowFactPipelineReceipt
    duplicate: bool

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, CertifiedShadowFactPipelineReceipt):
            raise TypeError("pipeline result receipt has the wrong type")
        if type(self.duplicate) is not bool:
            raise TypeError("pipeline result duplicate must be boolean")


class CertifiedShadowFactPipelineStore:
    """Bounded FULL-sync SQLite authority for terminal pipeline receipts."""

    _EXPECTED_COLUMNS = {
        "pipeline_manifest": {
            "singleton",
            "schema_version",
            "contract",
            "algorithm_version",
        },
        "pipeline_receipts": {
            "attempt_id",
            "request_sha256",
            "receipt_sha256",
            "canonical_receipt",
            "canonical_receipt_sha256",
            "receipt_bytes",
            "created_at",
            "record_sha256",
        },
    }

    def __init__(
        self,
        path: str | Path,
        *,
        max_receipts: int = MAX_PIPELINE_RECEIPTS,
        max_receipt_bytes: int = MAX_PIPELINE_RECEIPT_BYTES,
        max_database_bytes: int = MAX_PIPELINE_DATABASE_BYTES,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError("pipeline receipt path must be absolute")
        if not self.path.parent.is_dir():
            raise ValueError("pipeline receipt parent directory must exist")
        for label, value, minimum, maximum in (
            ("max_receipts", max_receipts, 1, MAX_PIPELINE_RECEIPTS),
            ("max_receipt_bytes", max_receipt_bytes, 1024, MAX_PIPELINE_RECEIPT_BYTES),
            (
                "max_database_bytes",
                max_database_bytes,
                1024 * 1024,
                MAX_PIPELINE_DATABASE_BYTES,
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its hard boundary")
        if max_receipt_bytes >= max_database_bytes:
            raise ValueError("receipt byte bound must be smaller than database bound")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._max_receipts = max_receipts
        self._max_receipt_bytes = max_receipt_bytes
        self._max_database_bytes = max_database_bytes
        self._clock = clock
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self.initialize()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=5.0)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        mode = str(db.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower()
        if mode != "delete":
            db.close()
            raise CertifiedShadowFactPipelineCorruption(
                "pipeline receipt journal mode differs"
            )
        page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
        db.execute(f"PRAGMA max_page_count={max(1, self._max_database_bytes // page_size)}")
        return db

    @staticmethod
    def _used_bytes(db: sqlite3.Connection) -> int:
        page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
        freelist = int(db.execute("PRAGMA freelist_count").fetchone()[0])
        return max(0, page_count - freelist) * page_size

    def initialize(self) -> None:
        with self._lock, closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS pipeline_manifest("
                    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                    "schema_version INTEGER NOT NULL,"
                    "contract TEXT NOT NULL,"
                    "algorithm_version TEXT NOT NULL)"
                )
                db.execute(
                    "CREATE TABLE IF NOT EXISTS pipeline_receipts("
                    "attempt_id TEXT PRIMARY KEY,"
                    "request_sha256 TEXT NOT NULL UNIQUE,"
                    "receipt_sha256 TEXT NOT NULL UNIQUE,"
                    "canonical_receipt BLOB NOT NULL,"
                    "canonical_receipt_sha256 TEXT NOT NULL,"
                    "receipt_bytes INTEGER NOT NULL,"
                    "created_at REAL NOT NULL,"
                    "record_sha256 TEXT NOT NULL)"
                )
                row = db.execute(
                    "SELECT * FROM pipeline_manifest WHERE singleton=1"
                ).fetchone()
                if row is None:
                    db.execute(
                        "INSERT INTO pipeline_manifest VALUES(1,?,?,?)",
                        (
                            CERTIFIED_SHADOW_FACT_STORE_SCHEMA_VERSION,
                            CERTIFIED_SHADOW_FACT_STORE_CONTRACT,
                            CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM,
                        ),
                    )
                self._verify_schema(db)
                self._verify_all(db)
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def _verify_schema(self, db: sqlite3.Connection) -> None:
        names = {
            str(row[0])
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            if not str(row[0]).startswith("sqlite_")
        }
        if names != set(self._EXPECTED_COLUMNS):
            raise CertifiedShadowFactPipelineCorruption(
                "pipeline receipt table inventory differs"
            )
        for table, expected in self._EXPECTED_COLUMNS.items():
            actual = {
                str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")
            }
            if actual != expected:
                raise CertifiedShadowFactPipelineCorruption(
                    "pipeline receipt columns differ"
                )
        manifest = db.execute(
            "SELECT * FROM pipeline_manifest WHERE singleton=1"
        ).fetchone()
        if (
            manifest is None
            or int(manifest["schema_version"])
            != CERTIFIED_SHADOW_FACT_STORE_SCHEMA_VERSION
            or manifest["contract"] != CERTIFIED_SHADOW_FACT_STORE_CONTRACT
            or manifest["algorithm_version"]
            != CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM
        ):
            raise CertifiedShadowFactPipelineCorruption(
                "pipeline receipt manifest differs"
            )

    @staticmethod
    def _record_sha256(
        *,
        attempt_id: str,
        request_sha256: str,
        receipt_sha256: str,
        canonical_receipt_sha256: str,
        receipt_bytes: int,
        created_at: float,
    ) -> str:
        return _digest({
            "attempt_id": attempt_id,
            "canonical_receipt_sha256": canonical_receipt_sha256,
            "created_at": created_at,
            "receipt_bytes": receipt_bytes,
            "receipt_sha256": receipt_sha256,
            "request_sha256": request_sha256,
            "store_contract": CERTIFIED_SHADOW_FACT_STORE_CONTRACT,
        })

    def _decode_row(self, row: sqlite3.Row) -> CertifiedShadowFactPipelineReceipt:
        body = bytes(row["canonical_receipt"])
        canonical_sha = hashlib.sha256(body).hexdigest()
        if (
            canonical_sha != row["canonical_receipt_sha256"]
            or len(body) != int(row["receipt_bytes"])
        ):
            raise CertifiedShadowFactPipelineCorruption(
                "pipeline receipt bytes differ"
            )
        try:
            value = json.loads(body.decode("ascii"))
        except (UnicodeError, json.JSONDecodeError) as error:
            raise CertifiedShadowFactPipelineCorruption(
                "pipeline receipt JSON is invalid"
            ) from error
        receipt = CertifiedShadowFactPipelineReceipt.from_mapping(value)
        expected_record_sha = self._record_sha256(
            attempt_id=row["attempt_id"],
            request_sha256=row["request_sha256"],
            receipt_sha256=row["receipt_sha256"],
            canonical_receipt_sha256=canonical_sha,
            receipt_bytes=len(body),
            created_at=float(row["created_at"]),
        )
        if (
            receipt.attempt_id != row["attempt_id"]
            or receipt.request_sha256 != row["request_sha256"]
            or receipt.receipt_sha256 != row["receipt_sha256"]
            or row["record_sha256"] != expected_record_sha
        ):
            raise CertifiedShadowFactPipelineCorruption(
                "pipeline receipt materialized columns differ"
            )
        return receipt

    def _verify_all(
        self,
        db: sqlite3.Connection,
    ) -> tuple[CertifiedShadowFactPipelineReceipt, ...]:
        self._verify_schema(db)
        rows = db.execute(
            "SELECT * FROM pipeline_receipts ORDER BY created_at, attempt_id"
        ).fetchall()
        if len(rows) > self._max_receipts:
            raise CertifiedShadowFactPipelineCorruption(
                "pipeline receipt row bound was exceeded"
            )
        return tuple(self._decode_row(row) for row in rows)

    def load_attempt(
        self,
        attempt_id: str,
        *,
        request_sha256: str | None = None,
    ) -> CertifiedShadowFactPipelineReceipt | None:
        attempt = _identifier(attempt_id, "attempt_id")
        if request_sha256 is not None:
            _sha(request_sha256, "request_sha256")
        with self._lock, closing(self._connect()) as db:
            self._verify_all(db)
            row = db.execute(
                "SELECT * FROM pipeline_receipts WHERE attempt_id=?",
                (attempt,),
            ).fetchone()
            if row is None:
                return None
            receipt = self._decode_row(row)
            if request_sha256 is not None and receipt.request_sha256 != request_sha256:
                raise CertifiedShadowFactPipelineConflict(
                    "attempt_id was reused for another request"
                )
            return receipt

    def record(
        self,
        receipt: CertifiedShadowFactPipelineReceipt,
    ) -> tuple[CertifiedShadowFactPipelineReceipt, bool]:
        if not isinstance(receipt, CertifiedShadowFactPipelineReceipt):
            raise TypeError("receipt has the wrong type")
        body = _canonical(receipt.to_mapping())
        if len(body) > self._max_receipt_bytes:
            raise CertifiedShadowFactPipelineCapacityError(
                "pipeline receipt exceeds configured byte capacity"
            )
        canonical_sha = hashlib.sha256(body).hexdigest()
        now = float(self._clock())
        if not math.isfinite(now) or not 0.0 <= now <= 2**53:
            raise CertifiedShadowFactPipelineError("pipeline store clock is invalid")
        with self._lock, closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing_receipts = self._verify_all(db)
                row = db.execute(
                    "SELECT * FROM pipeline_receipts WHERE attempt_id=?",
                    (receipt.attempt_id,),
                ).fetchone()
                if row is not None:
                    existing = self._decode_row(row)
                    if (
                        existing.request_sha256 != receipt.request_sha256
                        or existing.receipt_sha256 != receipt.receipt_sha256
                    ):
                        raise CertifiedShadowFactPipelineConflict(
                            "attempt identity collides with another receipt"
                        )
                    db.commit()
                    return existing, True
                if len(existing_receipts) >= self._max_receipts:
                    raise CertifiedShadowFactPipelineCapacityError(
                        "pipeline receipt row capacity is reached"
                    )
                if self._used_bytes(db) >= self._max_database_bytes:
                    raise CertifiedShadowFactPipelineCapacityError(
                        "pipeline receipt database capacity is reached"
                    )
                record_sha = self._record_sha256(
                    attempt_id=receipt.attempt_id,
                    request_sha256=receipt.request_sha256,
                    receipt_sha256=receipt.receipt_sha256,
                    canonical_receipt_sha256=canonical_sha,
                    receipt_bytes=len(body),
                    created_at=now,
                )
                db.execute(
                    "INSERT INTO pipeline_receipts VALUES(?,?,?,?,?,?,?,?)",
                    (
                        receipt.attempt_id,
                        receipt.request_sha256,
                        receipt.receipt_sha256,
                        body,
                        canonical_sha,
                        len(body),
                        now,
                        record_sha,
                    ),
                )
                self._fault("record.after_insert")
                self._fault("record.before_commit")
                db.commit()
            except sqlite3.IntegrityError as error:
                db.rollback()
                raise CertifiedShadowFactPipelineConflict(
                    "pipeline receipt uniqueness failed"
                ) from error
            except sqlite3.OperationalError as error:
                db.rollback()
                if (
                    getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL
                    or "database or disk is full" in str(error).lower()
                ):
                    raise CertifiedShadowFactPipelineCapacityError(
                        "SQLite pipeline receipt capacity is reached"
                    ) from error
                raise
            except BaseException:
                db.rollback()
                raise
        self._fault("record.after_commit")
        loaded = self.load_attempt(
            receipt.attempt_id,
            request_sha256=receipt.request_sha256,
        )
        if loaded is None:
            raise CertifiedShadowFactPipelineCorruption(
                "committed pipeline receipt cannot be reloaded"
            )
        return loaded, False

    def audit(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as db:
            receipts = self._verify_all(db)
            return {
                "algorithm_version": CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM,
                "contract": CERTIFIED_SHADOW_FACT_STORE_CONTRACT,
                "database_bytes": self._used_bytes(db),
                "live_admission_receipts": sum(
                    int(item.meteorological_admission_granted) for item in receipts
                ),
                "ready_fact_receipts": sum(
                    int(item.created_ready_fact) for item in receipts
                ),
                "receipt_count": len(receipts),
                "synthetic_test_receipts": sum(
                    int(item.synthetic_test_only) for item in receipts
                ),
            }


@runtime_checkable
class ScienceShadowProcessExecutor(Protocol):
    """Only the existing isolated runner's exact operation used here."""

    def execute(
        self,
        *,
        attempt_id: str,
        canonical_run_id: str,
        lineage_hypotheses: Iterable[CellTrackLineageHypothesis] = (),
        cancel_event: threading.Event | None = None,
    ) -> ScienceShadowProcessResult: ...


class CertifiedShadowFactPipeline:
    """One serialized, content-addressed and default-closed composition."""

    def __init__(
        self,
        *,
        desired_state_store: DesiredStateStore,
        canonical_evidence_store: CanonicalEvidenceStore,
        science_process_runner: ScienceShadowProcessExecutor,
        fact_bridge: ScienceOperationalFactBridge,
        receipt_store: CertifiedShadowFactPipelineStore,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(desired_state_store, DesiredStateStore):
            raise TypeError("desired_state_store has the wrong type")
        if not isinstance(canonical_evidence_store, CanonicalEvidenceStore):
            raise TypeError("canonical_evidence_store has the wrong type")
        if not isinstance(science_process_runner, ScienceShadowProcessExecutor):
            raise TypeError("science_process_runner has the wrong contract")
        if not isinstance(fact_bridge, ScienceOperationalFactBridge):
            raise TypeError("fact_bridge has the wrong type")
        if not isinstance(receipt_store, CertifiedShadowFactPipelineStore):
            raise TypeError("receipt_store has the wrong type")
        if not callable(monotonic):
            raise TypeError("monotonic must be callable")
        self._desired = desired_state_store
        self._canonical = canonical_evidence_store
        self._runner = science_process_runner
        self._bridge = fact_bridge
        self._receipts = receipt_store
        self._monotonic = monotonic
        self._lock = threading.RLock()

    @staticmethod
    def _mode(
        authority: DurableScienceShadowFactAuthority | SyntheticScienceTestFileAuthority,
    ) -> str:
        if type(authority) is DurableScienceShadowFactAuthority:
            return "real_shadow"
        if type(authority) is SyntheticScienceTestFileAuthority:
            return "synthetic_test"
        raise TypeError("fact authority type is not admitted by the pipeline")

    @staticmethod
    def _deadline(deadline_at: float, monotonic: Callable[[], float]) -> bool:
        if (
            isinstance(deadline_at, bool)
            or not isinstance(deadline_at, (int, float))
            or not math.isfinite(float(deadline_at))
        ):
            raise ValueError("deadline_at must be an absolute finite monotonic time")
        now = float(monotonic())
        if not math.isfinite(now):
            raise CertifiedShadowFactPipelineError("monotonic clock is invalid")
        return now < float(deadline_at)

    @staticmethod
    def _request_sha256(
        *,
        attempt_id: str,
        request: ScienceOperationalFactRequest,
        raw: CertifiedRawRecoveryReference,
        mode: str,
    ) -> str:
        return _digest({
            "algorithm_version": CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM,
            "attempt_id": attempt_id,
            "bridge_request_sha256": request.bridge_request_sha256,
            "canonical_run_id": request.canonical_run_id,
            "contract": "stv3-certified-shadow-fact-pipeline-request-v1",
            "desired_state_sha256": request.desired_state_sha256,
            "mode": mode,
            "raw_recovery_reference_sha256": raw.reference_sha256,
        })

    @staticmethod
    def _active_target_matches(
        desired: DesiredState,
        request: ScienceOperationalFactRequest,
    ) -> bool:
        matches = tuple(
            item for item in desired.targets if item.target_id == request.footprint.target_id
        )
        return (
            len(matches) == 1
            and matches[0].available
            and ScienceTargetFootprint.from_target(matches[0]) == request.footprint
        )

    @staticmethod
    def _normalized_bridge_mapping(
        result: ScienceOperationalFactBridgeResult,
        *,
        synthetic: bool,
    ) -> dict[str, Any]:
        binding = None
        if result.binding is not None:
            binding = {
                "authority_epoch": result.binding.authority_epoch,
                "desired_state_sha256": result.binding.desired_state_sha256,
                "pipeline_item_id": result.binding.pipeline_item_id,
                "process_receipt_sha256": result.binding.process_receipt_sha256,
                "revision_sha256": result.binding.revision_sha256,
                "snapshot_revision": result.binding.snapshot_revision,
                "snapshot_sha256": result.binding.snapshot_sha256,
            }
        status = result.status
        reasons = list(result.reason_codes)
        if synthetic and status in {"staged", "duplicate"}:
            status = "synthetic_test_fact_present"
            reasons = ["synthetic_test_only"]
        return {
            "binding": binding,
            "bridge_request_sha256": result.bridge_request_sha256,
            "contract": "stv3-certified-shadow-normalized-bridge-result-v1",
            "reason_codes": reasons,
            "science_receipt_sha256": result.science_receipt_sha256,
            "status": status,
        }

    def _terminal(
        self,
        *,
        request_sha256: str,
        request: ScienceOperationalFactRequest,
        mode: str,
        status: str,
        reasons: Iterable[str],
        verified_stages: Iterable[str],
        stage_hashes: Mapping[str, str],
        process_invoked: bool,
        process_verified: bool,
        synthetic_test_fact_staged: bool = False,
    ) -> CertifiedShadowFactPipelineResult:
        receipt = CertifiedShadowFactPipelineReceipt.create(
            request_sha256=request_sha256,
            attempt_id=request.science_attempt_id,
            canonical_run_id=request.canonical_run_id,
            desired_state_sha256=request.desired_state_sha256,
            authority_epoch=request.authority_epoch,
            mode=mode,
            status=status,
            reason_codes=reasons,
            verified_stages=verified_stages,
            stage_hashes=stage_hashes,
            science_process_invoked=process_invoked,
            science_process_verified=process_verified,
            synthetic_test_fact_staged=synthetic_test_fact_staged,
        )
        recorded, duplicate = self._receipts.record(receipt)
        return CertifiedShadowFactPipelineResult(recorded, duplicate)

    def process(
        self,
        request: ScienceOperationalFactRequest,
        *,
        recovery_binding: RawRecoveryBinding,
        raw_recovery_evidence: RecoveredCaptureEvidence,
        authority: DurableScienceShadowFactAuthority | SyntheticScienceTestFileAuthority,
        deadline_at: float,
        lineage_hypotheses: Iterable[CellTrackLineageHypothesis] = (),
        cancel_event: threading.Event | None = None,
    ) -> CertifiedShadowFactPipelineResult:
        """Verify and stage exactly one already specified fact request.

        The request must already name the expected science/process receipts.
        This is intentional: there is no product code in the repository that
        can honestly derive a rich operational fact from CellTrack output yet.
        The method therefore verifies existing durable output and never invents
        rain rates, target claims or admission certificates.
        """

        if not isinstance(request, ScienceOperationalFactRequest):
            raise TypeError("request has the wrong type")
        hypotheses = tuple(lineage_hypotheses)
        if hypotheses:
            raise CertifiedShadowFactPipelineError(
                "lineage_hypotheses must be empty for this exact contract"
            )
        attempt = _identifier(request.science_attempt_id, "science_attempt_id")
        raw = CertifiedRawRecoveryReference.create(
            binding=recovery_binding,
            evidence=raw_recovery_evidence,
        )
        mode = self._mode(authority)
        request_sha256 = self._request_sha256(
            attempt_id=attempt,
            request=request,
            raw=raw,
            mode=mode,
        )
        with self._lock:
            existing = self._receipts.load_attempt(
                attempt,
                request_sha256=request_sha256,
            )
            if existing is not None:
                return CertifiedShadowFactPipelineResult(existing, True)

            stage_hashes = {
                "bridge_request": request.bridge_request_sha256,
                "certificate_reference_set": _digest({
                    "certificate_sha256s": list(
                        request.provenance.certificate_sha256s
                    ),
                    "contract": "stv3-certified-certificate-reference-set-v1",
                }),
                "raw_capacity_commit": raw.capacity_commit_sha256,
                "raw_capture_receipt": raw.capture_receipt_sha256,
                "raw_content": raw.content_sha256,
                "raw_recovery_binding": raw.recovery_binding_sha256,
                "raw_recovery_reference": raw.reference_sha256,
                "raw_role_receipt": raw.role_receipt_sha256,
            }
            verified: set[str] = {"raw_recovery"}

            if mode == "real_shadow" and type(self._runner) is not ScienceShadowIsolatedRunner:
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="inert",
                    reasons=("real_science_process_executor_not_certified",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=False,
                    process_verified=False,
                )

            if not self._deadline(deadline_at, self._monotonic):
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="deadline_exceeded",
                    reasons=("pipeline_deadline_expired_before_science",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=False,
                    process_verified=False,
                )

            desired = self._desired.load_active()
            if desired is None:
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="inert",
                    reasons=("desired_state_unavailable",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=False,
                    process_verified=False,
                )
            desired_reasons: list[str] = []
            if (
                desired.state_sha256 != request.desired_state_sha256
                or desired.authority_epoch != request.authority_epoch
            ):
                desired_reasons.append("desired_state_stale")
            if not self._active_target_matches(desired, request):
                desired_reasons.append("target_footprint_mismatch")
            if desired_reasons:
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="inert",
                    reasons=desired_reasons,
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=False,
                    process_verified=False,
                )

            try:
                canonical = self._canonical.load(request.canonical_run_id)
            except Exception:
                canonical = None
            if canonical is None or not isinstance(canonical, DecoderRunReceipt):
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="inert",
                    reasons=("canonical_decoder_receipt_unavailable",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=False,
                    process_verified=False,
                )
            canonical_reasons: list[str] = []
            candidate = canonical.run.non_operational_candidate_artifact
            if canonical.run_id != request.canonical_run_id:
                canonical_reasons.append("canonical_run_identity_mismatch")
            if not canonical.is_structurally_complete_candidate or candidate is None:
                canonical_reasons.append("canonical_decoder_output_incomplete")
            if (
                canonical.run.capture_id != raw.capture_id
                or canonical.run.capture_receipt_sha256
                != raw.capture_receipt_sha256
            ):
                canonical_reasons.append("raw_canonical_capture_mismatch")
            if len(canonical.run.members) != raw.actual_entries:
                canonical_reasons.append("raw_canonical_member_count_mismatch")
            if raw.actual_entries == 1 and canonical.run.members:
                member = canonical.run.members[0]
                if (
                    member.content_sha256 != raw.content_sha256
                    or member.receipt_sha256 != raw.role_receipt_sha256
                ):
                    canonical_reasons.append("raw_canonical_member_digest_mismatch")
            if canonical_reasons:
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="inert",
                    reasons=canonical_reasons,
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=False,
                    process_verified=False,
                )
            stage_hashes.update({
                "canonical_candidate_artifact": candidate.sha256,
                "canonical_capture_manifest": canonical.capture_manifest_sha256,
                "canonical_receipt": canonical.receipt_sha256,
                "canonical_revision": canonical.revision_sha256,
                "canonical_result": canonical.result_sha256,
                "decoder_config": canonical.run.config_sha256,
            })
            verified.add("canonical_decoder_normalizer")

            try:
                process_result = self._runner.execute(
                    attempt_id=attempt,
                    canonical_run_id=request.canonical_run_id,
                    lineage_hypotheses=hypotheses,
                    cancel_event=cancel_event,
                )
            except Exception:
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="process_failed",
                    reasons=("science_process_call_failed",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=False,
                )
            if not isinstance(process_result, ScienceShadowProcessResult):
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="process_failed",
                    reasons=("science_process_result_contract_invalid",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=False,
                )
            stage_hashes["science_process_authority"] = process_result.authority_sha256
            reconciled_concrete_duplicate = (
                mode == "real_shadow"
                and type(self._runner) is ScienceShadowIsolatedRunner
                and process_result.duplicate is True
                and process_result.execution_outcome == "durable_duplicate"
                and process_result.committed
            )
            if not process_result.succeeded or not (
                process_result.isolation_enforced or reconciled_concrete_duplicate
            ):
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="process_failed",
                    reasons=(f"science_process_{process_result.status}",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=False,
                )

            metadata = process_result.receipt_metadata
            if metadata is None:
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="process_failed",
                    reasons=("science_process_metadata_missing",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=False,
                )
            provenance = metadata.get("provenance")
            link_reasons: list[str] = []
            if not isinstance(provenance, Mapping):
                link_reasons.append("science_provenance_missing")
                provenance = {}
            if (
                metadata.get("canonical_run_id") != request.canonical_run_id
                or metadata.get("engine_id") != request.engine_id
                or metadata.get("request_sha256") != request.science_request_sha256
                or metadata.get("runtime_profile_sha256")
                != request.runtime_profile_sha256
            ):
                link_reasons.append("science_process_request_binding_mismatch")
            if (
                metadata.get("authority_sha256")
                != request.process_authority_sha256
                or process_result.authority_sha256
                != request.process_authority_sha256
                or metadata.get("metadata_sha256")
                != request.process_receipt_sha256
            ):
                link_reasons.append("science_process_receipt_binding_mismatch")
            science_receipt_sha = metadata.get("receipt_sha256")
            if not isinstance(science_receipt_sha, str) or _SHA256_RE.fullmatch(
                science_receipt_sha
            ) is None:
                link_reasons.append("science_receipt_identity_invalid")
            elif (
                mode == "real_shadow"
                and request.science_receipt_sha256 != science_receipt_sha
            ):
                link_reasons.append("science_receipt_request_mismatch")
            required_sources = {
                raw.role_receipt_sha256,
                raw.capture_receipt_sha256,
                canonical.receipt_sha256,
            }
            if isinstance(science_receipt_sha, str):
                required_sources.add(science_receipt_sha)
            if not required_sources <= set(request.provenance.source_receipt_sha256s):
                link_reasons.append("source_receipt_coverage_incomplete")
            science_provenance_sha = _science_provenance_sha256(provenance)
            if (
                request.provenance.science_provenance_sha256
                != science_provenance_sha
                or request.provenance.profile_sha256
                != request.runtime_profile_sha256
            ):
                link_reasons.append("science_provenance_binding_mismatch")
            for hash_name, provenance_name in _REQUIRED_PROVENANCE_HASHES.items():
                value = provenance.get(provenance_name)
                if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                    link_reasons.append("science_stage_hash_coverage_incomplete")
                    continue
                stage_hashes[hash_name] = value
            if (
                provenance.get("canonical_commit_id") != canonical.commit_id
                or provenance.get("canonical_receipt_sha256")
                != canonical.receipt_sha256
                or provenance.get("canonical_result_sha256")
                != canonical.result_sha256
                or provenance.get("capture_id") != raw.capture_id
                or provenance.get("capture_receipt_sha256")
                != raw.capture_receipt_sha256
                or provenance.get("capture_manifest_sha256")
                != canonical.capture_manifest_sha256
            ):
                link_reasons.append("science_canonical_provenance_mismatch")
            required_certificates = {
                provenance.get("radar_semantics_certificate_sha256"),
                provenance.get("transform_certificate_sha256"),
            }
            if (
                None in required_certificates
                or not required_certificates
                <= set(request.provenance.certificate_sha256s)
            ):
                link_reasons.append("science_certificate_coverage_incomplete")
            if metadata.get("outcome") != "advanced" or metadata.get("stage") != "tracking":
                link_reasons.append("science_tracking_not_advanced")
            stage_hashes.update({
                "science_process_metadata": metadata["metadata_sha256"],
                "science_provenance": science_provenance_sha,
                "science_receipt": (
                    science_receipt_sha
                    if isinstance(science_receipt_sha, str)
                    and _SHA256_RE.fullmatch(science_receipt_sha)
                    else "0" * 64
                ),
                "science_request": request.science_request_sha256,
            })
            if link_reasons:
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="inert",
                    reasons=link_reasons,
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=False,
                )
            verified.update({
                "analysis_grid",
                "cell_detection",
                "cell_tracking",
                "native_radar_field",
            })

            if not self._deadline(deadline_at, self._monotonic):
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="deadline_exceeded",
                    reasons=("pipeline_deadline_expired_after_science",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=True,
                )

            desired_after = self._desired.load_active()
            if (
                desired_after is None
                or desired_after.state_sha256 != desired.state_sha256
                or desired_after.authority_epoch != desired.authority_epoch
            ):
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="inert",
                    reasons=("desired_state_changed_after_science",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=True,
                )

            try:
                bridge_result = self._bridge.stage(request, authority=authority)
            except Exception:
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="rejected",
                    reasons=("fact_staging_boundary_failed",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=True,
                )
            if not isinstance(bridge_result, ScienceOperationalFactBridgeResult):
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="rejected",
                    reasons=("fact_staging_result_contract_invalid",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=True,
                )
            normalized_bridge = self._normalized_bridge_mapping(
                bridge_result,
                synthetic=mode == "synthetic_test",
            )
            stage_hashes["bridge_result"] = _digest(normalized_bridge)
            verified.add("fact_staging_boundary")

            if mode == "synthetic_test" and bridge_result.status in {
                "staged",
                "duplicate",
            }:
                binding = bridge_result.binding
                assert binding is not None
                binding_mapping = normalized_bridge["binding"]
                assert isinstance(binding_mapping, Mapping)
                stage_hashes["synthetic_fact_binding"] = _digest(binding_mapping)
                stage_hashes["synthetic_fact_revision"] = binding.revision_sha256
                stage_hashes["synthetic_snapshot"] = binding.snapshot_sha256
                return self._terminal(
                    request_sha256=request_sha256,
                    request=request,
                    mode=mode,
                    status="synthetic_test_result",
                    reasons=("synthetic_test_only",),
                    verified_stages=verified,
                    stage_hashes=stage_hashes,
                    process_invoked=True,
                    process_verified=True,
                    synthetic_test_fact_staged=True,
                )

            if mode == "synthetic_test":
                reasons = bridge_result.reason_codes or (
                    "synthetic_test_authority_rejected",
                )
                status = "withheld" if bridge_result.status == "withheld" else "rejected"
            elif bridge_result.status == "withheld":
                reasons = tuple(bridge_result.reason_codes) + (
                    "meteorological_admission_certificate_authority_unavailable",
                    "rich_fact_projection_unavailable",
                    "operational_publication_forbidden",
                    "live_service_wiring_absent",
                )
                status = "withheld"
            elif bridge_result.status == "rejected":
                reasons = bridge_result.reason_codes
                status = "rejected"
            else:
                # The current real bridge cannot reach this branch.  If a
                # future contract starts staging real facts, this older
                # composition records the drift but never grants publication.
                reasons = ("real_fact_staging_not_admitted_by_pipeline",)
                status = "rejected"
            return self._terminal(
                request_sha256=request_sha256,
                request=request,
                mode=mode,
                status=status,
                reasons=reasons,
                verified_stages=verified,
                stage_hashes=stage_hashes,
                process_invoked=True,
                process_verified=True,
            )


__all__ = [
    "CERTIFIED_RAW_RECOVERY_REFERENCE_CONTRACT",
    "CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM",
    "CERTIFIED_SHADOW_FACT_PIPELINE_CONTRACT",
    "CERTIFIED_SHADOW_FACT_STORE_CONTRACT",
    "CERTIFIED_STAGE_HASHES_CONTRACT",
    "CertifiedPipelineStageHashes",
    "CertifiedRawRecoveryReference",
    "CertifiedShadowFactPipeline",
    "CertifiedShadowFactPipelineCapacityError",
    "CertifiedShadowFactPipelineConflict",
    "CertifiedShadowFactPipelineCorruption",
    "CertifiedShadowFactPipelineError",
    "CertifiedShadowFactPipelineReceipt",
    "CertifiedShadowFactPipelineResult",
    "CertifiedShadowFactPipelineStore",
    "ScienceShadowProcessExecutor",
]
