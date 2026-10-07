"""Canonical, payload-free provenance for decoder outcomes.

The schema-3 :mod:`storm_tracker_worker.raw_artifacts` store remains the only
authority for upstream bytes.  This module stores immutable decoder receipts
which *refer* to one fully verified raw capture; it never reads, copies, or
stores a raw payload or filesystem path.

This is provenance plumbing, not a decoder and not an operational weather
admission policy.  A ``decoded_complete`` receipt is only a structurally
complete, explicitly non-operational candidate.  No receipt from this module
is meteorological evidence or may be consumed as an admitted weather product.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
from itertools import islice
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Literal, Mapping, Protocol, runtime_checkable

from .raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
)


EVIDENCE_CONTRACT = "stv3-worker-canonical-derived-evidence-v2"
EVIDENCE_RECEIPT_SCHEMA = (
    "stv3-worker-canonical-derived-evidence-receipt-v2"
)
EVIDENCE_REVISION_SCHEMA = (
    "stv3-worker-canonical-derived-evidence-revision-v2"
)
RUN_IDENTITY_SCHEMA = "stv3-worker-canonical-decoder-run-identity-v2"
RESULT_IDENTITY_SCHEMA = "stv3-worker-canonical-decoder-result-identity-v2"
COMMIT_IDENTITY_SCHEMA = "stv3-worker-canonical-decoder-commit-identity-v2"
REVISION_SCOPE_SCHEMA = "stv3-worker-canonical-decoder-revision-scope-v2"
STORE_SCHEMA_VERSION = 2
STORE_ALGORITHM_VERSION = "canonical-evidence-store-v2"

DEFAULT_MAX_DATABASE_BYTES = 256 * 1024 * 1024
MAX_CANONICAL_OPERATION_BYTES = 64 * 1024
MAX_CAPTURE_MEMBERS = 64
MAX_TOTAL_RUNS = 1_000_000
MAX_TOTAL_MEMBER_LINKS = 16_000_000
MAX_TIMESTAMP_SECONDS = 253_402_300_799.0
MAX_ABSOLUTE_NATIVE_COORDINATE = 1.0e15

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CAPTURE_ID_RE = re.compile(r"^capture-[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^run-[0-9a-f]{64}$")
_COMMIT_ID_RE = re.compile(r"^commit-[0-9a-f]{64}$")
_SCOPE_ID_RE = re.compile(r"^scope-[0-9a-f]{64}$")
_DECODER_JOB_ID_RE = re.compile(r"^decoder-job-[0-9a-f]{64}$")
_ATTEMPT_ID_RE = re.compile(r"^decoder-attempt-[0-9a-f]{64}$")
_SAFE_CODE_RE = re.compile(r"^[a-z][a-z0-9_.:-]{0,127}$")
_OUTPUT_HANDLE_RE = re.compile(
    r"^artifact:[A-Za-z0-9][A-Za-z0-9._:@+-]{0,502}$"
)

DecoderOutcome = Literal[
    "decoded_complete",
    "decode_error",
    "unsupported",
    "incomplete",
]
_OUTCOMES = frozenset({
    "decoded_complete",
    "decode_error",
    "unsupported",
    "incomplete",
})
_NON_METEOROLOGICAL_OUTCOMES = frozenset({
    "decode_error",
    "unsupported",
    "incomplete",
})
_GRID_KINDS = frozenset({
    "regular",
    "affine",
    "curvilinear",
    "unstructured",
})
_AXIS_ORDERS = frozenset({"xy", "yx"})


class CanonicalEvidenceError(RuntimeError):
    """Base failure for canonical decoder provenance."""


class CanonicalEvidenceConflict(CanonicalEvidenceError):
    """A stable run or commit identity was reused for different facts."""


class CanonicalEvidenceIntegrityError(CanonicalEvidenceError):
    """The local catalog or referenced raw authority failed verification."""


class CanonicalEvidenceCapacityError(CanonicalEvidenceError):
    """A hard metadata-store capacity boundary rejected a whole commit."""


class CaptureVerificationError(CanonicalEvidenceIntegrityError):
    """The referenced schema-3 raw capture was absent or did not match."""


@runtime_checkable
class RawCaptureMemberReceiptView(Protocol):
    """Structural payload-free member view supplied by the raw authority."""

    @property
    def member_id(self) -> str: ...

    role: str
    receipt_id: str
    content_sha256: str
    receipt_sha256: str
    received_at: float


@runtime_checkable
class RawCaptureReceiptView(Protocol):
    """Structural schema-3 capture receipt returned by the raw authority."""

    capture_id: str
    provider: str
    product: str
    fetch_id: str
    upstream_capture_id: str
    product_timestamp: float | None
    manifest_sha256: str
    capture_receipt_sha256: str
    members: tuple[RawCaptureMemberReceiptView, ...]

    @property
    def raw_store_schema_version(self) -> int: ...

    @property
    def capture_schema(self) -> str: ...

    @property
    def capture_receipt_schema(self) -> str: ...

    @property
    def complete(self) -> bool: ...


@runtime_checkable
class RawArtifactCaptureReader(Protocol):
    """Only raw-store operation used by this module."""

    def load_capture_receipt(
        self,
        capture_id: str,
    ) -> RawCaptureReceiptView | None: ...


@dataclass(frozen=True, slots=True)
class DecoderMemberBinding:
    member_id: str
    role: str
    receipt_id: str
    content_sha256: str
    receipt_sha256: str
    received_at: float

    def to_mapping(self) -> dict[str, Any]:
        return {
            "member_id": self.member_id,
            "role": self.role,
            "receipt_id": self.receipt_id,
            "content_sha256": self.content_sha256,
            "receipt_sha256": self.receipt_sha256,
            "received_at": self.received_at,
        }


@dataclass(frozen=True, slots=True)
class NativeGridSpec:
    kind: str
    width: int
    height: int
    axis_order: str
    definition_sha256: str

    def to_mapping(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "width": self.width,
            "height": self.height,
            "axis_order": self.axis_order,
            "definition_sha256": self.definition_sha256,
        }


@dataclass(frozen=True, slots=True)
class CoverageSpec:
    kind: Literal["native_bbox"]
    minimum_x: float
    minimum_y: float
    maximum_x: float
    maximum_y: float
    definition_sha256: str

    def to_mapping(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "minimum_x": self.minimum_x,
            "minimum_y": self.minimum_y,
            "maximum_x": self.maximum_x,
            "maximum_y": self.maximum_y,
            "definition_sha256": self.definition_sha256,
        }


@dataclass(frozen=True, slots=True)
class NonOperationalCandidateArtifactRef:
    """Opaque structural candidate reference; never an admission token."""

    handle: str
    sha256: str

    def to_mapping(self) -> dict[str, Any]:
        return {"handle": self.handle, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class DecoderRun:
    """One fully bounded decoder result before durable commitment."""

    decoder_job_id: str
    attempt_id: str
    capture_id: str
    capture_receipt_sha256: str
    members: tuple[DecoderMemberBinding, ...]
    decoder_identity: str
    decoder_version: str
    config_sha256: str
    provider: str
    product: str
    product_time: float | None
    received_at: float
    evaluated_at: float
    quantity: str | None
    unit: str | None
    native_crs: str | None
    native_grid: NativeGridSpec | None
    coverage: CoverageSpec | None
    non_operational_candidate_artifact: NonOperationalCandidateArtifactRef | None
    outcome: DecoderOutcome
    reason_code: str

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": EVIDENCE_CONTRACT,
            "decoder_job_id": self.decoder_job_id,
            "attempt_id": self.attempt_id,
            "capture": {
                "capture_id": self.capture_id,
                "capture_receipt_sha256": self.capture_receipt_sha256,
                "members": [member.to_mapping() for member in self.members],
            },
            "decoder": {
                "identity": self.decoder_identity,
                "version": self.decoder_version,
                "config_sha256": self.config_sha256,
            },
            "provider": self.provider,
            "product": self.product,
            "product_time": self.product_time,
            "received_at": self.received_at,
            "evaluated_at": self.evaluated_at,
            "quantity": self.quantity,
            "unit": self.unit,
            "native_crs": self.native_crs,
            "native_grid": (
                None if self.native_grid is None
                else self.native_grid.to_mapping()
            ),
            "coverage": (
                None if self.coverage is None else self.coverage.to_mapping()
            ),
            "non_operational_candidate_artifact": (
                None if self.non_operational_candidate_artifact is None
                else self.non_operational_candidate_artifact.to_mapping()
            ),
            "outcome": self.outcome,
            "reason_code": self.reason_code,
        }


@dataclass(frozen=True, slots=True)
class DecoderRunReceipt:
    """Immutable, raw-payload-free receipt for one decoder run."""

    run_id: str
    commit_id: str
    revision_scope_id: str
    revision: int
    previous_receipt_sha256: str | None
    revision_sha256: str
    receipt_sha256: str
    result_sha256: str
    capture_manifest_sha256: str
    committed_at: float
    run: DecoderRun

    @property
    def is_structurally_complete_candidate(self) -> bool:
        """Whether all declared candidate fields are structurally present.

        This is deliberately not meteorological or operational admission.
        """
        return self.run.outcome == "decoded_complete"

    @property
    def non_operational_candidate_artifact_handle(self) -> str | None:
        """Opaque candidate handle; consumers must not treat it as admitted."""
        if (
            not self.is_structurally_complete_candidate
            or self.run.non_operational_candidate_artifact is None
        ):
            return None
        return self.run.non_operational_candidate_artifact.handle

    def to_metadata(self) -> dict[str, Any]:
        return {
            "schema": EVIDENCE_RECEIPT_SCHEMA,
            "run_id": self.run_id,
            "commit_id": self.commit_id,
            "revision_scope_id": self.revision_scope_id,
            "revision": self.revision,
            "previous_receipt_sha256": self.previous_receipt_sha256,
            "revision_sha256": self.revision_sha256,
            "receipt_sha256": self.receipt_sha256,
            "result_sha256": self.result_sha256,
            "capture_manifest_sha256": self.capture_manifest_sha256,
            "committed_at": self.committed_at,
            "is_structurally_complete_candidate": (
                self.is_structurally_complete_candidate
            ),
            "non_operational_candidate_artifact_handle": (
                self.non_operational_candidate_artifact_handle
            ),
            "run": self.run.to_mapping(),
        }


@dataclass(frozen=True, slots=True)
class _VerifiedCapture:
    capture_id: str
    provider: str
    product: str
    fetch_id: str
    upstream_capture_id: str
    product_timestamp: float | None
    manifest_sha256: str
    capture_receipt_sha256: str
    members: tuple[DecoderMemberBinding, ...]


def _exact_keys(
    value: object,
    expected: frozenset[str],
    name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    actual = frozenset(value.keys())
    string_keys = frozenset(key for key in actual if isinstance(key, str))
    if actual != expected or len(string_keys) != len(actual):
        missing = sorted(expected - string_keys)
        extra = sorted(repr(key) for key in actual if key not in expected)
        raise ValueError(f"{name} keys mismatch; missing={missing}, extra={extra}")
    return value


def _text(value: object, name: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{name} is invalid")
    return value


def _code(value: object, name: str) -> str:
    result = _text(value, name, 128)
    if not _SAFE_CODE_RE.fullmatch(result):
        raise ValueError(f"{name} is invalid")
    return result


def _sha256(value: object, name: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{name} is not a lowercase SHA-256")
    return value


def _timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} is invalid")
    result = float(value)
    if (
        not math.isfinite(result)
        or result < 0.0
        or result > MAX_TIMESTAMP_SECONDS
    ):
        raise ValueError(f"{name} is invalid")
    return result


def _nullable_timestamp(value: object, name: str) -> float | None:
    return None if value is None else _timestamp(value, name)


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} is invalid")
    result = float(value)
    if (
        not math.isfinite(result)
        or abs(result) > MAX_ABSOLUTE_NATIVE_COORDINATE
    ):
        raise ValueError(f"{name} is invalid")
    return result


def _nullable_text(
    value: object,
    name: str,
    maximum: int,
) -> str | None:
    return None if value is None else _text(value, name, maximum)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _parse_member(value: object, index: int) -> DecoderMemberBinding:
    raw = _exact_keys(
        value,
        frozenset({
            "member_id",
            "role",
            "receipt_id",
            "content_sha256",
            "receipt_sha256",
            "received_at",
        }),
        f"capture.members[{index}]",
    )
    member_id = _text(raw["member_id"], "member_id", 128)
    role = _text(raw["role"], "role", 128)
    if member_id != role:
        raise ValueError("schema-3 member_id must equal its unique role")
    return DecoderMemberBinding(
        member_id=member_id,
        role=role,
        receipt_id=_text(raw["receipt_id"], "receipt_id", 96),
        content_sha256=_sha256(raw["content_sha256"], "content_sha256"),
        receipt_sha256=_sha256(raw["receipt_sha256"], "receipt_sha256"),
        received_at=_timestamp(raw["received_at"], "member.received_at"),
    )


def _parse_grid(value: object) -> NativeGridSpec | None:
    if value is None:
        return None
    raw = _exact_keys(
        value,
        frozenset({
            "kind",
            "width",
            "height",
            "axis_order",
            "definition_sha256",
        }),
        "native_grid",
    )
    kind = _text(raw["kind"], "native_grid.kind", 32)
    if kind not in _GRID_KINDS:
        raise ValueError("native_grid.kind is unsupported")
    axis_order = _text(raw["axis_order"], "native_grid.axis_order", 2)
    if axis_order not in _AXIS_ORDERS:
        raise ValueError("native_grid.axis_order is unsupported")
    width = raw["width"]
    height = raw["height"]
    if (
        isinstance(width, bool)
        or not isinstance(width, int)
        or width < 1
        or width > 1_000_000
        or isinstance(height, bool)
        or not isinstance(height, int)
        or height < 1
        or height > 1_000_000
        or width * height > 2_147_483_647
    ):
        raise ValueError("native_grid dimensions are invalid")
    return NativeGridSpec(
        kind=kind,
        width=width,
        height=height,
        axis_order=axis_order,
        definition_sha256=_sha256(
            raw["definition_sha256"],
            "native_grid.definition_sha256",
        ),
    )


def _parse_coverage(value: object) -> CoverageSpec | None:
    if value is None:
        return None
    raw = _exact_keys(
        value,
        frozenset({
            "kind",
            "minimum_x",
            "minimum_y",
            "maximum_x",
            "maximum_y",
            "definition_sha256",
        }),
        "coverage",
    )
    kind = _text(raw["kind"], "coverage.kind", 32)
    if kind != "native_bbox":
        raise ValueError("coverage.kind must be native_bbox")
    minimum_x = _finite(raw["minimum_x"], "coverage.minimum_x")
    minimum_y = _finite(raw["minimum_y"], "coverage.minimum_y")
    maximum_x = _finite(raw["maximum_x"], "coverage.maximum_x")
    maximum_y = _finite(raw["maximum_y"], "coverage.maximum_y")
    if minimum_x >= maximum_x or minimum_y >= maximum_y:
        raise ValueError("coverage bounds are inverted or empty")
    return CoverageSpec(
        kind="native_bbox",
        minimum_x=minimum_x,
        minimum_y=minimum_y,
        maximum_x=maximum_x,
        maximum_y=maximum_y,
        definition_sha256=_sha256(
            raw["definition_sha256"],
            "coverage.definition_sha256",
        ),
    )


def _parse_candidate_artifact(
    value: object,
) -> NonOperationalCandidateArtifactRef | None:
    if value is None:
        return None
    raw = _exact_keys(
        value,
        frozenset({"handle", "sha256"}),
        "non_operational_candidate_artifact",
    )
    handle = _text(
        raw["handle"],
        "non_operational_candidate_artifact.handle",
        512,
    )
    if not _OUTPUT_HANDLE_RE.fullmatch(handle):
        raise ValueError(
            "output artifact must be an opaque artifact: handle, not a path"
        )
    return NonOperationalCandidateArtifactRef(
        handle=handle,
        sha256=_sha256(
            raw["sha256"],
            "non_operational_candidate_artifact.sha256",
        ),
    )


def parse_decoder_run(value: object) -> DecoderRun:
    """Parse one exact, closed decoder provenance contract."""
    raw = _exact_keys(
        value,
        frozenset({
            "contract",
            "decoder_job_id",
            "attempt_id",
            "capture",
            "decoder",
            "provider",
            "product",
            "product_time",
            "received_at",
            "evaluated_at",
            "quantity",
            "unit",
            "native_crs",
            "native_grid",
            "coverage",
            "non_operational_candidate_artifact",
            "outcome",
            "reason_code",
        }),
        "decoder run",
    )
    if raw["contract"] != EVIDENCE_CONTRACT:
        raise ValueError("unsupported decoder evidence contract")
    decoder_job_id = _text(raw["decoder_job_id"], "decoder_job_id", 96)
    if not _DECODER_JOB_ID_RE.fullmatch(decoder_job_id):
        raise ValueError(
            "decoder_job_id must be a preassigned decoder-job SHA-256 identity"
        )
    attempt_id = _text(raw["attempt_id"], "attempt_id", 96)
    if not _ATTEMPT_ID_RE.fullmatch(attempt_id):
        raise ValueError(
            "attempt_id must be a preassigned decoder-attempt SHA-256 identity"
        )
    capture = _exact_keys(
        raw["capture"],
        frozenset({"capture_id", "capture_receipt_sha256", "members"}),
        "capture",
    )
    capture_id = _text(capture["capture_id"], "capture_id", 72)
    if not _CAPTURE_ID_RE.fullmatch(capture_id):
        raise ValueError("capture_id is not a schema-3 capture identity")
    raw_members = capture["members"]
    if not isinstance(raw_members, list):
        raise ValueError("capture.members must be a list")
    if not 1 <= len(raw_members) <= MAX_CAPTURE_MEMBERS:
        raise ValueError("capture.members count is outside bounds")
    members = tuple(
        sorted(
            (_parse_member(member, index) for index, member in enumerate(raw_members)),
            key=lambda member: member.member_id,
        )
    )
    if (
        len({member.member_id for member in members}) != len(members)
        or len({member.receipt_id for member in members}) != len(members)
    ):
        raise ValueError("capture members are not uniquely bound")
    decoder = _exact_keys(
        raw["decoder"],
        frozenset({"identity", "version", "config_sha256"}),
        "decoder",
    )
    received_at = _timestamp(raw["received_at"], "received_at")
    evaluated_at = _timestamp(raw["evaluated_at"], "evaluated_at")
    if evaluated_at < received_at:
        raise ValueError("evaluated_at cannot predate received_at")
    outcome = _text(raw["outcome"], "outcome", 32)
    if outcome not in _OUTCOMES:
        raise ValueError("unsupported decoder outcome")
    reason_code = _code(raw["reason_code"], "reason_code")
    expected_prefix = (
        "decoded_complete"
        if outcome == "decoded_complete"
        else f"{outcome}:"
    )
    if (
        outcome == "decoded_complete" and reason_code != expected_prefix
    ) or (
        outcome in _NON_METEOROLOGICAL_OUTCOMES
        and not reason_code.startswith(expected_prefix)
    ):
        raise ValueError("reason_code does not match decoder outcome")
    quantity = _nullable_text(raw["quantity"], "quantity", 128)
    unit = _nullable_text(raw["unit"], "unit", 128)
    native_crs = _nullable_text(raw["native_crs"], "native_crs", 512)
    native_grid = _parse_grid(raw["native_grid"])
    coverage = _parse_coverage(raw["coverage"])
    candidate_artifact = _parse_candidate_artifact(
        raw["non_operational_candidate_artifact"]
    )
    candidate_fields = (
        quantity,
        unit,
        native_crs,
        native_grid,
        coverage,
        candidate_artifact,
    )
    if outcome == "decoded_complete":
        if any(item is None for item in candidate_fields):
            raise ValueError(
                "decoded_complete requires all structural candidate metadata"
            )
    elif any(item is not None for item in candidate_fields):
        raise ValueError(
            "non-complete outcomes cannot expose derived candidate fields"
        )
    run = DecoderRun(
        decoder_job_id=decoder_job_id,
        attempt_id=attempt_id,
        capture_id=capture_id,
        capture_receipt_sha256=_sha256(
            capture["capture_receipt_sha256"],
            "capture_receipt_sha256",
        ),
        members=members,
        decoder_identity=_text(
            decoder["identity"], "decoder.identity", 256
        ),
        decoder_version=_text(
            decoder["version"], "decoder.version", 128
        ),
        config_sha256=_sha256(
            decoder["config_sha256"], "decoder.config_sha256"
        ),
        provider=_text(raw["provider"], "provider", 128),
        product=_text(raw["product"], "product", 256),
        product_time=_nullable_timestamp(raw["product_time"], "product_time"),
        received_at=received_at,
        evaluated_at=evaluated_at,
        quantity=quantity,
        unit=unit,
        native_crs=native_crs,
        native_grid=native_grid,
        coverage=coverage,
        non_operational_candidate_artifact=candidate_artifact,
        outcome=outcome,  # type: ignore[arg-type]
        reason_code=reason_code,
    )
    encoded = _canonical_json(run.to_mapping())
    if len(encoded) > MAX_CANONICAL_OPERATION_BYTES:
        raise ValueError("decoder operation exceeds its byte bound")
    return run


def _validated_decoder_run(
    run: DecoderRun | Mapping[str, Any],
) -> DecoderRun:
    """Apply the closed contract even to directly constructed dataclasses."""
    return parse_decoder_run(run.to_mapping() if isinstance(run, DecoderRun) else run)


def decoder_result_sha256(run: DecoderRun | Mapping[str, Any]) -> str:
    """Hash result facts without the two identities assigned before decoding."""
    parsed = _validated_decoder_run(run)
    result = parsed.to_mapping()
    del result["decoder_job_id"]
    del result["attempt_id"]
    document = {"result": result, "schema": RESULT_IDENTITY_SCHEMA}
    return hashlib.sha256(_canonical_json(document)).hexdigest()


def decoder_run_id(run: DecoderRun | Mapping[str, Any]) -> str:
    """Bind one preassigned attempt to its independently hashed result."""
    parsed = _validated_decoder_run(run)
    identity = {
        "attempt_id": parsed.attempt_id,
        "decoder_job_id": parsed.decoder_job_id,
        "result_sha256": decoder_result_sha256(parsed),
        "schema": RUN_IDENTITY_SCHEMA,
    }
    digest = hashlib.sha256(_canonical_json(identity)).hexdigest()
    return f"run-{digest}"


def decoder_commit_id(run: DecoderRun | Mapping[str, Any]) -> str:
    """Derive stable retry identity solely from facts fixed before decoding."""
    parsed = _validated_decoder_run(run)
    document = {
        "attempt_id": parsed.attempt_id,
        "capture_id": parsed.capture_id,
        "capture_receipt_sha256": parsed.capture_receipt_sha256,
        "config_sha256": parsed.config_sha256,
        "decoder_identity": parsed.decoder_identity,
        "decoder_job_id": parsed.decoder_job_id,
        "decoder_version": parsed.decoder_version,
        "schema": COMMIT_IDENTITY_SCHEMA,
    }
    digest = hashlib.sha256(_canonical_json(document)).hexdigest()
    return f"commit-{digest}"


def _normalize_schema_sql(value: str) -> str:
    """Canonicalize layout outside quotes while preserving complete DDL."""
    source = value.strip()
    if source.endswith(";"):
        source = source[:-1].rstrip()
    result: list[str] = []
    quote: str | None = None
    pending_space = False
    index = 0
    while index < len(source):
        character = source[index]
        if quote is not None:
            result.append(character)
            closing = "]" if quote == "[" else quote
            if character == closing:
                if index + 1 < len(source) and source[index + 1] == closing:
                    index += 1
                    result.append(source[index])
                else:
                    quote = None
            index += 1
            continue
        if character.isspace():
            pending_space = True
            index += 1
            continue
        if pending_space and result:
            result.append(" ")
        pending_space = False
        result.append(character)
        if character in ("'", '"', "`", "["):
            quote = character
        index += 1
    return "".join(result)


_TABLE_DDL: tuple[tuple[str, str], ...] = (
    (
        "manifest",
        """CREATE TABLE manifest(
            key TEXT PRIMARY KEY,value TEXT NOT NULL
        ) WITHOUT ROWID""",
    ),
    (
        "decoder_runs",
        """CREATE TABLE decoder_runs(
            run_id TEXT PRIMARY KEY,commit_id TEXT NOT NULL UNIQUE,
            decoder_job_id TEXT NOT NULL,attempt_id TEXT NOT NULL,
            revision_scope_id TEXT NOT NULL,revision INTEGER NOT NULL,
            previous_receipt_sha256 TEXT,revision_sha256 TEXT NOT NULL,
            receipt_sha256 TEXT NOT NULL,result_sha256 TEXT NOT NULL,
            canonical_run TEXT NOT NULL,
            canonical_run_bytes INTEGER NOT NULL,
            capture_id TEXT NOT NULL,capture_receipt_sha256 TEXT NOT NULL,
            capture_manifest_sha256 TEXT NOT NULL,
            provider TEXT NOT NULL,product TEXT NOT NULL,
            decoder_identity TEXT NOT NULL,decoder_version TEXT NOT NULL,
            config_sha256 TEXT NOT NULL,product_time REAL,
            received_at REAL NOT NULL,evaluated_at REAL NOT NULL,
            quantity TEXT,unit TEXT,native_crs TEXT,
            native_grid_sha256 TEXT,coverage_sha256 TEXT,
            non_operational_candidate_handle TEXT,
            non_operational_candidate_sha256 TEXT,
            outcome TEXT NOT NULL,reason_code TEXT NOT NULL,
            member_count INTEGER NOT NULL,committed_at REAL NOT NULL,
            algorithm_version TEXT NOT NULL,
            UNIQUE(decoder_job_id,attempt_id),
            UNIQUE(revision_scope_id,revision),
            CHECK(revision>=1),
            CHECK(member_count BETWEEN 1 AND 64),
            CHECK(canonical_run_bytes BETWEEN 1 AND 65536),
            CHECK(length(result_sha256)=64),
            CHECK(length(revision_sha256)=64),
            CHECK(length(receipt_sha256)=64),
            CHECK(length(capture_receipt_sha256)=64),
            CHECK(length(capture_manifest_sha256)=64),
            CHECK(length(config_sha256)=64)
        ) WITHOUT ROWID""",
    ),
    (
        "decoder_run_members",
        """CREATE TABLE decoder_run_members(
            run_id TEXT NOT NULL REFERENCES decoder_runs(run_id),
            member_id TEXT NOT NULL,role TEXT NOT NULL,
            receipt_id TEXT NOT NULL,content_sha256 TEXT NOT NULL,
            receipt_sha256 TEXT NOT NULL,received_at REAL NOT NULL,
            algorithm_version TEXT NOT NULL,
            PRIMARY KEY(run_id,member_id),
            UNIQUE(run_id,role),UNIQUE(run_id,receipt_id),
            CHECK(member_id=role),
            CHECK(length(content_sha256)=64),
            CHECK(length(receipt_sha256)=64)
        ) WITHOUT ROWID""",
    ),
)

_INDEX_DDL: tuple[tuple[str, str, str], ...] = (
    (
        "decoder_runs_capture",
        "decoder_runs",
        """CREATE INDEX decoder_runs_capture
            ON decoder_runs(capture_id,run_id)""",
    ),
    (
        "decoder_runs_scope",
        "decoder_runs",
        """CREATE INDEX decoder_runs_scope
            ON decoder_runs(revision_scope_id,revision)""",
    ),
)

_TRIGGER_DDL: tuple[tuple[str, str, str], ...] = tuple(
    (
        f"{table}_no_{action}",
        table,
        f"""CREATE TRIGGER {table}_no_{action}
            BEFORE {action.upper()} ON {table} BEGIN
                SELECT RAISE(ABORT,'append-only table');
            END""",
    )
    for table in ("manifest", "decoder_runs", "decoder_run_members")
    for action in ("update", "delete")
)

_EXPECTED_SCHEMA_OBJECTS = tuple(sorted(
    [
        ("table", name, name, _normalize_schema_sql(sql))
        for name, sql in _TABLE_DDL
    ]
    + [
        ("index", name, table, _normalize_schema_sql(sql))
        for name, table, sql in _INDEX_DDL
    ]
    + [
        ("trigger", name, table, _normalize_schema_sql(sql))
        for name, table, sql in _TRIGGER_DDL
    ]
))
_EXPECTED_SCHEMA_FINGERPRINT = hashlib.sha256(
    _canonical_json(_EXPECTED_SCHEMA_OBJECTS)
).hexdigest()


class CanonicalEvidenceStore:
    """Append-only SQLite catalog of verified decoder-run receipts."""

    def __init__(
        self,
        path: str | Path,
        *,
        raw_artifact_reader: RawArtifactCaptureReader,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
        max_database_bytes: int = DEFAULT_MAX_DATABASE_BYTES,
    ) -> None:
        supplied = Path(path)
        if not supplied.is_absolute():
            raise ValueError("canonical evidence path must be absolute")
        if (
            isinstance(max_database_bytes, bool)
            or not isinstance(max_database_bytes, int)
            or max_database_bytes < 1024 * 1024
        ):
            raise ValueError("max_database_bytes must be at least 1 MiB")
        if not isinstance(raw_artifact_reader, RawArtifactCaptureReader):
            raise TypeError(
                "raw_artifact_reader must provide load_capture_receipt"
            )
        self.path = supplied
        self._raw_artifact_reader = raw_artifact_reader
        self._clock = clock
        self._fault_injector = fault_injector
        self._max_database_bytes = max_database_bytes
        self._lock = threading.RLock()
        self.initialize()

    def initialize(self) -> dict[str, int]:
        """Create or validate the isolated catalog; never migrate another store."""
        with self._lock:
            self._prepare_path()
            try:
                with closing(self._connect_unconfigured()) as database:
                    database.execute("PRAGMA foreign_keys=ON")
                    database.execute("PRAGMA synchronous=FULL")
                    database.execute("PRAGMA journal_mode=WAL")
                    database.execute("BEGIN IMMEDIATE")
                    try:
                        self._fault("initialize.after_begin")
                        tables = self._table_names(database)
                        if not tables:
                            self._create_schema(database)
                        self._validate_schema(database)
                        self._fault("initialize.before_commit")
                        database.commit()
                        self._fault("initialize.after_commit")
                    except BaseException:
                        if database.in_transaction:
                            database.rollback()
                        raise
                with closing(self._connect()) as database:
                    self._validate_schema(database, full_integrity=True)
            except sqlite3.DatabaseError as exc:
                raise CanonicalEvidenceIntegrityError(
                    "canonical evidence catalog is invalid"
                ) from exc
        return self.stats()

    def commit(
        self,
        run: DecoderRun | Mapping[str, Any],
    ) -> DecoderRunReceipt:
        """Verify schema-3 raw authority and atomically append one decoder run."""
        parsed = _validated_decoder_run(run)
        canonical_run = _canonical_json(parsed.to_mapping())
        if len(canonical_run) > MAX_CANONICAL_OPERATION_BYTES:
            raise ValueError("decoder operation exceeds its byte bound")
        result_sha256 = decoder_result_sha256(parsed)
        run_id = decoder_run_id(parsed)
        commit_id = decoder_commit_id(parsed)

        capture = self._verify_capture(parsed)
        scope_id = self._revision_scope_id(parsed)
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                self._fault("commit.after_begin")
                existing = database.execute(
                    "SELECT * FROM decoder_runs WHERE commit_id=?",
                    (commit_id,),
                ).fetchone()
                if existing is not None:
                    if (
                        str(existing["run_id"]) != run_id
                        or str(existing["result_sha256"]) != result_sha256
                        or str(existing["canonical_run"]).encode("utf-8")
                        != canonical_run
                    ):
                        raise CanonicalEvidenceConflict(
                            "commit identity was reused for different facts"
                        )
                    database.commit()
                    receipt = self._validated_receipt(
                        existing,
                        self._member_rows(database, run_id),
                    )
                    self._validate_previous_link(database, existing)
                    self._validate_capture_snapshot(receipt, capture)
                    return receipt
                existing_run = database.execute(
                    "SELECT commit_id FROM decoder_runs WHERE run_id=?",
                    (run_id,),
                ).fetchone()
                if existing_run is not None:
                    raise CanonicalEvidenceConflict(
                        "decoder run already has a different commit identity"
                    )
                existing_attempt = database.execute(
                    "SELECT commit_id FROM decoder_runs "
                    "WHERE decoder_job_id=? AND attempt_id=?",
                    (parsed.decoder_job_id, parsed.attempt_id),
                ).fetchone()
                if existing_attempt is not None:
                    raise CanonicalEvidenceConflict(
                        "decoder job attempt was reused for different facts"
                    )
                self._enforce_capacity(database, len(parsed.members))
                previous = database.execute(
                    "SELECT revision,receipt_sha256 FROM decoder_runs "
                    "WHERE revision_scope_id=? ORDER BY revision DESC LIMIT 1",
                    (scope_id,),
                ).fetchone()
                revision = 1 if previous is None else int(previous["revision"]) + 1
                previous_receipt_sha256 = (
                    None if previous is None else str(previous["receipt_sha256"])
                )
                revision_sha256 = self._revision_sha256(
                    scope_id=scope_id,
                    revision=revision,
                    previous_receipt_sha256=previous_receipt_sha256,
                    run_id=run_id,
                    result_sha256=result_sha256,
                )
                committed_at = _timestamp(self._clock(), "committed_at")
                receipt_sha256 = self._receipt_sha256(
                    run_id=run_id,
                    commit_id=commit_id,
                    revision_scope_id=scope_id,
                    revision=revision,
                    previous_receipt_sha256=previous_receipt_sha256,
                    revision_sha256=revision_sha256,
                    result_sha256=result_sha256,
                    capture_receipt_sha256=capture.capture_receipt_sha256,
                    capture_manifest_sha256=capture.manifest_sha256,
                    committed_at=committed_at,
                )
                grid_sha = self._optional_document_sha(parsed.native_grid)
                coverage_sha = self._optional_document_sha(parsed.coverage)
                candidate_handle = (
                    None
                    if parsed.non_operational_candidate_artifact is None
                    else parsed.non_operational_candidate_artifact.handle
                )
                candidate_sha = (
                    None
                    if parsed.non_operational_candidate_artifact is None
                    else parsed.non_operational_candidate_artifact.sha256
                )
                database.execute(
                    """
                    INSERT INTO decoder_runs(
                        run_id,commit_id,decoder_job_id,attempt_id,
                        revision_scope_id,revision,
                        previous_receipt_sha256,revision_sha256,receipt_sha256,
                        result_sha256,canonical_run,
                        canonical_run_bytes,capture_id,
                        capture_receipt_sha256,capture_manifest_sha256,
                        provider,product,
                        decoder_identity,decoder_version,config_sha256,
                        product_time,received_at,evaluated_at,quantity,unit,
                        native_crs,native_grid_sha256,coverage_sha256,
                        non_operational_candidate_handle,
                        non_operational_candidate_sha256,outcome,
                        reason_code,member_count,committed_at,algorithm_version
                    ) VALUES(
                        :run_id,:commit_id,:decoder_job_id,:attempt_id,
                        :revision_scope_id,:revision,
                        :previous_receipt_sha256,:revision_sha256,:receipt_sha256,
                        :result_sha256,:canonical_run,:canonical_run_bytes,
                        :capture_id,:capture_receipt_sha256,
                        :capture_manifest_sha256,:provider,:product,
                        :decoder_identity,:decoder_version,:config_sha256,
                        :product_time,:received_at,:evaluated_at,:quantity,:unit,
                        :native_crs,:native_grid_sha256,:coverage_sha256,
                        :non_operational_candidate_handle,
                        :non_operational_candidate_sha256,:outcome,
                        :reason_code,:member_count,:committed_at,:algorithm_version
                    )
                    """,
                    {
                        "run_id": run_id,
                        "commit_id": commit_id,
                        "decoder_job_id": parsed.decoder_job_id,
                        "attempt_id": parsed.attempt_id,
                        "revision_scope_id": scope_id,
                        "revision": revision,
                        "previous_receipt_sha256": previous_receipt_sha256,
                        "revision_sha256": revision_sha256,
                        "receipt_sha256": receipt_sha256,
                        "result_sha256": result_sha256,
                        "canonical_run": canonical_run.decode("utf-8"),
                        "canonical_run_bytes": len(canonical_run),
                        "capture_id": capture.capture_id,
                        "capture_receipt_sha256": capture.capture_receipt_sha256,
                        "capture_manifest_sha256": capture.manifest_sha256,
                        "provider": parsed.provider,
                        "product": parsed.product,
                        "decoder_identity": parsed.decoder_identity,
                        "decoder_version": parsed.decoder_version,
                        "config_sha256": parsed.config_sha256,
                        "product_time": parsed.product_time,
                        "received_at": parsed.received_at,
                        "evaluated_at": parsed.evaluated_at,
                        "quantity": parsed.quantity,
                        "unit": parsed.unit,
                        "native_crs": parsed.native_crs,
                        "native_grid_sha256": grid_sha,
                        "coverage_sha256": coverage_sha,
                        "non_operational_candidate_handle": candidate_handle,
                        "non_operational_candidate_sha256": candidate_sha,
                        "outcome": parsed.outcome,
                        "reason_code": parsed.reason_code,
                        "member_count": len(parsed.members),
                        "committed_at": committed_at,
                        "algorithm_version": STORE_ALGORITHM_VERSION,
                    },
                )
                self._fault("commit.after_run")
                database.executemany(
                    """
                    INSERT INTO decoder_run_members(
                        run_id,member_id,role,receipt_id,content_sha256,
                        receipt_sha256,received_at,algorithm_version
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    [
                        (
                            run_id,
                            member.member_id,
                            member.role,
                            member.receipt_id,
                            member.content_sha256,
                            member.receipt_sha256,
                            member.received_at,
                            STORE_ALGORITHM_VERSION,
                        )
                        for member in parsed.members
                    ],
                )
                self._fault("commit.after_members")
                self._fault("commit.before_commit")
                database.commit()
            except CanonicalEvidenceError:
                if database.in_transaction:
                    database.rollback()
                raise
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                self._raise_sqlite(exc)
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._fault("commit.after_commit")
        return DecoderRunReceipt(
            run_id=run_id,
            commit_id=commit_id,
            revision_scope_id=scope_id,
            revision=revision,
            previous_receipt_sha256=previous_receipt_sha256,
            revision_sha256=revision_sha256,
            receipt_sha256=receipt_sha256,
            result_sha256=result_sha256,
            capture_manifest_sha256=capture.manifest_sha256,
            committed_at=committed_at,
            run=parsed,
        )

    def load(self, run_id: str) -> DecoderRunReceipt | None:
        """Load a receipt and reverify both its hash chain and raw capture."""
        if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
            raise ValueError("run_id is invalid")
        with self._lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM decoder_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            receipt = self._validated_receipt(
                row,
                self._member_rows(database, run_id),
            )
            self._validate_previous_link(database, row)
        capture = self._verify_capture(receipt.run)
        self._validate_capture_snapshot(receipt, capture)
        return receipt

    def load_commit(self, commit_id: str) -> DecoderRunReceipt | None:
        """Load by commit identity with the same mandatory raw reverification."""
        if not isinstance(commit_id, str) or not _COMMIT_ID_RE.fullmatch(commit_id):
            raise ValueError("commit_id is invalid")
        with self._lock, closing(self._connect_read_only()) as database:
            row = database.execute(
                "SELECT run_id FROM decoder_runs WHERE commit_id=?",
                (commit_id,),
            ).fetchone()
        return None if row is None else self.load(str(row["run_id"]))

    def stats(self) -> dict[str, int]:
        with self._lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT "
                "(SELECT count(*) FROM decoder_runs),"
                "(SELECT count(*) FROM decoder_run_members)"
            ).fetchone()
        return {"decoder_runs": int(row[0]), "member_links": int(row[1])}

    def audit(self) -> dict[str, Any]:
        """Reverify every local receipt and every referenced raw capture."""
        issues: list[str] = []
        checked = 0
        try:
            with self._lock, closing(self._connect_read_only()) as database:
                self._validate_schema(database, full_integrity=True)
                run_ids = [
                    str(row[0])
                    for row in database.execute(
                        "SELECT run_id FROM decoder_runs ORDER BY run_id"
                    )
                ]
            for run_id in run_ids:
                self.load(run_id)
                checked += 1
        except (
            CanonicalEvidenceError,
            sqlite3.DatabaseError,
            OSError,
            TypeError,
            ValueError,
        ) as exc:
            issues.append(str(exc))
        return {
            "healthy": not issues,
            "checked_runs": checked,
            "issues": issues,
        }

    integrity_check = audit

    def _verify_capture(self, run: DecoderRun) -> _VerifiedCapture:
        try:
            view = self._raw_artifact_reader.load_capture_receipt(run.capture_id)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise CaptureVerificationError(
                "schema-3 raw capture verification failed"
            ) from exc
        if view is None:
            raise CaptureVerificationError("schema-3 raw capture is missing")
        try:
            capture_id = _text(view.capture_id, "raw.capture_id", 72)
            if not _CAPTURE_ID_RE.fullmatch(capture_id):
                raise ValueError("raw capture identity is invalid")
            if view.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION:
                raise ValueError("raw store schema is not schema 3")
            if view.capture_schema != RAW_ARTIFACT_CAPTURE_SCHEMA:
                raise ValueError("raw capture schema is incompatible")
            if (
                view.capture_receipt_schema
                != RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA
            ):
                raise ValueError("raw capture receipt schema is incompatible")
            if view.complete is not True:
                raise ValueError("raw capture is not complete")
            provider = _text(view.provider, "raw.provider", 128)
            product = _text(view.product, "raw.product", 256)
            fetch_id = _text(view.fetch_id, "raw.fetch_id", 512)
            upstream_capture_id = _text(
                view.upstream_capture_id,
                "raw.upstream_capture_id",
                1024,
            )
            product_timestamp = _nullable_timestamp(
                view.product_timestamp,
                "raw.product_timestamp",
            )
            manifest_sha256 = _sha256(
                view.manifest_sha256,
                "raw.manifest_sha256",
            )
            receipt_sha = _sha256(
                view.capture_receipt_sha256,
                "raw.capture_receipt_sha256",
            )
            source_members = list(
                islice(iter(view.members), MAX_CAPTURE_MEMBERS + 1)
            )
            if not 1 <= len(source_members) <= MAX_CAPTURE_MEMBERS:
                raise ValueError("raw capture member count is invalid")
            raw_members = tuple(
                sorted(
                    (
                        DecoderMemberBinding(
                            member_id=_text(
                                member.member_id,
                                "raw.member_id",
                                128,
                            ),
                            role=_text(member.role, "raw.member.role", 128),
                            receipt_id=_text(
                                member.receipt_id,
                                "raw.member.receipt_id",
                                96,
                            ),
                            content_sha256=_sha256(
                                member.content_sha256,
                                "raw.member.content_sha256",
                            ),
                            receipt_sha256=_sha256(
                                member.receipt_sha256,
                                "raw.member.receipt_sha256",
                            ),
                            received_at=_timestamp(
                                member.received_at,
                                "raw.member.received_at",
                            ),
                        )
                        for member in source_members
                    ),
                    key=lambda member: member.member_id,
                )
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise CaptureVerificationError(
                "raw capture receipt view is invalid"
            ) from exc
        if any(member.member_id != member.role for member in raw_members):
            raise CaptureVerificationError(
                "raw schema-3 member identity is not its role"
            )
        if (
            len({member.member_id for member in raw_members}) != len(raw_members)
            or len({member.receipt_id for member in raw_members})
            != len(raw_members)
        ):
            raise CaptureVerificationError("raw capture members are not unique")
        recomputed = self._raw_capture_receipt_sha256(
            capture_id=capture_id,
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            upstream_capture_id=upstream_capture_id,
            product_timestamp=product_timestamp,
            manifest_sha256=manifest_sha256,
            members=raw_members,
        )
        expected_received_at = max(member.received_at for member in raw_members)
        if (
            receipt_sha != recomputed
            or capture_id != run.capture_id
            or receipt_sha != run.capture_receipt_sha256
            or provider != run.provider
            or product != run.product
            or product_timestamp != run.product_time
            or expected_received_at != run.received_at
            or raw_members != run.members
        ):
            raise CaptureVerificationError(
                "decoder run does not exactly match verified raw capture"
            )
        if run.outcome == "decoded_complete" and product_timestamp is None:
            raise CaptureVerificationError(
                "decoded_complete requires an authoritative product time"
            )
        return _VerifiedCapture(
            capture_id=capture_id,
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            upstream_capture_id=upstream_capture_id,
            product_timestamp=product_timestamp,
            manifest_sha256=manifest_sha256,
            capture_receipt_sha256=receipt_sha,
            members=raw_members,
        )

    def _validated_receipt(
        self,
        row: sqlite3.Row,
        member_rows: list[sqlite3.Row],
    ) -> DecoderRunReceipt:
        try:
            canonical_run = str(row["canonical_run"]).encode("utf-8")
            if (
                len(canonical_run)
                != int(row["canonical_run_bytes"])
                or len(canonical_run) > MAX_CANONICAL_OPERATION_BYTES
            ):
                raise ValueError("canonical run byte count is invalid")
            parsed_json = json.loads(canonical_run.decode("utf-8"))
            run = parse_decoder_run(parsed_json)
            if _canonical_json(run.to_mapping()) != canonical_run:
                raise ValueError("canonical run is not canonical JSON")
            result_sha = decoder_result_sha256(run)
            if result_sha != str(row["result_sha256"]):
                raise ValueError("decoder result checksum mismatch")
            run_id = decoder_run_id(run)
            if run_id != str(row["run_id"]):
                raise ValueError("run identity checksum mismatch")
            commit_id = str(row["commit_id"])
            if commit_id != decoder_commit_id(run):
                raise ValueError("preassigned decoder attempt identity mismatch")
            scope_id = self._revision_scope_id(run)
            if scope_id != str(row["revision_scope_id"]):
                raise ValueError("revision scope checksum mismatch")
            revision = int(row["revision"])
            if revision < 1:
                raise ValueError("revision is invalid")
            previous = row["previous_receipt_sha256"]
            previous_sha = None if previous is None else _sha256(
                str(previous),
                "previous_receipt_sha256",
            )
            revision_sha = self._revision_sha256(
                scope_id=scope_id,
                revision=revision,
                previous_receipt_sha256=previous_sha,
                run_id=run_id,
                result_sha256=result_sha,
            )
            if revision_sha != str(row["revision_sha256"]):
                raise ValueError("revision checksum mismatch")
            committed_at = _timestamp(row["committed_at"], "committed_at")
            capture_manifest_sha256 = _sha256(
                row["capture_manifest_sha256"],
                "capture_manifest_sha256",
            )
            receipt_sha = self._receipt_sha256(
                run_id=run_id,
                commit_id=commit_id,
                revision_scope_id=scope_id,
                revision=revision,
                previous_receipt_sha256=previous_sha,
                revision_sha256=revision_sha,
                result_sha256=result_sha,
                capture_receipt_sha256=run.capture_receipt_sha256,
                capture_manifest_sha256=capture_manifest_sha256,
                committed_at=committed_at,
            )
            if receipt_sha != str(row["receipt_sha256"]):
                raise ValueError("decoder receipt checksum mismatch")
            members = tuple(
                DecoderMemberBinding(
                    member_id=str(member["member_id"]),
                    role=str(member["role"]),
                    receipt_id=str(member["receipt_id"]),
                    content_sha256=str(member["content_sha256"]),
                    receipt_sha256=str(member["receipt_sha256"]),
                    received_at=float(member["received_at"]),
                )
                for member in member_rows
            )
            if members != run.members or len(members) != int(row["member_count"]):
                raise ValueError("stored member bindings mismatch decoder run")
            self._validate_materialized_columns(row, run)
        except (
            KeyError,
            TypeError,
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            raise CanonicalEvidenceIntegrityError(
                "stored decoder receipt failed verification"
            ) from exc
        return DecoderRunReceipt(
            run_id=run_id,
            commit_id=commit_id,
            revision_scope_id=scope_id,
            revision=revision,
            previous_receipt_sha256=previous_sha,
            revision_sha256=revision_sha,
            receipt_sha256=receipt_sha,
            result_sha256=result_sha,
            capture_manifest_sha256=capture_manifest_sha256,
            committed_at=committed_at,
            run=run,
        )

    @staticmethod
    def _validate_capture_snapshot(
        receipt: DecoderRunReceipt,
        capture: _VerifiedCapture,
    ) -> None:
        if receipt.capture_manifest_sha256 != capture.manifest_sha256:
            raise CanonicalEvidenceIntegrityError(
                "stored raw capture snapshot no longer matches authority"
            )

    def _validate_materialized_columns(
        self,
        row: sqlite3.Row,
        run: DecoderRun,
    ) -> None:
        grid_sha = self._optional_document_sha(run.native_grid)
        coverage_sha = self._optional_document_sha(run.coverage)
        candidate_handle = (
            None
            if run.non_operational_candidate_artifact is None
            else run.non_operational_candidate_artifact.handle
        )
        candidate_sha = (
            None
            if run.non_operational_candidate_artifact is None
            else run.non_operational_candidate_artifact.sha256
        )
        actual = (
            str(row["decoder_job_id"]),
            str(row["attempt_id"]),
            str(row["capture_id"]),
            str(row["capture_receipt_sha256"]),
            str(row["provider"]),
            str(row["product"]),
            str(row["decoder_identity"]),
            str(row["decoder_version"]),
            str(row["config_sha256"]),
            None if row["product_time"] is None else float(row["product_time"]),
            float(row["received_at"]),
            float(row["evaluated_at"]),
            row["quantity"],
            row["unit"],
            row["native_crs"],
            row["native_grid_sha256"],
            row["coverage_sha256"],
            row["non_operational_candidate_handle"],
            row["non_operational_candidate_sha256"],
            str(row["outcome"]),
            str(row["reason_code"]),
            str(row["algorithm_version"]),
        )
        expected = (
            run.decoder_job_id,
            run.attempt_id,
            run.capture_id,
            run.capture_receipt_sha256,
            run.provider,
            run.product,
            run.decoder_identity,
            run.decoder_version,
            run.config_sha256,
            run.product_time,
            run.received_at,
            run.evaluated_at,
            run.quantity,
            run.unit,
            run.native_crs,
            grid_sha,
            coverage_sha,
            candidate_handle,
            candidate_sha,
            run.outcome,
            run.reason_code,
            STORE_ALGORITHM_VERSION,
        )
        if actual != expected:
            raise ValueError("materialized decoder columns mismatch operation")

    @staticmethod
    def _member_rows(
        database: sqlite3.Connection,
        run_id: str,
    ) -> list[sqlite3.Row]:
        return database.execute(
            "SELECT * FROM decoder_run_members WHERE run_id=? "
            "ORDER BY member_id",
            (run_id,),
        ).fetchall()

    @staticmethod
    def _validate_previous_link(
        database: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> None:
        revision = int(row["revision"])
        previous = row["previous_receipt_sha256"]
        if revision == 1:
            if previous is not None:
                raise CanonicalEvidenceIntegrityError(
                    "first decoder revision has a previous receipt"
                )
            return
        prior = database.execute(
            "SELECT receipt_sha256 FROM decoder_runs "
            "WHERE revision_scope_id=? AND revision=?",
            (str(row["revision_scope_id"]), revision - 1),
        ).fetchone()
        if prior is None or str(prior["receipt_sha256"]) != str(previous):
            raise CanonicalEvidenceIntegrityError(
                "decoder revision history is incomplete or corrupt"
            )

    def _enforce_capacity(
        self,
        database: sqlite3.Connection,
        member_count: int,
    ) -> None:
        run_count = int(
            database.execute("SELECT count(*) FROM decoder_runs").fetchone()[0]
        )
        member_links = int(
            database.execute(
                "SELECT count(*) FROM decoder_run_members"
            ).fetchone()[0]
        )
        if run_count >= MAX_TOTAL_RUNS:
            raise CanonicalEvidenceCapacityError("decoder run bound reached")
        if member_links + member_count > MAX_TOTAL_MEMBER_LINKS:
            raise CanonicalEvidenceCapacityError("member link bound reached")

    def _prepare_path(self) -> None:
        parent = self.path.parent
        if parent.exists() and (parent.is_symlink() or not parent.is_dir()):
            raise CanonicalEvidenceIntegrityError(
                "canonical evidence parent is not a safe directory"
            )
        parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise CanonicalEvidenceIntegrityError(
                "canonical evidence database cannot be a symlink"
            )

    def _connect_unconfigured(self) -> sqlite3.Connection:
        if self.path.is_symlink():
            raise CanonicalEvidenceIntegrityError(
                "canonical evidence database cannot be a symlink"
            )
        database = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
        )
        database.row_factory = sqlite3.Row
        return database

    def _connect(self) -> sqlite3.Connection:
        database = self._connect_unconfigured()
        try:
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA busy_timeout=30000")
            database.execute("PRAGMA synchronous=FULL")
            mode = str(database.execute("PRAGMA journal_mode=WAL").fetchone()[0])
            if mode.lower() != "wal":
                raise CanonicalEvidenceIntegrityError("SQLite WAL is unavailable")
            database.execute("PRAGMA wal_autocheckpoint=256")
            database.execute("PRAGMA journal_size_limit=16777216")
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            max_pages = max(1, self._max_database_bytes // page_size)
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
            if page_count > max_pages:
                raise CanonicalEvidenceCapacityError(
                    "canonical evidence database exceeds byte bound"
                )
            applied = int(
                database.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0]
            )
            if applied > max_pages:
                raise CanonicalEvidenceCapacityError(
                    "SQLite could not enforce database byte bound"
                )
            return database
        except BaseException:
            database.close()
            raise

    def _connect_read_only(self) -> sqlite3.Connection:
        if not self.path.exists() or self.path.is_symlink():
            raise CanonicalEvidenceIntegrityError(
                "canonical evidence database is missing or unsafe"
            )
        database = sqlite3.connect(
            self.path.resolve().as_uri() + "?mode=ro",
            uri=True,
            timeout=30.0,
        )
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        database.execute("PRAGMA foreign_keys=ON")
        return database

    @staticmethod
    def _table_names(database: sqlite3.Connection) -> set[str]:
        return {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }

    def _create_schema(self, database: sqlite3.Connection) -> None:
        """Create every object inside the caller's one explicit transaction."""
        for name, sql in _TABLE_DDL:
            database.execute(sql)
            self._fault(f"initialize.after_ddl.table.{name}")
        for name, _table, sql in _INDEX_DDL:
            database.execute(sql)
            self._fault(f"initialize.after_ddl.index.{name}")
        manifest = (
            ("schema_version", str(STORE_SCHEMA_VERSION)),
            ("contract", EVIDENCE_CONTRACT),
            ("algorithm_version", STORE_ALGORITHM_VERSION),
            ("sqlite_schema_sha256", _EXPECTED_SCHEMA_FINGERPRINT),
        )
        for key, value in manifest:
            database.execute(
                "INSERT INTO manifest(key,value) VALUES(?,?)",
                (key, value),
            )
            self._fault(f"initialize.after_manifest.{key}")
        for name, _table, sql in _TRIGGER_DDL:
            database.execute(sql)
            self._fault(f"initialize.after_ddl.trigger.{name}")

    @staticmethod
    def _schema_objects(
        database: sqlite3.Connection,
    ) -> tuple[tuple[str, str, str, str], ...]:
        rows = database.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE type IN ('table','index','view','trigger') "
            "AND name NOT LIKE 'sqlite_%' ORDER BY type,name"
        ).fetchall()
        return tuple(sorted(
            (
                str(row["type"]),
                str(row["name"]),
                str(row["tbl_name"]),
                "" if row["sql"] is None else _normalize_schema_sql(str(row["sql"])),
            )
            for row in rows
        ))

    def _validate_schema(
        self,
        database: sqlite3.Connection,
        *,
        full_integrity: bool = False,
    ) -> None:
        schema_objects = self._schema_objects(database)
        schema_fingerprint = hashlib.sha256(
            _canonical_json(schema_objects)
        ).hexdigest()
        if (
            schema_objects != _EXPECTED_SCHEMA_OBJECTS
            or schema_fingerprint != _EXPECTED_SCHEMA_FINGERPRINT
        ):
            raise CanonicalEvidenceIntegrityError(
                "canonical evidence sqlite_schema fingerprint is incompatible"
            )
        manifest = {
            str(row[0]): str(row[1])
            for row in database.execute("SELECT key,value FROM manifest")
        }
        expected_manifest = {
            "schema_version": str(STORE_SCHEMA_VERSION),
            "contract": EVIDENCE_CONTRACT,
            "algorithm_version": STORE_ALGORITHM_VERSION,
            "sqlite_schema_sha256": _EXPECTED_SCHEMA_FINGERPRINT,
        }
        if manifest != expected_manifest:
            raise CanonicalEvidenceIntegrityError(
                "canonical evidence manifest is incompatible"
            )
        if full_integrity:
            integrity = [
                str(row[0])
                for row in database.execute("PRAGMA integrity_check")
            ]
            if integrity != ["ok"]:
                raise CanonicalEvidenceIntegrityError(
                    "canonical evidence catalog fails integrity_check"
                )
        if full_integrity:
            if database.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise CanonicalEvidenceIntegrityError(
                    "canonical evidence catalog fails foreign keys"
                )

    @staticmethod
    def _optional_document_sha(
        value: NativeGridSpec | CoverageSpec | None,
    ) -> str | None:
        if value is None:
            return None
        return hashlib.sha256(_canonical_json(value.to_mapping())).hexdigest()

    @staticmethod
    def _revision_scope_id(run: DecoderRun) -> str:
        document = {
            "capture_id": run.capture_id,
            "capture_receipt_sha256": run.capture_receipt_sha256,
            "config_sha256": run.config_sha256,
            "decoder_identity": run.decoder_identity,
            "decoder_version": run.decoder_version,
            "schema": REVISION_SCOPE_SCHEMA,
        }
        digest = hashlib.sha256(_canonical_json(document)).hexdigest()
        return f"scope-{digest}"

    @staticmethod
    def _revision_sha256(
        *,
        scope_id: str,
        revision: int,
        previous_receipt_sha256: str | None,
        run_id: str,
        result_sha256: str,
    ) -> str:
        if not _SCOPE_ID_RE.fullmatch(scope_id):
            raise ValueError("revision scope identity is invalid")
        document = {
            "result_sha256": result_sha256,
            "previous_receipt_sha256": previous_receipt_sha256,
            "revision": revision,
            "revision_scope_id": scope_id,
            "run_id": run_id,
            "schema": EVIDENCE_REVISION_SCHEMA,
        }
        return hashlib.sha256(_canonical_json(document)).hexdigest()

    @staticmethod
    def _receipt_sha256(
        *,
        run_id: str,
        commit_id: str,
        revision_scope_id: str,
        revision: int,
        previous_receipt_sha256: str | None,
        revision_sha256: str,
        result_sha256: str,
        capture_receipt_sha256: str,
        capture_manifest_sha256: str,
        committed_at: float,
    ) -> str:
        document = {
            "capture_receipt_sha256": capture_receipt_sha256,
            "capture_manifest_sha256": capture_manifest_sha256,
            "commit_id": commit_id,
            "committed_at": committed_at,
            "result_sha256": result_sha256,
            "previous_receipt_sha256": previous_receipt_sha256,
            "revision": revision,
            "revision_scope_id": revision_scope_id,
            "revision_sha256": revision_sha256,
            "run_id": run_id,
            "schema": EVIDENCE_RECEIPT_SCHEMA,
        }
        return hashlib.sha256(_canonical_json(document)).hexdigest()

    @staticmethod
    def _raw_capture_receipt_sha256(
        *,
        capture_id: str,
        provider: str,
        product: str,
        fetch_id: str,
        upstream_capture_id: str,
        product_timestamp: float | None,
        manifest_sha256: str,
        members: tuple[DecoderMemberBinding, ...],
    ) -> str:
        document = {
            "capture_id": capture_id,
            "capture_schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
            "complete": True,
            "fetch_id": fetch_id,
            "manifest_sha256": manifest_sha256,
            "members": [member.to_mapping() for member in members],
            "product": product,
            "product_timestamp": product_timestamp,
            "provider": provider,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "schema": RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
            "upstream_capture_id": upstream_capture_id,
        }
        return hashlib.sha256(_canonical_json(document)).hexdigest()

    @staticmethod
    def _raise_sqlite(exc: sqlite3.DatabaseError) -> None:
        if "full" in str(exc).lower():
            raise CanonicalEvidenceCapacityError(
                "canonical evidence database capacity was exhausted"
            ) from exc
        raise CanonicalEvidenceIntegrityError(
            "canonical evidence SQLite operation failed closed"
        ) from exc

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)


__all__ = [
    "COMMIT_IDENTITY_SCHEMA",
    "CanonicalEvidenceCapacityError",
    "CanonicalEvidenceConflict",
    "CanonicalEvidenceError",
    "CanonicalEvidenceIntegrityError",
    "CanonicalEvidenceStore",
    "CaptureVerificationError",
    "CoverageSpec",
    "DEFAULT_MAX_DATABASE_BYTES",
    "DecoderMemberBinding",
    "DecoderOutcome",
    "DecoderRun",
    "DecoderRunReceipt",
    "EVIDENCE_CONTRACT",
    "EVIDENCE_RECEIPT_SCHEMA",
    "EVIDENCE_REVISION_SCHEMA",
    "MAX_CANONICAL_OPERATION_BYTES",
    "MAX_ABSOLUTE_NATIVE_COORDINATE",
    "MAX_CAPTURE_MEMBERS",
    "MAX_TIMESTAMP_SECONDS",
    "MAX_TOTAL_MEMBER_LINKS",
    "MAX_TOTAL_RUNS",
    "NativeGridSpec",
    "NonOperationalCandidateArtifactRef",
    "RUN_IDENTITY_SCHEMA",
    "REVISION_SCOPE_SCHEMA",
    "RESULT_IDENTITY_SCHEMA",
    "RawArtifactCaptureReader",
    "RawCaptureMemberReceiptView",
    "RawCaptureReceiptView",
    "STORE_ALGORITHM_VERSION",
    "STORE_SCHEMA_VERSION",
    "decoder_commit_id",
    "decoder_result_sha256",
    "decoder_run_id",
    "parse_decoder_run",
]
