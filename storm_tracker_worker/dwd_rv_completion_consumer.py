"""Crash-durable, unwired DWD RV completion consumer.

This module is the concrete consumer for completion events published under
``dwd-rv-quantitative-decoder-v1`` by :mod:`acquisition_evidence_lifecycle`.
It deliberately reuses that store as its only queue.  A lease is translated
through the reviewed DWD quantitative decoder, canonical evidence bridge,
official DWD grid certificate and lossless cells-shadow compositor.  Only a
bounded, payload-free output receipt is persisted; the result remains an
unadmitted shadow candidate.

The output receipt is committed before any lifecycle acknowledgement.  The
coordinator-facing :meth:`DwdRvCompletionConsumer.process_leased` never claims
or acknowledges; it returns a hash-only outcome after the durable receipt can
be reproved.  ``run_once`` is only a standalone composition harness.  A crash
in the output-to-ack window therefore replays without a network fetch or a
second decode after the lease expires.  This module is not imported by
``service.py`` and has no MQTT, Home Assistant, snapshot,
science-orchestrator or admission wiring.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import threading
import time
from typing import Any, Callable, Iterator, Mapping
from urllib.parse import quote

from .acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    CompletionLease,
    ConsumptionReceipt,
    DESIRED_AUTHORITY_SCHEMA,
    DWD_COMPLETION_PROOF_SCHEMA,
    desired_authority_proof,
)
from .analysis_grid_transform import (
    AnalysisGridTransformConfig,
    ExactAffineGridTransformAdapter,
    NativeFieldCandidateBinding,
    analysis_grid_sha256,
    native_candidate_content_sha256,
)
from .canonical_evidence_store import CanonicalEvidenceStore
from .canonical_radar_field_bridge import (
    ProviderDerivedArtifactAuthority,
    RadarFieldBridgeDeadlineError,
    default_radar_semantics_certificates,
    derive_canonical_radar_field,
)
from .dwd_rv_analysis_grid_certificate import (
    issue_dwd_rv_native_grid_certificate,
)
from .dwd_rv_capture_handoff import (
    HANDOFF_ADAPTER_ID,
    HANDOFF_ADAPTER_VERSION,
    HANDOFF_CONFIG_SHA256,
    TARGET_ARCHIVE_PRODUCT,
    TARGET_CAPTURE_PRODUCT,
    TARGET_PROVIDER,
)
from .dwd_rv_quantitative import (
    DwdRvDecodeDeadlineError,
    DwdRvDerivedArtifactStore,
    decode_dwd_rv_capture,
)
from .desired_state import DesiredState
from .evidence_to_cells_shadow import (
    EVIDENCE_TO_CELLS_ADMISSION_STATUS,
    EvidenceToCellsShadowDeadlineError,
    EvidenceToCellsShadowConfig,
    compose_evidence_to_cells_shadow,
)
from .raw_artifacts import RawArtifactReceipt, RawArtifactStore
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.hierarchical_detection import (
    HIERARCHICAL_DETECTOR_VERSION,
    HierarchicalDetectorConfig,
)
from .worker_first_runtime import PipelineOutcome, VerifiedPipelineOutcome


CONSUMER_ID = "dwd-rv-quantitative-decoder-v1"
STREAM_ID = "radar.dwd.radolan_rv"
WORKLOAD_ID = "radar.dwd.radolan_rv"
WORKLOAD_PROVIDER = "dwd"
WORKLOAD_PRODUCT = "dwd_radolan_rv_native_tar"
RESULT_STORE_CONTRACT = "stv3-dwd-rv-completion-output-store-v3"
LEGACY_RESULT_STORE_CONTRACT = "stv3-dwd-rv-completion-output-store-v2"
OUTPUT_DOCUMENT_SCHEMA = "stv3-dwd-rv-completion-output-v2"
OUTPUT_RECEIPT_SCHEMA = "stv3-dwd-rv-completion-output-receipt-v2"
PROCESS_RECEIPT_SCHEMA = "stv3-dwd-rv-hard-process-receipt-v1"
OUTPUT_DISPOSITION = "withheld"
SCHEMA_VERSION = 3
LEGACY_SCHEMA_VERSION = 2
OUTPUT_LIMITS_SCHEMA = "stv3-dwd-rv-completion-output-limits-v3"
LEGACY_OUTPUT_LIMITS_SCHEMA = "stv3-dwd-rv-completion-output-limits-v2"
STORE_IDENTITY_SCHEMA = "stv3-dwd-rv-completion-output-store-identity-v1"
DEFAULT_LEASE_SECONDS = 120.0
MAX_LEASE_SECONDS = 3_600.0
MAX_DIAGNOSTIC_BYTES = 4_096

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_EVENT_ID = re.compile(r"^completion-[0-9a-f]{64}$")
_RUN_ID = re.compile(r"^run-[0-9a-f]{64}$")
_FORBIDDEN_OUTPUT_KEY = re.compile(
    r"(?:artifact_path|password|passwd|api[_-]?key|access[_-]?token|"
    r"refresh[_-]?token|authorization|cookie|private[_-]?key)",
    re.IGNORECASE,
)


class DwdRvCompletionConsumerError(RuntimeError):
    """Base error for the closed completion consumer."""


class DwdRvCompletionIntegrityError(DwdRvCompletionConsumerError):
    """An immutable input or output authority changed."""


class DwdRvCompletionConflict(DwdRvCompletionConsumerError):
    """A stable completion identity was reused for different facts."""


class DwdRvCompletionCapacityError(DwdRvCompletionConsumerError):
    """A caller or durable-store capacity limit was reached."""


class DwdRvCompletionDeadlineError(DwdRvCompletionConsumerError):
    """The single consumer deadline expired before durable completion."""


class DwdRvCompletionCancelled(DwdRvCompletionConsumerError):
    """The owning worker lane requested cooperative cancellation."""


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
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise DwdRvCompletionIntegrityError(f"{label} is not SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise DwdRvCompletionIntegrityError(f"{label} is invalid")
    return value


def _timestamp(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0.0
    ):
        raise DwdRvCompletionIntegrityError(f"{label} is invalid")
    return float(value)


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) for key in value
    ):
        raise DwdRvCompletionIntegrityError(f"{label} is not an object")
    return value


def _exact_mapping(
    value: object,
    keys: frozenset[str],
    label: str,
) -> Mapping[str, Any]:
    result = _mapping(value, label)
    if frozenset(result) != keys:
        raise DwdRvCompletionIntegrityError(f"{label} schema differs")
    return result


def _assert_output_safe(value: Any, *, depth: int = 0) -> None:
    if depth > 24:
        raise DwdRvCompletionCapacityError("output receipt nesting exceeds bound")
    if isinstance(value, Mapping):
        if len(value) > 256:
            raise DwdRvCompletionCapacityError("output receipt object exceeds bound")
        for key, item in value.items():
            if not isinstance(key, str) or _FORBIDDEN_OUTPUT_KEY.search(key):
                raise DwdRvCompletionIntegrityError(
                    "credential or local-path key is forbidden in output receipt"
                )
            _assert_output_safe(item, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if len(value) > 4_096:
            raise DwdRvCompletionCapacityError("output receipt sequence exceeds bound")
        for item in value:
            _assert_output_safe(item, depth=depth + 1)
        return
    if isinstance(value, str):
        if len(value.encode("utf-8")) > 64 * 1024:
            raise DwdRvCompletionCapacityError("output receipt string exceeds bound")
        return
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise DwdRvCompletionIntegrityError("output receipt has non-finite number")
        return
    raise DwdRvCompletionIntegrityError("output receipt has unsupported value")


def _detector_mapping(config: HierarchicalDetectorConfig) -> dict[str, Any]:
    return {
        "centroid_method": config.centroid_method,
        "connectivity": config.connectivity,
        "core_position_sigma_m": config.core_position_sigma_m,
        "core_threshold": config.core_threshold,
        "detector_version": HIERARCHICAL_DETECTOR_VERSION,
        "minimum_core_area_m2": config.minimum_core_area_m2,
        "minimum_shield_area_m2": config.minimum_shield_area_m2,
        "shield_position_sigma_m": config.shield_position_sigma_m,
        "shield_threshold": config.shield_threshold,
    }


def _grid_mapping(grid: AnalysisGrid) -> dict[str, Any]:
    return {
        "axis_order": grid.axis_order,
        "contract_version": grid.contract_version,
        "coordinate_unit": grid.coordinate_unit,
        "crs": grid.crs,
        "grid_id": grid.grid_id,
        "height": grid.height,
        "origin_x_m": grid.origin_x_m,
        "origin_y_m": grid.origin_y_m,
        "row_order": grid.row_order,
        "width": grid.width,
        "x_resolution_m": grid.x_resolution_m,
        "y_resolution_m": grid.y_resolution_m,
    }


@dataclass(frozen=True, slots=True)
class DwdRvShadowChainProfile:
    """Explicit, content-bound geometry/detector configuration."""

    target_grid: AnalysisGrid
    transform_config: AnalysisGridTransformConfig
    detector_config: HierarchicalDetectorConfig
    pipeline_config: EvidenceToCellsShadowConfig

    def __post_init__(self) -> None:
        if not isinstance(self.target_grid, AnalysisGrid):
            raise TypeError("target_grid must be AnalysisGrid")
        if not isinstance(self.transform_config, AnalysisGridTransformConfig):
            raise TypeError("transform_config must be AnalysisGridTransformConfig")
        if not isinstance(self.detector_config, HierarchicalDetectorConfig):
            raise TypeError("detector_config must be HierarchicalDetectorConfig")
        if not isinstance(self.pipeline_config, EvidenceToCellsShadowConfig):
            raise TypeError("pipeline_config must be EvidenceToCellsShadowConfig")
        pixels = self.target_grid.width * self.target_grid.height
        if (
            pixels > self.transform_config.max_target_pixels
            or pixels > self.pipeline_config.max_target_pixels
            or pixels * 2 > self.pipeline_config.max_detection_objects
        ):
            raise DwdRvCompletionCapacityError(
                "target grid exceeds transform or lossless detection capacity"
            )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "detector": _detector_mapping(self.detector_config),
            "pipeline": self.pipeline_config.to_mapping(),
            "schema": "stv3-dwd-rv-shadow-chain-profile-v1",
            "target_grid": _grid_mapping(self.target_grid),
            "target_grid_sha256": analysis_grid_sha256(self.target_grid),
            "transform": self.transform_config.to_mapping(),
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.to_mapping())


@dataclass(frozen=True, slots=True)
class DwdRvCompletionOutputLimits:
    max_receipts: int = 4_096
    max_receipt_bytes: int = 128 * 1024
    max_database_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        for label, value, minimum, maximum in (
            ("max_receipts", self.max_receipts, 1, 1_000_000),
            ("max_receipt_bytes", self.max_receipt_bytes, 1_024, 1024 * 1024),
            (
                "max_database_bytes",
                self.max_database_bytes,
                64 * 1024,
                4 * 1024 * 1024 * 1024,
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its hard boundary")

    def to_mapping(self, *, schema: str = OUTPUT_LIMITS_SCHEMA) -> dict[str, Any]:
        if schema not in {OUTPUT_LIMITS_SCHEMA, LEGACY_OUTPUT_LIMITS_SCHEMA}:
            raise ValueError("output limits schema is unsupported")
        return {
            "max_database_bytes": self.max_database_bytes,
            "max_receipt_bytes": self.max_receipt_bytes,
            "max_receipts": self.max_receipts,
            "schema": schema,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.to_mapping())


@dataclass(frozen=True, slots=True)
class DwdRvCompletionOutputReceipt:
    receipt_sha256: str
    output_sha256: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    desired_state_sha256: str
    authority_sha256: str
    consumer_id: str
    stream_id: str
    disposition: str
    fact_revision_sha256: str | None
    isolation_authority_sha256: str
    isolation_effect_contract_sha256: str
    isolation_limits_sha256: str
    process_receipt_sha256: str
    profile_sha256: str
    candidate_content_sha256: str
    decoder_run_id: str
    created_at: float
    output: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_content_sha256": self.candidate_content_sha256,
            "created_at": self.created_at,
            "decoder_run_id": self.decoder_run_id,
            "desired_state_sha256": self.desired_state_sha256,
            "authority_sha256": self.authority_sha256,
            "consumer_id": self.consumer_id,
            "stream_id": self.stream_id,
            "disposition": self.disposition,
            "fact_revision_sha256": self.fact_revision_sha256,
            "isolation_authority_sha256": self.isolation_authority_sha256,
            "isolation_effect_contract_sha256": (
                self.isolation_effect_contract_sha256
            ),
            "isolation_limits_sha256": self.isolation_limits_sha256,
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "output": json.loads(json.dumps(self.output)),
            "output_sha256": self.output_sha256,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "process_receipt_sha256": self.process_receipt_sha256,
            "profile_sha256": self.profile_sha256,
            "receipt_sha256": self.receipt_sha256,
            "schema": OUTPUT_RECEIPT_SCHEMA,
        }


@dataclass(frozen=True, slots=True)
class DwdRvCompletionRunResult:
    disposition: str
    event_id: str | None
    output_receipt: DwdRvCompletionOutputReceipt | None
    consumption_receipt: ConsumptionReceipt | None


@dataclass(frozen=True, slots=True)
class DwdRvCompletionVerification:
    """Exact durable authority returned before a coordinator may acknowledge."""

    event_id: str
    event_sha256: str
    desired_state_sha256: str
    authority_sha256: str
    consumer_id: str
    stream_id: str
    output_sha256: str
    durable_output_receipt_sha256: str
    durable_process_receipt_sha256: str
    isolation_authority_sha256: str
    disposition: str
    fact_revision_sha256: str | None


_OUTPUT_KEYS = frozenset({
    "authority", "claims", "consumer_id", "decoder", "disposition", "event",
    "fact_revision", "handoff", "input", "process_isolation", "profile",
    "schema", "shadow", "stream_id",
})
_CLAIM_KEYS = frozenset({
    "forecast_generated", "hard_realtime_deadline_certified",
    "meteorological_admission_granted", "raw_refetch_performed",
    "wet_dry_classification_applied",
})


def _validated_output_document(
    value: Mapping[str, Any],
) -> tuple[
    dict[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[str, Any],
    Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], str | None,
]:
    document = dict(value)
    _assert_output_safe(document)
    exact = _exact_mapping(document, _OUTPUT_KEYS, "output document")
    authority = _exact_mapping(
        exact["authority"],
        frozenset({"authority_sha256", "desired_state_sha256"}),
        "output authority",
    )
    claims = _exact_mapping(exact["claims"], _CLAIM_KEYS, "output claims")
    event = _mapping(exact["event"], "output event")
    profile = _mapping(exact["profile"], "output profile")
    decoder = _mapping(exact["decoder"], "output decoder")
    shadow = _mapping(exact["shadow"], "output shadow")
    isolation = _exact_mapping(
        exact["process_isolation"],
        frozenset({
            "authority_sha256",
            "effect_contract_sha256",
            "limits_sha256",
            "state",
        }),
        "output process isolation",
    )
    if (
        exact["schema"] != OUTPUT_DOCUMENT_SCHEMA
        or exact["consumer_id"] != CONSUMER_ID
        or exact["stream_id"] != STREAM_ID
        or exact["disposition"] != OUTPUT_DISPOSITION
        or set(claims.values()) != {False}
        or shadow.get("admission_status") != EVIDENCE_TO_CELLS_ADMISSION_STATUS
        or isolation.get("state") != "enforced_before_target_start"
    ):
        raise DwdRvCompletionIntegrityError(
            "output route, disposition or claim contract differs"
        )
    desired_sha = _sha(
        authority.get("desired_state_sha256"), "output desired-state SHA-256"
    )
    authority_sha = _sha(
        authority.get("authority_sha256"), "output authority SHA-256"
    )
    _sha(
        isolation.get("authority_sha256"),
        "output process-isolation authority SHA-256",
    )
    _sha(
        isolation.get("effect_contract_sha256"),
        "output process-isolation effect-contract SHA-256",
    )
    _sha(
        isolation.get("limits_sha256"),
        "output process-isolation limits SHA-256",
    )
    fact = exact["fact_revision"]
    if fact is None:
        fact_sha = None
    else:
        fact_mapping = _mapping(fact, "staged fact revision")
        if not fact_mapping:
            raise DwdRvCompletionIntegrityError(
                "staged fact revision cannot be empty"
            )
        fact_sha = _digest(fact_mapping)
    return (
        document, authority, event, profile, decoder, shadow, isolation,
        fact_sha,
    )


class DwdRvCompletionOutputStore:
    """Bounded receipt authority; it is not a dispatch queue."""

    def __init__(
        self,
        path: str | Path,
        *,
        limits: DwdRvCompletionOutputLimits | None = None,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(os.path.abspath(os.fspath(path)))
        self._limits = limits or DwdRvCompletionOutputLimits()
        self._clock = clock
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._database_witness: tuple[int, int, int]
        self._store_instance_sha256: str
        self.initialize()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    @staticmethod
    def _normalized_path(path: Path) -> str:
        return os.path.normcase(os.path.normpath(str(path)))

    @staticmethod
    def _path_witness(path: Path) -> tuple[int, int, int]:
        try:
            details = path.lstat()
        except OSError as exc:
            raise DwdRvCompletionIntegrityError(
                "output receipt store witness is unavailable"
            ) from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or int(details.st_nlink) != 1
        ):
            raise DwdRvCompletionIntegrityError(
                "output receipt store path is unsafe"
            )
        return (
            int(details.st_dev),
            int(details.st_ino),
            int(details.st_mode),
        )

    def _prepare_path(self) -> tuple[int, int, int]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        parent = self.path.parent
        for candidate in (parent, *parent.parents):
            try:
                details = candidate.lstat()
            except OSError as exc:
                raise DwdRvCompletionIntegrityError(
                    "output receipt store parent is unavailable"
                ) from exc
            if candidate.is_symlink() or not stat.S_ISDIR(details.st_mode):
                raise DwdRvCompletionIntegrityError(
                    "output receipt store parent is unsafe"
                )
        resolved_parent = parent.resolve(strict=True)
        if self._normalized_path(resolved_parent) != self._normalized_path(parent):
            raise DwdRvCompletionIntegrityError(
                "output receipt store parent is aliased"
            )
        if self.path.is_symlink():
            raise DwdRvCompletionIntegrityError(
                "output receipt store path is unsafe"
            )
        if not self.path.exists():
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(self.path, flags, 0o600)
                try:
                    os.set_inheritable(descriptor, False)
                    details = os.fstat(descriptor)
                    if not stat.S_ISREG(details.st_mode):
                        raise DwdRvCompletionIntegrityError(
                            "output receipt store leaf is not regular"
                        )
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except DwdRvCompletionConsumerError:
                raise
            except OSError as exc:
                raise DwdRvCompletionIntegrityError(
                    "output receipt store leaf creation failed"
                ) from exc
        witness = self._path_witness(self.path)
        resolved = self.path.resolve(strict=True)
        if self._normalized_path(resolved) != self._normalized_path(self.path):
            raise DwdRvCompletionIntegrityError(
                "output receipt store path is aliased"
            )
        return witness

    def _verify_path(self) -> None:
        if (
            self._normalized_path(self.path.resolve(strict=True))
            != self._normalized_path(self.path)
            or self._path_witness(self.path) != self._database_witness
        ):
            raise DwdRvCompletionIntegrityError(
                "output receipt store instance changed"
            )

    def _connect(self) -> sqlite3.Connection:
        self._verify_path()
        uri_path = quote(self.path.as_posix(), safe="/:")
        database = sqlite3.connect(
            f"file:{uri_path}?mode=rw&nofollow=1",
            timeout=5.0,
            isolation_level=None,
            uri=True,
        )
        database.row_factory = sqlite3.Row
        try:
            self._verify_path()
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA journal_mode=DELETE")
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA trusted_schema=OFF")
            database.execute("PRAGMA busy_timeout=5000")
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
            max_pages = max(1, self._limits.max_database_bytes // page_size)
            enforced = int(database.execute(
                f"PRAGMA max_page_count={max_pages}"
            ).fetchone()[0])
            if page_count > max_pages or enforced > max_pages:
                raise DwdRvCompletionCapacityError(
                    "output receipt database already exceeds its page cap"
                )
        except BaseException:
            database.close()
            raise
        return database

    @staticmethod
    def _tables(database: sqlite3.Connection) -> set[str]:
        return {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            if not str(row[0]).startswith("sqlite_")
        }

    @staticmethod
    def _columns(database: sqlite3.Connection, table: str) -> set[str]:
        return {
            str(row[1]) for row in database.execute(f"PRAGMA table_info({table})")
        }

    @staticmethod
    def _normalized_sql(value: str) -> str:
        return " ".join(value.split()).casefold()

    @classmethod
    def _trigger_definitions(cls) -> dict[str, str]:
        return {
            "completion_outputs_no_update": (
                "CREATE TRIGGER completion_outputs_no_update BEFORE UPDATE ON "
                "completion_outputs BEGIN SELECT RAISE(ABORT, "
                "'completion output rows are immutable'); END"
            ),
            "completion_outputs_no_delete": (
                "CREATE TRIGGER completion_outputs_no_delete BEFORE DELETE ON "
                "completion_outputs BEGIN SELECT RAISE(ABORT, "
                "'completion output rows are immutable'); END"
            ),
            "store_metadata_no_update": (
                "CREATE TRIGGER store_metadata_no_update BEFORE UPDATE ON "
                "store_metadata BEGIN SELECT RAISE(ABORT, "
                "'completion output metadata is immutable'); END"
            ),
            "store_metadata_no_delete": (
                "CREATE TRIGGER store_metadata_no_delete BEFORE DELETE ON "
                "store_metadata BEGIN SELECT RAISE(ABORT, "
                "'completion output metadata is immutable'); END"
            ),
        }

    @classmethod
    def _install_immutability_triggers(
        cls, database: sqlite3.Connection
    ) -> None:
        for statement in cls._trigger_definitions().values():
            database.execute(statement)

    @classmethod
    def _validate_immutability_triggers(
        cls, database: sqlite3.Connection
    ) -> None:
        rows = database.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
        actual = {str(row[0]): str(row[1]) for row in rows}
        expected = cls._trigger_definitions()
        if set(actual) != set(expected) or any(
            cls._normalized_sql(actual[name])
            != cls._normalized_sql(statement)
            for name, statement in expected.items()
        ):
            raise DwdRvCompletionIntegrityError(
                "output store immutability triggers differ"
            )

    @staticmethod
    def _output_columns() -> set[str]:
        return {
            "event_id", "event_sha256", "producer_receipt_sha256",
            "desired_state_sha256", "authority_sha256", "consumer_id",
            "stream_id", "disposition", "fact_revision_sha256",
            "isolation_authority_sha256",
            "isolation_effect_contract_sha256", "isolation_limits_sha256",
            "profile_sha256", "candidate_content_sha256", "decoder_run_id",
            "output_sha256", "receipt_sha256", "process_receipt_sha256",
            "output_json", "output_bytes", "created_at",
        }

    def _create_schema(self, database: sqlite3.Connection) -> None:
        database.execute(
            """CREATE TABLE store_metadata(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                contract TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                max_receipts INTEGER NOT NULL,
                max_receipt_bytes INTEGER NOT NULL,
                max_database_bytes INTEGER NOT NULL,
                limits_sha256 TEXT NOT NULL,
                store_instance_sha256 TEXT NOT NULL
            )"""
        )
        database.execute(
            """CREATE TABLE completion_outputs(
                event_id TEXT PRIMARY KEY,
                event_sha256 TEXT NOT NULL,
                producer_receipt_sha256 TEXT NOT NULL,
                desired_state_sha256 TEXT NOT NULL,
                authority_sha256 TEXT NOT NULL,
                consumer_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                disposition TEXT NOT NULL,
                fact_revision_sha256 TEXT,
                isolation_authority_sha256 TEXT NOT NULL,
                isolation_effect_contract_sha256 TEXT NOT NULL,
                isolation_limits_sha256 TEXT NOT NULL,
                profile_sha256 TEXT NOT NULL,
                candidate_content_sha256 TEXT NOT NULL,
                decoder_run_id TEXT NOT NULL,
                output_sha256 TEXT NOT NULL UNIQUE,
                receipt_sha256 TEXT NOT NULL UNIQUE,
                process_receipt_sha256 TEXT NOT NULL UNIQUE,
                output_json BLOB NOT NULL,
                output_bytes INTEGER NOT NULL CHECK(output_bytes > 0),
                created_at REAL NOT NULL CHECK(created_at >= 0)
            )"""
        )
        instance_sha = secrets.token_hex(32)
        database.execute(
            "INSERT INTO store_metadata VALUES(1,?,?,?,?,?,?,?)",
            (
                RESULT_STORE_CONTRACT,
                SCHEMA_VERSION,
                self._limits.max_receipts,
                self._limits.max_receipt_bytes,
                self._limits.max_database_bytes,
                self._limits.fingerprint,
                instance_sha,
            ),
        )
        self._install_immutability_triggers(database)

    def _validate_legacy_v2_schema(
        self, database: sqlite3.Connection
    ) -> None:
        if self._tables(database) != {"store_metadata", "completion_outputs"}:
            raise DwdRvCompletionIntegrityError("legacy output schema differs")
        if self._columns(database, "store_metadata") != {
            "singleton", "contract", "schema_version", "max_receipts",
            "max_receipt_bytes", "max_database_bytes", "limits_sha256",
        }:
            raise DwdRvCompletionIntegrityError(
                "legacy output metadata columns differ"
            )
        if self._columns(database, "completion_outputs") != self._output_columns():
            raise DwdRvCompletionIntegrityError(
                "legacy output table columns differ"
            )
        if database.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='trigger'"
        ).fetchone()[0] != 0:
            raise DwdRvCompletionIntegrityError(
                "legacy output store has unexpected triggers"
            )
        metadata = database.execute(
            "SELECT * FROM store_metadata WHERE singleton=1"
        ).fetchone()
        if (
            metadata is None
            or database.execute("SELECT count(*) FROM store_metadata").fetchone()[0]
            != 1
            or str(metadata["contract"]) != LEGACY_RESULT_STORE_CONTRACT
            or int(metadata["schema_version"]) != LEGACY_SCHEMA_VERSION
            or int(metadata["max_receipts"]) != self._limits.max_receipts
            or int(metadata["max_receipt_bytes"])
            != self._limits.max_receipt_bytes
            or int(metadata["max_database_bytes"])
            != self._limits.max_database_bytes
            or str(metadata["limits_sha256"])
            != _digest(self._limits.to_mapping(schema=LEGACY_OUTPUT_LIMITS_SCHEMA))
        ):
            raise DwdRvCompletionIntegrityError(
                "legacy output store metadata differs"
            )
        count = int(database.execute(
            "SELECT count(*) FROM completion_outputs"
        ).fetchone()[0])
        if count > self._limits.max_receipts:
            raise DwdRvCompletionCapacityError(
                "legacy output store exceeds receipt cap"
            )
        if database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise DwdRvCompletionIntegrityError(
                "legacy output store integrity check failed"
            )
        for row in database.execute("SELECT * FROM completion_outputs"):
            self._from_row(row)

    def _migrate_v2_to_v3(self, database: sqlite3.Connection) -> None:
        self._validate_legacy_v2_schema(database)
        instance_sha = secrets.token_hex(32)
        database.execute(
            "ALTER TABLE store_metadata ADD COLUMN store_instance_sha256 TEXT"
        )
        database.execute(
            "UPDATE store_metadata SET contract=?,schema_version=?,"
            "limits_sha256=?,store_instance_sha256=? WHERE singleton=1",
            (
                RESULT_STORE_CONTRACT,
                SCHEMA_VERSION,
                self._limits.fingerprint,
                instance_sha,
            ),
        )
        self._fault("migration.after_metadata")
        self._install_immutability_triggers(database)
        self._fault("migration.after_triggers")

    def _is_legacy_v2(self, database: sqlite3.Connection) -> bool:
        if self._tables(database) != {"store_metadata", "completion_outputs"}:
            return False
        if "store_instance_sha256" in self._columns(database, "store_metadata"):
            return False
        row = database.execute(
            "SELECT contract,schema_version FROM store_metadata WHERE singleton=1"
        ).fetchone()
        return bool(
            row is not None
            and str(row["contract"]) == LEGACY_RESULT_STORE_CONTRACT
            and int(row["schema_version"]) == LEGACY_SCHEMA_VERSION
        )

    def _validate_schema(
        self,
        database: sqlite3.Connection,
        *,
        full: bool = False,
    ) -> str:
        if self._tables(database) != {"store_metadata", "completion_outputs"}:
            raise DwdRvCompletionIntegrityError("output store schema differs")
        if self._columns(database, "store_metadata") != {
            "singleton", "contract", "schema_version", "max_receipts",
            "max_receipt_bytes", "max_database_bytes", "limits_sha256",
            "store_instance_sha256",
        }:
            raise DwdRvCompletionIntegrityError(
                "output store metadata columns differ"
            )
        metadata = database.execute(
            "SELECT * FROM store_metadata WHERE singleton=1"
        ).fetchone()
        if (
            metadata is None
            or database.execute("SELECT count(*) FROM store_metadata").fetchone()[0]
            != 1
            or str(metadata["contract"]) != RESULT_STORE_CONTRACT
            or int(metadata["schema_version"]) != SCHEMA_VERSION
            or int(metadata["max_receipts"]) != self._limits.max_receipts
            or int(metadata["max_receipt_bytes"])
            != self._limits.max_receipt_bytes
            or int(metadata["max_database_bytes"])
            != self._limits.max_database_bytes
            or str(metadata["limits_sha256"]) != self._limits.fingerprint
        ):
            raise DwdRvCompletionIntegrityError("output store metadata differs")
        instance_sha = _sha(
            metadata["store_instance_sha256"], "output store instance SHA-256"
        )
        if (
            hasattr(self, "_store_instance_sha256")
            and not hmac.compare_digest(
                self._store_instance_sha256, instance_sha
            )
        ):
            raise DwdRvCompletionIntegrityError(
                "output store instance authority changed"
            )
        if self._columns(database, "completion_outputs") != self._output_columns():
            raise DwdRvCompletionIntegrityError("output table columns differ")
        self._validate_immutability_triggers(database)
        count = int(database.execute(
            "SELECT count(*) FROM completion_outputs"
        ).fetchone()[0])
        if count > self._limits.max_receipts:
            raise DwdRvCompletionCapacityError("output receipt cap exceeded")
        if full and database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise DwdRvCompletionIntegrityError("output store integrity check failed")
        if full:
            for row in database.execute("SELECT * FROM completion_outputs"):
                self._from_row(row)
        return instance_sha

    def initialize(self) -> None:
        with self._lock:
            self._database_witness = self._prepare_path()
            try:
                with closing(self._connect()) as database:
                    try:
                        database.execute("BEGIN IMMEDIATE")
                        if not self._tables(database):
                            self._create_schema(database)
                        elif self._is_legacy_v2(database):
                            self._migrate_v2_to_v3(database)
                        self._validate_schema(database)
                        self._fault("migration.before_commit")
                        database.commit()
                        self._fault("migration.after_commit")
                    except BaseException:
                        if database.in_transaction:
                            database.rollback()
                        raise
                with closing(self._connect()) as database:
                    self._store_instance_sha256 = self._validate_schema(
                        database, full=True
                    )
            except sqlite3.DatabaseError as exc:
                raise DwdRvCompletionIntegrityError(
                    "output receipt database is invalid"
                ) from exc

    @property
    def store_instance_sha256(self) -> str:
        self._verify_path()
        return self._store_instance_sha256

    @property
    def identity_sha256(self) -> str:
        self._verify_path()
        path_text = self._normalized_path(self.path)
        return _digest({
            "contract": RESULT_STORE_CONTRACT,
            "file_witness": {
                "st_dev": self._database_witness[0],
                "st_ino": self._database_witness[1],
                "st_mode": self._database_witness[2],
            },
            "limits_sha256": self._limits.fingerprint,
            "path_sha256": hashlib.sha256(path_text.encode("utf-8")).hexdigest(),
            "schema": STORE_IDENTITY_SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "store_instance_sha256": self.store_instance_sha256,
        })

    def assert_instance_held(
        self, expected_identity_sha256: str | None = None
    ) -> str:
        """Reprove the visible path/instance immediately at a caller boundary."""

        identity = self.identity_sha256
        if expected_identity_sha256 is not None and not hmac.compare_digest(
            identity,
            _sha(
                expected_identity_sha256,
                "expected output store identity SHA-256",
            ),
        ):
            raise DwdRvCompletionIntegrityError(
                "output store identity authority differs"
            )
        return identity

    def _from_row(self, row: sqlite3.Row) -> DwdRvCompletionOutputReceipt:
        body_bytes = bytes(row["output_json"])
        if len(body_bytes) != int(row["output_bytes"]):
            raise DwdRvCompletionIntegrityError("output receipt size differs")
        if len(body_bytes) > self._limits.max_receipt_bytes:
            raise DwdRvCompletionCapacityError("stored output receipt exceeds cap")
        try:
            output = json.loads(body_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DwdRvCompletionIntegrityError("output receipt JSON is corrupt") from exc
        if _canonical(output) != body_bytes:
            raise DwdRvCompletionIntegrityError("output receipt is not canonical")
        (
            document, authority, event, profile, decoder, shadow, isolation,
            fact_sha,
        ) = _validated_output_document(output)
        output_sha = hashlib.sha256(body_bytes).hexdigest()
        if not hmac.compare_digest(output_sha, str(row["output_sha256"])):
            raise DwdRvCompletionIntegrityError("output receipt checksum differs")
        expected = {
            "created_at": float(row["created_at"]),
            "output_sha256": output_sha,
            "schema": OUTPUT_RECEIPT_SCHEMA,
        }
        receipt_sha = _digest(expected)
        if not hmac.compare_digest(receipt_sha, str(row["receipt_sha256"])):
            raise DwdRvCompletionIntegrityError("durable receipt checksum differs")
        process_body = {
            "created_at": float(row["created_at"]),
            "effect_contract_sha256": isolation["effect_contract_sha256"],
            "event_id": event["event_id"],
            "isolation_authority_sha256": isolation["authority_sha256"],
            "limits_sha256": isolation["limits_sha256"],
            "output_receipt_sha256": receipt_sha,
            "output_sha256": output_sha,
            "schema": PROCESS_RECEIPT_SCHEMA,
        }
        process_receipt_sha = _digest(process_body)
        if not hmac.compare_digest(
            process_receipt_sha, str(row["process_receipt_sha256"])
        ):
            raise DwdRvCompletionIntegrityError(
                "durable hard-process receipt checksum differs"
            )
        if (
            event.get("event_id") != str(row["event_id"])
            or event.get("event_sha256") != str(row["event_sha256"])
            or event.get("producer_receipt_sha256")
            != str(row["producer_receipt_sha256"])
            or authority.get("desired_state_sha256")
            != str(row["desired_state_sha256"])
            or authority.get("authority_sha256") != str(row["authority_sha256"])
            or document["consumer_id"] != str(row["consumer_id"])
            or document["stream_id"] != str(row["stream_id"])
            or document["disposition"] != str(row["disposition"])
            or fact_sha != row["fact_revision_sha256"]
            or isolation.get("authority_sha256")
            != str(row["isolation_authority_sha256"])
            or isolation.get("effect_contract_sha256")
            != str(row["isolation_effect_contract_sha256"])
            or isolation.get("limits_sha256")
            != str(row["isolation_limits_sha256"])
            or profile.get("profile_sha256") != str(row["profile_sha256"])
            or shadow.get("content_sha256")
            != str(row["candidate_content_sha256"])
            or decoder.get("run_id") != str(row["decoder_run_id"])
        ):
            raise DwdRvCompletionIntegrityError("output receipt columns differ")
        return DwdRvCompletionOutputReceipt(
            receipt_sha256=receipt_sha,
            output_sha256=output_sha,
            event_id=str(row["event_id"]),
            event_sha256=str(row["event_sha256"]),
            producer_receipt_sha256=str(row["producer_receipt_sha256"]),
            desired_state_sha256=str(row["desired_state_sha256"]),
            authority_sha256=str(row["authority_sha256"]),
            consumer_id=str(row["consumer_id"]),
            stream_id=str(row["stream_id"]),
            disposition=str(row["disposition"]),
            fact_revision_sha256=(
                None
                if row["fact_revision_sha256"] is None
                else str(row["fact_revision_sha256"])
            ),
            isolation_authority_sha256=str(
                row["isolation_authority_sha256"]
            ),
            isolation_effect_contract_sha256=str(
                row["isolation_effect_contract_sha256"]
            ),
            isolation_limits_sha256=str(row["isolation_limits_sha256"]),
            process_receipt_sha256=process_receipt_sha,
            profile_sha256=str(row["profile_sha256"]),
            candidate_content_sha256=str(row["candidate_content_sha256"]),
            decoder_run_id=str(row["decoder_run_id"]),
            created_at=float(row["created_at"]),
            output=document,
        )

    def load(self, event_id: str) -> DwdRvCompletionOutputReceipt | None:
        event = _identifier(event_id, "event ID")
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM completion_outputs WHERE event_id=?", (event,)
            ).fetchone()
            return None if row is None else self._from_row(row)

    def verify_for_completion(
        self,
        *,
        event_id: str,
        event_sha256: str,
        desired_state_sha256: str,
        authority_sha256: str,
        consumer_id: str,
        stream_id: str,
        durable_output_receipt_sha256: str,
        durable_process_receipt_sha256: str,
        process_isolation_authority_sha256: str,
        output_sha256: str,
        disposition: str,
        staged_fact_revision: Mapping[str, Any] | None = None,
    ) -> DwdRvCompletionVerification:
        """Reprove every coordinator-visible fact before completion ack.

        ``staged_fact_revision`` is optional, but when supplied its complete
        canonical mapping must be byte-identical to the mapping already bound
        inside the immutable output.  This DWD shadow consumer currently binds
        ``None`` and therefore cannot be mistaken for a staged operational fact.
        """

        receipt = self.load(event_id)
        if receipt is None:
            raise DwdRvCompletionIntegrityError(
                "durable output receipt is unavailable"
            )
        return self._verify_loaded_for_completion(
            receipt=receipt,
            event_id=event_id,
            event_sha256=event_sha256,
            desired_state_sha256=desired_state_sha256,
            authority_sha256=authority_sha256,
            consumer_id=consumer_id,
            stream_id=stream_id,
            durable_output_receipt_sha256=durable_output_receipt_sha256,
            durable_process_receipt_sha256=durable_process_receipt_sha256,
            process_isolation_authority_sha256=(
                process_isolation_authority_sha256
            ),
            output_sha256=output_sha256,
            disposition=disposition,
            staged_fact_revision=staged_fact_revision,
        )

    def _verify_loaded_for_completion(
        self,
        *,
        receipt: DwdRvCompletionOutputReceipt,
        event_id: str,
        event_sha256: str,
        desired_state_sha256: str,
        authority_sha256: str,
        consumer_id: str,
        stream_id: str,
        durable_output_receipt_sha256: str,
        durable_process_receipt_sha256: str,
        process_isolation_authority_sha256: str,
        output_sha256: str,
        disposition: str,
        staged_fact_revision: Mapping[str, Any] | None,
    ) -> DwdRvCompletionVerification:
        if not isinstance(receipt, DwdRvCompletionOutputReceipt):
            raise TypeError("receipt must be DwdRvCompletionOutputReceipt")

        event = _identifier(event_id, "verification event ID")
        expected_event_sha = _sha(event_sha256, "verification event SHA-256")
        expected_desired = _sha(
            desired_state_sha256, "verification desired-state SHA-256"
        )
        expected_authority = _sha(
            authority_sha256, "verification authority SHA-256"
        )
        expected_consumer = _identifier(consumer_id, "verification consumer ID")
        expected_stream = _identifier(stream_id, "verification stream ID")
        expected_receipt = _sha(
            durable_output_receipt_sha256,
            "verification durable output receipt SHA-256",
        )
        expected_process_receipt = _sha(
            durable_process_receipt_sha256,
            "verification durable process receipt SHA-256",
        )
        expected_isolation_authority = _sha(
            process_isolation_authority_sha256,
            "verification process-isolation authority SHA-256",
        )
        expected_output = _sha(output_sha256, "verification output SHA-256")
        if disposition not in {"advanced", "withheld", "indeterminate"}:
            raise DwdRvCompletionIntegrityError(
                "verification disposition is invalid"
            )
        if staged_fact_revision is None:
            expected_fact = None
        else:
            fact = _mapping(staged_fact_revision, "verification fact revision")
            _assert_output_safe(fact)
            if not fact:
                raise DwdRvCompletionIntegrityError(
                    "verification fact revision cannot be empty"
                )
            expected_fact = _digest(fact)
        if receipt.event_id != event:
            raise DwdRvCompletionConflict(
                "coordinator completion event differs from durable output"
            )
        actual_fact = receipt.output.get("fact_revision")
        actual_fact_bytes = (
            None if actual_fact is None else _canonical(actual_fact)
        )
        expected_fact_bytes = (
            None
            if staged_fact_revision is None
            else _canonical(dict(staged_fact_revision))
        )
        checks = (
            (receipt.event_sha256, expected_event_sha),
            (receipt.desired_state_sha256, expected_desired),
            (receipt.authority_sha256, expected_authority),
            (receipt.consumer_id, expected_consumer),
            (receipt.stream_id, expected_stream),
            (receipt.receipt_sha256, expected_receipt),
            (receipt.process_receipt_sha256, expected_process_receipt),
            (
                receipt.isolation_authority_sha256,
                expected_isolation_authority,
            ),
            (receipt.output_sha256, expected_output),
            (receipt.disposition, disposition),
            (receipt.fact_revision_sha256, expected_fact),
            (actual_fact_bytes, expected_fact_bytes),
        )
        if any(actual != expected for actual, expected in checks):
            raise DwdRvCompletionConflict(
                "coordinator completion proof differs from durable output"
            )
        return DwdRvCompletionVerification(
            event_id=receipt.event_id,
            event_sha256=receipt.event_sha256,
            desired_state_sha256=receipt.desired_state_sha256,
            authority_sha256=receipt.authority_sha256,
            consumer_id=receipt.consumer_id,
            stream_id=receipt.stream_id,
            output_sha256=receipt.output_sha256,
            durable_output_receipt_sha256=receipt.receipt_sha256,
            durable_process_receipt_sha256=receipt.process_receipt_sha256,
            isolation_authority_sha256=receipt.isolation_authority_sha256,
            disposition=receipt.disposition,
            fact_revision_sha256=receipt.fact_revision_sha256,
        )

    @contextmanager
    def completion_reproof_lease(
        self,
        *,
        event_id: str,
        event_sha256: str,
        desired_state_sha256: str,
        authority_sha256: str,
        consumer_id: str,
        stream_id: str,
        durable_output_receipt_sha256: str,
        durable_process_receipt_sha256: str,
        process_isolation_authority_sha256: str,
        output_sha256: str,
        disposition: str,
        staged_fact_revision: Mapping[str, Any] | None = None,
    ) -> Iterator[DwdRvCompletionVerification]:
        """Fence the exact output row against writers through lifecycle ACK.

        The context owns a SQLite ``BEGIN IMMEDIATE`` transaction from the
        final row reproof until its caller has durably terminalized the
        lifecycle lease.  It therefore closes the cross-database gap in which
        an out-of-band DDL writer could otherwise drop the append-only trigger,
        alter or delete the row, recreate the trigger and race the ACK.  The
        transaction is read-only and is always rolled back to release its
        reserved writer lock.
        """

        event = _identifier(event_id, "verification event ID")
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database, full=True)
                row = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?",
                    (event,),
                ).fetchone()
                if row is None:
                    raise DwdRvCompletionIntegrityError(
                        "durable output receipt is unavailable"
                    )
                receipt = self._from_row(row)
                verified = self._verify_loaded_for_completion(
                    receipt=receipt,
                    event_id=event,
                    event_sha256=event_sha256,
                    desired_state_sha256=desired_state_sha256,
                    authority_sha256=authority_sha256,
                    consumer_id=consumer_id,
                    stream_id=stream_id,
                    durable_output_receipt_sha256=(
                        durable_output_receipt_sha256
                    ),
                    durable_process_receipt_sha256=(
                        durable_process_receipt_sha256
                    ),
                    process_isolation_authority_sha256=(
                        process_isolation_authority_sha256
                    ),
                    output_sha256=output_sha256,
                    disposition=disposition,
                    staged_fact_revision=staged_fact_revision,
                )
                yield verified
                self._verify_path()
                self._validate_schema(database)
                final_row = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?",
                    (event,),
                ).fetchone()
                if final_row is None:
                    raise DwdRvCompletionIntegrityError(
                        "durable output disappeared while reproof was fenced"
                    )
                final_receipt = self._from_row(final_row)
                if (
                    final_receipt.output_sha256 != receipt.output_sha256
                    or final_receipt.receipt_sha256 != receipt.receipt_sha256
                    or final_receipt.process_receipt_sha256
                    != receipt.process_receipt_sha256
                ):
                    raise DwdRvCompletionIntegrityError(
                        "durable output changed while reproof was fenced"
                    )
            finally:
                if database.in_transaction:
                    database.rollback()

    def commit(
        self,
        *,
        output: Mapping[str, Any],
    ) -> DwdRvCompletionOutputReceipt:
        (
            document, authority, event, profile, decoder, shadow, isolation,
            fact_sha,
        ) = _validated_output_document(output)
        body = _canonical(document)
        if not 0 < len(body) <= self._limits.max_receipt_bytes:
            raise DwdRvCompletionCapacityError("output receipt exceeds byte cap")
        event_id = _identifier(event.get("event_id"), "event ID")
        if _EVENT_ID.fullmatch(event_id) is None:
            raise DwdRvCompletionIntegrityError("output event ID differs")
        event_sha = _sha(event.get("event_sha256"), "event SHA-256")
        producer_sha = _sha(
            event.get("producer_receipt_sha256"), "producer receipt SHA-256"
        )
        desired_sha = _sha(
            authority.get("desired_state_sha256"), "desired-state SHA-256"
        )
        authority_sha = _sha(
            authority.get("authority_sha256"), "authority SHA-256"
        )
        profile_sha = _sha(profile.get("profile_sha256"), "profile SHA-256")
        candidate_sha = _sha(
            shadow.get("content_sha256"), "candidate content SHA-256"
        )
        decoder_run_id = _identifier(decoder.get("run_id"), "decoder run ID")
        if _RUN_ID.fullmatch(decoder_run_id) is None:
            raise DwdRvCompletionIntegrityError("decoder run ID differs")
        output_sha = hashlib.sha256(body).hexdigest()
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                existing = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?",
                    (event_id,),
                ).fetchone()
                if existing is not None:
                    receipt = self._from_row(existing)
                    if not hmac.compare_digest(receipt.output_sha256, output_sha):
                        raise DwdRvCompletionConflict(
                            "completion event already has a different output"
                        )
                    database.commit()
                    return receipt
                count = int(database.execute(
                    "SELECT count(*) FROM completion_outputs"
                ).fetchone()[0])
                if count >= self._limits.max_receipts:
                    raise DwdRvCompletionCapacityError("output receipt cap reached")
                created_at = _timestamp(self._clock(), "output receipt clock")
                receipt_sha = _digest({
                    "created_at": created_at,
                    "output_sha256": output_sha,
                    "schema": OUTPUT_RECEIPT_SCHEMA,
                })
                process_receipt_sha = _digest({
                    "created_at": created_at,
                    "effect_contract_sha256": isolation[
                        "effect_contract_sha256"
                    ],
                    "event_id": event_id,
                    "isolation_authority_sha256": isolation[
                        "authority_sha256"
                    ],
                    "limits_sha256": isolation["limits_sha256"],
                    "output_receipt_sha256": receipt_sha,
                    "output_sha256": output_sha,
                    "schema": PROCESS_RECEIPT_SCHEMA,
                })
                database.execute(
                    "INSERT INTO completion_outputs("
                    "event_id,event_sha256,producer_receipt_sha256,"
                    "desired_state_sha256,authority_sha256,consumer_id,stream_id,"
                    "disposition,fact_revision_sha256,"
                    "isolation_authority_sha256,"
                    "isolation_effect_contract_sha256,isolation_limits_sha256,"
                    "profile_sha256,candidate_content_sha256,decoder_run_id,"
                    "output_sha256,receipt_sha256,process_receipt_sha256,"
                    "output_json,output_bytes,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event_id, event_sha, producer_sha, desired_sha,
                        authority_sha, CONSUMER_ID, STREAM_ID,
                        OUTPUT_DISPOSITION, fact_sha,
                        isolation["authority_sha256"],
                        isolation["effect_contract_sha256"],
                        isolation["limits_sha256"],
                        profile_sha, candidate_sha, decoder_run_id, output_sha,
                        receipt_sha, process_receipt_sha, body, len(body), created_at,
                    ),
                )
                self._fault("output.before_commit")
                database.commit()
                self._fault("output.after_commit")
            except (DwdRvCompletionConsumerError, ValueError, TypeError):
                if database.in_transaction:
                    database.rollback()
                raise
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                message = str(exc).lower()
                if "full" in message:
                    raise DwdRvCompletionCapacityError(
                        "output receipt database capacity reached"
                    ) from exc
                raise DwdRvCompletionIntegrityError(
                    "output receipt database failed"
                ) from exc
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        receipt = self.load(event_id)
        if receipt is None:  # pragma: no cover - durable insert invariant
            raise DwdRvCompletionIntegrityError("committed output disappeared")
        return receipt

    def diagnostics(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            count, total = database.execute(
                "SELECT count(*),coalesce(sum(output_bytes),0) "
                "FROM completion_outputs"
            ).fetchone()
        result = {
            "contract": RESULT_STORE_CONTRACT,
            "healthy": True,
            "receipt_bytes": int(total),
            "receipts": int(count),
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise DwdRvCompletionCapacityError("output diagnostics exceed bound")
        return result


_PROOF_KEYS = frozenset({
    "adapter", "capacity_receipt", "desired_authority", "execution_receipts",
    "handoff_capture", "handoff_receipts", "meteorological_claim",
    "observation_policy", "raw_refetch_performed", "scheduler_terminal",
    "schema", "source_capture", "source_receipts", "workload",
})
_CAPTURE_KEYS = frozenset({
    "capture_id", "capture_receipt_schema", "capture_receipt_sha256",
    "complete", "created_at", "fetch_id", "manifest_sha256", "members",
    "product", "product_timestamp", "provider", "raw_store_schema_version",
    "schema", "upstream_capture_id",
})


def _safe_raw_receipt(receipt: RawArtifactReceipt) -> dict[str, Any]:
    return {
        "content_sha256": receipt.content_sha256,
        "content_type": receipt.content_type,
        "created_at": receipt.created_at,
        "fetch_id": receipt.fetch_id,
        "product": receipt.product,
        "provenance": receipt.provenance,
        "provenance_sha256": receipt.provenance_sha256,
        "provider": receipt.provider,
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": receipt.receipt_sha256,
        "received_at": receipt.received_at,
        "schema": "stv3-worker-raw-receipt-safe-evidence-v1",
        "size_bytes": receipt.size_bytes,
        "upstream_id": receipt.upstream_id,
    }


@dataclass(frozen=True, slots=True)
class _LeaseBinding:
    target_capture_id: str
    evaluated_at: float
    capture_receipt_sha256: str
    capture_manifest_sha256: str


def _validate_lease(
    lease: CompletionLease,
    raw_store: RawArtifactStore,
) -> _LeaseBinding:
    if not isinstance(lease, CompletionLease):
        raise TypeError("lease must be CompletionLease")
    if lease.consumer_id != CONSUMER_ID or lease.stream_id != STREAM_ID:
        raise DwdRvCompletionIntegrityError("completion routing differs")
    if (
        lease.size_bytes != len(lease.payload)
        or hashlib.sha256(lease.payload).hexdigest() != lease.content_sha256
    ):
        raise DwdRvCompletionIntegrityError("completion payload binding differs")
    proof = _exact_mapping(lease.proof, _PROOF_KEYS, "DWD completion proof")
    if (
        proof["schema"] != DWD_COMPLETION_PROOF_SCHEMA
        or proof["meteorological_claim"] is not False
        or proof["raw_refetch_performed"] is not False
        or proof["observation_policy"] != "base_plus000_only"
    ):
        raise DwdRvCompletionIntegrityError("DWD completion policy differs")
    authority = _exact_mapping(
        proof["desired_authority"],
        frozenset({
            "authority_epoch", "authority_sha256", "generation", "producer_id",
            "schema", "state_sha256",
        }),
        "desired authority",
    )
    authority_body = dict(authority)
    authority_sha = authority_body.pop("authority_sha256", None)
    if (
        authority["schema"] != DESIRED_AUTHORITY_SCHEMA
        or authority["state_sha256"] != lease.desired_state_sha256
        or authority_sha != lease.authority_sha256
        or _digest(authority_body) != authority_sha
    ):
        raise DwdRvCompletionIntegrityError("desired authority binding differs")
    workload = _mapping(proof["workload"], "workload")
    if (
        workload.get("job_id") != lease.acquisition_job_id
        or workload.get("snapshot_sha256") != lease.desired_state_sha256
        or workload.get("workload_id") != WORKLOAD_ID
        or workload.get("provider_id") != WORKLOAD_PROVIDER
        or workload.get("product_id") != WORKLOAD_PRODUCT
        or workload.get("secret_requirement_ids") != []
    ):
        raise DwdRvCompletionIntegrityError("closed DWD workload binding differs")
    scheduler = _mapping(proof["scheduler_terminal"], "scheduler terminal")
    if (
        scheduler.get("state") != "completed"
        or scheduler.get("last_transition") != "completed"
        or scheduler.get("desired_state_sha256") != lease.desired_state_sha256
        or scheduler.get("last_error_code") is not None
    ):
        raise DwdRvCompletionIntegrityError("scheduler terminal proof differs")
    evaluated_at = _timestamp(scheduler.get("terminal_at"), "terminal time")
    adapter = _mapping(proof["adapter"], "DWD handoff adapter")
    if (
        adapter.get("adapter_id") != HANDOFF_ADAPTER_ID
        or adapter.get("adapter_version") != HANDOFF_ADAPTER_VERSION
        or adapter.get("adapter_config_sha256") != HANDOFF_CONFIG_SHA256
    ):
        raise DwdRvCompletionIntegrityError("DWD handoff adapter differs")
    capture_proof = _exact_mapping(
        proof["handoff_capture"], _CAPTURE_KEYS, "handoff capture"
    )
    target_capture_id = _identifier(
        capture_proof.get("capture_id"), "handoff capture ID"
    )
    adapter_receipt = _mapping(adapter.get("receipt"), "handoff adapter receipt")
    if (
        adapter_receipt.get("target_capture_id") != target_capture_id
        or adapter_receipt.get("archive_sha256") != lease.content_sha256
        or adapter_receipt.get("adapter_config_sha256") != HANDOFF_CONFIG_SHA256
    ):
        raise DwdRvCompletionIntegrityError("handoff receipt binding differs")
    bundle = raw_store.load_capture_bundle_bytes(target_capture_id)
    if bundle is None:
        raise DwdRvCompletionIntegrityError("handoff capture is unavailable")
    capture, receipts, payloads = bundle
    if (
        capture.to_metadata() != dict(capture_proof)
        or capture.provider != TARGET_PROVIDER
        or capture.product != TARGET_CAPTURE_PRODUCT
        or set(receipts) != {"rv_tar"}
        or set(payloads) != {"rv_tar"}
        or payloads["rv_tar"] != lease.payload
    ):
        raise DwdRvCompletionIntegrityError("handoff raw authority differs")
    receipt = receipts["rv_tar"]
    proof_receipts = _mapping(proof["handoff_receipts"], "handoff receipts")
    if (
        dict(proof_receipts) != {"rv_tar": _safe_raw_receipt(receipt)}
        or receipt.provider != TARGET_PROVIDER
        or receipt.product != TARGET_ARCHIVE_PRODUCT
        or receipt.content_sha256 != lease.content_sha256
    ):
        raise DwdRvCompletionIntegrityError("handoff receipt authority differs")
    if evaluated_at < receipt.received_at:
        raise DwdRvCompletionIntegrityError("terminal time predates raw receipt")
    return _LeaseBinding(
        target_capture_id=target_capture_id,
        evaluated_at=evaluated_at,
        capture_receipt_sha256=capture.capture_receipt_sha256,
        capture_manifest_sha256=capture.manifest_sha256,
    )


def _identity(prefix: str, value: Mapping[str, Any]) -> str:
    return prefix + _digest(value)


def _check_deadline(deadline_at: float, monotonic: Callable[[], float]) -> None:
    now = float(monotonic())
    if not math.isfinite(now) or now >= deadline_at:
        raise DwdRvCompletionDeadlineError("DWD completion deadline expired")


def _execute_chain(
    *,
    lease: CompletionLease,
    binding: _LeaseBinding,
    raw_store: RawArtifactStore,
    artifact_store: DwdRvDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    profile: DwdRvShadowChainProfile,
    process_isolation: Mapping[str, Any],
    deadline_at: float,
    monotonic: Callable[[], float],
    fault: Callable[[str], None],
) -> dict[str, Any]:
    isolation = _exact_mapping(
        process_isolation,
        frozenset({
            "authority_sha256",
            "effect_contract_sha256",
            "limits_sha256",
            "state",
        }),
        "process isolation proof",
    )
    if isolation.get("state") != "enforced_before_target_start":
        raise DwdRvCompletionIntegrityError(
            "hard-process isolation was not enforced before target start"
        )
    for key in (
        "authority_sha256",
        "effect_contract_sha256",
        "limits_sha256",
    ):
        _sha(isolation.get(key), f"process isolation {key}")
    identity = {
        "event_id": lease.event_id,
        "event_sha256": lease.event_sha256,
        "producer_receipt_sha256": lease.producer_receipt_sha256,
        "schema": "stv3-dwd-rv-decoder-job-binding-v1",
    }
    decoder_job_id = _identity("decoder-job-", identity)
    attempt_id = _identity("decoder-attempt-", identity)
    decoded = decode_dwd_rv_capture(
        capture_id=binding.target_capture_id,
        decoder_job_id=decoder_job_id,
        attempt_id=attempt_id,
        raw_store=raw_store,
        artifact_store=artifact_store,
        evidence_store=evidence_store,
        evaluated_at=binding.evaluated_at,
        monotonic_deadline=deadline_at,
        monotonic_clock=monotonic,
    )
    if decoded.outcome != "decoded_complete" or decoded.artifact is None:
        raise DwdRvCompletionIntegrityError(
            f"DWD quantitative decode failed closed: {decoded.reason_code}"
        )
    fault("consumer.after_decode")
    _check_deadline(deadline_at, monotonic)
    authority = ProviderDerivedArtifactAuthority({"dwd_rv_v1": artifact_store})
    semantics = tuple(
        certificate
        for certificate in default_radar_semantics_certificates()
        if certificate.provider == TARGET_PROVIDER
        and certificate.product == TARGET_CAPTURE_PRODUCT
    )
    if len(semantics) != 1:
        raise DwdRvCompletionIntegrityError(
            "closed DWD semantics certificate is absent or ambiguous"
        )
    bridged = derive_canonical_radar_field(
        evidence_store,
        authority,
        run_id=decoded.evidence_receipt.run_id,
        certificates=semantics,
        deadline_at=deadline_at,
        monotonic=monotonic,
    )
    if not bridged.is_candidate or bridged.candidate is None:
        raise DwdRvCompletionIntegrityError(
            "canonical DWD field did not produce an exact shadow candidate"
        )
    native = bridged.candidate
    native_sha = native_candidate_content_sha256(native)
    certificate = issue_dwd_rv_native_grid_certificate(
        binding=NativeFieldCandidateBinding(
            candidate=native,
            candidate_content_sha256=native_sha,
            semantics_certificate_sha256=native.semantics_certificate_sha256,
        ),
        target_grid=profile.target_grid,
        config=profile.transform_config,
    )
    composed = compose_evidence_to_cells_shadow(
        evidence_store,
        authority,
        run_id=decoded.evidence_receipt.run_id,
        radar_semantics_certificates=semantics,
        target_grid=profile.target_grid,
        transform_adapter=ExactAffineGridTransformAdapter(certificate),
        transform_config=profile.transform_config,
        detector_config=profile.detector_config,
        pipeline_config=profile.pipeline_config,
        deadline_at=deadline_at,
        monotonic=monotonic,
    )
    if not composed.is_candidate or composed.candidate is None:
        raise DwdRvCompletionIntegrityError(
            "DWD evidence-to-cells chain returned no exact candidate"
        )
    candidate = composed.candidate
    if (
        candidate.admission_status != EVIDENCE_TO_CELLS_ADMISSION_STATUS
        or candidate.meteorological_admission_granted
        or candidate.forecast_generated
        or candidate.wet_dry_classification_applied
        or candidate.hard_realtime_deadline_certified
    ):
        raise DwdRvCompletionIntegrityError(
            "DWD cells output contains a forbidden operational claim"
        )
    fault("consumer.after_cells")
    _check_deadline(deadline_at, monotonic)
    return {
        "authority": {
            "authority_sha256": lease.authority_sha256,
            "desired_state_sha256": lease.desired_state_sha256,
        },
        "claims": {
            "forecast_generated": False,
            "hard_realtime_deadline_certified": False,
            "meteorological_admission_granted": False,
            "raw_refetch_performed": False,
            "wet_dry_classification_applied": False,
        },
        "consumer_id": CONSUMER_ID,
        "decoder": {
            "artifact_handle": decoded.artifact.handle,
            "artifact_sha256": decoded.artifact.sha256,
            "artifact_size_bytes": decoded.artifact.size_bytes,
            "attempt_id": attempt_id,
            "commit_id": decoded.evidence_receipt.commit_id,
            "job_id": decoder_job_id,
            "receipt_sha256": decoded.evidence_receipt.receipt_sha256,
            "result_sha256": decoded.evidence_receipt.result_sha256,
            "run_id": decoded.evidence_receipt.run_id,
        },
        "event": {
            "acquisition_job_id": lease.acquisition_job_id,
            "event_id": lease.event_id,
            "event_sha256": lease.event_sha256,
            "producer_receipt_sha256": lease.producer_receipt_sha256,
            "stream_sequence": lease.stream_sequence,
        },
        "disposition": OUTPUT_DISPOSITION,
        "fact_revision": None,
        "handoff": {
            "capture_id": binding.target_capture_id,
            "capture_manifest_sha256": binding.capture_manifest_sha256,
            "capture_receipt_sha256": binding.capture_receipt_sha256,
            "observation_policy": "base_plus000_only",
        },
        "input": {
            "content_sha256": lease.content_sha256,
            "size_bytes": lease.size_bytes,
        },
        "process_isolation": dict(isolation),
        "profile": {
            "profile_sha256": profile.fingerprint,
            "target_grid_sha256": analysis_grid_sha256(profile.target_grid),
            "transform_config_sha256": profile.transform_config.fingerprint,
            "detector_config_sha256": _digest(
                _detector_mapping(profile.detector_config)
            ),
            "pipeline_config_sha256": profile.pipeline_config.fingerprint,
        },
        "schema": OUTPUT_DOCUMENT_SCHEMA,
        "shadow": {
            "admission_status": candidate.admission_status,
            "analysis_field_content_sha256": (
                candidate.analysis_field_content_sha256
            ),
            "candidate_id": candidate.candidate_id,
            "content_sha256": candidate.content_sha256,
            "detection_objects": candidate.pixel_accounting.detection_objects,
            "operational_admission_blockers": list(
                candidate.operational_admission_blockers
            ),
            "target_grid_sha256": candidate.target_grid_sha256,
            "valid_at": candidate.normalized_frame.valid_at,
        },
        "stream_id": STREAM_ID,
    }


class DwdRvPipelineOutcomeAuthority:
    """Independent durable-output verifier for ``WorkerFirstRuntimeCoordinator``."""

    def __init__(
        self,
        *,
        output_store: DwdRvCompletionOutputStore,
        profile: DwdRvShadowChainProfile,
        process_isolation_authority_sha256: str,
    ) -> None:
        if not isinstance(output_store, DwdRvCompletionOutputStore):
            raise TypeError("output_store must be DwdRvCompletionOutputStore")
        if not isinstance(profile, DwdRvShadowChainProfile):
            raise TypeError("profile must be DwdRvShadowChainProfile")
        self.output_store = output_store
        self.profile = profile
        self.process_isolation_authority_sha256 = _sha(
            process_isolation_authority_sha256,
            "pipeline verifier process-isolation authority SHA-256",
        )

    @property
    def fingerprint(self) -> str:
        return _digest({
            "chain_profile_sha256": self.profile.fingerprint,
            "output_store_contract": RESULT_STORE_CONTRACT,
            "output_store_identity_sha256": self.output_store.identity_sha256,
            "process_isolation_authority_sha256": (
                self.process_isolation_authority_sha256
            ),
            "schema": "stv3-dwd-rv-pipeline-outcome-authority-v3",
        })

    def verify(
        self,
        *,
        lease: CompletionLease,
        desired_state: DesiredState,
        outcome: PipelineOutcome,
    ) -> VerifiedPipelineOutcome:
        """Reload and exactly bind every coordinator-visible completion field."""

        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        if not isinstance(desired_state, DesiredState):
            raise TypeError("desired_state must be DesiredState")
        if not isinstance(outcome, PipelineOutcome):
            raise TypeError("outcome must be PipelineOutcome")
        authority = desired_authority_proof(desired_state)
        if (
            lease.desired_state_sha256 != desired_state.state_sha256
            or lease.authority_sha256 != authority["authority_sha256"]
            or lease.consumer_id != CONSUMER_ID
            or lease.stream_id != STREAM_ID
            or outcome.fact_revision_staged
            or outcome.disposition != OUTPUT_DISPOSITION
        ):
            raise DwdRvCompletionConflict(
                "pipeline outcome authority differs from the DWD lease"
            )
        receipt = self.output_store.load(lease.event_id)
        if receipt is None:
            raise DwdRvCompletionIntegrityError(
                "durable pipeline output receipt is unavailable"
            )
        if (
            receipt.event_sha256 != lease.event_sha256
            or receipt.producer_receipt_sha256
            != lease.producer_receipt_sha256
            or receipt.desired_state_sha256 != lease.desired_state_sha256
            or receipt.authority_sha256 != lease.authority_sha256
            or receipt.consumer_id != lease.consumer_id
            or receipt.stream_id != lease.stream_id
            or receipt.profile_sha256 != self.profile.fingerprint
            or receipt.isolation_authority_sha256
            != self.process_isolation_authority_sha256
            or receipt.fact_revision_sha256 is not None
            or receipt.output_sha256 != outcome.output_sha256
            or receipt.process_receipt_sha256
            != outcome.process_receipt_sha256
            or receipt.disposition != outcome.disposition
        ):
            raise DwdRvCompletionConflict(
                "pipeline outcome differs from durable DWD output"
            )
        verified = self.output_store.verify_for_completion(
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            desired_state_sha256=desired_state.state_sha256,
            authority_sha256=lease.authority_sha256,
            consumer_id=lease.consumer_id,
            stream_id=lease.stream_id,
            durable_output_receipt_sha256=receipt.receipt_sha256,
            durable_process_receipt_sha256=receipt.process_receipt_sha256,
            process_isolation_authority_sha256=(
                self.process_isolation_authority_sha256
            ),
            output_sha256=outcome.output_sha256,
            disposition=outcome.disposition,
            staged_fact_revision=None,
        )
        if (
            verified.event_id != lease.event_id
            or verified.event_sha256 != lease.event_sha256
            or verified.desired_state_sha256 != desired_state.state_sha256
            or verified.authority_sha256 != lease.authority_sha256
            or verified.consumer_id != lease.consumer_id
            or verified.stream_id != lease.stream_id
            or verified.output_sha256 != outcome.output_sha256
            or verified.durable_output_receipt_sha256
            != receipt.receipt_sha256
            or verified.durable_process_receipt_sha256
            != outcome.process_receipt_sha256
            or verified.isolation_authority_sha256
            != self.process_isolation_authority_sha256
            or verified.disposition != outcome.disposition
            or verified.fact_revision_sha256 is not None
        ):
            raise DwdRvCompletionIntegrityError(
                "DWD completion verifier returned inconsistent authority"
            )
        return VerifiedPipelineOutcome(
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            producer_receipt_sha256=lease.producer_receipt_sha256,
            desired_state_sha256=desired_state.state_sha256,
            authority_epoch=desired_state.authority_epoch,
            authority_sha256=lease.authority_sha256,
            consumer_id=lease.consumer_id,
            stream_id=lease.stream_id,
            stream_sequence=lease.stream_sequence,
            output_sha256=receipt.output_sha256,
            output_receipt_sha256=receipt.receipt_sha256,
            process_receipt_sha256=receipt.process_receipt_sha256,
            disposition=receipt.disposition,
            verifier_profile_sha256=self.fingerprint,
            fact_binding=None,
        )


class DwdRvCompletionConsumer:
    """One serial lifecycle-consumer tick; no internal or secondary queue."""

    def __init__(
        self,
        *,
        lifecycle_store: AcquisitionEvidenceLifecycleStore,
        handoff_raw_store: RawArtifactStore,
        artifact_store: DwdRvDerivedArtifactStore,
        evidence_store: CanonicalEvidenceStore,
        output_store: DwdRvCompletionOutputStore,
        profile: DwdRvShadowChainProfile,
        process_runner: object,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        if not isinstance(lifecycle_store, AcquisitionEvidenceLifecycleStore):
            raise TypeError("lifecycle_store must be AcquisitionEvidenceLifecycleStore")
        if not isinstance(handoff_raw_store, RawArtifactStore):
            raise TypeError("handoff_raw_store must be RawArtifactStore")
        if not isinstance(artifact_store, DwdRvDerivedArtifactStore):
            raise TypeError("artifact_store must be DwdRvDerivedArtifactStore")
        if not isinstance(evidence_store, CanonicalEvidenceStore):
            raise TypeError("evidence_store must be CanonicalEvidenceStore")
        if not isinstance(output_store, DwdRvCompletionOutputStore):
            raise TypeError("output_store must be DwdRvCompletionOutputStore")
        if not isinstance(profile, DwdRvShadowChainProfile):
            raise TypeError("profile must be DwdRvShadowChainProfile")
        # Lazy import keeps the fixed child target free to import this module
        # while preventing an arbitrary/in-process runner from satisfying the
        # coordinator-facing consumer contract.
        from .dwd_rv_completion_process_isolation import (
            DwdRvCompletionIsolatedRunner,
        )
        if not isinstance(process_runner, DwdRvCompletionIsolatedRunner):
            raise TypeError(
                "process_runner must be DwdRvCompletionIsolatedRunner"
            )
        process_runner.assert_parent_binding(
            handoff_raw_store=handoff_raw_store,
            artifact_store=artifact_store,
            evidence_store=evidence_store,
            output_store=output_store,
            profile=profile,
        )
        self.lifecycle_store = lifecycle_store
        self.handoff_raw_store = handoff_raw_store
        self.artifact_store = artifact_store
        self.evidence_store = evidence_store
        self.output_store = output_store
        self.profile = profile
        self.process_runner = process_runner
        self._clock = clock
        self._monotonic = monotonic
        self._fault_injector = fault_injector

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _pipeline_outcome(
        self,
        lease: CompletionLease,
        output: DwdRvCompletionOutputReceipt,
    ) -> PipelineOutcome:
        self.output_store.verify_for_completion(
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            desired_state_sha256=lease.desired_state_sha256,
            authority_sha256=lease.authority_sha256,
            consumer_id=lease.consumer_id,
            stream_id=lease.stream_id,
            durable_output_receipt_sha256=output.receipt_sha256,
            durable_process_receipt_sha256=output.process_receipt_sha256,
            process_isolation_authority_sha256=(
                self.process_runner.authority_sha256
            ),
            output_sha256=output.output_sha256,
            disposition=output.disposition,
            staged_fact_revision=None,
        )
        return PipelineOutcome(
            output_sha256=output.output_sha256,
            disposition=output.disposition,
            fact_revision_staged=False,
            process_receipt_sha256=output.process_receipt_sha256,
        )

    def process_leased(
        self,
        lease: CompletionLease,
        desired_state: DesiredState,
        cancel_event: threading.Event,
    ) -> PipelineOutcome:
        """Process one coordinator-owned lease without claiming or acknowledging.

        The caller remains the sole lease owner.  This method only verifies the
        lease, commits/reloads the bounded durable output receipt and returns a
        hash-only :class:`PipelineOutcome`.  The coordinator must call
        :meth:`DwdRvCompletionOutputStore.verify_for_completion` immediately
        before its own lifecycle acknowledgement.
        """

        outcome, _diagnostic = self.process_leased_with_diagnostics(
            lease,
            desired_state,
            cancel_event,
        )
        return outcome

    def process_leased_with_diagnostics(
        self,
        lease: CompletionLease,
        desired_state: DesiredState,
        cancel_event: threading.Event,
    ) -> tuple[PipelineOutcome, Mapping[str, Any]]:
        """Process once and expose only the bounded technical child diagnostic.

        This offline measurement hook retains the same lease ownership rule as
        :meth:`process_leased`: it neither claims nor acknowledges.  The second
        return value is the process result's bounded, path-free mapping; it is
        not persisted in the lifecycle and contains no raw bytes or secrets.
        """

        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        if not isinstance(desired_state, DesiredState):
            raise TypeError("desired_state must be DesiredState")
        if not callable(getattr(cancel_event, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        if cancel_event.is_set():
            raise DwdRvCompletionCancelled("DWD completion was cancelled")
        authority = desired_authority_proof(desired_state)
        if (
            desired_state.state_sha256 != lease.desired_state_sha256
            or authority["authority_sha256"] != lease.authority_sha256
        ):
            raise DwdRvCompletionConflict(
                "active desired authority differs from completion lease"
            )
        wall_now = _timestamp(self._clock(), "consumer clock")
        remaining = lease.lease_expires_at - wall_now
        if not math.isfinite(remaining) or remaining <= 0.0:
            raise DwdRvCompletionDeadlineError(
                "completion lease expired before processing"
            )
        if remaining <= self.process_runner.minimum_lease_seconds:
            raise DwdRvCompletionDeadlineError(
                "completion lease is shorter than the hard-process boundary"
            )
        binding = _validate_lease(lease, self.handoff_raw_store)
        isolated = self.process_runner.execute(
            lease=lease,
            binding=binding,
            cancel_event=cancel_event,
        )
        self.process_runner.raise_for_terminal(isolated)
        output = isolated.output_receipt
        if output is None:  # pragma: no cover - result invariant
            raise DwdRvCompletionIntegrityError(
                "committed hard-process result has no output receipt"
            )
        self._fault("consumer.after_output_commit")
        if cancel_event.is_set():
            raise DwdRvCompletionCancelled("DWD completion was cancelled")
        if _timestamp(self._clock(), "post-process clock") >= lease.lease_expires_at:
            raise DwdRvCompletionDeadlineError(
                "completion lease expired after durable process commit"
            )
        return self._pipeline_outcome(lease, output), isolated.to_mapping()

    def run_once(
        self,
        owner_id: str,
        desired_state: DesiredState,
        *,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
    ) -> DwdRvCompletionRunResult:
        """Standalone harness composing claim -> process -> verify -> ack."""

        owner = _identifier(owner_id, "consumer owner")
        if not isinstance(desired_state, DesiredState):
            raise TypeError("desired_state must be DesiredState")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(float(lease_seconds))
            or not 0.0 < float(lease_seconds) <= MAX_LEASE_SECONDS
        ):
            raise ValueError("lease_seconds is outside its boundary")
        if self.process_runner.minimum_lease_seconds >= float(lease_seconds):
            raise ValueError("hard-process boundary must be shorter than standalone lease")
        wall_now = _timestamp(self._clock(), "consumer clock")
        lease = self.lifecycle_store.claim_next(
            CONSUMER_ID,
            STREAM_ID,
            owner,
            lease_seconds=float(lease_seconds),
            now=wall_now,
        )
        if lease is None:
            return DwdRvCompletionRunResult("idle", None, None, None)
        self._fault("consumer.after_claim")
        had_output = self.output_store.load(lease.event_id) is not None
        outcome = self.process_leased(lease, desired_state, threading.Event())
        output = self.output_store.load(lease.event_id)
        if output is None:  # pragma: no cover - process contract invariant
            raise DwdRvCompletionIntegrityError("pipeline output disappeared")
        verified = DwdRvPipelineOutcomeAuthority(
            output_store=self.output_store,
            profile=self.profile,
            process_isolation_authority_sha256=(
                self.process_runner.authority_sha256
            ),
        ).verify(lease=lease, desired_state=desired_state, outcome=outcome)
        expected_output_store_identity = self.output_store.identity_sha256
        with self.output_store.completion_reproof_lease(
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            desired_state_sha256=desired_state.state_sha256,
            authority_sha256=lease.authority_sha256,
            consumer_id=lease.consumer_id,
            stream_id=lease.stream_id,
            durable_output_receipt_sha256=verified.output_receipt_sha256,
            durable_process_receipt_sha256=(
                verified.process_receipt_sha256
            ),
            process_isolation_authority_sha256=(
                self.process_runner.authority_sha256
            ),
            output_sha256=verified.output_sha256,
            disposition=verified.disposition,
            staged_fact_revision=None,
        ) as fenced:
            if (
                fenced.output_sha256 != verified.output_sha256
                or fenced.durable_output_receipt_sha256
                != verified.output_receipt_sha256
                or fenced.durable_process_receipt_sha256
                != verified.process_receipt_sha256
            ):
                raise DwdRvCompletionIntegrityError(
                    "fenced DWD completion proof differs"
                )
            self._fault("consumer.after_final_reproof_before_ack")
            self.output_store.assert_instance_held(
                expected_output_store_identity
            )
            consumption = self.lifecycle_store.acknowledge(
                lease.event_id,
                lease.lease_token,
                output_sha256=verified.output_sha256,
                now=_timestamp(self._clock(), "acknowledgement clock"),
            )
        self._fault("consumer.after_ack")
        return DwdRvCompletionRunResult(
            "recovered_output" if had_output else "processed",
            lease.event_id,
            output,
            consumption,
        )


__all__ = [
    "CONSUMER_ID",
    "DwdRvCompletionCancelled",
    "DwdRvCompletionCapacityError",
    "DwdRvCompletionConflict",
    "DwdRvCompletionConsumer",
    "DwdRvCompletionConsumerError",
    "DwdRvCompletionDeadlineError",
    "DwdRvCompletionIntegrityError",
    "DwdRvCompletionOutputLimits",
    "DwdRvCompletionOutputReceipt",
    "DwdRvCompletionOutputStore",
    "DwdRvCompletionRunResult",
    "DwdRvCompletionVerification",
    "DwdRvPipelineOutcomeAuthority",
    "DwdRvShadowChainProfile",
    "LEGACY_RESULT_STORE_CONTRACT",
    "LEGACY_SCHEMA_VERSION",
    "OUTPUT_DOCUMENT_SCHEMA",
    "OUTPUT_DISPOSITION",
    "OUTPUT_LIMITS_SCHEMA",
    "OUTPUT_RECEIPT_SCHEMA",
    "RESULT_STORE_CONTRACT",
    "SCHEMA_VERSION",
    "STORE_IDENTITY_SCHEMA",
    "STREAM_ID",
]
