"""Inert adapters from closed completion stores to the sealed output archive.

The compatibility factory remains limited to DWD RV, KNMI RTCOR, Met Office
UK rate and METAR ground.  A second closed-catalogue factory also binds NOAA
ERR and every route-separated output in the shared raw-manifest audit store.
A source row is read twice: once to build the candidate archive record and
again, under the real store lock plus a SQLite ``BEGIN IMMEDIATE`` transaction,
while the archive seal is committed.  No provider store is mutated and this
module is not wired into a runtime.
"""

from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, ContextManager, Iterator, Mapping
import hashlib
import hmac
import json
import math
import os
import sqlite3
import stat
import time

from .completion_output_archive import (
    CompletionOutputArchiveEntry,
    CompletionOutputArchiveIntegrityError,
    CompletionOutputArchiveLimits,
    CompletionOutputArchiveRecord,
    CompletionOutputArchiveSeal,
    CompletionOutputArchiveSourceCatalog,
    CompletionOutputArchiveSourceProfile,
    CompletionOutputArchiveSourceReproofAuthority,
    CompletionOutputArchiveSourceReproofBinding,
    CompletionOutputArchiveSourceRowReproof,
    CompletionOutputArchiveStore,
    CompletionOutputHotWindowPolicy,
)
from .acquisition_activation import ACTIVATION_RULES, ActivationStatus
from .acquisition_capture_recovery import (
    RECOVERY_BINDINGS,
    ROLE_SET_DIGEST_CONTRACT,
    CaptureRecoveryIntegrityError,
    RawRecoveryBinding,
    recovery_binding_sha256,
)
from .dwd_rv_completion_consumer import (
    CONSUMER_ID as DWD_CONSUMER_ID,
    OUTPUT_DOCUMENT_SCHEMA as DWD_OUTPUT_DOCUMENT_SCHEMA,
    OUTPUT_RECEIPT_SCHEMA as DWD_OUTPUT_RECEIPT_SCHEMA,
    PROCESS_RECEIPT_SCHEMA as DWD_PROCESS_RECEIPT_SCHEMA,
    RESULT_STORE_CONTRACT as DWD_OUTPUT_STORE_CONTRACT,
    SCHEMA_VERSION as DWD_SCHEMA_VERSION,
    STREAM_ID as DWD_STREAM_ID,
    WORKLOAD_ID as DWD_WORKLOAD_ID,
    DwdRvCompletionOutputReceipt,
    DwdRvCompletionOutputStore,
)
from .dwd_rv_completion_process_isolation import (
    PROCESS_EFFECT_CONTRACT_SHA256 as DWD_PROCESS_EFFECT_CONTRACT_SHA256,
)
from .knmi_rtcor_completion_consumer import (
    CONSUMER_ID as KNMI_CONSUMER_ID,
    OUTPUT_DOCUMENT_SCHEMA as KNMI_OUTPUT_DOCUMENT_SCHEMA,
    OUTPUT_RECEIPT_SCHEMA as KNMI_OUTPUT_RECEIPT_SCHEMA,
    OUTPUT_STORE_CONTRACT as KNMI_OUTPUT_STORE_CONTRACT,
    PROCESS_EFFECT_CONTRACT_SHA256 as KNMI_PROCESS_EFFECT_CONTRACT_SHA256,
    PROCESS_RECEIPT_SCHEMA as KNMI_PROCESS_RECEIPT_SCHEMA,
    SCHEMA_VERSION as KNMI_SCHEMA_VERSION,
    STREAM_ID as KNMI_STREAM_ID,
    WORKLOAD_ID as KNMI_WORKLOAD_ID,
    KnmiRtcorCompletionOutputReceipt,
    KnmiRtcorCompletionOutputStore,
)
from .met_office_rate_completion_consumer import (
    CONSUMER_ID as MET_OFFICE_CONSUMER_ID,
    OUTPUT_DOCUMENT_SCHEMA as MET_OFFICE_OUTPUT_DOCUMENT_SCHEMA,
    OUTPUT_RECEIPT_SCHEMA as MET_OFFICE_OUTPUT_RECEIPT_SCHEMA,
    OUTPUT_STORE_CONTRACT as MET_OFFICE_OUTPUT_STORE_CONTRACT,
    PROCESS_EFFECT_CONTRACT_SHA256 as MET_OFFICE_PROCESS_EFFECT_CONTRACT_SHA256,
    PROCESS_RECEIPT_SCHEMA as MET_OFFICE_PROCESS_RECEIPT_SCHEMA,
    SCHEMA_VERSION as MET_OFFICE_SCHEMA_VERSION,
    STREAM_ID as MET_OFFICE_STREAM_ID,
    WORKLOAD_ID as MET_OFFICE_WORKLOAD_ID,
    MetOfficeRateCompletionOutputReceipt,
    MetOfficeRateCompletionOutputStore,
)
from .metar_ground_completion_consumer import (
    CONSUMER_ID as METAR_CONSUMER_ID,
    OUTPUT_DOCUMENT_SCHEMA as METAR_OUTPUT_DOCUMENT_SCHEMA,
    OUTPUT_RECEIPT_SCHEMA as METAR_OUTPUT_RECEIPT_SCHEMA,
    OUTPUT_STORE_CONTRACT as METAR_OUTPUT_STORE_CONTRACT,
    PROCESS_EFFECT_CONTRACT_SHA256 as METAR_PROCESS_EFFECT_CONTRACT_SHA256,
    PROCESS_RECEIPT_SCHEMA as METAR_PROCESS_RECEIPT_SCHEMA,
    SCHEMA_VERSION as METAR_SCHEMA_VERSION,
    STREAM_ID as METAR_STREAM_ID,
    WORKLOAD_ID as METAR_WORKLOAD_ID,
    MetarGroundCompletionOutputReceipt,
    MetarGroundCompletionOutputStore,
)
from .noaa_err_completion_consumer import (
    CONSUMER_ID as NOAA_CONSUMER_ID,
    OUTPUT_DOCUMENT_SCHEMA as NOAA_OUTPUT_DOCUMENT_SCHEMA,
    OUTPUT_RECEIPT_SCHEMA as NOAA_OUTPUT_RECEIPT_SCHEMA,
    OUTPUT_STORE_CONTRACT as NOAA_OUTPUT_STORE_CONTRACT,
    PROCESS_EFFECT_CONTRACT_SHA256 as NOAA_PROCESS_EFFECT_CONTRACT_SHA256,
    PROCESS_RECEIPT_SCHEMA as NOAA_PROCESS_RECEIPT_SCHEMA,
    OUTPUT_STORE_SCHEMA_VERSION as NOAA_SCHEMA_VERSION,
    STREAM_ID as NOAA_STREAM_ID,
    WORKLOAD_ID as NOAA_WORKLOAD_ID,
    NoaaErrCompletionOutputReceipt,
    NoaaErrCompletionOutputStore,
    NoaaErrRegionAuthority,
)
from .raw_manifest_audit_completion_consumer import (
    AUDIT_CONSUMER_CATALOG,
    OUTPUT_DOCUMENT_SCHEMA as RAW_AUDIT_OUTPUT_DOCUMENT_SCHEMA,
    OUTPUT_RECEIPT_SCHEMA as RAW_AUDIT_OUTPUT_RECEIPT_SCHEMA,
    OUTPUT_STORE_CONTRACT as RAW_AUDIT_OUTPUT_STORE_CONTRACT,
    PROCESS_EFFECT_CONTRACT_SHA256 as RAW_AUDIT_PROCESS_EFFECT_CONTRACT_SHA256,
    PROCESS_RECEIPT_SCHEMA as RAW_AUDIT_PROCESS_RECEIPT_SCHEMA,
    SCHEMA_VERSION as RAW_AUDIT_SCHEMA_VERSION,
    RawManifestAuditOutputReceipt,
    RawManifestAuditOutputStore,
)
from .terminal_raw_completion_router import (
    REFERENCE_TRANSFER_MODE,
    TERMINAL_RAW_CONSUMER_CATALOG,
)
from .worker_acquisition_plan import CATALOG_SHA256, acquisition_catalog_snapshot


ADAPTER_CATALOG_ID = "stable-completion-output-archive-adapters-v2"
ADAPTER_AUTHORITY_ID = "stable-completion-output-source-reproof-authority-v2"
ADAPTER_CONTRACT = "stv3-stable-completion-output-archive-adapters-v2"
ALL_ADAPTER_CATALOG_ID = "all-terminal-completion-output-archive-adapters-v2"
ALL_ADAPTER_AUTHORITY_ID = "all-terminal-completion-output-source-reproof-authority-v2"
ALL_ADAPTER_CONTRACT = "stv3-all-terminal-completion-output-archive-adapters-v2"
ADAPTER_DESCRIPTOR_SCHEMA = "stv3-completion-output-archive-adapter-descriptor-v2"
SOURCE_ROW_CANONICAL_SCHEMA = "stv3-completion-output-source-row-canonical-v1"
SOURCE_FILE_WITNESS_SCHEMA = "stv3-completion-output-source-file-witness-v1"
SOURCE_STORE_IDENTITY_SCHEMA = "stv3-completion-output-source-store-identity-v2"
SOURCE_RESOLVER_SCHEMA = "stv3-completion-output-source-resolver-v2"
DWD_ROW_REPROOF_CONTRACT = (
    "dwd-completion-output-trigger-immutable-plus-checksum-reproof-v3"
)
TRIGGER_ROW_REPROOF_CONTRACT = (
    "completion-output-trigger-immutable-plus-checksum-reproof-v2"
)
RAW_MANIFEST_ROW_REPROOF_CONTRACT = "raw-manifest-audit-route-reproof-v2"
RAW_MANIFEST_PHYSICAL_STORE_SCOPE = "shared-raw-manifest-audit-output-store-v2"
SOURCE_PATH_IDENTITY_CASE_POLICY = (
    "windows-normcase-v1" if os.name == "nt" else "platform-case-sensitive-v1"
)
STABLE_ARCHIVE_WORKLOAD_IDS = frozenset(
    {DWD_WORKLOAD_ID, KNMI_WORKLOAD_ID, MET_OFFICE_WORKLOAD_ID, METAR_WORKLOAD_ID}
)
RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS = frozenset(AUDIT_CONSUMER_CATALOG)
ALL_ARCHIVE_WORKLOAD_IDS = frozenset(
    STABLE_ARCHIVE_WORKLOAD_IDS
    | RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS
    | {NOAA_WORKLOAD_ID}
)


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CompletionOutputArchiveIntegrityError(
            "adapter authority is not canonical JSON"
        ) from exc


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _exact_sha256(value: object, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise CompletionOutputArchiveIntegrityError(f"{label} differs")
    return value


def _safe_store_path(value: object) -> Path:
    if not isinstance(value, Path) or not value.is_absolute():
        raise CompletionOutputArchiveIntegrityError(
            "completion source path must be absolute"
        )
    resolved = value.resolve(strict=True)
    if value != resolved:
        raise CompletionOutputArchiveIntegrityError(
            "completion source path is aliased"
        )
    cursor = value
    while True:
        if cursor.is_symlink():
            raise CompletionOutputArchiveIntegrityError(
                "completion source path uses a symlink"
            )
        parent = cursor.parent
        if parent == cursor:
            break
        cursor = parent
    if not stat.S_ISREG(value.lstat().st_mode):
        raise CompletionOutputArchiveIntegrityError(
            "completion source path is not a regular file"
        )
    return value


def _physical_row_bytes(
    *,
    row: sqlite3.Row,
    profile: CompletionOutputArchiveSourceProfile,
) -> bytes:
    columns: dict[str, object] = {}
    names = tuple(sorted(str(name) for name in row.keys()))
    if not names or len(set(names)) != len(names):
        raise CompletionOutputArchiveIntegrityError(
            "completion source row columns differ"
        )
    for name in names:
        value = row[name]
        if isinstance(value, bytes):
            columns[name] = {
                "blob_bytes": len(value),
                "blob_sha256": hashlib.sha256(value).hexdigest(),
                "sqlite_type": "blob",
            }
        elif value is None:
            columns[name] = {"sqlite_type": "null", "value": None}
        elif isinstance(value, str):
            columns[name] = {"sqlite_type": "text", "value": value}
        elif isinstance(value, int) and not isinstance(value, bool):
            columns[name] = {"sqlite_type": "integer", "value": value}
        elif isinstance(value, float) and math.isfinite(value):
            columns[name] = {"sqlite_type": "real", "value": float(value)}
        else:
            raise CompletionOutputArchiveIntegrityError(
                "completion source row contains an unsupported SQLite value"
            )
    return _canonical(
        {
            "columns": columns,
            "schema": SOURCE_ROW_CANONICAL_SCHEMA,
            "source_profile_sha256": profile.fingerprint,
            "source_store_contract": profile.source_store_contract,
            "source_store_schema_version": profile.source_store_schema_version,
        }
    )


def _dwd_store_authority(
    store: DwdRvCompletionOutputStore,
) -> tuple[str, str, str]:
    database: sqlite3.Connection | None = None
    try:
        with store._lock:
            database = store._connect()
            database.execute("BEGIN IMMEDIATE")
            instance_sha256 = store._validate_schema(database, full=True)
            metadata = database.execute(
                "SELECT limits_sha256,store_instance_sha256 "
                "FROM store_metadata WHERE singleton=1"
            ).fetchone()
            if (
                metadata is None
                or metadata["limits_sha256"] is None
                or metadata["store_instance_sha256"] != instance_sha256
                or store.store_instance_sha256 != instance_sha256
            ):
                raise CompletionOutputArchiveIntegrityError(
                    "DWD source store authority differs"
                )
            limits_sha256 = str(metadata["limits_sha256"])
            identity_sha256 = store.identity_sha256
            database.rollback()
            database.close()
            database = None
            return limits_sha256, instance_sha256, identity_sha256
    except CompletionOutputArchiveIntegrityError:
        raise
    except Exception as exc:
        raise CompletionOutputArchiveIntegrityError(
            "DWD source store authority reproof failed"
        ) from exc
    finally:
        if database is not None:
            if database.in_transaction:
                database.rollback()
            database.close()


def _native_store_authority(
    *, spec: "_StoreSpec", store: object
) -> tuple[str, str, str]:
    """Bind one exact native store lineage before constructing a resolver."""

    if spec.workload_id == DWD_WORKLOAD_ID:
        limits_sha256, instance_sha256, identity_sha256 = _dwd_store_authority(
            store
        )
    else:
        try:
            limits_sha256 = _exact_sha256(
                store.limits.fingerprint, "completion source limits SHA-256"
            )
            identity_sha256 = _exact_sha256(
                store.assert_instance_held(),
                "completion source identity SHA-256",
            )
            instance_sha256 = _exact_sha256(
                store.store_instance_sha256,
                "completion source instance SHA-256",
            )
            repeated_identity = _exact_sha256(
                store.assert_instance_held(identity_sha256),
                "repeated completion source identity SHA-256",
            )
        except Exception as exc:
            raise CompletionOutputArchiveIntegrityError(
                "completion source native authority reproof failed"
            ) from exc
        if not hmac.compare_digest(identity_sha256, repeated_identity):
            raise CompletionOutputArchiveIntegrityError(
                "completion source native identity changed during construction"
            )
    return (
        _exact_sha256(limits_sha256, "completion source limits SHA-256"),
        _exact_sha256(instance_sha256, "completion source instance SHA-256"),
        _exact_sha256(identity_sha256, "completion source identity SHA-256"),
    )


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveAdapterDescriptor:
    workload_id: str
    source_store_type: str
    source_profile_sha256: str
    source_store_identity_sha256: str
    native_store_identity_sha256: str
    native_store_instance_sha256: str
    resolver_sha256: str
    durable_production_retention_eligible: bool
    production_retention_claimed: bool
    source_payload_materialized: bool
    raw_capture_service_only: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "durable_production_retention_eligible": (
                self.durable_production_retention_eligible
            ),
            "native_store_identity_sha256": self.native_store_identity_sha256,
            "native_store_instance_sha256": self.native_store_instance_sha256,
            "production_retention_claimed": self.production_retention_claimed,
            "raw_capture_service_only": self.raw_capture_service_only,
            "resolver_sha256": self.resolver_sha256,
            "schema": ADAPTER_DESCRIPTOR_SCHEMA,
            "source_payload_materialized": self.source_payload_materialized,
            "source_profile_sha256": self.source_profile_sha256,
            "source_store_identity_sha256": self.source_store_identity_sha256,
            "source_store_type": self.source_store_type,
            "workload_id": self.workload_id,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())


@dataclass(frozen=True, slots=True)
class _SourceFileWitness:
    st_dev: int
    st_ino: int
    st_mode: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.st_dev, bool)
            or not isinstance(self.st_dev, int)
            or self.st_dev < 0
            or isinstance(self.st_ino, bool)
            or not isinstance(self.st_ino, int)
            or self.st_ino <= 0
            or isinstance(self.st_mode, bool)
            or not isinstance(self.st_mode, int)
            or self.st_mode <= 0
        ):
            raise CompletionOutputArchiveIntegrityError(
                "completion source file has no persistent instance witness"
            )

    def as_dict(self) -> dict[str, int | str]:
        return {
            "schema": SOURCE_FILE_WITNESS_SCHEMA,
            "st_dev": self.st_dev,
            "st_ino": self.st_ino,
            "st_mode": self.st_mode,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())


def _source_file_witness(path: Path) -> _SourceFileWitness:
    try:
        details = path.lstat()
    except OSError as exc:
        raise CompletionOutputArchiveIntegrityError(
            "completion source file witness is unavailable"
        ) from exc
    if path.is_symlink() or not stat.S_ISREG(details.st_mode):
        raise CompletionOutputArchiveIntegrityError(
            "completion source file witness type differs"
        )
    return _SourceFileWitness(
        st_dev=int(details.st_dev),
        st_ino=int(details.st_ino),
        st_mode=int(details.st_mode),
    )


@dataclass(frozen=True, slots=True)
class _StoreSpec:
    workload_id: str
    consumer_id: str
    stream_id: str
    store_type: type
    receipt_type: type
    store_contract: str
    schema_version: int
    output_schema: str
    output_receipt_schema: str
    process_receipt_schema: str
    effect_contract_sha256: str
    immutability_mode: str
    reproof_contract: str
    isolation_key: str
    store_kind: str = "completion_output"
    table_name: str = "completion_outputs"
    row_decoder_name: str = "_from_row"
    authority_output_key: str = "authority_sha256"
    authority_receipt_attribute: str = "authority_sha256"
    physical_store_scope_id: str | None = None
    recovery_binding: RawRecoveryBinding | None = None
    route_authority_sha256: str | None = None


_SPECS = (
    _StoreSpec(
        DWD_WORKLOAD_ID,
        DWD_CONSUMER_ID,
        DWD_STREAM_ID,
        DwdRvCompletionOutputStore,
        DwdRvCompletionOutputReceipt,
        DWD_OUTPUT_STORE_CONTRACT,
        DWD_SCHEMA_VERSION,
        DWD_OUTPUT_DOCUMENT_SCHEMA,
        DWD_OUTPUT_RECEIPT_SCHEMA,
        DWD_PROCESS_RECEIPT_SCHEMA,
        DWD_PROCESS_EFFECT_CONTRACT_SHA256,
        "append_only_sqlite_triggers",
        DWD_ROW_REPROOF_CONTRACT,
        "process_isolation",
    ),
    _StoreSpec(
        KNMI_WORKLOAD_ID,
        KNMI_CONSUMER_ID,
        KNMI_STREAM_ID,
        KnmiRtcorCompletionOutputStore,
        KnmiRtcorCompletionOutputReceipt,
        KNMI_OUTPUT_STORE_CONTRACT,
        KNMI_SCHEMA_VERSION,
        KNMI_OUTPUT_DOCUMENT_SCHEMA,
        KNMI_OUTPUT_RECEIPT_SCHEMA,
        KNMI_PROCESS_RECEIPT_SCHEMA,
        KNMI_PROCESS_EFFECT_CONTRACT_SHA256,
        "append_only_sqlite_triggers",
        TRIGGER_ROW_REPROOF_CONTRACT,
        "isolation",
    ),
    _StoreSpec(
        MET_OFFICE_WORKLOAD_ID,
        MET_OFFICE_CONSUMER_ID,
        MET_OFFICE_STREAM_ID,
        MetOfficeRateCompletionOutputStore,
        MetOfficeRateCompletionOutputReceipt,
        MET_OFFICE_OUTPUT_STORE_CONTRACT,
        MET_OFFICE_SCHEMA_VERSION,
        MET_OFFICE_OUTPUT_DOCUMENT_SCHEMA,
        MET_OFFICE_OUTPUT_RECEIPT_SCHEMA,
        MET_OFFICE_PROCESS_RECEIPT_SCHEMA,
        MET_OFFICE_PROCESS_EFFECT_CONTRACT_SHA256,
        "append_only_sqlite_triggers",
        TRIGGER_ROW_REPROOF_CONTRACT,
        "isolation",
    ),
    _StoreSpec(
        METAR_WORKLOAD_ID,
        METAR_CONSUMER_ID,
        METAR_STREAM_ID,
        MetarGroundCompletionOutputStore,
        MetarGroundCompletionOutputReceipt,
        METAR_OUTPUT_STORE_CONTRACT,
        METAR_SCHEMA_VERSION,
        METAR_OUTPUT_DOCUMENT_SCHEMA,
        METAR_OUTPUT_RECEIPT_SCHEMA,
        METAR_PROCESS_RECEIPT_SCHEMA,
        METAR_PROCESS_EFFECT_CONTRACT_SHA256,
        "append_only_sqlite_triggers",
        TRIGGER_ROW_REPROOF_CONTRACT,
        "isolation",
    ),
)


_NOAA_SPEC = _StoreSpec(
    workload_id=NOAA_WORKLOAD_ID,
    consumer_id=NOAA_CONSUMER_ID,
    stream_id=NOAA_STREAM_ID,
    store_type=NoaaErrCompletionOutputStore,
    receipt_type=NoaaErrCompletionOutputReceipt,
    store_contract=NOAA_OUTPUT_STORE_CONTRACT,
    schema_version=NOAA_SCHEMA_VERSION,
    output_schema=NOAA_OUTPUT_DOCUMENT_SCHEMA,
    output_receipt_schema=NOAA_OUTPUT_RECEIPT_SCHEMA,
    process_receipt_schema=NOAA_PROCESS_RECEIPT_SCHEMA,
    effect_contract_sha256=NOAA_PROCESS_EFFECT_CONTRACT_SHA256,
    immutability_mode="append_only_sqlite_triggers",
    reproof_contract=TRIGGER_ROW_REPROOF_CONTRACT,
    isolation_key="process_isolation",
    store_kind="noaa_err_output",
    authority_output_key="desired_authority_sha256",
    authority_receipt_attribute="desired_authority_sha256",
)


def _raw_route_authority(workload_id: str) -> tuple[RawRecoveryBinding, str]:
    consumer = AUDIT_CONSUMER_CATALOG.get(workload_id)
    rule = ACTIVATION_RULES.get(workload_id)
    recovery = RECOVERY_BINDINGS.get(str(getattr(rule, "handler_id", "")))
    if (
        consumer is None
        or TERMINAL_RAW_CONSUMER_CATALOG.get(workload_id) != consumer
        or rule is None
        or rule.status is not ActivationStatus.ACTIVE_SHADOW_RAW
        or not rule.executable
        or not isinstance(recovery, RawRecoveryBinding)
        or recovery.workload_id != workload_id
        or recovery.required_roles != rule.required_roles
        or recovery.ledger_provider != rule.provider_id
        or recovery.maximum_transport_bytes != rule.maximum_transport_bytes
        or recovery.maximum_artifacts != rule.maximum_artifacts
    ):
        raise CompletionOutputArchiveIntegrityError(
            "raw-manifest archive route authority differs"
        )
    authority_sha256 = _digest(
        {
            "activation_rule": rule.as_dict(),
            "consumer_binding": consumer.as_dict(),
            "recovery_binding_sha256": recovery_binding_sha256(recovery),
            "schema": "stv3-raw-manifest-archive-route-authority-v1",
            "workload_id": workload_id,
        }
    )
    return recovery, authority_sha256


def _raw_store_spec(workload_id: str) -> _StoreSpec:
    recovery, route_authority_sha256 = _raw_route_authority(workload_id)
    consumer = AUDIT_CONSUMER_CATALOG[workload_id]
    return _StoreSpec(
        workload_id=workload_id,
        consumer_id=consumer.consumer_id,
        stream_id=consumer.stream_id,
        store_type=RawManifestAuditOutputStore,
        receipt_type=RawManifestAuditOutputReceipt,
        store_contract=RAW_AUDIT_OUTPUT_STORE_CONTRACT,
        schema_version=RAW_AUDIT_SCHEMA_VERSION,
        output_schema=RAW_AUDIT_OUTPUT_DOCUMENT_SCHEMA,
        output_receipt_schema=RAW_AUDIT_OUTPUT_RECEIPT_SCHEMA,
        process_receipt_schema=RAW_AUDIT_PROCESS_RECEIPT_SCHEMA,
        effect_contract_sha256=RAW_AUDIT_PROCESS_EFFECT_CONTRACT_SHA256,
        immutability_mode="append_only_sqlite_triggers",
        reproof_contract=(
            f"{RAW_MANIFEST_ROW_REPROOF_CONTRACT}:{route_authority_sha256}"
        ),
        isolation_key="isolation",
        store_kind="raw_manifest_audit_output",
        table_name="audit_outputs",
        row_decoder_name="_row",
        physical_store_scope_id=RAW_MANIFEST_PHYSICAL_STORE_SCOPE,
        recovery_binding=recovery,
        route_authority_sha256=route_authority_sha256,
    )


_RAW_SPECS = tuple(
    _raw_store_spec(workload_id)
    for workload_id in sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)
)
_ALL_SPECS = (*_SPECS, _NOAA_SPEC, *_RAW_SPECS)


def _raw_role_set_digests(
    *, recovery: RawRecoveryBinding, roles: tuple[Mapping[str, Any], ...]
) -> tuple[str, str]:
    binding_sha256 = recovery_binding_sha256(recovery)
    content_body = {
        "binding_sha256": binding_sha256,
        "contract": ROLE_SET_DIGEST_CONTRACT,
        "digest_kind": "content_role_set",
        "members": [
            {
                "content_sha256": role["content_sha256"],
                "raw_product": recovery.member(str(role["role"])).raw_product,
                "role": role["role"],
                "size_bytes": role["size_bytes"],
            }
            for role in roles
        ],
    }
    receipt_body = {
        "binding_sha256": binding_sha256,
        "contract": ROLE_SET_DIGEST_CONTRACT,
        "digest_kind": "receipt_role_set",
        "members": [
            {
                "content_sha256": role["content_sha256"],
                "receipt_id": role["receipt_id"],
                "receipt_sha256": role["receipt_sha256"],
                "role": role["role"],
                "size_bytes": role["size_bytes"],
            }
            for role in roles
        ],
    }
    return _digest(content_body), _digest(receipt_body)


def _catalog_row(
    workload_id: str, *, raw_manifest_audit: bool = False
) -> Mapping[str, Any]:
    snapshot = acquisition_catalog_snapshot()
    if snapshot.get("catalog_sha256") != CATALOG_SHA256:
        raise CompletionOutputArchiveIntegrityError(
            "acquisition catalog authority checksum differs"
        )
    rows = snapshot.get("workloads")
    if not isinstance(rows, list):
        raise CompletionOutputArchiveIntegrityError(
            "acquisition catalog workload inventory differs"
        )
    matches = [row for row in rows if row.get("workload_id") == workload_id]
    if len(matches) != 1:
        raise CompletionOutputArchiveIntegrityError(
            "stable completion workload is absent or ambiguous"
        )
    row = matches[0]
    cadence = row.get("cadence_seconds")
    retention_eligible = row.get(
        "durable_production_retention_eligible"
    )
    if (
        isinstance(cadence, bool)
        or not isinstance(cadence, int)
        or not 1 <= cadence <= 86_400
        or row.get("admission_eligible") is not False
        or type(retention_eligible) is not bool
    ):
        raise CompletionOutputArchiveIntegrityError(
            "stable completion workload catalog row differs"
        )
    if retention_eligible is False:
        consumer = TERMINAL_RAW_CONSUMER_CATALOG.get(workload_id)
        rule = ACTIVATION_RULES.get(workload_id)
        consumer_evidence = (
            consumer.as_dict() if consumer is not None else {}
        )
        if (
            raw_manifest_audit is not True
            or consumer is None
            or consumer.consumer_kind != "raw_manifest_audit"
            or consumer.transfer_mode != REFERENCE_TRANSFER_MODE
            or consumer_evidence.get("meteorological_admission") is not False
            or consumer_evidence.get("runtime_wiring") != "inert"
            or rule is None
            or rule.status is not ActivationStatus.ACTIVE_SHADOW_RAW
            or not rule.executable
            or rule.raw_capture_service_only is not True
        ):
            raise CompletionOutputArchiveIntegrityError(
                "non-retention-eligible completion row is not an inert "
                "reference-only raw-manifest audit"
            )
    return row


class _BoundStoreAdapter:
    def __init__(
        self,
        *,
        spec: _StoreSpec,
        store: object,
        clock: Callable[[], float],
    ) -> None:
        if type(store) is not spec.store_type:
            raise TypeError(f"{spec.workload_id} completion store type differs")
        if not callable(clock):
            raise TypeError("completion adapter clock is not callable")
        self.spec = spec
        self.store = store
        self._clock = clock
        self.source_path = _safe_store_path(store.path)
        self._source_file_witness = _source_file_witness(self.source_path)
        is_raw_manifest_audit = spec.store_kind == "raw_manifest_audit_output"
        catalog = _catalog_row(
            spec.workload_id,
            raw_manifest_audit=is_raw_manifest_audit,
        )
        consumer = TERMINAL_RAW_CONSUMER_CATALOG.get(spec.workload_id)
        if (
            consumer is None
            or consumer.workload_id != spec.workload_id
            or consumer.consumer_id != spec.consumer_id
            or consumer.stream_id != spec.stream_id
        ):
            raise CompletionOutputArchiveIntegrityError(
                "terminal completion consumer binding differs"
            )
        if spec.store_kind == "raw_manifest_audit_output":
            recovery, route_authority_sha256 = _raw_route_authority(
                spec.workload_id
            )
            if (
                recovery != spec.recovery_binding
                or route_authority_sha256 != spec.route_authority_sha256
            ):
                raise CompletionOutputArchiveIntegrityError(
                    "raw-manifest archive route authority drifted"
                )
        (
            limits_sha,
            self._native_store_instance_sha256,
            self._native_store_identity_sha256,
        ) = _native_store_authority(spec=spec, store=store)
        self.profile = CompletionOutputArchiveSourceProfile(
            workload_id=spec.workload_id,
            consumer_id=spec.consumer_id,
            stream_id=spec.stream_id,
            cadence_seconds=int(catalog["cadence_seconds"]),
            source_store_contract=spec.store_contract,
            source_store_schema_version=spec.schema_version,
            output_document_schema=spec.output_schema,
            output_receipt_schema=spec.output_receipt_schema,
            process_receipt_schema=spec.process_receipt_schema,
            source_limits_sha256=limits_sha,
            effect_contract_sha256=spec.effect_contract_sha256,
            acquisition_catalog_sha256=CATALOG_SHA256,
            consumer_binding_sha256=consumer.fingerprint,
            source_immutability_mode=spec.immutability_mode,
            source_row_reproof_contract=spec.reproof_contract,
        )
        # Windows source identities follow the filesystem's case-insensitive
        # alias semantics.  POSIX identities deliberately retain case.
        identity_path = os.path.normpath(str(self.source_path))
        if os.name == "nt":
            identity_path = os.path.normcase(identity_path)
        path_sha = hashlib.sha256(identity_path.encode("utf-8")).hexdigest()
        store_identity_body = {
            "limits_sha256": limits_sha,
            "file_witness": self._source_file_witness.as_dict(),
            "path_case_policy": SOURCE_PATH_IDENTITY_CASE_POLICY,
            "path_sha256": path_sha,
            "native_store_identity_sha256": self._native_store_identity_sha256,
            "native_store_instance_sha256": self._native_store_instance_sha256,
            "schema": SOURCE_STORE_IDENTITY_SCHEMA,
            "schema_version": spec.schema_version,
            "store_contract": spec.store_contract,
            "store_type": spec.store_type.__name__,
            "workload_id": spec.physical_store_scope_id or spec.workload_id,
        }
        store_identity = _digest(store_identity_body)
        resolver_sha = _digest(
            {
                "canonical_row_schema": SOURCE_ROW_CANONICAL_SCHEMA,
                "catalog_retention_evidence": {
                    "durable_production_retention_eligible": catalog[
                        "durable_production_retention_eligible"
                    ],
                    "production_retention_claimed": False,
                    "raw_capture_service_only": bool(
                        getattr(
                            ACTIVATION_RULES.get(spec.workload_id),
                            "raw_capture_service_only",
                            False,
                        )
                    ),
                    "source_payload_materialized": False,
                },
                "profile_sha256": self.profile.fingerprint,
                "reproof_contract": spec.reproof_contract,
                "schema": SOURCE_RESOLVER_SCHEMA,
                "store_type": spec.store_type.__name__,
            }
        )
        self.descriptor = CompletionOutputArchiveAdapterDescriptor(
            workload_id=spec.workload_id,
            source_store_type=spec.store_type.__name__,
            source_profile_sha256=self.profile.fingerprint,
            source_store_identity_sha256=store_identity,
            native_store_identity_sha256=self._native_store_identity_sha256,
            native_store_instance_sha256=self._native_store_instance_sha256,
            resolver_sha256=resolver_sha,
            durable_production_retention_eligible=catalog[
                "durable_production_retention_eligible"
            ],
            production_retention_claimed=False,
            source_payload_materialized=False,
            raw_capture_service_only=bool(
                getattr(
                    ACTIVATION_RULES.get(spec.workload_id),
                    "raw_capture_service_only",
                    False,
                )
            ),
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(workload_id={self.spec.workload_id!r}, "
            f"profile_sha256={self.profile.fingerprint!r})"
        )

    def _assert_source_file_witness(self) -> None:
        current_path = _safe_store_path(self.store.path)
        if current_path != self.source_path:
            raise CompletionOutputArchiveIntegrityError(
                "completion source path changed after authority construction"
            )
        current = _source_file_witness(current_path)
        if not hmac.compare_digest(
            current.fingerprint, self._source_file_witness.fingerprint
        ):
            raise CompletionOutputArchiveIntegrityError(
                "completion source file instance changed"
            )

    def _assert_native_store_identity(self) -> None:
        try:
            current = _exact_sha256(
                self.store.assert_instance_held(
                    self._native_store_identity_sha256
                ),
                "live completion source identity SHA-256",
            )
        except CompletionOutputArchiveIntegrityError:
            raise
        except Exception as exc:
            raise CompletionOutputArchiveIntegrityError(
                "live completion source identity reproof failed"
            ) from exc
        if not hmac.compare_digest(
            current, self._native_store_identity_sha256
        ):
            raise CompletionOutputArchiveIntegrityError(
                "live completion source identity differs from bound authority"
            )

    def _validate_store_database(self, database: sqlite3.Connection) -> str:
        if self.spec.store_kind == "completion_output":
            instance_sha256 = self.store._validate_schema(database, full=True)
        elif self.spec.store_kind == "noaa_err_output":
            instance_sha256 = self.store._validate_schema(database, full=True)
        elif self.spec.store_kind == "raw_manifest_audit_output":
            instance_sha256 = self.store._validate(database, True)
        else:
            raise CompletionOutputArchiveIntegrityError(
                "completion source store kind is outside the closed authority"
            )
        instance_sha256 = _exact_sha256(
            instance_sha256, "live completion source instance SHA-256"
        )
        if not hmac.compare_digest(
            instance_sha256, self._native_store_instance_sha256
        ):
            raise CompletionOutputArchiveIntegrityError(
                "live completion source instance differs from bound authority"
            )
        return instance_sha256

    def _source_row(
        self, database: sqlite3.Connection, event_id: str
    ) -> sqlite3.Row | None:
        if self.spec.store_kind == "raw_manifest_audit_output":
            return database.execute(
                "SELECT * FROM audit_outputs WHERE event_id=? AND workload_id=?",
                (event_id, self.spec.workload_id),
            ).fetchone()
        return database.execute(
            "SELECT * FROM completion_outputs WHERE event_id=?",
            (event_id,),
        ).fetchone()

    @staticmethod
    def _validate_physical_receipt_projection(
        row: sqlite3.Row, receipt: object
    ) -> None:
        names = set(row.keys())
        output = getattr(receipt, "output", None)
        if not isinstance(output, Mapping):
            raise CompletionOutputArchiveIntegrityError(
                "completion source receipt has no output mapping"
            )
        body = bytes(row["output_json"])
        checks: list[tuple[object, object]] = [
            (row["event_id"], getattr(receipt, "event_id", None)),
            (row["output_sha256"], getattr(receipt, "output_sha256", None)),
            (row["receipt_sha256"], getattr(receipt, "receipt_sha256", None)),
            (
                row["process_receipt_sha256"],
                getattr(receipt, "process_receipt_sha256", None),
            ),
            (float(row["created_at"]), float(getattr(receipt, "created_at", -1.0))),
            (hashlib.sha256(body).hexdigest(), getattr(receipt, "output_sha256", None)),
            (_canonical(dict(output)), body),
        ]
        for column, attribute in (
            ("event_sha256", "event_sha256"),
            ("producer_receipt_sha256", "producer_receipt_sha256"),
            ("workload_id", "workload_id"),
            ("consumer_id", "consumer_id"),
            ("stream_id", "stream_id"),
            ("desired_state_sha256", "desired_state_sha256"),
            ("authority_sha256", "authority_sha256"),
            ("capture_id", "capture_id"),
            ("capture_manifest_sha256", "capture_manifest_sha256"),
            ("isolation_authority_sha256", "isolation_authority_sha256"),
            ("isolation_limits_sha256", "isolation_limits_sha256"),
        ):
            if column in names:
                expected = getattr(receipt, attribute, None)
                if expected is None and column in {
                    "workload_id",
                    "consumer_id",
                    "stream_id",
                }:
                    expected = output.get(column)
                checks.append((row[column], expected))
        if any(actual != expected for actual, expected in checks):
            raise CompletionOutputArchiveIntegrityError(
                "completion source row differs from its native receipt projection"
            )

    @contextmanager
    def _snapshot(self, event_id: str) -> Iterator[tuple[object, bytes]]:
        with self.store._lock:
            database: sqlite3.Connection | None = None
            try:
                self._assert_source_file_witness()
                self._assert_native_store_identity()
                database = self.store._connect()
                database.execute("BEGIN IMMEDIATE")
                self._assert_source_file_witness()
                self._validate_store_database(database)
                row = self._source_row(database, event_id)
                if row is None:
                    raise CompletionOutputArchiveIntegrityError(
                        "completion source row is unavailable"
                    )
                receipt = getattr(self.store, self.spec.row_decoder_name)(row)
                if type(receipt) is not self.spec.receipt_type:
                    raise CompletionOutputArchiveIntegrityError(
                        "completion source receipt type differs"
                    )
                self._validate_physical_receipt_projection(row, receipt)
                row_bytes = _physical_row_bytes(row=row, profile=self.profile)
                self._assert_source_file_witness()
            except CompletionOutputArchiveIntegrityError:
                if database is not None:
                    if database.in_transaction:
                        database.rollback()
                    database.close()
                raise
            except sqlite3.DatabaseError as exc:
                if database is not None:
                    if database.in_transaction:
                        database.rollback()
                    database.close()
                raise CompletionOutputArchiveIntegrityError(
                    "completion source database reproof failed"
                ) from exc
            except Exception as exc:
                if database is not None:
                    if database.in_transaction:
                        database.rollback()
                    database.close()
                raise CompletionOutputArchiveIntegrityError(
                    "completion source store reproof failed"
                ) from exc
            try:
                # Exceptions from the archive commit must pass through exactly;
                # they are not source-reproof failures.
                yield receipt, row_bytes
            finally:
                assert database is not None
                if database.in_transaction:
                    database.rollback()
                database.close()

    def _validate_receipt(self, receipt: object) -> tuple[Mapping[str, Any], int]:
        output = receipt.output
        if not isinstance(output, Mapping):
            raise CompletionOutputArchiveIntegrityError(
                "completion output document type differs"
            )
        if output.get("schema") != self.spec.output_schema:
            raise CompletionOutputArchiveIntegrityError(
                "completion output schema differs from source profile"
            )
        if output.get("consumer_id") != self.spec.consumer_id:
            raise CompletionOutputArchiveIntegrityError(
                "completion output consumer differs from source profile"
            )
        if output.get("stream_id") != self.spec.stream_id:
            raise CompletionOutputArchiveIntegrityError(
                "completion output stream differs from source profile"
            )
        if "workload_id" in output and output["workload_id"] != self.spec.workload_id:
            raise CompletionOutputArchiveIntegrityError(
                "completion output workload differs from source profile"
            )
        event = output.get("event")
        authority = output.get("authority")
        isolation = output.get(self.spec.isolation_key)
        if not all(isinstance(value, Mapping) for value in (event, authority, isolation)):
            raise CompletionOutputArchiveIntegrityError(
                "completion output authority shape differs"
            )
        sequence = event.get("stream_sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            raise CompletionOutputArchiveIntegrityError(
                "completion output stream sequence differs"
            )
        checks = (
            (event.get("event_id"), receipt.event_id),
            (event.get("event_sha256"), receipt.event_sha256),
            (event.get("producer_receipt_sha256"), receipt.producer_receipt_sha256),
            (authority.get("desired_state_sha256"), receipt.desired_state_sha256),
            (
                authority.get(self.spec.authority_output_key),
                getattr(receipt, self.spec.authority_receipt_attribute),
            ),
            (isolation.get("effect_contract_sha256"), self.spec.effect_contract_sha256),
        )
        if any(actual != expected for actual, expected in checks):
            raise CompletionOutputArchiveIntegrityError(
                "completion receipt differs from its exact output document"
            )
        if self.spec.store_kind == "noaa_err_output":
            self._validate_noaa_receipt(receipt, output, event, authority, isolation)
        elif self.spec.store_kind == "raw_manifest_audit_output":
            self._validate_raw_manifest_receipt(
                receipt, output, event, authority, isolation
            )
        return output, sequence

    def _validate_noaa_receipt(
        self,
        receipt: NoaaErrCompletionOutputReceipt,
        output: Mapping[str, Any],
        event: Mapping[str, Any],
        authority: Mapping[str, Any],
        isolation: Mapping[str, Any],
    ) -> None:
        try:
            region = NoaaErrRegionAuthority.from_mapping(
                output.get("region_authority")
            )
        except Exception as exc:
            raise CompletionOutputArchiveIntegrityError(
                "NOAA region authority reproof failed"
            ) from exc
        decoder = output.get("decoder")
        shadow = output.get("shadow")
        input_reference = output.get("input")
        if not all(
            isinstance(value, Mapping)
            for value in (decoder, shadow, input_reference)
        ):
            raise CompletionOutputArchiveIntegrityError(
                "NOAA completion evidence shape differs"
            )
        checks = (
            (region.fingerprint, receipt.region_authority_sha256),
            (region.acquisition_job_id, event.get("acquisition_job_id")),
            (region.desired_state_sha256, authority.get("desired_state_sha256")),
            (isolation.get("authority_sha256"), receipt.isolation_authority_sha256),
            (isolation.get("limits_sha256"), receipt.isolation_limits_sha256),
            (shadow.get("content_sha256"), receipt.candidate_content_sha256),
            (decoder.get("run_id"), receipt.decoder_run_id),
            (decoder.get("artifact_sha256"), shadow.get("content_sha256")),
        )
        if any(actual != expected for actual, expected in checks):
            raise CompletionOutputArchiveIntegrityError(
                "NOAA completion region, process or artifact receipt differs"
            )

    def _validate_raw_manifest_receipt(
        self,
        receipt: RawManifestAuditOutputReceipt,
        output: Mapping[str, Any],
        event: Mapping[str, Any],
        authority: Mapping[str, Any],
        isolation: Mapping[str, Any],
    ) -> None:
        recovery = self.spec.recovery_binding
        rule = ACTIVATION_RULES.get(self.spec.workload_id)
        audit = output.get("audit")
        raw_roles = audit.get("roles") if isinstance(audit, Mapping) else None
        if (
            not isinstance(recovery, RawRecoveryBinding)
            or rule is None
            or rule.status is not ActivationStatus.ACTIVE_SHADOW_RAW
            or not isinstance(audit, Mapping)
            or not isinstance(raw_roles, list)
            or not recovery.minimum_artifacts
            <= len(raw_roles)
            <= recovery.maximum_artifacts
            or any(not isinstance(raw_role, Mapping) for raw_role in raw_roles)
        ):
            raise CompletionOutputArchiveIntegrityError(
                "raw-manifest completion route shape differs"
            )
        actual_role_names = tuple(raw_role.get("role") for raw_role in raw_roles)
        try:
            materialized_members = recovery.members_for_roles(actual_role_names)
        except CaptureRecoveryIntegrityError as exc:
            raise CompletionOutputArchiveIntegrityError(
                "raw-manifest completion dynamic role set differs"
            ) from exc
        expected_role_names = tuple(
            sorted(member.role for member in materialized_members)
        )
        if actual_role_names != expected_role_names:
            raise CompletionOutputArchiveIntegrityError(
                "raw-manifest completion role ordering differs"
            )
        members_by_role = {
            member.role: member for member in materialized_members
        }
        roles: list[Mapping[str, Any]] = []
        for raw_role, role_name in zip(raw_roles, expected_role_names, strict=True):
            if raw_role.get("role") != role_name:
                raise CompletionOutputArchiveIntegrityError(
                    "raw-manifest completion role ordering differs"
                )
            member = members_by_role[role_name]
            size = raw_role.get("size_bytes")
            minimum_size = (
                0
                if member.provenance_kind
                in {
                    "eumetsat_mtg_li_search_v1",
                    "eumetsat_mtg_li_netcdf_v1",
                    "noaa_goes_glm_listing_v1",
                    "noaa_goes_glm_netcdf_v1",
                }
                else 1
            )
            if (
                raw_role.get("provider") != recovery.raw_provider
                or raw_role.get("product") != member.raw_product
                or raw_role.get("content_type") not in member.allowed_content_types
                or isinstance(size, bool)
                or not isinstance(size, int)
                or not minimum_size <= size <= member.maximum_transport_bytes
            ):
                raise CompletionOutputArchiveIntegrityError(
                    "raw-manifest completion member authority differs"
                )
            roles.append(raw_role)
        receipt_ids = [str(role.get("receipt_id")) for role in roles]
        total_bytes = sum(int(role["size_bytes"]) for role in roles)
        content_sha256, receipt_sha256 = _raw_role_set_digests(
            recovery=recovery,
            roles=tuple(roles),
        )
        checks = (
            (receipt.workload_id, self.spec.workload_id),
            (receipt.consumer_id, self.spec.consumer_id),
            (receipt.stream_id, self.spec.stream_id),
            (audit.get("provider_id"), rule.provider_id),
            (audit.get("product_id"), rule.product_id),
            (audit.get("total_bytes"), total_bytes),
            (audit.get("role_set_content_sha256"), content_sha256),
            (audit.get("role_set_receipt_sha256"), receipt_sha256),
            (audit.get("capture_id"), receipt.capture_id),
            (
                audit.get("capture_manifest_sha256"),
                receipt.capture_manifest_sha256,
            ),
            (authority.get("desired_state_sha256"), receipt.desired_state_sha256),
            (authority.get("authority_sha256"), receipt.authority_sha256),
            (isolation.get("authority_sha256"), receipt.isolation_authority_sha256),
            (isolation.get("limits_sha256"), receipt.isolation_limits_sha256),
            (event.get("event_id"), receipt.event_id),
        )
        if (
            len(set(receipt_ids)) != len(receipt_ids)
            or total_bytes > recovery.maximum_transport_bytes
            or any(actual != expected for actual, expected in checks)
        ):
            raise CompletionOutputArchiveIntegrityError(
                "raw-manifest completion receipt or role-set digest differs"
            )

    def build_record(self, event_id: str) -> CompletionOutputArchiveRecord:
        with self._snapshot(event_id) as (receipt, source_row):
            output, sequence = self._validate_receipt(receipt)
            record = CompletionOutputArchiveRecord.from_output(
                source_profile_sha256=self.profile.fingerprint,
                event_id=receipt.event_id,
                event_sha256=receipt.event_sha256,
                producer_receipt_sha256=receipt.producer_receipt_sha256,
                workload_id=self.spec.workload_id,
                consumer_id=self.spec.consumer_id,
                stream_id=self.spec.stream_id,
                stream_sequence=sequence,
                created_at=receipt.created_at,
                output_receipt_sha256=receipt.receipt_sha256,
                process_receipt_sha256=receipt.process_receipt_sha256,
                source_row_sha256=hashlib.sha256(source_row).hexdigest(),
                source_row_bytes=len(source_row),
                output=output,
            )
            if not hmac.compare_digest(record.output_sha256, receipt.output_sha256):
                raise CompletionOutputArchiveIntegrityError(
                    "archive canonical output differs from the source output bytes"
                )
            return record

    def resolver(
        self, record: CompletionOutputArchiveRecord
    ) -> ContextManager[CompletionOutputArchiveSourceRowReproof]:
        @contextmanager
        def reproof() -> Iterator[CompletionOutputArchiveSourceRowReproof]:
            with self._snapshot(record.event_id) as (receipt, source_row):
                output, sequence = self._validate_receipt(receipt)
                if (
                    receipt.event_sha256 != record.event_sha256
                    or receipt.producer_receipt_sha256
                    != record.producer_receipt_sha256
                    or receipt.receipt_sha256 != record.output_receipt_sha256
                    or receipt.process_receipt_sha256
                    != record.process_receipt_sha256
                    or receipt.output_sha256 != record.output_sha256
                    or sequence != record.stream_sequence
                    or _canonical(dict(output)) != record.output_json
                ):
                    raise CompletionOutputArchiveIntegrityError(
                        "seal-time completion receipt differs from archive record"
                    )
                yield CompletionOutputArchiveSourceRowReproof(
                    source_profile_sha256=self.profile.fingerprint,
                    event_id=record.event_id,
                    source_row_sha256=hashlib.sha256(source_row).hexdigest(),
                    source_row_bytes=len(source_row),
                    reproof_contract=self.spec.reproof_contract,
                    verified_at=float(self._clock()),
                )

        return reproof()


class StableCompletionOutputArchiveAdapters:
    """Closed, store-bound and deliberately unwired archive authority."""

    def __init__(self, adapters: tuple[_BoundStoreAdapter, ...]) -> None:
        inventory = {item.spec.workload_id for item in adapters}
        if len(adapters) == 4 and inventory == set(STABLE_ARCHIVE_WORKLOAD_IDS):
            catalog_id = ADAPTER_CATALOG_ID
            authority_id = ADAPTER_AUTHORITY_ID
            contract = ADAPTER_CONTRACT
            resolver_namespace = "stable-source-row"
        elif (
            len(adapters) == len(ALL_ARCHIVE_WORKLOAD_IDS)
            and inventory == set(ALL_ARCHIVE_WORKLOAD_IDS)
        ):
            if (
                set(TERMINAL_RAW_CONSUMER_CATALOG) != set(ALL_ARCHIVE_WORKLOAD_IDS)
                or set(AUDIT_CONSUMER_CATALOG)
                != set(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)
            ):
                raise CompletionOutputArchiveIntegrityError(
                    "all-terminal completion catalog inventory differs"
                )
            catalog_id = ALL_ADAPTER_CATALOG_ID
            authority_id = ALL_ADAPTER_AUTHORITY_ID
            contract = ALL_ADAPTER_CONTRACT
            resolver_namespace = "all-terminal-source-row"
        else:
            raise CompletionOutputArchiveIntegrityError(
                "completion archive adapter inventory differs"
            )
        by_path: dict[Path, list[_BoundStoreAdapter]] = {}
        for item in adapters:
            by_path.setdefault(item.source_path, []).append(item)
        for group in by_path.values():
            if len(group) == 1:
                continue
            if (
                {item.spec.workload_id for item in group}
                != set(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)
                or len({id(item.store) for item in group}) != 1
                or len(
                    {
                        item.descriptor.source_store_identity_sha256
                        for item in group
                    }
                )
                != 1
            ):
                raise CompletionOutputArchiveIntegrityError(
                    "completion source stores overlap outside the shared raw authority"
                )
        self._adapters = MappingProxyType(
            {item.spec.workload_id: item for item in adapters}
        )
        self._contract = contract
        self.source_catalog = CompletionOutputArchiveSourceCatalog(
            catalog_id=catalog_id,
            profiles=tuple(item.profile for item in adapters),
        )
        bindings = tuple(
            CompletionOutputArchiveSourceReproofBinding(
                source_profile_sha256=item.profile.fingerprint,
                resolver_id=(
                    f"{resolver_namespace}:{item.spec.workload_id}:v2"
                ),
                resolver_sha256=item.descriptor.resolver_sha256,
                source_store_identity_sha256=(
                    item.descriptor.source_store_identity_sha256
                ),
                reproof_contract=item.spec.reproof_contract,
                resolver=item.resolver,
            )
            for item in adapters
        )
        self.source_reproof_authority = (
            CompletionOutputArchiveSourceReproofAuthority(
                authority_id=authority_id,
                bindings=bindings,
            )
        )
        self.fingerprint = _digest(
            {
                "adapters": [
                    item.descriptor.as_dict()
                    for item in sorted(adapters, key=lambda value: value.spec.workload_id)
                ],
                "contract": contract,
                "source_catalog_sha256": self.source_catalog.fingerprint,
                "source_reproof_authority_sha256": (
                    self.source_reproof_authority.fingerprint
                ),
            }
        )

    def __repr__(self) -> str:
        return (
            "StableCompletionOutputArchiveAdapters("
            f"contract={self._contract!r}, fingerprint={self.fingerprint!r}, "
            "runtime_wiring='inert')"
        )

    @property
    def profiles(self) -> Mapping[str, CompletionOutputArchiveSourceProfile]:
        return MappingProxyType(
            {key: value.profile for key, value in self._adapters.items()}
        )

    @property
    def descriptors(self) -> Mapping[str, CompletionOutputArchiveAdapterDescriptor]:
        return MappingProxyType(
            {key: value.descriptor for key, value in self._adapters.items()}
        )

    def profile_for(self, workload_id: str) -> CompletionOutputArchiveSourceProfile:
        adapter = self._adapters.get(workload_id)
        if adapter is None:
            raise CompletionOutputArchiveIntegrityError(
                "workload is outside the stable completion adapter catalog"
            )
        return adapter.profile

    def build_record(
        self, workload_id: str, event_id: str
    ) -> CompletionOutputArchiveRecord:
        adapter = self._adapters.get(workload_id)
        if adapter is None:
            raise CompletionOutputArchiveIntegrityError(
                "workload is outside the stable completion adapter catalog"
            )
        return adapter.build_record(event_id)

    def open_archive_store(
        self,
        path: str | Path,
        *,
        limits: CompletionOutputArchiveLimits,
        hot_policy: CompletionOutputHotWindowPolicy,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> CompletionOutputArchiveStore:
        return CompletionOutputArchiveStore(
            path,
            source_catalog=self.source_catalog,
            source_reproof_authority=self.source_reproof_authority,
            expected_source_reproof_authority_sha256=(
                self.source_reproof_authority.fingerprint
            ),
            limits=limits,
            hot_policy=hot_policy,
            disjoint_paths=tuple(
                sorted(
                    {adapter.source_path for adapter in self._adapters.values()},
                    key=str,
                )
            ),
            clock=clock,
            fault_injector=fault_injector,
        )

    def _require_archive(self, archive: object) -> CompletionOutputArchiveStore:
        if type(archive) is not CompletionOutputArchiveStore:
            raise TypeError("completion archive store type differs")
        if (
            archive.source_catalog.fingerprint != self.source_catalog.fingerprint
            or archive.source_reproof_authority.fingerprint
            != self.source_reproof_authority.fingerprint
        ):
            raise CompletionOutputArchiveIntegrityError(
                "completion archive authority differs from its adapters"
            )
        return archive

    def seal(
        self,
        archive: CompletionOutputArchiveStore,
        workload_id: str,
        event_id: str,
    ) -> CompletionOutputArchiveSeal:
        exact_archive = self._require_archive(archive)
        return exact_archive.seal(self.build_record(workload_id, event_id))

    def load_archived(
        self,
        archive: CompletionOutputArchiveStore,
        workload_id: str,
        event_id: str,
    ) -> CompletionOutputArchiveEntry | None:
        exact_archive = self._require_archive(archive)
        profile = self.profile_for(workload_id)
        return exact_archive.load(profile.fingerprint, event_id)

    def verify_archived_for_ack(
        self,
        archive: CompletionOutputArchiveStore,
        workload_id: str,
        event_id: str,
        *,
        output_sha256: str,
        output_receipt_sha256: str,
        process_receipt_sha256: str,
        record_sha256: str | None = None,
    ) -> CompletionOutputArchiveEntry:
        exact_archive = self._require_archive(archive)
        profile = self.profile_for(workload_id)
        return exact_archive.verify_for_ack(
            profile.fingerprint,
            event_id,
            output_sha256=output_sha256,
            output_receipt_sha256=output_receipt_sha256,
            process_receipt_sha256=process_receipt_sha256,
            record_sha256=record_sha256,
        )

    def diagnostics(self) -> dict[str, object]:
        return {
            "adapter_authority_sha256": self.fingerprint,
            "contract": self._contract,
            "runtime_wiring": "inert",
            "source_catalog_sha256": self.source_catalog.fingerprint,
            "source_reproof_authority_sha256": (
                self.source_reproof_authority.fingerprint
            ),
            "workloads": sorted(self._adapters),
        }


def build_stable_completion_output_archive_adapters(
    *,
    dwd_store: DwdRvCompletionOutputStore,
    knmi_store: KnmiRtcorCompletionOutputStore,
    met_office_store: MetOfficeRateCompletionOutputStore,
    metar_store: MetarGroundCompletionOutputStore,
    clock: Callable[[], float] = time.time,
) -> StableCompletionOutputArchiveAdapters:
    stores = {
        DWD_WORKLOAD_ID: dwd_store,
        KNMI_WORKLOAD_ID: knmi_store,
        MET_OFFICE_WORKLOAD_ID: met_office_store,
        METAR_WORKLOAD_ID: metar_store,
    }
    return StableCompletionOutputArchiveAdapters(
        tuple(
            _BoundStoreAdapter(spec=spec, store=stores[spec.workload_id], clock=clock)
            for spec in _SPECS
        )
    )


def build_all_completion_output_archive_adapters(
    *,
    dwd_store: DwdRvCompletionOutputStore,
    knmi_store: KnmiRtcorCompletionOutputStore,
    met_office_store: MetOfficeRateCompletionOutputStore,
    metar_store: MetarGroundCompletionOutputStore,
    noaa_store: NoaaErrCompletionOutputStore,
    raw_manifest_store: RawManifestAuditOutputStore,
    clock: Callable[[], float] = time.time,
) -> StableCompletionOutputArchiveAdapters:
    """Bind the exact current terminal catalogue without runtime wiring."""

    stores = {
        DWD_WORKLOAD_ID: dwd_store,
        KNMI_WORKLOAD_ID: knmi_store,
        MET_OFFICE_WORKLOAD_ID: met_office_store,
        METAR_WORKLOAD_ID: metar_store,
        NOAA_WORKLOAD_ID: noaa_store,
        **{
            workload_id: raw_manifest_store
            for workload_id in RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS
        },
    }
    return StableCompletionOutputArchiveAdapters(
        tuple(
            _BoundStoreAdapter(
                spec=spec,
                store=stores[spec.workload_id],
                clock=clock,
            )
            for spec in _ALL_SPECS
        )
    )


__all__ = [
    "ADAPTER_AUTHORITY_ID",
    "ADAPTER_CATALOG_ID",
    "ADAPTER_CONTRACT",
    "ALL_ADAPTER_AUTHORITY_ID",
    "ALL_ADAPTER_CATALOG_ID",
    "ALL_ADAPTER_CONTRACT",
    "ALL_ARCHIVE_WORKLOAD_IDS",
    "ADAPTER_DESCRIPTOR_SCHEMA",
    "DWD_ROW_REPROOF_CONTRACT",
    "RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS",
    "RAW_MANIFEST_PHYSICAL_STORE_SCOPE",
    "RAW_MANIFEST_ROW_REPROOF_CONTRACT",
    "SOURCE_ROW_CANONICAL_SCHEMA",
    "SOURCE_RESOLVER_SCHEMA",
    "SOURCE_STORE_IDENTITY_SCHEMA",
    "SOURCE_PATH_IDENTITY_CASE_POLICY",
    "SOURCE_FILE_WITNESS_SCHEMA",
    "STABLE_ARCHIVE_WORKLOAD_IDS",
    "TRIGGER_ROW_REPROOF_CONTRACT",
    "CompletionOutputArchiveAdapterDescriptor",
    "StableCompletionOutputArchiveAdapters",
    "build_all_completion_output_archive_adapters",
    "build_stable_completion_output_archive_adapters",
]
