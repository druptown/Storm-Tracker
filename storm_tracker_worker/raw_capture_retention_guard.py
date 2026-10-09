"""Read-only, fail-closed capacity guard for continuous RAW capture.

The guard is deliberately not a reclamation executor.  It proves bounded
headroom and storage identities while the existing worker-first ownership
lease is held.  Exhaustion, drift, or an unverifiable store blocks the next
capture cycle; no file, receipt, reservation, or source byte is removed.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import stat
import threading
from typing import Any, Final

from .raw_artifacts import (
    raw_catalog_cycle_reserve_bytes,
    raw_catalog_main_database_max_bytes,
)
from .raw_capacity_ledger import (
    capacity_ledger_catalog_cycle_reserve_bytes,
    capacity_ledger_catalog_main_database_max_bytes,
)


CONTRACT: Final = "stv3-worker-raw-capture-retention-guard-v2"
CAPACITY_POLICY: Final = (
    "config_bound_optional_event_envelope_reserved_fail_closed_v2"
)
RECLAMATION_POLICY: Final = "offline_five_role_only_never_live_apply_v1"
MAX_SIGNED_INTEGER: Final = (1 << 63) - 1
MAX_PATH_COMPONENTS: Final = 64
MAX_DIAGNOSTIC_BYTES: Final = 12 * 1024
MIN_HOST_OPERATIONAL_FREE_RESERVE_BYTES: Final = 2 * 1024 * 1024 * 1024
WARNING_CYCLE_MULTIPLIER: Final = 2

RETENTION_GUARD_BLOCKING_ALARM_CODES: Final = frozenset({
    "cadence_catalog_capacity_exhausted",
    "cadence_capture_capacity_exhausted",
    "cadence_cycle_byte_capacity_exhausted",
    "cadence_cycle_entry_capacity_exhausted",
    "cadence_ledger_catalog_capacity_exhausted",
    "cadence_reservation_capacity_exhausted",
    "event_capacity_exhausted",
    "event_capacity_claim_not_available",
    "exclusive_process_owner_not_held",
    "filesystem_envelope_capacity_exhausted",
    "path_identity_reproof_failed",
    "retention_guard_not_inspected",
    "storage_integrity_failed",
})
_BLOCKING_ALARMS: Final = RETENTION_GUARD_BLOCKING_ALARM_CODES
_ALARM_PRIORITY: Final = (
    "exclusive_process_owner_not_held",
    "path_identity_reproof_failed",
    "storage_integrity_failed",
    "filesystem_envelope_capacity_exhausted",
    "cadence_cycle_byte_capacity_exhausted",
    "cadence_cycle_entry_capacity_exhausted",
    "cadence_capture_capacity_exhausted",
    "cadence_reservation_capacity_exhausted",
    "cadence_catalog_capacity_exhausted",
    "cadence_ledger_catalog_capacity_exhausted",
    "event_capacity_exhausted",
    "event_capacity_claim_not_available",
    "filesystem_envelope_capacity_low",
    "cadence_cycle_byte_capacity_low",
    "cadence_cycle_entry_capacity_low",
    "cadence_capture_capacity_low",
    "cadence_reservation_capacity_low",
    "cadence_catalog_capacity_low",
    "cadence_ledger_catalog_capacity_low",
    "event_capacity_low",
    "event_transport_degraded",
    "retention_guard_not_inspected",
)
_KNOWN_ALARMS: Final = frozenset(_ALARM_PRIORITY)


class RawCaptureRetentionGuardError(RuntimeError):
    """A bounded retention/capacity proof failed closed."""


class RawCaptureRetentionGuardBlocked(RawCaptureRetentionGuardError):
    """The next RAW capture cycle is not safely admissible."""

    def __init__(self, reason_code: str) -> None:
        """Keep only a fixed, capability-free blocking reason."""

        if reason_code not in RETENTION_GUARD_BLOCKING_ALARM_CODES:
            reason_code = "storage_integrity_failed"
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class RawCaptureRetentionGuardConfig:
    """Immutable storage envelope consumed by the read-only guard."""

    cadence_raw_root: Path
    cadence_ledger_path: Path
    event_control_root: Path
    event_raw_root: Path
    event_capacity_claimed: bool
    configured_persistent_max_bytes: int
    configured_raw_object_max_entries: int
    cadence_ledger_max_bytes: int
    cadence_ledger_max_entries: int
    cadence_ledger_max_reservations: int
    cadence_ledger_catalog_max_bytes: int
    cadence_raw_max_bytes: int
    cadence_raw_max_entries: int
    cadence_raw_max_receipts: int
    cadence_raw_max_captures: int
    cadence_raw_catalog_max_bytes: int
    cadence_cycle_reserve_bytes: int
    cadence_cycle_reserve_entries: int
    cadence_cycle_reserve_captures: int
    event_payload_reserve_bytes: int
    event_control_receipt_max_bytes: int
    event_control_intake_reserve: int
    event_control_receipt_capacity: int
    event_queue_capacity: int
    event_retained_payload_max_bytes: int
    event_retained_payload_max_entries: int
    host_operational_free_reserve_bytes: int

    def __post_init__(self) -> None:
        if type(self.event_capacity_claimed) is not bool:
            raise ValueError("event_capacity_claimed_invalid")
        paths = (
            (self.cadence_raw_root, "directory"),
            (self.cadence_ledger_path, "file"),
            (self.event_control_root, "directory"),
            (self.event_raw_root, "directory"),
        )
        normalized: list[Path] = []
        for supplied, _kind in paths:
            if not isinstance(supplied, Path) or not supplied.is_absolute():
                raise ValueError("retention_guard_path_invalid")
            if supplied == Path(supplied.anchor) or len(supplied.parts) > MAX_PATH_COMPONENTS:
                raise ValueError("retention_guard_path_invalid")
            canonical = Path(os.path.abspath(os.path.normpath(supplied)))
            if os.path.normcase(str(canonical)) != os.path.normcase(str(supplied)):
                raise ValueError("retention_guard_path_not_canonical")
            normalized.append(canonical)
        if len({os.path.normcase(str(path)) for path in normalized}) != len(normalized):
            raise ValueError("retention_guard_path_alias")
        directory_roots = (normalized[0], normalized[2], normalized[3])
        for index, left in enumerate(directory_roots):
            for right in directory_roots[index + 1 :]:
                if _paths_overlap(left, right):
                    raise ValueError("retention_guard_path_overlap")
        if any(_paths_overlap(root, normalized[1]) for root in directory_roots):
            raise ValueError("retention_guard_path_overlap")
        for name in (
            "configured_persistent_max_bytes",
            "configured_raw_object_max_entries",
            "cadence_ledger_max_bytes",
            "cadence_ledger_max_entries",
            "cadence_ledger_max_reservations",
            "cadence_ledger_catalog_max_bytes",
            "cadence_raw_max_bytes",
            "cadence_raw_max_entries",
            "cadence_raw_max_receipts",
            "cadence_raw_max_captures",
            "cadence_raw_catalog_max_bytes",
            "cadence_cycle_reserve_bytes",
            "cadence_cycle_reserve_entries",
            "cadence_cycle_reserve_captures",
            "event_payload_reserve_bytes",
            "event_control_receipt_max_bytes",
            "event_control_intake_reserve",
            "event_control_receipt_capacity",
            "event_queue_capacity",
            "event_retained_payload_max_bytes",
            "event_retained_payload_max_entries",
            "host_operational_free_reserve_bytes",
        ):
            _positive_integer(getattr(self, name), name)
        if (
            self.host_operational_free_reserve_bytes
            < MIN_HOST_OPERATIONAL_FREE_RESERVE_BYTES
        ):
            raise ValueError(
                "host_operational_free_reserve_bytes_invalid"
            )
        event_control_max_bytes = (
            self.event_control_receipt_capacity
            * self.event_control_receipt_max_bytes
        )
        minimum_persistent_envelope = (
            self.cadence_raw_max_bytes
            + self.cadence_raw_catalog_max_bytes
            + self.cadence_ledger_catalog_max_bytes
        )
        minimum_raw_entries = self.cadence_raw_max_entries
        if self.event_capacity_claimed:
            minimum_persistent_envelope += (
                self.event_retained_payload_max_bytes
                + event_control_max_bytes
            )
            minimum_raw_entries += self.event_retained_payload_max_entries
        if (
            self.cadence_cycle_reserve_bytes > self.cadence_ledger_max_bytes
            or self.cadence_cycle_reserve_bytes > self.cadence_raw_max_bytes
            or self.cadence_cycle_reserve_entries > self.cadence_ledger_max_entries
            or self.cadence_cycle_reserve_entries > self.cadence_raw_max_entries
            or self.cadence_cycle_reserve_entries > self.cadence_raw_max_receipts
            or self.cadence_cycle_reserve_entries
            < self.cadence_cycle_reserve_captures
            or self.cadence_cycle_reserve_captures
            > self.cadence_raw_max_captures
            or self.cadence_cycle_reserve_captures
            > self.cadence_ledger_max_reservations
            or self.cadence_ledger_catalog_max_bytes
            > self.configured_persistent_max_bytes
            or self.cadence_raw_catalog_max_bytes
            > self.configured_persistent_max_bytes
            or self.event_payload_reserve_bytes
            > self.event_retained_payload_max_bytes
            or self.event_control_intake_reserve
            >= self.event_control_receipt_capacity
            or self.configured_persistent_max_bytes
            + self.host_operational_free_reserve_bytes
            > MAX_SIGNED_INTEGER
            or event_control_max_bytes > MAX_SIGNED_INTEGER
            or minimum_persistent_envelope
            > self.configured_persistent_max_bytes
            or self.configured_raw_object_max_entries < minimum_raw_entries
        ):
            raise ValueError("retention_guard_envelope_invalid")


def _positive_integer(value: object, name: str) -> int:
    if type(value) is not int or not 1 <= value <= MAX_SIGNED_INTEGER:
        raise ValueError(f"{name}_invalid")
    return value


def _bounded_integer(value: object, name: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_SIGNED_INTEGER:
        raise RawCaptureRetentionGuardError(f"{name}_invalid")
    return value


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RawCaptureRetentionGuardError(f"{name}_invalid")
    return value


def _integer_field(value: Mapping[str, Any], key: str, name: str) -> int:
    if key not in value:
        raise RawCaptureRetentionGuardError(f"{name}_invalid")
    return _bounded_integer(value[key], name)


def _paths_overlap(left: Path, right: Path) -> bool:
    return _path_contains(left, right) or _path_contains(right, left)


def _path_contains(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def _is_reparse(metadata: os.stat_result) -> bool:
    attributes = getattr(metadata, "st_file_attributes", 0)
    marker = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(attributes & marker)


@dataclass(frozen=True, slots=True)
class _PathProof:
    fingerprints: tuple[tuple[str, int, int, int, int], ...]
    filesystems: tuple[int, ...]
    capacity_probe_root: Path


class RawCaptureRetentionGuard:
    """Continuously reprove capacity without wiring any delete authority."""

    def __init__(
        self,
        *,
        config: RawCaptureRetentionGuardConfig,
        ownership_assertion: Callable[[], None],
        ledger_stats_supplier: Callable[[], Mapping[str, Any]],
        ledger_audit_supplier: Callable[[], Mapping[str, Any]],
        raw_store_stats_supplier: Callable[[], Mapping[str, Any]],
        raw_store_audit_supplier: Callable[[], Mapping[str, Any]],
        event_diagnostics_supplier: Callable[[], Mapping[str, Any]] | None,
        disk_usage_supplier: Callable[[Path], Any] | None = None,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        if not isinstance(config, RawCaptureRetentionGuardConfig):
            raise TypeError("config must be RawCaptureRetentionGuardConfig")
        suppliers = (
            ownership_assertion,
            ledger_stats_supplier,
            ledger_audit_supplier,
            raw_store_stats_supplier,
            raw_store_audit_supplier,
        )
        if not all(callable(supplier) for supplier in suppliers):
            raise TypeError("retention_guard_supplier_invalid")
        if config.event_capacity_claimed:
            if not callable(event_diagnostics_supplier):
                raise TypeError("retention_guard_supplier_invalid")
        elif event_diagnostics_supplier is not None:
            raise TypeError("retention_guard_supplier_invalid")
        if disk_usage_supplier is not None and not callable(
            disk_usage_supplier
        ):
            raise TypeError("retention_guard_supplier_invalid")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("retention_guard_fault_injector_invalid")
        self._config = config
        self._ownership_assertion = ownership_assertion
        self._ledger_stats_supplier = ledger_stats_supplier
        self._ledger_audit_supplier = ledger_audit_supplier
        self._raw_store_stats_supplier = raw_store_stats_supplier
        self._raw_store_audit_supplier = raw_store_audit_supplier
        self._event_diagnostics_supplier = event_diagnostics_supplier
        self._disk_usage_supplier = disk_usage_supplier or shutil.disk_usage
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._inspection_count = 0
        self._full_audit_completed = False
        self._last = self._initial_diagnostics()

    def _fault(self, checkpoint: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(checkpoint)

    def _initial_diagnostics(self) -> dict[str, Any]:
        return self._diagnostics_document(
            admission_ready=False,
            alarms=["retention_guard_not_inspected"],
            capacity=self._zero_capacity(),
            full_audit=False,
            ownership_held=False,
            path_identity_reproved=False,
            same_filesystem_reproved=False,
        )

    def _zero_capacity(self) -> dict[str, int]:
        config = self._config
        return {
            "cadence_catalog_headroom_bytes": 0,
            "cadence_capture_headroom": 0,
            "cadence_capture_max_entries": config.cadence_raw_max_captures,
            "cadence_cycle_reserve_bytes": config.cadence_cycle_reserve_bytes,
            "cadence_cycle_reserve_captures": (
                config.cadence_cycle_reserve_captures
            ),
            "cadence_cycle_reserve_entries": config.cadence_cycle_reserve_entries,
            "cadence_ledger_byte_headroom": 0,
            "cadence_ledger_catalog_headroom_bytes": 0,
            "cadence_ledger_catalog_max_bytes": (
                config.cadence_ledger_catalog_max_bytes
            ),
            "cadence_ledger_entry_headroom": 0,
            "cadence_ledger_max_bytes": config.cadence_ledger_max_bytes,
            "cadence_ledger_max_entries": config.cadence_ledger_max_entries,
            "cadence_ledger_max_reservations": (
                config.cadence_ledger_max_reservations
            ),
            "cadence_raw_byte_headroom": 0,
            "cadence_raw_catalog_max_bytes": (
                config.cadence_raw_catalog_max_bytes
            ),
            "cadence_raw_entry_headroom": 0,
            "cadence_raw_max_bytes": config.cadence_raw_max_bytes,
            "cadence_raw_max_entries": config.cadence_raw_max_entries,
            "cadence_receipt_headroom": 0,
            "cadence_receipt_max_entries": config.cadence_raw_max_receipts,
            "cadence_reservation_headroom": 0,
            "configured_persistent_max_bytes": (
                config.configured_persistent_max_bytes
            ),
            "configured_raw_object_max_entries": (
                config.configured_raw_object_max_entries
            ),
            "event_control_headroom": 0,
            "event_payload_byte_headroom": 0,
            "event_payload_entry_headroom": 0,
            "filesystem_free_bytes": 0,
            "filesystem_total_bytes": 0,
            "host_operational_free_headroom_bytes": 0,
            "host_operational_free_reserve_bytes": (
                config.host_operational_free_reserve_bytes
            ),
            "persistent_envelope_free_required_bytes": (
                config.configured_persistent_max_bytes
            ),
            "physical_reserve_headroom_bytes": 0,
        }

    def _diagnostics_document(
        self,
        *,
        admission_ready: bool,
        alarms: list[str],
        capacity: Mapping[str, int],
        full_audit: bool,
        ownership_held: bool,
        path_identity_reproved: bool,
        same_filesystem_reproved: bool,
    ) -> dict[str, Any]:
        ordered_alarms = [code for code in _ALARM_PRIORITY if code in alarms]
        if len(ordered_alarms) != len(set(alarms)):
            raise RawCaptureRetentionGuardError("retention_guard_alarm_invalid")
        reason = "ready"
        for code in _ALARM_PRIORITY:
            if code in ordered_alarms:
                reason = code
                break
        result = {
            "admission_ready": admission_ready,
            "alarm_codes": ordered_alarms,
            "capacity": dict(capacity),
            "capacity_policy": CAPACITY_POLICY,
            "contract": CONTRACT,
            "event_capacity_claimed": self._config.event_capacity_claimed,
            "full_audit_completed": full_audit,
            "inspection_count": self._inspection_count,
            "live_delete_performed": False,
            "ownership": {
                "exclusive_filesystem_owner_proven": False,
                "exclusive_process_owner_held": ownership_held,
                "external_writer_exclusion_proven": False,
                "path_identity_reproved": path_identity_reproved,
                "same_filesystem_reproved": same_filesystem_reproved,
            },
            "reason_code": reason,
            "reclamation": {
                "automatic_reclaim": False,
                "live_apply_authorized": False,
                "live_apply_wired": False,
                "policy": RECLAMATION_POLICY,
                "source_reclaim_authorized": False,
            },
            "unmanaged_data_action": "alarm_and_block_never_delete",
        }
        encoded = json.dumps(
            result,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        if len(encoded) > MAX_DIAGNOSTIC_BYTES:
            raise RawCaptureRetentionGuardError(
                "retention_guard_diagnostics_capacity_exceeded"
            )
        return result

    def _observe_path_set(self, *, require_event_paths: bool) -> _PathProof:
        targets = [
            ("cadence_raw", self._config.cadence_raw_root, "directory"),
            ("cadence_ledger", self._config.cadence_ledger_path, "file"),
        ]
        if self._config.event_capacity_claimed:
            targets.extend((
                ("event_control", self._config.event_control_root, "directory"),
                ("event_raw", self._config.event_raw_root, "directory"),
            ))
        fingerprints: list[tuple[str, int, int, int, int]] = []
        filesystems: list[int] = []
        capacity_probe_root: Path | None = None
        for label, target, expected_kind in targets:
            current = Path(target.anchor)
            if not current.is_absolute():
                raise RawCaptureRetentionGuardError(
                    "path_identity_reproof_failed"
                )
            last_existing: tuple[Path, os.stat_result] | None = None
            final_exists = False
            missing_seen = False
            for ordinal, part in enumerate(target.parts[1:], start=1):
                current = current / part
                try:
                    metadata = current.lstat()
                except FileNotFoundError:
                    missing_seen = True
                    continue
                except OSError:
                    raise RawCaptureRetentionGuardError(
                        "path_identity_reproof_failed"
                    ) from None
                if missing_seen or stat.S_ISLNK(metadata.st_mode) or _is_reparse(metadata):
                    raise RawCaptureRetentionGuardError(
                        "path_identity_reproof_failed"
                    )
                is_final = ordinal == len(target.parts) - 1
                final_exists = is_final
                if is_final and expected_kind == "file":
                    if not stat.S_ISREG(metadata.st_mode):
                        raise RawCaptureRetentionGuardError(
                            "path_identity_reproof_failed"
                        )
                elif not stat.S_ISDIR(metadata.st_mode):
                    raise RawCaptureRetentionGuardError(
                        "path_identity_reproof_failed"
                    )
                fingerprint = (
                    label,
                    ordinal,
                    int(metadata.st_dev),
                    int(metadata.st_ino),
                    stat.S_IFMT(metadata.st_mode),
                )
                fingerprints.append(fingerprint)
                last_existing = (current, metadata)
            if (
                not final_exists
                and (
                    label in {"cadence_raw", "cadence_ledger"}
                    or require_event_paths
                )
            ):
                raise RawCaptureRetentionGuardError(
                    "path_identity_reproof_failed"
                )
            if last_existing is None:
                try:
                    anchor_metadata = Path(target.anchor).lstat()
                except OSError:
                    raise RawCaptureRetentionGuardError(
                        "path_identity_reproof_failed"
                    ) from None
                if (
                    not stat.S_ISDIR(anchor_metadata.st_mode)
                    or stat.S_ISLNK(anchor_metadata.st_mode)
                    or _is_reparse(anchor_metadata)
                ):
                    raise RawCaptureRetentionGuardError(
                        "path_identity_reproof_failed"
                    )
                last_existing = (Path(target.anchor), anchor_metadata)
            filesystems.append(int(last_existing[1].st_dev))
            if label == "cadence_raw":
                capacity_probe_root = last_existing[0]
        if capacity_probe_root is None or len(set(filesystems)) != 1:
            raise RawCaptureRetentionGuardError("path_identity_reproof_failed")
        return _PathProof(
            fingerprints=tuple(fingerprints),
            filesystems=tuple(filesystems),
            capacity_probe_root=capacity_probe_root,
        )

    def _validate_ledger(
        self,
        value: object,
        *,
        full_audit: object | None,
    ) -> dict[str, int]:
        stats = _mapping(value, "ledger_stats")
        fields = {
            key: _integer_field(stats, key, "ledger_stats")
            for key in (
                "catalog_file_bytes",
                "catalog_main_database_bytes",
                "catalog_main_database_max_bytes",
                "catalog_page_size_bytes",
                "committed_bytes",
                "committed_count",
                "committed_entries",
                "global_max_bytes",
                "global_max_entries",
                "max_catalog_bytes",
                "max_reservations",
                "reservation_rows",
                "reserved_bytes",
                "reserved_count",
                "reserved_entries",
            )
        }
        config = self._config
        expected_catalog_main_max = (
            capacity_ledger_catalog_main_database_max_bytes(
                max_catalog_bytes=fields["max_catalog_bytes"],
                page_size_bytes=fields["catalog_page_size_bytes"],
            )
        )
        if (
            fields["global_max_bytes"] != config.cadence_ledger_max_bytes
            or fields["global_max_entries"] != config.cadence_ledger_max_entries
            or fields["max_reservations"]
            != config.cadence_ledger_max_reservations
            or fields["max_catalog_bytes"]
            != config.cadence_ledger_catalog_max_bytes
            or fields["reservation_rows"]
            != fields["reserved_count"] + fields["committed_count"]
            or fields["reservation_rows"] > fields["max_reservations"]
            or fields["reserved_bytes"] + fields["committed_bytes"]
            > fields["global_max_bytes"]
            or fields["reserved_entries"] + fields["committed_entries"]
            > fields["global_max_entries"]
            or fields["catalog_file_bytes"] > fields["max_catalog_bytes"]
            or fields["catalog_main_database_max_bytes"]
            != expected_catalog_main_max
            or fields["catalog_main_database_bytes"]
            > fields["catalog_main_database_max_bytes"]
            or fields["catalog_main_database_bytes"]
            > fields["catalog_file_bytes"]
        ):
            raise RawCaptureRetentionGuardError("storage_integrity_failed")
        if full_audit is not None:
            audit = _mapping(full_audit, "ledger_audit")
            for key in (
                "reservation_rows",
                "reserved_count",
                "committed_count",
                "reserved_bytes",
                "committed_bytes",
                "reserved_entries",
                "committed_entries",
            ):
                if _integer_field(audit, key, "ledger_audit") != fields[key]:
                    raise RawCaptureRetentionGuardError(
                        "storage_integrity_failed"
                    )
        return fields

    def _validate_raw_store(
        self,
        value: object,
        *,
        full_audit: object | None,
    ) -> dict[str, int]:
        stats = _mapping(value, "raw_store_stats")
        fields = {
            key: _integer_field(stats, key, "raw_store_stats")
            for key in (
                "catalog_bytes",
                "catalog_database_bytes",
                "catalog_main_database_bytes",
                "catalog_main_database_max_bytes",
                "catalog_page_size_bytes",
                "filesystem_bytes",
                "filesystem_object_entries",
                "filesystem_staging_entries",
                "max_catalog_bytes",
                "max_capture_entries",
                "max_object_entries",
                "max_receipt_entries",
                "max_total_bytes",
                "capture_count",
                "object_count",
                "receipt_count",
                "unmanaged_file_count",
            )
        }
        config = self._config
        expected_catalog_main_max = raw_catalog_main_database_max_bytes(
            max_catalog_bytes=fields["max_catalog_bytes"],
            page_size_bytes=fields["catalog_page_size_bytes"],
        )
        if (
            fields["max_total_bytes"] != config.cadence_raw_max_bytes
            or fields["max_object_entries"] != config.cadence_raw_max_entries
            or fields["max_receipt_entries"]
            != config.cadence_raw_max_receipts
            or fields["max_capture_entries"]
            != config.cadence_raw_max_captures
            or fields["max_catalog_bytes"]
            != config.cadence_raw_catalog_max_bytes
            or fields["filesystem_bytes"] > fields["max_total_bytes"]
            or fields["catalog_bytes"] != fields["filesystem_bytes"]
            or fields["filesystem_object_entries"]
            > fields["max_object_entries"]
            or fields["object_count"] != fields["filesystem_object_entries"]
            or fields["receipt_count"] > fields["max_receipt_entries"]
            or fields["capture_count"] > fields["max_capture_entries"]
            or fields["catalog_database_bytes"] > fields["max_catalog_bytes"]
            or fields["catalog_main_database_max_bytes"]
            != expected_catalog_main_max
            or fields["catalog_main_database_bytes"]
            > fields["catalog_main_database_max_bytes"]
            or fields["catalog_main_database_bytes"]
            > fields["catalog_database_bytes"]
            or fields["filesystem_staging_entries"] != 0
            or fields["unmanaged_file_count"] != 0
            or stats.get("retention_policy") != "none"
        ):
            raise RawCaptureRetentionGuardError("storage_integrity_failed")
        if full_audit is not None:
            audit = _mapping(full_audit, "raw_store_audit")
            reclamation = _mapping(
                audit.get("reclamation"), "raw_store_reclamation_audit"
            )
            if (
                audit.get("healthy") is not True
                or audit.get("issues") != []
                or reclamation.get("enabled") is not False
                or _integer_field(
                    audit, "checked_objects", "raw_store_audit"
                )
                != fields["object_count"]
                or _integer_field(
                    audit, "checked_receipts", "raw_store_audit"
                )
                != fields["receipt_count"]
                or _integer_field(
                    audit, "checked_captures", "raw_store_audit"
                )
                != fields["capture_count"]
            ):
                raise RawCaptureRetentionGuardError("storage_integrity_failed")
        return fields

    def _validate_event(self, value: object) -> dict[str, int | bool]:
        if not self._config.event_capacity_claimed:
            return {
                "authority_available": False,
                "control_intake_headroom": 0,
                "control_receipt_capacity": 0,
                "control_receipts": 0,
                "degraded": False,
                "paths_required": False,
                "queue_capacity": 0,
                "queue_headroom": 0,
                "retained_payload_byte_capacity": 0,
                "retained_payload_byte_headroom": 0,
                "retained_payload_entry_capacity": 0,
                "retained_payload_entry_headroom": 0,
            }
        event = _mapping(value, "event_diagnostics")
        capacity = _mapping(event.get("capacity"), "event_capacity")
        fields = {
            key: _integer_field(capacity, key, "event_capacity")
            for key in (
                "control_intake_headroom",
                "control_receipt_capacity",
                "control_receipts",
                "queue_capacity",
                "queue_headroom",
                "retained_payload_byte_capacity",
                "retained_payload_byte_headroom",
                "retained_payload_entry_capacity",
                "retained_payload_entry_headroom",
            )
        }
        queue_depth = _integer_field(event, "queue_depth", "event_diagnostics")
        retained_bytes = _integer_field(
            event, "retained_payload_bytes", "event_diagnostics"
        )
        retained_entries = _integer_field(
            event, "retained_payload_entries", "event_diagnostics"
        )
        state = event.get("state")
        network_started = event.get("network_started")
        access_authority = event.get("access_retention_authority")
        if (
            not isinstance(state, str)
            or not 1 <= len(state) <= 64
            or not state.isascii()
            or type(network_started) is not bool
            or access_authority not in {"available", "unavailable"}
            or type(event.get("fault_latched")) is not bool
        ):
            raise RawCaptureRetentionGuardError("storage_integrity_failed")
        if (
            fields["control_receipt_capacity"]
            != self._config.event_control_receipt_capacity
            or fields["queue_capacity"] != self._config.event_queue_capacity
            or fields["retained_payload_byte_capacity"]
            != self._config.event_retained_payload_max_bytes
            or fields["retained_payload_entry_capacity"]
            != self._config.event_retained_payload_max_entries
            or queue_depth + fields["queue_headroom"]
            != fields["queue_capacity"]
            or retained_bytes + fields["retained_payload_byte_headroom"]
            != fields["retained_payload_byte_capacity"]
            or retained_entries + fields["retained_payload_entry_headroom"]
            != fields["retained_payload_entry_capacity"]
            or fields["control_receipts"]
            + fields["control_intake_headroom"]
            + self._config.event_control_intake_reserve
            != fields["control_receipt_capacity"]
        ):
            raise RawCaptureRetentionGuardError("storage_integrity_failed")
        alarm_codes = event.get("alarm_codes")
        if (
            type(alarm_codes) is not list
            or len(alarm_codes) > 16
            or any(
                not isinstance(code, str)
                or not 1 <= len(code) <= 96
                or not code.isascii()
                for code in alarm_codes
            )
            or len(alarm_codes) != len(set(alarm_codes))
        ):
            raise RawCaptureRetentionGuardError("storage_integrity_failed")
        authority_available = access_authority == "available"
        paths_required = authority_available and (
            network_started
            or state not in {"new", "authority_unavailable", "waiting_for_regions"}
            or queue_depth > 0
            or retained_bytes > 0
            or retained_entries > 0
            or fields["control_receipts"] > 0
        )
        return {
            **fields,
            "authority_available": authority_available,
            "degraded": bool(alarm_codes) or event.get("fault_latched") is True,
            "paths_required": paths_required,
        }

    def _disk_usage(self, path: Path) -> tuple[int, int]:
        value = self._disk_usage_supplier(path)
        total = getattr(value, "total", None)
        free = getattr(value, "free", None)
        if isinstance(value, Mapping):
            total = value.get("total")
            free = value.get("free")
        bounded_total = _bounded_integer(total, "filesystem_capacity")
        bounded_free = _bounded_integer(free, "filesystem_capacity")
        if bounded_total <= 0 or bounded_free > bounded_total:
            raise RawCaptureRetentionGuardError("storage_integrity_failed")
        return bounded_total, bounded_free

    def _capacity_document(
        self,
        *,
        ledger: Mapping[str, int],
        raw: Mapping[str, int],
        event: Mapping[str, int | bool],
        filesystem_total: int,
        filesystem_free: int,
    ) -> tuple[dict[str, int], list[str]]:
        config = self._config
        ledger_used_bytes = ledger["reserved_bytes"] + ledger["committed_bytes"]
        ledger_used_entries = (
            ledger["reserved_entries"] + ledger["committed_entries"]
        )
        ledger_byte_headroom = ledger["global_max_bytes"] - ledger_used_bytes
        ledger_entry_headroom = (
            ledger["global_max_entries"] - ledger_used_entries
        )
        reservation_headroom = (
            ledger["max_reservations"] - ledger["reservation_rows"]
        )
        raw_byte_headroom = raw["max_total_bytes"] - raw["filesystem_bytes"]
        raw_entry_headroom = (
            raw["max_object_entries"] - raw["filesystem_object_entries"]
        )
        receipt_headroom = raw["max_receipt_entries"] - raw["receipt_count"]
        capture_headroom = raw["max_capture_entries"] - raw["capture_count"]
        raw_catalog_physical_headroom = (
            raw["max_catalog_bytes"] - raw["catalog_database_bytes"]
        )
        raw_catalog_main_headroom = (
            raw["catalog_main_database_max_bytes"]
            - raw["catalog_main_database_bytes"]
        )
        raw_catalog_headroom = min(
            raw_catalog_physical_headroom,
            raw_catalog_main_headroom,
        )
        ledger_catalog_physical_headroom = (
            ledger["max_catalog_bytes"] - ledger["catalog_file_bytes"]
        )
        ledger_catalog_main_headroom = (
            ledger["catalog_main_database_max_bytes"]
            - ledger["catalog_main_database_bytes"]
        )
        ledger_catalog_headroom = min(
            ledger_catalog_physical_headroom,
            ledger_catalog_main_headroom,
        )
        raw_catalog_cycle_reserve = raw_catalog_cycle_reserve_bytes(
            artifact_entries=config.cadence_cycle_reserve_entries,
            captures=config.cadence_cycle_reserve_captures,
            page_size_bytes=raw["catalog_page_size_bytes"],
        )
        ledger_catalog_cycle_reserve = (
            capacity_ledger_catalog_cycle_reserve_bytes(
                reservations=config.cadence_cycle_reserve_captures,
                page_size_bytes=ledger["catalog_page_size_bytes"],
            )
        )
        known_persistent_bytes = (
            raw["filesystem_bytes"]
            + raw["catalog_database_bytes"]
            + ledger["catalog_file_bytes"]
            + (0 if not config.event_capacity_claimed else (
                int(event["retained_payload_byte_capacity"])
                - int(event["retained_payload_byte_headroom"])
            ))
        )
        if known_persistent_bytes > config.configured_persistent_max_bytes:
            raise RawCaptureRetentionGuardError("storage_integrity_failed")
        remaining_envelope = max(
            0,
            config.configured_persistent_max_bytes - known_persistent_bytes,
        )
        protected_free_required = (
            remaining_envelope + config.host_operational_free_reserve_bytes
        )
        if protected_free_required > MAX_SIGNED_INTEGER:
            raise RawCaptureRetentionGuardError("storage_integrity_failed")
        physical_headroom = max(0, filesystem_free - protected_free_required)
        capacity = {
            "cadence_catalog_headroom_bytes": raw_catalog_headroom,
            "cadence_capture_headroom": capture_headroom,
            "cadence_capture_max_entries": raw["max_capture_entries"],
            "cadence_cycle_reserve_bytes": config.cadence_cycle_reserve_bytes,
            "cadence_cycle_reserve_captures": (
                config.cadence_cycle_reserve_captures
            ),
            "cadence_cycle_reserve_entries": config.cadence_cycle_reserve_entries,
            "cadence_ledger_byte_headroom": ledger_byte_headroom,
            "cadence_ledger_catalog_headroom_bytes": ledger_catalog_headroom,
            "cadence_ledger_catalog_max_bytes": ledger["max_catalog_bytes"],
            "cadence_ledger_entry_headroom": ledger_entry_headroom,
            "cadence_ledger_max_bytes": ledger["global_max_bytes"],
            "cadence_ledger_max_entries": ledger["global_max_entries"],
            "cadence_ledger_max_reservations": ledger["max_reservations"],
            "cadence_raw_byte_headroom": raw_byte_headroom,
            "cadence_raw_catalog_max_bytes": raw["max_catalog_bytes"],
            "cadence_raw_entry_headroom": raw_entry_headroom,
            "cadence_raw_max_bytes": raw["max_total_bytes"],
            "cadence_raw_max_entries": raw["max_object_entries"],
            "cadence_receipt_headroom": receipt_headroom,
            "cadence_receipt_max_entries": raw["max_receipt_entries"],
            "cadence_reservation_headroom": reservation_headroom,
            "configured_persistent_max_bytes": (
                config.configured_persistent_max_bytes
            ),
            "configured_raw_object_max_entries": (
                config.configured_raw_object_max_entries
            ),
            "event_control_headroom": int(event["control_intake_headroom"]),
            "event_payload_byte_headroom": int(
                event["retained_payload_byte_headroom"]
            ),
            "event_payload_entry_headroom": int(
                event["retained_payload_entry_headroom"]
            ),
            "filesystem_free_bytes": filesystem_free,
            "filesystem_total_bytes": filesystem_total,
            "host_operational_free_headroom_bytes": physical_headroom,
            "host_operational_free_reserve_bytes": (
                config.host_operational_free_reserve_bytes
            ),
            "persistent_envelope_free_required_bytes": remaining_envelope,
            "physical_reserve_headroom_bytes": physical_headroom,
        }
        alarms: list[str] = []
        if filesystem_free < protected_free_required:
            alarms.append("filesystem_envelope_capacity_exhausted")
        elif filesystem_free < (
            protected_free_required + config.cadence_cycle_reserve_bytes
        ):
            alarms.append("filesystem_envelope_capacity_low")
        byte_headroom = min(ledger_byte_headroom, raw_byte_headroom)
        entry_headroom = min(
            ledger_entry_headroom,
            raw_entry_headroom,
            receipt_headroom,
        )
        if byte_headroom < config.cadence_cycle_reserve_bytes:
            alarms.append("cadence_cycle_byte_capacity_exhausted")
        elif byte_headroom < (
            config.cadence_cycle_reserve_bytes * WARNING_CYCLE_MULTIPLIER
        ):
            alarms.append("cadence_cycle_byte_capacity_low")
        if entry_headroom < config.cadence_cycle_reserve_entries:
            alarms.append("cadence_cycle_entry_capacity_exhausted")
        elif entry_headroom < (
            config.cadence_cycle_reserve_entries * WARNING_CYCLE_MULTIPLIER
        ):
            alarms.append("cadence_cycle_entry_capacity_low")
        if capture_headroom < config.cadence_cycle_reserve_captures:
            alarms.append("cadence_capture_capacity_exhausted")
        elif capture_headroom < (
            config.cadence_cycle_reserve_captures
            * WARNING_CYCLE_MULTIPLIER
        ):
            alarms.append("cadence_capture_capacity_low")
        if reservation_headroom < config.cadence_cycle_reserve_captures:
            alarms.append("cadence_reservation_capacity_exhausted")
        elif reservation_headroom < (
            config.cadence_cycle_reserve_captures
            * WARNING_CYCLE_MULTIPLIER
        ):
            alarms.append("cadence_reservation_capacity_low")
        if raw_catalog_headroom < raw_catalog_cycle_reserve:
            alarms.append("cadence_catalog_capacity_exhausted")
        elif raw_catalog_headroom < (
            raw_catalog_cycle_reserve * WARNING_CYCLE_MULTIPLIER
        ):
            alarms.append("cadence_catalog_capacity_low")
        if ledger_catalog_headroom < ledger_catalog_cycle_reserve:
            alarms.append("cadence_ledger_catalog_capacity_exhausted")
        elif ledger_catalog_headroom < (
            ledger_catalog_cycle_reserve * WARNING_CYCLE_MULTIPLIER
        ):
            alarms.append("cadence_ledger_catalog_capacity_low")
        if config.event_capacity_claimed and event["authority_available"] is not True:
            alarms.append("event_capacity_claim_not_available")
        elif config.event_capacity_claimed:
            if (
                int(event["retained_payload_byte_headroom"])
                < config.event_payload_reserve_bytes
                or int(event["retained_payload_entry_headroom"]) < 1
                or int(event["control_intake_headroom"]) < 1
                or int(event["queue_headroom"]) < 1
            ):
                alarms.append("event_capacity_exhausted")
            elif (
                int(event["retained_payload_byte_headroom"])
                < config.event_payload_reserve_bytes * WARNING_CYCLE_MULTIPLIER
                or int(event["retained_payload_entry_headroom"]) < 2
                or int(event["control_intake_headroom"]) < 2
                or int(event["queue_headroom"]) < 2
            ):
                alarms.append("event_capacity_low")
            if event["degraded"] is True:
                alarms.append("event_transport_degraded")
        return capacity, alarms

    def _cache_failure(self, code: str) -> None:
        if code not in _KNOWN_ALARMS:
            code = "storage_integrity_failed"
        self._full_audit_completed = False
        self._last = self._diagnostics_document(
            admission_ready=False,
            alarms=[code],
            capacity=self._zero_capacity(),
            full_audit=False,
            ownership_held=(code != "exclusive_process_owner_not_held"),
            path_identity_reproved=False,
            same_filesystem_reproved=False,
        )

    def inspect(self, *, full_audit: bool = False) -> dict[str, Any]:
        """Reprove bounded headroom while held, or block without mutation."""

        if type(full_audit) is not bool:
            raise TypeError("full_audit must be bool")
        with self._lock:
            try:
                self._ownership_assertion()
            except Exception:
                self._cache_failure("exclusive_process_owner_not_held")
                raise RawCaptureRetentionGuardBlocked(
                    "exclusive_process_owner_not_held"
                ) from None
            try:
                self._fault("after_ownership")
                event = self._validate_event(
                    self._event_diagnostics_supplier()
                    if self._event_diagnostics_supplier is not None
                    else None
                )
                require_event_paths = event["paths_required"] is True
                before = self._observe_path_set(
                    require_event_paths=require_event_paths
                )
                ledger_stats = self._ledger_stats_supplier()
                ledger_audit = self._ledger_audit_supplier() if full_audit else None
                ledger = self._validate_ledger(
                    ledger_stats,
                    full_audit=ledger_audit,
                )
                self._fault("after_ledger")
                raw_stats = self._raw_store_stats_supplier()
                raw_audit = self._raw_store_audit_supplier() if full_audit else None
                raw = self._validate_raw_store(raw_stats, full_audit=raw_audit)
                self._fault("after_raw_store")
                filesystem_total, filesystem_free = self._disk_usage(
                    before.capacity_probe_root
                )
                self._fault("after_capacity_probe")
                after = self._observe_path_set(
                    require_event_paths=require_event_paths
                )
                if before != after:
                    raise RawCaptureRetentionGuardError(
                        "path_identity_reproof_failed"
                    )
                self._fault("after_identity_reproof")
                try:
                    self._ownership_assertion()
                except Exception:
                    raise RawCaptureRetentionGuardError(
                        "exclusive_process_owner_not_held"
                    ) from None
                self._fault("after_ownership_reproof")
                capacity, alarms = self._capacity_document(
                    ledger=ledger,
                    raw=raw,
                    event=event,
                    filesystem_total=filesystem_total,
                    filesystem_free=filesystem_free,
                )
                full_audit_completed = (
                    self._full_audit_completed or full_audit
                )
                if not full_audit_completed:
                    alarms.append("retention_guard_not_inspected")
                unknown = set(alarms) - _KNOWN_ALARMS
                if unknown:
                    raise RawCaptureRetentionGuardError(
                        "storage_integrity_failed"
                    )
                self._inspection_count = min(
                    MAX_SIGNED_INTEGER, self._inspection_count + 1
                )
                self._full_audit_completed = full_audit_completed
                blocking = set(alarms) & _BLOCKING_ALARMS
                self._last = self._diagnostics_document(
                    admission_ready=not blocking,
                    alarms=alarms,
                    capacity=capacity,
                    full_audit=self._full_audit_completed,
                    ownership_held=True,
                    path_identity_reproved=True,
                    same_filesystem_reproved=True,
                )
                if blocking:
                    raise RawCaptureRetentionGuardBlocked(
                        self._last["reason_code"]
                    )
                return json.loads(json.dumps(self._last))
            except RawCaptureRetentionGuardBlocked:
                raise
            except RawCaptureRetentionGuardError as error:
                code = str(error)
                self._cache_failure(code)
                raise RawCaptureRetentionGuardBlocked(
                    self._last["reason_code"]
                ) from None
            except Exception:
                self._cache_failure("storage_integrity_failed")
                raise RawCaptureRetentionGuardBlocked(
                    "storage_integrity_failed"
                ) from None

    def diagnostics(self, *, ownership_held: bool) -> dict[str, Any]:
        """Return fixed, capability-free diagnostics without reading stores."""

        if type(ownership_held) is not bool:
            raise TypeError("ownership_held must be bool")
        with self._lock:
            result = json.loads(json.dumps(self._last))
        if ownership_held:
            return result
        alarms = set(result["alarm_codes"])
        alarms.add("exclusive_process_owner_not_held")
        return self._diagnostics_document(
            admission_ready=False,
            alarms=list(alarms),
            capacity=result["capacity"],
            full_audit=result["full_audit_completed"],
            ownership_held=False,
            path_identity_reproved=result["ownership"][
                "path_identity_reproved"
            ],
            same_filesystem_reproved=result["ownership"][
                "same_filesystem_reproved"
            ],
        )


__all__ = [
    "CAPACITY_POLICY",
    "CONTRACT",
    "RECLAMATION_POLICY",
    "RETENTION_GUARD_BLOCKING_ALARM_CODES",
    "RawCaptureRetentionGuard",
    "RawCaptureRetentionGuardBlocked",
    "RawCaptureRetentionGuardConfig",
    "RawCaptureRetentionGuardError",
]
