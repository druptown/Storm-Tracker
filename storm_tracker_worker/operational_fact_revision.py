"""Fact-specific admission boundary for rich operational snapshots.

Shadow output is deliberately not an operational weather claim.  This module
is the narrow boundary between an immutable, already validated worker fact and
the bounded Home Assistant read model.  Every non-unknown claim must have one
exact, current and checksummed admission receipt for the same desired-state
authority and the exact canonical value that is published.

The module performs no meteorological derivation and no I/O.  It never turns a
missing receipt into dry weather or ``next_threat=none``; callers must publish
the existing unknown snapshot when this validation fails.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import math
import re
from typing import Any, Iterable, Mapping

from .desired_state import DesiredState
from .tracking_core.operational_snapshot import (
    MAX_SNAPSHOT_TTL_SECONDS,
    canonical_json_bytes,
    validate_snapshot,
)


ADMISSION_RECEIPT_CONTRACT = "stv3-operational-fact-admission-receipt-v1"
COMMITTED_REVISION_CONTRACT = "stv3-operational-fact-revision-v1"
ADMISSION_POLICY_ID = "stv3-fact-specific-operational-admission-v1"
MAX_ADMISSION_RECEIPTS = 512
MAX_RECEIPT_REFERENCES = 64

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_REASON_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")


class OperationalFactAdmissionError(ValueError):
    """A committed revision cannot support its requested weather claims."""


class AdmissionDisposition(str, Enum):
    ADMITTED = "admitted"
    WITHHELD = "withheld"


class OperationalFactKind(str, Enum):
    REVISION_COMPLETE = "revision_complete"
    REGION_CURRENT = "region_current"
    PROVIDER_CURRENT = "provider_current"
    CURRENT_RAINING = "current_raining"
    CURRENT_DRY = "current_dry"
    GROUND_STATE = "ground_state"
    GROUND_CONFIRMATION = "ground_confirmation"
    NEXT_THREAT_AVAILABLE = "next_threat_available"
    NEXT_THREAT_NONE = "next_threat_none"
    CALIBRATED_PROBABILITY = "calibrated_probability"
    PASSAGE_EXPECTED = "passage_expected"
    UI_STORM = "ui_storm"
    MAP_ARTIFACT = "map_artifact"


_FULL_SCAN_KINDS = frozenset({
    OperationalFactKind.REVISION_COMPLETE,
    OperationalFactKind.CURRENT_DRY,
    OperationalFactKind.NEXT_THREAT_NONE,
})
_EXTERNAL_TRUTH_KINDS = frozenset({
    OperationalFactKind.CURRENT_DRY,
    OperationalFactKind.GROUND_STATE,
    OperationalFactKind.GROUND_CONFIRMATION,
    OperationalFactKind.NEXT_THREAT_NONE,
    OperationalFactKind.CALIBRATED_PROBABILITY,
    OperationalFactKind.PASSAGE_EXPECTED,
})


def _canonical_sha(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise OperationalFactAdmissionError(f"{label} is geen lowercase SHA-256")
    return value


def _safe_id(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or _SAFE_ID_RE.fullmatch(value) is None
        or "://" in value
    ):
        raise OperationalFactAdmissionError(f"{label} is geen veilig begrensd ID")
    return value


def _timestamp(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OperationalFactAdmissionError(f"{label} is geen tijdstip")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise OperationalFactAdmissionError(f"{label} is geen eindig tijdstip")
    return result


def _canonical_shas(
    values: object,
    label: str,
    *,
    allow_empty: bool,
) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise OperationalFactAdmissionError(f"{label} moet een tuple zijn")
    if len(values) > MAX_RECEIPT_REFERENCES:
        raise OperationalFactAdmissionError(f"{label} overschrijdt de limiet")
    result = tuple(_sha(item, label) for item in values)
    if result != tuple(sorted(set(result))):
        raise OperationalFactAdmissionError(f"{label} is niet uniek en gesorteerd")
    if not allow_empty and not result:
        raise OperationalFactAdmissionError(f"{label} mag niet leeg zijn")
    return result


def _reason_codes(values: object) -> tuple[str, ...]:
    if not isinstance(values, tuple) or not values:
        raise OperationalFactAdmissionError("reason_codes moet een niet-lege tuple zijn")
    if values != tuple(sorted(set(values))):
        raise OperationalFactAdmissionError("reason_codes is niet uniek en gesorteerd")
    for value in values:
        if not isinstance(value, str) or _REASON_RE.fullmatch(value) is None:
            raise OperationalFactAdmissionError("reason_codes bevat een ongeldig ID")
    return values


@dataclass(frozen=True, slots=True)
class FactAdmissionReceipt:
    """Immutable decision for one exact operational value and scope."""

    receipt_sha256: str
    fact_kind: OperationalFactKind
    scope_id: str
    fact_value_sha256: str
    desired_state_sha256: str
    authority_epoch: int
    source_receipt_sha256s: tuple[str, ...]
    certificate_sha256s: tuple[str, ...]
    process_receipt_sha256: str | None
    full_scan_complete: bool
    external_truth_validated: bool
    decided_at: float
    valid_until: float
    disposition: AdmissionDisposition
    reason_codes: tuple[str, ...]
    policy_id: str = ADMISSION_POLICY_ID
    contract: str = ADMISSION_RECEIPT_CONTRACT

    def __post_init__(self) -> None:
        _sha(self.receipt_sha256, "receipt_sha256")
        if not isinstance(self.fact_kind, OperationalFactKind):
            raise OperationalFactAdmissionError("fact_kind is ongeldig")
        _safe_id(self.scope_id, "scope_id")
        _sha(self.fact_value_sha256, "fact_value_sha256")
        _sha(self.desired_state_sha256, "desired_state_sha256")
        if (
            isinstance(self.authority_epoch, bool)
            or not isinstance(self.authority_epoch, int)
            or not 1 <= self.authority_epoch <= 2**63 - 1
        ):
            raise OperationalFactAdmissionError("authority_epoch is ongeldig")
        _canonical_shas(
            self.source_receipt_sha256s,
            "source_receipt_sha256s",
            allow_empty=self.disposition is AdmissionDisposition.WITHHELD,
        )
        _canonical_shas(
            self.certificate_sha256s,
            "certificate_sha256s",
            allow_empty=self.disposition is AdmissionDisposition.WITHHELD,
        )
        if self.process_receipt_sha256 is not None:
            _sha(self.process_receipt_sha256, "process_receipt_sha256")
        for label in ("full_scan_complete", "external_truth_validated"):
            if type(getattr(self, label)) is not bool:
                raise OperationalFactAdmissionError(f"{label} moet boolean zijn")
        decided = _timestamp(self.decided_at, "decided_at")
        valid_until = _timestamp(self.valid_until, "valid_until")
        if not decided < valid_until:
            raise OperationalFactAdmissionError("admission receipt verloopt niet na besluit")
        if valid_until - decided > MAX_SNAPSHOT_TTL_SECONDS:
            raise OperationalFactAdmissionError("admission receipt overschrijdt de TTL")
        if not isinstance(self.disposition, AdmissionDisposition):
            raise OperationalFactAdmissionError("disposition is ongeldig")
        _reason_codes(self.reason_codes)
        if self.policy_id != ADMISSION_POLICY_ID or self.contract != ADMISSION_RECEIPT_CONTRACT:
            raise OperationalFactAdmissionError("admission policy/contract is onbekend")
        if self.disposition is AdmissionDisposition.ADMITTED:
            if self.process_receipt_sha256 is None:
                raise OperationalFactAdmissionError(
                    "admitted fact mist een hard-processreceipt"
                )
            if self.fact_kind in _FULL_SCAN_KINDS and not self.full_scan_complete:
                raise OperationalFactAdmissionError(
                    "afwezigheids-/completenessclaim mist volledige scan"
                )
            if (
                self.fact_kind in _EXTERNAL_TRUTH_KINDS
                and not self.external_truth_validated
            ):
                raise OperationalFactAdmissionError(
                    "fact vereist onafhankelijk validatiebewijs"
                )
        payload = self.payload_mapping()
        if self.receipt_sha256 != _canonical_sha(payload):
            raise OperationalFactAdmissionError("admission receipthash klopt niet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "authority_epoch": self.authority_epoch,
            "certificate_sha256s": list(self.certificate_sha256s),
            "contract": self.contract,
            "decided_at": self.decided_at,
            "desired_state_sha256": self.desired_state_sha256,
            "disposition": self.disposition.value,
            "external_truth_validated": self.external_truth_validated,
            "fact_kind": self.fact_kind.value,
            "fact_value_sha256": self.fact_value_sha256,
            "full_scan_complete": self.full_scan_complete,
            "policy_id": self.policy_id,
            "process_receipt_sha256": self.process_receipt_sha256,
            "reason_codes": list(self.reason_codes),
            "scope_id": self.scope_id,
            "source_receipt_sha256s": list(self.source_receipt_sha256s),
            "valid_until": self.valid_until,
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FactAdmissionReceipt":
        expected = {
            "authority_epoch",
            "certificate_sha256s",
            "contract",
            "decided_at",
            "desired_state_sha256",
            "disposition",
            "external_truth_validated",
            "fact_kind",
            "fact_value_sha256",
            "full_scan_complete",
            "policy_id",
            "process_receipt_sha256",
            "reason_codes",
            "receipt_sha256",
            "scope_id",
            "source_receipt_sha256s",
            "valid_until",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise OperationalFactAdmissionError("admission receipt schema is niet exact")
        for label in (
            "certificate_sha256s",
            "reason_codes",
            "source_receipt_sha256s",
        ):
            sequence = value[label]
            if not isinstance(sequence, list) or any(
                not isinstance(item, str) for item in sequence
            ):
                raise OperationalFactAdmissionError(
                    f"admission receipt {label} is ongeldig"
                )
        try:
            return cls(
                receipt_sha256=value["receipt_sha256"],
                fact_kind=OperationalFactKind(value["fact_kind"]),
                scope_id=value["scope_id"],
                fact_value_sha256=value["fact_value_sha256"],
                desired_state_sha256=value["desired_state_sha256"],
                authority_epoch=value["authority_epoch"],
                source_receipt_sha256s=tuple(value["source_receipt_sha256s"]),
                certificate_sha256s=tuple(value["certificate_sha256s"]),
                process_receipt_sha256=value["process_receipt_sha256"],
                full_scan_complete=value["full_scan_complete"],
                external_truth_validated=value["external_truth_validated"],
                decided_at=value["decided_at"],
                valid_until=value["valid_until"],
                disposition=AdmissionDisposition(value["disposition"]),
                reason_codes=tuple(value["reason_codes"]),
                policy_id=value["policy_id"],
                contract=value["contract"],
            )
        except (TypeError, ValueError) as error:
            if isinstance(error, OperationalFactAdmissionError):
                raise
            raise OperationalFactAdmissionError(
                "admission receipt enum/schemawaarde is ongeldig"
            ) from error

    @classmethod
    def create(
        cls,
        *,
        fact_kind: OperationalFactKind,
        scope_id: str,
        fact_value: Any,
        desired_state_sha256: str,
        authority_epoch: int,
        source_receipt_sha256s: Iterable[str],
        certificate_sha256s: Iterable[str],
        process_receipt_sha256: str | None,
        full_scan_complete: bool,
        external_truth_validated: bool,
        decided_at: float,
        valid_until: float,
        disposition: AdmissionDisposition,
        reason_codes: Iterable[str],
    ) -> "FactAdmissionReceipt":
        values = {
            "fact_kind": fact_kind,
            "scope_id": scope_id,
            "fact_value_sha256": _canonical_sha(fact_value),
            "desired_state_sha256": desired_state_sha256,
            "authority_epoch": authority_epoch,
            "source_receipt_sha256s": tuple(sorted(set(source_receipt_sha256s))),
            "certificate_sha256s": tuple(sorted(set(certificate_sha256s))),
            "process_receipt_sha256": process_receipt_sha256,
            "full_scan_complete": full_scan_complete,
            "external_truth_validated": external_truth_validated,
            "decided_at": decided_at,
            "valid_until": valid_until,
            "disposition": disposition,
            "reason_codes": tuple(sorted(set(reason_codes))),
        }
        temporary = object.__new__(cls)
        for name, value in values.items():
            object.__setattr__(temporary, name, value)
        object.__setattr__(temporary, "policy_id", ADMISSION_POLICY_ID)
        object.__setattr__(temporary, "contract", ADMISSION_RECEIPT_CONTRACT)
        object.__setattr__(temporary, "receipt_sha256", "0" * 64)
        digest = _canonical_sha(temporary.payload_mapping())
        return cls(receipt_sha256=digest, **values)


def _claim_key(
    kind: OperationalFactKind,
    scope_id: str,
    value: Any,
) -> tuple[OperationalFactKind, str, str]:
    return kind, scope_id, _canonical_sha(value)


def _meteorological_projection(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "map_artifact": snapshot["map_artifact"],
        "product_valid_at": snapshot["product_valid_at"],
        "providers": snapshot["providers"],
        "received_at": snapshot["received_at"],
        "regions": snapshot["regions"],
        "targets": snapshot["targets"],
        "ui_storms": snapshot["ui_storms"],
    }


def required_fact_claims(
    snapshot: Mapping[str, Any],
) -> tuple[tuple[OperationalFactKind, str, str], ...]:
    """Return the exact admission keys required by one validated snapshot."""

    value = validate_snapshot(snapshot)
    claims: set[tuple[OperationalFactKind, str, str]] = set()
    if value["provenance_complete"]:
        claims.add(_claim_key(
            OperationalFactKind.REVISION_COMPLETE,
            f"snapshot:{value['revision']}",
            _meteorological_projection(value),
        ))
    for region in value["regions"]["items"]:
        if region["state"] in {"current", "degraded"}:
            claims.add(_claim_key(
                OperationalFactKind.REGION_CURRENT,
                region["region_id"],
                region,
            ))
    for provider in value["providers"]["items"]:
        if provider["freshness"] == "current":
            claims.add(_claim_key(
                OperationalFactKind.PROVIDER_CURRENT,
                provider["provider_id"],
                provider,
            ))
    for target in value["targets"]["items"]:
        target_id = target["target_id"]
        current = target["current_precipitation"]
        if current["state"] == "raining":
            claims.add(_claim_key(
                OperationalFactKind.CURRENT_RAINING,
                target_id,
                current,
            ))
        elif current["state"] == "dry":
            claims.add(_claim_key(
                OperationalFactKind.CURRENT_DRY,
                target_id,
                current,
            ))
        ground = current["ground_evidence"]
        if ground["status"] != "unknown":
            claims.add(_claim_key(
                OperationalFactKind.GROUND_STATE,
                target_id,
                ground,
            ))
        if ground["confirmation"] is not None:
            claims.add(_claim_key(
                OperationalFactKind.GROUND_CONFIRMATION,
                target_id,
                ground,
            ))
        threat = target["next_threat"]
        if threat["state"] == "available":
            claims.add(_claim_key(
                OperationalFactKind.NEXT_THREAT_AVAILABLE,
                target_id,
                threat,
            ))
        elif threat["state"] == "none":
            claims.add(_claim_key(
                OperationalFactKind.NEXT_THREAT_NONE,
                target_id,
                threat,
            ))
        if threat["calibrated_probability_percent"] is not None:
            claims.add(_claim_key(
                OperationalFactKind.CALIBRATED_PROBABILITY,
                target_id,
                threat,
            ))
        if threat["passage_status"] == "expected":
            claims.add(_claim_key(
                OperationalFactKind.PASSAGE_EXPECTED,
                target_id,
                threat,
            ))
    for storm in value["ui_storms"]["items"]:
        if storm["state"] != "unknown":
            claims.add(_claim_key(
                OperationalFactKind.UI_STORM,
                storm["storm_id"],
                storm,
            ))
    if value["map_artifact"]["artifact_handle"] is not None:
        claims.add(_claim_key(
            OperationalFactKind.MAP_ARTIFACT,
            f"snapshot:{value['revision']}",
            value["map_artifact"],
        ))
    return tuple(sorted(claims, key=lambda item: (item[0].value, item[1], item[2])))


@dataclass(frozen=True, slots=True)
class CommittedOperationalFactRevision:
    """One immutable all-target revision plus its exact admission decisions."""

    revision_sha256: str
    desired_state_sha256: str
    authority_epoch: int
    snapshot: Mapping[str, Any]
    admission_receipts: tuple[FactAdmissionReceipt, ...]
    contract: str = COMMITTED_REVISION_CONTRACT

    def __post_init__(self) -> None:
        _sha(self.revision_sha256, "revision_sha256")
        _sha(self.desired_state_sha256, "desired_state_sha256")
        if (
            isinstance(self.authority_epoch, bool)
            or not isinstance(self.authority_epoch, int)
            or self.authority_epoch < 1
        ):
            raise OperationalFactAdmissionError("authority_epoch is ongeldig")
        if self.contract != COMMITTED_REVISION_CONTRACT:
            raise OperationalFactAdmissionError("committed revision contract is onbekend")
        validated = validate_snapshot(self.snapshot)
        if validated["desired_state_sha256"] != self.desired_state_sha256:
            raise OperationalFactAdmissionError("snapshot gebruikt een andere desired state")
        if not isinstance(self.admission_receipts, tuple):
            raise OperationalFactAdmissionError("admission_receipts moet een tuple zijn")
        if len(self.admission_receipts) > MAX_ADMISSION_RECEIPTS:
            raise OperationalFactAdmissionError("te veel admissionreceipts")
        if any(
            not isinstance(item, FactAdmissionReceipt)
            for item in self.admission_receipts
        ):
            raise OperationalFactAdmissionError("admission_receipts bevat een fout type")
        receipt_hashes = tuple(item.receipt_sha256 for item in self.admission_receipts)
        if receipt_hashes != tuple(sorted(set(receipt_hashes))):
            raise OperationalFactAdmissionError("admissionreceipts zijn niet uniek gesorteerd")
        payload = self.payload_mapping(validated_snapshot=validated)
        if self.revision_sha256 != _canonical_sha(payload):
            raise OperationalFactAdmissionError("committed revision hash klopt niet")

    def payload_mapping(
        self,
        *,
        validated_snapshot: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot = (
            validate_snapshot(self.snapshot)
            if validated_snapshot is None
            else dict(validated_snapshot)
        )
        return {
            "admission_receipts": [
                item.to_mapping() for item in self.admission_receipts
            ],
            "authority_epoch": self.authority_epoch,
            "contract": self.contract,
            "desired_state_sha256": self.desired_state_sha256,
            "snapshot": snapshot,
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "revision_sha256": self.revision_sha256}

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
    ) -> "CommittedOperationalFactRevision":
        expected = {
            "admission_receipts",
            "authority_epoch",
            "contract",
            "desired_state_sha256",
            "revision_sha256",
            "snapshot",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise OperationalFactAdmissionError("committed revision schema is niet exact")
        raw_receipts = value["admission_receipts"]
        if not isinstance(raw_receipts, list):
            raise OperationalFactAdmissionError("committed revision receipts is geen lijst")
        if not isinstance(value["snapshot"], Mapping):
            raise OperationalFactAdmissionError("committed revision snapshot is geen object")
        return cls(
            revision_sha256=value["revision_sha256"],
            desired_state_sha256=value["desired_state_sha256"],
            authority_epoch=value["authority_epoch"],
            snapshot=value["snapshot"],
            admission_receipts=tuple(
                FactAdmissionReceipt.from_mapping(item)
                for item in raw_receipts
            ),
            contract=value["contract"],
        )

    @classmethod
    def create(
        cls,
        *,
        desired_state_sha256: str,
        authority_epoch: int,
        snapshot: Mapping[str, Any],
        admission_receipts: Iterable[FactAdmissionReceipt],
    ) -> "CommittedOperationalFactRevision":
        receipts = tuple(sorted(
            admission_receipts,
            key=lambda item: item.receipt_sha256,
        ))
        validated = validate_snapshot(snapshot)
        temporary = object.__new__(cls)
        object.__setattr__(temporary, "revision_sha256", "0" * 64)
        object.__setattr__(temporary, "desired_state_sha256", desired_state_sha256)
        object.__setattr__(temporary, "authority_epoch", authority_epoch)
        object.__setattr__(temporary, "snapshot", validated)
        object.__setattr__(temporary, "admission_receipts", receipts)
        object.__setattr__(temporary, "contract", COMMITTED_REVISION_CONTRACT)
        digest = _canonical_sha(temporary.payload_mapping(validated_snapshot=validated))
        return cls(
            revision_sha256=digest,
            desired_state_sha256=desired_state_sha256,
            authority_epoch=authority_epoch,
            snapshot=validated,
            admission_receipts=receipts,
        )


def validate_committed_operational_revision(
    desired_state: DesiredState,
    revision: CommittedOperationalFactRevision,
) -> dict[str, Any]:
    """Validate authority, targets and exact fact coverage; return a clone."""

    if not isinstance(desired_state, DesiredState):
        raise OperationalFactAdmissionError("desired_state is niet gevalideerd")
    if not isinstance(revision, CommittedOperationalFactRevision):
        raise OperationalFactAdmissionError("revision heeft het verkeerde type")
    if (
        revision.desired_state_sha256 != desired_state.state_sha256
        or revision.authority_epoch != desired_state.authority_epoch
    ):
        raise OperationalFactAdmissionError("revision authority is verouderd")
    snapshot = validate_snapshot(revision.snapshot)
    if revision.revision_sha256 != _canonical_sha(
        revision.payload_mapping(validated_snapshot=snapshot)
    ):
        raise OperationalFactAdmissionError(
            "committed revision werd na constructie gewijzigd"
        )
    desired_targets = {
        item.target_id: item for item in desired_state.targets
    }
    actual_targets = {
        item["target_id"]: item for item in snapshot["targets"]["items"]
    }
    if set(actual_targets) != set(desired_targets):
        raise OperationalFactAdmissionError("snapshot bevat niet exact alle targets")
    for target_id, desired_target in desired_targets.items():
        actual = actual_targets[target_id]
        if actual["display_name"] != desired_target.display_name:
            raise OperationalFactAdmissionError("targetnaam verschilt van desired state")
        if not desired_target.available and any((
            actual["current_precipitation"]["state"] != "unknown",
            actual["next_threat"]["state"] != "unknown",
        )):
            raise OperationalFactAdmissionError(
                "onbeschikbaar target mag geen operationele weerclaim dragen"
            )

    required = set(required_fact_claims(snapshot))
    supplied: dict[tuple[OperationalFactKind, str, str], FactAdmissionReceipt] = {}
    for receipt in revision.admission_receipts:
        key = (receipt.fact_kind, receipt.scope_id, receipt.fact_value_sha256)
        if key in supplied:
            raise OperationalFactAdmissionError("dubbele admission voor dezelfde claim")
        supplied[key] = receipt
        if (
            receipt.desired_state_sha256 != desired_state.state_sha256
            or receipt.authority_epoch != desired_state.authority_epoch
        ):
            raise OperationalFactAdmissionError("admissionreceipt heeft andere authority")
        if receipt.disposition is not AdmissionDisposition.ADMITTED:
            raise OperationalFactAdmissionError("withheld fact mag niet worden gepubliceerd")
        if receipt.decided_at > float(snapshot["evaluated_at"]):
            raise OperationalFactAdmissionError("admissionbesluit ligt na snapshot-evaluatie")
        if receipt.valid_until < float(snapshot["valid_until"]):
            raise OperationalFactAdmissionError("admission verloopt vóór de snapshot")
    if set(supplied) != required:
        missing = required - set(supplied)
        extra = set(supplied) - required
        raise OperationalFactAdmissionError(
            f"fact-specifieke admissiondekking verschilt; ontbreekt={len(missing)}, extra={len(extra)}"
        )
    return snapshot


__all__ = [
    "ADMISSION_POLICY_ID",
    "ADMISSION_RECEIPT_CONTRACT",
    "AdmissionDisposition",
    "COMMITTED_REVISION_CONTRACT",
    "CommittedOperationalFactRevision",
    "FactAdmissionReceipt",
    "OperationalFactAdmissionError",
    "OperationalFactKind",
    "required_fact_claims",
    "validate_committed_operational_revision",
]
