"""Closed, dependency-light participant projections for source reclamation.

Retention-v2 sealing and all five native source participants import these exact
builders.  This avoids a dangerous class of drift where an archive hashes one
shape while the store that will later delete the row hashes another.

The projections contain metadata and durable receipt identities only.  Raw
provider bytes remain in separate content-addressed archive objects and are
never embedded in a candidate page or projection.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
import re

from .acquisition_source_reclamation_protocol import (
    PARTICIPANT_APPLY_ORDER,
    AcquisitionSourceReclamationProtocolIntegrityError,
    canonical_bytes,
    digest_mapping,
    require_sha256,
)


VIEW_CONTRACT = "stv3-worker-acquisition-source-reclamation-role-views-v1"
LIFECYCLE_VIEW_SCHEMA = "stv3-worker-reclamation-lifecycle-candidate-view-v1"
EXECUTION_VIEW_SCHEMA = "stv3-worker-reclamation-execution-candidate-view-v1"
SCHEDULER_VIEW_SCHEMA = "stv3-worker-reclamation-scheduler-candidate-view-v1"
RAW_VIEW_SCHEMA = "stv3-worker-reclamation-raw-candidate-view-v1"
CAPACITY_VIEW_SCHEMA = "stv3-worker-reclamation-capacity-candidate-view-v1"

_ROLE_SCHEMAS = {
    "downstream_lifecycle": LIFECYCLE_VIEW_SCHEMA,
    "execution_receipts": EXECUTION_VIEW_SCHEMA,
    "scheduler": SCHEDULER_VIEW_SCHEMA,
    "raw_artifacts": RAW_VIEW_SCHEMA,
    "capacity_ledger": CAPACITY_VIEW_SCHEMA,
}
_ROLE_FIELDS = {
    "downstream_lifecycle": frozenset({"downstream_terminal"}),
    "execution_receipts": frozenset({"execution_history"}),
    "scheduler": frozenset({"scheduler_terminal"}),
    "raw_artifacts": frozenset({
        "raw_capture",
        "raw_objects",
        "raw_receipts",
    }),
    "capacity_ledger": frozenset({"capacity_commit"}),
}
_SENSITIVE_KEY = re.compile(
    r"(?:authorization|client_secret|password|payload_base64|private_key|"
    r"raw_bytes|raw_payload|refresh_token|access_token|api_key|apikey|token)",
    re.IGNORECASE,
)


def _normalise(value: object, *, depth: int, count: list[int]) -> object:
    if depth > 12:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "participant view nesting exceeds its bound"
        )
    count[0] += 1
    if count[0] > 16_384:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "participant view item count exceeds its bound"
        )
    if value is None or type(value) in {bool, int, float, str}:
        return value
    if isinstance(value, bytes):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "participant view may not embed bytes"
        )
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key in sorted(value):
            if not isinstance(key, str) or not key or len(key) > 128:
                raise AcquisitionSourceReclamationProtocolIntegrityError(
                    "participant view key is invalid"
                )
            if _SENSITIVE_KEY.search(key):
                raise AcquisitionSourceReclamationProtocolIntegrityError(
                    "participant view contains a secret or inline-payload key"
                )
            result[key] = _normalise(
                value[key],
                depth=depth + 1,
                count=count,
            )
        return result
    if isinstance(value, Sequence):
        if isinstance(value, (str, bytes, bytearray)):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "participant view sequence is invalid"
            )
        return [
            _normalise(item, depth=depth + 1, count=count)
            for item in value
        ]
    raise AcquisitionSourceReclamationProtocolIntegrityError(
        "participant view contains an unsupported value"
    )


def participant_candidate_view(
    *,
    role: str,
    candidate_sha256: str,
    projection: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the one canonical content projection for ``role``."""

    if role not in PARTICIPANT_APPLY_ORDER or role not in _ROLE_SCHEMAS:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "participant view role is outside the closed catalogue"
        )
    candidate = require_sha256(candidate_sha256, "participant candidate SHA-256")
    if not isinstance(projection, Mapping) or frozenset(projection) != (
        _ROLE_FIELDS[role]
    ):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "participant view projection shape differs"
        )
    normalised = _normalise(projection, depth=0, count=[0])
    if not isinstance(normalised, dict):  # pragma: no cover - mapping invariant
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "participant view projection is invalid"
        )
    result = {
        "candidate_sha256": candidate,
        "contract": VIEW_CONTRACT,
        "role": role,
        "schema": _ROLE_SCHEMAS[role],
        **normalised,
    }
    canonical_bytes(result)
    return result


def participant_candidate_view_sha256(
    *,
    role: str,
    candidate_sha256: str,
    projection: Mapping[str, Any],
) -> str:
    return digest_mapping(participant_candidate_view(
        role=role,
        candidate_sha256=candidate_sha256,
        projection=projection,
    ))


def participant_inventory_view_sha256(
    *,
    role: str,
    candidate_views: tuple[tuple[str, str], ...],
) -> str:
    """Hash an ordered inventory of already-validated per-candidate views."""

    if (
        role not in PARTICIPANT_APPLY_ORDER
        or not isinstance(candidate_views, tuple)
        or not candidate_views
        or tuple(item[0] for item in candidate_views)
        != tuple(sorted(item[0] for item in candidate_views))
        or len({item[0] for item in candidate_views}) != len(candidate_views)
    ):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "participant inventory view shape differs"
        )
    head = digest_mapping({
        "candidate_count": len(candidate_views),
        "contract": VIEW_CONTRACT,
        "role": role,
        "schema": "stv3-worker-reclamation-participant-inventory-root-v1",
    })
    for ordinal, (candidate_sha256, view_sha256) in enumerate(candidate_views):
        head = digest_mapping({
            "candidate_sha256": require_sha256(
                candidate_sha256,
                "inventory candidate SHA-256",
            ),
            "ordinal": ordinal,
            "previous_sha256": head,
            "role": role,
            "view_sha256": require_sha256(
                view_sha256,
                "inventory participant view SHA-256",
            ),
        })
    return digest_mapping({
        "candidate_count": len(candidate_views),
        "head_sha256": head,
        "role": role,
        "schema": "stv3-worker-reclamation-participant-inventory-final-v1",
    })


__all__ = [
    "CAPACITY_VIEW_SCHEMA",
    "EXECUTION_VIEW_SCHEMA",
    "LIFECYCLE_VIEW_SCHEMA",
    "RAW_VIEW_SCHEMA",
    "SCHEDULER_VIEW_SCHEMA",
    "VIEW_CONTRACT",
    "participant_candidate_view",
    "participant_candidate_view_sha256",
    "participant_inventory_view_sha256",
]
