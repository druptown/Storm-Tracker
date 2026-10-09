"""Provider-neutral exact-byte capture helper.

Every provider remains responsible for its official product semantics and
upstream identifiers.  This helper only standardises the lossless boundary:
all required response bodies are persisted before decode and then linked by one
complete capture receipt.  A partial attempt may leave useful immutable member
receipts, but can never appear as a complete capture.  Retrying the same fetch
reuses those exact receipts.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Any, Mapping, Sequence

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


METHOD = "worker_provider_neutral_native_capture_v1"
MAX_CAPTURE_MEMBERS = 32
MAX_FETCH_ID_LENGTH = 384
MAX_ROLE_LENGTH = 96
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]*$")
_SENSITIVE_KEYS = frozenset({
    "access_token",
    "refresh_token",
    "client_secret",
    "password",
    "authorization",
    "api_key",
    "apikey",
    "token",
})


class NativeCaptureError(ValueError):
    """The provider supplied an unsafe or incomplete capture description."""


@dataclass(frozen=True, slots=True)
class NativeCaptureMemberInput:
    """One exact response body and its caller-known upstream facts."""

    role: str
    product: str
    content_type: str
    received_at: float
    upstream_id: str
    payload: bytes
    provenance: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class NativeCaptureResult:
    method: str
    capture: RawArtifactCaptureReceipt
    members: tuple[RawArtifactReceipt, ...]

    def references(self) -> dict[str, Any]:
        """Return a bounded, payload-free and path-free set reference."""
        by_receipt = {receipt.receipt_id: receipt for receipt in self.members}
        return {
            "method": self.method,
            "capture_id": self.capture.capture_id,
            "provider": self.capture.provider,
            "product": self.capture.product,
            "fetch_id": self.capture.fetch_id,
            "upstream_capture_id": self.capture.upstream_capture_id,
            "product_timestamp": self.capture.product_timestamp,
            "manifest_sha256": self.capture.manifest_sha256,
            "members": [
                {
                    "role": member.role,
                    "receipt_id": member.receipt_id,
                    "content_sha256": member.content_sha256,
                    "received_at": member.received_at,
                    "content_type": by_receipt[member.receipt_id].content_type,
                    "size_bytes": by_receipt[member.receipt_id].size_bytes,
                }
                for member in self.capture.members
            ],
        }


def _identifier(value: object, name: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or _IDENTIFIER_RE.fullmatch(value) is None
    ):
        raise NativeCaptureError(f"{name} is geen veilig begrensd ID")
    return value


def _text(value: object, name: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not value
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise NativeCaptureError(f"{name} is geen veilige begrensde tekst")
    return value


def _timestamp(value: object, name: str, *, optional: bool = False) -> float | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise NativeCaptureError(f"{name} is geen tijdstip")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise NativeCaptureError(f"{name} is geen eindig positief tijdstip")
    return result


def _sensitive_key(value: object) -> bool:
    normalized = str(value).strip().lower().replace("-", "_")
    return (
        normalized in _SENSITIVE_KEYS
        or normalized.endswith("_token")
        or normalized.endswith("_secret")
        or normalized.endswith("_password")
        or normalized.endswith("_api_key")
    )


def _assert_secret_free(value: Any, path: str = "provenance") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if _sensitive_key(key):
                raise NativeCaptureError(f"{path} bevat credentialveld")
            _assert_secret_free(nested, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _assert_secret_free(nested, f"{path}[{index}]")
    elif isinstance(value, str) and "bearer " in value.lower():
        raise NativeCaptureError(f"{path} bevat credentialmateriaal")


def capture_native_set(
    store: RawArtifactStore,
    *,
    provider: str,
    capture_product: str,
    fetch_id: str,
    upstream_capture_id: str,
    product_timestamp: float | None,
    collector: str,
    collector_version: str,
    members: Sequence[NativeCaptureMemberInput],
) -> NativeCaptureResult:
    """Persist all exact members and atomically declare the set complete."""
    if not isinstance(store, RawArtifactStore):
        raise NativeCaptureError("store moet een RawArtifactStore zijn")
    provider_id = _identifier(provider, "provider", 128)
    product_id = _identifier(capture_product, "capture_product", 256)
    fetch = _identifier(fetch_id, "fetch_id", MAX_FETCH_ID_LENGTH)
    upstream_capture = _text(
        upstream_capture_id, "upstream_capture_id", 1024
    )
    collector_id = _identifier(collector, "collector", 128)
    collector_version_id = _identifier(
        collector_version, "collector_version", 256
    )
    nominal_at = _timestamp(
        product_timestamp, "product_timestamp", optional=True
    )
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes)):
        raise NativeCaptureError("members moet een begrensde reeks zijn")
    if not 1 <= len(members) <= MAX_CAPTURE_MEMBERS:
        raise NativeCaptureError("capture vereist een begrensd aantal members")

    normalized: list[tuple[NativeCaptureMemberInput, str, dict[str, Any]]] = []
    seen_roles: set[str] = set()
    for member in members:
        if not isinstance(member, NativeCaptureMemberInput):
            raise NativeCaptureError("ieder member moet NativeCaptureMemberInput zijn")
        role = _identifier(member.role, "member.role", MAX_ROLE_LENGTH)
        if role in seen_roles:
            raise NativeCaptureError("memberrollen moeten uniek zijn")
        seen_roles.add(role)
        _identifier(member.product, "member.product", 256)
        _text(member.content_type, "member.content_type", 256)
        _timestamp(member.received_at, "member.received_at")
        _text(member.upstream_id, "member.upstream_id", 1024)
        if not isinstance(member.payload, bytes):
            raise NativeCaptureError("member.payload moet exact bytes zijn")
        if not isinstance(member.provenance, Mapping):
            raise NativeCaptureError("member.provenance moet een object zijn")
        extras = dict(member.provenance)
        forbidden = {
            "representation", "capture_stage", "collector", "collector_version"
        } & set(extras)
        if forbidden:
            raise NativeCaptureError(
                "provider mag verplichte native provenance niet overschrijven"
            )
        _assert_secret_free(extras)
        normalized.append((
            member,
            role,
            {
                "representation": NATIVE_REPRESENTATION,
                "capture_stage": NATIVE_CAPTURE_STAGE,
                "collector": collector_id,
                "collector_version": collector_version_id,
                **extras,
            },
        ))

    receipts: list[RawArtifactReceipt] = []
    roles: dict[str, str] = {}
    for member, role, provenance in normalized:
        receipt = store.store(
            provider=provider_id,
            product=member.product,
            fetch_id=f"{fetch}:{role}",
            content_type=member.content_type,
            received_at=float(member.received_at),
            upstream_id=member.upstream_id,
            provenance=provenance,
            payload=member.payload,
        )
        receipts.append(receipt)
        roles[role] = receipt.receipt_id

    capture = store.commit_capture(
        provider=provider_id,
        product=product_id,
        fetch_id=f"{fetch}:capture",
        upstream_capture_id=upstream_capture,
        product_timestamp=nominal_at,
        members=roles,
    )
    return NativeCaptureResult(
        method=METHOD,
        capture=capture,
        members=tuple(receipts),
    )


__all__ = [
    "MAX_CAPTURE_MEMBERS",
    "METHOD",
    "NativeCaptureError",
    "NativeCaptureMemberInput",
    "NativeCaptureResult",
    "capture_native_set",
]
