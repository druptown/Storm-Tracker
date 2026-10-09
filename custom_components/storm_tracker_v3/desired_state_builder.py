"""Pure Home Assistant-side desired-state contract builder candidate.

The module deliberately has no Home Assistant, provider, storage, network or
MQTT imports.  It only validates bounded caller-owned values and produces the
exact canonical bytes accepted by ``storm_tracker_worker.desired_state``.
Nothing in this module publishes, registers or persists those bytes.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Any
import unicodedata


CONTRACT = "stv3-worker-desired-state-v1"
MAX_CONTRACT_BYTES = 64 * 1024
MAX_TARGETS = 64

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$")
_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")
_SECRET_KEY_RE = re.compile(
    r"(?:^|[-_.:@/])(?:access[-_]?token|refresh[-_]?token|authorization|"
    r"cookie|client[-_]?(?:id|secret)|application[-_]?key|api[-_]?key|"
    r"private[-_]?key|password|secret)(?:$|[-_.:@/])",
    re.IGNORECASE,
)
_SECRET_VALUE_RE = re.compile(
    r"(?:^bearer\s+|-----BEGIN [A-Z ]*PRIVATE KEY-----|"
    r"(?:access_token|refresh_token|client_secret|api_key)=)",
    re.IGNORECASE,
)
_JWT_RE = re.compile(r"^eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}$")

_ROOT_KEYS = frozenset({
    "contract",
    "producer_id",
    "authority_epoch",
    "generation",
    "issued_at",
    "previous_state_sha256",
    "targets",
    "settings",
})
_TARGET_KEYS = frozenset({
    "target_id",
    "display_name",
    "kind",
    "available",
    "latitude",
    "longitude",
    "position_observed_at",
    "location_accuracy_m",
    "observation_radius_km",
    "country_code",
})
_SETTINGS_KEYS = frozenset({
    "ground_radius_km",
    "lightning_source_mode",
    "model_guidance_enabled",
    "region_sharing_distance_km",
})
_TARGET_KINDS = frozenset({
    "home",
    "person",
    "test",
    "fixed",
    "device_tracker",
})
_LIGHTNING_SOURCE_MODES = frozenset({"auto", "satellite_test"})


class DesiredStateBuildError(ValueError):
    """The caller input cannot safely form the worker contract."""


class DesiredStateSizeError(DesiredStateBuildError):
    """The canonical uncompressed document exceeds 64 KiB."""


@dataclass(frozen=True, slots=True)
class DesiredStateEnvelope:
    """Exact transport bytes plus the SHA-256 CAS identity they define."""

    canonical_bytes: bytes
    envelope_sha256: str
    producer_id: str
    authority_epoch: int
    generation: int
    previous_state_sha256: str | None
    target_count: int

    @property
    def state_sha256(self) -> str:
        """Worker terminology for the same exact canonical-byte digest."""

        return self.envelope_sha256

    def as_dict(self) -> dict[str, Any]:
        """Return a detached JSON-compatible copy of the canonical document."""

        return json.loads(self.canonical_bytes)


def canonical_json_bytes(value: Any) -> bytes:
    """Encode the one worker-compatible UTF-8 JSON representation."""

    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError) as exc:
        raise DesiredStateBuildError(
            "desired-state kan niet canoniek als UTF-8 JSON worden gecodeerd"
        ) from exc


def _exact_keys(value: Any, expected: frozenset[str], path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DesiredStateBuildError(f"{path} moet een object zijn")
    actual = set(value)
    if any(not isinstance(key, str) for key in actual):
        raise DesiredStateBuildError(f"{path} bevat een niet-tekstuele sleutel")
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise DesiredStateBuildError(
            f"{path} heeft ongeldige sleutels; ontbrekend={missing}, extra={extra}"
        )
    return value


def _reject_secret_surface(value: Any, path: str = "desired-state") -> None:
    """Reject credential-shaped fields/strings without entropy guessing."""

    if isinstance(value, Mapping):
        if len(value) > 256:
            raise DesiredStateBuildError(f"{path} overschrijdt de objectgrens")
        for key, item in value.items():
            if not isinstance(key, str):
                raise DesiredStateBuildError(f"{path} bevat een niet-tekstuele sleutel")
            if _SECRET_KEY_RE.search(key):
                raise DesiredStateBuildError(
                    f"{path} bevat een verboden credentialveld"
                )
            _reject_secret_surface(item, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_TARGETS:
            raise DesiredStateBuildError(f"{path} overschrijdt de lijstgrens")
        for index, item in enumerate(value):
            _reject_secret_surface(item, f"{path}[{index}]")
        return
    if isinstance(value, str):
        if value != unicodedata.normalize("NFC", value):
            raise DesiredStateBuildError(f"{path} moet NFC-genormaliseerd zijn")
        if _SECRET_VALUE_RE.search(value) or _JWT_RE.fullmatch(value):
            raise DesiredStateBuildError(
                f"{path} bevat credentialvormige tekst"
            )


def _identifier(value: Any, path: str, *, maximum: int = 160) -> str:
    if not isinstance(value, str):
        raise DesiredStateBuildError(f"{path} moet tekst zijn")
    result = value.strip()
    if (
        not result
        or len(result) > maximum
        or _IDENTIFIER_RE.fullmatch(result) is None
        or _SECRET_KEY_RE.search(result)
    ):
        raise DesiredStateBuildError(f"{path} is geen veilig begrensd ID")
    return result


def _display_text(value: Any, path: str, *, maximum: int = 128) -> str:
    if not isinstance(value, str):
        raise DesiredStateBuildError(f"{path} moet tekst zijn")
    if value != unicodedata.normalize("NFC", value):
        raise DesiredStateBuildError(f"{path} moet NFC-genormaliseerd zijn")
    if (
        not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise DesiredStateBuildError(f"{path} is geen begrensde tekst")
    return value


def _number(value: Any, path: str, *, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DesiredStateBuildError(f"{path} moet een getal zijn")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise DesiredStateBuildError(
            f"{path} moet eindig en binnen [{minimum}, {maximum}] zijn"
        )
    return result


def _optional_number(
    value: Any,
    path: str,
    *,
    minimum: float,
    maximum: float,
) -> float | None:
    if value is None:
        return None
    return _number(value, path, minimum=minimum, maximum=maximum)


def _integer(value: Any, path: str, *, minimum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= 2**63 - 1
    ):
        raise DesiredStateBuildError(f"{path} is geen geldige 64-bit integer")
    return value


def _target(value: Any, index: int) -> dict[str, Any]:
    path = f"targets[{index}]"
    target = _exact_keys(value, _TARGET_KEYS, path)
    kind = target["kind"]
    if not isinstance(kind, str) or kind not in _TARGET_KINDS:
        raise DesiredStateBuildError(f"{path}.kind is niet ondersteund")
    available = target["available"]
    if type(available) is not bool:
        raise DesiredStateBuildError(f"{path}.available moet boolean zijn")
    observed_at = _optional_number(
        target["position_observed_at"],
        f"{path}.position_observed_at",
        minimum=0.0,
        maximum=float(2**53),
    )
    if available and observed_at is None:
        raise DesiredStateBuildError(
            f"{path} is beschikbaar maar mist position_observed_at"
        )
    accuracy = _optional_number(
        target["location_accuracy_m"],
        f"{path}.location_accuracy_m",
        minimum=0.0,
        maximum=100_000.0,
    )
    country = target["country_code"]
    if country is not None and (
        not isinstance(country, str) or _COUNTRY_RE.fullmatch(country) is None
    ):
        raise DesiredStateBuildError(
            f"{path}.country_code moet null of ISO alpha-2 zijn"
        )
    return {
        "available": available,
        "country_code": country,
        "display_name": _display_text(
            target["display_name"], f"{path}.display_name"
        ),
        "kind": kind,
        "latitude": _number(
            target["latitude"], f"{path}.latitude", minimum=-90.0, maximum=90.0
        ),
        "location_accuracy_m": accuracy,
        "longitude": _number(
            target["longitude"],
            f"{path}.longitude",
            minimum=-180.0,
            maximum=180.0,
        ),
        "observation_radius_km": _number(
            target["observation_radius_km"],
            f"{path}.observation_radius_km",
            minimum=1.0,
            maximum=1000.0,
        ),
        "position_observed_at": observed_at,
        "target_id": _identifier(target["target_id"], f"{path}.target_id"),
    }


def _settings(value: Any) -> dict[str, Any]:
    settings = _exact_keys(value, _SETTINGS_KEYS, "settings")
    lightning_mode = settings["lightning_source_mode"]
    if (
        not isinstance(lightning_mode, str)
        or lightning_mode not in _LIGHTNING_SOURCE_MODES
    ):
        raise DesiredStateBuildError(
            "settings.lightning_source_mode is niet ondersteund"
        )
    model_enabled = settings["model_guidance_enabled"]
    if type(model_enabled) is not bool:
        raise DesiredStateBuildError(
            "settings.model_guidance_enabled moet boolean zijn"
        )
    return {
        "ground_radius_km": _number(
            settings["ground_radius_km"],
            "settings.ground_radius_km",
            minimum=1.0,
            maximum=100.0,
        ),
        "lightning_source_mode": lightning_mode,
        "model_guidance_enabled": model_enabled,
        "region_sharing_distance_km": _number(
            settings["region_sharing_distance_km"],
            "settings.region_sharing_distance_km",
            minimum=1.0,
            maximum=500.0,
        ),
    }


def build_desired_state(
    *,
    producer_id: str,
    authority_epoch: int,
    generation: int,
    issued_at: int | float,
    previous_state_sha256: str | None,
    targets: Sequence[Mapping[str, Any]],
    settings: Mapping[str, Any],
) -> DesiredStateEnvelope:
    """Validate, sort and encode one worker desired-state CAS document."""

    if isinstance(targets, (str, bytes, bytearray)) or not isinstance(
        targets, Sequence
    ):
        raise DesiredStateBuildError("targets moet een begrensde sequence zijn")
    if len(targets) > MAX_TARGETS:
        raise DesiredStateBuildError("targets overschrijdt de limiet van 64")
    supplied = {
        "contract": CONTRACT,
        "producer_id": producer_id,
        "authority_epoch": authority_epoch,
        "generation": generation,
        "issued_at": issued_at,
        "previous_state_sha256": previous_state_sha256,
        "targets": targets,
        "settings": settings,
    }
    _reject_secret_surface(supplied)
    _exact_keys(supplied, _ROOT_KEYS, "desired-state")
    producer = _identifier(producer_id, "producer_id")
    epoch = _integer(authority_epoch, "authority_epoch", minimum=1)
    state_generation = _integer(generation, "generation", minimum=0)
    state_issued_at = _number(
        issued_at, "issued_at", minimum=0.0, maximum=float(2**53)
    )
    if previous_state_sha256 is not None and (
        not isinstance(previous_state_sha256, str)
        or _SHA256_RE.fullmatch(previous_state_sha256) is None
    ):
        raise DesiredStateBuildError("previous_state_sha256 is ongeldig")
    normalized_targets = [_target(item, index) for index, item in enumerate(targets)]
    normalized_targets.sort(key=lambda item: item["target_id"])
    target_ids = [item["target_id"] for item in normalized_targets]
    if len(target_ids) != len(set(target_ids)):
        raise DesiredStateBuildError("target_id moet uniek zijn")
    canonical_object = {
        "authority_epoch": epoch,
        "contract": CONTRACT,
        "generation": state_generation,
        "issued_at": state_issued_at,
        "previous_state_sha256": previous_state_sha256,
        "producer_id": producer,
        "settings": _settings(settings),
        "targets": normalized_targets,
    }
    canonical = canonical_json_bytes(canonical_object)
    if len(canonical) > MAX_CONTRACT_BYTES:
        raise DesiredStateSizeError(
            "canonieke desired-state overschrijdt het 64-KiB bytebudget"
        )
    digest = hashlib.sha256(canonical).hexdigest()
    return DesiredStateEnvelope(
        canonical_bytes=canonical,
        envelope_sha256=digest,
        producer_id=producer,
        authority_epoch=epoch,
        generation=state_generation,
        previous_state_sha256=previous_state_sha256,
        target_count=len(normalized_targets),
    )


def build_desired_state_document(value: Mapping[str, Any]) -> DesiredStateEnvelope:
    """Build from one exact closed document, rejecting root-level extras."""

    _reject_secret_surface(value)
    document = _exact_keys(value, _ROOT_KEYS, "desired-state")
    if document["contract"] != CONTRACT:
        raise DesiredStateBuildError(
            "desired-state contractversie is niet ondersteund"
        )
    return build_desired_state(
        producer_id=document["producer_id"],
        authority_epoch=document["authority_epoch"],
        generation=document["generation"],
        issued_at=document["issued_at"],
        previous_state_sha256=document["previous_state_sha256"],
        targets=document["targets"],
        settings=document["settings"],
    )


__all__ = [
    "CONTRACT",
    "DesiredStateBuildError",
    "DesiredStateEnvelope",
    "DesiredStateSizeError",
    "MAX_CONTRACT_BYTES",
    "MAX_TARGETS",
    "build_desired_state",
    "build_desired_state_document",
    "canonical_json_bytes",
]
