"""Bounded ``operational_snapshot_v1`` control-plane contract.

This module is deliberately pure.  It performs no I/O, imports no Home
Assistant code and never derives meteorology.  The worker produces the full
snapshot; Home Assistant validates it and exposes only indexed, bounded views.

The copy under ``storm_tracker_worker/tracking_core`` must remain byte-for-byte
identical.  Keeping the encoder and validator mirrored prevents either side of
the MQTT boundary from silently accepting a different schema.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
import hashlib
import json
import math
import re
import threading
from types import MappingProxyType
from typing import Any
import unicodedata


CONTRACT = "operational_snapshot_v1"
MESSAGE_TYPE = "operational_snapshot"

RECOMMENDED_CONTROL_ENVELOPE_BYTES = 32 * 1024
MAX_CONTROL_ENVELOPE_BYTES = 64 * 1024
# A producer may never make one control-plane assertion authoritative for
# longer than five minutes.  Consumers validate this bound independently and
# fail closed after ``valid_until``; transport heartbeat health cannot extend it.
MAX_SNAPSHOT_TTL_SECONDS = 5 * 60.0
# When a consumer supplies its receive clock, producer evaluation time may be
# at most one minute ahead.  The receiver also caps effective validity to its
# own five-minute authority window, so clock skew can never extend freshness.
MAX_RECEIVER_FUTURE_SKEW_SECONDS = 60.0

MAX_REGIONS = 16
MAX_TARGETS = 64
MAX_PROVIDERS = 64
MAX_UI_STORM_SUMMARIES = 64
MAX_REFERENCE_IDS = 64
MAX_REASON_CODES = 32
MAX_GROUND_SOURCES = 16

_MAX_COUNTER = 1_000_000_000
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.:@/-]{1,192}$")
_IMMUTABLE_ARTIFACT_HANDLE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_TOKEN_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
_ENVELOPE_KEYS = frozenset({
    "contract",
    "message_type",
    "revision",
    "snapshot_sha256",
    "snapshot",
})
_SNAPSHOT_KEYS = frozenset({
    "contract",
    "revision",
    "desired_state_sha256",
    "product_valid_at",
    "received_at",
    "evaluated_at",
    "valid_until",
    "provenance_complete",
    "provenance_source_ids",
    "provenance_reason_codes",
    "worker",
    "regions",
    "targets",
    "providers",
    "ui_storms",
    "map_artifact",
})
_WORKER_KEYS = frozenset({
    "state",
    "queue_depth",
    "failed_jobs",
    "status_text",
})
_COLLECTION_KEYS = frozenset({"complete", "overflow_count", "items"})
_REGION_KEYS = frozenset({
    "region_id",
    "display_name",
    "state",
    "provider_ids",
    "target_ids",
})
_TARGET_KEYS = frozenset({
    "target_id",
    "display_name",
    "region_id",
    "status_text",
    "current_precipitation",
    "next_threat",
})
_CURRENT_PRECIPITATION_KEYS = frozenset({
    "state",
    "freshness",
    "rain_rate_mm_h",
    "product_valid_at",
    "provider_ids",
    "reason_codes",
    "ground_evidence",
})
_GROUND_EVIDENCE_KEYS = frozenset({
    "status",
    "freshness",
    "confirmation",
    "same_track",
    "evidence_valid_at",
    "observed_at_min",
    "observed_at_max",
    "received_at",
    "physical_site_count",
    "wet_site_count",
    "source_ids",
    "reason_codes",
})
_NEXT_THREAT_KEYS = frozenset({
    "state",
    "freshness",
    "track_id",
    "calibrated_probability_percent",
    "probability_calibration_id",
    "eta_minutes",
    "distance_km",
    "passage_status",
    "product_valid_at",
    "provider_ids",
    "reason_codes",
})
_PROVIDER_KEYS = frozenset({
    "provider_id",
    "display_name",
    "state",
    "freshness",
    "product_valid_at",
    "received_at",
    "error",
    "reason_codes",
})
_PROVIDER_KEYS_WITH_READINESS = _PROVIDER_KEYS | {"readiness"}
_STORM_KEYS = frozenset({
    "storm_id",
    "region_id",
    "display_name",
    "state",
    "diagnostic_quality_percent",
    "diagnostic_quality_kind",
    "motion_speed_kmh",
    "motion_heading_deg",
    "max_dbz",
    "area_km2",
    "product_valid_at",
    "reason_codes",
})
_MAP_KEYS = frozenset({"artifact_handle", "etag"})

_WORKER_STATES = frozenset({
    "online", "busy", "degraded", "offline", "unknown",
})
_FRESHNESS_STATES = frozenset({"current", "stale", "unknown"})
_PROVIDER_STATES = frozenset({
    "operational", "degraded", "offline", "stale", "unknown", "api_required",
})
_PROVIDER_READINESS_STATES = frozenset({"ready", "not_ready"})
_REGION_STATES = frozenset({"current", "degraded", "stale", "unknown"})
_UI_STORM_STATES = frozenset({
    "active", "developing", "mature", "weakening", "dissipating", "unknown",
})
_CURRENT_PRECIPITATION_STATES = frozenset({"unknown", "dry", "raining"})
_NEXT_THREAT_STATES = frozenset({"unknown", "none", "available"})
_PASSAGE_STATES = frozenset({"unknown", "none", "possible", "expected"})
_GROUND_STATES = frozenset({"unknown", "dry", "wet", "mixed"})


class OperationalSnapshotError(ValueError):
    """Base class for a rejected operational snapshot."""


class SnapshotValidationError(OperationalSnapshotError):
    """The envelope or snapshot violates the strict schema."""


class SnapshotSizeError(OperationalSnapshotError):
    """The uncompressed control envelope exceeds 64 KiB."""


class SnapshotCanonicalEncodingError(OperationalSnapshotError):
    """Transport bytes are valid JSON but not the canonical representation."""


class SnapshotRevisionConflict(OperationalSnapshotError):
    """A stale or equivocal revision attempted to replace the cache head."""


class SnapshotCompareAndSwapError(SnapshotRevisionConflict):
    """The caller's expected cache revision did not match the cache head."""


class SnapshotUnavailableError(OperationalSnapshotError):
    """No operational snapshot has been applied yet."""


@dataclass(frozen=True, slots=True)
class SnapshotApplyResult:
    """Result of one atomic cache apply."""

    action: str
    revision: int
    snapshot_sha256: str
    envelope_bytes: int
    exceeds_recommended_bytes: bool

    @property
    def applied(self) -> bool:
        return self.action == "applied"

    @property
    def duplicate(self) -> bool:
        return self.action == "duplicate"


def canonical_json_bytes(value: Any) -> bytes:
    """Return the one accepted UTF-8 JSON representation."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError) as err:
        raise SnapshotValidationError("snapshot is niet canoniek JSON") from err


def _canonical_clone(value: Any) -> Any:
    return json.loads(canonical_json_bytes(value))


def _exact_keys(value: Any, expected: frozenset[str], path: str) -> Mapping:
    if not isinstance(value, Mapping):
        raise SnapshotValidationError(f"{path} moet een object zijn")
    keys = set(value)
    if any(not isinstance(key, str) for key in keys):
        raise SnapshotValidationError(f"{path} bevat een niet-tekstuele sleutel")
    if keys != expected:
        missing = sorted(expected - keys)
        extra = sorted(keys - expected)
        raise SnapshotValidationError(
            f"{path} heeft geen exacte sleutels; ontbreekt={missing}, extra={extra}"
        )
    return value


def _text(
    value: Any,
    path: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise SnapshotValidationError(f"{path} moet tekst zijn")
    if value != unicodedata.normalize("NFC", value):
        raise SnapshotValidationError(f"{path} moet NFC-genormaliseerd zijn")
    if value != value.strip():
        raise SnapshotValidationError(f"{path} bevat randspaties")
    if not allow_empty and not value:
        raise SnapshotValidationError(f"{path} mag niet leeg zijn")
    if len(value) > maximum:
        raise SnapshotValidationError(f"{path} is te lang")
    if any(ord(character) < 32 for character in value):
        raise SnapshotValidationError(f"{path} bevat controletekens")
    return value


def _identifier(value: Any, path: str) -> str:
    text = _text(value, path, maximum=192)
    if _IDENTIFIER_RE.fullmatch(text) is None:
        raise SnapshotValidationError(f"{path} is geen veilig ID")
    if text.lower().startswith(("base64:", "gzip:")):
        raise SnapshotValidationError(f"{path} mag geen inline codec bevatten")
    return text


def _token(value: Any, path: str, *, allowed=None) -> str:
    text = _text(value, path, maximum=48)
    if _TOKEN_RE.fullmatch(text) is None:
        raise SnapshotValidationError(f"{path} is geen statustoken")
    if allowed is not None and text not in allowed:
        raise SnapshotValidationError(f"{path} heeft onbekende status {text!r}")
    return text


def _boolean(value: Any, path: str) -> bool:
    if type(value) is not bool:
        raise SnapshotValidationError(f"{path} moet boolean zijn")
    return value


def _counter(value: Any, path: str, *, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= _MAX_COUNTER:
        raise SnapshotValidationError(f"{path} is geen geldige teller")
    return value


def _finite(
    value: Any,
    path: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    optional: bool = False,
) -> float | int | None:
    if value is None and optional:
        return None
    if type(value) not in {int, float} or not math.isfinite(float(value)):
        raise SnapshotValidationError(f"{path} moet een eindig getal zijn")
    number = float(value)
    if minimum is not None and number < minimum:
        raise SnapshotValidationError(f"{path} is kleiner dan toegestaan")
    if maximum is not None and number > maximum:
        raise SnapshotValidationError(f"{path} is groter dan toegestaan")
    return value


def _optional_boolean(value: Any, path: str) -> bool | None:
    if value is None:
        return None
    return _boolean(value, path)


def _optional_text(value: Any, path: str, *, maximum: int) -> str | None:
    if value is None:
        return None
    return _text(value, path, maximum=maximum)


def _sorted_unique_ids(
    value: Any,
    path: str,
    *,
    maximum_items: int,
) -> list[str]:
    if type(value) is not list:
        raise SnapshotValidationError(f"{path} moet een lijst zijn")
    if len(value) > maximum_items:
        raise SnapshotValidationError(f"{path} overschrijdt de itemlimiet")
    validated = [_identifier(item, f"{path}[{index}]") for index, item in enumerate(value)]
    if validated != sorted(set(validated)):
        raise SnapshotValidationError(f"{path} moet uniek en gesorteerd zijn")
    return validated


def _validate_collection(
    value: Any,
    path: str,
    *,
    maximum_items: int,
    item_validator,
    id_field: str,
) -> list[str]:
    collection = _exact_keys(value, _COLLECTION_KEYS, path)
    complete = _boolean(collection["complete"], f"{path}.complete")
    overflow = _counter(
        collection["overflow_count"], f"{path}.overflow_count"
    )
    if complete != (overflow == 0):
        raise SnapshotValidationError(
            f"{path}.complete en overflow_count spreken elkaar tegen"
        )
    items = collection["items"]
    if type(items) is not list:
        raise SnapshotValidationError(f"{path}.items moet een lijst zijn")
    if len(items) > maximum_items:
        raise SnapshotValidationError(f"{path}.items overschrijdt de limiet")
    identifiers = []
    for index, item in enumerate(items):
        item_validator(item, f"{path}.items[{index}]")
        identifiers.append(item[id_field])
    if identifiers != sorted(set(identifiers)):
        raise SnapshotValidationError(
            f"{path}.items moet uniek op ID en gesorteerd zijn"
        )
    return identifiers


def _validate_region(value: Any, path: str) -> None:
    region = _exact_keys(value, _REGION_KEYS, path)
    _identifier(region["region_id"], f"{path}.region_id")
    _text(region["display_name"], f"{path}.display_name", maximum=160)
    _token(region["state"], f"{path}.state", allowed=_REGION_STATES)
    _sorted_unique_ids(
        region["provider_ids"],
        f"{path}.provider_ids",
        maximum_items=MAX_PROVIDERS,
    )
    _sorted_unique_ids(
        region["target_ids"],
        f"{path}.target_ids",
        maximum_items=MAX_TARGETS,
    )


def _validate_ground_evidence(value: Any, path: str) -> None:
    ground = _exact_keys(value, _GROUND_EVIDENCE_KEYS, path)
    status = _token(
        ground["status"], f"{path}.status", allowed=_GROUND_STATES
    )
    freshness = _token(
        ground["freshness"],
        f"{path}.freshness",
        allowed=_FRESHNESS_STATES,
    )
    confirmation = _optional_boolean(
        ground["confirmation"], f"{path}.confirmation"
    )
    same_track = _optional_boolean(
        ground["same_track"], f"{path}.same_track"
    )
    evidence_valid_at = _finite(
        ground["evidence_valid_at"],
        f"{path}.evidence_valid_at",
        minimum=0,
        optional=True,
    )
    observed_at_min = _finite(
        ground["observed_at_min"],
        f"{path}.observed_at_min",
        minimum=0,
        optional=True,
    )
    observed_at_max = _finite(
        ground["observed_at_max"],
        f"{path}.observed_at_max",
        minimum=0,
        optional=True,
    )
    received_at = _finite(
        ground["received_at"],
        f"{path}.received_at",
        minimum=0,
        optional=True,
    )
    physical_sites = _counter(
        ground["physical_site_count"], f"{path}.physical_site_count"
    )
    wet_sites = _counter(
        ground["wet_site_count"], f"{path}.wet_site_count"
    )
    if wet_sites > physical_sites:
        raise SnapshotValidationError(
            f"{path}.wet_site_count overschrijdt physical_site_count"
        )
    sources = _sorted_unique_ids(
        ground["source_ids"],
        f"{path}.source_ids",
        maximum_items=MAX_GROUND_SOURCES,
    )
    _sorted_unique_ids(
        ground["reason_codes"],
        f"{path}.reason_codes",
        maximum_items=MAX_REASON_CODES,
    )
    if (observed_at_min is None) != (observed_at_max is None):
        raise SnapshotValidationError(
            f"{path} moet observed_at_min en observed_at_max samen leveren"
        )
    if (
        observed_at_min is not None
        and observed_at_max is not None
        and float(observed_at_min) > float(observed_at_max)
    ):
        raise SnapshotValidationError(
            f"{path}.observed_at_min ligt na observed_at_max"
        )
    if physical_sites > 0 and (
        observed_at_min is None
        or evidence_valid_at is None
        or received_at is None
        or not sources
    ):
        raise SnapshotValidationError(
            f"{path} mist tijd- of bronprovenance voor fysieke sites"
        )
    if physical_sites == 0 and wet_sites != 0:
        raise SnapshotValidationError(f"{path} heeft natte sites zonder sites")
    if status == "dry" and (physical_sites == 0 or wet_sites != 0):
        raise SnapshotValidationError(
            f"{path}.dry vereist fysieke sites en nul natte sites"
        )
    if status == "wet" and wet_sites == 0:
        raise SnapshotValidationError(f"{path}.wet vereist minstens één natte site")
    if status == "mixed" and not 0 < wet_sites < physical_sites:
        raise SnapshotValidationError(
            f"{path}.mixed vereist zowel natte als droge fysieke sites"
        )
    if freshness == "unknown":
        if (
            status != "unknown"
            or confirmation is not None
            or same_track is not None
            or physical_sites != 0
            or wet_sites != 0
            or any(
                item is not None
                for item in (
                    evidence_valid_at,
                    observed_at_min,
                    observed_at_max,
                    received_at,
                )
            )
            or sources
        ):
            raise SnapshotValidationError(
                f"{path}.unknown mag geen grondbewijs claimen"
            )
    elif freshness == "stale":
        if confirmation is not None or same_track is not None:
            raise SnapshotValidationError(
                f"{path}.stale vereist confirmation en same_track null"
            )
    else:
        if physical_sites == 0 and (
            confirmation is not None or same_track is not None
        ):
            raise SnapshotValidationError(
                f"{path} kan zonder fysieke sites niets bevestigen"
            )
        if confirmation is True and wet_sites == 0:
            raise SnapshotValidationError(
                f"{path}.confirmation vereist nat grondbewijs"
            )
        if same_track is not None and wet_sites == 0:
            raise SnapshotValidationError(
                f"{path}.same_track vereist nat grondbewijs"
            )


def _validate_current_precipitation(value: Any, path: str) -> None:
    current = _exact_keys(value, _CURRENT_PRECIPITATION_KEYS, path)
    state = _token(
        current["state"],
        f"{path}.state",
        allowed=_CURRENT_PRECIPITATION_STATES,
    )
    freshness = _token(
        current["freshness"],
        f"{path}.freshness",
        allowed=_FRESHNESS_STATES,
    )
    rain_rate = _finite(
        current["rain_rate_mm_h"],
        f"{path}.rain_rate_mm_h",
        minimum=0,
        optional=True,
    )
    product_valid_at = _finite(
        current["product_valid_at"],
        f"{path}.product_valid_at",
        minimum=0,
        optional=True,
    )
    providers = _sorted_unique_ids(
        current["provider_ids"],
        f"{path}.provider_ids",
        maximum_items=MAX_PROVIDERS,
    )
    _sorted_unique_ids(
        current["reason_codes"],
        f"{path}.reason_codes",
        maximum_items=MAX_REASON_CODES,
    )
    _validate_ground_evidence(
        current["ground_evidence"], f"{path}.ground_evidence"
    )
    ground = current["ground_evidence"]
    if state in {"dry", "raining"} and not (
        providers or ground["source_ids"]
    ):
        raise SnapshotValidationError(
            f"{path}.{state} vereist expliciete bronprovenance"
        )
    if freshness != "current":
        if state != "unknown" or rain_rate is not None:
            raise SnapshotValidationError(
                f"{path}.stale/unknown mag geen huidige neerslag claimen"
            )
        if ground["freshness"] == "current":
            raise SnapshotValidationError(
                f"{path} mag geen current grondbewijs onder stale status hangen"
            )
    elif product_valid_at is None:
        raise SnapshotValidationError(
            f"{path} mist product_valid_at voor actuele status"
        )
    if state == "unknown" and rain_rate is not None:
        raise SnapshotValidationError(
            f"{path}.unknown mag geen rain_rate_mm_h claimen"
        )
    if state == "dry" and rain_rate not in {None, 0, 0.0}:
        raise SnapshotValidationError(
            f"{path}.dry mag geen positieve rain_rate_mm_h claimen"
        )
    if ground["confirmation"] is True and state != "raining":
        raise SnapshotValidationError(
            f"{path} kan bevestigd nat grondbewijs niet als niet-raining tonen"
        )


def _validate_next_threat(value: Any, path: str) -> None:
    threat = _exact_keys(value, _NEXT_THREAT_KEYS, path)
    state = _token(
        threat["state"], f"{path}.state", allowed=_NEXT_THREAT_STATES
    )
    freshness = _token(
        threat["freshness"],
        f"{path}.freshness",
        allowed=_FRESHNESS_STATES,
    )
    track_id = threat["track_id"]
    if track_id is not None:
        _identifier(track_id, f"{path}.track_id")
    probability = _finite(
        threat["calibrated_probability_percent"],
        f"{path}.calibrated_probability_percent",
        minimum=0,
        maximum=100,
        optional=True,
    )
    calibration_id = threat["probability_calibration_id"]
    if calibration_id is not None:
        _identifier(calibration_id, f"{path}.probability_calibration_id")
        if not calibration_id.startswith("empirical:"):
            raise SnapshotValidationError(
                f"{path}.probability_calibration_id moet empirisch zijn"
            )
    if (probability is None) != (calibration_id is None):
        raise SnapshotValidationError(
            f"{path} vereist probability en empirisch calibration-ID samen"
        )
    eta = _finite(
        threat["eta_minutes"],
        f"{path}.eta_minutes",
        minimum=0,
        optional=True,
    )
    distance = _finite(
        threat["distance_km"],
        f"{path}.distance_km",
        minimum=0,
        optional=True,
    )
    passage = _token(
        threat["passage_status"],
        f"{path}.passage_status",
        allowed=_PASSAGE_STATES,
    )
    product_valid_at = _finite(
        threat["product_valid_at"],
        f"{path}.product_valid_at",
        minimum=0,
        optional=True,
    )
    providers = _sorted_unique_ids(
        threat["provider_ids"],
        f"{path}.provider_ids",
        maximum_items=MAX_PROVIDERS,
    )
    _sorted_unique_ids(
        threat["reason_codes"],
        f"{path}.reason_codes",
        maximum_items=MAX_REASON_CODES,
    )
    derived = (track_id, probability, calibration_id, eta, distance)
    if freshness != "current":
        if state != "unknown" or passage != "unknown" or any(
            item is not None for item in derived
        ):
            raise SnapshotValidationError(
                f"{path}.stale/unknown mag geen volgende dreiging claimen"
            )
        return
    if state in {"none", "available"} and not providers:
        raise SnapshotValidationError(
            f"{path}.{state} vereist expliciete bronprovenance"
        )
    if product_valid_at is None:
        raise SnapshotValidationError(
            f"{path} mist product_valid_at voor actuele status"
        )
    if state == "unknown":
        if passage != "unknown" or any(item is not None for item in derived):
            raise SnapshotValidationError(
                f"{path}.unknown mag geen dreigingsafleidingen claimen"
            )
    elif state == "none":
        if passage != "none" or any(item is not None for item in derived):
            raise SnapshotValidationError(
                f"{path}.none mag geen track, kans, ETA of afstand claimen"
            )
    else:
        if track_id is None:
            raise SnapshotValidationError(
                f"{path}.available vereist een track_id"
            )
        if passage not in {"unknown", "possible", "expected"}:
            raise SnapshotValidationError(
                f"{path}.available heeft ongeldige passage_status"
            )
        if passage == "expected" and eta is None:
            raise SnapshotValidationError(
                f"{path}.expected vereist eta_minutes"
            )


def _validate_target(value: Any, path: str) -> None:
    target = _exact_keys(value, _TARGET_KEYS, path)
    _identifier(target["target_id"], f"{path}.target_id")
    _text(target["display_name"], f"{path}.display_name", maximum=160)
    _identifier(target["region_id"], f"{path}.region_id")
    _text(
        target["status_text"],
        f"{path}.status_text",
        maximum=256,
        allow_empty=True,
    )
    _validate_current_precipitation(
        target["current_precipitation"], f"{path}.current_precipitation"
    )
    _validate_next_threat(
        target["next_threat"], f"{path}.next_threat"
    )
    if (
        target["current_precipitation"]["freshness"] != "current"
        and target["next_threat"]["freshness"] != "current"
        and target["status_text"]
    ):
        raise SnapshotValidationError(
            f"{path}.status_text moet leeg zijn wanneer beide feiten stale zijn"
        )

def _validate_provider(value: Any, path: str) -> None:
    if not isinstance(value, Mapping):
        raise SnapshotValidationError(f"{path} moet een object zijn")
    expected = (
        _PROVIDER_KEYS_WITH_READINESS
        if "readiness" in value
        else _PROVIDER_KEYS
    )
    provider = _exact_keys(value, expected, path)
    _identifier(provider["provider_id"], f"{path}.provider_id")
    _text(provider["display_name"], f"{path}.display_name", maximum=160)
    state = _token(
        provider["state"], f"{path}.state", allowed=_PROVIDER_STATES
    )
    if "readiness" in provider:
        _token(
            provider["readiness"],
            f"{path}.readiness",
            allowed=_PROVIDER_READINESS_STATES,
        )
    freshness = _token(
        provider["freshness"],
        f"{path}.freshness",
        allowed=_FRESHNESS_STATES,
    )
    if state in {"offline", "stale", "unknown", "api_required"} and freshness == "current":
        raise SnapshotValidationError(
            f"{path} mag voor een niet-operationele provider niet current zijn"
        )
    _finite(
        provider["product_valid_at"],
        f"{path}.product_valid_at",
        minimum=0,
        optional=True,
    )
    _finite(
        provider["received_at"],
        f"{path}.received_at",
        minimum=0,
        optional=True,
    )
    _optional_text(provider["error"], f"{path}.error", maximum=256)
    _sorted_unique_ids(
        provider["reason_codes"],
        f"{path}.reason_codes",
        maximum_items=MAX_REASON_CODES,
    )


def _validate_storm(value: Any, path: str) -> None:
    storm = _exact_keys(value, _STORM_KEYS, path)
    _identifier(storm["storm_id"], f"{path}.storm_id")
    _identifier(storm["region_id"], f"{path}.region_id")
    _text(storm["display_name"], f"{path}.display_name", maximum=160)
    _token(storm["state"], f"{path}.state", allowed=_UI_STORM_STATES)
    diagnostic_quality = _finite(
        storm["diagnostic_quality_percent"],
        f"{path}.diagnostic_quality_percent",
        minimum=0,
        maximum=100,
        optional=True,
    )
    diagnostic_kind = storm["diagnostic_quality_kind"]
    if diagnostic_kind is not None:
        _token(diagnostic_kind, f"{path}.diagnostic_quality_kind")
    if (diagnostic_quality is None) != (diagnostic_kind is None):
        raise SnapshotValidationError(
            f"{path} vereist diagnostic quality en kind samen"
        )
    _finite(
        storm["motion_speed_kmh"],
        f"{path}.motion_speed_kmh",
        minimum=0,
        optional=True,
    )
    heading = _finite(
        storm["motion_heading_deg"],
        f"{path}.motion_heading_deg",
        minimum=0,
        maximum=360,
        optional=True,
    )
    if heading == 360:
        raise SnapshotValidationError(
            f"{path}.motion_heading_deg moet kleiner dan 360 zijn"
        )
    _finite(storm["max_dbz"], f"{path}.max_dbz", optional=True)
    _finite(storm["area_km2"], f"{path}.area_km2", minimum=0, optional=True)
    _finite(
        storm["product_valid_at"],
        f"{path}.product_valid_at",
        minimum=0,
    )
    _sorted_unique_ids(
        storm["reason_codes"],
        f"{path}.reason_codes",
        maximum_items=MAX_REASON_CODES,
    )


def _validate_map_artifact(value: Any, path: str) -> None:
    artifact = _exact_keys(value, _MAP_KEYS, path)
    handle = artifact["artifact_handle"]
    etag = artifact["etag"]
    if (handle is None) != (etag is None):
        raise SnapshotValidationError(
            f"{path} moet handle en ETag samen leveren of beide null maken"
        )
    if handle is not None:
        _identifier(handle, f"{path}.artifact_handle")
        if _IMMUTABLE_ARTIFACT_HANDLE_RE.fullmatch(handle) is None:
            raise SnapshotValidationError(
                f"{path}.artifact_handle moet content-addressed SHA-256 zijn"
            )
        _text(etag, f"{path}.etag", maximum=160)


def validate_snapshot(value: Any) -> dict:
    """Validate and detach one strict operational snapshot."""
    snapshot = _exact_keys(value, _SNAPSHOT_KEYS, "snapshot")
    if snapshot["contract"] != CONTRACT:
        raise SnapshotValidationError("snapshot.contract is niet ondersteund")
    revision = _counter(snapshot["revision"], "snapshot.revision", minimum=1)
    desired_state_sha256 = snapshot["desired_state_sha256"]
    if (
        not isinstance(desired_state_sha256, str)
        or _SHA256_RE.fullmatch(desired_state_sha256) is None
    ):
        raise SnapshotValidationError(
            "snapshot.desired_state_sha256 is ongeldig"
        )
    product_valid_at = _finite(
        snapshot["product_valid_at"],
        "snapshot.product_valid_at",
        minimum=0,
        optional=True,
    )
    received_at = _finite(
        snapshot["received_at"],
        "snapshot.received_at",
        minimum=0,
        optional=True,
    )
    evaluated_at = _finite(
        snapshot["evaluated_at"], "snapshot.evaluated_at", minimum=0
    )
    valid_until = _finite(
        snapshot["valid_until"], "snapshot.valid_until", minimum=0
    )
    if float(evaluated_at) >= float(valid_until):
        raise SnapshotValidationError(
            "snapshot.valid_until moet strikt na evaluated_at liggen"
        )
    if float(valid_until) - float(evaluated_at) > MAX_SNAPSHOT_TTL_SECONDS:
        raise SnapshotValidationError(
            "snapshot geldigheidsduur overschrijdt de harde TTL-grens"
        )
    provenance_complete = _boolean(
        snapshot["provenance_complete"],
        "snapshot.provenance_complete",
    )
    _sorted_unique_ids(
        snapshot["provenance_source_ids"],
        "snapshot.provenance_source_ids",
        maximum_items=MAX_REFERENCE_IDS,
    )
    _sorted_unique_ids(
        snapshot["provenance_reason_codes"],
        "snapshot.provenance_reason_codes",
        maximum_items=MAX_REASON_CODES,
    )

    worker = _exact_keys(snapshot["worker"], _WORKER_KEYS, "snapshot.worker")
    worker_state = _token(
        worker["state"], "snapshot.worker.state", allowed=_WORKER_STATES
    )
    _counter(worker["queue_depth"], "snapshot.worker.queue_depth")
    _counter(worker["failed_jobs"], "snapshot.worker.failed_jobs")
    _text(
        worker["status_text"],
        "snapshot.worker.status_text",
        maximum=256,
        allow_empty=True,
    )

    region_ids = _validate_collection(
        snapshot["regions"],
        "snapshot.regions",
        maximum_items=MAX_REGIONS,
        item_validator=_validate_region,
        id_field="region_id",
    )
    target_ids = _validate_collection(
        snapshot["targets"],
        "snapshot.targets",
        maximum_items=MAX_TARGETS,
        item_validator=_validate_target,
        id_field="target_id",
    )
    provider_ids = _validate_collection(
        snapshot["providers"],
        "snapshot.providers",
        maximum_items=MAX_PROVIDERS,
        item_validator=_validate_provider,
        id_field="provider_id",
    )
    _validate_collection(
        snapshot["ui_storms"],
        "snapshot.ui_storms",
        maximum_items=MAX_UI_STORM_SUMMARIES,
        item_validator=_validate_storm,
        id_field="storm_id",
    )
    _validate_map_artifact(snapshot["map_artifact"], "snapshot.map_artifact")

    region_set = set(region_ids)
    target_set = set(target_ids)
    provider_set = set(provider_ids)
    target_region = {
        item["target_id"]: item["region_id"]
        for item in snapshot["targets"]["items"]
    }
    for item in snapshot["regions"]["items"]:
        unknown_targets = set(item["target_ids"]) - target_set
        unknown_providers = set(item["provider_ids"]) - provider_set
        if unknown_targets or unknown_providers:
            raise SnapshotValidationError(
                f"regio {item['region_id']} verwijst naar onbekende IDs"
            )
        if any(
            target_region[target_id] != item["region_id"]
            for target_id in item["target_ids"]
        ):
            raise SnapshotValidationError(
                f"regio {item['region_id']} bevat target uit andere regio"
            )
    assigned_targets = {
        target_id
        for item in snapshot["regions"]["items"]
        for target_id in item["target_ids"]
    }
    if assigned_targets != target_set:
        raise SnapshotValidationError("niet ieder target hoort exact bij een regio")
    for item in snapshot["targets"]["items"]:
        if item["region_id"] not in region_set:
            raise SnapshotValidationError(
                f"target {item['target_id']} verwijst naar onbekende regio"
            )
        current = item["current_precipitation"]
        threat = item["next_threat"]
        referenced_providers = (
            set(current["provider_ids"])
            | set(current["ground_evidence"]["source_ids"])
            | set(threat["provider_ids"])
        )
        if referenced_providers - provider_set:
            raise SnapshotValidationError(
                f"target {item['target_id']} verwijst naar onbekende provider"
            )
        if current["state"] == "dry" and not provenance_complete:
            raise SnapshotValidationError(
                "dry is verboden zolang provenance onvolledig is"
            )
        if threat["state"] == "none" and not provenance_complete:
            raise SnapshotValidationError(
                "next_threat none is verboden zolang provenance onvolledig is"
            )
        if worker_state in {"offline", "unknown"} and any((
            current["state"] != "unknown",
            current["freshness"] not in {"stale", "unknown"},
            threat["state"] != "unknown",
            threat["freshness"] not in {"stale", "unknown"},
        )):
            raise SnapshotValidationError(
                "offline/unknown worker mag geen actuele targetclaim leveren"
            )
    for item in snapshot["ui_storms"]["items"]:
        if item["region_id"] not in region_set:
            raise SnapshotValidationError(
                f"storm {item['storm_id']} verwijst naar onbekende regio"
            )
    has_current_meteorological_claim = any((
        any(
            item["state"] == "current"
            for item in snapshot["regions"]["items"]
        ),
        any(
            item["current_precipitation"]["freshness"] == "current"
            or item["next_threat"]["freshness"] == "current"
            for item in snapshot["targets"]["items"]
        ),
        any(
            item["freshness"] == "current"
            for item in snapshot["providers"]["items"]
        ),
        any(
            item["state"] != "unknown"
            for item in snapshot["ui_storms"]["items"]
        ),
        snapshot["map_artifact"]["artifact_handle"] is not None,
    ))
    if (
        product_valid_at is None or received_at is None
    ) and has_current_meteorological_claim:
        raise SnapshotValidationError(
            "een actuele meteorologische claim vereist product- en receipttijd"
        )
    if provenance_complete and (
        product_valid_at is None
        or received_at is None
        or not snapshot["provenance_source_ids"]
    ):
        raise SnapshotValidationError(
            "complete provenance vereist producttijd, receipttijd en bron-ID"
        )
    if worker_state in {"offline", "unknown"}:
        if provenance_complete:
            raise SnapshotValidationError(
                "offline/unknown worker kan geen complete provenance claimen"
            )
        if any(
            item["state"] not in {"stale", "unknown"}
            for item in snapshot["regions"]["items"]
        ):
            raise SnapshotValidationError(
                "offline/unknown worker vereist stale/unknown regio's"
            )
        if any(
            item["freshness"] not in {"stale", "unknown"}
            for item in snapshot["providers"]["items"]
        ):
            raise SnapshotValidationError(
                "offline/unknown worker vereist stale/unknown providers"
            )
        if any(
            item["state"] not in {"offline", "stale", "unknown"}
            for item in snapshot["providers"]["items"]
        ):
            raise SnapshotValidationError(
                "offline/unknown worker vereist niet-operationele providers"
            )
        if any(
            item["state"] != "unknown"
            for item in snapshot["ui_storms"]["items"]
        ):
            raise SnapshotValidationError(
                "offline/unknown worker mag geen actuele stormsamenstelling claimen"
            )
        if any(
            any(
                item[key] is not None
                for key in (
                    "diagnostic_quality_percent",
                    "diagnostic_quality_kind",
                    "motion_speed_kmh",
                    "motion_heading_deg",
                    "max_dbz",
                    "area_km2",
                )
            )
            for item in snapshot["ui_storms"]["items"]
        ):
            raise SnapshotValidationError(
                "offline/unknown worker mag geen stormafleidingen claimen"
            )

    # ``revision`` is intentionally read above even though the detached clone
    # is returned: this makes bools and out-of-range revisions fail closed.
    assert revision >= 1
    return _canonical_clone(snapshot)


validate_operational_snapshot = validate_snapshot


def snapshot_sha256(snapshot: Any) -> str:
    """Hash the validated canonical snapshot, excluding its envelope."""
    validated = validate_snapshot(snapshot)
    return hashlib.sha256(canonical_json_bytes(validated)).hexdigest()


def build_control_envelope(snapshot: Any) -> dict:
    """Build the exact, uncompressed control envelope."""
    validated = validate_snapshot(snapshot)
    digest = hashlib.sha256(canonical_json_bytes(validated)).hexdigest()
    envelope = {
        "contract": CONTRACT,
        "message_type": MESSAGE_TYPE,
        "revision": validated["revision"],
        "snapshot_sha256": digest,
        "snapshot": validated,
    }
    encoded = canonical_json_bytes(envelope)
    if len(encoded) > MAX_CONTROL_ENVELOPE_BYTES:
        raise SnapshotSizeError(
            "operational snapshot overschrijdt de harde 64-KiB-grens"
        )
    return envelope


def encode_control_envelope(snapshot: Any) -> bytes:
    """Encode a snapshot without compression or base64."""
    return canonical_json_bytes(build_control_envelope(snapshot))


def _reject_duplicate_keys(pairs) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise SnapshotValidationError(f"dubbele JSON-sleutel: {key}")
        result[key] = value
    return result


def _reject_nonfinite_constant(value: str) -> None:
    raise SnapshotValidationError(f"niet-eindige JSON-waarde: {value}")


def validate_control_envelope(value: Any) -> dict:
    """Validate a decoded envelope and its snapshot digest."""
    envelope = _exact_keys(value, _ENVELOPE_KEYS, "envelope")
    if envelope["contract"] != CONTRACT:
        raise SnapshotValidationError("envelope.contract is niet ondersteund")
    if envelope["message_type"] != MESSAGE_TYPE:
        raise SnapshotValidationError("envelope.message_type is ongeldig")
    revision = _counter(envelope["revision"], "envelope.revision", minimum=1)
    digest = envelope["snapshot_sha256"]
    if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
        raise SnapshotValidationError("envelope.snapshot_sha256 is ongeldig")
    snapshot = validate_snapshot(envelope["snapshot"])
    if snapshot["revision"] != revision:
        raise SnapshotValidationError("envelope- en snapshotrevision verschillen")
    actual_digest = hashlib.sha256(canonical_json_bytes(snapshot)).hexdigest()
    if digest != actual_digest:
        raise SnapshotValidationError("snapshotdigest komt niet overeen")
    detached = {
        "contract": CONTRACT,
        "message_type": MESSAGE_TYPE,
        "revision": revision,
        "snapshot_sha256": digest,
        "snapshot": snapshot,
    }
    if len(canonical_json_bytes(detached)) > MAX_CONTROL_ENVELOPE_BYTES:
        raise SnapshotSizeError(
            "operational snapshot overschrijdt de harde 64-KiB-grens"
        )
    return detached


def decode_control_envelope(payload: bytes | bytearray | memoryview | str) -> dict:
    """Decode only canonical, uncompressed envelope bytes under 64 KiB."""
    if isinstance(payload, str):
        try:
            raw = payload.encode("utf-8")
        except UnicodeEncodeError as err:
            raise SnapshotValidationError(
                "control envelope is geen geldige UTF-8"
            ) from err
    elif isinstance(payload, (bytes, bytearray, memoryview)):
        raw = bytes(payload)
    else:
        raise TypeError("control envelope moet UTF-8 bytes of tekst zijn")
    if not raw:
        raise SnapshotValidationError("control envelope is leeg")
    if len(raw) > MAX_CONTROL_ENVELOPE_BYTES:
        raise SnapshotSizeError(
            "operational snapshot overschrijdt de harde 64-KiB-grens"
        )
    try:
        decoded = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonfinite_constant,
        )
    except UnicodeDecodeError as err:
        raise SnapshotValidationError("control envelope is geen UTF-8") from err
    except (json.JSONDecodeError, RecursionError) as err:
        raise SnapshotValidationError("control envelope is geen geldige JSON") from err
    envelope = validate_control_envelope(decoded)
    canonical = canonical_json_bytes(envelope)
    if raw != canonical:
        raise SnapshotCanonicalEncodingError(
            "control envelope is niet exact canoniek gecodeerd"
        )
    return envelope


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(nested) for key, nested in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(nested) for nested in value)
    return value


def _copy_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _copy_value(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return [_copy_value(nested) for nested in value]
    return value


def _copy_record(value: Mapping | None) -> dict | None:
    if value is None:
        return None
    return _copy_value(value)


def _append_reason(record: dict, reason: str) -> None:
    reasons = set(record.get("reason_codes") or [])
    reasons.discard(reason)
    retained = sorted(reasons)[:MAX_REASON_CODES - 1]
    record["reason_codes"] = sorted([*retained, reason])


@dataclass(frozen=True, slots=True)
class _CacheState:
    revision: int
    digest: str
    envelope_bytes: int
    exceeds_recommended_bytes: bool
    availability: str
    status: Mapping
    regions: Mapping
    targets: Mapping
    providers: Mapping
    storms: Mapping
    map_artifact: Mapping
    collection_status: Mapping
    stale_reason: str | None


def _index(items: list[dict], id_field: str) -> Mapping:
    return MappingProxyType({
        item[id_field]: _freeze(item)
        for item in items
    })


def _collection_status(snapshot: dict) -> Mapping:
    return MappingProxyType({
        name: MappingProxyType({
            "complete": snapshot[name]["complete"],
            "overflow_count": snapshot[name]["overflow_count"],
            "item_count": len(snapshot[name]["items"]),
        })
        for name in ("regions", "targets", "providers", "ui_storms")
    })


def _receiver_valid_until(
    snapshot: Mapping,
    *,
    receiver_now: float | None,
    max_future_skew_seconds: float,
) -> float:
    producer_valid_until = float(snapshot["valid_until"])
    if receiver_now is None:
        return producer_valid_until
    received_at = _finite(receiver_now, "receiver_now", minimum=0)
    future_skew = _finite(
        max_future_skew_seconds,
        "max_future_skew_seconds",
        minimum=0,
    )
    if float(snapshot["evaluated_at"]) > float(received_at) + float(future_skew):
        raise SnapshotValidationError(
            "snapshot.evaluated_at overschrijdt de ontvangende klokgrens"
        )
    return min(
        producer_valid_until,
        float(received_at) + MAX_SNAPSHOT_TTL_SECONDS,
    )


def _state_from_envelope(
    envelope: dict,
    envelope_bytes: int,
    *,
    effective_valid_until: float | None = None,
) -> _CacheState:
    snapshot = envelope["snapshot"]
    worker_state = snapshot["worker"]["state"]
    availability = (
        "current" if worker_state in {"online", "busy", "degraded"} else "stale"
    )
    status = MappingProxyType({
        "revision": snapshot["revision"],
        "desired_state_sha256": snapshot["desired_state_sha256"],
        "snapshot_sha256": envelope["snapshot_sha256"],
        "availability": availability,
        "product_valid_at": snapshot["product_valid_at"],
        "received_at": snapshot["received_at"],
        "evaluated_at": snapshot["evaluated_at"],
        "valid_until": (
            snapshot["valid_until"]
            if effective_valid_until is None
            else effective_valid_until
        ),
        "provenance_complete": snapshot["provenance_complete"],
        "worker_state": worker_state,
        "recommended_size_exceeded": (
            envelope_bytes > RECOMMENDED_CONTROL_ENVELOPE_BYTES
        ),
    })
    return _CacheState(
        revision=snapshot["revision"],
        digest=envelope["snapshot_sha256"],
        envelope_bytes=envelope_bytes,
        exceeds_recommended_bytes=(
            envelope_bytes > RECOMMENDED_CONTROL_ENVELOPE_BYTES
        ),
        availability=availability,
        status=status,
        regions=_index(snapshot["regions"]["items"], "region_id"),
        targets=_index(snapshot["targets"]["items"], "target_id"),
        providers=_index(snapshot["providers"]["items"], "provider_id"),
        storms=_index(snapshot["ui_storms"]["items"], "storm_id"),
        map_artifact=_freeze(snapshot["map_artifact"]),
        collection_status=_collection_status(snapshot),
        stale_reason=None,
    )


def _degraded_state(state: _CacheState, *, reason: str, worker_state: str) -> _CacheState:
    targets = {}
    for target_id, frozen in state.targets.items():
        target = _copy_record(frozen)
        target["status_text"] = (
            "Snapshot verlopen; actuele toestand is onbekend"
            if reason == "snapshot_expired"
            else "Worker offline; laatste snapshot is stale"
        )
        current = target["current_precipitation"]
        current.update({
            "state": "unknown",
            "freshness": "stale",
            "rain_rate_mm_h": None,
        })
        _append_reason(current, reason)
        ground = current["ground_evidence"]
        ground.update({
            "freshness": "stale",
            "confirmation": None,
            "same_track": None,
        })
        _append_reason(ground, reason)
        threat = target["next_threat"]
        threat.update({
            "state": "unknown",
            "freshness": "stale",
            "track_id": None,
            "calibrated_probability_percent": None,
            "probability_calibration_id": None,
            "eta_minutes": None,
            "distance_km": None,
            "passage_status": "unknown",
        })
        _append_reason(threat, reason)
        targets[target_id] = _freeze(target)

    providers = {}
    for provider_id, frozen in state.providers.items():
        provider = _copy_record(frozen)
        provider.update({
            "state": "offline" if worker_state == "offline" else "unknown",
            "freshness": "stale",
            "error": reason,
        })
        _append_reason(provider, reason)
        providers[provider_id] = _freeze(provider)

    regions = {}
    for region_id, frozen in state.regions.items():
        region = _copy_record(frozen)
        region["state"] = "stale"
        regions[region_id] = _freeze(region)

    storms = {}
    for storm_id, frozen in state.storms.items():
        storm = _copy_record(frozen)
        storm.update({
            "state": "unknown",
            "diagnostic_quality_percent": None,
            "diagnostic_quality_kind": None,
            "motion_speed_kmh": None,
            "motion_heading_deg": None,
            "max_dbz": None,
            "area_km2": None,
        })
        _append_reason(storm, reason)
        storms[storm_id] = _freeze(storm)

    status = dict(state.status)
    status.update({
        "availability": "stale",
        "provenance_complete": False,
        "worker_state": worker_state,
    })
    return replace(
        state,
        availability="stale",
        status=MappingProxyType(status),
        regions=MappingProxyType(regions),
        targets=MappingProxyType(targets),
        providers=MappingProxyType(providers),
        storms=MappingProxyType(storms),
        stale_reason=reason,
    )


class OperationalSnapshotCache:
    """Thread-safe, atomic, in-memory read model for Home Assistant.

    Validation and index construction happen before the short compare-and-swap
    lock.  Property reads only capture the immutable state reference, perform a
    dictionary lookup and return a bounded shallow copy.
    """

    __slots__ = ("_lock", "_state")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: _CacheState | None = None

    @property
    def revision(self) -> int:
        state = self._state
        return 0 if state is None else state.revision

    @property
    def snapshot_sha256(self) -> str | None:
        state = self._state
        return None if state is None else state.digest

    @property
    def availability(self) -> str:
        state = self._state
        return "unknown" if state is None else state.availability

    @property
    def valid_until(self) -> float | None:
        """Return the producer-bounded expiry of the installed cache head."""
        state = self._state
        if state is None:
            return None
        return float(state.status["valid_until"])

    def is_expired(self, *, now: float) -> bool:
        """Check expiry without mutating or extending the cache head."""
        checked_at = _finite(now, "now", minimum=0)
        valid_until = self.valid_until
        return valid_until is not None and float(checked_at) >= valid_until

    def apply_bytes(
        self,
        payload: bytes | bytearray | memoryview | str,
        *,
        expected_revision: int | None = None,
        expected_desired_state_sha256: str | None = None,
        receiver_now: float | None = None,
        max_future_skew_seconds: float = MAX_RECEIVER_FUTURE_SKEW_SECONDS,
    ) -> SnapshotApplyResult:
        envelope = decode_control_envelope(payload)
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        return self._apply_validated(
            envelope,
            envelope_bytes=len(raw),
            expected_revision=expected_revision,
            expected_desired_state_sha256=expected_desired_state_sha256,
            receiver_now=receiver_now,
            max_future_skew_seconds=max_future_skew_seconds,
        )

    def apply_envelope(
        self,
        envelope: Mapping,
        *,
        expected_revision: int | None = None,
        expected_desired_state_sha256: str | None = None,
        receiver_now: float | None = None,
        max_future_skew_seconds: float = MAX_RECEIVER_FUTURE_SKEW_SECONDS,
    ) -> SnapshotApplyResult:
        validated = validate_control_envelope(envelope)
        envelope_bytes = len(canonical_json_bytes(validated))
        return self._apply_validated(
            validated,
            envelope_bytes=envelope_bytes,
            expected_revision=expected_revision,
            expected_desired_state_sha256=expected_desired_state_sha256,
            receiver_now=receiver_now,
            max_future_skew_seconds=max_future_skew_seconds,
        )

    def apply_snapshot(
        self,
        snapshot: Mapping,
        *,
        expected_revision: int | None = None,
        expected_desired_state_sha256: str | None = None,
        receiver_now: float | None = None,
        max_future_skew_seconds: float = MAX_RECEIVER_FUTURE_SKEW_SECONDS,
    ) -> SnapshotApplyResult:
        return self.apply_bytes(
            encode_control_envelope(snapshot),
            expected_revision=expected_revision,
            expected_desired_state_sha256=expected_desired_state_sha256,
            receiver_now=receiver_now,
            max_future_skew_seconds=max_future_skew_seconds,
        )

    def _apply_validated(
        self,
        envelope: dict,
        *,
        envelope_bytes: int,
        expected_revision: int | None,
        expected_desired_state_sha256: str | None,
        receiver_now: float | None,
        max_future_skew_seconds: float,
    ) -> SnapshotApplyResult:
        if expected_revision is not None:
            _counter(expected_revision, "expected_revision", minimum=0)
        if expected_desired_state_sha256 is not None and (
            not isinstance(expected_desired_state_sha256, str)
            or _SHA256_RE.fullmatch(expected_desired_state_sha256) is None
        ):
            raise SnapshotValidationError(
                "expected_desired_state_sha256 is ongeldig"
            )
        if (
            expected_desired_state_sha256 is not None
            and envelope["snapshot"]["desired_state_sha256"]
            != expected_desired_state_sha256
        ):
            raise SnapshotRevisionConflict(
                "snapshot hoort bij een andere desired-state-authoriteit"
            )
        effective_valid_until = _receiver_valid_until(
            envelope["snapshot"],
            receiver_now=receiver_now,
            max_future_skew_seconds=max_future_skew_seconds,
        )
        candidate = _state_from_envelope(
            envelope,
            envelope_bytes,
            effective_valid_until=effective_valid_until,
        )
        with self._lock:
            current = self._state
            current_revision = 0 if current is None else current.revision
            if (
                expected_revision is not None
                and expected_revision != current_revision
            ):
                raise SnapshotCompareAndSwapError(
                    f"cache staat op revision {current_revision}, niet {expected_revision}"
                )
            if current is not None and candidate.revision < current.revision:
                raise SnapshotRevisionConflict(
                    "oudere operational snapshot mag de cache niet vervangen"
                )
            if current is not None and candidate.revision == current.revision:
                if candidate.digest != current.digest:
                    raise SnapshotRevisionConflict(
                        "dezelfde revision heeft een andere snapshotdigest"
                    )
                return SnapshotApplyResult(
                    action="duplicate",
                    revision=current.revision,
                    snapshot_sha256=current.digest,
                    envelope_bytes=envelope_bytes,
                    exceeds_recommended_bytes=(
                        envelope_bytes > RECOMMENDED_CONTROL_ENVELOPE_BYTES
                    ),
                )
            self._state = candidate
        return SnapshotApplyResult(
            action="applied",
            revision=candidate.revision,
            snapshot_sha256=candidate.digest,
            envelope_bytes=envelope_bytes,
            exceeds_recommended_bytes=candidate.exceeds_recommended_bytes,
        )

    def mark_worker_offline(self) -> bool:
        """Atomically degrade all active views without deriving weather."""
        return self.mark_stale(reason="worker_offline", worker_state="offline")

    def mark_expired(self, *, now: float) -> bool:
        """Atomically test and degrade one expired non-offline cache head."""
        checked_at = _finite(now, "now", minimum=0)
        with self._lock:
            current = self._state
            if current is None or current.status["worker_state"] == "offline":
                return False
            if float(checked_at) < float(current.status["valid_until"]):
                return False
            if (
                current.availability == "stale"
                and current.status["worker_state"] == "unknown"
                and current.stale_reason == "snapshot_expired"
            ):
                return False
            self._state = _degraded_state(
                current,
                reason="snapshot_expired",
                worker_state="unknown",
            )
            return True

    def mark_expired_head(
        self,
        *,
        snapshot_sha256: str,
        valid_until: float,
    ) -> bool:
        """Expire exactly the cache head captured by a monotonic timer.

        The bridge converts the accepted wall-clock deadline to one
        ``call_later`` delay immediately after receipt.  This compare-and-swap
        method therefore must not consult the wall clock again: a later cache
        head, an offline transition or an authority transition can never be
        overwritten by an old timer callback.
        """
        if (
            not isinstance(snapshot_sha256, str)
            or _SHA256_RE.fullmatch(snapshot_sha256) is None
        ):
            raise SnapshotValidationError("snapshot_sha256 is ongeldig")
        deadline = _finite(valid_until, "valid_until", minimum=0)
        with self._lock:
            current = self._state
            if (
                current is None
                or current.digest != snapshot_sha256
                or float(current.status["valid_until"]) != float(deadline)
                or current.availability != "current"
                or current.stale_reason is not None
                or current.status["worker_state"] == "offline"
            ):
                return False
            self._state = _degraded_state(
                current,
                reason="snapshot_expired",
                worker_state="unknown",
            )
            return True

    def mark_stale(
        self,
        *,
        reason: str = "snapshot_stale",
        worker_state: str = "unknown",
    ) -> bool:
        reason = _identifier(reason, "reason")
        worker_state = _token(
            worker_state, "worker_state", allowed={"offline", "unknown"}
        )
        with self._lock:
            current = self._state
            if current is None:
                return False
            if (
                current.availability == "stale"
                and current.status["worker_state"] == worker_state
                and current.stale_reason == reason
            ):
                return False
            self._state = _degraded_state(
                current,
                reason=reason,
                worker_state=worker_state,
            )
            return True

    def get_status(self) -> dict:
        state = self._state
        if state is None:
            return {
                "revision": 0,
                "snapshot_sha256": None,
                "desired_state_sha256": None,
                "availability": "unknown",
                "product_valid_at": None,
                "received_at": None,
                "evaluated_at": None,
                "valid_until": None,
                "provenance_complete": False,
                "worker_state": "unknown",
                "recommended_size_exceeded": False,
            }
        return dict(state.status)

    def get_collection_status(self, name: str) -> dict | None:
        state = self._state
        if state is None or name not in state.collection_status:
            return None
        return dict(state.collection_status[name])

    def get_region(self, region_id: str) -> dict | None:
        state = self._state
        if state is None or not isinstance(region_id, str):
            return None
        return _copy_record(state.regions.get(region_id))

    def get_target(self, target_id: str) -> dict | None:
        state = self._state
        if state is None or not isinstance(target_id, str):
            return None
        return _copy_record(state.targets.get(target_id))

    def get_provider(self, provider_id: str) -> dict | None:
        state = self._state
        if state is None or not isinstance(provider_id, str):
            return None
        return _copy_record(state.providers.get(provider_id))

    def get_ui_storm(self, storm_id: str) -> dict | None:
        state = self._state
        if state is None or not isinstance(storm_id, str):
            return None
        return _copy_record(state.storms.get(storm_id))

    def get_map_artifact(self) -> dict | None:
        state = self._state
        if state is None:
            return None
        return _copy_record(state.map_artifact)


__all__ = [
    "CONTRACT",
    "MAX_CONTROL_ENVELOPE_BYTES",
    "MAX_RECEIVER_FUTURE_SKEW_SECONDS",
    "MAX_SNAPSHOT_TTL_SECONDS",
    "MAX_PROVIDERS",
    "MAX_REGIONS",
    "MAX_TARGETS",
    "MAX_UI_STORM_SUMMARIES",
    "MESSAGE_TYPE",
    "OperationalSnapshotCache",
    "OperationalSnapshotError",
    "RECOMMENDED_CONTROL_ENVELOPE_BYTES",
    "SnapshotApplyResult",
    "SnapshotCanonicalEncodingError",
    "SnapshotCompareAndSwapError",
    "SnapshotRevisionConflict",
    "SnapshotSizeError",
    "SnapshotUnavailableError",
    "SnapshotValidationError",
    "build_control_envelope",
    "canonical_json_bytes",
    "decode_control_envelope",
    "encode_control_envelope",
    "snapshot_sha256",
    "validate_control_envelope",
    "validate_operational_snapshot",
    "validate_snapshot",
]
