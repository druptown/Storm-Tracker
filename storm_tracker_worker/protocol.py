"""Versioned MQTT contract shared by the standalone worker."""
from __future__ import annotations

import base64
import json
import time
from typing import Any

from .tracking_core.engine.bounded_payload import decompress_limited

CONTRACT_VERSION = 1
DEFAULT_TOPIC_ROOT = "storm_tracker/v1"
# Operational tracking contains the shared raw observation history for two
# region engines. Keep a hard bound, but leave enough room for one complete
# batch so HA never has to split a coherent tracking state.
MAX_PAYLOAD_BYTES = 1_000_000
MAX_UNCOMPRESSED_JOB_BYTES = 32_000_000


def topic(root: str, suffix: str) -> str:
    return f"{root.rstrip('/')}/{suffix.lstrip('/')}"


def encode(message: dict[str, Any]) -> bytes:
    payload = json.dumps(
        message, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    if len(payload) > MAX_PAYLOAD_BYTES:
        raise ValueError("MQTT payload overschrijdt contractlimiet")
    return payload


def decode(payload: bytes | str) -> dict[str, Any]:
    if isinstance(payload, bytes):
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise ValueError("MQTT payload overschrijdt contractlimiet")
        payload = payload.decode("utf-8")
    message = json.loads(payload)
    if not isinstance(message, dict):
        raise ValueError("MQTT-bericht moet een object zijn")
    if message.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("Niet-ondersteunde contractversie")
    return message


def job_payload(message: dict[str, Any]) -> dict[str, Any]:
    """Lees een gewone of verliesloos gecomprimeerde jobpayload."""
    codec = message.get("payload_codec")
    if codec is None:
        value = message.get("payload") or {}
    elif codec == "gzip+base64+json":
        compressed = base64.b64decode(
            str(message["payload_data"]).encode("ascii"), validate=True
        )
        raw = decompress_limited(
            compressed,
            codec="gzip",
            maximum_compressed_bytes=MAX_PAYLOAD_BYTES,
            maximum_uncompressed_bytes=MAX_UNCOMPRESSED_JOB_BYTES,
        )
        declared = message.get("payload_uncompressed_bytes")
        if declared is not None and int(declared) != len(raw):
            raise ValueError("Uitgepakte workerjob heeft een ongeldige lengte")
        value = json.loads(raw)
    else:
        raise ValueError(f"Niet-ondersteunde payloadcodec: {codec}")
    if not isinstance(value, dict):
        raise ValueError("Workerjobpayload moet een object zijn")
    return value


def envelope(message_type: str, **values: Any) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "message_type": message_type,
        "timestamp": time.time(),
        **values,
    }
