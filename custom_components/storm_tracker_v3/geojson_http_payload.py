"""Pure helpers for composing the authenticated map payload."""
from __future__ import annotations

from typing import Any, Mapping


def with_radar_overlays(
    collection: Mapping[str, Any],
    legacy_overlays: object,
) -> dict[str, Any]:
    """Return a copy with legacy overlays plus authoritative worker overlays."""
    result = dict(collection)
    merged: dict[str, Any] = {}
    if isinstance(legacy_overlays, dict):
        merged.update(legacy_overlays)
    worker_overlays = collection.get("radar_overlays")
    if isinstance(worker_overlays, dict):
        merged.update(worker_overlays)
    result["radar_overlays"] = merged
    return result
