"""Configuratiecontract voor meerdere personen en locaties."""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TargetSpec:
    target_id: str
    name: str
    entity_id: str
    fallback_lat: float | None = None
    fallback_lon: float | None = None
    primary: bool = False

    @property
    def entity_suffix(self) -> str:
        value = re.sub(r"[^a-z0-9]+", "_", self.target_id.lower()).strip("_")
        return value or "target"


def build_target_specs(
    home_lat: float,
    home_lon: float,
    configured: list[dict] | None = None,
    test_tracker_entity: str | None = None,
) -> list[TargetSpec]:
    """Bouw home, Life360-personen en een optionele testtracker."""
    specs = [TargetSpec(
        target_id="home",
        name="Thuis",
        entity_id="zone.home",
        fallback_lat=float(home_lat),
        fallback_lon=float(home_lon),
        primary=True,
    )]
    seen_ids = {"home"}
    seen_entities = {"zone.home"}
    for raw in configured or []:
        target_id = str(raw["id"]).strip()
        entity_id = str(raw["location_entity"]).strip()
        if not target_id or target_id in seen_ids:
            raise ValueError(f"Dubbel of leeg target-id: {target_id!r}")
        if entity_id in seen_entities:
            raise ValueError(f"Locatie-entiteit dubbel geconfigureerd: {entity_id}")
        lat = raw.get("latitude")
        lon = raw.get("longitude")
        if (lat is None) != (lon is None):
            raise ValueError(f"Target {target_id}: latitude en longitude horen samen")
        specs.append(TargetSpec(
            target_id=target_id,
            name=str(raw.get("name") or target_id),
            entity_id=entity_id,
            fallback_lat=float(lat) if lat is not None else None,
            fallback_lon=float(lon) if lon is not None else None,
        ))
        seen_ids.add(target_id)
        seen_entities.add(entity_id)
    if test_tracker_entity:
        entity_id = str(test_tracker_entity).strip()
        if entity_id not in seen_entities:
            specs.append(TargetSpec(
                target_id="test_tracker",
                name="Fictieve tracker (test)",
                entity_id=entity_id,
            ))
    return specs


def coordinates_from_state(state, spec: TargetSpec) -> tuple[float, float] | None:
    """Lees een HA-locatiestatus met expliciete fallback voor vaste targets."""
    if state is not None:
        lat = state.attributes.get("latitude")
        lon = state.attributes.get("longitude")
        if lat is not None and lon is not None:
            return float(lat), float(lon)
    if spec.fallback_lat is not None and spec.fallback_lon is not None:
        return spec.fallback_lat, spec.fallback_lon
    return None
