"""Storm Tracker V3 — providers/base.py v0.1.0

Plugincontract voor alle Storm Tracker V3 providers.

Verantwoordelijkheden:
  - Definieert het uniforme interface dat elke provider moet implementeren
  - Definieert Capability (wat levert een provider?)
  - Definieert CoverageResult (hoe goed dekt een provider een gebied?)
  - Definieert CoverageArea (het te monitoren gebied)
  - Definieert ProviderContext (wat krijgt een provider mee bij activatie?)
  - Definieert ProviderRegistry (selecteert providers per capability)

Architectuurprincipes (uit review 12 juli 2026):
  - ProviderRegistry werkt met een STRATEGIE per capability,
    niet altijd "de beste provider":
      LIGHTNING   → altijd Blitzortung, geen selectie
      RADAR       → selectie op kwaliteit/dekking/prioriteit
      RAIN_GAUGE  → meerdere bronnen combineren
      NOWCAST     → beste beschikbare + fallback
      WARNING     → meerdere bronnen combineren
  - Sharing radius is configureerbaar (DEFAULT_ENGINE_SHARING_DISTANCE_KM)
  - Observatie horizon is afgeleid: forecast_time × max_storm_speed
  - CoverageResult geeft niet alleen True/False maar ook fractie en kwaliteit

Versiegeschiedenis:
  v0.1.0 — eerste versie; Capability, CoverageArea, CoverageResult,
            ProviderContext, ProviderPlugin Protocol, ProviderRegistry
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional, Protocol, runtime_checkable

from ..engine.observation import Observation

_LOGGER = logging.getLogger(__name__)

# ── Configureerbare parameters ────────────────────────────────────────────────

DEFAULT_ENGINE_SHARING_DISTANCE_KM = 50.0
"""Maximale afstand tussen twee ProjectionTargets om dezelfde RegionEngine
te delen. Configureerbaar — waarde wordt bepaald op basis van praktijkervaring."""

DEFAULT_MAX_STORM_SPEED_KMH = 100.0
"""Maximale buisnelheid voor berekening observatie horizon."""

DEFAULT_FORECAST_TIME_H = 2.0
"""Gewenste voorspellingstijd in uren."""


def compute_observation_horizon(
    forecast_time_h: float = DEFAULT_FORECAST_TIME_H,
    max_storm_speed_kmh: float = DEFAULT_MAX_STORM_SPEED_KMH,
) -> float:
    """
    Bereken de observatie horizon in km.
    Afgeleid van voorspellingstijd en maximale buisnelheid — niet hardcoded.

    Voorbeeld: 2u × 100km/u = 200km horizon
    """
    return forecast_time_h * max_storm_speed_kmh


# ── Capability ────────────────────────────────────────────────────────────────

class Capability(Enum):
    """
    Wat voor type data levert een provider?

    Elke provider declareert zijn capabilities bij registratie.
    De ProviderRegistry gebruikt dit voor providerselectie per datatype.
    """
    LIGHTNING  = "lightning"   # blikseminslagen met lat/lon/timestamp
    RADAR      = "radar"       # neerslag rasterdata (pixels met intensiteit)
    RAIN_GAUGE = "rain_gauge"  # grondmetingen (neerslag, wind, druk per station)
    NOWCAST    = "nowcast"     # korte-termijn voorspelling (0-2u)
    FORECAST   = "forecast"    # langere termijn voorspelling (>2u)
    WARNING    = "warning"     # officiële meteorologische waarschuwingen


# ── CoverageArea ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CoverageArea:
    """
    Het geografische gebied dat een RegionEngine monitort.

    Later uit te breiden naar polygon-gebaseerde dekking.
    Momenteel een cirkel gedefinieerd door centrum + horizon radius.
    """
    center_lat:     float
    center_lon:     float
    horizon_km:     float  # observatie horizon (afgeleid, niet hardcoded)

    def contains(self, lat: float, lon: float) -> bool:
        """True als het punt binnen de horizon valt."""
        return _haversine(self.center_lat, self.center_lon, lat, lon) <= self.horizon_km

    def overlap_fraction(self, other: "CoverageArea") -> float:
        """Ruwe schatting van de overlap fractie met een andere CoverageArea."""
        dist = _haversine(
            self.center_lat, self.center_lon,
            other.center_lat, other.center_lon
        )
        combined = self.horizon_km + other.horizon_km
        if dist >= combined:
            return 0.0
        if dist <= abs(self.horizon_km - other.horizon_km):
            return 1.0
        return 1.0 - (dist / combined)


# ── CoverageResult ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CoverageResult:
    """
    Geeft aan in welke mate een provider een bepaald gebied dekt.

    Niet alleen True/False, maar ook fractie en kwaliteit.
    Zo kan de ProviderRegistry intelligente keuzes maken:
      - KMI dekt 95% van een Belgische engine met hoge kwaliteit
      - RainViewer dekt 100% maar met lagere kwaliteit
      → KMI krijgt voorkeur, RainViewer is fallback
    """
    supported:         bool
    coverage_fraction: float  # 0.0 = geen dekking, 1.0 = volledige dekking
    quality:           float  # 0.0 = laag, 1.0 = hoog (resolutie, latency, nauwkeurigheid)
    reason:            str = ""

    @property
    def score(self) -> float:
        """Gecombineerde score voor providerselectie."""
        if not self.supported:
            return 0.0
        return self.coverage_fraction * self.quality


# ── ProviderContext ────────────────────────────────────────────────────────────

@dataclass
class ProviderContext:
    """
    Alles wat een provider nodig heeft bij activatie.

    Wordt aangemaakt door de RegionEngine en meegegeven bij async_start().
    """
    hass:              object           # HomeAssistant instantie
    area:              CoverageArea     # het te monitoren gebied
    on_observation:    Callable[[Observation], None]  # callback per observatie
    config:            dict = field(default_factory=dict)  # provider-specifieke config


# ── ProviderPlugin Protocol ───────────────────────────────────────────────────

@runtime_checkable
class ProviderPlugin(Protocol):
    """
    Interface dat elke Storm Tracker V3 provider moet implementeren.

    Eigenschappen:
      plugin_id:    unieke identifier (bv. "kmi", "blitzortung")
      capabilities: frozenset van Capability waarden
      priority:     hogere waarde = hogere voorkeur bij providerselectie
                    nationaal = 100, OPERA = 70, globale fallback = 40

    Methoden:
      supports():     geeft CoverageResult voor een bepaald gebied
      async_start():  provider activeren met context
      async_stop():   provider deactiveren
      async_fetch():  data ophalen (wordt periodiek aangeroepen door poller)
    """
    plugin_id:    str
    capabilities: frozenset
    priority:     int

    def supports(self, area: CoverageArea) -> CoverageResult:
        """Geeft aan of en hoe goed deze provider het gebied dekt."""
        ...

    async def async_start(self, context: ProviderContext) -> None:
        """Provider activeren. Wordt aangeroepen bij RegionEngine aanmaak."""
        ...

    async def async_stop(self) -> None:
        """Provider deactiveren. Wordt aangeroepen bij RegionEngine verwijdering."""
        ...

    async def async_fetch(self) -> list[Observation]:
        """Data ophalen. Wordt periodiek aangeroepen door de poller."""
        ...


# ── ProviderRegistry ──────────────────────────────────────────────────────────

class ProviderRegistry:
    """
    Centrale registry voor alle geregistreerde providers.

    Werkt met een STRATEGIE per capability — niet altijd "de beste provider":

      LIGHTNING   → altijd alle beschikbare bronnen (Blitzortung is uniek)
      RADAR       → selectie: beste dekking + kwaliteit + prioriteit; één primair + fallback
      RAIN_GAUGE  → combineren: meerdere bronnen samen (Netatmo + nationale stations)
      NOWCAST     → selectie: beste beschikbare + fallback
      WARNING     → combineren: meerdere bronnen samen (MeteoAlarm + nationaal)
      FORECAST    → selectie: beste beschikbare

    Versiegeschiedenis:
      v0.1.0 — eerste versie; strategie per capability
    """

    def __init__(self) -> None:
        self._plugins: list[ProviderPlugin] = []

    def register(self, plugin: ProviderPlugin) -> None:
        """Registreer een provider plugin."""
        self._plugins.append(plugin)
        _LOGGER.debug("ProviderRegistry: %s geregistreerd (capabilities: %s, priority: %d)",
                      plugin.plugin_id,
                      {c.value for c in plugin.capabilities},
                      plugin.priority)

    def select_for_area(
        self,
        area: CoverageArea,
        capability: Capability,
    ) -> list[ProviderPlugin]:
        """
        Selecteer providers voor een capability en gebied.
        Past de strategie toe die bij de capability hoort.

        Returns: lijst van te activeren providers (volgorde = prioriteit)
        """
        # Kandidaten: providers die deze capability hebben en het gebied (deels) dekken
        candidates = [
            (p, p.supports(area))
            for p in self._plugins
            if capability in p.capabilities
        ]
        candidates = [(p, r) for p, r in candidates if r.supported]

        if not candidates:
            _LOGGER.debug("ProviderRegistry: geen provider voor %s in %s",
                          capability.value, area)
            return []

        strategy = _CAPABILITY_STRATEGIES.get(capability, _strategy_select_best)
        selected = strategy(candidates)

        _LOGGER.info(
            "ProviderRegistry: %s → %s",
            capability.value,
            [p.plugin_id for p in selected]
        )
        return selected

    def select_all_for_area(
        self, area: CoverageArea
    ) -> dict[Capability, list[ProviderPlugin]]:
        """
        Selecteer providers voor alle capabilities.
        Returns: dict capability → lijst providers
        """
        return {
            cap: self.select_for_area(area, cap)
            for cap in Capability
        }

    @property
    def all_plugins(self) -> list[ProviderPlugin]:
        return list(self._plugins)


# ── Strategiefuncties ─────────────────────────────────────────────────────────

def _strategy_select_best(
    candidates: list[tuple[ProviderPlugin, CoverageResult]]
) -> list[ProviderPlugin]:
    """
    Selecteer de beste provider + één fallback.
    Gebruikt voor: RADAR, NOWCAST, FORECAST
    """
    sorted_candidates = sorted(
        candidates,
        key=lambda x: (x[0].priority, x[1].score),
        reverse=True
    )
    # Primaire provider + eerste fallback (andere priority)
    selected = []
    primary_priority = None
    for plugin, result in sorted_candidates:
        if primary_priority is None:
            selected.append(plugin)
            primary_priority = plugin.priority
        elif plugin.priority < primary_priority and len(selected) == 1:
            # Eerste fallback van lagere prioriteit
            selected.append(plugin)
            break
    return selected


def _strategy_combine_all(
    candidates: list[tuple[ProviderPlugin, CoverageResult]]
) -> list[ProviderPlugin]:
    """
    Combineer alle beschikbare bronnen.
    Gebruikt voor: RAIN_GAUGE, WARNING
    """
    return [p for p, _ in sorted(candidates, key=lambda x: x[0].priority, reverse=True)]


def _strategy_always_all(
    candidates: list[tuple[ProviderPlugin, CoverageResult]]
) -> list[ProviderPlugin]:
    """
    Activeer alle beschikbare bronnen zonder selectie.
    Gebruikt voor: LIGHTNING (Blitzortung is uniek, geen concurrenten)
    """
    return [p for p, _ in candidates]


_CAPABILITY_STRATEGIES = {
    Capability.LIGHTNING:  _strategy_always_all,
    Capability.RADAR:      _strategy_select_best,
    Capability.RAIN_GAUGE: _strategy_combine_all,
    Capability.NOWCAST:    _strategy_select_best,
    Capability.FORECAST:   _strategy_select_best,
    Capability.WARNING:    _strategy_combine_all,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Afstand in km (Haversine formule)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
