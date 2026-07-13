# Storm Tracker V3 — Ontwikkelplan

**Datum:** 12 juli 2026  
**Huidige versie:** v0.3.0 (OFE + StormEngine geactiveerd, nog niet gedeployed)

---

## Status huidig

| Module | Status | Versie |
|---|---|---|
| Alle providers | ✅ Werkend en gevalideerd | zie CHANGELOG |
| OFE | ✅ Gebouwd, net geactiveerd | v0.1.0 |
| StormEngine | ✅ Gebouwd, net geactiveerd | v0.1.0 |
| RegionEngine/StormManager | ✅ Gebouwd, niet geïntegreerd | v0.5.0 |
| Plugincontract | ❌ Niet gebouwd | — |
| ProviderRegistry | ❌ Niet gebouwd | — |
| ProjectionEngine | ❌ Niet gebouwd | — |
| Storm sensoren | ✅ Net toegevoegd | v0.2.0 |

---

## Fase 1 — v0.3.0 deployen en valideren

**Doel:** Bevestigen dat OFE + StormEngine correct werken met alle providers.

**Acceptatiecriteria:**
- `sensor.stv3_actieve_storms` toont storms bij regen of onweer
- `sensor.stv3_dichtstbijzijnde_storm` toont afstand, richting, snelheid
- Logs tonen "StormEngine: X actieve storms na batch van Y observaties"
- RADAR, RAIN en LIGHTNING observaties leiden samen tot correcte WeatherSystems

**Pas naar Fase 2 als Fase 1 gevalideerd is.**

---

## Fase 2 — Plugincontract implementeren

**Doel:** Één stabiel providercontract vastleggen voor alle huidige en toekomstige providers.

**Nieuw bestand:** `providers/base.py`

```python
class Capability(Enum):
    LIGHTNING    = "lightning"
    RADAR        = "radar"
    RAIN_GAUGE   = "rain_gauge"
    NOWCAST      = "nowcast"
    FORECAST     = "forecast"
    WARNING      = "warning"

@dataclass(frozen=True)
class CoverageResult:
    supported:         bool
    coverage_fraction: float   # 0.0 - 1.0
    quality:           float   # 0.0 - 1.0
    reason:            str = ""

class ProviderPlugin(Protocol):
    plugin_id:    str
    capabilities: frozenset[Capability]
    priority:     int

    def supports(self, area: CoverageArea) -> CoverageResult: ...
    async def async_start(self, context: ProviderContext) -> None: ...
    async def async_stop(self) -> None: ...
    async def async_fetch(self) -> list[Observation]: ...
```

**Aanpak:** contract vastleggen + unit tests. Nog geen providers migreren.

---

## Fase 3 — Providers stap voor stap migreren

**Doel:** Elke provider aanpassen aan het plugincontract. Na elke migratie valideren voor de volgende.

**Volgorde:**

| Stap | Provider | Capability | Prioriteit |
|---|---|---|---|
| 3.1 | Blitzortung | LIGHTNING | 100 |
| 3.2 | KMI | RADAR, NOWCAST | 100 |
| 3.3 | RainViewer | RADAR | 40 |
| 3.4 | KNMI | RADAR, NOWCAST | 100 |
| 3.5 | Netatmo | RAIN_GAUGE | 100 |
| 3.6 | Open-Meteo | NOWCAST, FORECAST | 40 |

**Elke stap:** migreren → deployen → valideren → volgende stap.

---

## Fase 4 — ProviderRegistry met strategie per capability

**Doel:** Slimme providerselectie per datatype. Niet altijd "de beste provider" maar een strategie per capability.

**Strategieën:**

| Capability | Strategie |
|---|---|
| LIGHTNING | Altijd Blitzortung — geen selectie |
| RADAR | Selectie op kwaliteit/dekking/prioriteit; één actieve primaire + fallback |
| RAIN_GAUGE | Meerdere bronnen combineren (Netatmo + later nationale stations) |
| NOWCAST | Beste beschikbare + fallback |
| WARNING | Meerdere bronnen combineren (MeteoAlarm + nationale) |

---

## Fase 5 — RegionEngine integratie

**Doel:** RegionEngine/StormManager koppelen aan ProviderRegistry. Multi-persoon tracking.

**Pas starten na Fase 4 volledig gevalideerd.**

**Configureerbare parameters (geen hardcoded waarden):**

```python
# Bepaalt wanneer targets dezelfde engine delen
DEFAULT_ENGINE_SHARING_DISTANCE_KM = 50   # configureerbaar

# Automatisch afgeleid — groeit mee met gewenste voorspellingstijd
observation_horizon = forecast_time_hours * max_storm_speed_kmh
```

**ProjectionTargets:**
- `device_tracker.fictieve_tracker`
- Life360 trackers (te bepalen welke)

---

## Fase 6 — ProjectionEngine

**Doel:** Per ProjectionTarget de ETA, intensiteit en passageduur berekenen.

**Input:** Storm objecten (centroid, richting, snelheid, radius)  
**Output:** ETA, verwachte intensiteit, passageduur per persoon

---

## Wat we NIET doen voor Fase 6 klaar is

- Geen nieuwe landplugins (DWD, Météo-France, ...)
- Geen OPERA/ORD integratie
- Geen MeteoAlarm
- Geen UI/Lovelace kaart
- Geen kalenderplanning — fasen zonder datum

---

## Architectuurbeslissingen (vastgelegd)

| Beslissing | Waarde | Status |
|---|---|---|
| Sharing radius | 50km (configureerbaar) | Voorlopig, te valideren in Fase 5 |
| Observatie horizon | forecast_time × max_storm_speed | Afgeleid, niet hardcoded |
| ProviderRegistry strategie | Per capability, niet per engine | Vastgelegd |
| RegionEngine integratie | Na volledige provider migratie | Vastgelegd |
| Kalenderplanning | Fasen zonder datum | Vastgelegd |
