# Storm Tracker V3 — Architectuurreview RegionEngine

**Datum:** 12 juli 2026  
**Status:** Ter nazicht — geen code gewijzigd

---

## Kernvraag

Hoe bepaal je het optimale werkgebied van een RegionEngine?

---

## Probleem met een vaste radius

Een vaste radius van 700km heeft een fundamenteel probleem:  
**de radius heeft twee verschillende taken die elkaar tegenwerken.**

| Taak | Beschrijving | Probleem met 700km |
|---|---|---|
| **Sharing** | Wanneer mag een nieuw target dezelfde engine gebruiken? | 700km is veel te groot — Nathan in Amsterdam en Jochem in Parijs zouden dezelfde engine delen |
| **Dataverzameling** | Hoe ver kijkt de engine voor weersdata? | Hangt af van buisnelheid en voorspellingstijd, niet van geografische nabijheid |

**Conclusie:** één vaste radius doet twee dingen tegelijk en doet beide slecht.

---

## Voorstel: twee aparte concepten

### 1. Sharing radius — bepaalt wanneer targets dezelfde engine delen

**Voorstel: 50km**

Redenering: twee personen binnen 50km zien grotendeels dezelfde buien.  
Ze kunnen stormdata van dezelfde engine gebruiken.  
De ProjectionEngine berekent voor elk target apart de ETA.

Voorbeelden:
- Nathan in Amsterdam + Wim in Heffen → 150km → **aparte engines**
- Nathan in Utrecht + Wim in Breda → 40km → **zelfde engine**

### 2. Observatie horizon — bepaalt welke observaties de StormEngine verwerkt

**Voorstel: 200km**

Redenering: bij 100km/u en 2 uur vooruit = 200km detectiehorizon.  
Observaties buiten 200km van het engine centrum worden genegeerd door de OFE.

Dit is **geen eigenschap van de provider** maar een filter in de OFE.

### 3. Provider dekking — ongewijzigd

Elke provider bepaalt zelf wat hij levert via `supports()`.  
De RegionEngine vraagt enkel: "geef mij alles voor mijn centrum."

| Provider | Dekking |
|---|---|
| Blitzortung | Wereldwijd — geen horizon |
| KMI | Vast (België/NL/Noord-FR) |
| KNMI | Vast (lat 48.9-56°N, lon 0-10.9°O) |
| RainViewer | Tiles rond centrum (~600km) |
| Netatmo | Instelbare radius (standaard 175km) |
| Open-Meteo | Grid tot 200km rond centrum |

---

## Samenvatting

| Concept | Huidige aanpak | Voorstel |
|---|---|---|
| Sharing radius | 700km (te groot) | **50km** |
| Observatie horizon | Ontbreekt | **200km filter in OFE** |
| Provider grenzen | Via `supports()` ✅ | Behouden |
| Engine lifecycle | Dynamisch ✅ | Behouden |
| Multi-target sharing | Aanwezig ✅ | Behouden |
| Auto-aanmaken | Aanwezig ✅ | Behouden |
| Auto-verwijderen | Aanwezig ✅ | Behouden |

---

## Wat NIET verandert

- RegionEngines zijn dynamisch
- RegionEngines worden automatisch aangemaakt en verwijderd
- Meerdere ProjectionTargets mogen dezelfde RegionEngine delen
- ProjectionTargets bevatten zelf geen observatie- of stormdata
- Alle zware berekeningen gebeuren uitsluitend binnen RegionEngines

---

## Openstaande vragen

- Akkoord met 50km sharing radius?
- Akkoord met 200km observatie horizon?
- Moet de observatie horizon configureerbaar zijn (bv. per provider)?
- Moet de sharing radius afhangen van de regio (Europa vs. rest van de wereld)?
