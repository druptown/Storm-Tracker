# Dun operationeel Storm Tracker-pad

**Datum:** 20 september 2026
**Status:** goedgekeurd ontwerp, nog niet geïmplementeerd

## Doel

Storm Tracker haalt uitsluitend geografisch toepasselijke weerbronnen op,
normaliseert die naar één intern model, voert kwaliteitscontrole en bronfusie
uit, herkent en volgt buien, maakt voorspellingen, bewaart bruikbare data in
PostGIS en publiceert actuele resultaten via MQTT naar Home Assistant.

Home Assistant blijft verantwoordelijk voor locaties, instellingen,
dashboardweergave en waarschuwingen. De worker doet netwerk-I/O en zware
meteorologische berekeningen buiten Home Assistant.

## Niet-doelen

Het live pad bevat geen:

- permanent lokaal RAW-archief;
- content-addressed object store;
- manifest- of receiptketen;
- capacity ledger of reserveringsprotocol;
- authority- of admissionbewijs;
- completion routers;
- reclamationrollen;
- corpusbrede opstartaudit;
- globale fail-closed stop omdat één provider faalt.

PostGIS is de duurzame gegevensopslag. Een kleine lokale configuratie- of
retry-state is toegestaan, maar geen tweede meteorologische database.

## Architectuur

```text
HA locaties/configuratie
          |
          v
geografische providerselectie
          |
          v
parallel ophalen per werkelijk verschuldigde provider
          |
          v
providerdecoder -> NormalizedObservation
          |
          v
kwaliteitscontrole + bronfusie
          |
          v
celherkenning + tracking + voorspelling
          |                         |
          v                         v
       PostGIS                  MQTT snapshot
                                    |
                                    v
                              Home Assistant
```

### 1. Locaties en providerselectie

HA publiceert een compacte gewenste toestand met target-id, coördinaten,
observatieradius en een monotonisch revisienummer. Life360 blijft in HA; de
worker ontvangt alleen de resulterende coördinaten.

Iedere provider heeft een expliciete geografische dekking. De planner maakt
alleen requests voor beschikbare targets binnen die dekking. Per provider is
maximaal één request tegelijk actief; onafhankelijke providers mogen parallel
lopen. Een eenvoudige `next_due_at`-planning vervangt de huidige job- en
receiptscheduler.

### 2. Eén intern observatiemodel

Iedere decoder levert `NormalizedObservation` met minimaal:

- provider- en product-id;
- observatietijd en ontvangsttijd;
- geografische dekking of rastertransformatie;
- meteorologische variabele, waarde en eenheid;
- datakwaliteit, leeftijd en bronstatus;
- compacte herkomst: URL/product/revisie, zonder bewijs- of hashketen.

Radar, bliksem, satelliet, grondmetingen en modelguidance gebruiken hetzelfde
omhulsel met typespecifieke payloads. Decoderfouten blijven lokaal bij de
betreffende provider.

### 3. Kwaliteitscontrole en bronfusie

De kwaliteitslaag controleert eenheid, tijdigheid, geografische dekking,
plausibele grenzen en ontbrekende waarden. Daarna worden compatibele
observaties per target en tijdvenster gefuseerd. De fusie legt per resultaat
vast welke providers bijdroegen en waarom een bron eventueel werd geweigerd.

Een falende of verouderde provider verlaagt de beschikbare bronkwaliteit maar
stopt de cyclus niet. Alleen wanneer geen bruikbare bron overblijft, wordt het
betreffende resultaat `onvoldoende_data`.

### 4. Buien herkennen, volgen en voorspellen

De bestaande bewezen V3-algoritmen voor celherkenning, tracking, beweging en
targetaankomst worden hergebruikt waar hun invoercontract past. CPU-zware
decode en berekeningen blijven begrensd tot twee rekenthreads; provider-I/O is
asynchroon en onafhankelijk.

Uitvoer per target bevat minimaal huidige neerslagstatus, intensiteit of
reflectiviteit, eerstvolgende dreiging, ETA, betrouwbaarheid en reden wanneer
geen voorspelling mogelijk is. Kaartuitvoer bevat actuele relevante
neerslag-/celgeometrie, niet alleen toekomstige aankomsten.

### 5. PostGIS

PostGIS bewaart genormaliseerde observaties, fusieresultaten, cellen, tracks en
voorspellingen. Inserts zijn idempotent op provider/product/observatietijd en
target of geometrie. Een tijdelijke PostGIS-storing blokkeert MQTT niet: de
worker rapporteert de opslagstoring, buffert slechts begrensd in geheugen en
herstelt bij de volgende succesvolle verbinding.

### 6. MQTT en Home Assistant

De worker publiceert:

- heartbeat met worker-versie en actuele cyclusstatus;
- één compacte operationele snapshot;
- providerstatussen met `ready` plus actuele toestand;
- actuele targets, meetresultaten, voorspellingen en kaartobjecten.

HA converteert deze berichten alleen naar entiteiten en dashboardkaarten. HA
voert geen providerfetch, fusie of zware berekening uit.

## Foutafhandeling

- time-out, beperkte retries en back-off per provider;
- circuit breaker per provider, nooit globaal;
- laatste geldige observatie blijft beschikbaar met expliciete leeftijd;
- decoder- en fusiefouten bevatten vaste reden-codes zonder credentials;
- een defecte provider, PostGIS-storing of ongeldige target stopt de worker
  niet;
- processtop alleen voor een niet-herstelbare eigen configuratie- of
  programmeerfout.

## Migratie zonder impulsieve verwijdering

1. Bouw het dunne pad naast het bestaande pad achter één expliciete
   runtimekeuze.
2. Hergebruik uitsluitend providerfetchers, decoders, V3-rekenlogica,
   PostGIS-projectie en MQTT-contracten die direct een van de zes stappen
   bedienen.
3. Test eerst Belgische targets met KMI, OPERA, KNMI/DWD-randdekking,
   Open-Meteo, grondmetingen en beschikbare bliksemdata.
4. Vergelijk live output met de oorspronkelijke V3-integratie en met zichtbare
   werkelijkheid.
5. Schakel HA-dashboard en waarschuwingen over zodra het dunne pad aantoonbaar
   actuele resultaten levert.
6. Verwijder daarna de oude RAW-, audit-, ledger-, authority-, receipt-,
   router- en reclamationcode plus bijbehorende tests en documentatie.

Er wordt geen bestaande meteorologische data verwijderd tijdens de migratie.
Oude databases blijven alleen als rollbackkopie totdat de gebruiker hun
verwijdering afzonderlijk goedkeurt.

## Acceptatiecriteria

- heartbeat binnen 15 seconden na appstart;
- eerste actuele Belgische snapshot binnen 60 seconden, uitgezonderd een
  aantoonbare externe providerstoring;
- regen boven een target verschijnt als actuele meting en op de kaart;
- ten minste één bruikbare voorspelling zodra twee opeenvolgende radarbeelden
  beweging toelaten;
- één falende provider stopt geen andere provider of MQTT-publicatie;
- genormaliseerde observaties en resultaten zijn aantoonbaar in PostGIS;
- HA toont dezelfde snapshot zonder zelf zware berekeningen uit te voeren;
- geen van de uitgesloten infrastructuurlagen wordt door het dunne live pad
  geïmporteerd of aangeroepen.
