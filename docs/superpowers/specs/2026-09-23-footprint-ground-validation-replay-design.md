# Voetafdrukvoorspelling, grondvalidatie en offline replay

**Datum:** 23 september 2026  
**Status:** ontwerp ter beoordeling  
**Basis:** `2026-09-20-thin-operational-pipeline-design.md`

## Doel

Het dunne operationele pad voorspelt niet alleen de baan van het middelpunt
van een bui, maar verplaatst de volledige gedetecteerde buienvoetafdruk. Per
target levert het een verwachte binnenkomst- en vertrektijd. Netatmo, METAR en
Wunderground valideren die voorspellingen met grondmetingen. Historische
resultaten worden uitsluitend offline uit PostGIS herspeeld om instellingen
en voorspellingsfouten te beoordelen.

De uitbreiding neemt alleen meteorologische berekeningen uit de oude worker
over. RAW-archieven, receipts, manifests, authority-gates, capacity ledgers,
opstartaudits en reclamationcode blijven uitgesloten.

## 1. Voetafdrukvoorspelling

De radartracker levert naast centrum, snelheid en richting ook de actuele
regenvoetafdruk van iedere bevestigde cel. Een zuivere rekenmodule vertaalt
die geometrie met de geschatte oost- en noordcomponent van de beweging.

Voor ieder target berekent de module binnen een begrensde horizon:

- eerste snijmoment (`entry_at` en `eta_minutes`);
- laatste snijmoment (`exit_at`);
- cel-, track- en provideridentiteit;
- gebruikte methode en onzekerheidsreden;
- voorspelde intensiteit voor zover de bron dit ondersteunt.

De berekening gebruikt de werkelijke footprintpixels of -polygoon. De huidige
middelpuntlijn met vaste dreigingsstraal blijft alleen beschikbaar als
expliciet gemarkeerde fallback wanneer een bron geen bruikbare geometrie
levert. Een oudere radarframe mag een nieuwe track nooit terugdraaien.

## 2. Offline stationcatalogus

PostGIS bevat een providerneutrale catalogus met uitsluitend metadata:

- provider en stabiele station-ID;
- locatie als PostGIS-punt;
- beschikbare meetsoorten;
- eerste en laatste waarneming;
- laatste discovery- en succesvolle polltijd;
- actuele geschiktheids- en kwaliteitsstatus;
- bronvoorwaarden die polling beperken.

De catalogus is geen tweede weerarchief. Observaties blijven in de bestaande
genormaliseerde observatietabel.

### Discovery

- Er bestaat een expliciet, hervatbaar eenmalig bootstrapcommando dat een zo
  volledig mogelijke offline momentopname maakt van alle stations die de
  officiële providerinterface op dat moment openbaar aanbiedt. Het commando
  gebruikt checkpoints, respecteert rate limits en kan over meerdere runs of
  dagen verdergaan zonder reeds voltooide tegels opnieuw op te vragen.
- Netatmo wordt langzaam en begrensd per geografische tegel geïnventariseerd
  via de officiële geografische publieke-data-interface. De bootstrap bestrijkt
  het volledige door die interface ondersteunde gebied, niet alleen actieve
  Storm Tracker-regio's.
- METAR importeert de volledige officiële wereldcatalogus en vereist daarna
  alleen een trage metadatarefresh.
- Wunderground start met de bekende stationslijst. Zodra de API-sleutel en de
  bijbehorende officiële documentatie beschikbaar zijn, importeert de
  bootstrap iedere station-ID die een ondersteunde officiële directory- of
  discovery-interface retourneert. Bestaat zo'n interface niet, dan is een
  volledige Wunderground-catalogus technisch niet bewijsbaar en groeit de
  catalogus via expliciete stationlijsten en officieel ontdekte regio's. Er
  wordt niet gescrapet.
- Discovery draait buiten de snelle weercyclus. Een nieuwe regio blokkeert
  nooit de workerstart of andere providers.
- De catalogus houdt per tegel bij wanneer en met welke provider die voor het
  laatst is vernieuwd. Alleen ontbrekende of verouderde tegels worden opnieuw
  ontdekt.

Bij een nieuwe regio voert de worker eerst één lokale PostGIS-zoekopdracht
uit. Alleen wanneer de catalogus voor dat gebied ontbreekt of te oud is, mag
een begrensde discoverytaak worden ingepland. Tot die klaar is blijven radar,
bliksem, modellen en reeds bekende grondstations gewoon werken.

### Pollselectie

De planner rangschikt stations op afstand, actuele beschikbaarheid,
meetmogelijkheden, leeftijd en historische betrouwbaarheid. Hij kiest alleen
de kleinste set die de vereiste meetsoorten en ruimtelijke richtingen rond de
regio voldoende afdekt. Nabije targets die één regio delen, delen ook dezelfde
stationpolls. Per fysiek station is maximaal één request tegelijk actief.

API-budgetten zijn providergebonden. Discovery en live observatie krijgen
aparte budgetten; live observaties hebben voorrang. Sleutels en tokens komen
nooit in PostGIS, MQTT, logs of foutmeldingen.

## 3. Grondvalidatie

Iedere actuele Netatmo-, METAR- of Wunderground-meting wordt op plaats en tijd
gekoppeld aan voorspelde celpassages. De validator registreert minimaal:

- correcte hit;
- gemiste neerslag;
- vals alarm;
- te vroege of te late aankomst;
- fout op entry- en exittijd;
- radarintensiteit tegenover gemeten regen;
- gebruikte stations en bronkwaliteit.

Validatie is in de eerste fase observerend: zij verandert live gewichten of
waarschuwingen niet automatisch. Automatische kalibratie wordt pas apart
geactiveerd nadat een minimumaantal onafhankelijke gevallen en expliciete
acceptatiecriteria zijn gehaald.

## 4. PostGIS

De bestaande dunne tabellen blijven leidend. De uitbreiding voegt
idempotente opslag toe voor:

- stationmetadata en tegeldekking;
- targetpassages met entry/exit en footprintmethode;
- grondvalidatieresultaten;
- offline replayruns en samengevatte metrics.

Celgeometrieën worden als echte PostGIS-geometrie bewaard. Migraties zijn
idempotent. Een PostGIS-storing verhindert MQTT-publicatie niet en houdt
slechts de bestaande begrensde geheugenwachtrij vast.

## 5. Offline replay en kalibratie

Een afzonderlijk commando leest historische celgeometrieën, tracks,
targetpassages en grondmetingen uit PostGIS en roept exact dezelfde zuivere
voetafdruk- en validatiefuncties aan als het live pad. Het commando maakt geen
providerrequests en draait nooit tijdens workerstart.

Een replay rapporteert minimaal hitratio, missratio, false-alarmratio,
gemiddelde en percentielen van entry-/exitfout en uitsplitsingen per provider,
station en regio. Resultaten worden reproduceerbaar opgeslagen met de
gebruikte algoritmeversie en parameters, zonder hash- of receiptketen.

## Foutafhandeling

- Een onbereikbaar grondstation verlaagt alleen de validatiedekking.
- Een ontbrekende Wunderground-sleutel geeft `ready/blocked_api_key`.
- Ontbrekende stationcatalogusdekking geeft `discovery_pending`, nooit een
  globale fout.
- Ongeldige stationwaarden worden met vaste reden-codes geweigerd.
- Een mislukte replay heeft geen invloed op de live worker.

## Acceptatiecriteria

- Een bewegende cel met polygoongeometrie levert aantoonbaar een entry- én
  exittijd voor een doorsneden target.
- Een target buiten de vertaalde footprint krijgt geen aankomstwaarschuwing,
  ook als het dicht bij de middelpuntlijn ligt.
- Twee targets in dezelfde regio veroorzaken geen dubbele stationpoll.
- Een nieuwe regio selecteert bekende stations via één lokale PostGIS-query.
- De eenmalige bootstrap is hervatbaar, idempotent en rapporteert per provider
  het aantal opgeslagen stations, voltooide tegels en resterende dekking.
- Netatmo-, METAR- en Wunderground-observaties gebruiken hetzelfde interne
  observatiemodel en dezelfde validator.
- Offline replay produceert dezelfde passage-uitkomst als live verwerking
  voor identieke invoer.
- Workerheartbeat en eerste snapshot wachten nooit op discovery of replay.
- Geen uitgesloten oude infrastructuurmodule wordt geïmporteerd.
