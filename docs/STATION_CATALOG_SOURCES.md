# Stationcatalogus: officiële broninterfaces

Gecontroleerd op 23 september 2026. Deze interfaces worden uitsluitend door
de handmatige, hervatbare catalogusbootstrap en de begrensde live
stationpolling gebruikt.

## METAR

- Officiële documentatie: <https://connect.aviationweather.gov/data/api/>
- Volledige wereldcatalogus:
  <https://aviationweather.gov/data/cache/stations.cache.json.gz>
- De officiële documentatie beveelt de dagelijkse cache expliciet aan voor
  volledige datasets. API-endpoints zijn begrensd tot 100 requests per minuut
  en meestal 400 resultaten per query; de bootstrap gebruikt daarom één
  dagelijkse cache in plaats van duizenden stationcalls.

## Netatmo

- Officiële documentatie:
  <https://dev.netatmo.com/apidocumentation/weather#getpublicdata>
- Endpoint: `https://api.netatmo.com/api/getpublicdata`
- Discovery gebruikt alleen begrensde bounding boxes met `lat_ne`, `lon_ne`,
  `lat_sw` en `lon_sw`. De bootstrap bewaart na iedere tegel een checkpoint en
  stopt op het ingestelde call- of bytebudget.
- Een wereldsnapshot wordt uitsluitend in hervatbare batches uitgevoerd; hij
  draait nooit tijdens workerstart of een live providercyclus.
- De operationele Belgische catalogus is op 23 september 2026 volledig
  gebootstrapt voor de bounding box `49.4..51.7 N, 2.0..6.7 E`: alle 50
  hoofdtegels zijn compleet. Een te dichte tegel rond Brussel/Antwerpen is
  veilig in vier tegels van 0,25 graad opgesplitst en daarna als compleet
  gereconcilieerd.
- PostGIS bevat daarna 8.013 unieke Netatmo-stations, waarvan 7.144 binnen de
  Belgische operationele bounding box. Het hogere totaal omvat eerder
  ontdekte stations buiten die box; stationselectie blijft altijd lokaal en
  geografisch begrensd.
- De import gebruikt standaard acht seconden tussen calls en is hervatbaar.
  Een wereldwijde inventaris wordt niet als compleet geclaimd: die zou met
  kleine officiële bounding boxes tienduizenden calls vereisen en levert geen
  meerwaarde voor de huidige Belgische waarschuwingsregio's.

## Wunderground / The Weather Company PWS

- Officiële huidige PWS-observatie:
  <https://www.ibm.com/docs/en/environmental-intel-suite?topic=apis-pws-observations-current-conditions>
- Officiële locatiezoekdienst nabij een geocode:
  <https://www.ibm.com/docs/en/environmental-intel-suite?topic=apis-location-service-near>
- Huidige observaties gebruiken
  `https://api.weather.com/v2/pws/observations/current` met station-ID en
  API-sleutel. Observaties ouder dan 60 minuten worden door de dienst als
  verlopen behandeld.
- Totdat de ontvangen sleutel tegen de officiële location-service is getest,
  importeert de bootstrap alleen de door de gebruiker bevestigde station-ID's.
  Er wordt nooit HTML of een Wunderground-kaart gescrapet.
- De seedcatalogus bevat 9 bevestigde stations. Volledige directorydekking
  blijft expliciet geblokkeerd tot de Wunderground/API-sleutel beschikbaar is.

## Actuele offline catalogus

Stand na de eenmalige bootstrap van 23 september 2026:

- METAR: 9.552 stations uit de volledige officiële wereldcache;
- Netatmo: 8.013 unieke stations, met complete tegeldekking voor België en de
  directe grensbuffer;
- Wunderground: 9 door de gebruiker bevestigde seeds; geen onbewezen globale
  directoryclaim zonder API-sleutel.

De catalogus bewaart alleen stationmetadata, locatie en beschikbare
meetcapaciteiten. Live metingen worden uitsluitend opgehaald voor het kleine,
gededupliceerde stationsplan rond actieve waarschuwingsregio's.
Stationmetadata heeft geen automatische vervaldatum; alleen expliciet
niet-operationele stations vallen af. De actuele observaties die van zo'n
station worden opgehaald behouden wel hun eigen strikte versheidsgrens.
