# Netatmo World Catalog Importer Design

## Doel

Bouw een eenmalige, hervatbare wereldinventaris van alle stations die de
officiële Netatmo `getpublicdata`-API tijdens de scan publiek beschikbaar
maakt. De inventaris bewaart stationidentiteit, positie en aangeboden
meetcapaciteiten in de bestaande PostGIS-stationcatalogus. Privé-, offline-
en niet-gedeelde stations vallen noodzakelijk buiten het bereik.

## Uitvoering

De importer draait als een afzonderlijke tijdelijke container op de NAS en
niet in Home Assistant of in de actieve Storm Tracker-worker. Hij gebruikt
de bestaande `storm_tracker_worker.thin_station_bootstrap`- en
`thin_station_catalog`-modules en schrijft rechtstreeks naar dezelfde
PostGIS-database.

De container heeft geen MQTT-toegang en voert geen radar-, fusie- of
voorspellingswerk uit. Een storing of herstart van de importer mag de worker,
Home Assistant en PostGIS niet stoppen.

## Wereldscan

- Basisraster: `-85..85` breedtegraad en `-180..180` lengtegraad in vakken
  van exact 1 graad: 61.200 basisvakken.
- Bij HTTP 503, een te groot antwoord of een aantoonbaar onbetrouwbaar dicht
  vak wordt deterministisch gesplitst naar 0,5 graad en zo nodig 0,25 graad
  of kleiner.
- Een vak wordt alleen compleet gemarkeerd nadat het antwoord succesvol is
  verwerkt, of nadat alle vier de kindvakken compleet zijn.
- Stations uit overlappende vakken worden op `provider_id + station_id`
  gededupliceerd.
- Na iedere afgeronde tegel wordt een PostGIS-checkpoint opgeslagen. Een
  herstart herhaalt geen reeds complete tegel.
- Na alle basisvakken wordt een afzonderlijke wereld-compleetmarker bewaard,
  waarna de container zonder nieuwe API-calls afsluit.

## API-budget en tokens

- De importer doet maximaal 400 Netatmo-discoverycalls per klokuur en nooit
  parallel meer dan één discoverycall.
- De resterende accountcapaciteit blijft beschikbaar voor actuele regionale
  Netatmo-metingen van de worker.
- HTTP 429 en tijdelijke 5xx-antwoorden veroorzaken begrensde back-off zonder
  verlies van checkpoints.
- OAuth-access tokens worden tijdens de meerdaagse scan vernieuwd. Tokens en
  andere secrets komen niet in PostGIS, logs, containerlabels of diagnostiek.

## Opgeslagen gegevens

Per station worden uitsluitend catalogusgegevens bewaard:

- Netatmo-station-ID en WGS84-coördinaten;
- ontdekkings- en laatst-gezienmoment;
- operationele vlag en kwaliteitsscore;
- alle door de publieke stationsbeschrijving herkenbaar aangeboden
  sensortypes, waaronder regen, wind, luchtdruk, temperatuur en
  luchtvochtigheid.

De importer bewaart alle publiek gevonden stations met minstens één herkenbaar
sensortype. Relevantieselectie gebeurt daarna zonder de broninventaris te
vernietigen: de actieve worker gebruikt alleen stations die minstens één
kernsensor uit de groepen regen, wind of luchtdruk aanbieden. Stations met
uitsluitend temperatuur en/of luchtvochtigheid blijven als inactieve
catalogusmetadata beschikbaar, maar worden niet live gepolld. Een relevant
station hoeft niet alle drie de kerngroepen aan te bieden: nabijgelegen
regen-, wind- en drukstations mogen elkaar aanvullen. Stations die meerdere
kerngroepen afdekken krijgen selectievoorrang. Een latere opschoning kan de
niet-relevante rijen met één expliciete PostGIS-operatie verwijderen, maar
maakt geen deel uit van de wereldimport.

De importer archiveert niet voortdurend de actuele wereldwijde meetwaarden.
De gewone worker selecteert later per actieve waarschuwingsregio een klein
aantal relevante stations en pollt alleen daarvan live metingen.

## Deployment en levenscyclus

De container krijgt een eigen minimale image en startcommando. Configuratie
bestaat uitsluitend uit Netatmo OAuth-gegevens, PostGIS-verbindingsgegevens,
het callbudget en het rasterprofiel. Hij krijgt geen toegang tot Home
Assistant-configuratie, MQTT of workeropslag.

Na bewezen wereld-compleetstatus mag de container worden gestopt en verwijderd.
De catalogus en checkpoints blijven in PostGIS behouden. Een latere handmatige
refresh gebruikt dezelfde checkpoints of een expliciet nieuwe scanversie.

## Acceptatie

- Unit tests bewijzen deterministische werelddekking, adaptieve splitsing,
  budgetstop, back-off, tokenvernieuwing, deduplicatie en hervatting.
- Een dry-run rapporteert exact 61.200 basisvakken zonder API-call.
- Een live canary verwerkt enkele nog niet afgeronde vakken, schrijft
  checkpoints en laat worker/HA operationeel.
- De containerstatus toont aantallen complete/resterende vakken en stations,
  maar nooit secrets.
- De volledige scan claimt alleen de publiek bereikbare stations tijdens het
  scanvenster; geen absolute inventaris van verborgen Netatmo-apparaten.
