# Stationgerichte PostGIS-opslag en bewaarbeleid

**Datum:** 25 september 2026  
**Status:** ter schriftelijke goedkeuring  
**Scope:** Storm Tracker op NAS-Home Assistant en de bestaande PostGIS-container

## Doel

Storm Tracker bewaart iedere fysieke meting precies eenmaal, onafhankelijk van
het aantal gevolgde personen of actieve waarschuwingsregio's. Grondmetingen
worden gekoppeld aan één blijvend stationrecord. Alle tijdgebonden meet- en
resultaatdata blijft maximaal zes maanden beschikbaar en wordt daarna
automatisch verwijderd. De permanente stationscatalogus en importcheckpoints
blijven behouden.

De wijziging moet de bestaande Netatmo-wereldimport ongemoeid laten, de huidige
live worker tijdens de overgang bruikbaar houden en nooit bestaande data
verwijderen voordat de nieuwe opslag aantoonbaar volledig en correct is.

## Aanleiding

`stv3_observation` bevat momenteel dezelfde stationsmeting meermaals wanneer
het station relevant is voor meerdere targets. `target_id` maakt deel uit van
de huidige opslagidentiteit, terwijl temperatuur, druk, wind of regen van een
fysiek station geen persoonsgebonden feit is. De live tabel bevat miljoenen
rijen, grotendeels Netatmo-data, en kent geen automatische bewaartermijn.

De Netatmo-importer vult ondertussen terecht afzonderlijke catalogustabellen:

- `stv3_ground_station`: blijvende stationidentiteit en positie;
- `stv3_station_capability`: aangeboden sensortypes;
- `stv3_station_discovery_tile`: hervatbare importvoortgang.

Deze catalogus is stamdata en vormt de basis voor de nieuwe stationgerichte
meetopslag.

## Beschouwde aanpakken

### A. Stationcatalogus plus aparte tijdreeksen — gekozen

Stations blijven één permanente rij per `provider_id + station_id`. Iedere
meetwaarde wordt een afzonderlijke tijdreeksrij met een verwijzing naar dat
station. Targets en regio's worden pas bij selectie, fusie en voorspelling aan
de meting gekoppeld.

Dit model voorkomt duplicaten, ondersteunt Netatmo, METAR en later
Weather Underground op dezelfde manier, en laat metingen efficiënt per tijd,
gebied en sensortype opvragen.

### B. Alle meetwaarden als JSON in het stationrecord — afgewezen

Dit lijkt eenvoudig, maar maakt ieder stationrecord onbeperkt groot, veroorzaakt
schrijfconflicten en maakt tijdvensters, deduplicatie en opschoning duur.

### C. De huidige observatietabel behouden en achteraf dedupliceren — afgewezen

Een periodieke schoonmaak bestrijdt alleen het gevolg. De worker zou dezelfde
fysieke meting blijven vermenigvuldigen per target en opslag- en indexruimte
blijven verspillen.

## Datamodel

### Permanente stamdata

De volgende tabellen blijven permanent en worden niet door het
zesmaandenbeleid verwijderd:

- `stv3_ground_station`;
- `stv3_station_capability`;
- `stv3_station_discovery_tile`;
- `stv3_meta.schema_migrations` en andere technische schemadefinities.

Een station wordt uniek geïdentificeerd door `provider_id + station_id`.
Positie, `discovered_at`, `last_seen_at`, kwaliteit en operationele status
blijven in `stv3_ground_station`. Het aanbod van regen-, wind-, druk-,
temperatuur- en vochtmetingen blijft in `stv3_station_capability`.

### Canonieke stationmetingen

Er komt een afzonderlijke, op `observed_at` gepartitioneerde tabel
`stv3_station_measurement` met ten minste:

- `provider_id` en `station_id`, als foreign key naar
  `stv3_ground_station`;
- `observed_at` en `received_at` in UTC;
- `measurement_type`, bijvoorbeeld `precipitation_rate`,
  `precipitation_accumulation`, `wind_speed`, `wind_gust`,
  `wind_direction`, `pressure`, `temperature` of `humidity`;
- genormaliseerde numerieke `value` en `unit`;
- `quality` en compacte bronmetadata;
- optioneel `product_id` voor provenance, maar niet als duplicerende
  targetidentiteit.

De logische unieke sleutel is:

`provider_id + station_id + observed_at + measurement_type`

`target_id` komt niet in deze sleutel of tabel. Een herhaalde ontvangst wordt
een idempotente upsert. Bij een conflict wint deterministisch de bruikbare
meting met de beste kwaliteit; bij gelijke kwaliteit wint de laatst ontvangen
versie. Verschillende grootheden zoals gemiddelde windsnelheid en windstoot
blijven afzonderlijke `measurement_type`-waarden.

### Overige fysieke observaties

Niet-stationaire bronfeiten, zoals blikseminslagen en compacte
radar-/satellietobservaties, blijven als fysieke feiten opgeslagen zonder
persoonsafhankelijke identiteit. Als dezelfde bronwaarneming voor meerdere
regio's relevant is, wordt zij eenmaal bewaard en ruimtelijk aan die regio's
gekoppeld tijdens verwerking of raadpleging.

Volledige radarrasterschijven worden niet door deze wijziging als grote
PostGIS-payloads toegevoegd. Het bestaande ontwerp bewaart alleen compacte
observaties, fusiesamenvattingen, cellen, tracks en voorspellingen.

### Targetgebonden resultaten

Een `target_id` blijft uitsluitend toegestaan waar het resultaat werkelijk
persoons- of locatiegebonden is:

- targetposities;
- targetvoorspellingen;
- verwachte passage en entry-/exittijd;
- waarschuwingen en forecastvalidatie.

Eén fysieke grondmeting kan daardoor meerdere voorspellingen valideren zonder
zelf meermaals te worden opgeslagen.

## Datastroom

1. De catalogus selecteert geografisch relevante stations en hun
   capaciteiten voor een actieve regio.
2. Een provideradapter haalt de actuele waarden van die stations op en
   normaliseert type, waarde, eenheid en tijd.
3. De PostGIS-writer slaat ieder fysiek stationfeit eenmaal op in
   `stv3_station_measurement`.
4. Fusie en validatie vragen metingen op basis van ruimte, tijd en
   sensortype; ze gebruiken geen per-target kopieën.
5. Targetgebonden voorspellingen en validatieresultaten worden in hun eigen
   tabellen opgeslagen.
6. Retentie verwijdert tijdgebonden rijen ouder dan zes maanden.

## Bewaartermijn en partitionering

Alle tijdgebonden PostGIS-data heeft één uniforme maximale bewaartermijn van
zes maanden. Dit omvat:

- ruwe en genormaliseerde grondmetingen;
- niet-stationaire observaties zoals bliksemfeiten;
- fusiecycli en fusiesamenvattingen;
- stormcellen en trackpunten;
- targetposities, voorspellingen en passages;
- forecastvalidaties en replayresultaten.

De tijdreeks- en resultaattabellen worden per kalendermaand gepartitioneerd.
Een onderhoudstaak:

1. maakt vóór gebruik tijdig de volgende maandpartitie aan;
2. verwijdert volledig verlopen maandpartities met `DROP TABLE`/detach;
3. verwijdert in begrensde batches de verlopen rijen uit de ene
   grenspartitie, zodat geen rij ouder dan exact zes kalendermaanden blijft;
4. rapporteert per tabel cutoff, verwijderde rijen, oudste resterende rij en
   eventuele fout.

De onderhoudstaak mag provideracquisitie niet blokkeren. Zij gebruikt een
PostgreSQL advisory lock zodat nooit twee opruimrondes tegelijk lopen. Een
fout stopt alleen de retentieronde en verwijdert geen jongere data.

De permanente catalogus, capaciteiten, discoverycheckpoints, schemahistoriek,
back-ups en handmatig gemaakte exports vallen niet onder deze automatische
PostGIS-retentie. Back-ups en exports krijgen afzonderlijk beheer buiten deze
wijziging.

## Veilige migratie

De bestaande tabellen worden niet in één blokkerende transactie herschreven.
De overgang verloopt in controleerbare fasen:

1. Maak de nieuwe tabellen, partities, foreign keys en indexen aan zonder de
   catalogustabellen of importer te wijzigen.
2. Laat de worker nieuwe stationmetingen target-onafhankelijk naar de nieuwe
   tabel schrijven. Tijdens een korte overgang mag hij aanvullend de oude
   tabel blijven vullen wanneer dat voor rollback nodig is.
3. Kopieer bestaande stationmetingen in begrensde batches. Leid
   `station_id` en `measurement_type` uit de bestaande metadata af en gebruik
   de canonieke unieke sleutel om per-target duplicaten te verwijderen.
4. Registreer onvolledige of onherkenbare rijen in een migratiesamenvatting;
   verzin geen stationidentiteit. Zulke rijen blijven in de oude tabel tot ze
   beoordeeld zijn.
5. Vergelijk per provider, station, meettype en tijdvenster aantallen en
   waardebereiken tussen oud en nieuw. Bewijs daarnaast dat identieke
   stationmetingen met meerdere targets nog maar eenmaal bestaan.
6. Schakel lezers over naar de nieuwe stationtabel en observeer minimaal één
   volledige providercyclus plus retentiedry-run.
7. Stop het oude stationgerichte schrijfpad pas na geslaagde vergelijking en
   livecontrole.
8. Verwijder of archiveer oude dubbele rijen pas in een afzonderlijke,
   expliciet goedgekeurde opruimstap. Deze ontwerpfase voert geen destructieve
   cleanup uit.

De lopende Netatmo-wereldimporter blijft tijdens alle fasen actief. Geen
migratie herstart, wijzigt of verwijdert diens container, catalogusrijen of
checkpoints.

## Indexen en querygedrag

Iedere stationmeetpartitie krijgt minimaal een index op:

- `(provider_id, station_id, observed_at DESC)`;
- `(measurement_type, observed_at DESC)`.

Ruimtelijke selectie gebruikt de permanente GiST-index op
`stv3_ground_station.position`; de meettabel dupliceert de stationspositie
niet. Een query selecteert eerst stations binnen de regio en haalt daarna hun
metingen binnen het gewenste tijdvenster op.

Resultaattabellen behouden hun huidige functionele indexen, aangevuld met de
partitioneringstijd waar PostgreSQL dat voor pruning vereist.

## Foutafhandeling en herstel

- Een onbekend station wordt eerst idempotent in de catalogus opgenomen of de
  meting wordt met een vaste reden afgewezen; foreign keys worden niet omzeild.
- Een ontbrekende of ongeldige `observed_at` wordt niet vervangen door een
  verzonnen meettijd.
- Een onbekend meettype wordt niet stil samengevoegd met een ander type.
- Bij PostGIS-uitval blijft de bestaande begrensde workerfoutafhandeling van
  kracht; herstel mag geen dubbele stationmetingen creëren.
- Retentie draait fail-closed: zonder geldige cutoff of partitiecontrole wordt
  niets verwijderd.
- Migratie- en retentielogs bevatten aantallen en vaste foutcodes, geen
  providersecrets of volledige bronpayloads.

## Monitoring

De operationele status rapporteert compact:

- aantal permanente stations en capabilities per provider;
- aantal stationmetingen per provider en meettype binnen zes maanden;
- oudste en nieuwste meettijd per tijdgebonden tabel;
- deduplicatieconflicten/upserts;
- laatst geslaagde retentieronde en cutoff;
- migratievoortgang en aantal niet-migreerbare rijen.

Deze statistieken worden op aanvraag of met een licht interval berekend. Een
volledige tabelaudit hoort niet in de normale workeropstart.

## Teststrategie

1. Twee targets ontvangen dezelfde Netatmo-meting; PostGIS bevat daarna exact
   één stationmeetrij.
2. Dezelfde stationtijd met regen, wind en druk levert drie afzonderlijke
   meetrijen.
3. Een retry met gelijke sleutel is idempotent en volgt de vastgelegde
   kwaliteits-/ontvangstregel.
4. Een stationselectie over een regio gebruikt de cataloguspositie en haalt
   alleen relevante stationmetingen op.
5. METAR, Netatmo en een toekomstige Weather Underground-adapter voldoen aan
   hetzelfde opslagcontract zonder providerspecifieke tabel.
6. De backfill dedupliceert per-target kopieën en rapporteert rijen zonder
   herkenbare stationidentiteit of meettype.
7. Partitiepruning beperkt tijdqueries tot relevante maanden.
8. Een retentiedry-run noemt exact de te verwijderen partities en rijen maar
   wijzigt niets.
9. De echte retentietaak bewaart geen rij ouder dan zes kalendermaanden en
   verwijdert nooit permanente station- of capabilitydata.
10. Een onderbroken migratie of retentieronde kan veilig hervatten.
11. De Netatmo-importer kan tijdens de migratie catalogus/checkpoints blijven
    schrijven zonder lockconflict met meetdata.
12. Forecasts, passages en validaties blijven targetgebonden en verwijzen naar
    dezelfde eenmalig opgeslagen fysieke meting.

## Acceptatiecriteria

- Eén fysieke stationmeting bestaat maximaal eenmaal per provider, station,
  meettijd en meettype, ongeacht het aantal targets of regio's.
- Alle nieuwe stationmetingen verwijzen naar een bestaand permanent
  stationrecord.
- Geografische selectie en fusie leveren functioneel dezelfde of betere
  resultaten zonder per-target kopieën.
- Alle tijdgebonden PostGIS-meet- en resultaatdata wordt automatisch op
  maximaal zes maanden gehouden.
- Stationscatalogus, capabilities en importcheckpoints blijven permanent.
- De lopende Netatmo-importer wordt niet gestopt of opnieuw gestart.
- De oude data blijft beschikbaar tot tellingen, waardebereiken, live
  providercycli en rollbackroute aantoonbaar zijn gecontroleerd.
- Normale workeropstart bevat geen volledige databaseaudit.
- Geen bestaande database-export, backup of oude tabel wordt automatisch
  verwijderd als onderdeel van de migratie.

## Buiten scope

- Wijziging van providerpollfrequenties of API-budgetten.
- Herbouw van radarbronfusie, celtracking of meteorologische algoritmen.
- Migratie van de worker naar een andere container of host.
- Automatische verwijdering van NAS-back-ups en handmatige PostGIS-exports.
- Herstart of wijziging van de NUC-Home Assistant.
