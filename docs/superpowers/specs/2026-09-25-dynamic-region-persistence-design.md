# Dynamische regio-opslag in PostGIS

**Datum:** 25 september 2026  
**Status:** ter review  
**Basis:** het bestaande `ThinRegionPlanner`-model met thuisregio, externe
clusters, hysteresis en een afzonderlijke radaranalysestraal.

## Doel

Bewaar meteorologische bronfeiten precies één keer en koppel ze aan de exacte
dynamische regio waarvoor ze werden opgehaald of gebruikt. Personen delen de
brondata van hun regio, maar behouden een eigen locatiegebonden voorspelling.
Een verdwenen, verplaatste of later opnieuw aangemaakte externe regio mag
historische gegevens niet met een andere regioversie vermengen.

## Identiteiten en levenscyclus

Het model gebruikt drie niveaus:

- `region_id`: logische planneridentiteit, zoals `region:home` of de hash van
  een extern doelpersonencluster;
- `region_instance_id`: één ononderbroken activatie van die logische regio;
- `region_revision_id`: één exacte geografische voetafdruk binnen die
  activatie, inclusief centrum, waarschuwingsstraal en radaranalysestraal.

De thuisregio behoudt één duurzame instantie. Een externe regio krijgt bij
iedere nieuwe activatie een nieuwe instantie. Kleine GPS-wijzigingen binnen de
hysteresis veranderen niets. Een recentering boven de bestaande drempel maakt
een nieuwe revisie binnen dezelfde instantie. Verdwijnt een regio, dan worden
instantie, laatste revisie en doelkoppelingen afgesloten met `valid_until`;
ze worden niet direct verwijderd.

De planner levert instantie- en revisie-identiteiten mee in ieder
`ThinRegionPlan`. De identiteiten zijn deterministisch binnen de lopende
activatie. Een workerherstart mag een actieve externe regio als nieuwe
instantie openen; hierdoor worden nooit twee gescheiden activaties
samengevoegd.

## Stationsselectie

De stationscatalogus blijft globaal en doelpersoononafhankelijk. Een station
wordt dus niet permanent eigendom van een regio.

Bij iedere nieuwe regio-revisie selecteert PostGIS op positie, dekking,
operationele status en benodigde sensormogelijkheden de toepasselijke
stations. De bestaande minimale-selectielogica bepaalt welke daarvan werkelijk
worden gepolld. De selectie verwijst naar `region_revision_id` en heeft een
geldigheidsperiode. Bij recentering wordt de selectie opnieuw berekend; bij
deactivatie stopt polling voor die revisie.

Een nieuw ontdekt station wordt eenmaal aan de globale catalogus toegevoegd.
Vanaf de volgende relevante selectie kan het automatisch worden gebruikt.

## Opslagcontract

PostGIS bevat:

- `stv3_region_instance`: activatie, status en begin/eindtijd;
- `stv3_region_revision`: exacte geometrie, stralen en geldigheid;
- `stv3_region_target`: doelpersoon, exacte positie, revisie en
  geldigheidsperiode;
- `stv3_observation`: één canonieke algemene bronobservatie;
- `stv3_observation_region`: veel-op-veelkoppeling van observatie naar
  regio-revisie;
- `stv3_station_measurement`: één canonieke fysieke stationsmeting;
- `stv3_station_measurement_region`: veel-op-veelkoppeling van meting naar
  regio-revisie;
- bestaande cellen, tracks, passages en doelvoorspellingen, uitgebreid met de
  toepasselijke `region_revision_id` waar dat voor herleidbaarheid nodig is.

De canonieke observatiesleutel bevat nooit een doelpersoon of regio. Eén feit
kan daardoor zonder duplicatie voor meerdere overlappende regio’s bruikbaar
zijn. Koppeltabellen bewaren alleen de context waarin het feit is gebruikt.

## Voorspellingen

Radar-, bliksem- en gronddata worden per regio-revisie genormaliseerd en
gefuseerd. Celherkenning en tracking verwerken het regionale ruimtelijke veld.
Daarna wordt de voorspelde buivoetafdruk afzonderlijk met de exacte positie van
iedere actieve doelpersoon gekruist.

Twee personen in dezelfde regio delen dus brondata en buitracks, maar kunnen
verschillende aankomsttijd, vertrektijd, intensiteit, bliksemrisico en
confidence krijgen. Een positiewijziging binnen dezelfde revisie vereist
alleen een nieuwe doelberekening; brondata wordt niet opnieuw opgeslagen.

## Retentie en herstel

Operationele historie en gesloten regiolifecycles worden zes maanden bewaard.
De stationscatalogus, sensormogelijkheden, importerprogressie en configuratie
vallen niet onder deze verwijdering. Foreign keys met cascades ruimen alleen
contextkoppelingen op wanneer hun zes maanden oude bronfeit wordt verwijderd.

De writer schrijft in één transactie: regio-instantie en revisie, actieve
doelkoppelingen, canonieke bronfeiten, regiokoppelingen en resultaten. Een
PostGIS-fout laat de bestaande retrybuffer intact en publiceert geen
gedeeltelijke regiotoestand.

Migratie `0006` is nog niet live toegepast en wordt vóór deployment vervangen
door dit model. Omdat de oude operationele historie reeds bewust is gewist,
hoeft geen onbetrouwbare legacy-doelpersoonkoppeling naar regio’s te worden
gegokt. De behouden stationscatalogus en importerstatus blijven onaangeroerd.

## Acceptatie

- Twee doelpersonen in één regio veroorzaken één observatie, één meting en één
  koppeling per gebruikte regio-revisie.
- Twee regio’s die hetzelfde bronfeit gebruiken veroorzaken één bronrij en
  twee regiokoppelingen.
- Recentering maakt een nieuwe revisie; oude feiten blijven naar de oude
  revisie wijzen.
- Deactivatie sluit de externe instantie en stopt stationspolling zonder
  historie te verwijderen.
- Heractivatie maakt een nieuwe instantie, ook bij dezelfde doelpersonen.
- Doelvoorspellingen blijven per persoon verschillen op basis van hun exacte
  positie.
- Retentie verwijdert feiten ouder dan zes maanden maar behoudt catalogus,
  capabilities en importerprogressie.
- Gerichte tests, de brede dunne-runtime/PostGIS-suite, releasebouw en live
  PostGIS-controle op uitsluitend NAS-HA slagen vóór oplevering.

## Niet-doelen

- Geen nieuwe audit-, RAW-, authority- of manifestinfrastructuur.
- Geen permanente duplicatie van stations per regio.
- Geen NUC-deploy of NUC-mirror.
- Geen wijziging aan de meteorologische fusieformules buiten het toevoegen van
  de correcte regio-revisiecontext.
