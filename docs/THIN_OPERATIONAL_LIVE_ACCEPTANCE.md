# Thin operational live acceptance

Doel: uitsluitend NAS-Home Assistant `192.168.0.15`. De NUC
`192.168.0.144` wordt niet benaderd of gewijzigd.

## Release

- integratie: `1.14.129`
- worker: `1.18.26`
- runtime: `thin_operational`
- rollbackback-up: `ad4478bc`
- deployrun:
  `/backup/storm_tracker_deploy_runs/integration_1_14_129_20260923T194321Z`

## Verplichte live checks

- [x] Worker-App start en publiceert binnen 15 seconden een online heartbeat.
- [x] HA stuurt de actuele desired state; een oudere generatie kan die niet
  overschrijven.
- [x] Binnen 60 seconden verschijnt een geldig snapshot op
  `storm_tracker/v1/operational/snapshot`.
- [x] HA accepteert dat snapshot als `current` en toont worker `1.18.26`.
- [x] OPERA en Open-Meteo zijn voor een Belgische tracker `ready` en leveren
  hun actuele status onafhankelijk van elkaar.
- [x] METAR en beschikbare Netatmo-data worden geografisch begrensd verwerkt.
- [x] KMI toont `not_ready/api_required` zolang de officiële API-toegang
  ontbreekt; dit blokkeert de andere bronnen niet.
- [x] Actuele regen verschijnt als meting én als kaartcel; een gevolgde cel kan
  een ETA/voorspelling leveren.
- [x] De tabellen `stv3_observation`, `stv3_fusion_cycle`, `stv3_storm_cell`,
  `stv3_track_point` en `stv3_target_forecast` in PostGIS worden bijgewerkt.
- [x] Een tijdelijke PostGIS-fout maakt de worker `degraded`, maar onderdrukt
  heartbeat en snapshot niet.
- [x] Geen `raw_`, `acquisition_`, `completion_` of `terminal_` runtime wordt
  door het thin pad geladen.

Live bewijs 2026-09-20: MQTT-kaart ontvangen door HA; snapshot en worker zijn
actueel. Vanaf worker `1.18.9` heeft de kaart geen vaste limiet van 64
analysecellen meer: alle actuele cellen worden gepubliceerd zolang de volledige
GeoJSON-payload onder 900.000 bytes blijft. De
PostGIS-migratie `0002_thin_operational_results` is op 20 september 2026
eenmalig met de beheerrol toegepast. Worker `1.18.3` voert geen DDL meer uit;
`storm_ingest` behoudt alleen `USAGE` en `SELECT`/`INSERT`/`UPDATE`. Live groeide
de opslag van de eerste cyclus naar 1.094 observaties, 19 fusiecycli, 1.216
stormcellen, 1.216 trackpunten en 152 targetvoorspellingen. De worker logde
`postgis=current` en MQTT/HA bleef actueel.

Aanvullend live bewijs 2026-09-23: alle acht actuele doelen delen exact één
`region:home`; de regio-entiteit rapporteert `1` en `count_valid=true`.
Meerdere volledige cycli bevatten 7.060–7.252 observaties met METAR, Netatmo,
Blitzortung, Open-Meteo, DWD, KNMI en OPERA operationeel. KMI bleef expliciet
`api_required/external_access_required`. PostGIS bleef `current`, de worker en
snapshot bleven actueel en de gerichte thin/release/verpakkingssuite slaagde
met 133 tests. Het volledige releasebewijs staat in
`docs/DEPLOY_2026-09-23_1.14.123_1.18.22.md`.

Release `1.14.124 / 1.18.23` verwijdert vervolgens de laatste onbedoelde
runtime-importen van de oude RAW/acquisitionlaag. Een schoon proces laadt geen
verboden legacy-prefix meer. De eerste live cyclus bevatte 7.665 observaties,
publiceerde acht kaartfeatures en schreef met `postgis=current`; het
releasebewijs staat in `docs/DEPLOY_2026-09-23_1.14.124_1.18.23.md`.
Netatmo leverde in de eerste cyclus actuele waarden maar rapporteerde in twee
latere cycli `ready/degraded` met `RuntimeError`; de andere providers en
PostGIS bleven operationeel/current. Die bronfout blijft daarom een expliciete
vervolgactie en blokkeert de Belgische output niet.

## Niet blokkeren op externe bronnen

Providers buiten België worden niet meer als voorwaarde voor de Belgische live
acceptatie behandeld. Iedere afgewerkte adapter behoudt wel een vaste
`ready/not_ready`-status plus de actuele operationele status. Een geografisch
niet-toepasselijke of extern geblokkeerde provider mag Belgische output nooit
tegenhouden.

## Uitbreiding 1.14.126 / 1.18.25

De actuele NAS-release voegt de dunne stationscatalogus, volledige
voetafdrukpassages, grondvalidatie en offline replay toe. PostGIS-migratie
`0003_station_catalog_validation` is toegepast met contract
`stv3-thin-station-catalog-validation-v1`; bestaande operationele tabellen en
rijen zijn behouden. Een live gevonden `location_accuracy_m=null` van een
Life360-doel is vóór de definitieve release met een regressietest hersteld.

Live bewijs na de hotfix:

- `sensor.stv3_worker_status=online`, worker `1.18.25` en een oplopende
  snapshotrevision;
- eerste volledige cyclus: 24 observaties, acht kaartfeatures en PostGIS
  `current`;
- METAR, Netatmo, Blitzortung, Open-Meteo, DWD, KNMI en OPERA bleven
  provider-lokaal operationeel; KMI bleef correct `api_required` en
  Wunderground correct `blocked_api_key`;
- de relevante thin-, integratie-, release- en verpakkingssuite slaagde met
  `261 passed`;
- de eenmalige offline catalogus bevat 9.552 METAR-stations, 8.013 unieke
  Netatmo-stations en 9 Wunderground-seeds. Voor de Belgische Netatmo-box zijn
  alle 50 hoofdtegels compleet; 7.144 stations liggen binnen die box.

Volledig releasebewijs staat in
`docs/DEPLOY_2026-09-23_1.14.126_1.18.25.md`.

## Catalogusduurzaamheid 1.14.127 / 1.18.26

De offline stationmetadata verloopt niet langer automatisch na zeven dagen.
Alleen een expliciet niet-operationeel station wordt uit het stationsplan
geweerd; de versheid van iedere live meetwaarde blijft onafhankelijk streng
gecontroleerd. Daarmee blijft de eenmalige catalogus werkelijk bruikbaar.

Live na de update:

- integratie `1.14.127` geladen en worker-App `1.18.26` gestart;
- worker online, snapshotrevision oplopend en exact één geldige regio;
- Netatmo opnieuw `operational` na de eenmalige catalogusbelasting;
- PostGIS-catalogus ongewijzigd behouden: 9.552 METAR, 8.013 Netatmo en
  9 Wunderground; 54 Netatmo-checkpoints compleet en nul incompleet;
- gerichte thin-, release- en verpakkingssuite: `263 passed`.

Volledig releasebewijs staat in
`docs/DEPLOY_2026-09-23_1.14.127_1.18.26.md`.

## Betrouwbare config-entry-reload 1.14.129 / 1.18.26

Een live reloadtest na ontvangst van kaart-, passage- en validatiedata vond
achtergebleven entrycaches die de volgende setup ten onrechte als een tweede
globale runtime zagen. Integratie `1.14.129` ruimt nu ook die kaartcaches en
markers op.

Live bewijs:

- de entry werd na een reeds ontvangen workerkaart officieel herladen en bleef
  `loaded`;
- geen nieuwe melding `globale runtime is bezet`;
- worker-App bleef op `1.18.26` en werd door de integratie-only installatie
  niet gewijzigd;
- na één workerherstart: worker online, snapshotrevision `212356197`, één
  geldige regio en Netatmo `operational`;
- fictieve tracker exact hersteld naar `51.0498071, 4.4186241`;
- finale lifecycle-, integratie-, release- en route-suite: `148 passed`.

Volledig releasebewijs staat in
`docs/DEPLOY_2026-09-23_1.14.129_1.18.26.md`.

## Geldige radarcontouren 1.14.129 / 1.18.27

De worker splitst een rastergat dat de buitencontour op één hoekpunt raakt nu
in afzonderlijke polygonringen. Daarmee kan een OPERA-component niet langer
door dat patroon als zelfsnijdende ring in PostGIS belanden. De gerichte
operationele/release-suite slaagde met `152 passed`; alle dunne
`test_thin_*`-modules samen met `153 passed`.

Live op NAS-HA: worker `1.18.27` online, snapshot en PostGIS `current`, de
Belgische bronnen operationeel, en actuele fusiecycli, cellen, tracks en
doelvoorspellingen aanwezig. Er is niets naar de NUC gestuurd. Een latere
livecontrole van het echte OPERA-product van `06:55 UTC` genereerde achttien
buipolygonen; PostGIS verklaarde ze alle achttien geldig. De gevalideerde lokale
contourcode en het bestand op de NAS hadden exact dezelfde SHA-256. Volledig
Een vastgelopen desired-state-overdracht is aansluitend met uitsluitend een
officiële integratiereload hersteld: controller idle, snapshot current en
OPERA ready/operational/current. Volledig bewijs staat in
`docs/DEPLOY_2026-09-24_1.14.129_1.18.27.md`.
