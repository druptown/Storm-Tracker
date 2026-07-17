# Storm Tracker V3 — Versiegeschiedenis

## v0.4.28

- Eerste backwards-compatible multi-targetlaag: naast de bestaande fictieve
  tracker kunnen meerdere personen of vaste locaties worden geregistreerd.
- Elk target krijgt een stabiele eigen neerslagstatussensor en wordt gekoppeld
  aan een gedeelde of aparte RegionEngine op basis van de sharing distance.
- Operationele observaties worden via de StormManager naar alle relevante
  regio-engines gerouteerd.

## v0.4.27

- Bewegingshistoriek van radarsystemen wordt nu samen met de MCS-snapshot
  bewaard en na een Home Assistant-herstart onmiddellijk hersteld.
- Oude snapshots zonder bewegingshistoriek worden automatisch uit hun
  radarcellen en parent-footprints gereconstrueerd.
- Meerdere updates van hetzelfde radarproduct vervangen hetzelfde centroidpunt
  in plaats van kunstmatige nul-tijdsintervallen aan de regressie toe te voegen.

## v0.4.26

- Nieuwe `sensor.stv3_neerslagstatus` vat de operationele toestand samen als
  `droog`, `waargenomen`, `bevestigd` of `naderend`.
- De sensor verkiest bevestigde systemen boven eenmalige echo's en publiceert
  afstand, impactpunt, dBZ, radardekking, beweging, ETA en luchtdruktrend in
  één stabiel datacontract voor dashboard en automatiseringen.

## v0.4.25

- Sluimerende systemen blijven intern kort beschikbaar voor lifecyclebeheer,
  maar worden niet langer als actieve storms naar Home Assistant gepubliceerd.
- Systemen tonen nu `tracking_status`, het aantal opeenvolgende radarframes en
  het tijdstip van het laatste radarframe.
- Eén radarframe krijgt status `waargenomen`; vanaf twee aansluitende frames is
  het systeem `bevestigd`. Daardoor kan het dashboard tijdelijke echo's herkenbaar
  scheiden van aanhoudende neerslag.
- Naderingssnelheid en ETA van radarsystemen worden onderdrukt tot het systeem
  door minstens twee opeenvolgende frames bevestigd is.

## v0.4.24

- OPERA `qi_total` is niet langer de enige zelfstandige toelatingsroute:
  meteorologisch plausibele cellen van minstens 50 km², gemiddeld minstens
  20 dBZ en met een piek van minstens 30 dBZ worden als `structured_echo`
  aanvaard, ook bij een lage quality-score.
- Zwakke brede echo's blijven kruisbronbevestiging vereisen; de foutcel nabij
  België van circa 12-14 dBZ wordt dus niet opnieuw toegelaten.
- Diagnostiek onderscheidt quality-, structuur- en kruisbronacceptatie.

## v0.4.23

- OPERA-cellen kunnen niet langer door de groene KMI-basiskaart worden
  bevestigd; het KMI-beeld wordt niet als onafhankelijke pixelreferentie
  gebruikt zolang kaartkleuren en regenkleuren niet betrouwbaar gescheiden zijn.
- De opaak-witte KNMI WMS-achtergrond (intensiteit 1) telt niet langer als
  neerslagbevestiging; alleen echte KNMI-radarkleuren vanaf intensiteit 2 en
  RainViewer-neerslag mogen lage-kwaliteit OPERA-echo's bevestigen.
- OPERA-diagnostiek toont voortaan zowel het ruwe als bruikbare aantal
  corroboratiereferenties.

## v0.4.22

- Netatmo-luchtdruk wordt per station over twee uur gevolgd; regionale
  drukverandering gebruikt de mediaan van gepaarde stations en is daardoor
  niet gevoelig voor onderlinge hoogteverschillen.
- Nieuwe sensor `sensor.stv3_netatmo_luchtdruktrend` toont de verandering over
  60 minuten, trends over 15/30/60 minuten, stationsdekking en snelle drukval.
- Onrealistische drukwaarden en sprongen worden geweigerd; een trend verschijnt
  pas zodra minstens drie stations een bruikbare historische vergelijking geven.
- De twee uur drukhistoriek wordt in Home Assistant-opslag bewaard, zodat een
  herstart de opgebouwde trend niet wist.

## v0.4.21

- De dichtstbijzijnde-storm-sensor blijft beschikbaar wanneer een nieuw
  stormsysteem nog geen snelheidsvector heeft; lege naderingssnelheid wordt
  nu veilig als `null` gepubliceerd.

## v0.4.20

- Bewegingskwaliteit gebruikt nu zowel het aantal meetpunten als de tijdspanne
  en regressie-fit, zodat een korte of grillige reeks minder vertrouwen krijgt.
- De dichtstbijzijnde-storm-sensor toont koers, windroosrichting,
  naderingssnelheid en of het systeem werkelijk naar de tracker beweegt.
- Een ETA wordt alleen berekend uit de snelheidscomponent richting de tracker;
  zijwaarts bewegende en wegtrekkende systemen krijgen geen misleidende ETA.
- Bewegingspunten, historie en fitkwaliteit zijn als diagnostische attributen
  beschikbaar op de stormsensoren.

## v0.4.19

- Blitzortung abonneert niet langer op de volledige wereldfeed, maar gebruikt
  een gededupliceerde unie van geohash-topics rond alle actieve RegionEngines.
- Regiowissels verversen subscriptions op dezelfde gedeelde MQTT-client.
- MQTT-disconnects worden direct gedetecteerd en krijgen exponentiële backoff
  van één tot vijftien minuten, met jitter om proxy-rate-limits te respecteren.
- Foutmeldingen tonen voortaan de foutklasse en een bruikbare reden in plaats
  van een lege `()`-melding iedere 25 seconden.

## v0.4.18

- De bestaande Open-Meteo Gear-sensor wordt nu daadwerkelijk bij Home
  Assistant geregistreerd, zodat `sensor.stv3_open_meteo_gear` niet langer
  als een oude herstelde, maar niet-beschikbare entiteit blijft staan.

## v0.4.17

- RainViewer gebruikt voortaan de werkelijke frame-timestamp uit het manifest
  in plaats van het lokale verwerkingstijdstip.
- Radarframes ouder dan twintig minuten worden als ongezond geweigerd en
  kunnen niet langer als operationele fallback of OPERA-bevestiging dienen.
- De RainViewer-sensor toont poll-, succes- en frametijd, frameleeftijd,
  providergezondheid, foutreden, foutenteller en het gebruikte framepad.
- Manifestproblemen en herstel worden zichtbaar gelogd zonder elke vijf
  minuten dezelfde waarschuwing te herhalen.

## v0.4.16

- Open-Meteo-grid verlaagd van 948 naar 324 modelpunten; OPERA blijft de
  fijnmazige primaire radarbron.
- Succesvolle Open-Meteo-resultaten worden 30 minuten gecachet en niet opnieuw
  als verse OFE-observaties verwerkt.
- HTTP 429 activeert exponentiÃ«le backoff van 30 minuten tot maximaal zes uur
  en respecteert een numerieke `Retry-After`-header.
- `wet_locations_now` wordt nu werkelijk geleverd voor grondverificatie.

## v0.4.15

- De `StormManager` bezit nu de werkelijk gebruikte StormEngine/OFE-runtime;
  `region_manager.py` is niet langer ongebruikte architectuurcode.
- Sharing distance en observatieradius zijn onafhankelijk configureerbaar.
- Een target buiten de sharing distance krijgt een nieuwe dynamische
  RegionEngine; een lege engine wordt automatisch verwijderd.
- De globale Blitzortung-stream wordt naar alle relevante actieve engines
  gerouteerd zonder een extra listener per regio.
- Compacte MCS-framehistoriek wordt met Home Assistant `Store` per geografische
  runtime-regio bewaard en na een restart hersteld.
- Volledige OPERA-rasters worden nooit persistent opgeslagen.
- Nieuwe diagnostische entiteit `sensor.stv3_region_engines` toont actieve
  engines, centra, observatieradius, gekoppelde targets en WeatherSystems.

## v0.4.14

- Radar-gebaseerde MCS-classificatie toegevoegd met afzonderlijke statussen
  `candidate` en `confirmed`.
- Een kandidaat vereist minstens twee kernen van 40 dBZ, minstens één kern van
  50 dBZ en een convectieve span van minstens 100 km.
- Bevestiging gebeurt pas wanneer de criteria drie uur aaneengesloten gelden;
  korte onderbrekingen tot twintig minuten zijn toegestaan.
- Zes uur parent-framehistoriek wordt compact per WeatherSystem bijgehouden.
- Nieuwe entiteit `sensor.stv3_mcs_detectie` toont kandidaten, bevestigde MCS'en
  en de gebruikte metriek/criteria.

## v0.4.13

- Eén brede OPERA-component wordt als parent-WeatherSystem behouden, met de
  adaptief gesegmenteerde zware kernen als afzonderlijke RadarCells.
- Parent- en cell-ID's, oppervlaktes en compacte footprints blijven door de
  Observation Pipeline tot in de WeatherSystem Engine beschikbaar.
- Afstand tot de dichtstbijzijnde storm gebruikt voortaan de dichtste lokale
  RadarCell-footprint in plaats van uitsluitend de systeemcentroid.
- Diagnostiek toont per OPERA-cel ook de parent-component en parentoppervlakte.

## v0.4.12

- Buitensporig grote OPERA-componenten worden adaptief opgesplitst rond
  afzonderlijke zware regenkernen.
- Lichte regen mag nog vijf rasterpixels rond een kern meegroeien, maar kan
  verschillende kernen niet opnieuw aan elkaar verbinden.
- Kleine en gelijkmatig lichte regengebieden behouden de bestaande 8 dBZ-
  gevoeligheid.

## v0.4.11

- OPERA-observaties bewaren voortaan een compacte footprint van werkelijk
  bezette rasterblokken.
- RainViewer en nationale radars kunnen een lage-quality OPERA-cel nu over de
  volledige celvorm bevestigen, ook wanneer het centroid ver weg ligt.
- De footprint gebruikt echte celpixels en geen brede bounding box, zodat lege
  ruimte naast een langgerekte bui geen valse bevestiging veroorzaakt.

## v0.4.10

- RainViewer draait parallel als verificatiebron wanneer OPERA gezond is.
- Lage OPERA-quality kan nu door recente RainViewer-neerslag bevestigd worden.
- Bij OPERA-uitval wordt dezelfde RainViewer-fetch als fallback hergebruikt.
- Een ongewijzigd RainViewer-frame hergebruikt zijn observaties in plaats van
  tijdelijk nul observaties te publiceren.

## v0.4.9

- Alle providerobservaties worden begrensd tot het actuele monitoringsgebied.
- Wereldwijde Blitzortung-inslagen buiten de ingestelde radarradius worden genegeerd.
- Bij een trackerverplaatsing verdwijnen WeatherSystems buiten de nieuwe regio.
- Wachtende OFE-observaties uit de vorige regio worden bij de wissel gewist.
- De sensor `STV3 Actieve Radarbron` publiceert opnieuw zijn runtimewaarde.

## Overzicht per module

| Module | Huidig | Omschrijving |
|---|---|---|
| `__init__.py` | v0.2.0 | Hoofdsetup, providers, polling, fictieve tracker |
| `sensor.py` | v0.1.0 | HA sensoren voor alle providers |
| `const.py` | v0.1.0 | Constanten (DOMAIN) |
| `providers/base.py` | v0.1.0 | Plugincontract (Capability, CoverageResult, ProviderPlugin, ProviderRegistry) |
| `providers/opera.py` | v0.1.0 | OPERA/MeteoGate S3 radar provider (heel Europa, 1km², HDF5) |
| `engine/observation.py` | v0.1.0 | Observation dataclass |
| `engine/observation_fusion_engine.py` | v0.1.0 | OFE: deduplicatie, batching |
| `engine/storm.py` | v0.1.0 | Storm dataclass |
| `engine/storm_engine.py` | v0.1.0 | Clustering, merge, lifecycle, regressie |
| `engine/region_manager.py` | v0.5.0 | Dynamische RegionEngines per ProjectionTarget |
| `geometry/bounding_box.py` | v0.1.0 | Bounding box berekeningen |
| `geometry/distance.py` | v0.1.0 | Haversine afstandsberekening |
| `geometry/geocode.py` | v0.1.0 | Plaatsnaam lookup |
| `geometry/hull.py` | v0.1.0 | Convex hull berekening |
| `plogger/provider_logger.py` | v0.2.0 | CSV logging per provider |
| `providers/blitzortung.py` | v0.6.0 | Blitzortung MQTT provider |
| `providers/kmi.py` | v0.3.0 | KMI radar provider |
| `providers/knmi.py` | v0.2.0 | KNMI WMS radar + nowcast provider |
| `providers/netatmo.py` | v0.2.0 | Netatmo grondstations provider |
| `providers/open_meteo.py` | v0.3.0 | Open-Meteo grid provider |
| `providers/rainviewer.py` | v0.1.0 | RainViewer tile provider |

---

## Changelog per module

### `__init__.py`
- **v0.2.0** — Providers volgen fictieve tracker locatie; herinitialisatie bij verplaatsing >1km; CoreState/EVENT_HOMEASSISTANT_STARTED pattern; cross-trigger verwijderd
- **v0.1.0** — Eerste versie; providers hardcoded op home_lat/lon

### `sensor.py`
- **v0.1.0** — Eerste versie; sensoren voor alle providers; async_added_to_hass fix voor unavailable state

### `providers/blitzortung.py`
- **v0.6.0** — paho-mqtt MQTTv311 exact zoals bestaande HA-integratie; geen filter op afstand
- **v0.5.0** — aiomqtt (timeout problemen)
- **v0.4.0** — directe WebSocket (SSL + beleidsproblemen)
- **v0.3.0** — factory-patroon + supports()
- **v0.2.0** — Observation model
- **v0.1.0** — state listener op HA-integratie (afhankelijk van externe integratie)

### `providers/kmi.py`
- **v0.3.0** — volledige radarplaatjes downloaden via URI uit animatiesequentie; dynamische afmetingen; ww weercode uitlezen
- **v0.2.0** — correcte API: getForecasts + lat/lon + User-Agent: be.meteo.app
- **v0.1.0** — eerste versie (getIncaList zonder lat/lon → lege response)

### `providers/knmi.py`
- **v0.2.0** — correcte dataset/layer namen (radar_forecast_2.0/precipitation_nowcast); aparte WMS API key; alle 25 nowcast tijdstappen
- **v0.1.0** — eerste versie (verkeerde dataset/layer namen → 404)

### `providers/netatmo.py`
- **v0.2.0** — volledige parsing: rain_live, rain_5min, wind_strength, wind_angle, gust_strength, pressure, temperature, humidity per station
- **v0.1.0** — eerste versie (alleen rain_live)

### `providers/open_meteo.py`
- **v0.3.0** — lat/lon van natte punten bijgehouden in wet_locations_now/forecast; timezone als array fix
- **v0.2.0** — timezone als array (vereist door Open-Meteo POST API); minutely_15 nowcast 90min
- **v0.1.0** — eerste versie (timezone als string → 400 error)

### `providers/rainviewer.py`
- **v0.1.0** — eerste versie; 3×3 tile grid; pixel → lat/lon conversie

### `plogger/provider_logger.py`
- **v0.2.0** — non-blocking via async_add_executor_job; locaties in open_meteo log
- **v0.1.0** — eerste versie (blocking I/O in event loop)

### `engine/region_manager.py`
- **v0.5.0** — volledig dynamische RegionEngines zonder vaste landsgrenzen; providers bepalen zelf dekking via supports()
- **v0.4.0** — ProjectionTarget-terminologie
- **v0.3.0** — één canonieke blitz_entity per regio (verouderd)
- **v0.2.0** — geografische regio's op basis van land (verouderd)
- **v0.1.0** — eerste opzet

### `engine/observation_fusion_engine.py`
- **v0.1.0** — eerste versie; deduplicatie per type/locatie; 1s batching; 1u buffer

### `engine/storm_engine.py`
- **v0.1.0** — eerste versie; clustering, merge, lifecycle, regressie, geocoding

### `engine/storm.py`
- **v0.1.0** — eerste versie; Storm dataclass met centroid, beweging, geometrie

### `geometry/*`
- **v0.1.0** — eerste versie van alle geometrie helpers

---

## Bekende issues / TODO

- [ ] OFE nog niet verbonden met providers in `__init__.py`
- [ ] StormEngine nog niet geactiveerd
- [ ] RegionEngine/StormManager nog niet geïntegreerd
- [ ] ProjectionEngine (ETA, passage) nog niet gebouwd
- [ ] Multi-persoon tracking nog niet geïmplementeerd
- [ ] Open-Meteo polling interval: 120 min (tijdelijk na rate limit issues)
- [ ] Versioned ZIP backups ontbreken nog

### `providers/base.py`
- **v0.1.0** — eerste versie; Capability, CoverageArea, CoverageResult, ProviderContext, ProviderPlugin Protocol, ProviderRegistry met strategie per capability

### `providers/opera.py`
- **v0.1.0** — eerste versie; OPERA DBZH composiet via MeteoGate S3 bucket; HDF5 parsing via h5py/numpy/pyproj; component labeling uit PoC; Observations per regencel
# v0.4.4

- OPERA discovery controleert recente, deterministische vijfminutenbestanden
  nu eerst rechtstreeks met HTTP HEAD.
- De brede S3-listing blijft beschikbaar als fallback.
- Extra regressietest voor discovery wanneer listing een actueel bestand mist.
- Testomgeving bevat nu alle providerafhankelijkheden en een portable
  `radar_policy`-fixture.
# v0.4.5

- OPERA `qi_total` wordt niet langer als harde quality-filter gebruikt; live
  CIRRUS-producten rapporteerden `0.0` voor echte neerslagpixels.
- Detectiedrempel verlaagd van 10 naar 8 dBZ.
- Minimum celoppervlakte verlaagd van 9 naar 5 pixels/km².
- Actieve detectiedrempels toegevoegd aan de OPERA-sensordiagnostiek.
# v0.4.6

- Standaard OPERA/radarradius verhoogd van 200 naar 300 km om cellen boven
  Drenthe/Overijssel vanuit de thuisregio tijdig te volgen.
- Werkelijke radius en WGS84-bbox worden voortaan getoond als attributen van
  `sensor.stv3_opera_observaties`.
# v0.4.7

- `sensor.stv3_opera_observaties` toont maximaal 40 ruwe OPERA-cellen met
  coördinaten, afstand, oppervlakte, dBZ, quality en pixelcount.
- Diagnostieklijst is begrensd om state-attributen compact te houden.
- Cellen in de verre hoeken van de geprojecteerde bbox worden nu met een echte
  grootcirkelafstand begrensd tot `radius_km`.
