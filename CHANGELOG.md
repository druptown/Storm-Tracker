# Storm Tracker V3 — Versiegeschiedenis

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
