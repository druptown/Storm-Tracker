# Storm Tracker V3

Experimentele Home Assistant-integratie voor het combineren van bliksem-,
radar- en grondobservaties tot weersystemen en locatiegebonden voorspellingen.

> Deze integratie is nog in actieve ontwikkeling. Gebruik waarschuwingen niet
> als enige bron voor veiligheidskritische beslissingen.

## Architectuur

```text
ProjectionTargets -> StormManager -> dynamische RegionEngine(s)
                                      |-> Providers -> OFE -> Storm Engine
                                      `-> persistente MCS-historiek
```

De huidige providers omvatten onder andere Blitzortung, OPERA/EUMETNET,
KMI, KNMI, RainViewer, Netatmo en Open-Meteo. OPERA-cellen met lage
bronkwaliteit worden alleen operationeel gebruikt wanneer KMI, KNMI of
RainViewer ze geografisch en temporeel bevestigt.

Brede OPERA-systemen behouden hun lokale RadarCells. De integratie markeert
een systeem eerst als MCS-kandidaat wanneer de radarstructuur aan de
convectieve criteria voldoet, en pas na drie uur continuïteit als bevestigde
MCS. De aparte sensor `sensor.stv3_mcs_detectie` maakt dit onderscheid
zichtbaar.

## Installatie

Kopieer `custom_components/storm_tracker_v3` naar de map
`/config/custom_components/storm_tracker_v3` van Home Assistant en herstart
Home Assistant.

Ga in Home Assistant naar **Instellingen > Apparaten & diensten > Integratie
toevoegen**, kies **Storm Tracker V3** en selecteer de Life360-trackers van de
personen die je wilt volgen. De thuislocatie en coördinaten worden automatisch
uit Home Assistant gelezen. Een gewone `device_tracker` kan optioneel als
fictieve testtracker worden geselecteerd.

Voor bestaande YAML-installaties blijft deze configuratie voorlopig ondersteund:

```yaml
storm_tracker_v3:
  home_lat: !secret home_latitude       # nieuwe UI-installaties lezen dit uit HA
  home_lon: !secret home_longitude
  fictieve_tracker_entity: device_tracker.fictieve_tracker  # optionele testtracker
  radar_radius_km: 350
  engine_sharing_distance_km: 150
  targets:
    - id: elke
      name: Elke
      location_entity: person.elke
    - id: oma
      name: Oma
      location_entity: device_tracker.oma
      latitude: 51.05       # optionele fallback wanneer de tracker geen GPS heeft
      longitude: 4.42
  knmi_api_key: !secret knmi_api_key
  knmi_wms_api_key: !secret knmi_wms_api_key
```

KNMI- en Netatmo-instellingen zijn optioneel. Bewaar echte tokens en sleutels
uitsluitend in `secrets.yaml`; commit die nooit naar Git.

Extra targets binnen de geconfigureerde `radar_radius_km` delen de operationele
radardata. Een verder target krijgt bewust `onvoldoende_data` tot een eigen
locatiegebonden providerruntime beschikbaar is; zo wordt ontbrekende dekking
niet ten onrechte als droog weer gepubliceerd.

## Ontwikkelen en testen

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Zie [`docs/`](docs/) voor architectuurnotities, het ontwikkelplan en eerdere
release notes.

## Versie

De huidige integratieversie is **0.4.30**.
