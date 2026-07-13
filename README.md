# Storm Tracker V3

Experimentele Home Assistant-integratie voor het combineren van bliksem-,
radar- en grondobservaties tot weersystemen en locatiegebonden voorspellingen.

> Deze integratie is nog in actieve ontwikkeling. Gebruik waarschuwingen niet
> als enige bron voor veiligheidskritische beslissingen.

## Architectuur

```text
Providers -> Observation Fusion Engine -> Storm Engine -> Home Assistant
```

De huidige providers omvatten onder andere Blitzortung, OPERA/EUMETNET,
KMI, KNMI, RainViewer, Netatmo en Open-Meteo. OPERA-cellen met lage
bronkwaliteit worden alleen operationeel gebruikt wanneer KMI, KNMI of
RainViewer ze geografisch en temporeel bevestigt.

## Installatie

Kopieer `custom_components/storm_tracker_v3` naar de map
`/config/custom_components/storm_tracker_v3` van Home Assistant en herstart
Home Assistant.

Voeg daarna bijvoorbeeld het volgende toe aan `configuration.yaml`:

```yaml
storm_tracker_v3:
  home_lat: !secret home_latitude
  home_lon: !secret home_longitude
  fictieve_tracker_entity: device_tracker.fictieve_tracker
  radar_radius_km: 350
  knmi_api_key: !secret knmi_api_key
  knmi_wms_api_key: !secret knmi_wms_api_key
```

KNMI- en Netatmo-instellingen zijn optioneel. Bewaar echte tokens en sleutels
uitsluitend in `secrets.yaml`; commit die nooit naar Git.

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

De huidige integratieversie is **0.4.13**.
