# Gewone Home Assistant: integration-only deploy

## Doel en status

Dit is de veilige route wanneer Storm Tracker voorlopig op een gewone Home
Assistant-installatie draait, zonder de Storm Tracker Worker App te installeren.
De releasebuilder blijft de bestaande, gekoppelde integratie- en workerartefacten
plus evidence produceren en verifiëren. De installatieroute hieronder installeert
uitsluitend:

```text
storm_tracker_v3_v<versie>_deploy.zip
```

De worker-`_deploy.tar.gz` wordt door het pair-evidence read-only geverifieerd,
maar wordt niet gekopieerd, uitgepakt, geïnstalleerd of gestart.

Deze route verandert niets live uit zichzelf. Het lokale resultaat houdt alle
live gates expliciet open en zet `live_installation_ready` altijd op `false`.

## Verwacht gedrag zonder worker

De kandidaat moet statisch aantonen dat:

- de config-entry schema `v3` gebruikt;
- `runtime_mode` exact `worker_only` is;
- Home Assistant geen legacy providers, decoders of meteorologische compute
  start;
- een afwezige MQTT-client/worker als workerstatus `offline` wordt weergegeven;
- ontbrekende of stale workerfeiten als `onvoldoende_data` worden weergegeven,
  nooit als `droog` of als een actuele waarschuwing.

`offline` en `onvoldoende_data` zijn in deze tijdelijke opstelling dus een
eerlijke gedegradeerde toestand. Ze zijn geen bewijs dat de volledige
meteorologische productfunctie operationeel is.

## Lokale, reproduceerbare dry-run

Bouw eerst de normale releasepair met de bestaande builder. Gebruik daarna het
ongewijzigde pair-evidence als input voor de gewone-HA dry-run:

```powershell
python -B scripts/dry_run_ordinary_ha_release_install.py `
  --candidate-evidence <release-map>\storm_tracker_release_v<integratie>_worker_v<worker>_evidence.json `
  --run-root .analysis\ordinary-ha-integration-only-<unieke-id>
```

De `run-root` moet nieuw zijn en onder project-`.analysis` of de OS-tempmap
liggen. De simulator:

1. verifieert het bestaande integratie/worker pair-evidence en beide archieven;
2. bindt en materialiseert uitsluitend de integratiezip;
3. controleert in de uitgepakte kandidaat schema-v3, `worker_only`, de lichte
   MQTT-afhankelijkheid en de fail-closed offlineprojecties;
4. simuleert installatie terwijl de config-entry uitgeschakeld is;
5. verplaatst de gesimuleerde mislukte kandidaat naar quarantine;
6. herstelt exact dezelfde integratieversie uit exact dezelfde SHA-256;
7. publiceert een content-addressed rapport zonder netwerk- of liveclaim.

Een groene lokale simulatie sluit alleen de lokale blocker namespace. Alle
live gates blijven in het rapport staan.

## Niet-overlappende doelpaden en exact één manifest

Normaliseer vóór iedere doelmutatie alle ontdekte paden op Home Assistant naar
hun canonieke absolute pad. De volgende layout is verplicht:

```text
/homeassistant/custom_components/storm_tracker_v3
/homeassistant/storm_tracker_deploy/<unieke-run>/predeploy-snapshot/storm_tracker_v3
/homeassistant/storm_tracker_deploy/<unieke-run>/staging-initial/storm_tracker_v3
/homeassistant/storm_tracker_deploy/<unieke-run>/quarantine/storm_tracker_v3.candidate_failed
/homeassistant/storm_tracker_deploy/<unieke-run>/staging-same-version-recovery/storm_tracker_v3
```

Het eerste pad is het enige actieve componentpad. De andere componentmappen
moeten strikt onder dezelfde unieke deployrun liggen. Geen twee van deze
componentmappen mogen gelijk zijn of als ouder/kind overlappen. Een symlink,
`..`, niet-genormaliseerd aliaspad of verborgen mapnaam om deze scheiding te
omzeilen is ongeldig.

Maak onder de canonieke `custom_components`-map nooit een predeploykopie,
snapshotmap, stagingmap of quarantine-map. Ook een verborgen sibling zoals
`.storm_tracker_v3_predeploy_*` is verboden: Home Assistant scant die map nog
steeds en kan haar tweede `manifest.json` als een tweede custom integration
behandelen.

Onmiddellijk vóór iedere Home Assistant-herstart moet een volledige inventaris
van manifests onder de canonieke `custom_components`-map worden gemaakt. Lees
hun manifestdomein en eis dat de domeingefilterde set voor `storm_tracker_v3`
exact één Storm Tracker-manifest oplevert:

```text
/homeassistant/custom_components/storm_tracker_v3/manifest.json
```

Nul Storm Tracker-manifests, een tweede Storm Tracker-manifest, of hetzelfde
manifest via een ander canoniek pad is een stopconditie. Manifests van andere
custom integrations blijven toegestaan en tellen niet mee in deze
domeingefilterde set. Houd de config-entry uitgeschakeld, verplaats de exact
geïdentificeerde niet-actieve componentmap naar de unieke deployrun en voer de
inventaris opnieuw uit. Herstart niet zolang deze poort niet groen is.

## Verplichte predeployvolgorde op Home Assistant

Volg daarnaast altijd
`docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`. Voor deze integration-only route zijn
de volgende stappen niet onderhandelbaar en moeten ze vóór de eerste
doelmutatie aantoonbaar voltooid zijn:

1. Leg de bedoelde integratieversie, bestandsnaam en lokale SHA-256 vast.
2. Upload uitsluitend de integratiezip naar
   `/homeassistant/storm_tracker_deploy` en vergelijk daar opnieuw de SHA-256.
3. Maak een volledige Home Assistant-back-up. Noteer de back-upidentiteit en
   controleer dat Home Assistant de back-up als voltooid ziet.
4. Ontdek read-only het werkelijk actieve componentpad en de bijbehorende
   `custom_components`-map. Leg hun canonieke absolute paden vast. Neem geen pad
   aan op basis van alleen domeinnaam of uploadlocatie.
5. Maak in de deploymap een gedateerde snapshot van de werkelijk actieve
   `storm_tracker_v3`-component en leg de SHA-256 van die snapshot vast. Deze
   snapshot is forensisch bewijs en een bron voor bestandsinspectie; na de
   schema-v3-migratie is het geen functionele config-entryrollback. Een
   uitgepakte snapshot- of predeploy-componentmap staat eveneens onder de
   unieke deployrun, nooit als sibling onder `custom_components`.
6. Leg de actuele config-entryversie en status vast en schakel de Storm
   Tracker-config-entry uit voordat componentbestanden worden vervangen.
7. Pak de kandidaat uit in een nieuwe, unieke stagingmap strikt onder de
   deployrun en buiten `custom_components`. Controleer daar de topmap
   `storm_tracker_v3`, `manifest.json`, versie en vereiste bestanden.
8. Controleer canonieke niet-overlap van actief pad, predeploy/snapshot,
   staging en quarantine. Verplaats het oude actieve componentpad rechtstreeks
   naar de vooraf vastgelegde predeploymap onder de deployrun; laat geen
   tijdelijke of verborgen sibling onder `custom_components` achter. Activeer
   daarna uitsluitend de gevalideerde staged map op het ontdekte actieve pad.
   Installeer of wijzig geen workerapp en wijzig geen database.
9. Inventariseer alle manifests onder de canonieke `custom_components`-map,
   filter op manifestdomein `storm_tracker_v3` en eis in die set exact
   `storm_tracker_v3/manifest.json`. Herstart Home Assistant pas daarna, eerst
   met de Storm Tracker-entry nog uitgeschakeld. Bevestig dat Home Assistant en
   overige integraties normaal starten.
10. Schakel de entry gecontroleerd in. Bevestig daarna config-entry schema v3,
    `runtime_mode=worker_only`, een geladen integratie en de bedoelde sensoren.
11. Zonder worker/MQTT-snapshot mogen de waarden alleen `offline`, stale,
    unknown of `onvoldoende_data` zijn. Stop wanneer een actuele
    meteorologische claim zonder gevalideerde workersnapshot verschijnt.

De integratiezip mag nooit rechtstreeks over de actieve map worden uitgepakt.
Een fout in uploadhash, staging, manifest of versie beëindigt de procedure vóór
de actieve component wordt aangeraakt.

## Quarantine en exact same-version recovery

Bij een startuptraceback, een entry die niet laadt, onverwachte compute in Home
Assistant of een onveilige actuele weerclaim:

1. schakel de Storm Tracker-entry uit en laat ze uitgeschakeld;
2. herstart Home Assistant indien nodig om de rest van het systeem gezond te
   houden, maar pas nadat de domeingefilterde één-manifestpoort groen is;
3. verplaats uitsluitend de zojuist geïnstalleerde, exact geïdentificeerde
   componentmap naar een unieke quarantine-map onder dezelfde deployrun en
   buiten `custom_components`;
4. materialiseer opnieuw
   `storm_tracker_v3_v<dezelfde-versie>_deploy.zip` met exact dezelfde vooraf
   vastgelegde SHA-256 in een nieuwe stagingmap;
5. herstel diezelfde versie naar het ontdekte componentpad, bevestig opnieuw
   dat onder `custom_components` exact alleen het actieve Storm
   Tracker-manifest staat en herstart met de entry nog uitgeschakeld;
6. schakel de entry pas opnieuw in na een schone basiscontrole.

Dit herstelpad heet exact `exact_same_version_integration_recovery`. Het is
bedoeld voor een onvolledige of corrupte bestandsoverdracht; het beweert niet
dat dezelfde code een inhoudelijke fout kan oplossen.

## Wat uitdrukkelijk geen functionele rollback is

- iedere `1.11.x`-integratiezip;
- integratie `1.13.1`;
- alleen de predeploy-componentsnapshot terugzetten nadat de entry al naar
  schema v3 is gemigreerd.

Die bestanden kunnen de gemigreerde config-entry en Home Assistant-storage
niet aantoonbaar als één coherent geheel terugbrengen. Presenteer ze daarom
nooit als functionele rollback.

Als exact same-version recovery de entry niet veilig laat laden, blijft de
entry uitgeschakeld. Terugkeer naar de toestand vóór de schema-v3-migratie
vereist herstel van de volledige, vóór de deploy gemaakte Home
Assistant-back-up. Voer zo'n volledige restore alleen als afzonderlijke live
recovery uit en controleer daarna opnieuw Home Assistant, de config-entry,
databases en historische data.

## Acceptatie voor de tijdelijke gewone-HA-opstelling

De integration-only deploy is technisch geslaagd wanneer:

- de bedoelde integratieversie uit exact de geverifieerde zip actief is;
- Home Assistant normaal draait en de config-entry schema v3 `worker_only` is;
- de entry en sensoren laden zonder legacy compute;
- zonder worker de workerstatus eerlijk `offline` blijft;
- weerfeiten zonder actuele workersnapshot `onvoldoende_data` blijven;
- er geen nieuwe HA-, MQTT- of Zigbee2MQTT-latency ontstaat;
- de volledige HA-back-up en actieve-componentsnapshot met hashes bewaard zijn;
- snapshot-, staging- en quarantine-componentmappen buiten
  `custom_components` onder de unieke deployrun staan;
- vóór iedere herstart exact één Storm Tracker-manifest onder
  `custom_components` is aangetroffen.

Dit is een bruikbare integratie-installatie en herstelbare deploybasis. Het is
niet hetzelfde als een volledig operationele Storm Tracker-productieomgeving:
actuele meteorologische output vereist later een bereikbare, geaccepteerde
worker en de toepasselijke live/meteo-admission gates.
