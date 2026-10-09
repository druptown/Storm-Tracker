# Historisch ontwerp: standalone worker op een Synology DS1618+

Status: uitgesteld tot na de functionele productafronding en niet de actieve
deployroute. Storm Tracker draait nu als HAOS-App op de NAS-Home Assistant-VM
`192.168.0.15`. Gebruik voor iedere live installatie uitsluitend
`docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md` en
`scripts/install_ha_release_pair_ssh.py`. De onderstaande Compose-informatie is
alleen bewaard als eventueel toekomstig standalone ontwerp. Start deze
migratie pas nadat de actuele roadmap alle provider-, decoder-, kalibratie-,
voorspellings-, waarschuwings-, notificatie- en duurtestgates als aantoonbaar
afgerond vermeldt. De eerdere invoering van een afzonderlijke
PostgreSQL/PostGIS-databasecontainer is uitdrukkelijk geen toestemming om ook
de worker te verplaatsen.

De lokale workerverpakking ondersteunt inmiddels twee expliciete startmodi:
zonder `STV3_MQTT_HOST` leest `run.sh` uitsluitend Home Assistant
Supervisor-services en add-onopties; met `STV3_MQTT_HOST` gebruikt hij
uitsluitend de standalone environment. `compose.yaml` geeft dezelfde
worker-only providercredentialautoriteiten door, mount een duurzame hostmap op
`/share/storm_tracker_worker`, houdt de numerieke threadgrens vast op twee en
reserveert standaard 1,5 GiB containergeheugen. Compose kiest nu standaard
`worker_only`, niet `legacy`. Wanneer alle drie de handmatige pins leeg zijn,
maakt `run.sh` ze idempotent aan onder
`/share/storm_tracker_worker/runtime/worker_only_production_v1`. Een gedeeltelijk
ingevulde pinset blijft fail-closed ongeldig. De voorbeeldwaarden staan in
`.env.example`. Dit is packagingbewijs, nog geen geteste DS1618+-installatie.

De container heeft daarnaast vier onafhankelijke lage-prioriteitsgrenzen:

- `nice 15` en twee numerieke threads;
- een interne fail-closed CPU-limiter van standaard 40 procent;
- Compose `cpus=2.0` als bovengrens en `cpu_shares=128` als lage relatieve
  prioriteit bij contention;
- 1,5 GiB geheugen, maximaal 128 processen en 35 seconden gecontroleerde stop.

Deze grenzen gelden alleen voor de Storm-worker. Ze wijzigen geen Home
Assistant- of Zigbee2MQTT-instellingen op de NUC.

## Korte conclusie

Ja: een DS1618+ met 8 GB kan de Storm Tracker-worker en de duurzame raw-,
receipt- en replayopslag veel beter isoleren van Home Assistant dan de huidige
NUC-opstelling. Home Assistant en Zigbee2MQTT blijven daarbij op de NUC en doen
alleen de lichte desired-state- en snapshotprojectie. De NAS haalt, decodeert,
trackt en archiveert; MQTT is de enige operationele grens tussen beide.

De DS1618+ is echter geen snelle science-server. Hij heeft een Intel Atom C3538
met vier cores op 2,1 GHz en is een model uit 2018. Acht GB is ruim genoeg voor
de seriële provider-captures en de bewezen DWD-processen, maar zware challengers
zoals v5 mogen niet tegelijk of zonder harde CPU/RSS/wall-limieten draaien. Voor
nieuwe aankoop verdient een recenter x86-64 Plus-model met sterkere CPU en
langere productondersteuning de voorkeur wanneer de prijs niet veel hoger is.

Officiële bronnen:

- [DS1618+ productspecificatie](https://global.download.synology.com/download/Document/Hardware/ProductSpec/DiskStation/18-year/DS1618%2B/enu/Product_Spec_DS1618%2B_enu.pdf)
- [DS1618+ download- en DSM-status](https://www.synology.com/en-global/support/download/DS1618%2B)
- [Synology product support status](https://www.synology.com/en-global/products/status?status=all)
- [Container Manager-versiecompatibiliteit](https://kb.synology.com/es-mx/DSM/tutorial/Why_is_Container_Manager_1630_not_available_on_my_Synology_NAS)

Synology vermeldt momenteel volledige DSM-updates maar beperkte technische
support voor dit stopgezette model. De DS1618+ staat bovendien bij de modellen
die de nieuwste Container Manager-build niet krijgen; gebruik uitsluitend de
laatste expliciet compatibele packageversie.

## Beoogde scheiding

```text
NUC
  Home Assistant + Zigbee2MQTT
    - targets/settings → retained desired-state via MQTT
    - workerstatus + operationele snapshot → lichte sensoren
    - geen provider-HTTP, decode, tracking, kalibratie of worker-SQLite

LAN/MQTT QoS 1
  - afzonderlijke credentials en ACL per producer/consumer
  - retained desired-state, workerstatus en operationele snapshot
  - geen raw rasterbytes of volledige authorities

DS1618+
  Storm Tracker worker-container
    - één seriële acquisitionlane
    - harde child-process CPU/RSS/wall/outputgrenzen
    - provider raw store, receipts, capacity ledger en sealed archives
    - science/ground alleen na admission; anders shadow/withheld
```

## Minimale NAS-indeling

Gebruik lokale NAS-volumes, geen SQLite-database op een vanuit de NUC gemounte
SMB-share. Een praktische indeling is:

```text
/volume1/storm_tracker/config
/volume1/storm_tracker/state
/volume1/storm_tracker/raw
/volume1/storm_tracker/archive
/volume1/storm_tracker/evidence
```

Voor de huidige Compose-layout wijst `STV3_DATA_DIR` naar de parentmap die als
`/share/storm_tracker_worker` wordt gemount. De applicatiestores blijven daar
elk in hun eigen begrensde submap; credentials horen uitsluitend in de lokale
Compose-environment/secretvoorziening en niet in die datamap.
Alleen de na OAuth-rotatie noodzakelijke Netatmo-refreshstate is een duurzame
uitzondering: `STV3_NETATMO_TOKEN_STATE` wijst standaard naar de private
mode-0600-submap van dit volume. Ook `STV3_JOB_RECEIPT_DB` wijst in standalone
modus naar de duurzame `state`-submap, zodat een containerrecreatie de
idempotencyreceipts niet stil wist. In Home Assistant-modus blijven beide
defaults in de door Supervisor beheerde `/data`-map.

- `config`: read-only containerconfig en gepinde authorityprofielen;
- `state`: SQLite/WAL-stores op het lokale Btrfs-volume;
- `raw`: content-addressed upstreambytes en staging;
- `archive`: sealed segmenten en manifests;
- `evidence`: replay-/soakrapporten en releasehashes.

## Historische standalone Compose-route (niet actief)

Gebruik deze route niet voor de huidige NAS-HA-installatie. Ze geldt alleen als
later expliciet wordt besloten de worker buiten HAOS rechtstreeks onder DSM
Container Manager te draaien. De NUC blijft ook dan geen deploy- of
fallbackdoel.

1. Kopieer de releaseverpakking `storm_tracker_worker` naar een eigen map op de
   NAS, bijvoorbeeld `/volume1/docker/storm-tracker-worker`.
2. Kopieer daar `.env.example` naar `.env`. Vul minstens de echte
   `STV3_MQTT_HOST`, het aparte workeraccount en wachtwoord in. Laat
   `STV3_RUNTIME_MODE=worker_only` staan. Zet `STV3_DATA_DIR` op de lokale
   Btrfs-map `/volume1/storm_tracker`; gebruik geen SMB-pad.
3. Laat de drie `STV3_WORKER_FIRST_*`-pins alle drie leeg voor de eerste start.
   De bootstrap maakt één samenhangende pinset in de gemounte runtime-map. Vul
   nooit slechts één of twee pins in.
4. Vul providercredentials alleen in `.env` of de lokale secretvoorziening van
   Container Manager in. Zet geen secrets in Compose, MQTT, screenshots of
   logs. Retention-authorities blijven leeg tenzij de bijbehorende voorwaarden
   expliciet zijn aanvaard.
5. Controleer vóór start vanuit de verpakkingsmap:

   ```text
   docker compose config
   docker compose build --pull
   ```

   De gerenderde configuratie moet `worker_only`, de ene mount naar
   `/share/storm_tracker_worker`, `cpu_shares: 128`, `pids_limit: 128`, de
   ingestelde geheugenlimiet en geen onverwachte hostpoorten tonen. Deel de
   uitvoer niet wanneer Container Manager daarin ingevulde secrets weergeeft.
6. Start pas daarna:

   ```text
   docker compose up -d
   docker compose logs --tail 100 storm-tracker-worker
   ```

   De startlog moet de begrensde thread-/CPU-instelling en een geldige
   worker-CPU-set tonen. Een ontbrekende limiter, onveilige opslagroot,
   gedeeltelijke pinset of bootstrapdrift moet de container stoppen.
7. Controleer via MQTT eerst alleen de workerstatus, exact 29 bronposities en de
   gewenste-state-SHA. Operationele resultaten blijven tijdens de migratie een
   canary; HA/Z2M-latentie en de NUC-belasting mogen niet veranderen.

Rollback is eveneens één route:

```text
docker compose stop storm-tracker-worker
```

Controleer vervolgens dat de MQTT-LWT de worker offline zet en HA geen oude
snapshot als actueel presenteert. Verwijder bij rollback geen NAS-data, pins,
SQLite-bestanden of sealed archives. Herstart niet de oude zware HA-worker als
automatische fallback.

Btrfs-snapshots zijn nuttig voor herstel, maar vervangen de applicatie-eigen
receipts, fsyncs, archiveanchors en offsite-back-up niet. Gebruik bij voorkeur
een UPS; een snapshot na een stroomonderbreking bewijst geen voltooide
cross-store transactie.

## Resourcebeleid voor 8 GB

Start conservatief:

- exact één acquisition/science child tegelijk;
- twee workerthreads maximaal; geen BLAS/OpenMP-threadexplosie;
- 512 MiB standaard childgrens voor native captures;
- 1 GiB alleen voor de reeds gekalibreerde DWD-ROI/replayklasse;
- minimaal 2 GiB vrij laten voor DSM, filesystemcache en packagebeheer;
- v5 en andere zware challengers uitsluitend handmatig/offline;
- temperatuur, CPU-throttling, RSS, swap en disk-latentie als harde soak-SLO's.

De bestaande testhostmetingen tonen ongeveer 224 MiB echte peak RSS voor de
DWD-decoder en ongeveer 277 MiB voor de 2-km sciencecalibratie, maar dat is geen
bewijs voor deze Atom-CPU of dit NAS-filesystem. De caps worden dus niet uit die
metingen naar beneden bijgesteld vóór een echte NAS-soak.

## Netwerk- en veiligheidsvoorwaarden

1. Geef de NAS-worker een vaste IP/DNS-naam en eigen MQTT-account.
2. ACL: worker mag desired-state lezen en uitsluitend zijn eigen status,
   receipts en snapshots publiceren; HA mag geen raw-storetopics schrijven.
3. Gebruik TLS wanneer MQTT buiten een volledig vertrouwd lokaal segment komt.
4. Expose geen worker-HTTP-poort naar internet; providers worden outbound-only.
5. Bewaar providercredentials uitsluitend als containersecrets/environment op
   de NAS en nooit in MQTT, desired-state, logs of Home Assistant-attributen.
6. Maak offsiteback-ups van config, authorityroots en sealed archives; raw data
   alleen conform de providerrechten.

## Verplichte migratieproef

De NAS wordt pas operationeel na deze volgorde:

1. lege worker installeren met alle providers en publicatie uitgeschakeld;
2. storeprofielen, filesystem/fsync en MQTT-PUBACK testen;
3. bestaande sealed fixtures read-only replayen en hashes vergelijken;
4. 24 uur shadowcapture zonder HA-feiten;
5. 48–72 uur gecombineerde soak met NUC-HA/Z2M-latentie en NAS-resources;
6. stroom-/containerkill-, MQTT-disconnect-, opslagvol- en restartproeven;
7. alleen admitted facts naar een canarytracker publiceren;
8. na expliciete eindvalidatie gefaseerd omschakelen, met geteste rollback naar
   de uitgeschakelde worker en niet naar het zware HA-legacypad.

## Koopadvies

De DS1618+ met 8 GB is bruikbaar als goedkope, gescheiden collector/worker en
ruime evidence-opslag. Koop hem niet uitsluitend voor zware realtime
multi-hypothesis-science: daarvoor is de C3538 de beperkende factor. Wanneer een
recente Plus-NAS met sterkere x86-64 CPU, uitbreidbaar geheugen en volledige
Container Manager-support binnen het budget past, is die voor Storm Tracker de
duurzamere keuze.
