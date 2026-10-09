# Home Assistant deploy- en herstelprocedure

## Status

Deze procedure is de vaste werkwijze voor de actuele NAS-installatie. Gebruik
ze voor iedere volgende worker- of integratiedeploy, tenzij een aantoonbare
wijziging in Home Assistant/Supervisor ze technisch onmogelijk maakt. Stel in
dat geval eerst read-only vast wat veranderd is; gok niet met paden, slugs of
archiefformaten.

De meest recente technisch live gecontroleerde release is integratie
`1.14.120` met worker `1.18.8`. Het uitvoeringsbewijs, de hashes en de live
nacontrole staan in `DEPLOY_2026-09-20_1.14.120_1.18.8.md`. Bij deze
Core-versie kan het
inschakelen van de config-entry een expliciete Core-herstart vereisen. Wacht
na die ene herstart tot de opstartmelding verdwenen is en controleer vervolgens
de geladen config-entry en beide live versies; stuur tijdens het stoppen
ontstane `CancelledError`-regels niet aan als een nieuwe setupfout.

De ingebedde MCP-server kan na een Core-herstart circa 35 seconden
`Initialiseren` tonen en daarna zonder handmatige reload online komen. Wacht
die korte opstartperiode af voordat je de config-entry zelf herlaadt.

Reken op de huidige corpusgrootte niet met een vaste auditduur. Worker
`1.17.125` herstelt ruim 2700 captures vóór de eerste volledige providerbatch.
`started`
in Supervisor is geen admissionbewijs: wacht op een actuele workerheartbeat
met `job_admission_open=true`. Breek de audit niet af en start de worker niet
herhaaldelijk opnieuw.

## Actieve NAS-route vanaf 30 augustus 2026

De enige actieve Storm Tracker-doelhost is NAS-Home Assistant
`192.168.0.15`. De NUC op `192.168.0.144` is geen ontwikkel-, test-, deploy- of
fallbackdoel meer en wordt niet gewijzigd.

Bewezen beheerroute:

- broninstallatie: `scripts/install_ha_release_pair_ssh.py`;
- de standaardhost en -gebruiker in zowel het paar- als integratie-only-script
  zijn vastgezet op `192.168.0.15` en `hassio`; geef ze bij handmatige runs
  desondanks expliciet mee en controleer de geprinte doelhost vóór bevestiging;
- SSH-gebruiker: `hassio`;
- voor NAS-host `192.168.0.15` forceert het installatiescript AES-GCM en
  schakelt het de CTR/CBC-ciphers alleen client-side uit. Dit omzeilt de op
  deze VM waargenomen `Corrupted MAC on input`-transportfout zonder de
  SSH-server, firewall of beveiligingsinstellingen te wijzigen;
- bestaande sleutel:
  `C:\Users\drupt\.ssh\codex_nas_ha_deploy`;
- verwachte fingerprint:
  `SHA256:fmR9WQr3izw9zmvmUtT+wHSOiOGX23P7zTGYx1cxA/w`;
- vaste OpenSSH-MAC voor deze NAS-route:
  `hmac-sha2-256-etm@openssh.com`;
- integratiepad: `/homeassistant/custom_components/storm_tracker_v3`;
- workerbron: `/addons/storm_tracker_worker`;
- App-slug: `local_storm_tracker_worker`;
- alle HA-, integratie-, App- en Supervisorbediening: MCP-server
  `home_assistant_NAS`.

De MAC-pin is toegevoegd nadat de standaardonderhandeling herhaaldelijk
`Corrupted MAC on input` gaf, terwijl exact dezelfde read-only SSH-controle met
`-o MACs=hmac-sha2-256-etm@openssh.com` direct slaagde. Pas deze optie daarom
toe op losse Windows-OpenSSH-diagnostiek. De broninstallatie gebruikt Paramiko
via het vaste script en heeft geen OpenSSH-optie nodig. Herstart geen Home
Assistant- of SSH-onderdeel en roteer geen sleutel om deze transportfout te
omzeilen.

De NAS-SSH-App biedt wel de benodigde bestandstoegang maar geen Docker-socket
en geen Supervisor-token. Gebruik daarom op de NAS **niet** rechtstreeks
`ha store reload` en ook niet de historische `docker exec`-route. Vernieuw de
Store via MCP-tool `ha_call_service` met:

```json
{
  "ws_command": "supervisor/api",
  "data": {"endpoint": "/store/reload", "method": "post", "timeout": null}
}
```

Daarna moet `version_latest` exact de nieuwe workerversie tonen vóór de
officiële App-actie `update` wordt uitgevoerd.

Actuele bewezen toestand na release `1.14.121 / 1.18.9`:

- Home Assistant Core `2026.9.3`, status `RUNNING`;
- integratie `1.14.121`, status `loaded`;
- worker `1.18.9`, status `started`, MQTT-status `online`;
- `compute_profile: dedicated_nas_fast`;
- `cpu_limit_percent: 200`, `worker_nice: 0`, `cpu_threads: 2`;
- dunne operationele cyclus met 847 observaties, `postgis=current` en geen
  cyclusfout;
- stormsensor exact 39, `count_valid=true`;
- kaartpayload 47 features / 30.129 bytes;
- release-evidence:
  `.analysis/release-1.14.121-1.18.9-20260920/storm_tracker_release_v1.14.121_worker_v1.18.9_evidence.json`;
- integratie-SHA-256
  `66f4bbde15247e7a140b0ba3af9e69ec403f8aa6d2dee13895a89d3cac5ffa6b`;
- worker-SHA-256
  `e21ceb4cca203fd230d613460f41ecb75ef9918c98f5078fdf9a723154a07233`;
- evidence-SHA-256
  `e9b3fcf6d09b224615e7e62f0acd6635c572d2c807fdf075d230085cb7164e61`;
- herstelbackup vóór de paarupdate: `0cb07fe8`.

Kandidaat `1.14.76 / 1.17.80` (live nacontrole 5 september afgekeurd):

- evidence:
  `.analysis/release-1.14.76-1.17.80-20260905-a/storm_tracker_release_v1.14.76_worker_v1.17.80_evidence.json`;
- integratie-SHA-256
  `bcef961f64fd5b5f78a40b4153ffed4ccf8efeaf9c368f610ffdb5947f110c55`;
- worker-SHA-256
  `464ea7c4c6491f3287ab2328821d7a862e017f5d3b6d790e3f3c628601c6086d`;
- content-addressed evidence-SHA-256
  `df5538b6bb00279ba7e707c15391b779041a0ebbf76826c8507e7ea5f9eedbce`;
- releasepoorten: 455 geslaagd/8 platformskips, 41 geslaagd/1 skip voor
  raw-release/attestatie, 171 runtimechecks en 7 payloadgrenschecks;
- Op 5 september kwam NAS-MCP terug met Core `2026.9.0`. De kandidaat werd
  geïnstalleerd, maar de nieuwe worker leverde binnen drie minuten geen
  heartbeat. Rollback naar `1.14.75 / 1.17.79` is gestart. Lees de actuele
  nacontrole in `DEPLOY_2026-09-05_1.14.76_1.17.80.md`.
- De marker `deployment_performed=false` blijft onveranderd onderdeel van het
  oorspronkelijke lokale build-evidence; gebruik hem niet als live status.

Live nacontrole bewees bovendien queue `0`, failed jobs `0`, acht gevolgde
targets en een behouden/groeiende raw-kalibratie-index. Die bevat op het
laatste controlepunt minstens 149 exacte raw snapshots, verdeeld over 11
bron-ID's en 9 providers. Integratie `1.14.53` toont
naast de actuele verwerkingsstatus de exact begrensde, payloadvrije
raw-inventaris. De volledige providerpayloads blijven uitsluitend in de
workeropslag en worden niet als zware HA-sensorattributen gepubliceerd.

PostgreSQL 17.5 met PostGIS 3.5.2 draait afzonderlijk in de NAS-container
`storm-tracker-postgis`. De worker schrijft met de beperkte rol `storm_ingest`
alleen de payloadvrije projectie `stv3_ingest.raw_captures`; de lokale
raw-artifactstore en duurzame SQLite-outbox blijven de gezaghebbende
commit-/herstelbuffer. Live zijn 149 rijen door de worker als exact geprojecteerd
bevestigd met projectiestatus `current`, een lege outbox en nul mislukte
workerjobs. Een fout in worker `1.17.50` bewees dat database-uitval de worker
niet blokkeert en de outbox behoudt; worker `1.17.51` herstelde de
timestampbinding en werkte de achterstand automatisch weg.

Worker `1.17.53` behoudt die snapshotreparatie en replayt op de eerste normale
tick na een herstart ook de behouden canonieke grondcaptures. De publieke
KMI-AWS-decoder gebruikt uitsluitend de officiële `PRECIP_QUANTITY` in
millimeter over de voorafgaande tien minuten. Live snapshot `577` is
`current`, bevat alle 29 bronposities en geen enkele
`provider_snapshot_missing`; KMI staat waarheidsgetrouw `stale` zonder
decoderfout. De KMI-/grond-/rawset telde 50 geslaagde tests, de gerichte
regressieset 40 en de brede worker-/snapshot-/PostGIS-suite 357. De
release-/configsuite telde 483 geslaagde tests, 8 platformskips en één daarna
herstelde PostGIS-contractfout; de 10 gerichte releasechecks zijn na die
correctie groen uitgevoerd, maar de volledige suite is niet nogmaals integraal
gedraaid. Het releasepaar gebruikt integratiehash
`592a415431cb2f194584ff77e6981cafb679c2a2492a6243fd5d1594c781ebef`,
workerhash
`aa10816bbda73c785b4209bc01e6d225845a838c331ae18333e52c89d3caca5b`
en evidencehash
`f1aa9681bee4ab96a87bef8b3464620e911ce863a4675108ca26b774069b77f1`.

Release `1.14.49 / 1.17.48` corrigeert de publieke KMI-WCS-opvraag: de worker
laat GeoServer zelf de nieuwste/default-tijd selecteren en stuurt geen
ongeldige tijdsubset meer. Een echte, niet-gemockte predeploycapture leverde
exact 4.194.720 bytes `image/tiff` en werd volledig in de raw-store gecommit.
De seriële live scheduler moet de eerste postdeploy KMI-capture nog terminal
uitvoeren; stel `awaiting_first_capture` daarom niet gelijk aan een geslaagde
live bron.

De geografische nacontrole verplaatste alleen de beheerde fictieve tracker van
Manchester naar Chicago. Na de gebufferde desired-statewissel werden Synoptic
en beide GOES-18-bronnen toepasselijk, terwijl AEMET en DPC expliciet
`target_outside_catalog_scope` bleven. De historische Met Office-capture bleef
zichtbaar maar haar captureaantal en ontvangstmoment veranderden niet. Na de
test is de retained MQTT-positie exact naar Manchester hersteld. Gebruik bij
volgende bronnentests dezelfde scheiding tussen historische database-inhoud en
nieuwe acquisities onder de actuele locatie-authority.

De rest van dit document bevat nog historische NUC-recovery-informatie. Die
is alleen naslag en verleent geen toestemming om `192.168.0.144` te benaderen.

## Historische NUC-restorepoort van 30 augustus 2026

Na gelijktijdige storingen in meerdere HA-onderdelen is de volledige backup van
25 augustus teruggezet. Read-only controle na die restore bevestigde:

- integratie `1.14.13`;
- geïnstalleerde worker `1.17.13`, status `stopped`;
- Home Assistant actief zonder containerherstart;
- Zigbee2MQTT `healthy` zonder containerherstart.

De precieze oorzaak van de eerdere storingen is na de restore niet bewezen.
Daarom is de productiepoort gesloten: een lokaal geslaagde build, dry-run of
testreeks is op zichzelf geen toestemming om de NUC te wijzigen. Nieuwe
kandidaten mogen lokaal worden gebouwd en verzegeld, maar blijven
`deployment_performed=false` en `real_installation_ready=false` totdat een
nieuwe expliciete go/no-go is uitgevoerd. De NAS is het voorkeursdoel voor de
uiteindelijke zware worker.

Wanneer toch opnieuw op de gedeelde NUC wordt getest, geldt aanvullend deze
canaryvolgorde:

1. maak eerst een nieuwe volledige HA-backup en bevestig read-only dat HA en
   Z2M stabiel zijn;
2. installeer alleen terwijl integratie en worker gestopt zijn;
3. start alleen de worker, met de integratie nog uitgeschakeld;
4. bewijs onmiddellijk de harde limiet van 25%, `nice 15`, maximaal twee
   rekenthreads en de vrije fysieke HA-core; iedere afwijking stopt de worker;
5. controleer HA en Z2M minimaal iedere 60 seconden. Z2M `unhealthy`, een
   herstart, een niet-responsieve HA-API of merkbare Z2M-latency stopt uitsluitend
   de Storm-worker; HA en Z2M worden niet vertraagd of begrensd;
6. wacht zonder boost tot job admission open is en laat daarna minstens één
   volledige provider-/herberekeningscyclus plus 30 minuten stabiele werking
   verlopen;
7. schakel de integratie pas daarna in en bewijs dat snapshots langer dan één
   volledige vijfminuten-TTL `current` blijven.

Zonder dit bewijs blijft de NUC-deploy een `no-go`; er wordt dan verder gebouwd
en getest buiten Home Assistant.

## Bevestigde worker-only route op de huidige HA-host (25 augustus 2026)

Voor de huidige installatie op `192.168.0.144` is de volgende identiteit
read-only en live bevestigd:

- SSH-gebruiker: `wim` (niet root, lid van `wheel`);
- wachtwoordloze beheersprong: `sudo -n`;
- actieve integratie: `/homeassistant/custom_components/storm_tracker_v3`;
- actieve workerbron: `/addons/storm_tracker_worker`;
- beide actieve mappen en `/backup/storm_tracker_deploy_runs` zijn
  `root:root` en mogen dus **niet rechtstreeks als `wim` worden beschreven**;
- Supervisor/App-slug: `local_storm_tracker_worker`.

De vaste uitvoerbare route is vastgelegd in:

```text
scripts/install_ha_release_pair_ssh.py
```

Dit script verifieert eerst het content-addressed evidencebestand en beide
archieven, test `sudo -n`, controleert de canonieke actieve paden en de
één-manifestpoort, maakt een unieke run onder
`/backup/storm_tracker_deploy_runs`, uploadt met `sudo -n tee`, staged en
valideert daar, bewaart beide actieve bronmappen en vervangt pas daarna de
actieve bron. Tot slot herstelt en controleert het expliciet eigenaar
`root:root`.

Gebruik op deze host dus nooit eerst een gewone `mkdir`, `cp`, `rm`, `cat >`
of uitpakactie als `wim` onder `/homeassistant` of `/addons`. Als
`sudo -n true` faalt, is dat een stopconditie; probeer dan geen alternatieve
schrijfroute.

De volledige volgorde voor een gekoppelde integratie- en workerdeploy is:

1. bouw de kandidaat met `scripts/build_release_candidate.py` en laat de
   gerichte en release-installatietests slagen;
2. lees live status/versies, controleer HA-config, bevestig dat een recente
   automatische volledige HA-backup bestaat en maak daarnaast één gerichte
   deploybackup volgens de backupregels hieronder;
3. schakel de Storm Tracker-config-entry via de officiële HA-API uit en stop
   App `local_storm_tracker_worker` via de Supervisor/HA-API;
4. voer exact dit installatiescript uit met het zojuist gebouwde evidence:

   ```powershell
   $env:STV3_SSH_KEY = '<bestaande-beheersleutel>'
   python scripts/install_ha_release_pair_ssh.py `
     --evidence '<release-map>\storm_tracker_release_v<integratie>_worker_v<worker>_evidence.json' `
     --confirm-components-stopped `
     --apply
   ```

5. vernieuw de lokale Store-metadata via de reeds bewezen non-interactieve
   route en controleer daarna dat Supervisor de nieuwe lokale workerversie als
   `version_latest` toont:

   ```text
   sudo -n docker exec app_a0d7b954_ssh ha store reload --no-progress
   ```

   Voer over de externe SSH-shell niet eerst rechtstreeks
   `ha store reload --no-progress` uit: die shell heeft geen Supervisor-token
   en antwoordt daarom terecht met `unauthorized`. Alleen wanneer de vaste
   containernaam aantoonbaar gewijzigd is, wordt die eerst read-only opnieuw
   ontdekt;
6. update App `local_storm_tracker_worker` via de officiële App-API. Gebruik
   `rebuild` alleen wanneer bron en geïnstalleerde versie bewust gelijk zijn;
   bij een nieuwe versie is `update` de standaardroute;
7. controleer HA-config, herstart HA en wacht tot niet alleen poort 8123 maar
   ook de HA-API/WebSocket antwoordt;
8. start daarna de worker-App en bewijs via Supervisor plus een verse MQTT-
   heartbeat dat de nieuwe worker werkelijk online is. Wacht bovendien tot
   die status expliciet `job_admission_open=true` meldt; `state=online` alleen
   verschijnt al tijdens het herstellen van bewaarde captures en is dus geen
   voldoende control-plane readinessbewijs. Bij een integratie-only deploy
   blijft de worker draaien en wordt deze stap read-only gecontroleerd;
9. schakel pas nadat de worker online én zijn job admission open is de Storm
   Tracker-integratie in. Dit
   voorkomt dat een behouden oude `online`-status tijdens een gestopte worker
   de eerste desired-state handshake vóór de echte workerstart laat stranden;
10. bewijs live de runtimeversie via
   `sensor.stv3_worker_technical_diagnostics`; Supervisorbronmetadata alleen
   is niet voldoende;
11. controleer operational snapshot, providerstatussen, regiofail-closed,
   wachtrij/mislukte jobs, logs, databasebehoud en dashboardweergave.

Schakel de integratie dus niet preventief in terwijl de worker nog gestopt of
nog niet admission-ready is. Wanneer die volgorde toch is omgedraaid en de
worker na start wel verse heartbeats maar geen controljobs ontvangt, gebruik
dan één officiële `homeassistant.reload_config_entry` nadat de worker
`job_admission_open=true` meldt; probeer geen alternatieve MQTT- of
bestandsschrijfroute.

Het installatiescript bedient bewust geen HA-config-entry en start of herstart
geen App/HA. Die lifecycle blijft via de officiële Home Assistant-API lopen,
zodat een bronkopie nooit stilzwijgend een halfgestarte productieomgeving
oplevert.

## Bewezen uitgangstoestand

De recovery is pas als succesvol aanvaard nadat het volgende live werd
vastgesteld:

- Home Assistant was herstart en operationeel;
- Storm Tracker-integratie `1.11.0` had status `loaded`;
- Storm Tracker Worker `1.14.0` had Supervisorstatus `started`;
- `sensor.stv3_worker_status` was `online`;
- `queue_depth` was `0`;
- `failed_jobs` was `0`;
- alle geregistreerde workerprocessen waren healthy;
- de workerlog meldde maximaal twee CPU-threads;
- procesinspectie bevestigde `nice 15`;
- één fysieke core bleef worker-vrij voor Home Assistant en Zigbee2MQTT.

De gebruikte, onderling gecontroleerde artefacten waren:

- `storm_tracker_v3_v1.11.0_deploy.zip`;
- `storm_tracker_worker_v1.14.0_deploy.tar.gz`.

Vastgelegde SHA-256 van deze specifieke recoveryartefacten:

- integratie: `50bc31f665eb8896fb5e2f75cfaa072d853942671f6d1d2728eb9ba813e1e260`;
- worker: `b9f4736e056ad433e9086e47287515ce2ca5789d5f641b13d6d9abaa0b6a6b9f`.

Deze hashes bewijzen alleen exact die twee bestanden. Voor een nieuwe versie
moet altijd een nieuwe hash worden berekend en aan beide kanten vergeleken.

## Vaste naam- en locatieafspraken

| Onderdeel | Verplicht formaat | Vaste uploadlocatie |
| --- | --- | --- |
| Integratie | `storm_tracker_v3_v<versie>_deploy.zip` | `/backup/storm_tracker_deploy_runs` |
| Worker | `storm_tracker_worker_v<versie>_deploy.tar.gz` | `/backup/storm_tracker_deploy_runs` |

Een workerzip is geen geldig standaarddeployartefact. Een bestandsnaam zonder
het suffix `_deploy` is evenmin een geldig overdrachtsartefact.

## Standaardprocedure

### 1. Scope en versie vastleggen

Leg vóór iedere mutatie afzonderlijk vast:

- gewenste integratieversie;
- gewenste workerversie;
- of alleen de worker, alleen de integratie of beide worden gewijzigd;
- actuele live versies en statussen;
- verwachte rollbackversies.

Een workerdeploy impliceert geen integratiedeploy en omgekeerd. Controleer hun
protocolcompatibiliteit vóór installatie.

### 2. Artefact lokaal valideren

Controleer vóór upload:

- exacte bestandsnaam en extensie;
- SHA-256;
- dat het archief zonder fout opent;
- dat de verwachte topmap aanwezig is;
- dat de interne manifest- of configversie exact overeenkomt met de
  bestandsnaam;
- dat geen `__pycache__`, `.pyc`, testoutput, database, geheim of tijdelijke
  stagingmap is mee verpakt.

Stop bij iedere mismatch. Hernoemen van een fout opgebouwd archief maakt het
niet geldig.

De lokale kandidaatbuilder voert deze controles reproduceerbaar uit en leest
na de bouw ieder bestand opnieuw uit beide archieven:

```powershell
python scripts/build_release_candidate.py `
  --integration-root custom_components/storm_tracker_v3 `
  --worker-root storm_tracker_worker `
  --integration-version <integratieversie> `
  --worker-version <workerversie> `
  --output-directory <lege-bestaande-uitvoermap>
```

De builder weigert bestaande uitvoer en produceert exact een `_deploy.zip`,
een `_deploy.tar.gz` en een content-addressed `*_evidence.json`. Dat laatste
bestand is de lokale commit marker: zonder een geldig evidencebestand vormen
losse of gedeeltelijk gebouwde archieven geen releasekandidaat. De builder
installeert of deployt niets. De hashes moeten na upload nog steeds afzonderlijk
op Home Assistant worden vergeleken.

### 3. Upload controleren

Gebruik uitsluitend de vaste map
`/backup/storm_tracker_deploy_runs`. Controleer read-only dat het exacte
bestand daar aanwezig is en vergelijk de SHA-256 met de lokale hash.

Gebruik de bestaande, reeds werkende beheerverbinding. Voeg niet spontaan een
nieuwe SSH-sleutel toe en open geen tijdelijke HTTP-server of firewallregel.
Bij `Corrupted MAC on input` niet blijven herhalen met de standaardcipher:
gebruik de vastgelegde NAS-scripts, die voor deze host AES-GCM afdwingen.

Op de huidige root-owned host gebeurt de upload uitsluitend via het vaste
installatiescript. Dat gebruikt `sudo -n tee` naar de unieke deployrun onder de
vaste deploymap. Een gewone upload als `wim` naar die map is geen geldige
preflight en zal terecht met `Permission denied` stoppen.

### 4. Live identiteit ontdekken

Ontdek vóór installatie de daadwerkelijke Supervisor-appslug en status. Gebruik
geen afgeleide mapnaam als slug. De bij de succesvolle recovery bevestigde slug
is:

```text
local_storm_tracker_worker
```

Gebruik de actuele `ha apps`-interface. `ha addons` is verouderd en wordt niet
als standaardroute gebruikt.

Controleer ook het werkelijk actieve bron-/installatiepad vóór daar bestanden
worden vervangen. Paden zoals `/addons/storm_tracker_worker` of
`/addons/local_storm_tracker_worker` mogen nooit worden aangenomen enkel op
basis van de slug of pakketnaam.

### 5. Rollbackback-up maken

Maak vóór vervanging een gedateerde tar- of zipback-up van de werkelijk actieve
worker- of integratiemap in de deploymap. Bij de succesvolle recovery werd
bijvoorbeeld bewaard:

```text
/backup/storm_tracker_deploy_runs/storm_tracker_worker_pre_rollback_20260731.tar.gz
```

Neem geen Home Assistant-database of kalibratiedatabase op in een gewone
codebackup en maak die databases niet leeg. Databases worden alleen gewijzigd
op een afzonderlijke, expliciete opdracht met een eigen backup- en
rollbackplan.

Maak voor een gewone gekoppelde Storm Tracker-code-update geen tweede volledige
kopie van alle HA Apps, `share` en `media`. Bevestig eerst dat de normale recente
volledige systeembackup bestaat en maak daarna via de officiële Supervisor-
backup-API één gerichte deploybackup met:

- Home Assistant-configuratie inbegrepen;
- `homeassistant_exclude_database=true`, omdat de Recorder-database niet door
  deze code-update wordt gemigreerd;
- alleen App `local_storm_tracker_worker` inbegrepen;
- geen `share`, `media` of niet-geraakte Apps.

De worker-App declareert `backup_exclude` voor
`storm_tracker_worker_runtime_production_v1*`. Die map bevat uitsluitend de als
één geheel heropbouwbare worker-only runtime: raw captures, afgeleide bestanden
en hun tijdelijke verwijzende databases. Appopties en credentials onder
`/data/options.json` blijven wel in de gerichte backup. Sluit nooit losse raw
submappen uit terwijl hun runtime-indexen worden behouden; zo'n halve restore
is inconsistent.

Deze regel is toegevoegd nadat backupvergelijking op 30 augustus 2026 bewees
dat het gecomprimeerde workerdeel van 79,8 MB naar 801,8 MB was gegroeid en
daarmee het grootste deel van de gemelde extra gigabyte verklaarde. Bestaande
backups worden niet automatisch verwijderd.

Bewaar ook de uitgepakte release- en rollbackruns niet onder
`/homeassistant`: die map wordt als configuratie opnieuw in iedere backup
opgenomen. De vaste route is daarom `/backup/storm_tracker_deploy_runs`.
Deze wijziging volgde nadat de oude deploymap op 5 september 2026 al 2,0 GB
bedroeg en één nieuwe backup daardoor onnodig lang bleef comprimeren.

### 6. Installeren via de bewezen route

Gebruik dezelfde bestaande beheerverbinding en Supervisor-installatieroute als
bij de succesvolle recovery van 31 juli 2026:

Voor de huidige host betekent "de bewezen route" concreet
`scripts/install_ha_release_pair_ssh.py`; voer de onderstaande bestandsstappen
niet nogmaals met losse, geïmproviseerde SSH-commando's uit.

1. stop alleen het onderdeel dat werkelijk vervangen moet worden;
2. pak het vooraf gevalideerde artefact uit in een unieke stagingmap;
3. controleer in staging opnieuw de interne versie en vereiste bestanden;
4. vervang uitsluitend de **inhoud** van de ontdekte doelmap. De vaste installer
   streamt die inhoud met `tar -C <bron> -cf - . | tar -C <doel> -xf -`;
   gebruik hier geen BusyBox-gevoelige `cp -a <bron>/. <doel>/`, want die kan
   de volledige bronmap één niveau te diep nesten;
5. vernieuw na gewijzigde workerbestanden de lokale Store-metadata met
   `ha store reload --no-progress` in de reeds aangemelde, beschermde
   Advanced SSH & Web Terminal-App. Een `ha store reload` over de externe
   deploy-SSH-sessie is geen geldige route: daar ontbreekt de Supervisor-token.
   Gebruik op de NAS evenmin de historische `docker exec`-route en schakel
   Protection Mode niet uit;
6. controleer dat `version_latest` de bedoelde nieuwe versie is en voer daarna
   via de officiële App-API `update` uit. Alleen bij bewust ongewijzigde
   versienummers is `rebuild` passend;
7. gebruik bij workerbediening de ontdekte appslug
   `local_storm_tracker_worker`;
8. valideer vóór een vereiste Core-herstart de HA-configuratie via de officiële
   config-check;
9. herstart Home Assistant wanneer de integratie is vervangen, wacht tot Core
   én de MCP-beheerlaag opnieuw antwoorden, start daarna de worker en activeer
   de integratieconfig-entry;
10. geef worker-only recovery maximaal acht minuten om de duurzame state te
   openen, runtime-admission te bewijzen en de eerste nieuwe heartbeat te
   publiceren. De actuele 612-capturestore opende admission na circa zes
   minuten; `started` in Supervisor alleen is nog geen runtimebewijs.

Voer geen destructieve `find ... rm`, wildcardverwijdering of kopieeractie uit
wanneer bron, stagingmap of doelmap niet eerst expliciet bestaan en gevalideerd
zijn. Een mislukte extractie beëindigt de procedure vóór de actieve installatie
wordt aangeraakt.

### 7. Capaciteitsinstellingen behouden

De actieve, geïsoleerde NAS-HA-configuratie is op expliciet gebruikersbesluit
profiel `dedicated_nas_fast` met maximaal 200% totale CPU, `worker_nice: 0`,
een providerinterval van 2 seconden en maximaal twee CPU-threads. De CPU-set
houdt minstens één fysieke core buiten de worker. Deze waarden mogen niet
stilzwijgend naar de NUC worden gekopieerd en worden bij een code-update niet
automatisch gewijzigd.

**Harde NUC-regel:** op de gedeelde Home Assistant-host mag de CPU-limiter van
de Storm-worker nooit worden gepauzeerd, gestopt, omzeild of tijdelijk verhoogd
voor een snellere deploy, opstart, test of verificatie. De HA-runtime kapt een
hogere Storm-instelling af op 25%. De workerlauncher controleert de limiter
continu en stopt de worker fail-closed zodra die limiter wordt gepauzeerd of
wegvalt. Home Assistant en Zigbee2MQTT worden hierdoor niet begrensd.

Een test die niet binnen deze vaste Storm-capaciteit voltooit, duurt langer of
wordt buiten de NUC uitgevoerd. De uiteindelijke zware workerproductie hoort op
de NAS; dat is geen reden om de beveiliging op de tijdelijke NUC-host te
versoepelen.

De historische gedeelde-HA/NUC-recoveryconfiguratie is:

```yaml
cpu_threads: 2
reserve_ha_core: true
worker_nice: 15
queue_size: 16
```

De queue blijft behouden en verwerkt zware jobs gecontroleerd. Home Assistant
en Zigbee2MQTT behouden CPU-ruimte doordat de worker maximaal twee rekenthreads
gebruikt, één fysieke core vrijlaat en met lagere OS-prioriteit draait.

Wijzig deze waarden niet stilzwijgend bij een code-update. Een wijziging is een
aparte capaciteitsbeslissing en vereist daarna een latency-, queue-, CPU- en
temperatuurcontrole.

### 8. Verplichte nacontrole

Een commando met exitcode nul is geen bewijs van een geslaagde deploy. De
deploy is pas klaar nadat alle toepasselijke punten zijn bevestigd:

- Supervisor toont exact de bedoelde workerversie;
- workerstatus is `started`;
- de workerlog toont geen startuptraceback of crashloop;
- `sensor.storm_tracker_workerstatus` wordt na durable recovery `online` en
  meldt exact de bedoelde workerversie;
- `sensor.stv3_worker_technical_diagnostics` is `current`, meldt exact de
  bedoelde integratieversie en toont een geclaimde desired-state-authority
  zonder failures;
- `queue_depth` loopt terug en blijft niet onbeperkt groeien;
- `failed_jobs` stijgt niet door de nieuwe start;
- geregistreerde workerprocessen zijn healthy;
- log en procesinspectie bevestigen maximaal twee CPU-threads en het gekozen
  profiel: op de actieve geïsoleerde NAS `dedicated_nas_fast`/`nice 0`, op de
  historische gedeelde NUC uitsluitend `shared_ha`/`nice 15`;
- integratieconfig-entry is `loaded`;
- de bedoelde Storm Tracker-sensoren zijn beschikbaar;
- Home Assistant en Zigbee2MQTT vertonen geen nieuwe latency;
- databases en historische kalibratiedata zijn nog aanwezig.
- wanneer PostGIS is ingeschakeld: projectiestatus is `current`, retries en
  laatste fout blijven leeg/nul, en een onafhankelijke SQL-telling bevestigt
  dat de projectie vooruitgaat. Een kleine `ready`-staart tijdens actieve
  acquisitie is een wachtrij en geen dataverlies.

Noteer de versies, hashes en resultaten bij iedere deploy.

## Stop- en rollbackvoorwaarden

Stop de uitrol en herstel de vooraf gemaakte backup wanneer één van deze
toestanden optreedt:

- archiefversie, bestandsnaam en gewenste versie verschillen;
- uploadhash verschilt van de lokale hash;
- staging mist de verwachte config of manifest;
- Supervisor ziet de lokale app niet meer;
- de app komt in `error` of een crashloop;
- de integratie blijft `not_loaded`;
- workerstatus wordt niet online;
- failed jobs lopen snel op;
- HA- of Z2M-latency neemt merkbaar toe.

Na rollback wordt opnieuw de volledige nacontrole uitgevoerd. Verwijder een
rollbackartefact pas nadat een latere release langdurig stabiel is gebleken.

## Verboden shortcuts

- Nooit `cpulimit` pauzeren of de Storm-worker op de gedeelde NUC tijdelijk
  onbeperkt laten rekenen, ook niet voor liveverificatie.
- Niet gokken dat appslug en add-onmap dezelfde naam hebben.
- Niet uitpakken naar een stagingmap die niet bestaat.
- Niet doorgaan nadat `tar`, versiecontrole of kopiëren faalt.
- Niet vertrouwen op uitsluitend `Command completed successfully`.
- Niet een tarball vervangen door een zip of omgekeerd.
- Niet de integratie en worker als één versie behandelen.
- Niet zonder expliciete opdracht databases legen of vervangen.
- Geen geheimen, wachtwoorden of private sleutels in commando-uitvoer,
  documentatie, logs of deployartefacten opnemen.
