# Storm Tracker V3 — actuele roadmap 2026-09-01

Dit document is de actuele waarheid voor de worker-only route. Oudere roadmaps
blijven ontwerp- en beslissingshistoriek, maar mogen niet worden gebruikt om
een onderdeel als live of afgewerkt voor te stellen.

## Live platform

Actuele override 20 september: Core `2026.9.3`, integratie `1.14.109` en
worker `1.17.125` zijn live op de NAS. De worker is online, de raw-index en
PostGIS liepen bij de nacontrole gelijk op 2848 captures en de Belgische
kernbronnen KMI app-v4, OPERA, DWD, KNMI, Open-Meteo, directe Blitzortung en
METAR zijn `ready` en `operational`. Integratie 1.14.109 toont bovendien de
reeds berekende celtracking live op Monitoring; bij de nacontrole waren 1449
cellen actief en 228 bewegingsklaar. Integratie 1.14.108 voorkwam eerder dat
een semantisch identieke Life360-livenessvernieuwing een
nog geldige weersnapshot tijdens een lopende providerbatch stale maakt of met
een tijdelijke lege authority-handoff overschrijft; echte locatie- en
instellingswijzigingen blijven fail-closed. Het releasebewijs staat in
`DEPLOY_2026-09-20_1.14.109_1.17.125.md`.

Actuele override 5 september: Core `2026.9.0`, integratie `1.14.81` en worker
`1.17.85` zijn live op de NAS. De worker is online, job-admission staat open en
22/22 geaccepteerde jobs zijn zonder workerfout afgerond. De worker-first
runtime heeft nul fatale fouten. De gezaghebbende raw-store en PostGIS staan
exact gelijk op 672/672 captures; de PostGIS-outbox is leeg en de
projectiestatus is `current`. Operationele snapshot 1737 is `current`. KMI
app-v4 is voor het eerst live `operational` en heeft een exact bewaarde raw
capture. Twee kwantitatieve radar-inconsistenties werden veilig ingehouden:
`quantitative_failures` steeg naar twee, maar de runtime bleef `running` met
`failures: 0`. Onderstaande oudere meetpunten zijn historische bewijzen, geen
actuele teller.

- Het enige ontwikkel-, test- en deploydoel is NAS-Home Assistant op
  `192.168.0.15`. De NUC op `192.168.0.144` is geen fallbackdoel en wordt niet
  benaderd.
- Home Assistant Core `2026.9.0` is `RUNNING`.
- Integratie `1.14.81` en worker `1.17.85` zijn live op de NAS. De worker- en
  MQTT-heartbeat zijn online, desired-state is gesynchroniseerd en de
  acquisitiecontroller gebruikt in gezonde toestand alle werkelijk due
  workloads parallel, met een gemeten lanegrens van 26.
- De worker draait als App `local_storm_tracker_worker` in modus `worker_only`
  met profiel `dedicated_nas_fast`, een intern providerinterval van 2 seconden,
  maximaal 200% totale CPU en twee threads. De CPU-set houdt minstens één
  fysieke core vrij voor Home Assistant.
- Workerstatus is online, queue en failed jobs zijn nul. Snapshot 1737 is
  `current`; na de deploy groeide de behouden index live van 657 naar 672
  captures en projecteerde PostGIS iedere nieuwe commit mee. De bronindex
  groeide van 18 naar 19 bron-ID's doordat `radar.kmi.appv4_visual` zijn eerste
  echte capture opsloeg en zichtbaar `operational` werd.
- Worker `1.17.85` behandelt een inconsistente verzameling kwantitatieve
  radarobservaties als ingehouden verrijking. De betrokken bronclaims worden
  niet als meteorologische waarheid gepubliceerd, maar acquisitie, RAW-opslag,
  PostGIS en de rest van de worker blijven draaien. De live proef trof deze
  toestand tweemaal en bevestigde daarna runtime `running`, `failures: 0`,
  queue 0 en verder oplopende snapshots.
- Worker `1.17.63` herbindt tijdens snapshot-settle een veilig achterhaalde
  fact aan de nieuwste desired-state-authority. De live 2-secondenproef bleef
  daardoor voorbij het vroegere uitvalpunt gezond: op uptime 732 seconden
  waren 17 van 17 jobs voltooid, queue 0, runtimefouten 0 en snapshot 716
  `current`. De worker gebruikte circa 28% CPU en 320 MiB RAM; HA antwoordde
  tijdens de proef rond 0,10 seconde.
- Worker `1.17.64` herstelt daarnaast na iedere workerherstart de laatst
  bewezen terminale uitkomst per cadenceworkload uit de duurzame receipts.
  Daardoor vallen echte providerfouten niet meer terug naar het misleidende
  `awaiting_first_capture`. De live NAS-proef na herstart publiceerde snapshot
  755 `current`, hield de queue op nul en meldde 8 afgeronde jobs en 0
  mislukte workerjobs. Van de 28 bronposities bleef nog exact één bron op
  `awaiting_first_capture`; alle reeds terminal bewezen fouten behielden hun
  concrete reden.
- Worker `1.17.65` begrenst herhaald falende providerpogingen adaptief tot
  20 seconden nadat twee terminale fouten duurzaam bewezen zijn. Iedere zesde
  poging behoudt het volledige native budget, zodat een herstelde of legitiem
  trage bron opnieuw kan toetreden. Succesvolle bronnen zoals DWD behouden hun
  volledige productspecifieke budget. De live NAS-proef publiceerde snapshot
  783, schreef na de herstart nieuwe DWD- en NOAA-ERR-captures, rondde 18 jobs
  af zonder workerfout en hield PostGIS exact gelijk op 225/225 captures.
- Worker `1.17.66` reserveert in de ene seriële acquisitielane om de beurt een
  Belgische prioriteitsbeurt voor KMI-AWS, DWD RADOLAN RV en de publieke
  KMI-WCS-radar. Algemene bronnen behouden via de tussenliggende beurt hun
  fairness. De live NAS-proef herstelde eerst 240 captures, voltooide daarna de
  desired-state-handshake voor 8 doelen zonder fout en publiceerde snapshot
  810. KMI-AWS groeide live van 9 naar 10 captures en werd `operational`; de
  PostGIS-projectie liep gelijktijdig door van 240/240 naar 241/241.
- Release `1.14.70 / 1.17.74` schakelde op de geïsoleerde NAS de vijf-lane
  herstelgrens uit en liet alle werkelijk due workloads parallel starten.
  De live proef bleef responsief bij circa 27% normale worker-CPU, een korte
  piek rond 45% en ongeveer 343 MiB RAM; PostGIS bleef exact gelijk aan RAW.
- Release `1.14.71 / 1.17.75` herstelt de daarbij blootgelegde downstreamgrens:
  alle terminale resultaten uit één parallelle batch worden afzonderlijk
  geconsumeerd. Daardoor kunnen eerdere finishers niet meer uit bronstatus,
  kalibratie-index, PostGIS of productspecifieke verwerking verdwijnen doordat
  een andere provider als laatste afrondt. De live NAS-batch bewees dit met
  gelijktijdige groei van KMI-WCS 2 naar 3 en DWD 165 naar 166; RAW en PostGIS
  bleven exact gelijk op 306/306, de terminale jobteller steeg van 9 naar 14,
  queue en failures bleven nul.
- Release `1.14.72 / 1.17.76` maakt die acquisitiecontroller zichtbaar in HA:
  het dashboard toont full-parallelmodus, effectieve lanegrens, batchgrootte,
  batchduur en vaste ongezondheidsredenen zonder providerpayload of geheimen.
- Release `1.14.73 / 1.17.77` isoleert uitsluitend verwachte duurzame
  schedulerconflicten en klokconflicten per providerlane. Een dergelijke lane
  wordt veilig opnieuw aangeboden zonder dat één conflict de volledige
  worker-first runtime nog fail-closed herstart; onbekende en
  integriteitsfouten blijven wel fail-closed. De gerichte scheduler- en
  worker-onlysuite telt 89 geslaagde tests; de brede releaseset telt 574
  geslaagde tests en 8 bewuste platformskips.
- Release `1.14.74 / 1.17.78` verwijdert de oude seriële NUC-probe van twintig
  seconden uit full-parallel NAS-acquisitie. Ieder product krijgt opnieuw zijn
  volledige, productspecifieke harde I/O-deadline. KMI-WCS groeide live eerst
  van 14 naar 16 captures en daarna tot 23; onbekende batchfouten loggen alleen
  nog een gesloten categorie en exceptionklasse.
- Release `1.14.75 / 1.17.79` isoleert een tijdelijke
  `SchedulerDesiredStateError` per betrokken lane wanneer een versere
  desired-state de oude planning vervangt. De overige providerrequests en de
  worker blijven doorlopen; een inconsistente desired-state blijft bewust
  fail-closed. De gerichte en acquisitieruntimesuites tellen 76 geslaagde
  tests. De brede release-/installatieset telt 463 inhoudelijk geslaagde tests
  en 5 platformskips; één tempmapnaam met `release` activeerde uitsluitend de
  bestaande lekcontrole op de substring `lease` en slaagde direct in een
  neutraal pad.
- Kandidaat `1.14.76 / 1.17.80` verwijdert de nog verborgen globale
  subprocess-singleton uit uitsluitend de I/O-gebonden exact-raw route. De 26
  full-parallel lanes kunnen daardoor werkelijk tegelijk netwerkrequests
  uitvoeren; per provider blijft één request actief en CPU-zware decode/fusie
  blijft op twee threads. Open-Meteo stuurt bij acht targets nu ook acht
  `elevation=nan`-waarden; de exacte live request gaf HTTP 200. De kandidaat
  doorstond 171 runtimechecks, 7 payloadgrenschecks, 455 release-/installatie-
  checks met 8 platformskips en 41 raw-release-/attestatiechecks met 1 skip.
  De archieven zijn gebouwd maar niet gedeployed (`deployment_performed=false`):
  op 5 september 2026 waren NAS-HA-poorten 8123, 9584 en 22 vanaf de
  ontwikkel-pc onbereikbaar. De bereikbare NUC wordt niet als fallback gebruikt.
- Release `1.14.79 / 1.17.83` herstelt crashbestendig de raw-store. Een
  geverifieerd compleet stagingobject wordt content-addressed gepromoveerd;
  uitsluitend een bewezen privé en onvolledig scratchbestand wordt veilig
  opgeruimd. De live worker catalogiseerde één geverifieerd weesobject,
  passeerde het eerdere uitvalpunt van circa 160 seconden en opende admission
  na circa 359 seconden. Daarna waren 26/26 jobs voltooid, runtimefouten nul,
  RAW/PostGIS 612/612 en snapshot 1713 `current`. De gecombineerde lokale
  regressieset telt 179 geslaagde tests en 2 bewuste platformskips.
- Voor release `1.14.75 / 1.17.79` bestaat de beschermde predeploybackup
  `storm-tracker-pre-1.14.75-1.17.79` (`c5ab1fb8`, 635.709.440 bytes), met de
  Recorder-database uitgesloten. Het releasebewijs heeft SHA-256
  `9a7432bf2123858ec2ee57f3dceec640e0baa4678694ccbb99e54b288b1434fa`;
  de integratie- en workerarchieven hebben respectievelijk
  `fb3df22600f28369a3f41c3553bab9c550155a1a83c81cb590ea6cac9aab660f`
  en `bdb89274fb61aaa30fa72be02f38f665c58c2c75db8edaad672ea575eef5a428`.
- Voor release `1.14.73 / 1.17.77` bestaat de beschermde gerichte
  predeploybackup `storm-tracker-pre-1.14.73-1.17.77` (`155e2b52`,
  615.987.200 bytes), met de Recorder-database uitgesloten. Het content-
  addressed releasebewijs heeft SHA-256
  `aa60b5cc81e2841f8ad0c20db015bcdda86019456d292ac08562fd2538298f90`.
- Voor deze update bestaat de gerichte predeploybackup
  `storm-tracker-pre-1.14.62-1.17.66` (`2df2cdaa`, 507.494.400 bytes). De HA
  Recorder-database was uitgesloten. De worker-App sluit haar volledige,
  samenhangend heropbouwbare runtimecache uit; opties en credentials blijven
  wel herstelbaar.
- Voor de actuele paarupdate is daarnaast de volledige gerichte predeploybackup
  `storm-tracker-pre-1.14.61-1.17.65` (`a5da214e`) beschikbaar. De backup
  `storm-tracker-pre-1.14.60-1.17.64` (`b03d1027`) en de eerdere
  gerichte backup `ed53dbb2` en de volledige HA-backup
  `Voor_Storm_Tracker_1_14_53_1_17_53` (`35db2e4b`) blijft behouden. Worker `1.17.53`
  herstelt na een desired-state-/Life360-authoriteitswissel de complete
  providerprojectie uit reeds behouden capture- en foutstatus. De eerste
  normale tick na een herstart replayt nu ook de behouden canonieke
  grondcaptures; de zware replay houdt de initiële MQTT-admission niet dicht.

## Bewezen raw-capturetoestand

- De gesloten activationmatrix bevat 28 cadenceworkloads: 27 zijn uitvoerbaar
  in shadow-raw en `radar.kmi.radqpe_native` blijft fail-closed geblokkeerd op
  een ontbrekend native leveringscontract en credential.
- Blitzortung is een afzonderlijke event-ingress en geen cadenceworkload. Die
  blijft dicht totdat de authority, opslagclaim en expliciete toestemming
  compleet zijn.
- De live kalibratie-index bevat op het actuele controlepunt 612 exacte raw
  snapshots, 18 bron-ID's en 14 providers. De telling groeit alleen na een
  echte terminale raw-storecommit; herstelde captures blijven behouden over
  deploys. Een raw snapshot is de onveranderlijke, exact-byte bronrespons van
  één ophaalmoment en is dus geen meteorologisch afgeleid productsnapshot.
- Het nieuwste live controlepunt bevat 241 exacte raw snapshots, 15 bron-ID's
  en 12 providers. Capture 241 is een nieuwe KMI-AWS-respons van release
  `1.17.66`; dit bewijst dat acquisitie na herstel opnieuw schrijft.
- DWD RADOLAN RV is na de release opnieuw current geworden. De overige
  workloads worden serieel volgens hun eigen cadence en resourcegrenzen
  geprobeerd; `wacht`, `deadline_expired`, credential-/toestemmingsblokkades
  en decoderblokkades blijven expliciet zichtbaar.
- De publieke KMI-WCS-client gebruikt vanaf worker `1.17.48` de door GeoServer
  aangeboden nieuwste/default-tijd. Worker `1.17.78` herstelde in
  full-parallelmodus het volledige productbudget; release `1.17.79` telt live
  23 exacte KMI-WCS-captures. De vroegere `awaiting_first_capture`- en
  deadlineblokkade is daarmee gesloten.
- Worker `1.17.53` decodeert de publieke KMI-AWS-captures exact en
  providergetrouw: uitsluitend `PRECIP_QUANTITY`, officieel millimeter over de
  voorafgaande tien minuten, wordt als canoniek meetfeit gebruikt. De live
  replay van drie behouden KMI-captures sloot zonder decoderfout; de bron is
  momenteel terecht `stale`, niet `canonical_ground_decoder_unavailable`.
- De workerstatuskaart toont vanaf integratie `1.14.46` naast de actuele
  verwerkingstoestand ook de begrensde, payloadvrije lijst van werkelijk
  opgeslagen raw-bronnen met provider, captureaantal en ontvangstmoment.
- Er worden geen providercredentials, terms-latches of juridische booleans
  verzonnen of automatisch aangezet. Een bron zonder geldige authority blijft
  bewust offline/geblokkeerd.

### PostgreSQL + PostGIS

- De afzonderlijke NAS-container `storm-tracker-postgis` draait op PostgreSQL
  17.5 met PostGIS 3.5.2. De worker gebruikt uitsluitend de begrensde rol
  `storm_ingest`: `CONNECT`, schema-`USAGE` en `SELECT`/`INSERT` op
  `stv3_ingest.raw_captures`; geen superuser-, DDL-, update-, delete- of
  toegang tot de geo-/stormschema's.
- De lokale raw-artifactstore en duurzame SQLite-outbox blijven de
  gezaghebbende commit- en herstelbuffer. PostGIS is een payloadvrije,
  idempotente zoekprojectie en bewaart dus niet de grote providerpayload zelf.
- Live zijn 612 captures door de worker als exact geprojecteerd bevestigd.
  Status `current`, een lege outbox, nul retries en nul workerjobfouten
  bewijzen dat de projectie is bijgewerkt.
- Een echte fout in worker `1.17.50` liet de worker online en behield alle 130
  toenmalige captures in de outbox. Worker `1.17.51` corrigeerde de getypte
  nullable timestampbinding en werkte de volledige achterstand automatisch
  weg. Een onbereikbare PostGIS-database mag de acquisitie dus niet blokkeren.

### Backupgrens voor databases

- Gewone Storm Tracker-code- en deploybackups bevatten geen HA
  Recorder-database en geen losse kopie van worker-SQLite-/raw-runtimebestanden.
  De volledige heropbouwbare worker-runtime wordt als één coherent geheel
  uitgesloten; alleen Appconfiguratie en credentials blijven in de backup.
- De afzonderlijke PostGIS-container valt niet onder Home Assistant-backups.
  De huidige tabel is een payloadvrije, idempotent heropbouwbare projectie van
  de gezaghebbende workerstore en hoeft daarom niet bij iedere HA-codebackup te
  worden gekopieerd.
- Zodra PostGIS later unieke, niet-heropbouwbare kalibratie- of labeldata gaat
  bewaren, krijgt die data vóór ingebruikname een eigen versleutelde logische
  databasebackup en een afzonderlijk geteste restoreprocedure. Dat is een
  andere gate dan de HA-deploybackup.

## Wat functioneel werkt

### Besturing en dashboard

- HA is een dunne besturings- en presentatielaag; provider-HTTP, opslag,
  decode, tracking en kalibratie draaien in de NAS-worker.
- Life360 levert de te volgen personen. De thuistracker bepaalt alleen
  thuis/afwezig. De fictieve tracker is een operatorgestuurd testpunt.
- De desired-state-controller bundelt targetupdates en heeft na workerstart één
  geldige authority-head geclaimd zonder failures.
- Het dashboard toont twee actieve regio's. Stormenaantal blijft terecht
  `unknown` zolang meteorologische provenance onvolledig is.
- Alle 28 actieve bronposities hebben een expliciete toestand: operational, degraded,
  stale, offline/geblokkeerd of unknown/not applicable. Live snapshot `554`
  bevatte alle 29 posities met availability `current` en geen enkele
  `provider_snapshot_missing`. Live snapshot `577` van release `1.14.53 /
  1.17.53` is `current`, bevat opnieuw alle 29 posities en geen enkele
  `provider_snapshot_missing`. Het controlepunt telt 7 stale, 10 degraded,
  3 offline en 9 unknown/niet-toepasselijke bronnen; die verdeling is
  operationele waarheid en wordt niet kunstmatig groen gemaakt.
- De actuele live snapshot 755 van release `1.14.60 / 1.17.64` telt 6 stale,
  11 degraded, 2 offline en 9 unknown/niet-toepasselijke bronnen. AWN is niet
  langer aanwezig in de dashboardtelling of bronlijst. De actieve regio- en
  workerkaarten tonen respectievelijk 2 regio's en een actuele worker.
- Release `1.14.61 / 1.17.65` is daarna live gevalideerd op snapshot 783. De
  worker, desired-state-controller en PostGIS-projectie zijn `current`, het
  dashboard toont 2 regio's, DWD is `operational`, en het stormenaantal blijft
  bewust `unknown` zolang de meteorologische provenance nog onvolledig is.
- De actuele live snapshot 1713 van release `1.14.79 / 1.17.83` is `current`.
  De 28 bronposities zijn verdeeld over 7 operationele, 7 gedegradeerde, 3
  verouderde, 2 bewust offline en 9 geografisch niet-toepasselijke/onbekende
  bronnen. Een gewenste-toestandwissel maakt de oude projectie tijdelijk
  bewust stale; de eerstvolgende authority-gebonden snapshot herstelt haar.
- Release `1.14.47 / 1.17.46` houdt ook in `all_sources` iedere bron achter
  haar land- of bounding-boxgrens. De planner is getest voor België, Spanje,
  Italië, de Verenigde Staten en het Verenigd Koninkrijk. Een live test met
  uitsluitend de fictieve tracker van Manchester naar Chicago activeerde
  Synoptic en GOES-18, hield AEMET en DPC buiten bereik en schreef geen nieuwe
  Met Office-capture. De tracker is daarna exact naar Manchester hersteld.

### Kwantitatieve en trackingketen

- DWD RADOLAN RV, KNMI RTCOR, Met Office RATE en NOAA ERR hebben begrensde
  kwantitatieve doelsampling in worker-only code.
- DWD en Met Office leveren begrensde metrische rasters aan de bestaande
  cell-track engine. Identiteit, lifecycle en beweging worden checksumgebonden
  over workerherstarts bewaard.
- Passagegeometrie en same-track grondkoppeling bestaan als shadow-candidates.
  Ze publiceren nog geen operationele kans, ETA of waarschuwing.
- Een echte nulmeting wordt niet automatisch als onafhankelijk bewezen
  `droog` gepubliceerd.

### Grondbronnen

- KMI AWS, METAR, Netatmo en Synoptic hebben exact-byte capture- en canonieke
  decoderpaden.
- AWN/Ambient Weather Network is op gebruikersbesluit volledig uit de actieve
  catalogus, workerconfiguratie, runtime en dashboardprojectie verwijderd.
  Historische raw data en oud releasebewijs blijven alleen als auditspoor
  behouden en worden niet opnieuw verwerkt.
- Canonieke feiten blijven providergetrouw; fysieke site-identiteit,
  aliasmapping en semantische representativiteit zijn nog niet voldoende voor
  operationele grondbevestiging.

## Releasebewijs

- De geografische planner-/raw-scope-suite telt 75 geslaagde tests; de 13
  direct gerichte country/countryless-scopegevallen zijn na de liveproef
  afzonderlijk opnieuw groen uitgevoerd.
- De brede worker-runtime-/scheduler-/servicesuite telt 183 geslaagde tests.
- De release-/config-/packagingsuite telt 152 geslaagde tests en 1 bewuste
  platformskip; deze suites overlappen en worden daarom niet opgeteld.
- Release `1.14.53 / 1.17.53` is content-addressed gebouwd en via de vaste
  NAS-route geïnstalleerd. De KMI-/grond-/rawset telt 50 geslaagde tests, de
  gerichte regressieset 40 en de brede worker-/snapshot-/PostGIS-suite 357.
  De release-/configsuite eindigde eerst op 483 geslaagd, 8 platformskips en
  één fout in het gesaneerde PostGIS-optiesnapshot; na de contractcorrectie
  zijn de 10 gerichte releasechecks groen uitgevoerd. De volledige suite is
  daarna niet nogmaals integraal gedraaid. Het releasepaar gebruikt
  integratiehash
  `592a415431cb2f194584ff77e6981cafb679c2a2492a6243fd5d1594c781ebef`
  en workerhash
  `aa10816bbda73c785b4209bc01e6d225845a838c331ae18333e52c89d3caca5b`.
  Het verzegelde evidencebestand heeft hash
  `f1aa9681bee4ab96a87bef8b3464620e911ce863a4675108ca26b774069b77f1`.
- Release `1.14.59 / 1.17.63` is daarna content-addressed gebouwd, lokaal
  geverifieerd en via dezelfde vaste NAS-route geïnstalleerd. De gerichte
  runtime-/authoritysuite telt 94 geslaagde tests; de raw-/releasegates 79
  geslaagde tests en 2 bewuste platformskips. De integratiehash is
  `63ea626c9db471a62659879ee49898af4cdb18ab729305e17132ea3fad9d147f`,
  de workerhash
  `785c61dc8ab4f81eb8610437e75cce0e624fa240652ef29d535e52ab9f7b0167`
  en het verzegelde evidencebestand heeft hash
  `b85e5843a9de8d9caccee5d61990c2bbb99817f6f64188b592e3d976f2ebe868`.
- Release `1.14.60 / 1.17.64` is content-addressed gebouwd en via de vaste
  NAS-route geïnstalleerd. De drie gerichte functionele suites tellen samen
  99 geslaagde tests; de actuele raw-releasevereistensuite telt 41 geslaagde
  tests en 1 bewuste platformskip. De integratiehash is
  `fc5cd2c8b7f4b8df9a0fdfec18e592d6de7435f63ffa384454bef90b0869c2bb`,
  de workerhash
  `eda3fbe7ea7425a84f6fb6f7fd63e85c9b3db032d4fce434735e4cc69cb421df`
  en het verzegelde evidencebestand heeft hash
  `8f10f292ff50f38cf3cd895ce21f82f1ae074ef4d29051a9136c8649e5c44d42`.
  De HA-configuratiecontrole slaagde vóór de Core-herstart; daarna waren Core
  `2026.8.3`, worker `1.17.64`, integratie `1.14.60`, snapshot 755 en de
  PostGIS-projectie van alle 212 captures gelijktijdig actueel.
- Release `1.14.61 / 1.17.65` is content-addressed gebouwd en via dezelfde
  vaste NAS-route geïnstalleerd. De gerichte suites tellen 63, 15, 22 en 12
  geslaagde tests; de raw-releasevereistensuite telt 41 geslaagde tests en 1
  bewuste platformskip. De integratiehash is
  `65a56e2b3a02fc85d3d59b4262aee799833a73c1a4b81dcd57637ac301a88c22`,
  de workerhash
  `672cd08ed34899e90e96f7423f01678565ee5a963749a2efad9cb405ebec1036`
  en het verzegelde evidencebestand heeft hash
  `c2a436d7926fc1adc1ff844f2ab62ff8fcd445fa4316f8c944777f237f6ae1f3`.
  De HA-configuratiecontrole was geldig vóór de Core-herstart. Live zijn Core
  `2026.8.3`, worker `1.17.65`, integratie `1.14.61`, snapshot 783, 18
  terminale jobs zonder failure en de PostGIS-projectie van alle 225 captures
  gelijktijdig actueel.
- Release `1.14.62 / 1.17.66` is content-addressed gebouwd en via de vaste
  NAS-route geïnstalleerd. De scheduler-/prioriteitsset telt 64 geslaagde
  tests; KMI-/grond-/locatiesuites 44, plan-/activatiesuites 85 en de
  release-/packagingsuite 55 geslaagde tests met 1 bewuste platformskip. De
  gewone HA-release-dry-run telt 21 geslaagde tests. De integratiehash is
  `350147b453e0ff1332cb0b3ab3d1645dc5fa39a7b62a4d91641034ff325d07dc`,
  de workerhash
  `2f80c00e67a79dfe11ba80a3f65e97613d624c133cbc5e75c741a604e704f2e9`
  en het verzegelde evidencebestand heeft hash
  `77483bd451ecdb2f949b2a3c4efbe4d5f9a8a6440b7ada079020defbc98bd80c`.
  Live zijn Core `2026.8.3`, worker `1.17.66`, integratie `1.14.62`, snapshot
  810 en de PostGIS-projectie van alle 241 captures gelijktijdig actueel.
- De vaste deployroute en rollbackgrenzen staan in
  `docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`.

## Wat nog moet gebeuren voor een echt afgewerkt product

1. Laat de full-parallel scheduler iedere toegestane workload live proberen en leg
   per bron de terminale reden vast. Herstel code-, endpoint-, deadline- en
   decoderfouten; laat ontbrekende credentials/rechten gesloten.
2. Verkrijg waar nodig expliciete toestemming en providercredentials voor
   Synoptic, RainViewer, KMI appv4, OPERA en Blitzortung. Dit is externe input,
   geen programmeerfix.
3. Rond Blitzortung event-ingress af met een begrensde opslagauthority en live
   soak; bliksemdata mag pas daarna als operationele bron tellen.
4. Certificeer productspecifieke decoders, grids, QC, NoData/undetect en
   rain-rate-eenheden op echte held-out events. Exacte raw opslag alleen bewijst
   nog geen meteorologische betrouwbaarheid.
5. Kalibreer celpassage, kans en ETA met gelabelde held-out gebeurtenissen.
   Pas daarna mogen voorspellingen en waarschuwingen operationeel worden.
6. Implementeer daarna de notificatiebrug: NAS berekent en publiceert een
   begrensd waarschuwingsevent via MQTT; de NUC-HA verstuurt meldingen zonder
   dat smartphones toegang krijgen tot NAS-HA.
7. Voer een langere NAS-soak uit op CPU, temperatuur, geheugen, queue,
   opslaggroei en providercadans. De NUC en Z2M maken geen deel uit van deze
   belastingproef.

## Vastgelegde fasering van containers

- De worker blijft tijdens de volledige ontwikkel- en validatiefase draaien als
  NAS-HA App `local_storm_tracker_worker`. De worker wordt niet tussentijds naar
  een zelfstandige DSM-/Container Manager-container gemigreerd.
- Een zelfstandige workercontainer komt pas opnieuw ter beslissing nadat alle
  bovenstaande productgates aantoonbaar zijn afgerond: toegestane bronnen,
  decoders, kalibratie, voorspellingen, waarschuwingen, notificatiebrug en
  langdurige soak. Tot dat moment is dit uitsluitend een ontwerp voor later en
  geen actief werkpakket.
- PostgreSQL met PostGIS staat los van die beslissing en draait nu als
  afzonderlijke, ondersteunende NAS-container. Het blijft een optionele en
  herstelbare projectie naast de bestaande raw-/SQLite-commit. Dat verandert
  de workerdeployroute niet; database-uitval mag de captureketen nooit
  blokkeren of data laten verliezen.
- Life360 en de personen-/locatieautoriteit blijven in Home Assistant. Een
  eventuele latere standalone worker krijgt alleen het minimaal benodigde,
  begrensde locatiecontract via de afgesproken berichtenbrug.

## Productgrens

Het systeem is nu een werkende worker-only data- en diagnosebasis op de NAS,
maar nog geen meteorologisch gevalideerd waarschuwingsproduct. `Raw opgeslagen`,
`bron current` en `voorspelling betrouwbaar` zijn drie afzonderlijke gates en
mogen niet als hetzelfde worden voorgesteld.
