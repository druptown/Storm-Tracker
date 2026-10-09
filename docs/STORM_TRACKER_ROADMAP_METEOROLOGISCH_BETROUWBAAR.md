# Storm Tracker V3 — roadmap naar een meteorologisch betrouwbare toepassing

## Documentstatus

| Veld | Waarde |
|---|---|
| Status | Uitvoeringsroadmap en architectuurcontract |
| Datum | 2026-08-21 |
| Laatst gemelde live Home Assistant-integratie | onbevestigd; laatst gemeld `1.11.1` |
| Laatst gemelde live worker | onbevestigd; laatst gemeld `1.14.0` |
| Lokale worker-first kandidaat | integration `1.13.1` + worker `1.16.1`; vier RAW-releasefiles primary/repro byte-identiek, niet gedeployed en targetbundle niet compleet |
| Lokale regressiestatus | historische freeze blijft 4.934 passed + 15 skips; actuele cross-cutting 1.13.1/1.16.1-matrix 1.377 passed + 11 skips, 0 failures/warnings; inventories overlappen en worden niet opgeteld; live fault-, replay-, power-loss- en NUC-soakgates blijven open |
| Huidige RAW-releasekandidaat | [LOCAL_RELEASE_CANDIDATE_1.13.1_WORKER_1.16.1.md](LOCAL_RELEASE_CANDIDATE_1.13.1_WORKER_1.16.1.md) |
| Bevroren RC1-bewijsstatus | [ROADMAP_EXECUTION_STATUS_2026-08-11.md](ROADMAP_EXECUTION_STATUS_2026-08-11.md) |
| Post-RC1-validatiestatus | [ROADMAP_EXECUTION_STATUS_2026-08-11_POST_RC1.md](ROADMAP_EXECUTION_STATUS_2026-08-11_POST_RC1.md) |
| Lokale opt-in RAW-service | 27 cadenceworkloads plus afzonderlijke Blitzortung-ingang; default legacy ongewijzigd; RAW-commit terminaal, downstream/meteo/dashboard uit; targetpins en voldoende capaciteit ontbreken nog |
| Operationeel meetprotocol | `OPERATIONAL_SLO_MEASUREMENT_PROTOCOL.md` |
| Brondocumenten | `STORM_TRACKER_AS_BUILT_1.11.0.md`, `DATA_GAP_AUDIT.md`, actuele werkboom |
| Reikwijdte | Provideringest, opslag, tracking, nowcast, ground fusion, worker, HA, validatie en uitrol |
| Hoofdkeuze | Nieuwe meteorologische kern in shadow mode; bestaande waardevolle infrastructuur behouden |

Dit document is de uitvoerbare opvolger van de as-built-audit en de
datagatenlijst. Het beschrijft niet alleen het gewenste eindbeeld, maar ook:

- de volgorde waarin het systeem veilig wordt hersteld;
- de gegevens- en eventcontracten;
- de meteorologische methoden;
- concrete tickets met afhankelijkheden;
- tests en meetbare acceptatiecriteria;
- migratie, rollback, security en observability;
- de Definition of Done voor iedere fase en voor de volledige toepassing.

De actuele werkboom bevat reeds parallelle P0-wijzigingen. Die worden in dit
document uitdrukkelijk als **geïmplementeerd in werkboom, nog niet
vrijgegeven** aangeduid. Een codewijziging is pas klaar na gerichte tests, de
volledige suite, fault injection, een replaycontrole en live verificatie.

---

## 1. Eindbesluit

### 1.1 Wat we herbouwen

De meteorologische besliskern wordt opnieuw opgebouwd rond een persistent
`CellTrack`-model:

- hiërarchische radardetectie;
- lokale optical flow;
- globale associatie;
- expliciete birth/death/split/merge-lineage;
- probabilistische veldnowcast;
- tracker-specifieke passage- en intensiteitsverdelingen;
- ground fusion die aan exact dezelfde track en hetzelfde tijdvenster is
  gekoppeld;
- empirisch gekalibreerde onzekerheid.

De bestaande `StormEngine` blijft tijdens de herbouw alleen een comparator. Hij
is niet de wetenschappelijke waarheid voor de nieuwe engine.

### 1.2 Wat we behouden

De volgende delen leveren reeds waarde en worden niet onnodig weggegooid:

- Home Assistant-configuratie, targets, sensoren en kaartcontracten;
- regionale isolatie via `RegionEngine`;
- providerregistry en geografische providerselectie;
- providerdownloads en bruikbare parsers;
- de aparte worker-add-on;
- MQTT-heartbeat, build-ID, bounded queue en jobdiagnostiek;
- de replay- en kalibratie-infrastructuur;
- opslag van provideroverlays en radarfootprints;
- target-specifieke forecastverificatie;
- virtual probes als observerende validatielaag;
- SQLite WAL en transactionele writes;
- bestaande contract- en regressietests die inhoudelijk correct gedrag
  vastleggen.

Deze delen worden waar nodig achter nieuwe contracten geplaatst. Behouden
betekent niet dat hun huidige semantiek automatisch correct is.

### 1.3 Waarom geen big-bangvervanging

Een volledige gelijktijdige vervanging zou:

- live dataverzameling onderbreken;
- de nieuwe engine zonder betrouwbare baseline laten starten;
- regressies in providers, HA-entiteiten en workertransport moeilijk
  lokaliseerbaar maken;
- rollback bijna onmogelijk maken;
- nieuwe meteorologische fouten direct operationeel maken.

Daarom gebruiken we een strangler-/shadowstrategie:

1. verlies en actuele verkeerde conclusies stoppen;
2. alle input immutable en replaybaar maken;
3. de nieuwe engine naast de oude bouwen;
4. beide engines op exact dezelfde batches laten rekenen;
5. vooraf vastgelegde metrics vergelijken;
6. per provider en regio gecontroleerd omschakelen;
7. pas daarna de oude trackingkern verwijderen.

---

## 2. Wat “meteorologisch betrouwbaar” hier betekent

Geen radar, grondstation of nowcastmodel is absolute waarheid. Radar meet een
volume in de atmosfeer, een regenmeter meet een punt aan de grond en een
extrapolatiemodel kan nieuw ontstane convectie niet uit niets voorspellen.

Storm Tracker is betrouwbaar wanneer het:

1. iedere ontvangen meting bewaart of aantoonbaar registreert waarom de
   ontvangst technisch mislukte;
2. geen echte detectie stil verwijdert wegens een capaciteits- of
   plausibiliteitslimiet;
3. individuele cellen door de tijd volgt met meetbare identity uncertainty;
4. splits en merges als lineage bewaart;
5. providerwissels als measurement discontinuity behandelt;
6. actuele footprint, historische footprint en forecastcorridor niet
   verwart;
7. ground evidence aan dezelfde track, plaats en tijd koppelt;
8. “regen hier”, “regen nabij”, “bui bevestigd” en “volgende dreiging”
   afzonderlijk rapporteert;
9. ETA, intensiteit en passage als kansverdelingen publiceert;
10. probabilities alleen als probabilities toont wanneer ze empirisch
    gekalibreerd zijn;
11. bij onvoldoende of te oude data `unknown` rapporteert en geen
    schijnzekerheid produceert;
12. iedere operationele conclusie via foreign keys tot de gebruikte
    bronrecords kan reproduceren;
13. aantoonbaar beter presteert dan persistence en de huidige engine op
    onafhankelijke replays;
14. onder worker-, queue-, opslag- en providerfouten geen weerobservaties
    verliest.

### 2.1 Niet-doelen

De eerste betrouwbare release hoeft niet:

- een volledig numeriek weermodel te vervangen;
- convectieve initiatie tot uren vooruit deterministisch te voorspellen;
- iedere provider tot één universele “waarheid” samen te mengen;
- ieder ensemblelid in Home Assistant op te slaan;
- een waarschuwingendienst van een nationaal meteorologisch instituut te
  vervangen;
- één universele detectiedrempel voor alle radarproducten te gebruiken.

### 2.2 Onveranderlijke semantische regel

Elke laag krijgt een eigen verantwoordelijkheid:

- **raw artifact:** ontvangen bronpayload;
- **observation:** geïnterpreteerde meting;
- **detection:** object in één frame;
- **association:** mogelijke relatie tussen twee tijdstappen;
- **track:** persistente identiteit met covariance;
- **lineage:** birth, death, split, merge en gap;
- **forecast:** kansverdeling;
- **ground evidence:** tijd- en plaatsgebonden likelihoodbijdrage;
- **dashboard:** presentatie en prioritering.

Een latere laag mag een record uit een eerdere laag nooit stil verwijderen.

---

## 3. Actuele P0-status

### 3.1 Parallel geïmplementeerd, nog niet vrijgegeven

| Ticket | Probleem | Actuele werkboom | Nog vereist voor “klaar” |
|---|---|---|---|
| P0-L01 | OFE wist of blokkeerde batches vóór callbacksucces | Batch houdt bij retry dezelfde ID; iedere observation wordt vóór cachemutatie duurzaam `RECEIVED` of `DEDUPED`. Poison-retries, atomische splits, lineage-epochs en quarantaine zitten in het duurzame journal. Recovery hervat ontvangen input en herstelbare batches FIFO vóór provideradmission. Vaste cumulatieve counters bewaken offered/delivered/requeued/discarded/lost voor observaties en rootbatches met een side-effectvrije conservation-invariant | live HA-restart, brokerdisconnect, providerdruk en langdurige fault injection |
| P0-L02 | Workerfout of laat resultaat kon toch als verwerkt gelden | Trackingprotocol v5 bindt batch-ID, input, startstate en startrevision met exacte hashes en bewaart producttijd, ontvangsttijd en bronprovenance verliesloos; HA herberekent de volledige snapshot-hash en commit alleen via revision-CAS; lokale fallback met dezelfde batch-ID is idempotent; lokale mutatie loopt op een working copy en bewaart gepubliceerde Storm-handles atomair | timeout/late-result, queue-full, disconnect en workerrestart end-to-end op live MQTT testen |
| P0-L03 | `MAX_STORMS=15` kon nieuwe detecties weggooien | lokaal niet-evictend policycontract `target_distance_non_evicting_v1`; iedere targetwijziging, batchcommit en restore publiceert nu een vooraf berekende RegionEngine-snapshot. Een interne schedulerfout valt zichtbaar terug op execute-all; de relevante zestiende bui, 96-objectmicroreplay, 30-objectruntimecommit, side-effectvrije sensorread en bytepariteit zijn groen | alle voorkeurspasses door echte STEPS/LINDA-jobs laten consumeren en live NUC queue-/CPU-/RSS-/thermische soak |
| P0-G01 | Polygonafstand gebruikte alleen vertices | De gedeelde rasterketen bewaart nu immutable MultiPolygon-topologie met losse componenten en holes door Observation, cell/frame, durable/restartstate en HA/workertracking; bolgeometrie en grondmatching respecteren die topologie en een pixel-exacte fallback voorkomt stille geometrieverlies | aparte legacy OPERA-contourtracer, echte providerframes, payloadbudget en immutable live New York-replay |
| P0-G02 | Ground confirmation telde storms samen | Groundobservations worden per `storm_id` gegroepeerd en alleen een groep kan bevestigen; matching gebruikt het tijdsdichtste actuele radarframe. Een nieuwe immutable PhysicalSite-catalogus resolveert uitsluitend expliciet geverifieerde, versiegebonden aliases; nabijheid creëert nooit identiteit en gedeeld ICAO/STID kan alleen conservatief correleren. Catalogus-, registry- en mappingfingerprints lopen nu getypeerd door tot de immutable, hashgedekte groundbeslissing en forecasttrace | autoritatieve en gehashte crosswalks/capabilities/sitehistoriek vullen, cataloguswijzigings-/resolutie-audit events opslaan en echte globale groundreplays uitvoeren |
| P0-W01 | Queue-full/invalid gaf geen terminal jobstatus | Naast terminale, duurzame receipts en geïsoleerde deadline-executie heeft ieder productiejobtype nu een immutable effectcontract. Netatmo-credentialrotatie commit in de parent vóór childdispatch; secretredactie, fail-closed tokenstate, exact handlercontractbereik en zichtbaar compensatiebeleid begrenzen hard-kill-risico | echte SIGKILL/power-cut, broker/provider/storagefouten, `/share`-directorydurability en NUC-soak |

Deze zes tickets mogen niet opnieuw als architectuurwerk worden uitgevonden.
Ze moeten eerst volledig worden bewezen en daarna als vaste regressiebarrière
voor de nieuwe engine blijven bestaan.

### 3.2 Overige P0 die nog open blijven

| Ticket | Probleem | Actuele werkboom | Nog vereist voor “klaar” |
|---|---|---|---|
| P0-G03 | Groundmatch gebruikte een historische systeemhull | code gebruikt het tijdsdichtste actuele radarframe; de gedeelde rasterproviders bewaren outer rings, holes en losse componenten door actuele cell/frame- en restartstate, en de matcher beoordeelt de volledige actuele geometrie in één keer | OPERA-native topologie, formele runtimeproducerbinding en immutable live New York-replay |
| P0-S01 | Sensorreadpad kon auditlijsten muteren | propertyreads zijn puur; alleen expliciete state-updates schrijven een per target/minuut gededupliceerd auditrecord | volledige HA-eventtest |
| P0-D01 | Immutable operationele ingest moest crash-durable worden | lokaal schema v3 bewaart iedere genormaliseerde observation vóór zware verwerking met canonieke identity en expliciete `RECEIVED`/`DEDUPED`/`BATCHED`-status | live wiring met de vNext RawArtifactStore voor originele providerpayloads |
| P0-D02 | Duurzaam batchreceipt en idempotentie over restart moesten één totaalcontract vormen | batch, attempts, transitions, versioned trackinghead en receipt zijn persistent; state+receipt committen atomair met revision-CAS. Echte `os._exit`-faultinjectie op twaalf SQL-grenzen ziet na reopen uitsluitend oude of volledige nieuwe state. De finale offline freeze van 2026-08-11 neemt deze regressies mee in 4,934 groene tests met warnings als errors | live HA/addon/power-lossmatrix, migratie/rollback en disk-full/corruptietests blijven releasegate |
| P0-C01 | Harde snelheidsgrenzen beïnvloedden associatie/preflight | lokaal zijn de vaste 180/160/120/100-km/h-afkeursemantieken verwijderd. Extreme intervalbeweging blijft meetbaar; het rasterzoekbudget is expliciet computationeel (maximaal 625 kandidaten) en een gecensureerde rand wordt `motion_indeterminate`, nooit “onmogelijk” | volledige suite, echte snelle replay, NUC-resource-soak en pas daarna deployment |
| P0-F01 | `current_precipitation` en `next_threat` zijn nog onvoldoende gescheiden | het getypeerde HA-/workercontract bewaakt signed absolute tijden, providerfreshness en volledige scans; `ABSENT`/`NOT_DETECTED` kan alleen uit verse, volledige typed footprint- en candidate-evidence volgen, terwijl malformed/overflow fail-closed onbepaald blijft | formele producer-/opslagwiring, volledige runtimepariteit en immutable New York plus andere trackerreplays |
| P0-R01 | Afsluiten kon providerfetch en OFE-reset laten racen | lifecycle-admission sluit nu eerst, actieve lifecyclefetch wordt geannuleerd, providercycluslocks worden uitgewacht en pas daarna volgen calibrationflush, OFE-reset en stateopslag; recente HA gebruikt de awaited shutdown-job-API | alle niet-lifecycleproviders onder hetzelfde runtimecontract brengen en crash/restart-faultmatrix uitvoeren |

### 3.3 Worker-only cutoverfundering van 2026-08-01

De totale ombouw is nog niet operationeel geactiveerd, maar de nieuwe
procesgrens heeft nu afzonderlijk geteste bouwstenen:

- een nieuw content-addressed workloadmanifest sluit de volledige HA-naar-
  workerombouw af tegen vergeten werk. Slechts desired-state/CAS, één bounded
  bridge en O(1)-snapshotprojectie mogen uiteindelijk in HA blijven. De audit
  vond daarbij twee echte migratiegaten die eerder niet in de providerjobmatrix
  stonden: MeteoLux, GeoSphere Austria en ItaliaMeteo voeren ondanks hun
  `validation_only`-rol nog HTTP/JSON/lifecyclewerk in HA uit, en de regionale
  Blitzortung-pushsubscription is eveneens nog HA-owned. Beide blijven nu
  expliciet `blocked_gap`; de gecombineerde manifest/control-plane-suite staat
  op 54 groene tests;

- desired-state v1 bewaart maximaal 64 targets en uitsluitend relevante
  instellingen crash-durable in de worker, met authority fencing,
  generation/revision-CAS, positie- en klokcontroles en zonder credentials. Een
  afzonderlijke pure HA-builder produceert inmiddels byte-identiek dezelfde
  canonieke bytes en SHA/CAS-identiteit, sorteert targets deterministisch en
  weigert extra velden, credentials, nonfinite waarden en meer dan 64 targets.
  Een ongewirede lichte controller serialiseert bovendien
  `claim -> snapshot -> apply`, coalescet updates, bewaart hoogstens één
  onzekere poging en herstelt CAS/reconnect zonder lokale fallback; de 92
  gecombineerde contracttests zijn groen. Event-/entrywiring en registratie
  zijn nog niet aangesloten;
- een nieuwe autonome scheduler leest alleen die actieve desired-state, maakt
  deterministische cadence-slot/job-ID's en bewaart een begrensde durable
  outbox met één lease, at-least-once restartrecovery en fail-closed klok- en
  stale-statecontrole; runtimeactivatie ontbreekt nog;
- een afzonderlijke ongewirede acquisitieplanner vertaalt dezelfde
  cryptografisch herbevestigde desired-state naar 27 gesloten cadenceworkloads
  voor radar, satelliet, grond en model. Blitzortung is daarvan losgemaakt als
  één afzonderlijk event-ingresscontract. Complete-link regiodeling
  voorkomt kettingclusters; globale, coverage- en regionale requests worden
  deterministisch gededupliceerd. Cadans, deadline, transportbytes,
  artefactaantal, wall time, prioriteit, abstracte `worker_secret.*`-vereisten,
  job en terminal receipt zijn contentgebonden. De kandidaat laat exact één
  in-flight job toe. De huidige activationmatrix telt exact elf lokale
  `active_shadow_raw`-regels, negen `blocked_unwired`-regels en zeven
  `blocked_unsupported`-regels. De standaard/ontbrekende servicemodus blijft
  legacy en telt nul worker-first-workloads; de expliciete lokaal gepinde
  `worker_first_canary` laat uitsluitend `radar.dwd.radolan_rv` toe. Een
  durable process-safe binding met de
  bestaande outbox, echte handlers, secretresolver, officieel versioned
  coverage, catch-upbeleid en provider-/NUC-soak blijven gates;
- het byte-identieke HA-/workercontract `operational_snapshot_v1` scheidt
  huidige neerslag strikt van de volgende dreiging, begrenst de onbewerkte
  MQTT-envelope op 64 KiB en laat ontbrekende product-/receipttijd alleen toe
  wanneer werkelijk geen actuele meteorologische claim bestaat;
- de worker bewaart iedere canonieke operationele snapshot append-only met een
  transactionele head en exact-retryreceipt voordat publicatie mogelijk is;
- de initiële snapshotbuilder kan alle targets zichtbaar maken als
  `unknown` zonder coördinaten, providerclaim, verzonnen producttijd, regen,
  dreiging of probability te publiceren;
- de nieuwe HA-ontvangstlaag valideert uitsluitend canonieke bytes op een
  dedicated QoS-1-topic, verwisselt de O(1)-cache atomair en genereert geen
  update-event voor duplicates of ongeldige input;
- een afzonderlijke pure HA-projectielaag leest worker-, target-, provider- en
  collectiestatus uitsluitend via O(1)-cachelookups; ontbrekende of stale
  feiten blijven `onvoldoende_data`, terwijl een beschikbare volgende dreiging
  nooit actuele regen of droogte afleidt; de bestaande entities zijn nog niet
  naar deze projectielaag omgeschakeld;
- workeruitval degradeert de cache atomair naar `unknown/stale`, nooit naar
  synthetisch `dry` en nooit naar zware lokale HA-fallback;
- de seriële workerqueue gebruikt maximaal twee native threads, `nice 15` en
  sluit één volledige fysieke core uit voor HA, Z2M, MQTT en het OS;
- de KMI-identiteit is inmiddels in drie afzonderlijke fail-closed producten
  gesplitst. Appv4 is gedecommissioneerde categorische visualisatie; de
  publieke WCS is `unknown_quantity` omdat quantity, unit, nodata en native
  RADQPE-identiteit niet bewijsbaar zijn; native RADQPE blijft extern
  geblokkeerd totdat delivery/authenticatie, exacte productspecificatie en
  retentierecht contractueel beschikbaar zijn. KNMI RTCOR is een andere
  provider/productketen en staat wel als exacte lokale shadow-raw regel;
- DWD bewaart de exacte RV-tar en heeft een geïsoleerde, inert importeerbare
  ODIM-decoderkandidaat die native waarden/maskers als niet-operationeel
  canonical evidence bewaart. Een afzonderlijk DWD+ODIM-certificaat bindt nu
  ook het exacte 1100×1200 DE1200-grid en zijn native buitenranden. De eerste
  volledige shadowcompositor loopt daardoor van committed canonical run via
  raw-herverificatie, fysieke mm/u en gecertificeerde area-overlap naar
  lossless rain shields en embedded cores. Iedere analysis-pixel behoudt zijn
  disposition en alle vier oppervlaktefracties; niets wordt stil verwijderd.
  De detector controleert dezelfde absolute deadline en het objectbudget
  coöperatief binnen zijn lussen. Harde procesisolatie, overige
  providercertificaten, held-out replays en admission blijven gesloten;
- KNMI heeft een geïsoleerde exacte RTCOR-HDF5-capture die het officiële
  OpenAPI-contract en een echte live golden bindt aan `COR_NA`, HDF-tag 3.6,
  `PRECIP_[MM]`, quality- en adjustmentlagen. Een afzonderlijke kwantitatieve
  decoder bewaart nu 5-minutenaccumulatie, native quality en de reeds
  toegepaste adjustmentlaag afzonderlijk als niet-operationeel canonical
  evidence. Capture en decoder gebruiken duurzame validation-verdicts,
  absolute deadlines, proceslocks en netwerkloze complete retries; de gerichte
  en gecombineerde suites zijn groen. Harde child CPU/RSS-isolatie, NUC-soak
  en onafhankelijke held-out replays blijven gates en er is geen wiring actief;
- Met Office heeft een geïsoleerde exacte publieke S3-capture voor zowel het
  vorige als actuele UTC-dagoverzicht en het geselecteerde oorspronkelijke
  `Surface Rain Rate Estimate` UK-composiet. De echte golden bindt een
  kwartierproduct aan ODIM 2.2, `COMP/RATE`, een vast 2175×1725 float32-grid
  van 1 km, bronradars, Z-R-parameters en de afzonderlijke input-start/eindtijd.
  De inspectie leest bewust geen rasterpixels; 31 gerichte tests bewijzen
  durable-before-parse, complete/partial retry, vaste endpoint- en bytegrenzen,
  schemafailures en een volledig exacte drieledercapture. De ODIM
  2.2-specificatie bindt `RATE` inmiddels aan `mm/h`; quantitatieve pixeldecode,
  provider-specifieke nodata/undetect/QC-certificatie, held-out UK-events, harde
  child-isolatie, NUC-soak, admission en wiring blijven gesloten;
- AviationWeather METAR heeft een geïsoleerde exacte gzipcache-capture die het
  echte positionele 44-kolommenschema en vier wolkenlagen verwerkt, streaming
  bounded inspecteert en bij retry met exact één geverifieerde raw-read zonder
  netwerk hervat. Duurzame structurele verdicts, absolute deadline en
  procesgrenzen zijn getest. De afzonderlijke canonical-grounddecoder bewaart
  weer-/vicinitytokens categorisch, laat `Prrrr` zonder bewezen vorige
  rapporttijd ongekwantificeerd en geeft alleen ondubbelzinnige 3/6/24-uurs-
  accumulaties een echt interval. De 22 gerichte en 113 gecombineerde tests,
  inclusief een echte AWC-golden met 5.111 rijen, zijn groen; leeg, sensor-off,
  stale, future en onbekend worden nooit droog. De decoder commit nu haar hele
  begrensde factset atomisch met één raw-reverificatie en één SQLite-transactie.
  Held-out events, representativiteit, admission en wiring blijven gesloten;
- NOAA Enterprise Rain Rate heeft een geïsoleerde exacte publieke S3-capture
  voor listing-XML plus de geselecteerde finale `GLB-5` NetCDF-4. De live file
  definieert DQF nu exact als de bitmaskers `bad_quality` en
  `degraded_due_to_LZA_or_latitude_threshold_exceeded`; een NOAA-beeld/NetCDF-
  paar voor hetzelfde tijdstip verifieert rij 0 = 70°N met phi 0,619 tegen
  0,009 voor de omgekeerde kandidaat. De begrensde regionale v2-decoder bindt
  nu raw manifest, DQF, grid, producttijd en artifactprovenance. De exacte
  golden decodeert 251×351 float32-pixels, bewaart DQF-maskers afzonderlijk en
  is fail-closed op de vijf upstream-inputidentiteiten. Admission, harde child
  CPU/RSS-isolatie, NUC-soak en wiring blijven gesloten;
- Netatmo heeft een geïsoleerde exacte `getpublicdata`-capture die de response
  vóór JSON-inspectie in schema 3 bewaart. De bounding-boxquery blijft
  credentialvrij, terwijl het access token uitsluitend vluchtig in de
  Authorization-header bestaat. Exacte retry, receipt-resume, begrensde
  structure-verdicts en proceslocks zijn adversarieel getest; de 49 gerichte
  tests maken uitdrukkelijk geen regen-, droog-, dekking- of
  grondbevestigingsclaim. Een afzonderlijke ground-decoder bewaart bovendien
  station/module/tijd/getal/QC als opaque canonical facts: exacte nul,
  missing en een zachte waarde blijven onderscheiden, maar ieder fact markeert
  unit en tijdondersteuning expliciet als ongecertificeerd. De 17 gerichte
  decodertests zijn groen en ook deze decoder commit haar volledige begrensde
  factset nu atomisch met één raw-authority-read. API-/opslagrecht, officiële
  veldsemantiek, fysieke units/intervallen, een geautoriseerde golden, globale
  queryplanning, admission en wiring blijven gesloten;
- Synoptic heeft een geïsoleerde exacte Time Series JSON-capture met
  credentialvrije, checksumgebonden querycontext en netwerkloze complete
  retry. Gecodeerde credentialecho, providerfout/retrysemantiek, actuele
  code-2-vormen, station/QC-volledigheid, absolute deadline en duurzame
  success-/afkeurverdicts zijn adversarieel getest. De afzonderlijke
  canonical-grounddecoder bewaart iedere sensorset apart, gebruikt voor
  intervalindex `i>0` uitsluitend de twee opeenvolgende providertijden en laat
  index 0 onbekend wanneer de vorige tijd ontbreekt. Cumulatieve reeksen
  blijven request-windowwaarden met ongebonden start; native QC en provisional
  status worden niet tot automatische afkeur vertaald. De 14 gerichte en 198
  gecombineerde tests zijn groen. Zonder lokaal token bestaat nog geen live
  golden; de volledige factset commit inmiddels atomisch met één
  raw-authority-read. Site-/netwerkvalidatie, admission en wiring blijven
  gesloten en er wordt geen regen-, droog- of groundclaim gepubliceerd;
- RainViewer heeft een onafhankelijk geredteamde exact-byte kandidaat voor één
  manifest, negen onverzachte radar-PNG's en negen bijbehorende
  coverage-PNG's. Regio/horizon, opaque frame-ID, veranderlijk manifest,
  volledige receipt-resume en een jobdeadline zijn gebonden; een echte New
  York-golden en 53 gerichte tests zijn groen. Het blijft één frame van een
  categorische reflectiviteitsvisualisatie met kaartgeneratietijd, dus geen
  kwantitatieve regen- of motionwaarheid;
- OPERA/CIRRUS heeft een geïsoleerde exact-byte HDF5-capturekandidaat voor een
  vast vijfminutenslot. De live golden bindt ODIM 2.4, `COMP/MAX/DBZH`,
  4400×3800 float64 reflectiviteit plus native QI en exacte inputtijden;
  capture v3 heeft 107 gerichte tests, revision-aware file-backed transport,
  duurzame verdicts, proceslocks en absolute deadline. De afzonderlijke
  regionale DBZH/QI-decoder heeft 23 tests en een echte Belgische golden; die
  golden bevat `undetect` DBZH en ontbrekende QI en levert dus expliciet geen
  rain-rate-, nat- of droogclaim. Distributierechten/attributie, onafhankelijke
  kwantitatieve certificatie, harde child-isolatie en wiring blijven gesloten;
- `CanonicalEvidenceStore` v2 bewaart decoderresultaten uitsluitend als
  `non_operational_candidate_artifact`, met crash-atomische schemafingerprint
  en stabiele job/attempt-identiteit; alleen een toekomstige afzonderlijke
  admissionreceipt kan meteorologische toelating verlenen;
- `CanonicalGroundEvidenceStore` v1 bewaart afzonderlijk het laagste
  station-/site-, tijd-, variabele-, exacte waarde-/missing-/censor- en
  provider-QC-feit. Ieder fact en iedere read verifieert opnieuw de complete
  schema-3 raw capture; de store maakt nooit zelf een regen-, droog-,
  grondbevestigings-, confidence- of forecastclaim. `commit_batch()` valideert
  eerst de volledige concrete sequence en alle aggregate limieten, leest de raw
  authority exact één keer en schrijft alle nieuwe facts in één
  `BEGIN IMMEDIATE`; mixed/all-existing retries behouden de aanroepvolgorde en
  voeren één durability-barrier uit. De onafhankelijke betrokken matrix van 144
  tests bewijst append-only idempotentie, conflicts, alle capaciteitsklassen,
  `SQLITE_FULL`, concurrency, deadlines en echte crashgrenzen. Providerdecoders
  en admission blijven ongewired en gesloten;
- de nieuwe generieke hard-decodergrens start per decode een vers `spawn`-
  proces, begrenst wall time en serialiseert decoderjobs OS-breed binnen de
  worker-runtime. Linux gebruikt `RLIMIT_AS`, `RLIMIT_CPU`, parentaccounting,
  process-group kill en parent-death signal; Windows gebruikt een Job Object
  met geheugen-/tijdgrenzen en kill-on-close. De 20 gecombineerde tests zijn
  groen, maar dit is nog uitsluitend een ongewirede kandidaat: een echte
  Linux-workercontainer, kill-during-publish, gemeten overshoot en NUC-
  thermische soak blijven releasegates. Descendant-spawning wordt geweigerd,
  omdat het Linux-contract nog geen aggregate descendantbudget bewijst;
- voor de nog HA-owned Blitzortung-push bestaan nu twee ongewirede worker-
  grenzen. Schema-3 raw ingress bewaart ieder concreet QoS-0 MQTT-payload
  byte-exact vóór parsing en bindt topic, retain, ontvangsttijd, broker-,
  sessie-, delivery-, desired-state- en subscriptionidentiteit. Een aparte
  event-ingressauthority voegt proces-/OS-ownership, sessie-epochs, een
  begrensde duurzame queue, expliciete overflow-/gapreceipts, leases,
  crashrecovery en exact-once completionaccounting toe. Ongeldige reconnects
  beëindigen geen geldige sessie; verlopen leases worden duurzaam opnieuw
  aangeboden; raw bytes worden vóór completion opnieuw geverifieerd. De vaste
  semantiek `best_effort_qos0_unacknowledged` maakt afwezigheid altijd
  onbekend, nooit `no_lightning`. De gecombineerde ingress-, catalogus-,
  cutover- en isolatiematrix telt 181 groene tests. Worker-owned MQTT,
  decoder/semantiek, publieke retentie-/provenancerechten, bronreclamatie,
  Linux-durability en HA/Z2M-soak ontbreken nog; de live HA-client is niet
  gewijzigd;
- vijf nieuwe snapshot-only HA-sensorkandidaten gebruiken samen één event,
  doen uitsluitend O(1)-cacheprojectie en behandelen stale/unknown nooit als
  droog; ze zijn nog niet geregistreerd of live gekoppeld;
- een ongewirede thin-runtime composeert nu exact één snapshotcache,
  desired-state-controller, workerbridge en gedeelde eventhub met maximaal 64
  entities. De onafhankelijke matrix van 121 tests bewijst idempotente
  start/unload, één in-flight desired-state-update, offline `unknown/stale`,
  O(1)-sensorreads en afwezigheid van providers, engine, kalibratie, SQLite,
  bestanden, HTTP, executors of lokale fallback in deze compositielaag. De
  module wordt nog door geen live integratiebestand geïmporteerd; platformwiring,
  HA-restartmatrix en HA/Z2M-latentiesoak blijven releasegates;
- de schema-3 workerstore is de beoogde enige raw authority. De oudere
  schema-2 raw-storemirrors en de eigen raw-blobtabellen van het geïsoleerde
  `MeteorologicalEvidenceStore` blijven legacy/shadow en mogen niet als tweede
  operationele waarheid worden gebruikt; hun afgeleide records moeten nog via
  een cryptografisch gebonden externe raw-reference worden gemigreerd;
- de schema-3 raw-store bindt nu alle byte-, entry-, capture- en
  catalogusgrenzen immutable aan één storeprofiel. Iedere admission doet een
  volledige object-/staging-layout- en bestandslengtescan; de directorycache is alleen
  telemetrie. SQLite gebruikt een conservatief begrensde DELETE-journalenvelop
  in plaats van een bij gepinde readers groeiende WAL. De 38 gerichte tests en
  één platformskip omvatten in-place groei, onbekende entries, limietdrift,
  catalogus-full en exacte retries. Sharded/quota-optimalisatie, echte
  power-cut/filesystemmatrix en OS-level immutability blijven open gates.
- een afzonderlijke ongewirede `RawCapacityLedger` reserveert globale én
  providergebonden bytes/entries vóór publicatie en zet die reservatie met één
  `BEGIN IMMEDIATE` om naar immutable committed verbruik. De 29 onafhankelijke
  tests omvatten cross-process oversubscription, commits die boven hun
  reservatie groeien, echte procescrashes, catalogus-full en opnieuw uitgevoerde
  durability-barriers bij exacte retries. Dit is nog geen vrijgave: de ledger
  moet atomair aan de echte raw-publicatie worden gekoppeld en daarna door een
  filesystem-/power-loss-/quota-reconciliatiematrix en NUC-soak.

### 3.4 Actuele acquisition-shadowmatrix van 2026-08-09

De gesloten cadencecatalogus bevat 27 workloads. Daarvan zijn nu exact elf
workloads `active_shadow_raw` met handler-, child-, recovery-, capacity- en
provenancebinding:

- `ground.metar.global`;
- `radar.aemet.composite`;
- `radar.dpc.sri`;
- `radar.dwd.radolan_rv`;
- `radar.knmi.rtcor`;
- `radar.meteofrance.dpradar`;
- `radar.metoffice.uk_rate`;
- `satellite.goes18.rrqpe`;
- `satellite.goes19.rrqpe`;
- `satellite.hsaf.h40b`;
- `satellite.noaa.err`.

`active_shadow_raw` betekent uitsluitend dat originele transportbytes onder
een exact en begrensd workercontract kunnen worden opgeslagen en na crash
worden hervalideerd. Het betekent nadrukkelijk niet dat de provider al een
gecertificeerde decoder, trackinginput, operationeel feit of live scheduler-
wiring heeft. H SAF gebruikt bijvoorbeeld exact 66 MiB, twee rollen en een
90-secondenregel, maar blijft geblokkeerd op plain-FTP-transport, een echte
geautoriseerde golden, `rr/qind`-decoderbewijs, held-out validatie en NUC-soak.

De inert/ongewirede
[`terminal_raw_completion_router`](TERMINAL_RAW_COMPLETION_ROUTER.md) sluit de
terminal raw→completiongrens voor exact deze elf regels. Hij publiceert per
seriële aanroep hoogstens één content-addressed lifecycle-event en bewijst
crashreplay, cap, privacy, raw-tamper en cross-job rolebinding. DWD behoudt zijn
  bestaande bytepayload; KNMI, NOAA ERR en Met Office gebruiken maximaal 64 KiB
  grote manifestreferenties. Alleen DWD heeft nu een lokale opt-in
  servicecanary met acquisition-, bridge- en consumerketen; de andere tien
  routes hebben nog geen consumerloop of servicewiring:
de KNMI RTCOR- en NOAA ERR-consumers hebben inmiddels ieder een hard begrensde
spawn, provider-specifieke decode, durable output- plus processreceipt en
crashreconciliatie vóór acknowledgement, maar blijven factless en ongewired.
Beide herverifiëren vlak vóór ack opnieuw desired state, lease, raw bytes,
canonieke evidence, fysiek artefact en exact ConsumptionReceipt. NOAA gebruikt
precies één decodechild plus een afzonderlijke finale authorityreproofchild,
eist een begrensde target-regioauthority en kent geen globale/default fallback.
Eén generieke raw-manifestauditconsumer sluit nu ook
exact AEMET, DPC, Météo-France, GOES-18, GOES-19 en H SAF: de child herhasht
alle raw rollen en parent herhaalt vlak vóór ack de desired-, raw-, output- en
ConsumptionReceiptcontrole. Dit is audit-only, geen decode of feit. METAR
heeft nu eveneens een hard begrensde consumer: de child decodeert exact het
raw capture naar CanonicalGroundEvidenceStore, terwijl parent het volledige
factset, processreceipt, raw bewijs, desired state en lease opnieuw bewijst
vóór exact acknowledgement. De 23 gerichte en 231 brede tests zijn groen;
groundconfirmation, operationele facts en admission blijven false. Met Office
heeft dezelfde harde grens voor zijn vorige listing, actuele listing en ODIM-
object, met kwantitatieve decode en fysieke evidence-/artefactreproof vóór ack;
ook die output blijft factless. Meteorologische admission blijft voor alle
providers dicht.

Een permanent corrupt of contractueel onleesbaar lifecycle-event kan de
seriële streamhead niet langer onbeperkt vergiftigen. Lifecycle-schema v3
heeft daarvoor een expliciete permanente quarantaine met vijf gesloten,
eventgebonden redencodes. `QuarantineTerminalResult` bindt de immutable
QuarantineReceipt en de gewone ConsumptionReceipt in dezelfde transactie vóór
de cursor beweegt; het resultaat blijft semantisch `quarantined`, nooit
`acknowledged`. Tijdelijke capacity-, disk-, deadline-, cancellation-, busy- en
system-storefouten hebben geen quarantainecode en blijven retrybaar. Hot en
sealed archive replay, v1/v2-readercompatibiliteit, v3 child-first prune,
tamper en terminaltopologie zijn groen in 64 gerichte plus 87 brede tests.

De completion-outputretentie heeft nu een afzonderlijke provider-neutrale,
append-only archiefkern. Iedere seal bindt het volledige outputdocument, de
output- en processreceipt, event/producereceipt, routeprofiel en de canonieke
bronrow. De bron wordt tijdens commit opnieuw bewezen door een bij de
archiefstore vastgelegde reproofauthority; een caller kan dus niet per seal een
eigen verifier injecteren. DWD is expliciet `checksum_reproof_required`, omdat
zijn huidige outputstore nog geen no-update/no-delete-triggers heeft. De 34
gerichte tests, onafhankelijke 34-testherhaling en 96 aangrenzende regressies
zijn groen. Deze kern archiveert nog geen live providerstore en autoriseert
geen hot prune, bronverwijdering of reclamation.

Voor providerbrede offline replay is bovendien een immutable lokale
corpusauthority verzegeld. Zij bevat 9 content-addressed objecten en 8.621.601
ingebedde echte providerbytes voor NOAA ERR, METAR/AWC en GOES-18/19, plus
hashgebonden verwijzingen naar de bestaande DWD-, publieke KMI-WCS- en
Houston/RainViewer-reeksen. Het manifest
`99500ab54f19196c0115176f31115039ec9b992d3e34b54235e91d726e06d5e9`
is read-only heropend en slaagt voor tamper, padtraversal, symlink/reparse,
duplicate en cap-tests. KNMI RTCOR en Met Office RATE blijven afzonderlijk als
synthetische contractfixtures gelabeld omdat hun echte upstream-HDF5-bytes
niet retained zijn; zij leveren dus geen providerbyte- of skillbewijs.

De overige zestien cadenceworkloads blijven zichtbaar fail-closed. Hun gates
omvatten KMI-product-/delivery-/retentierecht, OPERA-rechten/geheugen,
RainViewer/API-rechten, ground-providerretentie/OAuth, Open-Meteo-activatie en
providerbrede quota-/terms-/retentieauthority,
EUMETSAT-credentials, dynamische GOES-GLM-memberrecovery en validation-only
providercontracten. Geen daarvan mag door een generieke fallback stil actief
worden. Blitzortung staat als aparte event-ingress buiten deze cadencetelling.

Open-Meteo heeft intussen een afzonderlijke inert/ongewirede exact-raw
forecastcapture. Die dwingt authority- en querybinding, exacte JSON-vorm,
units, UTC-cadence, responsecaps, raw-before-inspect en een canonieke
CC BY-attributionpolicy af; 67 gerichte en 298 gecombineerde tests zijn groen
(één platformskip). Een afzonderlijke duurzame usage-authority reserveert en
verbruikt exact `2*N` requestunits, bindt vier UTC-quotavensters, one-shot
fetchpermits, de 900-s cataloguscadence, een duurzame grens van vijf locaties,
terms-hercontrole, clockrollback, crash/CAS en fysieke storecaps; 30 gerichte,
97 gecombineerde capture/query/usage- en 211 brede tests zijn groen. Dit is
modelbegeleiding, nooit radar- of groundtruth. Activatie/child/recovery/router
blijven atomair buiten bereik totdat ook een vaste officiële modelbronauthority
en alle bijbehorende provenance- en recoverybindingen gesloten zijn.
De finale activatie-audit heeft dit bewust als machineleesbare blocker
verzegeld: `icon_eu` is niet globaal, `lightning_potential` is ICON-D2-only en
de JSON-response draagt geen verifieerbare model-ID. Activation, recovery en
terminalcatalogus bleven daardoor byte-inhoudelijk op exact 11/11/11; de
gecombineerde blokkermatrix is 308/308 groen.

Nog open vóór deze grens breed of operationeel gebruikt mag worden: duurzame
binding van de overige autonome providerplanning aan handlers/outbox en
serviceactivatie buiten de lokale DWD-only canary, raw captures voor alle
overige providers,
decode/fusion/history,
vNext tracking/forecast/ground/mapproductie, pure HA-sensor-/HTTP-consumptie,
replays, faultmatrix, integration-off versus candidate HA/Z2M-latentiesoak en
gefaseerde shadow/canary. Er is daarom geen live deploy of releaseclaim.

### 3.5 Finale lokale codefreeze van 2026-08-11

De lokale integration `1.13.0` en worker `1.16.0` zijn als reproduceerbare
releasekandidaat gebouwd, maar niet gedeployed. De volledige offline
testinventory telt 4,949 nodes uit 251 bestanden in 48 seriële batches:
4,934 passed, 15 verklaarde platform-/live-dataskips en nul failures, errors of
warnings onder `-W error`. De twee CloudFerro-livecases uit
`tests/test_opera_integration.py` zijn volledig buiten de inventory gehouden en
gelden niet als pass of skip. Het normatieve bewijs staat in
`ROADMAP_EXECUTION_STATUS_2026-08-11.md`.

De codefreeze sluit ook de lokale v2-bronreclamationketen: een
retention-backed current authority en vijf echte participants doorlopen
PREPARED -> COMMITTED -> APPLIED in de vaste lifecycle/execution/scheduler/raw/
capacityvolgorde. Reopen en exact replay hebben geen dubbel effect; registry-
epochdrift vóór COMMITTED faalt gesloten en wordt via abort plus successor
hersteld. Dit vinkte geen live gate af. Raw en lifecycle houden
`exclusive_filesystem_owner_dirfd_policy_required_for_crash_atomic_hardlink_exclusion`
open; servicewiring, productie-enable, doel-OS-power-loss, HA/Zigbee2MQTT-soak
en meteorologische admission blijven uit.

### 3.6 Post-RC1 lokale validatie vanaf 2026-08-11

De RC1-freeze en zijn artefactbytes blijven ongewijzigd en niet gedeployed. De
bevroren telling blijft 4,934 passed en 15 skips uit 4,949 nodes; al het bewijs
in deze paragraaf is aanvullend post-RC1-bewijs en wordt daar niet bij opgeteld.

De native vijfrollen-`os._exit`-matrix is na een volledige finale run 36/36
groen in `785.69s`; de aangrenzende native integratiematrix is 2/2 groen in
`31.29s`, beide zonder warnings. Daarop zijn de exact gepinde lokale
reclamationgrenzen aanvullend gesloten: 24 PREPARE-, 2 COMMIT- en 24
ABORT-cases zijn 50/50 groen in zeven disjuncte seriële shards (`1353.55s`),
gevolgd door 7/7 groene APPLIED replay-/reproofcases (`200.51s`).

De aanvullende provider-/capturematrices zijn groen voor AEMET (5), AWN plus
Open-Meteo (6), HSAF plus Météo-France (8) en DWD-capturehandoff plus
acquisition recovery (7), met de vastgelegde aangrenzende regressies. De
completion-/routermatrices zijn groen voor de archivekern (6), seriële
dispatcher inclusief gesloten-rootafwijzing (17), DWD/KNMI (23), METAR/Met
Office (20), NOAA (12) en raw-manifest-audit (9): samen 87/87 cases. Een
onafhankelijke statische completionaudit rapporteert nul P0-gaten en dekt alle
gevonden productiefaultcalls, inclusief de echte `os._exit(91)`-
archivecommitgrens.

Dit bewijs betreft lokale logische procescrashes, niet fysieke power-loss of
`fsync`-durability. De volledige P0-subprocessfaultmatrixcheckbox blijft open
door resterende acquisition- en integratiegrenzen en externe opslag-, live- en
soakgates. Resultaten, bronlinks en SHA-256-provenance staan in
[ROADMAP_EXECUTION_STATUS_2026-08-11_POST_RC1.md](ROADMAP_EXECUTION_STATUS_2026-08-11_POST_RC1.md).

Na de bevroren Phase-C-bronvalidatie is op 2026-08-14 ook een nieuwe,
afzonderlijk gehashte current-source CLI-simulatie uitgevoerd. Zij sloot beide
lokale output-authorityblockers, rapporteerde
`local_simulation_passed=true` en liet zowel de lokale als
Supervisor-blockernamespace leeg. De gebruikte volledige kandidaat is tweemaal
byte-identiek gebouwd, maar draagt voor deze local-only controle nog dezelfde
interne versies `1.13.0` en `1.16.0` als oudere RC1-bytes. Zij is daarom niet
deploybaar of distribueerbaar; integration `1.11.0` en worker `1.14.0` bleven
onaangeroerd als rollbackbaseline.

Er zijn geen live paden aangeraakt, geen netwerkacties uitgevoerd en niets
gedeployed. Zeven externe gates blijven open — doelinstallatie, data-/schema-
rollback, hostile same-user-races, menselijke deployapproval, runtime
stop/start, Supervisor-build/cache en power-loss op het doelbestandssysteem —
waardoor `real_installation_ready=false` blijft. De exacte artifact-, rapport-,
rollback- en optionshashes staan in het
[post-RC1-statussupplement](ROADMAP_EXECUTION_STATUS_2026-08-11_POST_RC1.md).

### 3.7 Lokale opt-in DWD-servicecanary vanaf 2026-08-13

De worker heeft nu een gesloten runtimekeuze met `legacy` als onveranderde
default. Alleen `worker_first_canary` met een canoniek configuratiepad, exacte
config-SHA-256 en bestaande authority-SHA-256 mag de afzonderlijke v2-root lazy
construeren. Die root laat exact `radar.dwd.radolan_rv` toe en sluit legacy
handlers, receipt-owner, scheduler-owner en procesexecutor uit.

De eerste lokale gerichte gate was in één strict run 204/204 groen in `73.15s`,
inclusief de echte loader,
authority, DWD-only builder, OS-fence, hermetische MQTT-fout, begrensde stop en
fence-heracquire. De exact uit `run.sh` geëxtraheerde gate is uitvoerend onder
Git Bash getest en bereikt de echte `service.main`/loader, maar er is geen
addon-image of lokale broker in deze omgeving. Daarom blijven
`production_enable_ready=false`,
`meteorological_admission=false`, science-shadow, factless, geen reclamation en
geen deploy- of liveclaim ongewijzigd.

De securityhardening van 2026-08-13 supersedet die eerste gate met een strict
gecombineerde canarymatrix van 242/242 groen. De v2-canary acquiret nu de
niet-wachtende OS-fence vóór iedere mutable storeconstructie of -audit; busy
fence betekent dus nul storeconstructie en nul storemutatie. Disconnect sluit
readiness, jobadmission en subscriptionauthority meteen. Een reconnect opent
pas opnieuw na verse exacte runtimediagnostiek die volledig aan de nieuwe MQTT-
transportgeneratie is gebonden; stale of generatie-overschrijdend bewijs wordt
afgewezen.

De DWD-only hard-processroute vernauwt de environment zowel aan de OS-
spawngrens als opnieuw in de child tot een vaste technische allowlist zonder
ambient MQTT- of providersecrets. `run.sh` unset in canarymodus vóór `exec` alle
niet-DWD-providersecrets en het legacy receiptpad. Legacy blijft de default en
zijn service- en hard-decodercontract zijn niet gewijzigd. Afzonderlijk zijn de
acquisitionruntime 93/93, de eerdere volledige hard-decoderfile 20/20 en de
toen afzonderlijke gerichte OS-spawn-boundary 1/1 groen. Daarna is de volledige
hard-decoderfile inclusief die regressie definitief als één matrix 21/21 groen
in `14.98s`. Packaging plus `.dockerignore` is 28 passed met één verklaarde
Windows-skip. Dit alles blijft post-RC1, offline en zonder deploy- of liveclaim.

---

## 4. Doelarchitectuur

```text
Provider/API/file
      |
      v
RawArtifactStore ---------> Fetch/parse audit
      |
      v
ObservationNormalizer ----> ObservationRecord + QualityFlags
      |
      +--------------------> GroundObservation
      |
      v
RadarFrameStore ----------> native frame + analysis-grid view
      |
      +---------> HierarchicalDetection
      |
      +---------> OpticalFlowField
                       |
                       v
                 AssociationGraph
                       |
                       v
                 CellTrackEngine
                       |
          +------------+-------------+
          |                          |
          v                          v
    LineageGraph                WeatherSystem
          |                          |
          +------------+-------------+
                       |
                       v
             ProbabilisticNowcast
                       |
                       v
               TrackerForecast
                       |
                       v
                GroundFusion
                       |
                       v
          ImmutableDecisionSnapshot
                       |
          +------------+-------------+
          |                          |
          v                          v
     HA sensors/map              Verification
```

### 4.1 Procesgrens

Home Assistant:

- beheert configuratie, targetlocaties en entiteiten;
- publiceert uitsluitend een kleine, begrensde desired-state wanneer
  configuratie of targetlocaties wijzigen;
- ontvangt en bewaart uitsluitend compacte, immutable operationele snapshots;
- toont status en diagnostiek;
- plant geen providerfetches of meteorologische jobs;
- opent geen providerverbindingen en schrijft geen meteorologische database;
- voert geen providerselectie, fusion, groundmatching, optical flow, ensemble-,
  raster-, tracking-, forecast- of GeoJSON-berekening uit;
- valt bij een onbereikbare worker fail-closed terug op `unknown/stale`, nooit
  op een lokale meteorologische berekening.

Worker:

- bewaart de desired-state crash-durable en plant zelf alle providercadences;
- haalt alle radar-, satelliet-, bliksem- en grondbronnen op;
- bewaart de originele ontvangen bytes en tijd-/bronprovenance vóór decode;
- decodeert en normaliseert zware bronproducten;
- bouwt analysis grids;
- detecteert cellen;
- berekent optical flow;
- voert associatie, tracking en ensemble-nowcast uit;
- voert providerfusion, groundmatching, forecastverificatie, kalibratie en
  GeoJSON-opbouw uit;
- schrijft immutable resultaten met checksums en publiceert alleen een compact
  operationeel snapshot naar Home Assistant;
- publiceert voor elke job exact één terminal state.

Database:

- bewaart raw provenance, observations, detections, tracks, forecasts en
  decisions;
- is de basis voor replay;
- wordt niet via sensorproperties gemuteerd;
- heeft expliciete schema- en migratieversies.

---

## 5. Data- en eventmodel

### 5.1 Identiteitsregels

- IDs zijn stabiel, niet afgeleid van een veranderend centroid.
- Providerrecord-ID plus provider, product, variabele, station en
  meetinterval vormt de natuurlijke deduplicatiesleutel.
- Wanneer een upstream-ID ontbreekt, gebruiken we een SHA-256 van de
  canonical raw inhoud plus bronmetadata.
- Een duplicate met exact dezelfde checksum is idempotent.
- Dezelfde natuurlijke sleutel met een andere checksum wordt niet
  overschreven maar krijgt een `source_revision`-event.
- `INSERT OR REPLACE` is verboden voor meteorologische feiten.
- Afgeleide tabellen verwijzen met foreign keys naar hun inputs.
- Algorithm-, parser-, calibration- en schema-versies zijn verplicht.
- Tijden worden als UTC epoch en ISO-8601 geëxporteerd; lokale tijd is alleen
  presentatie.

### 5.2 `RawArtifact`

```text
RawArtifact
  artifact_id
  provider
  product
  upstream_object_id
  fetch_id
  requested_at
  received_at
  nominal_valid_at
  content_type
  content_encoding
  byte_length
  sha256
  storage_uri
  native_projection
  advertised_resolution
  fetch_http_status
  fetch_headers_redacted
  parser_version
  credential_reference
```

Regels:

- `credential_reference` bevat alleen een alias, nooit een secret.
- Raw bytes worden vóór parsing gehasht.
- Wanneer licentie of volume raw opslag verhindert, bewaren we minimaal de
  exacte upstream object-key, checksum en decode-metadata.
- “Original radar frame” wordt alleen gebruikt wanneer het werkelijk raw
  upstreamniveau is; een provider-overlay heet expliciet
  `provider_overlay`.

### 5.3 `ObservationRecord`

```text
ObservationRecord
  observation_id
  artifact_id
  provider
  product
  provider_record_id
  variable
  observation_kind
  observed_at
  interval_start
  interval_end
  received_at
  latitude
  longitude
  elevation_m
  geometry
  native_value
  native_unit
  normalized_value
  normalized_unit
  categorical_code
  quantitative
  native_projection
  native_resolution_m
  quality_flags
  quality_index
  parser_version
  calibration_version
```

`observation_kind` onderscheidt minimaal:

- `radar_reflectivity`;
- `radar_rain_rate`;
- `radar_accumulation`;
- `satellite_rain_rate`;
- `lightning_event`;
- `ground_instantaneous_rate`;
- `ground_interval_accumulation`;
- `ground_present_weather`;
- `ground_dry_capable_observation`;
- `ground_unknown`.

Een categorische METAR `-RA` wordt nooit als fictieve `0.1 mm`, `0.1
mm/5min` en `0.1 mm/h` opgeslagen.

### 5.4 `RadarFrame`

```text
RadarFrame
  frame_id
  artifact_id
  provider
  product
  nominal_valid_at
  received_at
  native_grid_definition
  analysis_grid_definition
  data_variable
  unit
  nodata_mask_reference
  quality_grid_reference
  native_data_reference
  analysis_data_reference
  transform_version
  coverage_polygon
  completeness_fraction
```

Native data blijft leidend. De analysis-grid-view:

- mag de bron niet als fijner voorstellen dan de native resolutie;
- bevat een missing/nodata-mask;
- gebruikt een metrische, gedocumenteerde projectie;
- bewaart resamplingmethode en transformversie;
- gebruikt geen nulwaarde voor “geen data”.

### 5.5 `CellDetection`

```text
CellDetection
  detection_id
  frame_id
  detection_level
  parent_detection_id
  algorithm_version
  threshold_definition
  polygon
  holes
  centroid
  weighted_centroid
  area_km2
  perimeter_km
  orientation_deg
  compactness
  intensity_min
  intensity_p50
  intensity_p90
  intensity_max
  integrated_intensity
  texture_features
  quality_flags
  position_covariance
```

`detection_level` ondersteunt:

- rain shield;
- precipitation object;
- embedded core;
- convective candidate;
- low-confidence fragment.

Kleine objecten worden niet verwijderd. Ze blijven als
`low_confidence_fragment` beschikbaar.

### 5.6 `MotionField`

```text
MotionField
  motion_field_id
  input_frame_ids
  valid_from
  valid_to
  method
  method_parameters
  u_grid_reference
  v_grid_reference
  residual_grid_reference
  forward_backward_error
  support_mask_reference
  global_quality
  algorithm_version
```

Per detection wordt daarnaast bewaard:

- lokale mediaan `u/v`;
- robuuste spreiding;
- flow coverage binnen het object;
- residual;
- forward/backward consistency;
- covariance;
- fallbackreden.

### 5.7 Association en lineage

```text
AssociationCandidate
  association_id
  previous_detection_id
  current_detection_id
  predicted_polygon
  advected_iou
  symmetric_difference
  edge_distance_km
  mahalanobis_distance
  area_ratio
  intensity_similarity
  orientation_similarity
  flow_consistency
  source_continuity
  elapsed_seconds
  total_cost
  probability
  selected
  rejection_or_uncertainty_reasons
```

```text
LineageEvent
  lineage_event_id
  event_type
  event_time
  parent_track_ids
  child_track_ids
  supporting_detection_ids
  association_ids
  confidence
  algorithm_version
```

`event_type` is een van:

- `birth`;
- `continuation`;
- `temporarily_missing`;
- `reacquired`;
- `split`;
- `merge`;
- `death`;
- `provider_handoff`.

### 5.8 `CellTrack`

```text
CellTrack
  track_id
  lifecycle_state
  created_at
  ended_at
  end_reason
  parent_track_ids
  child_track_ids
  lineage_generation
  identity_confidence
```

```text
CellTrackSnapshot
  track_snapshot_id
  track_id
  detection_id
  valid_at
  x_m
  y_m
  vx_m_s
  vy_m_s
  state_covariance
  current_polygon
  area_trend
  intensity_trend
  source_history
  motion_method
  position_quality
  identity_quality
```

Een track met twee correcte detections:

- krijgt een exacte intervalverplaatsing tussen die detections;
- krijgt een voorlopige velocity;
- blijft `preliminary`;
- krijgt brede covariance;
- krijgt nog geen bewezen acceleration.

### 5.9 `WeatherSystem`

Een `WeatherSystem` is een actuele groepering van tracks, geen vervanging
ervan.

```text
WeatherSystemSnapshot
  system_snapshot_id
  system_id
  valid_at
  member_track_ids
  rain_shield_detection_ids
  aggregate_current_footprint
  descriptive_motion
  classification
  system_uncertainty
```

De system motion:

- is beschrijvend;
- mag niet naar alle membertracks worden teruggeschreven;
- wordt nooit gebruikt om onderlinge puppy-/celbeweging uit te wissen;
- mag alleen als prior/fallback dienen wanneer een individuele track
  onvoldoende eigen informatie heeft.

### 5.10 Forecastrecords

```text
NowcastRun
  nowcast_run_id
  initialization_time
  input_frame_ids
  input_track_snapshot_ids
  model
  model_version
  ensemble_size
  lead_times
  perturbation_configuration
  quality_flags
  runtime_seconds
  worker_build_id
```

```text
TrackerForecast
  forecast_id
  nowcast_run_id
  target_id
  target_location
  lead_time
  passage_probability
  rain_threshold_probabilities
  eta_p10
  eta_p50
  eta_p90
  eta_conditional_on_passage
  local_intensity_p10
  local_intensity_p50
  local_intensity_p90
  selected_track_id
  alternative_track_ids
  valid_horizon
  confidence_kind
  uncertainty_components
```

Een ETA zonder `passage_probability` is verboden. Wanneer een substantieel
deel van het ensemble niet passeert, is de ETA-verdeling conditioneel op
passage en wordt dat expliciet getoond.

### 5.11 Groundrecords

```text
PhysicalSite
  site_id
  latitude
  longitude
  elevation_m
  independence_group
  provider_aliases
  station_capabilities
  siting_metadata
```

```text
GroundObservation
  ground_observation_id
  observation_id
  site_id
  provider
  station_id
  sensor_id
  observed_at
  interval_start
  interval_end
  received_at
  measurement_kind
  value
  unit
  weather_code
  quantitative
  can_observe_dry
  provider_quality_flags
  raw_reference
```

```text
GroundMatch
  ground_match_id
  ground_observation_id
  track_id
  system_id
  radar_frame_id
  time_offset_seconds
  point_inside_current_polygon
  current_edge_distance_km
  predicted_edge_distance_km
  match_probability
  ambiguity
  reasons
```

```text
GroundDecision
  ground_decision_id
  target_id
  forecast_id
  selected_track_id
  evaluated_at
  contributing_ground_match_ids
  excluded_ground_observation_ids
  likelihood_components
  posterior_before
  posterior_after
  current_precipitation_state
  same_track_confirmed
  decision_version
```

### 5.12 Batch- en jobreceipt

```text
ObservationBatch
  batch_id
  region_id
  first_observation_time
  last_observation_time
  observation_ids
  created_at
  persisted_at
  processing_state
```

```text
ProcessingAttempt
  attempt_id
  batch_id
  job_id
  engine
  worker_id
  worker_build_id
  requested_at
  started_at
  terminal_at
  terminal_state
  result_checksum
  retry_of
  fallback_reason
```

Exact-once businesssemantiek wordt bereikt door:

1. at-least-once transport;
2. stable `batch_id`;
3. idempotente state transition;
4. resultaatchecksum;
5. atomic commit van state plus receipt;
6. retry zonder dubbele trackupdate.

De twee duurzame state-machines zijn expliciet en monotonisch:

```text
Ingest:
  RECEIVED
    genormaliseerde observation duurzaam; verwerking nog verplicht
      ├─> DEDUPED
      │     runtimeclassificatie duurzaam en terminaal; niet opnieuw aanbieden
      └─> BATCHED ─> COMMITTED
            observation zit in immutable batch ─> batch is toegepast

Batch:
  PREPARED
    immutable observation_ids, input_digest, region_id en start_revision vast
      └─> EXECUTING
            attempt met worker_id/job_id/startstate; recoverybaar
              └─> RESULT_STAGED
                    resultaat valide, maar nog niet zichtbaar
                      └─> COMMITTED
                            trackingstate plus receipt in één DB-transactie
  QUARANTINED
    input bewaard; reden, lineage, epoch en expliciet retrycontract aanwezig
  PARTITIONED
    parentbatch terminaal opgesplitst; duurzame childlineage bevat alle input
```

Schema v3 bewaart hier de genormaliseerde observation crash-durable. Het
originele providerartifact met volledige raw provenance hoort bij de vNext
`RawArtifactStore` en is nog niet door dit operationele journal vervangen.

Regels:

- alleen `COMMITTED` betekent meteorologisch toegepast;
- een MQTT-status, worker-“completed” of lokaal returnstatement is op zichzelf
  nooit een commitbewijs;
- `COMMITTED` is terminaal en idempotent: dezelfde `batch_id` retourneert het
  bestaande receipt zonder de trackstate opnieuw te muteren;
- `EXECUTING` met verlopen lease keert bij recovery terug naar `PREPARED`;
- `RESULT_STAGED` zonder corresponderende statecommit wordt bij recovery
  opnieuw gevalideerd en atomair gecommit of teruggezet naar `PREPARED`;
- een statecommit zonder receipt mag door één database-transactie niet kunnen
  bestaan;
- quarantaine verwijdert geen observation of aanwezige provenance;
- `PARTITIONED` verwijdert de parent niet en verliest geen observation tussen
  de duurzame children;
- operatorretry creëert een nieuwe `ProcessingAttempt`, niet een nieuwe batch;
- iedere overgang bewaart `previous_state`, timestamp, actor/build-ID en
  foutcategorie in een append-only auditregel.

---

## 6. Meteorologische methoden

### 6.1 Providerkwaliteit vóór providerprioriteit

Providerselectie mag niet alleen een statische voorkeurslijst zijn. Per frame
worden minimaal beoordeeld:

- producttype: reflectivity, rain rate, accumulation of afgeleid beeld;
- native resolutie;
- nominale tijd en observation age;
- fetch age en cache age;
- werkelijk gedekte footprint;
- completeness/nodata;
- officiële quality index waar beschikbaar;
- geolocatie- en projectiekwaliteit;
- radarbeam-/mosaicbeperkingen;
- bekende conversie naar rain rate;
- actuele parserhealth;
- discontinuïteit ten opzichte van vorige frames;
- empirische regionale performance.

De providerquality-score is een verklaarbare qualityscore, geen
meteorologische probability.

### 6.2 Providerhandoff

Een providerwissel is een measurement discontinuity.

Procedure:

1. Bewaar oude en nieuwe bron gedurende een overlapvenster wanneer beide
   beschikbaar zijn.
2. Reprojecteer alleen hun analysis views naar dezelfde fysieke grid.
3. Bepaal ruimtelijke registratie-offset met phase correlation/flow en
   controleer hem tegen polygonoverlap.
4. Vergelijk intensiteitsquantielen en missing masks.
5. Maak een `ProviderHandoffEvent`.
6. Laat bestaande trackstates voorspellen naar het handofftijdstip.
7. Match nieuwe detections tegen predicted polygons, niet tegen het ruwe oude
   centroid.
8. Vergroot covariance op basis van registratie- en productverschil.
9. Houd top-alternatieven bij wanneer continuïteit niet overtuigend is.
10. Markeer de track voorlopig totdat latere frames de associatie bevestigen.

Zonder overlap mag de engine continuïteit niet als zeker voorstellen.

### 6.3 Hiërarchische detectie

Eén threshold kan een brede regenband, embedded cores en losse convectieve
cellen niet tegelijk correct representeren.

De detection pipeline gebruikt daarom:

- een lage, product- en providergekalibreerde threshold voor het rain shield;
- één of meer hogere thresholds voor precipitation objects en cores;
- watershed/component-tree-separatie voor embedded maxima;
- 8-connectiviteit waar geometrisch passend;
- holes en MultiPolygons;
- area, perimeter, orientation, compactness en textuur;
- intensiteitspercentielen, niet alleen maximum;
- quality mask en native resolutie;
- een sensitivity run rond thresholds op de validatieset.

Thresholds worden niet blind tussen dBZ en mm/h gedeeld. Een rainrateproduct
en een reflectivityproduct krijgen elk een gedocumenteerde semantiek.

Minimumarea:

- verwijdert geen raw detection;
- bepaalt hoogstens `low_confidence_fragment`;
- wordt in km² berekend;
- wordt per native resolutie en use case gevalideerd.

### 6.4 Optical flow

Praktische eerste keuze:

- dense Lucas–Kanade via pySTEPS als kostenefficiënte baseline;
- VET als challenger op dezelfde replays;
- werkelijke timestampverschillen gebruiken;
- minimaal twee frames voor een eerste motion field;
- bij voorkeur drie of meer recente frames voor stabielere preprocessing en
  nowcast;
- twintig of meer frames gebruiken voor eventreplay en validatie, niet als
  harde operationele startvoorwaarde.

Preprocessing:

- gebruik dezelfde fysieke grid en mask;
- behandel nodata niet als nulregen;
- gebruik de transformatie die bij de gekozen pySTEPS-methode hoort;
- bewaar transformparameters;
- verwijder geen intense waarden zonder audit;
- controleer frame-registration vóór motion.

Motionkwaliteit:

- forward/backward consistency;
- residual na éénstapsadvectie;
- support coverage binnen de detection;
- lokale flowspreiding;
- vergelijking met trackvelocity;
- vergelijking met eenvoudig translation-/persistencebaseline;
- covariance die groeit bij ontbrekende of inconsistente frames.

Een regionale flow is:

- een prior voor jonge tracks;
- een fallback bij tijdelijke gaten;
- nooit een harde overschrijving van volwassen lokale tracks.

### 6.5 Persistente `CellTrack`

Eerste state estimator:

```text
state = [x, y, vx, vy]
model = constant velocity
uncertainty = volledige covariance
```

Waarom:

- constant velocity is uitlegbaar;
- twee punten ondersteunen geen betrouwbare acceleration;
- covariance maakt onzekerheid expliciet;
- een Kalmanachtige filter kan verschillende meetcovarianties verwerken;
- providerhandoff kan als grotere measurement covariance worden ingevoerd.

Acceleration:

- pas als extra model in een Interacting Multiple Model of vergelijkbare
  challenger;
- alleen na voldoende consistente trackhistorie;
- alleen wanneer hindcast/replay verbetering aantoont;
- nooit via een onbegrensde kwadratische 90-minutenextrapolatie;
- niet verborgen door een universele snelheids- of acceleratiecap.

Trackstatus:

- `candidate`: één detection;
- `preliminary`: twee plausibel geassocieerde detections;
- `tracked`: voldoende consistente frames;
- `ambiguous`: meerdere vergelijkbare associaties;
- `temporarily_missing`: één of meer gemiste frames binnen gapbeleid;
- `split_parent_ended`;
- `merge_parent_ended`;
- `ended`.

### 6.6 Globale associatie

Voor ieder bestaand track voorspellen we polygon en state naar het nieuwe
tijdstip.

Candidate cost combineert:

- advected intersection-over-union;
- symmetric difference;
- afstand tot polygonranden;
- Mahalanobisafstand van position state;
- area ratio;
- intensity-distributie;
- orientation;
- lokale flowconsistentie;
- broncontinuïteit;
- verstreken tijd;
- quality flags.

De één-op-één continuaties worden globaal opgelost met Hungarian of min-cost
flow. Splits en merges vragen daarnaast een expliciete eventgraph of
clusteroplossing; alleen Hungarian kan ze niet representeren.

Hard gates:

- mogen uitsluitend rekentijd begrenzen;
- moeten uit covariance, tijdsgat en meetresolutie volgen;
- mogen geen observation of detection verwijderen;
- moeten alle uitgesloten kandidaten met reden bewaren;
- mogen niet als universele `180 km/h`-natuurwet worden gecodeerd.

### 6.7 Split/merge-lineage

Gekozen identitybeleid:

- bij split eindigt de parenttrack;
- ieder child krijgt een nieuwe `track_id`;
- children verwijzen naar de parent;
- bij merge eindigen alle parenttracks;
- de merged detection start een nieuwe mergetrack;
- de mergetrack verwijst naar alle parents;
- een `WeatherSystem` kan een stabiele bovenliggende gebruikersidentiteit
  behouden.

Dit voorkomt dat een willekeurig grootste child of parent de volledige
historie van andere cellen erft.

Iedere eventbeslissing bewaart:

- inputdetections;
- overlap-/costcomponenten;
- geselecteerde en verworpen hypotheses;
- event confidence;
- algorithm version.

### 6.8 WeatherSystem als gecombineerde hondenlijn

De gebruikersmetafoor wordt als volgt gemodelleerd:

- de begeleider en het centrale aanknopingspunt zijn het `WeatherSystem`;
- iedere puppy is een `CellTrack`;
- alle puppies kunnen een gedeelde grootschalige verplaatsing hebben;
- iedere puppy behoudt daarnaast eigen lokale beweging, groei, krimp en
  levenscyclus;
- de systeembeweging is nooit een vervanging voor lokale trackbeweging;
- systeemfootprint is een actuele union/hierarchie, geen convex hull over
  historische posities.

System grouping gebruikt:

- actuele rain-shieldconnectiviteit;
- afstand tussen actuele polygons;
- coherente flow;
- gedeelde evolutie;
- tijdrelatie;
- eventueel expliciete MCS-classificatie.

### 6.9 Probabilistische neerslag-nowcast

Baselinevolgorde:

1. Eulerian persistence;
2. eenvoudig translation-/advectionmodel;
3. pySTEPS deterministic extrapolation;
4. S-PROG/STEPS met schaaldecompositie;
5. LINDA als challenger voor lokale intense regen en growth/decay.

We bouwen geen eigen stochastic generator voordat deze baselines op onze
providers en regio’s zijn gemeten.

Ensembleperturbaties omvatten:

- motionvector;
- optical-flow-residual;
- position/geolocation;
- shape;
- growth en decay;
- intensiteit;
- providerbias;
- ontbrekende frames;
- handoffonzekerheid.

Publicatie per tracker:

- kans op enige neerslag per lead-timevenster;
- kansen op vooraf gekozen lokale intensiteitsthresholds;
- passage probability;
- ETA p10/p50/p90 conditioneel op passage;
- lokale intensiteit p10/p50/p90;
- geselecteerde track en alternatieven;
- geldige empirische horizon;
- onzekerheidsredenen.

De forecast gebruikt waarden op/om de trackerlocatie. Het maximum van een
volledig WeatherSystem is geen lokale intensiteitsverwachting.

### 6.10 Convectieve initiatie en predictability

Pure radaradvection kan een nieuwe cel die nog niet bestaat niet voorspellen.

Totdat een apart, gevalideerd initiatiemodel bestaat:

- meldt de engine deze beperking expliciet;
- daalt passage confidence met lead time en convectiviteit;
- wordt de geldige horizon empirisch verkort;
- betekent “geen geadvecteerde echo” niet automatisch “zeker geen regen”;
- kunnen bliksem, satelliet en NWP alleen als aparte evidence/candidate
  worden gebruikt;
- mag geen auxiliary bron zonder kalibratie als radarregen worden behandeld.

### 6.11 Ground fusion

Ground fusion begint bij semantiek:

- METAR present weather is categorisch;
- METAR `Prrrr` is een intervalaccumulatie;
- Synoptic `precip_intervals` draagt begin/einde en bronvariabele;
- Netatmo live/afgeleide waarden dragen module-ID en echte meettijd;
- missing is `unknown`, niet `dry`;
- een nulmeting is alleen dry evidence wanneer die sensor dat tijdvenster
  werkelijk kon meten.

Procedure per ground observation:

1. identificeer fysieke site en independence group;
2. valideer capability, QC en freshness;
3. bewaar het exacte meetinterval;
4. vergelijk met het tijdsuitgelijnde actuele radarframe;
5. gebruik point-in-polygon en echte edge distance;
6. bepaal mogelijke trackmatches en ambiguity;
7. bereken empirische likelihoodbijdrage;
8. groepeer uitsluitend per geselecteerde track/lineagetak;
9. bewaar uitgesloten en gebruikte evidence;
10. update een posterior, niet de raw radarstate.

Statuscontract:

- `rain_observed_at_target`;
- `rain_observed_near_target`;
- `same_track_ground_supported`;
- `same_track_ground_confirmed`;
- `radar_ground_disagreement`;
- `no_capable_ground_data`;
- `ground_data_stale`;
- `unknown`.

Een station op 3 km is niet automatisch bewijs dat het op de tracker regent.
De representativiteitsafstand volgt uit stationtype, radarresolutie,
position uncertainty en empirische validatie.

Stationweights:

- worden niet handmatig als eindprobability gepresenteerd;
- bevatten provider-QC, leeftijd, meettype en historische betrouwbaarheid;
- corrigeren voor dubbele providers van dezelfde fysieke site;
- corrigeren voor gecorreleerde stations/netwerken;
- worden op held-out data gekalibreerd.

### 6.12 Uncertaintybudget

Iedere forecast bewaart afzonderlijke onzekerheidscomponenten:

- measurement uncertainty;
- radar geolocation/resolution uncertainty;
- missing-data uncertainty;
- detection-threshold uncertainty;
- association uncertainty;
- track-state covariance;
- optical-flow uncertainty;
- growth/decay uncertainty;
- providerbias;
- providerhandoff uncertainty;
- ground sampling/representativeness;
- station quality;
- model calibration uncertainty;
- staleness en queue delay.

Heuristische qualityscores heten `quality_score`. Alleen empirisch
geverifieerde kansen heten `probability` of `confidence_percent`.

---

## 7. Uitvoeringsfasen en tickets

## Fase P0 — verlies en actuele verkeerde conclusies stoppen

Doel: de huidige installatie mag tijdens de herbouw geen echte batches,
detections of relevante ground evidence stil verliezen en mag geen andere
storm bevestigen.

### P0-L01 — Lossless Observation Fusion Engine

**Status:** durable ingest, `DEDUPED`-classificatie,
batch-/quarantainelineage, restartrecovery en cumulatieve
delivery-accounting zijn lokaal geïmplementeerd en getest, inclusief abrupte
subprocesscrashes. `offered` wordt voor een observation alleen bij opname in
de delivery-liability geteld en voor een batch alleen bij het openen van de
root; retries en splits bieden die root dus niet opnieuw aan. Een root geldt
pas als delivered wanneer al zijn leaf-observations gecommit zijn. Alleen live
HA/MQTT/addon/power-loss, providerdruk en langdurige soak blijven open.

Werk:

- atomisch batch snapshotten;
- bij callbackerror/cancellation de batch vóór nieuwere pending observations
  terugplaatsen;
- succesvolle batch exact eenmaal verwijderen;
- reset expliciet als bewuste region teardown behandelen;
- side-effectvrije, cumulatieve metrics voor offered, delivered, requeued,
  discarded en lost voor zowel observations als rootbatches;
- een batchbegrensde precommit-snapshot herstelt inhoud en volgorde wanneer
  een callback de lijst muteert en daarna faalt of geannuleerd wordt;
- aparte activitycounters voor failure-retry, cancellation, onverwachte
  `BaseException`-requeue, split, cooldown, manual retry en recovery;
- reset als expliciete discardtransactie: pending en reeds gebatchte
  observations worden apart geteld en nooit als `lost` gemaskeerd;
- vaste O(1)-counters en alleen tijdelijke diagnostische sets over de reeds
  resident aanwezige queue; geen onbegrensde historie van batch-ID's. De
  counters zijn cumulatief over region-resets binnen één engine-runtime;
  restartrecovery opent in de nieuwe runtime één recovery-root per root-ID en
  het durable journal blijft de autoriteit over procesgrenzen.

Tests:

- callbackerror;
- cancellation tijdens callback;
- observation tijdens callback;
- reset tijdens sleep en callback;
- twee gelijktijdige manual flushes;
- event-loop shutdown;
- deterministische root-/retry-/split-/cancellation-/recovery-/resettests;
- seeded randomized property-run van 36 cycli met concurrente add/flush,
  transient retries, callbackcancellation, poison-splits, manual recovery en
  reset vóór en tijdens callback.

DoD:

- `lost_batches_total == 0`;
- observation-invariant: `offered == delivered + discarded + lost + outstanding`;
- rootbatch-invariant: `offered == delivered + discarded + lost + outstanding`;
- volgorde behouden;
- geen duplicate delivery na success;
- een failure is zichtbaar;
- volledige test- en soaktest groen.

### P0-L02 — Worker local fallback

**Status:** lokaal exact-once worker-/fallbackcontract geïmplementeerd en
gericht getest; live MQTT-, restart- en late-resultaatmatrix blijft open.

Trackingprotocol v5 bindt een workerresultaat aan de exacte `batch_id`,
`start_revision`, observation count/hash en startstate count/hash. De response
bevat `result_revision = start_revision + 1`, het aantal snapshots en een
SHA-256 over de volledige, canoniek gesorteerde snapshots. HA herberekent al
die waarden en past het resultaat uitsluitend via compare-and-swap op de
ongewijzigde startrevision toe.

Bij timeout, transportfout, queue-full, workererror, protocolmismatch of een
inhoudelijk ongeldig resultaat blijft de gepubliceerde beginstate ongewijzigd
en verwerkt de lokale engine exact dezelfde batch met dezelfde batch-ID. Die
lokale mutatie gebeurt op een working copy; een precommitfout kan daardoor
geen reeds gepubliceerde `Storm`-handle half wijzigen. Een laat
workerresultaat kan na een lokale commit de nieuwere revision niet
overschrijven.

Werk:

- alleen een volledig gevalideerde en toegepaste workersnapshot retourneert
  `processed=True`;
- timeout, malformed result, workererror of disconnect geeft `False`;
- `RegionManager` verwerkt exact dezelfde scoped batch lokaal vanaf de nog
  ongewijzigde startstate;
- resultaatchecksum en startstatechecksum bewaren.

Tests:

- worker timeout;
- queue-full;
- invalid contract;
- verkeerde regio;
- verkeerd observation count;
- ontbrekende snapshots;
- replace-state-error;
- brokerdisconnect;
- workerrestart;
- late worker completion na lokale fallback.

DoD:

- iedere batch eindigt in workercommit of één lokale commit;
- nooit beide commits;
- nooit geen commit;
- late resultaten kunnen committed state niet overschrijven.

### P0-L03 — Geen `MAX_STORMS`-drop

**Status:** plan en lokale RegionEngine-wiring zijn deterministisch
geverifieerd; dure nowcastconsumptie en live resource-/thermische soak blijven
open.

Werk:

- capaciteit nooit als ingestfilter gebruiken;
- alle detections persistent bewaren;
- dashboard en rekenscheduler mogen prioriteren;
- actieve trackstate kan tiering gebruiken: relevant, background,
  low-confidence;
- eviction beëindigt alleen compute state met audit, niet de detectionrecord.

Tests:

- >15 gelijktijdige objects;
- 15 ruisobjects gevolgd door een bui boven target;
- worker/HA parity;
- memory/CPU soak;
- backgroundtrack opnieuw relevant maken.

DoD:

- relevante zestiende detectie blijft beschikbaar en forecastbaar;
- geen observation/detectionverlies;
- performancebeleid is zichtbaar en reproduceerbaar.

Lokale invulling:

- `MAX_STORMS` is geen ingest- of evictionpad en bepaalt hoogstens de
  voorkeursgrootte van compute-pass 0;
- `build_compute_schedule()` bevat altijd alle tracking- en historyrecords en
  markeert iedere track als vereiste forecastinput;
- targetafstand gebruikt de meest recente radarfootprint en valt zichtbaar
  terug op centroidafstand;
- tierorde, tie-breaks, policyfingerprint, schedulefingerprint, counts,
  canonieke documentgrootte en positieve invarianten zijn gepubliceerd;
- targetrelevantie gaat vóór confidence, zodat een nieuwe lokale bui niet
  achter verre, oudere objecten belandt;
- tiering wordt per call opnieuw berekend en muteert geen state;
- de gerichte 96-objectmicroreplay bewaart 96/96 tracks, histories, exports en
  forecastinputs, evalueert deterministisch 192 track-targetparen en levert
  bij vijftien voorkeursslots zeven volledige passes;
- RegionEngine publiceert na targetwijziging, gecommitteerde batch en
  persistente restore een revision-bound schedulesnapshot;
- een 30-objectruntimebatch bewaart 30/30 tracks en forecastinputs over twee
  voorkeurspasses;
- sensorreads tonen een begrensde vooraf berekende snapshot zonder state te
  muteren;
- een interne planningsfout publiceert zichtbaar execute-all en laat geen
  track of reeds gecommitteerd batch verdwijnen;
- HA en worker zijn byte-identiek.

De reproduceerbare contractdetails en beperkingen staan in
`docs/P0_L03_COMPUTE_PRIORITY_POLICY.md`. De microreplay is bewust geen
wall-clock-, RSS-, NUC-temperatuur- of thermalthrottlingclaim.

### P0-G01 — Point-in-polygon en edge distance

**Status:** gedeelde rasterketen lokaal geïmplementeerd en gericht
geverifieerd; OPERA-, echte-data-, resource- en livevalidatie blijven open.

Werk:

- gedeelde geometriebibliotheek voor HA en worker;
- point-in-polygon;
- afstand tot segmenten;
- punt- en lijnfootprints;
- Polygon/MultiPolygon met holes;
- actuele CRS/latitudebehandeling documenteren.

Tests:

- target binnen brede polygon;
- target op rand;
- buiten nabij midden van lange edge;
- holes;
- MultiPolygon;
- duplicate closing point;
- één/twee punten;
- hoge latitude en antimeridiaan;
- New York-replay.

DoD:

- binnen betekent exact nul afstand;
- HA en worker bit-equivalent binnen tolerantie;
- geen vertex-only codepad meer.

### P0-G02 — Ground confirmation per storm/track

**Status:** geïmplementeerd in werkboom, verificatie gedeeltelijk open.

Werk:

- groupeer evidence per exact `storm_id` in legacy;
- in vNext per `track_id`/lineagetak;
- stations van verschillende storms nooit optellen;
- meerdere passende storms als ambiguity bewaren;
- bevestigde ID expliciet publiceren.

Tests:

- twee natte stations bij twee verschillende storms;
- twee stations bij dezelfde storm;
- één station binnen overlappende footprints;
- nat station buiten geselecteerde footprint;
- andere bui boven tracker;
- duplicated station via twee providers.

DoD:

- cross-storm confirmation is onmogelijk;
- ground evidence blijft wel regionaal zichtbaar;
- gebruikte IDs en redenen staan in decisionrecord.

### P0-W01 — Terminal workerstatus

**Status:** lokaal technisch geïmplementeerd en gericht geverifieerd; echte
proces-, transport-, opslag- en power-lossvalidatie blijft open.

Werk:

- queue-full -> `failed(queue_full)`;
- identificeerbare invalid request -> `failed(invalid_request)`;
- expired -> terminal `expired`;
- één strikt seriële queue met één persistent `spawn`-childproces;
- absolute jobdeadline stopt of killt een geblokkeerd childproces;
- childcrash faalt alleen de actieve job en de volgende job start in een
  schoon childproces;
- lazy handlerimports en childcache blijven tussen normale jobs behouden;
- shutdown sluit admission, terminaliseert active en queued jobs en wacht de
  dispatcher uit vóór offline-status en MQTT-disconnect;
- accepted/terminal/completed/failed/expired/cancelled-counters zijn
  reconcileerbaar;
- eenmaal terminale status kan niet door een laat queued/running-bericht
  worden teruggedraaid;
- HA bridge kent alle terminal states;
- jobfuture altijd opruimen;
- ieder productiejobtype heeft exact één immutable en import-time
  gecontroleerd effectcontract;
- parent-only secrets behoren niet tot requestdigest, receipt of repr;
- Netatmo-refresh en credentialrotatie committen fail-closed, met `0600`,
  file-fsync, atomic replace en Linux directory-fsync, vóór childdispatch;
- recursieve redactie verwijdert gevoelige keys, bekende secretwaarden,
  Bearer- en token-querypatronen vóór MQTT, receipts en logging;
- refresh start alleen met budget voor zowel OAuth als de begrensde
  childquery.

Tests:

- queue-full;
- invalid JSON met en zonder recoverable job-ID;
- invalid compressed request;
- expired voor queue en na dequeue;
- duplicate;
- broker reconnect;
- status die voor result arriveert;
- result die na HA-timeout arriveert;
- handler die voorbij de deadline blokkeert;
- childcrash gevolgd door een succesvolle volgende job;
- cachebehoud tussen normale jobs;
- shutdown met actieve én queued jobs;
- late niet-terminale publicatie na terminal status;
- ontbrekend, onbewezen of onveilig effectcontract faalt gesloten;
- corrupte of niet-duurzaam opgeslagen tokenstate faalt vóór providerquery;
- broker-/access-token ontbreekt uit MQTT, requestdigest, receipt en ruwe
  SQLite-bytes;
- directory-fsync en volledig refresh-plus-querydeadlinebudget.

DoD:

- ieder geaccepteerd of identificeerbaar job-ID krijgt exact één terminal
  outcome;
- geen future wacht tot timeout nadat terminal status bekend is;
- handlerhealth is correct;
- parent-/containercrash kan na herstart de duurzame jobreceipt en terminale
  toestand reconstrueren;
- een hard afgebroken handler kan geen externe side effect onzichtbaar
  verdubbelen of verliezen;
- status publiceert vereist contractbereik, verificatie, effectklasse,
  voorbereiding en compensatie.

### P0-G03 — Actuele footprint los van historische hull

**Lokale shadowbasis:** de matcher accepteert een lossless immutable
`polygons`-structuur met per polygon exact één outer ring en nul of meer holes.
De volledige geometrie wordt in één bolgeometrie-aanroep gematcht; een holte
wordt dus niet langer als een zelfstandig nat vlak behandeld. De bestaande
`rings`-input blijft compatibel maar betekent expliciet afzonderlijke
MultiPolygoncomponenten. Ongeldige of ambigue topologie faalt gesloten. De
gedeelde rasterketen vult deze vorm nu door Observation, cell/frame,
durable/restartstate en HA/workertracking; RainViewer behoudt losse
parentcomponenten. De afzonderlijke legacy OPERA-contourtracer,
operationele producerbinding en New York-live replay blijven open.

Werk:

- `current_footprint` als union/hierarchie van detections op huidige nominal
  time;
- `history_envelope` uitsluitend voor visualisatie/analyse;
- `forecast_corridor` apart;
- groundmatch gebruikt alleen tijdsuitgelijnde current/predicted footprint.

DoD:

- station in oude positie bevestigt huidige track niet;
- lege ruimte in convex hull bevestigt niet;
- velden zijn afzonderlijk zichtbaar.

### P0-S01 — Sensorreads zonder side effects

Werk:

- alle auditrecords in expliciete coordinator/updatepipeline creëren;
- sensorproperties lezen alleen immutable snapshots;
- pending auditlijsten worden niet door dashboardreads uitgebreid;
- beslissings-ID blijft gelijk bij herhaald lezen.

DoD:

- honderd propertyreads veranderen database, pending lists en decision count
  niet.

### P0-D01/P0-D02 — Durable ingest en idempotente batches

Actuele tussenstand: het lokale schema v3-journal implementeert stabiele
`batch_id`, persistente observations, attempts, transitions, quarantaine,
permanente regio-identiteit, versioned trackingstate en crash-durable
commitreceipts. State en receipt committen atomair met revision-CAS.
Per-regio routeadmission opent na herstel van die regio; daarna controleert
startup de globale journalintegriteit en pas na succes starten providers. Een
directe routecaller kan technisch al in het korte interval vóór die globale
controle binnenkomen. Een globale admissionbarrier of later
`recovery_ready=True` zetten blijft daarom een P0-releasegate. Een echte
subprocessmatrix breekt het proces met `os._exit` af op twaalf afzonderlijke
SQL-checkpoints en bewijst na reopen oude-of-volledig-nieuwe state. Dit sluit
de lokale codegrenzen, maar niet de nog vereiste live HA/MQTT/addon/power-loss,
migratie-, disk-full- en corruptieproeven; daarom mag de kandidaat nog niet
live.

Lokaal gerealiseerd:

- genormaliseerde observation persist vóór zware verwerking;
- stable `batch_id`;
- SQLite-tabellen voor `observation_batch`, `processing_attempt`,
  `batch_state_transition` en versioned tracking snapshot;
- unique constraint op `(engine_version, region_id, batch_id)`;
- processing lease met monotone deadline en wall-clock auditwaarde;
- canonical input-, startstate- en resultdigest;
- `BEGIN IMMEDIATE`: verwachte revision controleren, nieuwe trackingstate
  schrijven, revision verhogen en receipt `COMMITTED` maken vóór `COMMIT`;
- pas na geslaagde DB-commit de immutable in-memory snapshot en HA-events
  publiceren;
- retry/fallback idempotent;
- startup recovery vóór provideradmission: verlopen leases vrijgeven,
  `PREPARED`/herstelbare `RESULT_STAGED` FIFO hervatten en pas daarna nieuwe
  polls toelaten;
- quarantaine, lineage-epochs, partitionchildren en operatorretry duurzaam
  opslaan.

Nog open:

- volledige raw providerartifacts content-addressed bewaren in schema vNext;
- compaction uitsluitend van aantoonbaar `COMMITTED` receipts nadat raw
  retention en reproduceerbare replay zijn geborgd;
- expliciete migratie en rollback voor iedere pre-release schemaversie;
- foutinjectie voor volle schijf, I/O-fouten en SQLite-corruptie;
- echte HA-, addon-, broker- en host/power-lossmatrix;
- langdurige live soak met recovery- en resourcetelemetrie.

Lokaal bewezen:

- exceptionfaults plus abrupte `os._exit` vóór/na iedere receive- en
  commitgrens verliezen geen persisted observation;
- iedere geteste SQL-crashgrens levert na recovery uitsluitend de oude of de
  volledig nieuwe state, nooit een hybride;
- replay toont exact welke batch nog verwerking nodig heeft;
- duplicate transport maakt geen duplicate trackupdate;
- twee gelijktijdige committers voor dezelfde startrevision leveren precies één
  winnaar; de verliezer leest de nieuwe revision en verwerkt niet dubbel;
- HA start geen provider vóór recoverybacklog en receiptintegriteitscontrole
  afgerond zijn.

De volledige DoD is pas gehaald wanneer dezelfde eigenschappen ook in de live
proces-, opslag- en transportomgeving bewezen zijn.

### P0-C01 — Geen meteorologische afkeur op vaste snelheidsgrenzen

**Status:** lokaal geïmplementeerd en gericht getest; echte-data- en
resourcevalidatie blijven open.

Werk:

- vaste meteorologische afkeurgrenzen van 180, 160, 120 en 100 km/h zijn uit
  associatie, preflight en virtuele-probepaden verwijderd;
- afstand en snelheid worden op de bol berekend uit de feitelijke posities en
  tijdstempels;
- een eindig, niet-negatief resultaat blijft een meting, ook wanneer de
  snelheid uitzonderlijk hoog is;
- een begrensd zoekvenster is uitsluitend een computationeel budget: een
  oplossing op de rand wordt `motion_indeterminate` en niet “meteorologisch
  onmogelijk”;
- de policy is gelijkgetrokken tussen HA en worker.

Tests:

- twee bekende posities met een uitzonderlijk hoge verplaatsingssnelheid;
- ontbrekende, gelijke en ongeldige tijdstempels;
- antimeridiaan en hoge breedte;
- randcensurering van het zoekbudget;
- HA-/workerpariteit;
- regressie met circa 880 km/h zonder stille afkeur.

DoD:

- geen vaste snelheidswaarde kan een echte observatie verwijderen;
- snelheid en onzekerheid blijven afzonderlijke velden;
- onvolledige zoekruimte wordt zichtbaar als onbepaald;
- een immutable snelle echte replay en NUC-soak bevestigen dat ruimere
  computationele zoekruimte HA niet onresponsief maakt.

### P0-F01 — Actuele neerslag en volgende dreiging zijn verschillende feiten

**Status:** getypeerd lokaal contract aanwezig; producer-, opslag- en
runtimewiring worden nog integraal geverifieerd.

Werk:

- `current_precipitation` beschrijft uitsluitend neerslag die de actuele,
  tijdsuitgelijnde trackerfootprint raakt;
- `nearby_precipitation` beschrijft een nabij object zonder actuele overlap;
- `next_threat` verwijst naar een afzonderlijk voorspeld object, tijdvenster
  en onzekerheid;
- radar-, grond- en forecastbronnen behouden elk hun eigen timestamp,
  dekking, kwaliteit en reden;
- ontbrekende grondbron betekent `unknown`, niet automatisch droog;
- HA-sensoren mogen een nabije of volgende bui nooit als actuele regen tonen.

Tests:

- bui boven de tracker;
- bui vlak naast de tracker;
- huidige bui plus andere naderende bui;
- radar beschikbaar terwijl een grondprovider ontbreekt;
- grondconflict en verouderde meting;
- serialisatie, herstel en HA-/workerpariteit.

DoD:

- ieder zichtbaar veld heeft precies één tijds- en ruimtelijke betekenis;
- actuele status kan niet door `next_threat` worden overschreven;
- provideruitval verlaagt de bewijsstatus zichtbaar zonder een vals droog- of
  natbesluit;
- immutable replays voor New York en ten minste één andere tracker bevestigen
  de semantiek end-to-end.

### P0-R01 — Geordend afsluiten zonder ingest-/reset-race

**Status:** lifecyclepad lokaal afgeschermd; volledige provider- en
procesfaultmatrix blijft open.

Werk:

- nieuwe lifecycle-admission sluit vóór shutdown;
- actieve lifecyclefetches worden geannuleerd en providercycluslocks worden
  uitgewacht;
- pas daarna volgen calibrationflush, OFE-reset en stateopslag;
- shutdown gebruikt de awaited Home Assistant-job-API;
- durable receipts en geaccepteerde batches bepalen na herstart wat hervat,
  opnieuw aangeboden of terminaal gemaakt moet worden.

Tests:

- stop tijdens providerfetch;
- stop tijdens queueverwerking en statecommit;
- herhaald starten/stoppen;
- late providercallback na admissionclose;
- herstart met open lease of staged resultaat;
- geen reset vóór alle toegelaten producers gestopt zijn.

DoD:

- geen geaccepteerde observatie verdwijnt door een normale shutdown;
- reset en stateflush kunnen niet met een actieve producer racen;
- alle niet-lifecycleproviders vallen onder hetzelfde admissioncontract;
- addon-, broker-, HA-, host- en storagecrashes zijn end-to-end gereplayed.

### P0 releasegate

Fase P0 is pas klaar wanneer:

- [x] alle lokale gerichte tests na de laatste integratiewijziging groen zijn;
- [x] de volledige lokale eindsuite na **alle** roadmapwijzigingen opnieuw
  groen is met warnings als errors: 4,934 passed en 15 verklaarde skips uit
  4,949 nodes; nul failures, errors of warnings. De twee uitgesloten
  CloudFerro-livecases zijn geen onderdeel van deze telling;
- [ ] de volledige exception- en abrupte subprocessfaultmatrix na de laatste
  provider-, router- en reclamationwijzigingen opnieuw groen is;
- [x] continuïteit boven dertig onafhankelijke tracks lokaal is getest;
- [x] workerstatus na lokale queue-/reject-/deadlinepaden herstelt;
- [x] een globale startup-admissionbarrier blokkeert ook directe routecallers
  tot de journalintegriteitscontrole geslaagd is;
- [ ] geen batchverlies optreedt in een langdurige live soaktest;
- [ ] de New York polygon-/groundcase met immutable echte input correct
  replayt;
- [ ] de lokale deterministische 96-objectmicroreplay met relevante cellen en
  ruisobjecten is verliesvrij; een representatieve live NUC-replay moet nog
  queue, CPU, RSS en thermalthrottling bewijzen;
- [ ] addon-/broker-/HA-/hostcrashes en diskfouten de durable contracten
  end-to-end bevestigen;
- [ ] HA responsief blijft en live sensoren de recoverydiagnostiek tonen;
- [ ] migratie-, deploy- en rollbackartefacten zijn gebouwd en droog getest.

---

## Fase P1-A — immutable ingest en schema vNext

### P1-D01 — Nieuwe append-only database

Maak naast de legacytabellen een vNext-schema. Destructief leegmaken van de
kalibratiedatabase is geen migratiestrategie.

**Lokale shadowbasis:** `raw_artifact_store.py` bewaart exacte payloadbytes
content-addressed met SHA-256 in een eigen SQLite-database met schema v2.
`store()` schrijft één blob plus immutable providerreceipt atomair.
`store_set()` valideert en commit een volledige multi-artifactset — alle
blobs, memberreceipts, setreceipt en memberships — in één
`BEGIN IMMEDIATE`-transactie. De setreceipt bindt een canonieke manifesthash,
callerprovenance, memberaantal, bytegrootte en iedere memberrol. Abrupte
subprocesskills op zeven transactiegrenzen leveren na reopen uitsluitend de
oude of de volledig nieuwe set.

Schema-admission controleert de canonieke DDL en de volledige allowlist van
tabellen, indexen, views en triggers. Onbekende, extra, oude, toekomstige of
corrupte schema's falen gesloten; gelijktijdige eerste initialisatie door acht
threads en zes processen is getest. Vlak vóór commit worden receipt-ID,
metadatahash, exacte payloadbytes en volledige set opnieuw uit de transactie
gelezen. Dezelfde upstream identiteit is idempotent en kan niet stil aan
andere bytes, nominale tijd, media type, parserversie, membership of provenance
worden gebonden.

`provider_artifact_contracts.py`, `artifact_retention_planner.py` en
`artifact_capture_admission.py` leggen secretvrije bron-, quota-,
licentie-/HOLD- en capacitybeslissingen vast. De admissionbrug controleert de
exacte request/decisionbinding en gebruikt voor multi-artifacts uitsluitend
`store_set()`. Processing blijft altijd toegestaan: raw storage op HOLD,
ARCHIVE_REQUIRED of REJECT_CAPACITY verwerpt geen weer.

Er zijn nog geen volledige live providerwiring, atomische quotareservering,
duurzame admissionjournal, archiverings-/restore-/delete-executor of echte
ENOSPC/WAL/SHM/power-/mediafaultproeven. Schema v1 wordt bewust niet inline
gemigreerd; vóór toekomstig gebruik van een bestaande v1-store is een
afzonderlijke offline migrator met bron-/doelchecksums, restore en rollback
vereist. De huidige `original_radar_frames` blijven genormaliseerde overlays
en mogen niet als upstream raw payload worden voorgesteld.

Tickets:

- `P1-D01a`: raw artifact table en content-addressed storage;
- `P1-D01b`: observationrecord en stable IDs;
- `P1-D01c`: frame/native/analysis references;
- `P1-D01d`: detection/motion/association/track/lineage tables;
- `P1-D01e`: nowcast/forecast tables;
- `P1-D01f`: physical site/ground match/decision tables;
- `P1-D01g`: batch/job receipts;
- `P1-D01h`: foreign keys, indexes en schema migrations;
- `P1-D01i`: conflict/revision events in plaats van replace.

DoD:

- iedere forecast is via foreign keys tot raw artifacts herleidbaar;
- database-integritycheck groen;
- duplicate replay wijzigt feiten niet;
- schema kan vanaf lege én bestaande installatie migreren;
- rollback leest legacy zonder vNext te verwijderen.

### P1-D02 — Retention en opslagbudget

Data wordt in tiers beheerd:

- raw hot: recente operationele periode;
- raw archive: gecomprimeerd per provider/datum;
- normalized frames: langere replayperiode;
- detections/tracks/forecasts: langdurig;
- aggregates: onbeperkt volgens beleid.

**Lokale shadowbasis:** de pure retentieplanner berekent STORE, HOLD,
ARCHIVE_REQUIRED of REJECT_CAPACITY uit een expliciete policy, inventory en
capacitysnapshot. De capture-size-audit meet de bestaande database zonder ze
te wijzigen en toont dat de historische `original_radar_frames` overlays zijn,
geen upstream raw. De 30-dagen-/jaarprojectie is bewust alleen rekenkundig en
geen retentievoorspelling zolang er geen representatieve volledige dagen en
revisionhistoriek zijn.

**Offline uitbreiding 2026-08-11:** v1 sealed export blijft een forensische,
archive-aware herstelbron en op zichzelf nooit een delete-token. V2 bindt nu
een append-only consumerregistry, actuele epoch/root, gepinde historische
archivebytes en dezelfde vijf sourceparticipants. De echte lokale
vijfrollenintegratie bewijst child-first reclamation, exacte capacityrelease,
permanente semantische tombstones, stale-epochweigering, abort/successor en
APPLIED replay na reopen. De flow is niet aan service of deployment gekoppeld.
Exclusief filesystemownership tegen externe hardlinkcreatie, geaudite
directorydurability, echte power-loss/disk/mediafaults, streaming of harde raw-
processisolatie en NUC-resource-/thermal soak blijven releasegates.

Tickets:

- dag-/providerpartities;
- storagequota;
- disk free-space alarms;
- archiveringsmanifest met checksums;
- restoretest;
- no-delete hold voor gouden replays.

DoD:

- voorspelde databasegroei per dag zichtbaar;
- geen onbegrensde `original_radar_frames`;
- volle disk stopt ingest gecontroleerd en zichtbaar, niet stil.

### P1-D03 — Contract v2/vNext

Het huidige observation contract wordt uitgebreid met:

- echte `observed_at` en `received_at`;
- intervalstart/einde;
- product/variabele/eenheid;
- categorical versus quantitative;
- providerrecord-ID;
- raw reference;
- quality flags/index;
- native projection/resolution;
- parser/calibration version.

DoD:

- HA en worker gebruiken dezelfde canonical schemafixtures;
- contractpariteit wordt in CI getest;
- onbekende velden zijn forward-compatible;
- breaking change vereist contractversion bump.

---

## Fase P1-B — providersemantiek en groundrecords

### P1-P01 — Provideradaptercertificaat

Voor iedere radarprovider — onder meer KMI, KNMI, DWD/RADOLAN, Met Office,
OPERA, RainViewer en NOAA-producten — komt een certificaat met:

- upstream productnaam;
- autoritatieve documentatie;
- native resolution/cadence/projectie;
- variable en unit;
- valid-timebetekenis;
- missing/nodata;
- QC-velden;
- transformaties vóór opslag;
- raw opslagniveau;
- bekende beperkingen;
- testfixture met checksum;
- parser owner/version.

**KMI-audit:** officiële KMI-documentatie bevestigt dat native RADQPE het
gewenste kwantitatieve 1-km-product in Belgian Lambert 2008 is. De actuele
publieke WCS is wel anoniem en vijfminuutactueel, maar declareert uitsluitend
`GRAY_INDEX`, een niet-meteorologische stralingseenheid, EPSG:4326-resampling
en geen bruikbare embedded quantity-, tijd- of nodatabinding. Zij mag daarom
niet als RADQPE of mm/u worden geïnterpreteerd. App-v4 blijft een
gedecommissioneerde categorische visualisatie. Voor native RADQPE ontbreken
nog een openbaar delivery-/authcontract, uitvoerbare limieten en eenduidige
productie-/retentierechten. `docs/KMI_OFFICIAL_DELIVERY_GATE.md` bewaart de
live officiële endpointaudit, voorbeeldhash en minimale promotieroute. Tot
schriftelijke bevestiging blijven publieke WCS `unknown_quantity`, native
RADQPE `external_contract_blocked` en meteorologische admission gesloten.

DoD:

- geen provider heet “origineel” zonder bewezen raw niveau;
- iedere analysis-grid pixel is naar native input herleidbaar;
- providerwijziging triggert contract/replaytests.

### P1-P02 — Providerqualityruntime

**Lokale shadowbasis:** `provider_quality_runtime.py` beoordeelt ieder frame
met expliciete callergewichten voor freshness, completeness, upstream-QC,
geolocatie, echocontinuïteit, operationele gezondheid, held-out
replayperformance en handoff-readiness. Een stale maar technisch gezond frame
blijft bewaard en auditbaar, maar is niet eligible als huidige beste bron.
Ontbrekende positief gewogen evidence geeft geen hergenormaliseerde score maar
een fail-closed onvolledige assessment. Een echte top-score én valid-time-tie
wordt `TIED`, niet opgelost via verborgen providerprioriteit. De twee
implementatiemirrors zijn exact en acht strikte tests zijn lokaal groen. Live
componentwiring, empirisch gekozen gewichten/freshnesscurves en shadowsoak
blijven open.

Maak providerquality een meetbare runtimefunctie:

- frame age;
- completeness;
- quality index;
- geolocation consistency;
- echo continuity;
- parser/fetch errors;
- empirische replayperformance;
- handoff readiness.

DoD:

- providerselectie publiceert scorecomponenten en reden;
- fallback wordt niet alleen door “HTTP werkte” bepaald;
- stale maar technisch gezonde data geldt niet als actuele beste bron.

### P1-P03 — Handoff-engine

**Lokale shadowbasis:** `provider_handoff.py` bouwt een immutable,
content-addressed `ProviderHandoffEvent` met beide bronframes, geldigheid,
providerpaar, expliciete registratietransform en -covariance, overlap- en
intensiteitsdiagnostiek, missing-maskvergelijking, alle
continuïteitskandidaten en reason-codes. Cross-provider displacement wordt
niet stil als velocity geïnterpreteerd. Zwak of ontbrekend overlapbewijs wordt
`provisional` of `indeterminate`; detections en alternatieven blijven bewaard.
HA en worker zijn byte-identiek en de gecombineerde relevante testset telt 62
strikte tests. Zie `docs/PROVIDER_HANDOFF_SHADOW_ENGINE.md`. Echte lossless
overlapcaptures, onafhankelijk geverifieerde transformaties,
providerpaar-/regiocalibratie, held-out lineagelabels, shadowsoak en expliciete
assignment-/estimatorwiring blijven open.

Implementatie volgens sectie 6.2.

DoD:

- providerwisselcase heeft geen kunstmatige velocityspike;
- trackcontinuïteit is voorlopig wanneer bewijs zwak is;
- handoffevent is volledig replaybaar;
- continuitymetric is beter dan legacy.

### P1-G01 — Netatmo semantiek

- echte `measure.res`-/module-timestamp;
- module-ID en gekozen veld;
- instantaneous versus afgeleide waarde;
- geen pseudo-5-minutenmeting zonder label;
- freshness op observed time;
- lage-kostensensor-QC;
- worker-owned OAuth-ingest wordt nooit door ontbrekende legacy
  HA-providerobjecten geblokkeerd;
- nul ontvangen stations blijft `onvoldoende_data` en is geen droge
  tegenwaarneming.

De live veldcase en de lokale admissionreparatie zijn vastgelegd in
`docs/NETATMO_WORKER_ADMISSION.md`.

### P1-G02 — Synoptic semantiek

- bronvariabele en sensorset bewaren;
- ieder interval met begin/einde;
- accumulated versus interval/rate scheiden;
- provider-QC en SUMMARY bewaren;
- geen maximum als tegelijk rain rate en 5-minutenaccumulatie gebruiken;
- station-ID’s en age distribution per cycle.

### P1-G03 — METAR semantiek

- raw text en weather groups bewaren;
- present weather categorisch;
- intensity/proximity/descriptors apart;
- `Prrrr` en andere accumulaties met interval;
- `VC`, begin/einde, `PWINO`, `PNO` en maintenanceflags;
- freshness voor actuele bevestiging veel korter dan historische opslag;
- geen neerslagcode als fictief volume.

### P1-G04 — Physical site registry

**Lokale shadowbasis:** `physical_site_catalog.py` levert een immutable,
content-addressed en tijdsversiegebonden catalogus. Adaptervoorstellen voor
site-ID of independence group worden genegeerd; alleen expliciet geverifieerde
provideraliasmapping resolveert een site. Nabijheid is uitsluitend auditcontext,
coördinaatdrift buiten caller-tolerantie faalt gesloten, moves/replacements
gebruiken niet-overlappende versies en exacte ICAO/STID-correlatie merge nooit
zelfstandig sites. Capabilities, siting, autoritatieve identifiers en mappings
vereisen provenance. HA en worker zijn byte-identiek; 20 catalogustests en 60
gecombineerde catalogus/adapter/fusiontests zijn strikt groen. Echte
autoritatieve crosswalks, regionale toleranties, historische sitegegevens,
duurzame decision-wiring en globale shadowreplays blijven externe/live
poorten.

- stationaliases van METAR, Synoptic, MADIS en andere doorleveringen;
- geospatiale plus metadata-match;
- independence group;
- capability per sensor;
- moved/replaced sensor als versioned site event.

DoD voor P1-G01 t/m G04:

- dezelfde fysieke ASOS-site telt eenmaal;
- iedere waarde heeft bronveld, meettijd, interval en unit;
- stale observations blijven opgeslagen maar wegen nul voor actueel;
- parser kan historische raw records opnieuw interpreteren.

---

## Fase P1-C — detection, optical flow en `CellTrack`

### P1-M01 — Analysis grid service

**Lokale shadowbasis:** `analysis_grid_transform.py` zet uitsluitend een
checksum- en semanticsgebonden native `mm/h`-kandidaat om naar één expliciet
caller-supplied metrisch `AnalysisGrid`. De eerste gesloten adapter accepteert
alleen gecertificeerde north-up exact-affine pixelranden. Conservative
area-overlap behoudt de rate-oppervlakte-integraal; echte nul, `nodata`,
`undetect`, gemengde steun en buitengebied blijven afzonderlijke immutable
maskers met exhaustieve oppervlaktefracties. Alle bron-QC blijft behouden en
doel-QC ontstaat alleen bij exact area-consensus. Certificaat, config,
roundtripreceipt, pixelaccounting en output zijn content-addressed. De lus is
begrensd op pixels, bytes, overlapbijdragen en één absolute deadline, ook
binnen één zeer grove doelcel. DWD RV heeft nu het eerste echte
providercertificaat: de officiële DWD-DE1200-definitie en ODIM_H5 2.3 binden
1100×1200, 1 km, noord→zuid/west→oost en de exacte native buitenranden aan de
checksumgebonden DWD-kandidaat. Het certificaat staat uitsluitend een target in
de byte-identieke DWD-native CRS toe en kan dus geen reprojection veinzen. De
KNMI RTCOR-gate blijft daarentegen expliciet dicht: officiële KNMI-bronnen
publiceren strijdige ellipsoïdes en leveren nog geen sluitend pixel-edge- en
oriëntatiebewijs. Buiten de expliciete lokale DWD-only servicecanary is er geen
providerbrede scheduler-, service-, HA- of live wiring;
providercertificaten voor de overige bronnen, cross-providerreprojection,
hard-childisolatie en echte multi-event providerreplays blijven verplichte
poorten.

- metrische region grid;
- provider-native resolution respecteren;
- masks en quality grid;
- deterministic transforms;
- transformchecksum;
- worker-only zware verwerking.

DoD:

- coordinate roundtripfout maximaal een halve native pixel;
- nodata blijft nodata;
- identieke input maakt identieke gridchecksum.

### P1-M02 — Hiërarchische detector

Implementatie volgens sectie 6.3.

**Lokale shadowbasis:** connected rain shields en embedded cores blijven
afzonderlijk. Minimumarea is nu uitsluitend een auditeerbare
`low_confidence_fragment`-classificatie; geen enkel boven-threshold component
wordt nog verwijderd. Footprint-afgeleide detectie-ID's blijven bovendien
stabiel wanneer een los fragment bijkomt. Polygonen met gaten,
watershed/component trees, aanvullende vorm-/textuurkenmerken en
threshold-sensitivity op echte held-out events blijven open gates.

De detector accepteert nu bovendien één absolute monotone deadline en een
expliciet maximumaantal verliesloze objecten. Raster-, component-, footprint-
en kenmerklussen controleren die grens periodiek. Overschrijding geeft een
zichtbare deadline- of capaciteitsexception en nooit een gedeeltelijk frame.
De ongewirede `evidence_to_cells_shadow.py` bewijst end-to-end dat alleen
geldige rates naar deze detector gaan, terwijl nul, nodata, undetect, mixed en
buiten-dekking volledig auditbaar blijven. Parent-owned hard process isolation
is nog niet op deze compositor aangesloten en blijft dus releasegate.

DoD:

- alle finite precipitation pixels zijn als background/detection
  verklaarbaar;
- kleine objecten blijven geregistreerd;
- brede band plus embedded cores worden afzonderlijk gerepresenteerd;
- threshold sensitivity staat in validatierapport.

### P1-M03 — Optical-flowservice

- Lucas–Kanade baseline;
- VET challenger;
- actual elapsed time;
- quality/residual/covariance;
- missing-framebeleid;
- resultcache per frame-ID-set en algorithm version.

DoD:

- éénstapsadvectie verslaat Eulerian persistence op primaire held-out cases;
- slechte flow wordt als onzeker gemarkeerd;
- nooit een motionfield uit frames met incompatibele grids zonder registratie.

### P1-T01 — Associationgraph

**Lokale shadowbasis:** `association_graph.py` bouwt voor twee expliciete
metrische detectieframes deterministisch de volledige bipartiete graph. Ieder
paar blijft aanwezig, inclusief extreme intervalsnelheden. De caller moet de
motion-uncertainty expliciet leveren; de geometrische Gaussian NLL is alleen
een rangschikkingsfeature en wordt nergens als probability of identiteitskeuze
gepubliceerd. HA-output, legacy tracking en schema zijn niet gekoppeld.
Een deterministische 24-frame contractreplay met twee bekende synthetische
cellen bewaart alle 92 edges en houdt ook kruis-candidates boven 1000 km/u
auditbaar. Omdat de fixture de identiteit vooraf kent, bewijst dit de
verliesvrije graph en vectorschatting, niet de nog ontbrekende globale
assignment of prestaties op echt weer.

- predicted polygons;
- candidate features/cost;
- globale one-to-one assignment;
- expliciete split/mergehypotheses;
- top-alternatieven;
- reason codes.

DoD:

- geen universele 180-km/h-reject;
- extreme candidate blijft auditbaar;
- ID-switches en fragmentation dalen tegenover legacy;
- ambiguity wordt niet als zekerheid opgeslagen.

### P1-T02 — `CellTrack` state estimator

**Lokale shadowbasis:** `cell_track.py` definieert immutable
`CellDetection`/`CellTrackSnapshot` op één metrisch analysis grid. Na een
extern genomen associatiebeslissing produceren exact twee posities hun echte
intervalvector `[vx, vy]` met 4×4 statecovariantie. De byte-identieke
`recursive_track_estimator.py`-mirrors leveren daarnaast een expliciet
white-acceleration constant-velocity filter over alle geassocieerde frames.
Elke revisie bewaart zowel de ongefilterde laatste-twee-positiesnelheid als de
recursieve state, Joseph-covariantie, innovation en normalized innovation.
Geen snelheid of grote innovation wordt afgewezen; de recursieve schatter is
een shadow-challenger en wordt niet als betere waarheid voorgesteld zonder
held-out motionlabels. Graph en schatters zijn nog niet aan live ingest,
duurzame snapshots of UI verbonden.

Een eerste volledig ongewirede `CellTrackEngine` composeert deze primitives nu
end-to-end over meerdere frames. Shields en cores blijven afzonderlijke
tracks; last-two intervalmotion, Lucas–Kanade cold start, volledige
cross-productfeatures, exacte top-k-assignment, recursive covariance,
gap/reacquisition en expliciete split/merge-lineage lopen door één immutable
transition. WeatherSystems zijn uitsluitend een niet-terugschrijvende
afgeleide view. Extreme snelheid of innovation blijft kandidaat; overschrijding
van een computationele bound retourneert de volledige detectieset en
ongewijzigde state als `indeterminate`. De 16 nieuwe enginetests en een
onafhankelijke gecombineerde matrix van 116 trackingtests zijn groen. Dit is
nog synthetisch shadowbewijs: kostenkalibratie, provider/gridhandoff,
schema-3-durability, echte meerregioreplays en NUC-soak blijven gesloten.

- constant-velocity state;
- measurement covariance;
- preliminary/tracked/ambiguous/gapstates;
- fitted velocity werkelijk gebruiken;
- acceleration alleen challenger;
- immutable snapshots.

DoD:

- twee posities produceren correcte intervalvelocity plus brede covariance;
- velocity komt uit één consistente estimator;
- missing frames vergroten uncertainty;
- providerhandoff vergroot measurement covariance.

### P1-T03 — Immutable lineage

**Lokale shadowbasis:** `cell_track_lifecycle.py` bevat nu immutable,
caller-geïdentificeerde events voor `birth`, `missed`, `gap`, `reacquired`,
`death`, `split` en `merge`, met echte framegeldigheidstijd, redenen,
detectionrefs, ledgerrevision en betrokken trackrevisions. De atomische
assignmentmodus valideert alle eventmetadata vóór een nieuwe ledger wordt
gebouwd. Een gap/reacquisition behoudt dezelfde track-ID en bevat geen
snelheids-, afstands- of innovation-afkeur; ook extreme maar reeds
geassocieerde posities blijven auditbaar. HA en worker zijn byte-identiek en
36 strikte tests zijn groen. Een storeschemamigratie, event-ID-idempotentie,
track+event-transactie, crash/exact-once replay en live caller blijven
verplichte poorten.

- birth/death/split/merge/gap/reacquired;
- parent en child IDs;
- gekozen identitybeleid;
- graph integrity;
- lineagevisualisatie voor replay.

DoD:

- ieder split-/mergegoldencase correct;
- geen cycli in lineagegraph;
- parents/children wederzijds consistent;
- iedere eventbeslissing reproduceerbaar.

### P1-T04 — WeatherSystem secondary grouping

**Lokale shadowbasis:** `weather_system_grouping.py` is exact gespiegeld in HA
en worker. Iedere CellTrack blijft immutable en exact eenmaal aanwezig.
Pairwise evidence bewaart connected, uncertain en separate relaties; gedeeld
actueel rain shield houdt de gecombineerde hondenlijn bij elkaar terwijl
lokale puppybeweging en conflicten zichtbaar blijven. De actuele footprint is
de exacte pixelunion op één grid/tijdstip, system motion is alleen
beschrijvende rain-shield-/membermediaan en birth/continue/membership
change/merge/split-lineage is deterministisch en lossless. Geen speed- of
trackcountcap, geen MCS- of forecastclaim en nog geen live wiring.

- actuele membertracks;
- rain shield en actuele union;
- descriptive motion;
- geen historische convex hull voor actuele grondmatch;
- system ID afzonderlijk van track IDs.

DoD:

- lokale trackmotion blijft intact;
- brede systemen springen niet door puppybeweging;
- samenvoeging van systemen verwijdert geen cell lineage.

---

## Fase P1-D — probabilistische nowcast en lokale trackeruitkomst

### P1-N01 — Deterministische baselines

Implementeer en versioneer:

**Lokale shadowbasis:** `nowcast_baselines.py` levert Eulerian persistence en
two-frame constant-motion op een expliciet `AnalysisGrid`, met echte
timestamps, nodatamask en strict-nodata semi-Lagrangiaanse interpolatie.
De baselines maken nooit probabilities en bevatten geen speed cap, smoothing
of verborgen afwijzing. pySTEPS-extrapolation en S-PROG ontbreken nog.

**Lokale native-fieldbrug:** `canonical_radar_field_bridge.py` vormt nu een
ongewirede, fail-closed brug van raw-hergeverifieerde
`CanonicalEvidenceStore`-receipts en checksumgebonden decoderartefacten naar
`candidate_only_unadmitted` native rain-ratevelden. Het exacte
provider/product/decoder/configcertificaat, quantity/unit, tijdvenster, native
CRS/grid/coverage, nodata/undetect en QC blijven behouden. KNMI RTCOR en DWD RV
worden uitsluitend bij exact 300 seconden als
`interval_mean_precipitation_rate` omgerekend; NOAA ERR blijft instantane
satelliet-QPE; Met Office houdt zijn nominale RATE, niet-uniforme inputvenster
en ontbrekende QC zichtbaar. OPERA DBZH is hard uitgesloten zonder afzonderlijk
Z-R-certificaat. Er is geen reprojection, resampling, thresholding, wet/droog,
admission of wiring. De 19 gerichte/isolation-tests zijn groen met
ResourceWarning-as-error. Open blijven harde child-isolatie, exact gevalideerde
analysis-gridtransformatie/reprojectie voor niet-metrische of niet-exact
geankerde native grids, held-out meteorologie, multi-frame ingest, NUC-soak en
iedere operationele vrijgave.

- persistence;
- twee-frame translation;
- pySTEPS extrapolation;
- optioneel S-PROG deterministic.

DoD:

- iedere vNext-nowcast wordt tegen dezelfde baselines geverifieerd;
- geen verbetering wordt zonder baselineclaim vrijgegeven.

### P1-N02 — STEPS-ensemble

**Lokale haalbaarheidsaudit:** Python 3.13 valt volgens de actuele officiële
pySTEPS-documentatie binnen de geteste Pythonreeks, maar de Alpine-worker mist
nog meerdere verplichte en optionele dependencies. `num_workers > 1` vereist
een expliciete Dask/pyFFTW-route; de documentatie adviseert daarbij
`OMP_NUM_THREADS=1`. Alleen een hogere `cpu_threads`-waarde instellen maakt de
huidige seriële Pythonjob dus niet multicore en zes threads zijn niet langer
een toegestane productieoptie. Iedere toekomstige interne parallelisatie moet
binnen het totale tweethreadbudget en de worker-affinity blijven. Eerst is een
afzonderlijk pinned image, import-/runtime-/RSS-benchmark en
crash-/deadlineproef vereist.

- cascade levels en thresholds per product valideren;
- motion- en precipitation-noise;
- ensembleleden intern multicore;
- reproduceerbare random seeds voor replay;
- compacte probabilityproducten terug naar HA.

DoD:

- Brier Skill Score positief tegenover relevante baseline op de primaire
  lead times;
- reliability en sharpness gerapporteerd;
- ensembleconvergentie versus ledenaantal bekend;
- HA ontvangt geen onnodige volledige cubes.

### P1-N03 — LINDA challenger

- alleen na betrouwbare baselines;
- test op lokale intensieve cellen/banden;
- growth/decay en predictability loss;
- kosten versus skill.

DoD:

- alleen activeren in strata waar held-out skill aantoonbaar beter is;
- automatische fallback naar eenvoudiger model bij onvoldoende input.

### P1-N04 — Trackerforecastcontract

**Lokale shadowbasis:** `tracker_forecast_contract.py` scheidt expliciet
no-passage van conditionele ETA en lokale intensiteit. Passagekansen worden
alleen geaccepteerd als exacte ensemblefrequentie of als versiegebonden
empirische calibratie; ontbrekend bewijs wordt INDETERMINATE en nooit 0%,
droog of een fictieve ETA. `forecast_verification.py` levert eventgewogen
Brier/BSS, reliability, sharpness, false/missed passage, conditionele
ETA-metrics, ROC/AUC, masked FSS en eventbootstrap zonder releasegrenzen te
verzinnen. Dezelfde shadowmodule berekent exacte empirical-ensemble-CRPS,
ensemble-mean MAE/bias en een tie-safe rankhistogram, maar uitsluitend wanneer
de volledige uitwisselbare membervector en onafhankelijke scalar truth
beschikbaar zijn. Ze reconstrueert die metrics uitdrukkelijk niet uit enkele
kwantielen. Live memberopslag en echte onafhankelijke labels blijven open
validatiepoorten.

- current precipitation los van next threat;
- passage probability;
- threshold probabilities;
- conditionele ETA-verdeling;
- lokale intensiteitsverdeling;
- alternatives en provenance;
- dynamische geldige horizon.

DoD:

- nooit een systeemmaximum als trackerintensiteit;
- ETA mag nieuw sterker bewijs niet door continuity smoothing negeren;
- no-passage ensembleleden correct verwerkt;
- stale input maakt forecast `unknown/stale`.

### P1-N05 — Empirische calibratie

**Lokaal gerealiseerde onderzoeksbasis:** de event-held-out
validatiecontracten zijn fail-closed en een externe-annotatiepacker kan uit een
immutable read-only legacy- of vNext-SQLite-snapshot een content-addressed pack
met lege, geblindeerde templates maken. Database-, schema-, request-, protocol-,
split-, evidence-, missing-evidence- en template-inhoud worden cryptografisch
gebonden en bij ingest semantisch opnieuw afgeleid. De tooling maakt zelf geen
labels, kiest geen marges en toont kandidaatoutput niet als waarheid. Zie
`docs/EXTERNAL_ANNOTATION_PACK.md`.

Daarnaast levert `event_probability_calibration.py` een byte-identieke
HA-/workershadowimplementatie van event-gebalanceerde PAV-isotonic
calibratie. Fit accepteert uitsluitend confirmed, onafhankelijk eligible
trainlabels met verschillende kandidaat-/truthartifacts en een immutable
corpus- en splithash. Ieder meteorologisch event krijgt totaalgewicht één,
ongeacht het aantal cases. Minimumsteun voor events, positieve events en
negatieve events is verplicht callerbeleid zonder defaults. Exacte en bewust
gepoolde strata zijn mogelijk, maar de fallbackvolgorde wordt volledig door de
caller aangeleverd en geaudit. Held-out evaluatie weigert train/testlekkage en
rapporteert event-gebalanceerde raw en calibrated Brier loss zonder zelf een
releasegrens te kiezen. Elf strikte tests zijn lokaal groen; zie
`docs/EVENT_PROBABILITY_CALIBRATION.md`. Echte onafhankelijke passage-events,
preregistratie, supportgrenzen, driftbeleid en shadow/live forecastwiring
blijven open.

- reliability per provider/regio/lead/threshold;
- isotonic/logistic calibrator alleen waar samplegrootte voldoende is;
- hiërarchische fallback voor dunne strata;
- calibration version;
- held-out evaluation;
- driftmonitoring.

DoD:

- `confidence_percent` heeft een reliabilitybetekenis;
- qualityscore en probability zijn gescheiden;
- calibratie traint nooit op dezelfde events als release-evaluatie.

---

## Fase P1-E — ground fusion vNext

### P1-F01 — Ground matcher

- tijdsuitlijning;
- current polygon;
- predicted polygon;
- edge distance;
- match probability;
- ambiguity;
- exact track-/system-ID.

### P1-F02 — Ground likelihoodmodel

- capability;
- freshness;
- meettype;
- provider-QC;
- site reliability;
- spatial representativeness;
- independence group;
- positive en negative evidence;
- radar-ground disagreement.

### P1-F03 — Decision engine

- posterior before/after;
- evidence uitsluitend per geselecteerde track;
- local observed versus same-track-confirmed;
- immutable decision;
- explanationcodes.

### P1-F04 — Ground verification

**Lokale shadowbasis:** `ground_verification.py` laat uitsluitend confirmed,
onafhankelijk eligible `WetDry`-truth toe met exacte event-, frame-, site- en
meetintervalkoppeling. Unknown en unavailable blijven afzonderlijk;
sensitiviteit, specificiteit en coverage worden eventgewogen en per
provider/capabilitystratum gerapporteerd. Exacte duplicate-sitecases en
cross-trackgebruik worden geaudit, en tegenstrijdige truth voor dezelfde
evaluatie-eenheid faalt gesloten. HA en worker zijn byte-identiek; 17 gerichte
en 53 gecombineerde ground-/validatietests zijn strikt groen. Zie
`docs/GROUND_VERIFICATION_FOUNDATION.md`. Cross-stormcorrectheid kan nog niet
empirisch worden bewezen omdat WetDry-labels geen causale cel-/trackidentiteit
leveren; onafhankelijke lineagelabels en echte held-out events blijven dus een
releasegate.

- leave-one-station/site-out;
- provider/networkstrata;
- wet/dry confusionmatrix;
- calibration;
- duplicated-sourcecases;
- virga/radar-ground disagreementcases.

DoD voor P1-E:

- cross-storm confirmation altijd nul;
- duplicated site telt eenmaal;
- stale/missing telt niet als actueel;
- nabij station overschrijft trackerstatus niet alsof het colocated is;
- dry evidence alleen van capable sensor;
- iedere decision volledig replaybaar.

---

## Fase P2 — operationalisering en verfijning

### P2-O01 — Observability en SLO’s

Zie sectie 11. Alle kernmetrics worden als machineleesbare counters,
histograms en compacte HA-diagnostiek gepubliceerd.

### P2-O02 — Adaptive compute zonder dataverlies

- queue blijft één zware job tegelijk verwerken;
- één job gebruikt meerdere cores voor ensembleleden/tiles;
- een korte temperatuurpiek tot 100 °C is geen zelfstandige foutconditie;
- alleen werkelijk gemelde throttling, deadline-/geheugendruk of oplopende
  queueleeftijd mag een volgende job expliciet gedegradeerd configureren;
- een reeds gestarte gedeeltelijke ensembleberekening wordt nooit als volledig
  ensemble of geldige probability gepubliceerd;
- lage-prioriteitsreplay bij bewezen resource-/queuedruk uitstellen;
- live raw ingest en persistence blijven doorgaan;
- degraded mode expliciet publiceren.

### P2-O03 — Gzip/decompressiebeveiliging

**Lokaal gerealiseerde shadowbasis:** een exact gemirrorde pure
bounded-payloadlaag
begrenst gzip/zlib-output tijdens decompressie, accepteert exact één stream
en weigert truncatie, concatenated members en trailing data. MQTT-jobs,
rasterartifacts, DWD/OPERA-workerartifacts, DWD-tarmembers, H SAF,
kalibratieoverlays, METAR en alle actieve HTTP-providerpaden gebruiken het
contract. Het volledige OPERA-composiet heeft een expliciet 384 MiB-contract;
ook de losse dekkingssurveys zijn begrensd. Productspecifieke documentatie,
live groottepercentielen en observability bij overschrijding blijven open.

- compressed-size limit;
- streaming uncompressed limit;
- ratio/timeout;
- geen `gzip.decompress` van onbegrensde input vóór limiet;
- terminal invalid status.

### P2-O04 — Future timestamps en clock health

**Lokaal gerealiseerde shadowbasis:** toekomstige en stale METAR-, Netatmo- en
Synoptic-grondmetingen worden niet meer stil verwijderd of als actuele meting
gebruikt. De oorspronkelijke meetinhoud, interval-/moduletijd, QC-context,
gesigneerde leeftijd en policyfingerprint blijven in
`quarantined_observations`; een ontbrekende Netatmo-meettijd wordt expliciet
`timestamp_estimated` en kan niet stemmen. De exact gemirrorde fail-closed
groundadapter maakt van `usable_for_current_decision=false` nooit natte of
droge evidence. Een exact gemirrorde, providerneutrale tijdkwaliteitslaag
bewaart daarnaast transportdelay, future-at-receipt/evaluation en
receipt-clock-skew. Iedere toegelaten skew is callerbeleid met hash;
toekomstige tijden worden alleen bruikbaar met vers onafhankelijk
`SYNCED`-klokbewijs. Radar-/satellietbrede wiring, duurzame evidence en echte
NTP-/HA-/workerdiagnostiek ontbreken nog.

- future observation markeren;
- toegelaten clock skew;
- NTP/HA/worker clock diagnostics;
- geen negatieve age stil naar nul normaliseren.

### P2-O05 — Source-aware deduplicatie

- provider/product/station/variable/interval-aware;
- volgorde-onafhankelijk;
- revision events;
- correlated source is geen independent evidence.

### P2-O06 — Dashboard en uitleg

Techniekpagina toont:

- actuele provider per regio;
- frame age, observation age en cache age;
- worker queue en terminal jobs;
- active/fallback/degraded;
- selected track en alternatives;
- current versus next threat;
- passage probability en ETA-interval;
- groundsites en gebruikte/excluded reasons;
- calibration/replay/build versions;
- duidelijke foutgrenzen.

### P2-O07 — Continue science governance

- champion/challengermodel;
- providerdrift;
- seasonal/regional recalibration;
- maandelijkse reliabilityrapporten;
- golden replayregressie;
- model change log en rollbackmodel.

---

## 8. Migratieplan

### 8.1 Geen destructieve reset

De bestaande kalibratiedatabase wordt niet zomaar leeggemaakt.

Procedure:

1. maak een checksum en read-only snapshot;
2. schrijf schema vNext naast legacy;
3. classificeer bestaande “original frames” eerlijk als raw of
   provider-overlay;
4. dual-write nieuwe observations naar legacy waar nodig en vNext;
5. backfill alleen velden die werkelijk uit bestaande data bewijsbaar zijn;
6. gebruik `unknown`, niet verzonnen waarden;
7. valideer counts en foreign keys;
8. archiveer de oude database pas na succesvolle cutover;
9. verwijder legacydata alleen via een apart, goedgekeurd retentionticket.

### 8.2 Dual-run

Per batch:

- dezelfde immutable observation-IDs gaan naar legacy en vNext;
- elke engine krijgt een eigen state namespace;
- outputs hebben aparte engine/modelversion;
- vergelijkingsrecord koppelt beide forecasts aan dezelfde inputset;
- vNext kan geen legacy state muteren.

### 8.3 HA-entiteiten

Shadowfase:

- legacy blijft de operationele hoofdentity;
- vNext-resultaten komen in technische shadowattributes/sensoren;
- automatiseringen blijven op legacy;
- er is een expliciete opt-in testtarget.

Canary:

- één fictieve tracker;
- daarna één echte niet-kritische tracker;
- daarna één regio/provider;
- pas daarna bredere omschakeling.

### 8.4 Rollback

Rollback is een feature-flagwijziging:

- vNext-consumptie uit;
- legacy-output weer actief;
- immutable ingest blijft doorlopen;
- vNext-data blijft beschikbaar voor diagnose;
- geen databaseschema wordt tijdens incidentrollback verwijderd.

Voor iedere release:

- vorige integratiezip;
- vorige worker-tar;
- schema compatibility matrix;
- config backup;
- rollbackprocedure;
- bekende irreversible migrations;
- build-ID/commit;
- test- en replayrapport.

---

## 9. Home Assistant- en workerperformance

### 9.1 Gewenst rekenmodel op de NUC

Voor de NUC8i7BEH-klasse met vier fysieke cores en acht logical threads:

- buitenste workerqueue: één zware job tegelijk;
- binnen één nowcastjob: maximaal twee native reken-threads;
- de volledige workerprocess tree draait met `nice 15`;
- CPU-affinity sluit één volledige fysieke core, inclusief beide SMT-threads,
  uit voor de worker zodat HA, Z2M, MQTT en het OS altijd worker-vrije
  rekencapaciteit behouden;
- NumPy/OpenBLAS/MKL/NumExpr/OMP blijven hard op twee threads begrensd;
- geen gelijktijdige pySTEPS-, replay- en kalibratiepieken.

Zes workerthreads zijn geen toegestane productieconfiguratie meer. Twee threads,
één gereserveerde fysieke core en `nice 15` zijn samen vereist, maar vormen nog
geen bewijs van soepelheid: de kandidaat moet daarnaast de HA/Z2M-gates hieronder
halen.

### 9.2 HA-regels

Verboden op de HA-event loop:

- HDF5-decode;
- grote gzip-decompressie;
- rasterreprojectie;
- connected-component detection;
- optical flow;
- ensemble-nowcast;
- grote SQLite-scans;
- complexe GeoJSON-opbouw.

HA gebruikt:

- async desired-state- en snapshottransport;
- compact begrensd immutable snapshotresultaat;
- executor alleen voor kleine onvermijdelijke blocking handelingen;
- coordinatorupdates;
- pure sensorproperties.

Ook buiten de event loop zijn providerpolling, meteorologische SQLite,
rasterhistoriek, trackingstate, forecastcompute en GeoJSON-opbouw in het
HA-proces verboden. Een executor verplaatst CPU- of I/O-druk immers niet van de
NUC en is daarom geen geldige oplossing voor de gemeten HA/Z2M-latentie.

### 9.3 Backpressure

Wanneer de worker achterloopt:

1. de worker bewaart het raw artifact vóór verdere verwerking;
2. de worker bewaart de observation;
3. job met stable batch-ID queued;
4. de worker publiceert queue age;
5. HA toont het laatst gevalideerde snapshot als stale/degraded;
6. lage-prioriteitswerk uitstellen;
7. ensembleleden verminderen volgens getest beleid;
8. nooit observations of detections weggooien.

### 9.4 Harde engineering-SLO’s

Iedere live kandidaat wordt vergeleken met een representatieve meetperiode met
de Storm Tracker-integratie én worker uitgeschakeld. Dezelfde HA-, MQTT- en
Z2M-belasting wordt daarna met de kandidaat herhaald. Een kandidaat faalt als
een globale gemiddelde waarde een zichtbare staartlatentie verbergt.

- verloren batches: exact `0`;
- unhandled workerjobs zonder terminal state: exact `0`;
- HA event-loop lag p99 maximaal `20 ms` én maximaal `10 ms` slechter dan de
  integration-off-basislijn;
- geen door Storm Tracker veroorzaakte event-loopstall boven `100 ms`;
- Z2M broker-ingress-naar-HA-state p95 maximaal `50 ms` en p99 maximaal
  `100 ms` slechter dan de integration-off-basislijn;
- verloren, dubbel verwerkte of buiten volgorde afgeleverde Z2M-berichten door
  Storm Tracker-belasting: exact `0`;
- sensorpropertyreads en kaart-HTTP doen uitsluitend begrensde O(1)-snapshotreads
  en starten geen compute, database-scan of serialisatiejob;
- worker queue age p95 korter dan één cadence van het actieve radarproduct;
- forecast krijgt `stale` vóór twee gemiste productcadences;
- package temperature blijft contexttelemetrie en is op zichzelf geen
  afkeur- of alarmcriterium; ook een korte gemeten piek tot `100 °C` wordt
  niet automatisch als storing geïnterpreteerd;
- geen door CPU/OS/hardware gemelde thermalthrottling en geen gelijktijdige
  verslechtering van runtime, queue-age of HA-latency;
- incidentele piek blijft onder de hardwareveiligheidsgrens;
- disk free-spacealarm vóór minder dan zeven dagen verwachte groeiruimte.

Temperatuurgrenzen worden niet als reden gebruikt om raw data te verliezen.

### 9.5 Local fallback op langere termijn

De actuele P0-local fallback beschermt tracking tegen batchverlies. Voor de
zware vNext-engine mag HA niet permanent dezelfde compute overnemen.

Langetermijnbeleid:

- HA valideert en verwisselt alleen atomair een begrensd snapshot in geheugen;
- iedere meteorologische job blijft durable pending voor workerretry;
- optionele tweede worker later;
- HA publiceert bij workeruitval uitsluitend degraded/stale/unknown;
- raw ingest en een eventuele legacy comparator functioneren uitsluitend in de
  worker en delen exact dezelfde immutable bronartifacts.

---

## 10. Security en secrets

### 10.1 Actuele lokale toestand en resterend risico

De lokale, nog niet gedeployde kandidaat transporteert geen
Netatmo-credentials meer in MQTT-jobpayloads. De integratie verzendt alleen
`credential_ref=worker_environment`; de worker haalt de credentials uitsluitend
uit add-on-environment, weigert secretbevattende jobs en bindt een eventueel
geroteerd refreshcredential aan een fingerprint van de geconfigureerde
credentialset. Ook de handmatige ground-coveragesurvey weigert Netatmo- en
Synoptic-secrets in jobpayloads. Gerichte contracttests voor deze paden zijn
groen.

De afzonderlijke DWD-only servicecanary sluit nu ook ambient secretinheritance
door hard-decoderchildren: een vaste technische allowlist geldt zowel aan de
OS-spawngrens als opnieuw in de child vóór import/decode. In canarymodus unset
`run.sh` alle niet-DWD-providercredentials vóór `exec`. Deze begrenzing wijzigt
de standaard `legacy`-modus niet. De reconnectauthority is eveneens
generatiegebonden: disconnect sluit readiness/admission en alleen vers bewijs
uit de nieuwe MQTT-generatie mag die opnieuw openen.

Dit bewijst nog niet dat historische retained data, brokerlogs of alle live
topics secretvrij zijn. MQTT QoS en brokerauthentication leveren bovendien
betrouwbaarheid, geen geheimhouding tegen iedere brokerclient met topictoegang.
Vóór productie zijn daarom nog een live topiccapture, log-/diagnostiekaudit en
rotatie van historisch gebruikte tokens nodig.

### 10.2 Doelcontract

- Secrets staan uitsluitend in HA secret storage/add-on options of
  environment;
- jobpayload bevat alleen `credential_ref`;
- geen secret in MQTT, logs, traces, database, raw artifactmetadata,
  diagnostics of exceptiontext;
- workerresultaten redacteren headers en queryparameters;
- tokens worden nooit in een checksum-input opgenomen die wordt getoond;
- credentialrotatie is mogelijk zonder model-/datamigratie.

### 10.3 MQTT-beveiliging

- aparte brokeraccounts voor HA-integratie en worker;
- topic-ACL:
  - HA mag requests publiceren en results/status lezen;
  - worker mag requests lezen en results/status publiceren;
- geen wildcardtoegang buiten de Storm Tracker-topicroot;
- TLS wanneer verkeer host-/netwerkgrenzen passeert;
- unieke job-ID, deadline en contractversion;
- replay-/duplicatebescherming;
- maximale compressed en uncompressed payload;
- redacted terminal errors;
- retained alleen voor workerstatus, niet voor secrets of jobpayload.

### 10.4 Bestand- en databasebeveiliging

- raw artifacts en DB alleen toegankelijk voor add-on/integratie;
- deployarchives bevatten geen options/secrets;
- backups versleuteld wanneer ze buiten HA worden gekopieerd;
- debugexports standaard redacted;
- checksums verifiëren deployartefact;
- dependencyversies vastgepind en periodiek gescand.

### 10.5 Securitytickets

| Ticket | Werk | DoD |
|---|---|---|
| SEC-01 | secrets uit MQTT-jobpayload | lokaal geïmplementeerd en contractueel getest; live capture van alle topics bevat geen credential |
| SEC-02 | brokeraccounts en ACL | negatieve ACL-tests slagen |
| SEC-03 | structured redaction | logs/diagnostics/raw metadata secretvrij |
| SEC-04 | bounded streaming decompress | gzip-bombtest faalt gecontroleerd |
| SEC-05 | deploy provenance | artifactchecksum, version en build-ID zichtbaar |
| SEC-06 | credentialrotatie | lokaal fingerprintgebonden persistent token getest; live rotatie kan zonder code- of DB-reset |

Omdat secrets historisch al via MQTT kunnen zijn verzonden, worden relevante
tokens na SEC-01 geroteerd.

---

## 11. Observability

### 11.1 Verplichte counters

Ingest:

- `raw_fetch_attempts_total`;
- `raw_fetch_success_total`;
- `raw_artifacts_persisted_total`;
- `observations_parsed_total`;
- `observations_persisted_total`;
- `parse_rejects_total{reason}`;
- `source_revisions_total`;
- `observations_lost_total`.

Batch/worker:

- `batches_created_total`;
- `batches_persisted_total`;
- `batches_offered_total`;
- `batches_delivered_total`;
- `batches_acked_total`;
- `batches_requeued_total`;
- `batches_discarded_total`;
- `batches_local_fallback_total`;
- `batches_lost_total`;
- `observations_offered_total`;
- `observations_delivered_total`;
- `observations_requeued_total`;
- `observations_discarded_total`;
- `observations_lost_total`;
- `failure_retry_transitions_total`;
- `cancellation_requeue_transitions_total`;
- `split_transitions_total`;
- `reset_discard_events_total`;
- `jobs_terminal_total{state,error}`;
- `queue_depth`;
- `queue_oldest_age_seconds`;
- `job_runtime_seconds{type}`;
- `worker_build_id`;
- `handler_heartbeat_age_seconds`.

Meteorologie:

- `frames_by_provider`;
- `frame_age_seconds`;
- `frame_completeness`;
- `provider_quality_score`;
- `detections_total{level,quality}`;
- `active_tracks{state}`;
- `association_ambiguity`;
- `identity_confidence`;
- `split_events_total`;
- `merge_events_total`;
- `motion_residual`;
- `valid_nowcast_horizon_minutes`;
- `passage_probability`;
- `forecast_calibration_version`.

Ground:

- `ground_observations_total{provider,kind}`;
- `ground_fresh_total`;
- `ground_stale_total`;
- `ground_capable_total`;
- `ground_matched_total`;
- `ground_ambiguous_total`;
- `ground_excluded_total{reason}`;
- `same_track_confirmations_total`;
- `radar_ground_disagreements_total`.

Systeem:

- HA event-loop lag;
- worker CPU;
- per-core utilization;
- process/threadcount;
- package temperature;
- thermal throttle;
- RSS;
- disk free;
- DB bytes en groei/dag;
- archive backlog.

### 11.2 Alarmen

Direct P0-alarm:

- `observations_lost_total > 0`;
- `batches_lost_total > 0`;
- job zonder terminal state na deadline plus grace;
- DB write failure;
- raw artifact niet duurzaam vóór verwerking;
- cross-storm confirmationcontract geschonden.

Hoog:

- queue age > twee productcadences;
- worker offline;
- provider actief maar frames stale;
- diskruimte onder grens;
- HA event-loop langdurig vertraagd;
- build-/contractmismatch.

Waarschuwing:

- calibration drift;
- providerquality verslechtert;
- meer association ambiguity;
- te veel ground duplicates/stale data;
- nowcast geldige horizon krimpt.

### 11.3 Geen observability via side effects

Metrics en auditrecords worden in de pipeline geschreven. Een sensorread:

- verhoogt geen counter;
- maakt geen decision;
- schrijft niet naar SQLite;
- start geen providerfetch;
- verandert geen pending lijst.

---

## 12. Replay- en verificatieprogramma

### 12.0 Wat de lokale kalibratiebackup nu werkelijk bewijst

De immutable lokale kopie
`.calibration-data/storm_tracker_calibration_20260730_092754.sqlite3`
is op 2026-07-30 opnieuw uitsluitend via SQLite `mode=ro` en
`PRAGMA query_only` geaudit. SHA-256 vóór en na de audit bleef exact
`177d4704d665000722dbd131f3db03a238b26de072ce32bde6ea4650815cb263`;
de audit heeft de backup dus niet gewijzigd.

Aanwezige bruikbare inhoud:

- 1.461 genormaliseerde frames en 118.487 framepunten;
- 2.230 `original_radar_frames`, maar daarin zitten provider-overlays en niet
  aantoonbaar de exacte oorspronkelijke upstreambytes met complete receipts;
- 72 genormaliseerde `pysteps_frame_cache`-frames;
- één aaneengesloten KMI-reeks voor `region-1` van 36 frames op 10 minuten,
  waarvan 33 natte frames;
- voor `region-2` twee RainViewer-reeksen van respectievelijk 16 en 19 frames,
  gescheiden door één geschat ontbrekend frame, plus één los NOAA
  Enterprise-rainrateframe;
- 5.134 forecast-verificatierecords, maar geen opgeslagen
  passagewaarschijnlijkheden;
- 151 virtual-probe-uitkomsten: 5 numeriek nat, 146 onbekend en 0 numeriek
  droog;
- 5.492 performance-/thermische samples, bruikbaar voor historische
  diagnostiek maar niet als vervanging voor een representatieve live soak.

Daaruit volgen harde grenzen:

- de KMI-reeks is bruikbaar voor **exploratieve** multi-frame detectie,
  associatie en bewegingsreplay;
- interne centroidverschuiving is geen onafhankelijke snelheidstruth;
- de backup bevat geen onafhankelijk gereviewde cell-identiteit of lineage en
  kan dus geen speed error, IDF1, HOTA, split/merge-kwaliteit of
  trackingcorrectheid bewijzen;
- er zijn geen droge ground-outcomes en de tabel `ground_observations`
  ontbreekt, zodat ground-specificiteit en false-confirmation-rate niet
  berekend mogen worden;
- zonder passageprobabilities en onafhankelijke positieve én negatieve
  passage-uitkomsten kan geen geldige Brier/BSS-releaseclaim worden gemaakt;
- vergelijking met Buienradar, eigen statussen, provideragreement of
  workerpariteit is geen onafhankelijke meteorologische waarheid;
- complete ensemblemembers en onafhankelijke lokale-intensiteitslabels
  ontbreken nog voor echte CRPS- en rankhistogramevaluatie.

De bestaande database is dus waardevol om techniek en replaydeterminisme te
testen, maar onvoldoende om te verklaren dat de toepassing meteorologisch
gekalibreerd of betrouwbaar is. Die conclusie is een releasebarrière, geen
reden om de aanwezige data weg te gooien.

Een read-only live momentopname op 2026-07-30 tijdens een brede regenzone over
België is apart vastgelegd in
`docs/LIVE_EVENT_OBSERVATION_20260730_BELGIUM.md`. KMI was de gekozen bron,
OPERA zag binnen de 250-km-regio een oudercomponent van circa 97.422 km² en de
live calibratiedatabase was gegroeid tot 1.682 frames. Tegelijk noemde de
motion-preflight zichzelf `unstable_motion`: rastertranslation circa
50,8 km/u, centroidtranslation circa 346,9 km/u en een operationele
tweepuntsassociatie van 124,1 km/u. Dit is waardevolle foutdiagnostiek, geen
KMI-speedtruth. De brede zone wordt als kandidaat-event behouden voor een
latere immutable native-artifactreplay met onafhankelijke identiteit en
groundtruth.

### 12.1 Datasetopbouw

Een bruikbare eventreplay bevat bij voorkeur minstens twintig opeenvolgende
originele frames. Twintig is een praktisch minimum voor langere
motion/lineage-inspectie, geen natuurwet. De dataset bevat ook:

- cold starts met twee of drie frames;
- onregelmatige tijdsintervallen;
- één of meer ontbrekende frames;
- providerfailures en handoffs;
- ground observations met echte meetintervallen;
- raw/provider-overlayclassificatie;
- alle algorithm/configversions.

### 12.2 Verplichte cases

Meteorologische regimes:

- losse kleine cel;
- brede stratiforme band;
- embedded convectieve cores;
- lijnvormig systeem;
- groei;
- decay;
- birth;
- split;
- merge;
- kruisende/nabije cellen;
- snel bewegend systeem;
- vrijwel stationaire bui;
- vormverandering zonder grote translatie;
- radar-ground disagreement;
- mogelijke virga.

Operationele cases:

- België: KMI/OPERA en relevante fallback;
- New York: NOAA/RainViewer en groundstations;
- andere VS-locaties;
- Duitsland/RADOLAN;
- VK/Met Office;
- providerhandoff met en zonder overlap;
- >15 objects;
- queue-full;
- worker timeout/restart;
- HA restart;
- database recovery;
- te oude/future observations.

Groundcases:

- station binnen actuele footprint;
- station alleen binnen historische hull;
- station vlak buiten edge;
- tracker binnen brede polygon;
- twee stations bij verschillende storms;
- dezelfde fysieke site via METAR en Synoptic;
- categorische METAR versus kwantitatieve gauge;
- stale wet station;
- capable dry station;
- missing station;
- nabij station maar niet op tracker.

### 12.3 Labels

De lokale exporter/ingester voor deze stap bestaat en weigert onder andere een
hot WAL/journal, gewijzigde snapshot, ongeldige SQLite-integriteit, verkeerde
eventscope, leakage, onbekende evidence-references, candidate-derived truth,
ontbrekende blinding en een onvolledige templateverzameling. Een unresolved
template blijft expliciet unresolved. De echte labels en externe bronartifacts
ontbreken nog; hun bytes moeten vóór vrijgave immutable worden gearchiveerd en
onafhankelijk tegen de gedeclareerde SHA-256 worden gecontroleerd.

Voor de gouden subset:

- twee onafhankelijke reviewers waar haalbaar;
- detection- en lineageannotatie;
- consensusrecord;
- ambiguity mag als zodanig gelabeld worden;
- reviewer ziet geen output van de te beoordelen engine;
- events, niet losse frames, worden tussen train/validation/test gesplitst.

### 12.4 Baselines

Iedere release vergelijkt met:

- Eulerian persistence;
- twee-frame lineaire centroidtranslation;
- huidige legacyengine;
- pySTEPS deterministic extrapolation;
- laatste vrijgegeven vNextversie.

### 12.5 Metrics

Detection:

- POD;
- FAR;
- CSI;
- area/IoU;
- threshold sensitivity;
- object count bias.

Tracking:

- ID switches;
- fragmentation;
- completeness;
- IDF1;
- HOTA en componenten;
- split precision/recall;
- merge precision/recall;
- position error;
- speed error;
- heading error;
- lineagegraph integrity;
- providerhandoff continuity.

Veldnowcast:

- POD/FAR/CSI per threshold en lead;
- Fractions Skill Score per ruimtelijke schaal;
- MAE/quantile error;
- displacement error;
- skillful horizon.

Probabilistisch:

- Brier score/Brier Skill Score;
- reliability diagram;
- sharpness;
- ROC als aanvullende, niet enige metric;
- CRPS;
- rank histogram;
- p10/p90-coverage;
- calibration error met confidence intervals.

Tracker:

- passage Brier/BSS;
- false passage/missed passage;
- ETA MAE;
- ETA bias;
- ETA intervalcoverage;
- lokale intensity MAE/CRPS;
- current-versus-next classification.

Ground:

- wet/dry/categorical confusionmatrix;
- calibration per provider en capability;
- duplicate-sitefouten;
- cross-storm false confirmation;
- representativiteit per afstand.

Operationeel:

- exact ingest/persist/process counts;
- retry/fallback;
- queue age;
- runtime;
- CPU/temperatuur;
- HA lag;
- storagegroei.

### 12.6 Statistische evaluatie

- bootstrap per event, niet per sterk gecorreleerd frame;
- confidence intervals bij metricverschillen;
- aparte strata per provider, regio, regime en lead time;
- geen tuning op release-testset;
- vooraf gekozen primary metrics;
- zowel gemiddelde als slechtste relevante strata rapporteren;
- sampleverzameling voortzetten totdat confidence intervals smal genoeg zijn
  voor de gekozen praktische beslisgrens.

---

## 13. Release- en acceptatiecriteria

### 13.1 P0 absolute criteria

- verloren raw artifacts na succesvolle ontvangst: `0`;
- verloren observations: `0`;
- verloren batches: `0`;
- identificeerbare jobs zonder terminal state: `0`;
- stil weggegooide detections door `MAX_STORMS` of speedgate: `0`;
- cross-storm ground confirmations: `0`;
- sensorread side-effectrecords: `0`;
- polygon inside/edge contract: 100% golden tests.

### 13.2 Trackingcriteria

Op de onafhankelijke testset:

- minder ID-switches dan legacy;
- minder fragmentation dan legacy;
- track completeness niet slechter;
- position/speed error niet slechter;
- split/merge precision en recall gerapporteerd en boven vooraf
  vastgelegde releasegrens;
- providerhandoff continuity beter dan directe legacy-centroidmatch;
- geen ernstige regressie in een kernregio verborgen door het globale
  gemiddelde.

De exacte praktische marges worden na de baseline preregistreerd. Ze mogen
niet na het zien van testresultaten worden aangepast.

### 13.3 Nowcastcriteria

- positieve Brier Skill Score tegenover de gekozen baseline in de primaire
  lead-timevensters;
- FSS minimaal niet slechter en aantoonbaar beter waar de nieuwe methode
  wordt geactiveerd;
- reliability zonder zware systematische overconfidence;
- nominale prediction intervals hebben passende empirische coverage;
- geldige horizon stopt waar skill verdwijnt;
- lokale intensity komt van de trackerlocatie, niet van system maximum;
- no-passage probability en conditionele ETA zijn correct gescheiden.

### 13.4 Groundcriteria

- dezelfde fysieke site telt eenmaal;
- categorie en hoeveelheid nooit verwisseld;
- observed/received/intervaltijden correct;
- stale heeft nul actuele bevestigingsbijdrage;
- dry alleen van capable sensor;
- same-track confirmation gebruikt alleen dezelfde track/lineage;
- iedere beslissing is exact replaybaar.

### 13.5 Operationele criteria

- volledige unit-/contract-/integrationtests groen;
- golden replays groen;
- fault-injectionmatrix groen;
- minimaal zeven dagen shadow soak zonder verlies;
- opslagprojectie en retention bewezen;
- HA- en Z2M-SLO’s gehaald tegenover een vastgelegde integration-off-basislijn;
- twee workerthreads, `nice 15` en één fysiek gereserveerde core zijn runtime
  aantoonbaar actief;
- geen door CPU/OS/hardware gemelde thermalthrottling of aantoonbare
  performance-instorting in een representatieve piek; package temperature
  alleen is geen releasefout;
- securitytests groen;
- deploy/rollback dry run groen;
- actieve integration-, worker-, schema-, parser- en modelversies zichtbaar.

### 13.6 Cutovercriterium

Omschakeling gebeurt uitsluitend voor een provider/regio-combinatie die alle
criteria haalt. Andere regio’s blijven op legacy of op een eenvoudiger
gevalideerd model.

---

## 14. Uitrol

### Stap 1 — P0 hardening

- lokaal geïmplementeerde P0-fixes als regressiebarrière bevriezen;
- resterende live, opslag- en operationele releasegates sluiten;
- migratie- en rollbackpad vastleggen;
- patchrelease;
- live fault- en New Yorkcontrole.

### Stap 2 — volledige immutable feitopslag

- het operationele schema-v3-journal uitbreiden naar schema vNext;
- originele providerartifacts en volledige evidence/provenance bewaren;
- dual-write;
- raw/observation/frame receipts;
- nog geen operationele vNext forecast.

### Stap 3 — offline replay

- detector, flow en CellTrack uitsluitend op replays;
- baselines en labels;
- geen HA-automatiseringen wijzigen.

### Stap 4 — live shadow

- vNext consumeert dezelfde immutable batches;
- compacte shadowdiagnostiek;
- legacy blijft beslissend.

### Stap 5 — fictieve tracker canary

- vNext zichtbaar op een expliciete testtracker;
- ground en providerhandoff live controleren;
- rollbackflag testen.

### Stap 6 — regionale canary

- één provider/regio;
- minimaal één volledige weersituatie plus soak;
- vergelijking en sign-off.

### Stap 7 — gefaseerde productie

- per regio/provider;
- model/version pinned;
- automatische rollback bij P0-contractbreuk;
- voortdurende reliabilitymonitoring.

### Stap 8 — legacy retirement

Pas wanneer:

- alle actieve regio’s gemigreerd zijn;
- terugvalperiode voorbij is;
- replays en audits behouden zijn;
- automatiseringen nieuwe semantics gebruiken;
- gebruiker de retirement goedkeurt.

---

## 15. Risicoregister

| Risico | Gevolg | Mitigatie |
|---|---|---|
| Nieuwe convectie ontstaat buiten bestaande echo | miss op langere lead | dynamische horizon, uncertainty, later gevalideerde NWP/satellietinitiatie |
| Providerproducten meten andere variabelen/resoluties | valse motion/intensity bij handoff | native provenance, overlapregistratie, covariance, provisional identity |
| Split/merge is fysiek én algoritmisch ambigu | instabiele IDs | immutable graph, nieuw child/merge-ID, alternatives/confidence |
| Groundstations zijn puntmetingen | onterechte gebiedsconclusie | representativiteitsmodel, nearby versus at-target |
| Dezelfde site via meerdere providers | dubbele bevestiging | PhysicalSite en independence group |
| Crowd sensors zijn slecht geplaatst/gekalibreerd | bias | provider-QC, historische reliability, nooit gelijk gewicht |
| Werkelijk gemelde CPU-/OS-throttling of gelijktijdige runtime-/queuedruk | HA traag/stale forecast | één job, intern begrensd multicore, adaptive ensemble, geen dataverlies; een korte piek tot 100 °C alleen is expliciet geen fout |
| Database groeit te snel | volle disk, ingestuitval | quota, archive, alarms, restoretest |
| MQTT duplicate/late result | dubbele statecommit | batch-ID, result checksum, atomic receipt |
| HA fallback wordt te zwaar | event-loopimpact | alleen lichte fallback, durable retry voor zware vNext |
| Probabilities zijn overfit | schijnzekerheid | event-level holdout, reliability, versioned calibrator |
| Groene tests leggen fout gedrag vast | valse zekerheid | wetenschappelijke golden replays en metricgates |
| Secrets lekken via jobs/logs | credentialcompromis | credential refs, ACL, redaction, rotatie |
| Parallelle HA/workerkern divergeert | verschillende uitkomsten | gedeeld package/artifact en parityfixtures |

---

## 16. Concrete ticketvolgorde

### Werkpakket 0 — onmiddellijk

1. `P0-L01` OFE lossless lokaal afgerond; live recovery/soak bewijzen.
2. `P0-L02` worker/local exact-once lokaal afgerond; live foutmatrix bewijzen.
3. `P0-W01` terminal status en durable receipts lokaal afgerond; addon- en
   brokercrashes live bewijzen.
4. `P0-L03` >15 objects en performance bewijzen.
5. `P0-G01` volledige polygonpariteit afronden.
6. `P0-G02` per-storm legacy confirmation afronden.
7. `P0-G03` actuele footprint losmaken.
8. `P0-S01` sensor side effects verwijderen.
9. `P0-D01/D02` lokaal ingevoerd; migratie, diskfouten en live power-loss
   bewijzen.
10. P0 patch releasen en live verifiëren.

### Werkpakket 1 — betrouwbare feiten

1. `P1-D01` vNext-schema.
2. `P1-D03` observation contract.
3. `P1-P01` providercertificaten.
4. `P1-G01/G02/G03` Netatmo/Synoptic/METAR-semantiek.
5. `P1-G04` PhysicalSite.
6. `P1-D02` retention.

### Werkpakket 2 — nieuwe trackingkern

1. `P1-M01` analysis grid.
2. `P1-M02` detector.
3. `P1-M03` optical flow.
4. `P1-T01` associationgraph.
5. `P1-T02` CellTrack estimator.
6. `P1-T03` lineage.
7. `P1-T04` WeatherSystem grouping.

### Werkpakket 3 — forecast en ground

1. `P1-N01` baselines.
2. `P1-N02` STEPS.
3. `P1-N04` trackerforecastcontract.
4. `P1-F01/F02/F03` ground fusion.
5. `P1-N05` calibration.
6. `P1-N03` LINDA challenger.
7. `P1-F04` groundverification.

### Werkpakket 4 — productie

1. `P2-O01` observability/SLO.
2. `P2-O02` adaptive multicore compute.
3. securitytickets.
4. shadow soak.
5. fictieve tracker canary.
6. regionale canary.
7. gefaseerde cutover.

---

## 17. Definition of Done

### 17.1 Ticket-DoD

Ieder ticket is pas klaar wanneer:

- code en contract zijn gedocumenteerd;
- unit-, contract- en relevante replaytests bestaan;
- failure paths zijn getest;
- nieuwe metrics/diagnostiek bestaan;
- data provenance volledig is;
- HA-/workerpariteit waar relevant is bewezen;
- security/redaction is gecontroleerd;
- performanceimpact is gemeten;
- migratie en rollback zijn getest;
- user-facing semantics zijn bijgewerkt;
- geen stale documentatie het nieuwe gedrag tegenspreekt.

### 17.2 Fase-DoD

Een fase is pas klaar wanneer:

- alle phase tickets hun DoD halen;
- volledige suite groen is;
- golden replayrapport bestaat;
- operationele metrics geen P0-regressie tonen;
- schema/versionmatrix is bijgewerkt;
- deploy- en rollbackartefact getest zijn;
- open afwijkingen expliciet geaccepteerd of geblokkeerd zijn.

### 17.3 Product-DoD

Storm Tracker V3 is meteorologisch betrouwbaar genoeg voor productie wanneer:

1. ingest aantoonbaar lossless en idempotent is;
2. raw, normalized en derived data strikt gescheiden zijn;
3. iedere detection bewaard blijft;
4. `CellTrack` de primaire identiteit is;
5. splits/merges immutable lineage hebben;
6. WeatherSystem alleen secundair groepeert;
7. motion lokaal en uncertainty-aware is;
8. providerhandoff geen ongemarkeerde fysieke sprong creëert;
9. forecast probabilistisch en empirisch gekalibreerd is;
10. current rain, nearby rain, same-track confirmation en next threat aparte
    states zijn;
11. ground evidence per fysieke site, track en tijd is gekoppeld;
12. alle outputs volledig replaybaar zijn;
13. onafhankelijke metrics de baselines aantoonbaar verslaan;
14. HA responsief blijft onder pieklast;
15. worker- of providerfalen geen weerdata wist;
16. secrets niet in jobtransport of diagnostiek voorkomen;
17. observability afwijkingen direct zichtbaar maakt;
18. regionale rollback zonder dataverlies werkt.

---

## 18. Wetenschappelijke en autoritatieve basis

De roadmap volgt geen bron als kant-en-klaar recept. De volgende primaire of
autoritatieve bronnen onderbouwen de centrale ontwerpkeuzes:

- [Pulkkinen et al. (2019), pySTEPS](https://gmd.copernicus.org/articles/12/4185/2019/):
  optical flow, Lagrangiaanse extrapolatie, schaalafhankelijke
  predictability, ensembles en probabilistische verificatie.
- [Pulkkinen, Chandrasekar & Niemi (2021), LINDA](https://journals.ametsoc.org/view/journals/atot/38/12/JTECH-D-21-0013.1.xml):
  lokale rain cells, advection, growth/decay, verlies van kleinschalige
  voorspelbaarheid en stochastic uncertainty.
- [Dixon & Wiener (1993), TITAN](https://journals.ametsoc.org/abstract/journals/atot/10/6/1520-0426_1993_010_0785_ttitaa_2_0_co_2.xml):
  objectidentificatie, geoptimaliseerde tracking en split/mergelogica.
- [pySTEPS DATing, operationele TRT-gebaseerde tracking](https://pysteps.readthedocs.io/en/stable/pysteps_reference/tracking.html):
  cellen per frame detecteren, vorige footprints met optical flow advecteren
  en pas daarna tegen nieuwe detecties matchen.
- [Meredith, Ulbrich & Rust (2023), Cell-TAO](https://gmd.copernicus.org/articles/16/851/2023/):
  detection- en trackingkeuzes beïnvloeden cell properties en resultaten
  materieel.
- [Object-based and Lagrangian hailstorm analysis (2025)](https://wcd.copernicus.org/articles/6/645/2025/wcd-6-645-2025.html):
  expliciete objectcorrespondentie, motion anticipation en split/merge.
- [Roberts & Lean (2008), Fractions Skill Score](https://centaur.reading.ac.uk/31220/):
  schaalafhankelijke verificatie van convectieve neerslag.
- [Brier (1950), verification of probability forecasts](https://journals.ametsoc.org/view/journals/mwre/78/1/1520-0493_1950_078_0001_vofeit_2_0_co_2.xml):
  formele verificatie van probabilities.
- [WMO Guide to Instruments and Methods of Observation, No. 8](https://community.wmo.int/site/knowledge-hub/programmes-and-initiatives/instruments-and-methods-of-observation-programme-imop/guide-instruments-and-methods-of-observation-wmo-no-8-0):
  meetmethoden, kwaliteit en onzekerheid voor precipitation, stations en
  radar.
- [WMO Guide to Operational Weather Radar Best Practices, No. 1257](https://community.wmo.int/site/knowledge-hub/programmes-and-initiatives/weather-radar-observations/guide-operational-weather-radar-best-practices-wmo-no-1257):
  operationele radar-QC, processing en netwerk-/compositepraktijken.
- [EUMETNET OPERA ODIM HDF5 2.4](https://eumetnet.eu/wp-content/uploads/2021/07/ODIM_H5_v2.4.pdf):
  officiële Europese radardata- en metadatamodellering.
- [NOAA MRMS Surface Precipitation Rate](https://vlab.noaa.gov/web/wdtd/-/surface-precipitation-rate-spr-1?selectedFolder=9234881):
  quality-controlled radar/mosaic rain-rateproduct en resolutie/cadence.
- [NOAA MADIS Quality Control](https://madis.ncep.noaa.gov/madis_qc.shtml):
  QC-flags, temporal en spatial consistency voor surface observations.
- [Synoptic Precipitation Service](https://docs.synopticdata.com/services/precipitation-service-explained):
  officiële semantiek van `precip_intervals` en derived precipitation.
- [NOAA Aviation Weather METAR data](https://aviationweather.gov/help/data/):
  officiële METAR present-weather- en accumulatiesemantiek.

---

## 19. Samenvattende koers

De toepassing wordt niet betrouwbaar door alle veiligheidsregels te
verwijderen en evenmin door steeds meer heuristische remmen toe te voegen.

De juiste koers is:

> bewaar ieder bronfeit, maak iedere afleiding herleidbaar, volg individuele
> cellen persistent, modelleer splits en merges expliciet, gebruik optical
> flow voor het veld, groepeer cellen alleen secundair als WeatherSystem,
> behandel ground data als tijd- en trackgebonden evidence, publiceer
> probabilities met gekalibreerde onzekerheid en schakel alleen over wanneer
> onafhankelijke replays aantonen dat de nieuwe engine werkelijk beter is.
