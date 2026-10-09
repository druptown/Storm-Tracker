# P0-hardening testkandidaat 1 — integratie 1.12.0 / worker 1.15.0

Status: lokaal gebouwd en gevalideerd, **niet vrijgegeven en niet
deployen** zolang de resterende P0-releaseblockers hieronder openstaan.

## Doel van deze release

Deze P0-release repareert verlies- en verkeerde-koppelingsfouten vóór de
meteorologische kern verder wordt herbouwd. Ze maakt nog niet ieder bestaand
bewegingsmodel meteorologisch definitief; ze zorgt eerst dat echte input niet
stil verdwijnt en dat bevestiging bij het juiste systeem terechtkomt.

## Opgeloste fouten

1. Een trackingbatch wordt alleen als door de worker verwerkt beschouwd als
   het resultaat volledig valide is en atomair werd toegepast. Bij timeout,
   transportfout of een ongeldig resultaat verwerkt de lokale engine exact
   dezelfde batch vanaf de ongewijzigde beginsituatie.
2. De Observation Fusion Engine zet een batch bij callbackfout of annulering
   vóór nieuwere observaties terug in de wachtrij. Iedere batch houdt bij
   retry dezelfde ID; nieuwe observaties blijven een afzonderlijke latere
   batch. Een commit gevolgd door een callbackfout wordt niet opnieuw
   meteorologisch toegepast. Ook `CancelledError` vanuit een reeds gestarte
   achtergrondcallback plant de teruggezette batch nu zelfstandig opnieuw.
3. Trackingprotocol v4 bindt ieder workerantwoord aan batch-ID,
   startrevision, exacte inputhash, exacte startstatehash en een opnieuw door
   HA berekende volledige snapshot-hash. Compare-and-swap weigert een laat
   resultaat als de trackerstate ondertussen wijzigde. De lokale commit loopt
   op een working copy; een precommitfout kan daardoor ook geen reeds
   gepubliceerde `Storm`-referentie half gemuteerd achterlaten.
4. `max_storms` gooit geen bliksem- of radarwaarnemingen meer weg. De waarde
   blijft alleen als compatibele capaciteitsrichtwaarde bestaan. De worker
   accepteert daarbij maximaal 512 in plaats van 30 startsnapshots; een test
   bewaakt continuïteit boven dertig onafhankelijke tracks.
5. Afstanden tot radarfootprints gebruiken punt-in-polygoon en de
   dichtstbijzijnde polygonrand. Een target midden in een grote footprint
   heeft dus afstand nul, ook wanneer alle vertices meer dan acht kilometer
   verder liggen.
6. Natte Netatmo-, Synoptic- en METAR-waarnemingen worden per storm-ID
   gegroepeerd. Twee stations onder verschillende buien kunnen samen niet
   langer één van beide bevestigen. Eén station wordt aan hoogstens één
   eenduidige storm gekoppeld; gelijke overlappende footprints blijven
   expliciet ambigu.
7. De targetsensor accepteert systeembevestiging alleen als de bevestigde
   storm-ID gelijk is aan het geselecteerde/getrackte systeem en gebruikt
   score, confidence en bronbasis van precies die storm. Lokale regen blijft
   wel als lokale waarneming zichtbaar.
8. Queue-full, identificeerbare ongeldige en verlopen workerjobs eindigen met
   een terminale MQTT-status. Handler en dispatcher blijven niet foutief op
   `working` staan.
9. Groundobservaties tellen per canonieke fysieke site. Hetzelfde
   METAR/Synoptic-station kan dus geen twee onafhankelijke stemmen leveren.
   Een stormobject matcht bovendien tegen het radarframe dat het dichtst bij
   het meettijdstip ligt; een historische samengestelde hull wordt niet meer
   als actuele grondbevestiging gebruikt. Alle losse systeemcontouren van
   hetzelfde radartijdstip tellen afzonderlijk mee, zonder fictieve
   verbindingszone tussen twee cellen.
10. Sensor-propertyreads zijn side-effectvrij. Ground-auditrecords ontstaan
    alleen tijdens een expliciete state-update en worden per target/minuut
    gededupliceerd.
11. MQTT QoS1-redelivery van dezelfde workerjob is idempotent. De worker
    herpubliceert queued/running of het gecachete terminale resultaat en HA
    activeert daardoor niet langer ten onrechte lokale fallback terwijl de
    oorspronkelijke job nog rekent.
12. Een blijvende precommitfout kan de OFE niet meer in een onbeperkte
    head-of-line retry- en logstorm zetten. Retries hebben exponentiële
    backoff, nieuwere data mag tijdens die backoff door, gemengde batches
    worden atomair gesplitst en blijvend foute records blijven zichtbaar in
    quarantaine met expliciete retry op dezelfde ID. Het circuit gebruikt
    opeenvolgende fouten sinds het laatste geldige deelresultaat: verspreide
    poisonrecords kunnen geldige siblings daardoor niet meer mee blokkeren.
    Een apart callback- én wandtijdbudget per lineage-epoch pauzeert alleen de
    nog onopgeloste deelbatches en hervat ze automatisch na cooldown; niets
    wordt wegens CPU- of logdruk verwijderd.
13. Home Assistant biedt de operatorservice
    `storm_tracker_v3.retry_quarantined_observations`, voor één RegionEngine
    of alle actieve regio's. De Region Engines-sensor publiceert pending,
    retry-, cooldown- en quarantaine-aantallen.
14. De afsluitvolgorde is één geordende barrière: nieuwe polls en services
    sluiten, een actieve lifecyclefetch wordt geannuleerd, hogere
    providercycli lopen uit, calibrationdata wordt geflusht en pas daarna
    worden OFE's gereset en trackingstates opgeslagen. Op recente Home
    Assistant-versies draait dit als een door Core awaited shutdown job; de
    oude stop-eventroute blijft als compatibiliteitsfallback bestaan.
15. De worker voert zware jobs strikt serieel uit in één persistent
    `spawn`-childproces. Een absolute deadline, childcrash of shutdown stopt
    dat proces hard, terminaliseert de betrokken job en laat de volgende job
    met een schoon childproces doorgaan. Lazy handlerimports behouden caches
    tussen normale jobs zonder de zware provider- en NumPy-modules in de
    MQTT-parent te laden. Accepted-, terminal-, completed-, failed-,
    expired- en cancelled-counters maken de queuebalans controleerbaar;
    eenmaal terminale jobstatus kan niet meer door een laat niet-terminaal
    bericht worden teruggedraaid.
16. Iedere geaccepteerde observation wordt vóór zware verwerking in een
    afzonderlijk crash-durable SQLite-journal vastgelegd. `RECEIVED`,
    `DEDUPED`, `BATCHED`, `PREPARED`, `EXECUTING`, `RESULT_STAGED`,
    `COMMITTED`, `PARTITIONED` en `QUARANTINED` hebben expliciete,
    controleerbare overgangen. De versioned trackinghead en het commitreceipt
    worden in één transactie geschreven; recovery kan daardoor uitsluitend de
    oude of de volledig nieuwe state zien.
17. Regionale opslagidentiteit is niet langer afgeleid van afgeronde
    coördinaten. Een duurzame registry koppelt targets en kleine GPS-jitter aan
    dezelfde permanente regio-ID. Provideradmission en routing blijven bij
    opstart gesloten tot journalintegriteit, staged results, prepared batches,
    quarantainelineage en ontvangen backlog van die regio zijn hersteld.
18. Workeracceptatie en terminale resultaten hebben nu een lokale
    SQLite-receiptstore onder `/data`. De requestdigest bindt een stabiele
    job-ID aan jobtype en payload; een andere payload met dezelfde ID faalt
    gesloten. Na een workerrestart worden terminale resultaten opnieuw
    geleverd en achtergelaten queued/running jobs expliciet als `cancelled`
    geterminaliseerd. HA herpubliceert bij transportstilte maximaal één
    byte-identiek QoS1-request met dezelfde ID.
19. Blitz-observaties worden niet meer via een los, niet-afgewacht taskvenster
    naar de OFE gestuurd. De async callback wacht tot duurzame ontvangst is
    bevestigd voordat de MQTT-handler verdergaat. De publieke Blitz-feed zelf
    blijft echter QoS0 en kan dus geen upstream replay na een onderbroken
    verbinding garanderen.

## Validatie

- Baseline vóór P0: `602 passed, 1 skipped`
- OFE retry-/cancellation-/poison-/epoch-/durable-ingesttests: groen
- StormEngine transactie-/geometrietests: `50 passed`
- Groundmatchingtests: `16 passed`
- HA/worker tracking- en geometriepariteit: `2 passed`
- Durable-journaltests, inclusief echte abrupte `os._exit` op twaalf
  SQL-checkpoints: `42 passed`
- Worker service- en bridgetests, inclusief restartreceipt en
  idempotencyconflict: `28 passed`
- CellTrack-shadowcontractreplay: 24 frames, twee bekende cellen en alle 92
  bipartiete candidates behouden; synthetisch, dus geen live meteorologisch
  bewijs
- RawArtifactStore exact-byte/idempotentie/schema-/inhoudsvalidatie en vijf
  echte procescrashgrenzen: `24 passed`; primitive nog niet aan providers
  gekoppeld
- Volledige suite na durable P0, CellTrack, associationgraph en
  RawArtifactStore: `809 passed, 1 skipped`
- Python compilecontrole: geslaagd
- `git diff --check`: geslaagd
- HA/worker trackingcore-parity: inhoudelijk identiek
- Polygonimplementatie HA/worker-parity: getest

## Deployartefacten

### Home Assistant-integratie

Het eerdere 1.12.0-archief dateert van vóór trackingprotocol v4 en is
achterhaald. Een nieuw testarchief wordt pas na de resterende live
foutinjectie- en releasegates gebouwd; er is op dit moment bewust geen
deploybare SHA-256 vastgelegd.

### Worker

Het eerdere 1.14.0-archief is door trackingprotocol v4 achterhaald. Een nieuw
1.15.0-testarchief wordt pas na de resterende live foutinjectie- en releasegates
gebouwd; er is op dit moment bewust geen deploybare SHA-256 vastgelegd.

## Baseline en rollback

Het herstelpunt staat beschreven in `docs/P0_BASELINE_2026-07-29.md`.
De lokale bronsnapshot en getrackte patch staan onder `.analysis/`.
De live Home Assistant- en kalibratiedatabases zijn niet gewijzigd en zitten
niet in die lokale snapshot.

## Resterende P0-releaseblockers

Deze kandidaat mag nog niet live. De duurzame kern is lokaal geïmplementeerd,
maar de resterende releasegates zijn:

- brokerdisconnect, QoS1-redelivery, laat resultaat, HA-restart,
  workerrestart, addon-containerkill en host/power-loss moeten nog end-to-end
  op de echte HA/MQTT/addonroute worden bewezen. De lokale exceptionmatrix en
  abrupte subprocessmatrix zijn groen, maar vervangen die live proef niet;
- schema v3 is bewust fail-closed. Er moet vóór uitrol een expliciet
  migratie-/rollbackbesluit komen voor iedere eventueel gebruikte
  pre-release durable schema-versie en voor een bestaande of beschadigde
  worker-receiptdatabase. Ook volle schijf en SQLite-corruptie moeten worden
  gefault-injecteerd;
- externe side effects van een op de deadline gedode workerhandler vereisen
  per jobtype een expliciet idempotentie- of compensatiecontract;
- de publieke Blitz-MQTT-feed gebruikt QoS0. Zodra een bericht de callback
  bereikt is de lokale ontvangstbarrière duurzaam, maar verlies vóór die
  callback kan zonder upstream replay niet technisch worden uitgesloten;
- providergeometrie losse ringen/MultiPolygon niet overal end-to-end bewaart;
  de groundmatcher kan ze nu correct verwerken, maar een provider mag ze
  voordien nog niet flattenen;
- harde associatie-/snelheidsgates nog identity loss kunnen veroorzaken;
- de resourcegroei na het opheffen van `MAX_STORMS` nog met spatial indexing,
  backpressure zonder dataverlies en een piekreplay moet worden begrensd;
- de New York-groundcase, HA-eventside-effects, durable recoverydiagnostiek,
  restartmatrix en langdurige resource-/temperatuursoak nog met echte data
  moeten slagen.

## Nog bewust niet opgelost na P0

- De eerste CellTrack-shadowprimitive en verliesvrije candidategraph zijn
  lokaal aanwezig, maar detector, optical-flowpredictions, globale assignment,
  birth/death/split/merge, duurzame tracks en een afzonderlijke
  WeatherSystem-aggregatielaag zijn nog niet operationeel verbonden.
- Vervanging van harde bewegings- en associatiegrenzen door probabilistische
  gating met expliciete onzekerheid.
- Een duurzame `PhysicalSite`-catalogus met provider-onafhankelijke identiteit;
  de huidige P0-code dedupliceert al expliciete IDs, dezelfde ICAO-code en
  vrijwel samenvallende stations, maar dat is nog geen wereldwijde catalogus.
- Volledige provider skill-score per locatie, product, neerslagregime en
  lead time.
- Append-only observation/event store met reproduceerbare replays en
  complete provenance. Een exacte-byte `RawArtifactStore`-primitive bestaat
  lokaal, maar provideradapters, veilige upstream record-ID's, retentie,
  quota, restore en dual-write zijn nog niet aangesloten of gevalideerd.
- Operationele kalibratie op voldoende verse echte cases en onafhankelijke
  hindcasts.

Deze punten horen bij de volgende roadmapfasen; P0 mag daarom niet worden
gepresenteerd als een volledig gevalideerde meteorologische eindversie.
