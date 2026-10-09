# Worker-first runtimecompositie — uitvoeringscontract

Status: implementatiespecificatie; niet live gewired en geen vrijgavebewijs

## Doel

Deze compositie vervangt de losse shadoweilanden door één worker-owned
levenscyclus zonder meteorologische verwerking in Home Assistant. Zij mag pas
worden geactiveerd wanneer iedere hieronder genoemde stage haar eigen lokale
contract en foutmatrix heeft gehaald. Een ontbrekende of ongeldige stage maakt
de bijbehorende operationele feiten `unknown`; zij activeert nooit een legacy
HA-fallback.

## Eén eigenaar en één zware uitvoeringsbaan

De worker bezit exact één `WorkerFirstRuntimeCoordinator`. Die instantie bezit
of krijgt onvervangbaar geïnjecteerd:

1. de actuele `DesiredStateStore`;
2. de autonome acquisitieplanner en `AcquisitionShadowRuntime`;
3. een duurzame pipeline-outbox met stagecursor;
4. providergebonden kwantitatieve decoders en gridcertificaten;
5. de harde science-processrunner;
6. de harde ground-fusion-processrunner;
7. de toelatingsbeslisser per operationeel feit;
8. de bestaande operationele snapshotstore, outbox en QoS 1-publicatie.

Constructie vereist daarnaast een reeds gehouden cross-process
`WorkerFirstOwnershipLease` en exact zes absolute storebindings
(completion, desired state, fact, snapshot, pipeline-output en outbox), elk met
immutable schema-/profile-SHA. De runtime assert die authority vóór en na elke
duurzame grens. Stop geeft haar pas vrij nadat processor, alle overige mutable
futures en de snapshotruntime aantoonbaar stil zijn; een onbevestigde stop houdt
de fence fail-closed vast.

Acquisitie, decode, tracking, groundfusion en andere zware jobs delen één
globale proces-/leaseslot. Er mag nooit tegelijk een autonome en een door HA
aangevraagde zware child rekenen. MQTT-callbacks leggen alleen begrensde input
vast en wekken de coordinator; zij wachten niet op download, SQLite-scan,
decode of tracking.

De add-on blijft begrensd op twee numerieke threads, `nice 15` en een
CPU-affiniteit die minstens één volledige fysieke core vrijlaat voor Home
Assistant en Zigbee2MQTT. De seriële pipeline is geen toestemming voor een
onbegrensde wachtlijst: iedere stage heeft een duurzame row-/bytecap,
deadline, cadence en expliciete overflowreceipt.

## Duurzame stagetransities

Iedere overgang gebruikt de exacte huidige desired-state-SHA en een immutable
inputdigest. De producer schrijft eerst zijn resultaat en terminale receipt;
pas daarna mag de consumerstage zichtbaar `ready` worden.

| Van | Naar | Verplichte autoriteit vóór overgang |
| --- | --- | --- |
| acquisitiejob | exacte rawcapture | raw object- en capturehash, capacitycommit, terminal executionreceipt |
| rawcapture | canonical radar-/groundevidence | expliciete provider/product/collector-handoff, decoderreceipt, quantity/unit/tijd/grid/QC-certificaat |
| canonical radar evidence | cell-/trackrevision | volledige pixelaccounting, transformcertificaat, hard-processreceipt, atomair science-receipt/checkpoint |
| canonical ground evidence | cell-specifieke groundrevision | fysieke-site/capability/alias/footprintcertificaten, hard-processreceipt, volledige disposition-accounting |
| committed facts | operationele snapshot | fact-specifieke admissionreceipts, actuele desired-state-SHA, complete revision en geldige TTL |

Een crash tussen twee kolommen geeft na herstart uitsluitend een opnieuw
aanbiedbare producerreceipt of een volledig aanwezige consumerreceipt. Een
input kan nooit door alleen een in-memory callback als verwerkt gelden.

## Pipeline-outbox

De coordinator vereist één checksummed SQLite-outbox met minstens:

- `pipeline_item_id`, stage, immutable input-ID en input-SHA;
- desired-state-SHA en authority epoch;
- producttijd, ontvangttijd, deadline en cadence;
- attempt, lease-owner/token/expiry en terminale disposition;
- producerreceipt-SHA, consumerreceipt-SHA en resultaat-SHA;
- create/update sequence en een checksum over de volledige row.

De unieke sleutel bindt stage plus inputdigest. `BEGIN IMMEDIATE` en
compare-and-swap voorkomen dubbele consumers. Recovery trekt verlopen leases
in, verifieert eerst alle producerreceipts en geeft alleen contiguous geldige
items vrij. Oude desired-state-items worden duurzaam `superseded`; hun data
blijft auditbaar maar mag geen actuele snapshot meer beïnvloeden.

Prioriteit is vooraf vast: terminalrecovery, expirerende actuele observaties,
HA-controlerequests, actuele acquisitie, science, ground en ten slotte
achtergrondkalibratie/replay. Prioriteit verandert geen deadline of
meteorologische inhoud en mag geen item verwijderen.

## Fact-specifieke toelating

Toelating geldt nooit voor “de provider” of “het systeem” als geheel. Zij is
gebonden aan provider, product, decoder, grid, regio, tijdvenster en feitsoort.

- `current_precipitation=raining` vereist verse, ruimtelijk dekkende positieve
  radarevidence of geldige natte grondmeting met volledige provenance.
- `current_precipitation=dry` vereist verse volledige coverage, geldige
  no-data/absence-semantiek en waar van toepassing een sensorcapability die
  afwezigheid werkelijk kan waarnemen.
- `ground_confirmation=true` vereist hetzelfde actuele CellTrack en dezelfde
  footprint/tijdrevision; losse natte stations elders bevestigen niets.
- `next_threat=none` vereist een volledige kandidaat- en footprintscan. Een
  cap, deadline, stale bron of ontbrekende regio maakt dit `unknown`.
- kans, ETA en `expected` vereisen een onafhankelijk geëvalueerd empirisch
  calibratie-ID. Een lineaire extrapolatie zonder dat bewijs blijft een
  diagnostische kandidaat en geen operationele kans.
- UI-storms zijn compacte diagnostiek, geen onafhankelijke waarheid. Hun
  snelheid is een trackschatting met uncertainty/provenance en wordt nooit
  door een vaste meteorologische snelheidsafkeur “realistisch” gemaakt.

## Snapshotcommit

De rich-snapshotbuilder leest één immutable committed revision, nooit losse
laatste rows uit verschillende stages. Iedere targetclaim verwijst intern naar
exacte science-, ground-, forecast- en admissionreceipts. De builder publiceert
alleen de bestaande begrensde `operational_snapshot_v1`; bulkgeometrie blijft
in een content-addressed mapartefact.

Wanneer niet alle targetclaims dezelfde authority dragen, wordt voor die
target een complete unknown-projectie gebouwd. Een deelclaim mag niet worden
samengevoegd met oudere “goede” output. Publicatie is pas committed na echte
QoS 1 PUBACK; timeout/disconnect houdt dezelfde bytes pending voor replay.

## Linearization points en completion-acksemantiek

De keten gebruikt meerdere duurzame stores en pretendeert daarom geen
schijn-atomaire compare-and-swap over afzonderlijke SQLite-WAL's. Iedere
authorityovergang heeft één lokaal, controleerbaar linearization point:

| Overgang | Linearization point |
| --- | --- |
| desired state Dn wordt actief | de SQLite-commit van `DesiredStateStore.apply()` |
| pipeline-output wordt duurzaam | de commit van het exacte output-/processreceipt |
| factrevision wordt `ready` | de commit van `OperationalFactRevisionStore.stage()` |
| snapshotrevision bestaat | de commit van `OperationalSnapshotStore.commit()` |
| snapshot is extern gepubliceerd | `PublicationOutboxStore.mark_published()` na de echte publisher-PUBACK |
| HA accepteert een snapshot | vervanging van de immutable cache-head onder de cachelock, na controle van de verwachte desired-state-SHA |
| completion is logisch geconsumeerd | de commit van de immutable row in `consumption_receipts` |
| completioncursor maakt voortgang zichtbaar | de latere cursorcommit; dit is recoveryvoortgang, niet de completion-ack zelf |

`acknowledge()` betekent dus uitsluitend: *de output voor dit immutable
completion-event is duurzaam ontvangen*. Het betekent niet: *de desired state
van dat event is op ditzelfde nanoseconde nog de globale actuele desired
state*. Het event, producerreceipt, outputreceipt, factbinding en iedere
eventuele snapshot blijven wel allemaal aan exact dezelfde Dn-SHA gebonden.

De laatste desired-state-read vlak vóór `acknowledge()` is een fail-closed
observatie die de meest voorkomende authoritywissel vroeg terminaliseert. Zij
is geen linearization point. Wanneer D2 of tijdelijk `None` precies binnen de
volgende `acknowledge()`-aanroep ontstaat, mag het oude D1-event nog een D1-
consumptionreceipt krijgen. Dat kan geen D1-meteorologie onder D2 activeren:

1. een eventuele D1-factsnapshot was vóór de ack al als exact D1-fact en exact
   D1-outboxreceipt committed;
2. de HA-authoritywissel maakt de bestaande cache onmiddellijk stale;
3. een late D1-delivery faalt op de verwachte D2-SHA en kan de cache-head niet
   vervangen;
4. de volgende worker-tick publiceert een D2-unknown of een exact toegelaten
   D2-fact; bij `None` blijft de runtime fail-closed `unconfigured`;
5. een volgende echte D2-completion wordt normaal als D2 verwerkt en
   geacknowledged; na de eerste D2-snapshot kan geen D1-snapshot de actuele
   cache terugwinnen.

De crashmatrix rond de ack is daarom:

| Crashgrens | Duurzame waarheid na herstart | Verplicht herstelgedrag |
| --- | --- | --- |
| vóór de consumptionreceipt-commit | geen ack | lease verloopt/herstelt en exact hetzelfde event blijft aanbiedbaar |
| `ack.after_receipt_commit`, vóór terminale state/cursorcommit | één immutable consumptionreceipt; outbox kan nog `leased` en cursor N-1 zijn | `claim_next()` reconcilieert receipt → `consumed` en schuift de contiguous cursor naar N zonder processor-redelivery |
| na terminale state/cursorcommit | receipt, `consumed` en cursor N | idempotent geen redelivery |
| desired D2/`None` precies binnen ack | oude eventreceipt blijft aan D1; actuele desired authority is D2/afwezig | oude meteorologie nooit als D2 presenteren; volgende tick convergeert fail-closed |

Een sterker, ander contract — “ack mag alleen committen wanneer Dn op dat
exacte commitmoment nog globaal actief is” — vereist dat desired-head en
completionreceipt in één control-ledgertransactie wonen. Een CAS tussen de
huidige afzonderlijke databases zou geen crash-atomaire garantie geven. Dat
sterkere contract is voor meteorologische correctheid niet vereist zolang de
bovenstaande SHA-binding, outbox-PUBACK en HA-stale/expected-SHA-invarianten
behouden blijven.

## Start- en stopvolgorde

Startup is fail-closed en exact geordend:

1. CPU-/thread-/filesystemgrenzen valideren;
2. desired-state, pipeline-outbox en capaciteitsledgers openen en auditen;
3. rawcapture-reconciliatie uitvoeren;
4. decoder-/science-/groundreceipts en checkpoints herstellen;
5. snapshotstore en publicatie-outbox herstellen;
6. stale/obsolete authority terminaliseren;
7. pas daarna nieuwe acquisitie en snapshotadmission openen.

Shutdown sluit eerst nieuwe admission en MQTT-jobacceptatie. Daarna wordt een
actieve child begrensd afgerond of hard geannuleerd, worden receipts en stores
geflusht, stopt de snapshotruntime en wordt pas als laatste MQTT verbroken. Een
offline/LWT-status maakt de HA-cache stale; shutdown publiceert geen fictieve
droge snapshot.

### Begrensde control- en stopgrenzen

Runtimecontract `stv3-worker-first-runtime-v3` geeft iedere blocking
controloperatie een expliciete deadline (standaard 30 s, configureerbaar tot
maximaal 300 s) en iedere stopaanroep een onafhankelijke korte wall-clockcap
(standaard 2 s, maximaal 30 s). `async_stop()` sluit admission en zet het
processor-cancelsignaal voordat het op de seriële gate wacht. Daardoor wacht
stop nooit impliciet tot de langere processingdeadline.

Een timeout of externe cancellation annuleert alleen de waiter, niet de
onderliggende thread of snapshottaak. Die echte operatie blijft geregistreerd
als mutable authority. Zolang zij niet werkelijk klaar is:

- start geen tweede tick, claim, verify, ack, reject of snapshotoperatie;
- blijft de cross-process ownershiplease gehouden;
- wordt een late durable ack/reject exact één keer gevalideerd en in de
  terminalisatietellers gereconcilieerd;
- blijft stop diagnostisch `stopped_pending_*` en kan stop na echte quiescence
  veilig worden herprobeerd.

Snapshot start/tick/publicatie/stop worden als afzonderlijke shielded taken
gevolgd. Alleen een gevalideerde `stopped`-status maakt de snapshotauthority
quiescent. Externe cancellation van stop kan de taak dus niet wees maken of de
fence vroeg vrijgeven.

Dit contract pretendeert geen thread-kill. Een werkelijk oneindige Python-
thread of snapshotcoroutine houdt bewust de ownershiplease vast tot het proces
eindigt. Zware decode/science/groundcompute moet daarom uitsluitend via de
harde processrunners met parent-enforced wall/CPU/RSS/output/tree-limieten
lopen. De in-process controlbaan is alleen voor begrensde duurzame
storeoperaties en coöperatieve snapshot-I/O.

## Verplichte integratietests vóór enige wiring in `service.py`

1. Eén proces/slot bij gelijktijdige HA-job, acquisitie, science en ground.
2. Exacte raw→decode→cells→track→ground→snapshot-replay over restart.
3. Kill vóór en na ieder producercommit geeft geen verlies of duplicaat.
4. Desired-statewijziging op iedere stage publiceert nooit oude authority.
5. Queuecap, deadline, disk-full, corrupte row/store, MQTT-disconnect en
   PUBACK-verlies blijven fail-closed en recoverybaar.
6. Een niet-toegelaten provider/product kan wel exact raw worden bewaard maar
   nooit een meteorologische snapshotclaim genereren.
7. HA worker-only setup veroorzaakt nul provider-, decoder-, tracking-,
   ground-, kalibratie- of meteorologische databasecalls.
8. Een representatieve multi-frame replay houdt volledige detectie-,
   kandidaat-, lineage- en pixelaccounting en blijft binnen CPU/RSS/tijdcaps.
9. Baseline- en kandidaatsoak voldoen aan het vergrendelde HA/Z2M-protocol.
10. Herhaalde setup/unload/reconnect bezit steeds exact één coordinator en
    laat geen thread, child, lease of MQTT-listener achter.

Pas nadat deze integratietests en de onafhankelijke meteorologische
admissiongates groen zijn, mag `service.py` de coordinator onder een expliciete
versiegebonden runtimeconfig construeren. Tot dan blijft deze compositie lokaal
en shadow-only.
