# Storm Tracker Worker

Deze Home Assistant App voert Storm Tracker-werk buiten Home Assistant Core
uit. Worker 1.17.0 gebruikt op Home Assistant standaard de expliciete modus
`worker_only`. In die modus haalt uitsluitend de worker de
geselecteerde bronnen op en bewaart hij de exact ontvangen upstreambytes;
Home Assistant voert geen providers, decoders, meteorologische berekeningen
of Storm Tracker-databasewerk uit.

De RAW-modus heeft 27 cadencebronnen met gesloten handler- en herstelbindingen.
Blitzortung is een afzonderlijke, expliciet geautoriseerde event-ingang en is
in het geselecteerde cadence-only doelprofiel uitgeschakeld en niet als
capaciteit geclaimd. KMI
native RADQPE blijft fail-closed zolang een echt leveringscontract en toegang
ontbreken. Een duurzame RAW-commit is terminaal: downstreamverwerking,
meteorologische toelating, dashboardoutput en waarschuwingen blijven uit.
Automatische reclamation is eveneens uit; een volle store stopt fail-closed.
Een afzonderlijke read-only retentieguard bewaakt de cadence-opslag, ledger,
catalogi en operationele hostreserve. De expliciete offline reclamationcontroller
kan alleen na operatorreview en archief-/holdbewijs door PREPARE, COMMIT en APPLY;
hij is niet aan de live service gekoppeld. Doelactivatie voor het cadence-only
profiel vereist gepinde configuratie en authority plus ten minste
20.694.695.936 bytes aantoonbare vrije capaciteit, inclusief 2 GiB vrije
hostreserve. Deze kandidaat is nog niet
op een doel geactiveerd of als productieklare meteorologische release
vrijgegeven.

Trackingprotocol v4 bindt een resultaat aan batch-ID, startrevision en exacte
input-/statehashes. MQTT QoS1-redelivery voert een job niet opnieuw uit:
queued/running wordt opnieuw gemeld en een reeds voltooid resultaat wordt uit
de hot cache of crash-durable SQLite-receiptstore herpubliceerd. De requesthash
bindt een stabiele job-ID aan exact één jobtype en payload. Een workerrestart
terminaliseert achtergelaten accepted werk expliciet als `cancelled`, zodat
accepted jobs niet onzichtbaar blijven zweven.

De geïsoleerde fase-3-replay gebruikt uitsluitend synthetische testframes en
publiceert niets naar operationele sensoren, kaarten of meldingen.

Daarnaast bestaan twee **niet operationeel gekoppelde shadowprimitives**:

- `CellDetection`/`CellTrackSnapshot` meet na een externe koppeling de exacte
  tweeframevector plus statecovariantie zonder speed cap;
- de associationgraph bewaart ieder celpaar als candidate en gebruikt zijn
  geometrische score niet als probability of automatische identity.

De `RawArtifactStore` bewaart exacte upstreambytes content-addressed,
idempotent en crash-atomair, inclusief volledige multi-artifactsets met
immutable membership, provenance- en manifestchecksums. De RAW-shadowruntime
bindt alle 27 cadencebronnen aan deze store en herstelt een reeds ontvangen
capture zonder stille refetch of gedeeltelijk succes. OPERA gebruikt daarbij
een begrensde streamingroute; grote payloads worden niet volledig in het
geheugen geladen.

Die KMI-PNG is een gerenderde categorie-afbeelding zonder gecertificeerd
kwantitatief neerslagcontract. Ze blijft daarom
`uncertified_visualisation_only`: nul operationele radarobservaties, geen
mm/u-claim en geen toelating tot meteorologische fusie. De producttijd uit het
KMI-frame blijft afzonderlijk van de ontvangstmomenten van beide downloads.

De CellTrack-contractreplay gebruikt 24 frames en markeert expliciet dat hij
synthetisch is en geen globale assignment of echt weer valideert. De RAW-store
heeft harde object- en totaalcapaciteitsgrenzen, maar nog geen automatische
productieretentie of eviction; een overschrijding weigert de volledige capture
zonder gedeeltelijk succes. RAW-opslag is geen bewijs dat een bron
meteorologisch betrouwbaar, gekalibreerd of operationeel toegelaten is.

Na installatie en start hoort `sensor.stv3_worker_status` binnen circa tien
seconden `online` te worden. Iedere dertig seconden controleert de integratie
met een lichte ping ook het volledige opdracht- en antwoordpad.

Het dashboard toont voor MQTT, de taakverdeler en iedere geregistreerde
rekentaak een afzonderlijke heartbeat, werkstatus en foutteller.

## CPU-begrenzing

Om de impact op HA/Z2M-latency te beperken heeft de worker drie expliciete
profielen:

- `shared_ha` (standaard): maximaal 25% CPU en minstens nice 15 voor een NUC
  waarop HA en Zigbee2MQTT voorrang moeten houden
- `dedicated_nas`: maximaal 50% CPU en minstens nice 10 op de aparte NAS-HA
- `dedicated_nas_fast`: maximaal twee volledige CPU-cores (cpulimit 200) en
  instelbare nice 0..19 op de geisoleerde NAS-testhost
- `STV3_CPU_THREADS = 2`
- altijd minstens één volledige fysieke core buiten de worker-CPU-set
- `heartbeat_seconds: 10` voor lichte MQTT-liveness
- `provider_cycle_seconds: 2..300` voor de zware provider-/snapshotcyclus;
  `shared_ha` en `dedicated_nas` bewaren een minimum van 30 seconden,
  `dedicated_nas_fast` laat voor de geïsoleerde NAS-grenstest 2 seconden toe

`STV3_CPU_THREADS` is nog steeds een bovengrens; pure Python/JSON-paden blijven
in de praktijk vaak op één core draaien.

De workerqueue verwerkt bewust één zware taak tegelijk. Een ongewijzigde
statusheartbeat start geen providerwerk; alleen de aparte operationele cyclus
of een echte statuswijziging maakt de zware runtime wakker.
De CPU-limiet geldt samen voor het hoofdproces, threads en geïsoleerde
kindprocessen. Daardoor kan ook één pure-Python-taak niet langer een volledige
logische CPU blijven gebruiken.

Daarnaast krijgt de queue prioriteit voor HA-/Z2M-gerelateerde taken:

- `ping`
- `netatmo_fetch`
- `synoptic_fetch`
- `metar_fetch`

Daardoor raken die paden sneller opnieuw in behandeling als de wachtrij onder druk staat.
