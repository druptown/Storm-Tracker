# Science-shadow hard process boundary v1

## Status en afbakening

`storm_tracker_worker/science_shadow_process_isolation.py` is een expliciet
aanroepbare, worker-only kandidaat rond
`DurableScienceShadowOrchestrator`. De module is niet geïmporteerd door
`service.py`, MQTT, Home Assistant, een scheduler of de operationele
snapshotketen. Zij publiceert niets en verleent geen meteorologische admission.

De parent gebruikt voor iedere verse science-run één nieuw `spawn`-proces via
de bestaande `HardDecoderExecutor`. De expliciete IPC is uitsluitend canonical
JSON. Er gaat geen raster, CellTrack-state, providerpayload, fouttekst of
credential over de pipe. Python `multiprocessing` gebruikt intern pickle voor
zijn eigen spawnbootstrap van de reeds door de parent gemaakte `bytes`; de
sciencecode accepteert of decodeert nergens een externe pickle.

## Parent-owned authority

Een run wordt alleen gestart met één absolute authority-file en de vooraf
bekende SHA-256 daarvan. Het canonical JSON-document bindt exact:

- engine-ID en volledig runtime-profilefingerprint;
- science/checkpoint/retentielimieten;
- hard wall-, CPU-, RSS-, authority-, request-, result- en pathlimieten;
- een checksummed, parent-audited factorycontract;
- bounded factoryconfig zonder secret-achtige sleutels;
- maximaal 32 genormaliseerde absolute paden, ieder met toegangstype;
- het exact als `transactional_file` geautoriseerde SQLite-receiptpad.

Een `read_file` krijgt bovendien een content-SHA. Symlinks, relatieve paden,
afwezige paden, dubbele labels/paden, een andere factory-SHA, een stale
authority-SHA en ieder absoluut configpad buiten de autorisatielijst falen vóór
spawn. De child leest dezelfde authority opnieuw en herhaalt alle controles.

Het factorycontract moet expliciet bevestigen dat de vaste importtarget alleen
geautoriseerde paden opent, geen descendants maakt, hard-kill-safe is en als
enige durable side effect de transactionele receiptstore gebruikt. Dit is een
audited-codecontract, geen algemene Windows/Linux filesystemcontainer. Een
productief factorycertificaat en eventuele kernel-MAC/filesystemnamespace zijn
dus aparte admissionvragen; deze kandidaat beweert niet dat willekeurige
kwaadaardige native factorycode door een pad-sandbox wordt tegengehouden.

## Vaste standaardgrenzen

- wall: **90 s**;
- CPU: **60 s**;
- RSS/process memory: **1 GiB**;
- authority: **256 KiB**;
- IPC-request: **32 KiB**;
- IPC-resultaat: **64 KiB**;
- geautoriseerde paden: **32**;
- terminate/kill grace: exact **0,35 s**.

De code heeft daarnaast niet-verruimbare hard ceilings: wall/CPU 3.600 s, RSS
64 GiB, authority 1 MiB, science-request 256 KiB, science-result 1 MiB en 64
paden. De authority moet exact dezelfde limieten bevatten als de parent; de
child kan ze niet verruimen.

Op Linux worden `RLIMIT_AS`, `RLIMIT_CPU`, core-dumpverbod, `PDEATHSIG`, een
eigen processgroep en parent `killpg` gebruikt. Op Windows gebruikt de parent
een Job Object met process-memory, process-CPU en `KILL_ON_JOB_CLOSE`; de target
wordt pas vrijgegeven nadat de child aan het Job Object is gekoppeld. De
generic executor houdt bovendien één thread- én cross-processslot aan. Een
concurrerende sciencecall wordt dus expliciet `busy` en mag niet parallel tegen
de state-head schrijven.

## Preëmptieve terminalstatus

De parent wacht nooit onbeperkt op coöperatieve Pythonchecks. Tracking die in
`advance_cell_track_engine()` blijft hangen of CPU blijft verbranden wordt van
buiten het childproces afgebroken. De parent onderscheidt:

- `success`: childreceipt is byte-semantisch gelijk aan de opnieuw geladen
  durable receipt;
- `deadline`: harde wall deadline;
- `resource`: CPU, RSS of outputlimiet;
- `crash`: abnormale exit of sciencefout;
- `corrupt_contract`: authority-, request-, store- of IPC-contractbreuk;
- `cancelled`: parentcancel vóór spawn of hard kill tijdens uitvoering;
- `busy`: het exclusieve isolatieslot is bezet;
- `boundary`: de vereiste kernelgrens kon niet veilig worden gezet.

Foutdetails zijn vaste codes. Paden, exceptiontekst, traceback en secrets komen
niet in `ScienceShadowProcessResult` terecht. Het resultaat draagt altijd
`meteorological_admission_granted=false` en
`operational_snapshot_claim_generated=false`.

## Kill/commit-race en idempotency

De pipe is nooit state-authoriteit. Na **iedere** terminal childstatus opent de
parent zelfstandig de exacte geautoriseerde receiptstore. Hij zoekt eerst het
attempt met de exacte request-SHA en daarna, conform het bestaande
orchestratorcontract, een reeds advanced canonical run. Iedere gevonden row
wordt door de store volledig op schema, checksum, engine, runtimeprofiel, head
en provenance gevalideerd.

Daarom bestaan bij timeout, cancel, CPU/RSS-kill of crash slechts drie eerlijke
durable dispositions:

- `not_committed`: de oude head is volledig behouden;
- `committed`: de volledige atomische receipt/state/headtransactie is zichtbaar
  en de parent levert bounded, checksummed receiptmetadata;
- `unknown`: de store zelf kon niet veilig worden gevalideerd.

Een kill vóór SQLite-commit laat geen nieuwe head achter. Een kill nadat
`COMMIT` duurzaam is maar vóór de pipe-response levert de technische
deadline/resource/crashstatus én `committed`; een retry vindt exact dezelfde
receipt. Een nieuwe attemptkey voor dezelfde reeds advanced canonical run is
eveneens een exacte duplicate. Buiten het door de checkpointstore gegarandeerde
retentievenster blijft de bestaande expliciete uitkomst `window_expired` gelden.

## Bewijs en resterende blockers

De gerichte tests gebruiken het echte detector-/CellTrack-/checkpointpad met
een synthetische, deterministische upstream composer. Ze bewijzen Windows
spawn, happy path, echte preëmptieve trackingdeadline, CPU/RSS/outputgrenzen,
crash, cancel, kill vóór en na commit, duplicate/restart, stale authority,
ongeautoriseerd configpad, corrupte IPC, exclusieve concurrency, privacy en
resourcecleanup. De bestaande science-suite bewijst afzonderlijk SQLite-CAS,
checkpointfaults, tamper/gap/checksum/lineage en langdurige retentie.

Niet opgelost en daarom niet operationeel:

- geen service/MQTT/HA/scheduler/admission-wiring;
- geen productie-factorycertificaat of kernel-filesystemcontainer;
- Linux-codepad is contractueel aanwezig maar in deze Windows-werkruimte niet
  uitgevoerd;
- geen meteorologische admissioncertificaten;
- geen operationele snapshotpublicatie.

