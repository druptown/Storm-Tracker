# Harde procesisolatie voor kwantitatieve decoderjobs

Status: **lokaal/shadow-kandidaat, niet operationeel aangesloten**.

De module `storm_tracker_worker.hard_decoder_isolation` voert per aanroep
exact één top-level importeerbare decoderfunctie uit in een vers gestart
proces. Er is geen persistent decoderproces en dus ook geen overgedragen
procescache of opgetelde CPU-limiet van een vorige job. De module is bewust
niet geïmporteerd door `service.py`, geen productiehandler verwijst ernaar en
Home Assistant gebruikt hem niet.

## Doel en niet-doel

De grens moet een foute, vastgelopen of te zware toekomstige kwantitatieve
decoder technisch stoppen zonder daaruit `droog`, `geen regen`, `onbekende
neerslag` of enige andere meteorologische conclusie af te leiden. Alle
mislukkingen zijn technische outcomes. Deze laag beoordeelt de inhoudelijke
meteorologische correctheid van een decoder niet.

De kandidaat vervangt evenmin de bestaande provideradmission, raw authority,
canonical-evidencevalidatie, thermische soaktest of de eind-tot-eind
effectaudit. Die bewijzen blijven afzonderlijk verplicht.

## Uitvoeringscontract

Een aanroep bevat:

- een strikte referentie `package.module:function`; alleen één functie direct
  onder een importeerbare module is toegestaan, niet `__main__`, een closure,
  methode of geneste callable;
- een request dat volledig als canonical JSON kan worden gecodeerd en binnen
  `max_request_bytes` blijft; daarnaast gelden 64 nestingniveaus en 262.144
  JSON-nodes zodat parentserialisatie zelf begrensd blijft;
- per-job wall-, CPU-, geheugen- en resultlimieten;
- een `DecoderEffectContract` met een stabiel contract-ID, een expliciete
  `hard_kill_safe=True` claim en als durable-effectstrategie `none`,
  `idempotent` of `transactional`;
- een expliciete `spawns_descendants=False` claim. Een decoder die zelf een
  proces kan starten wordt op deze Linux-grens niet toegelaten.

Een writer mag pas `idempotent` of `transactional` claimen nadat onafhankelijk
is aangetoond dat terminate/kill op elk instructiepunt geen gedeeltelijk
zichtbaar resultaat, corrupte catalogus of dubbel evidence kan opleveren.
Zonder dat bewijs weigert de parent de job voordat een child wordt gestart.

Er is zowel een threadslot als een OS-slot over parentprocessen heen. Linux
gebruikt een non-blocking `flock` op een owner-only reguliere lockfile in
dezelfde runtime-tempnamespace; Windows gebruikt een named local kernel mutex.
Een gelijktijdige tweede aanroep krijgt `busy`; hij wordt niet in een verborgen
tweede computequeue gezet. Daardoor kunnen verschillende executorinstanties of
parentprocessen binnen dezelfde worker-runtime niet samen meerdere zware
decoders starten. Afzonderlijke containers/tempnamespaces delen dit Linux-slot
niet; operationeel mag daarom maar één workercontainer deze queue bezitten.

## Proceslevenscyclus

1. De parent valideert effectcontract, importreferentie en begrensde canonical
   JSON.
2. Alleen op een ondersteund platform wordt een nieuw `spawn`-proces gestart.
3. Het child importeert of draait de decoder nog niet. Het zet eerst de
   platformgrens op en stuurt een klein `ready`-bericht.
4. Op Windows koppelt de parent het wachtende child aan een nieuw Job Object.
   Pas na succesvolle koppeling stuurt hij `start`.
5. Het child decodeert exact één request, codeert exact één begrensd
   JSON-resultaat en sluit af.
6. De parent wacht met korte polls. Op wall deadline volgt terminate, een
   begrensde join, daarna kill en opnieuw een begrensde join.
7. De procesgrens en handles worden altijd gesloten. Een volgende job krijgt
   een volledig nieuw proces.

Een geïnjecteerde multiprocessingcontext wordt alleen geaccepteerd als zijn
startmethode exact `spawn` is; `fork`/`forkserver` kunnen dus niet stilzwijgend
de boot- en isolatieaannames veranderen. De kill-safety-audit omvat ook alle
top-level importeffecten en imports van de targetmodule, omdat die na `start`
maar vóór de functieaanroep binnen dezelfde killbare job plaatsvinden.

De wall clock start vóór `process.start()` en begrenst dus ook de spawnboot.
De CPU/RSS-kernelgrens kan door de standaard multiprocessing-API pas in het
vaste, vertrouwde child-entrypoint (Linux) of na diens `ready` (Windows) worden
geplaatst. De Python-spawnbootstrap zelf valt daardoor niet onder die CPU/RSS-
claim; hij mag de decoder-target nog niet importeren of uitvoeren. Een volledig
begrensde onbetrouwbare interpreterboot zou een eigen native suspended launcher
of containergrens vereisen en wordt hier niet beweerd.

## IPC en geheimen

Het expliciete protocol gebruikt uitsluitend `Connection.send_bytes` en
canonical UTF-8 JSON. `recv_bytes(maxlength=...)` begrenst ook de ontvangende
kant. Providerbytes, responses en decoderresultaten worden door deze laag
nooit met `pickle.loads` geïnterpreteerd.

Python `multiprocessing` met startmethode `spawn` gebruikt intern wel pickle
voor zijn eigen bootstrap. Ook de door de parent gemaakte `request_bytes`
worden dus als opaque bytes-object intern gepickled en kunnen gevalideerde
providerwaarden bevatten. De veiligheidsclaim is smaller: de parent maakt die
bytes zelf uit begrensde canonical JSON, en deze laag accepteert of decodeert
nooit een extern aangeleverde picklepayload.

Een decoderexception levert alleen `decoder_failed`. Exceptiontekst,
traceback, argumenten, requestvelden en providersecrets gaan niet terug over
IPC. Ook boot-, protocol- en boundaryfouten gebruiken vaste detailcodes.

## Harde grens per platform

### Linux

Vereisten: `resource.RLIMIT_AS`, `resource.RLIMIT_CPU` en `setrlimit`.

- `RLIMIT_AS` stelt een harde limiet op de virtuele adresruimte van het
  decoderproces.
- `RLIMIT_CPU` gebruikt een soft limiet op het naar boven afgeronde gehele
  aantal CPU-seconden en een hard limit één seconde later. `SIGXCPU` wordt als
  `cpu_limit_exceeded` behandeld; de hard limit blijft de kernel-fallback.
- De parent leest voor dit exacte PID `/proc/<pid>/stat` en
  `/proc/<pid>/status`. Hij beëindigt de procesgroep zodra de geconfigureerde
  totale user+kernel-CPU of harde virtuele-geheugenlimiet bereikt is. Dit geeft
  ook voor sub-seconde CPU-configuraties een begrensde parentcontrole en bewaart
  de laatste meting voor classificatie na een kernel-exit. Zonder leesbare
  procfs rapporteert deze kandidaat Linux unsupported.
- Core dumps worden uitgeschakeld.
- `prctl(PR_SET_PDEATHSIG, SIGKILL)` zorgt dat een abrupt gestorven parent geen
  decoder zonder wall-deadlinemonitor als orphan laat doorlopen. Ontbreekt
  `prctl`, dan rapporteert Linux unsupported en draait de target niet.
- Het child maakt een eigen process group. Bij wall timeout beëindigt de parent
  eerst de groep en gebruikt daarna SIGKILL als begrensde fallback.

Eerlijke beperking: `RLIMIT_AS` en `RLIMIT_CPU` zijn per proces, niet als
geaggregeerd budget voor willekeurige descendants. De process-group-kill pakt
gewone descendants mee, maar een kwaadwillend child kan zich opnieuw
groeperen. Daarom wordt `spawns_descendants=True` al vóór spawn geweigerd en
moet de audit bewijzen dat de toegelaten decoder geen subprocess/API daarvoor
gebruikt. Voor niet-vertrouwde decodercode is aanvullend een cgroup/container
nodig; deze kandidaat beweert geen job-wide Linux CPU/RSS-grens.

De kernel-`RLIMIT_CPU` heeft gehele-secondegranulariteit; de aanvullende
procfs-parentcontrole en wall deadline pollen iedere circa 20 ms en kunnen dus
sub-seconde configuraties handhaven met die scheduler/poll-overshoot.

### Windows

Na de child-`ready`, maar vóór `start`, maakt de parent een nieuw Windows Job
Object en zet:

- `JOB_OBJECT_LIMIT_PROCESS_MEMORY`;
- `JOB_OBJECT_LIMIT_PROCESS_TIME`;
- `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.

Daarna wordt het child aan dat object toegewezen. Mislukt create, configure of
assign (bijvoorbeeld door een onverenigbare bestaande job policy), dan wordt
de decoder niet vrijgegeven en is de outcome `boundary_setup_failed`.

Naast de kernelinstellingen leest de parent tijdens de job Job Object
accounting. Als de CPU-limiet bereikt is, beëindigt hij het Job Object zelf;
dit is de parent-side fail-safe en geeft dezelfde technische
`cpu_limit_exceeded` outcome. Een door `MemoryError`/`ENOMEM` gerapporteerde
commit failure is `memory_limit_exceeded`; peak memory telt alleen als de
ingestelde harde limiet werkelijk is bereikt. Voor CPU wordt de volledige
limiet gebruikt tijdens de lopende parentcontrole én bij classificatie na een
abnormale exit; er is geen onder-de-limietheuristiek die een gewone crash als
CPU- of geheugenfout mag hernoemen.

Normale childprocessen erven de Job Object-grens. Expliciete breakaway of
malafide code valt buiten dit vertrouwde-decodercontract.

De Windows-claims volgen de primaire Microsoft-documentatie voor
[Job Objects](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects)
en
[JOBOBJECT_BASIC_LIMIT_INFORMATION](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_limit_information):
de process-timegrens wordt periodiek door het systeem gecontroleerd,
process-memorycommit boven de grens faalt, normale childprocessen blijven
standaard in het Job Object en kill-on-close beëindigt de bijbehorende
processen.

### Andere platformen

Er is geen zachte fallback. `hard_boundary_support()` meldt unsupported en de
parent retourneert `unsupported_platform` voordat het target draait.

## Technische outcomes

Mogelijke terminale outcomes zijn:

- `succeeded`;
- `busy`;
- `unsafe_effect_contract`;
- `invalid_request`;
- `unsupported_platform`;
- `boundary_setup_failed`;
- `deadline_exceeded`;
- `cpu_limit_exceeded`;
- `memory_limit_exceeded`;
- `target_load_failed`;
- `decoder_failed`;
- `result_not_json`;
- `result_too_large`;
- `child_crashed`;
- `protocol_error`.

Geen van die waarden is een weerstatus. Alleen `succeeded` bevat een
JSON-resultaat; toelating daarvan als meteorologisch evidence is werk voor de
latere decoder- en canonical-evidencegates.

## Lokaal testbewijs

`tests/test_worker_hard_decoder_isolation.py` controleert onder meer:

- een nieuw PID per succesvolle job;
- kill van een hang op wall deadline en herstel met een volgende job;
- abrupte `os._exit` als afzonderlijke childcrash;
- request-, result- en ontvangende IPC-grenzen;
- afwijzing van niet-JSON-data zonder onbetrouwbare pickle-deserialisatie;
- afwezigheid van exceptiontekst en requestsecret in de outcome;
- weigering van een niet-kill-safe writer vóór zijn marker kan worden gemaakt;
- exact één procesbrede actieve decoderjob;
- uitsluiting van een decoder die door een afzonderlijk parentproces wordt
  gestart via het OS-slot;
- weigering van elk effectcontract dat descendants kan starten;
- fail-closed gedrag op een unsupported platform;
- afzonderlijke CPU- en geheugenlimietoutcomes waar de platformkernel die
  grens ondersteunt;
- AST-bewijs dat service en Home Assistant deze kandidaat niet importeren.

Voor operationele toelating zijn aanvullend Linux-tests in de echte
workercontainer, gemeten RSS/CPU/wall-overshoot, kill tijdens iedere durable
publishfase, NUC-thermische soak en een service-queuefaultmatrix nodig.
