# Worker-first composition-root — gesloten shadowcontract

Status: v1 shadowroot plus afzonderlijke lokaal securitygeharde DWD-only opt-in
servicecanary per 2026-08-13; niet gedeployed en niet productie-enable

## Afbakening

`storm_tracker_worker/worker_first_composition.py` is de eerste production-like
v1-composition-root rond de bestaande worker-first authority. De module bouwt
exact één `WorkerFirstRuntimeCoordinator` en importeert geen `service.py`,
MQTT-main, Home Assistant, providertransport of integratieconfiguratie. Deze
directe v1-root blijft shadow-only. Een afzonderlijk v2-servicecontract in
`worker_first_service_composition.py` kan nu uitsluitend na de expliciete
`worker_first_canary`-selectie lazy door de add-onservice worden geconstrueerd.

De root bewijst op dit moment uitsluitend dat de reeds bestaande duurzame
onderdelen onder één eigenaar en één seriële completionbaan kunnen worden
samengesteld. Het is geen go-livebesluit, geen providerplanner en geen
meteorologisch admissioncertificaat.

## Gesloten configuratiedocument

Het contract is `stv3-worker-first-composition-config-v1`. Extra of ontbrekende
velden worden afgewezen. De vaste top-levelautoriteit bevat:

- het vaste DWD consumer- en stream-ID;
- `mode=shadow_unwired` en `live_wiring=false`;
- `meteorological_admission=exact_fact_binding_only`;
- een begrensd owner-ID;
- exact elf absolute, genormaliseerde, niet-aliassende paden;
- de volledige DWD chain profile plus diens SHA-256;
- de volledige hard-processlimieten plus hun SHA-256;
- lease-, processing-, cancel-, control- en stopdeadlines.

Relatieve paden, `..`-normalisatie, symlinks/reparse points, dubbele paden en
stores die binnen een andere geconfigureerde storemap vallen worden
fail-closed geweigerd. De authorityfile en permanente lockanchor moeten
dezelfde oudermap hebben. De processdeadline moet minstens de hard-process
wallcap plus één seconde kunnen omvatten; de lease moet daarnaast processing
plus cancelgrace omvatten.

`WorkerFirstCompositionConfig.fingerprint` is de SHA-256 van de canonieke
configuratiebytes. Zowel bootstrap als runtimeconstructie eisen deze SHA als
een afzonderlijk gepinde input. Er wordt dus nooit impliciet uit environment,
cwd of een serviceglobal gelezen.

Voor het geneste DWD-profile hergebruikt de inert composition-root bewust de
exacte parser die ook de vaste hard-processchild gebruikt. Dat is momenteel een
private modulefunctie en dus een gekende interne API-koppeling; een contracttest
bewijst dat een extra of afwijkend profileveld gesloten wordt geweigerd. Er is
geen tweede, los evoluerende profileparser in de composition-root.

## Exact zes mutable authoritybindings

De bestaande ownershipauthority blijft gesloten op exact deze rollen:

1. `acquisition_completion_store`;
2. `desired_state_store`;
3. `operational_fact_store`;
4. `operational_snapshot_store`;
5. `pipeline_output_store`;
6. `snapshot_outbox_store`.

Iedere identity-SHA is content-addressed over:

- `stv3-worker-first-store-identity-v1`;
- de rol;
- de volledige composition-config-SHA;
- de relevante storecontract-, schema-, algoritme-, retentie- en
  capaciteitsgrenzen.

De pipeline-outputidentity bindt bovendien het DWD profile, de exacte
`DwdRvPipelineOutcomeAuthority` en de hard-processauthority. Die laatste
authority bindt de handoff-rawstore, derived-artifactstore,
canonical-evidencestore en outputstore met hun paden en limieten. Deze vier
DWD-supportstores worden dus niet als zevende ownershiprol toegevoegd, maar
zijn wel transitief en checksummed in de bestaande pipeline-outputrol.

## Eenmalige authoritybootstrap

`initialize_worker_first_composition_authority()` is een afzonderlijke,
expliciete handeling:

1. valideer de gepinde config-SHA;
2. open/initialiseer alle echte stores met hun bestaande schema's;
3. voer de beschikbare volledige storeaudits uit;
4. construeer de DWD hard-processrunner, completionconsumer en
   outcome-authority;
5. bereken de zes storeidentity-SHA's;
6. maak/verifieer de permanente lockanchor zonder truncate of replacement;
7. bouw canonieke ownershipauthoritybytes;
8. maak de authorityfile uitsluitend met create-once-semantiek, of eis dat een
   reeds bestaand bestand byte-identiek is;
9. laad alles opnieuw en verifieer de exacte coordinatorbinding.

Een afwijkende bestaande authorityfile wordt nooit overschreven. Een crash
tijdens een eerste create kan daarom een onvolledig bestand achterlaten; dat
blijft bewust een fail-closed operatorfout en wordt niet automatisch verwijderd
of vervangen.

Een bootstrap met dezelfde paden maar een gewijzigde config/profile-SHA wordt
eveneens geweigerd terwijl zowel de bestaande authoritybytes als de permanente
lockanchor byte-identiek behouden blijven.

De bootstrapreceipt bevat alleen SHA-256's, contractcodes en aantallen. Zij
bevat geen paden, target-ID's, coördinaten, payloads of credentials.

## Runtimeconstructie en rollback

`build_worker_first_composition()` volgt een andere, niet-mutabele
authorityvolgorde:

1. valideer eerst de gepinde config-SHA en authority-SHA;
2. laad de authority en controleer de zes paden voordat stores worden geopend;
3. open en audit de echte stores;
4. herbereken de volledige store- en hard-processidentiteiten;
5. vergelijk die byte-semantisch met de authority;
6. bouw de lokale snapshotruntime;
7. acquire de niet-wachtende OS-fence;
8. injecteer lease, stores, DWD consumer en outcome-authority in exact één
   `WorkerFirstRuntimeCoordinator`.

Een fout vóór acquire laat geen ownership achter. Een fout tijdens de laatste
coordinatorconstructie releaset de lease en sluit de fence; een nieuwe exacte
constructie kan onmiddellijk slagen. Na succesvolle overdracht wordt de fence
alleen nog door `WorkerFirstRuntimeCoordinator.async_stop()` vrijgegeven, met
diens bestaande quiescence- en timeoutcontract. Een niet-quiescente processor
of snapshottask houdt de fence dus bewust vast.

### Aangescherpte v2-canaryvolgorde

De servicebereikbare v2-root volgt sinds 2026-08-13 een strengere volgorde dan
de hierboven beschreven directe v1-shadowbuilder. Na uitsluitend read-only
validatie van configpin, authoritypin en de zes gebonden paden acquiret
`build_worker_first_service_composition()` eerst de niet-wachtende OS-fence.
Pas onder die ownershipfence worden mutable stores geconstrueerd/geopend,
volledig geaudit en tegen de authority gebonden. Daardoor veroorzaakt een busy
fence nul storeconstructies en nul storemutaties. Iedere fout na acquire sluit
reeds geconstrueerde onderdelen en geeft lease en fence vrij; een exacte nieuwe
constructie kan daarna opnieuw proberen.

### Readiness over MQTT-generaties

Voor de worker-first servicecanary zijn transportverbinding en runtimebewijs
één admissionvoorwaarde. Disconnect sluit atomair de runtime-readiness,
jobadmission en subscriptionauthority. Een geslaagde reconnect verhoogt de
MQTT-transportgeneratie maar opent de jobsubscription niet direct. De
runtime-sidecar moet eerst verse exacte diagnostiek leveren die binnen dezelfde
nieuwe generatie begon en eindigde. Een ouder proof of een proof dat een
disconnect/reconnectgrens kruiste, kan readiness of admission niet heropenen.
Deze generatiebinding geldt alleen voor de expliciete canary; de legacyservice
blijft ongewijzigd.

### Gesloten DWD-childenvironment

De canaryfactory geeft `HardDecoderExecutor` een immutable technische
environmentallowlist zonder `STV3_*` MQTT- of providercredentials. Omdat
`multiprocessing.Process` geen eigen `env`-argument heeft, vernauwt de parent de
environment synchroon aan de OS-spawngrens en herstelt daarna zijn volledige
environment exact. De eerste child-instructie herhaalt de scrub vóór target-
import of requestdecode. Dit is defence in depth: secrets bereiken noch de
spawnbootstrap noch de decoder.

`run.sh` verwijdert in `worker_first_canary` vóór `exec` tevens alle niet-DWD-
providercredentials en het mutable legacy receiptpad. In `legacy` blijft dit
pad ongewijzigd; ook een `HardDecoderExecutor` zonder expliciete allowlist houdt
zijn bestaande backwards-compatible environmentcontract.

## Geen livepublicatie en geen verzonnen weatherfact

De snapshotpublisher is vast ingebouwd als
`stv3-worker-first-hash-shadow-sink-v1`. Hij valideert de canonieke envelope,
berekent één SHA-256 en bewaart alleen die hash plus een begrensde teller. Er is
geen injecteerbare MQTT-, HA- of netwerkpublisher in deze composition-root.

De DWD completionketen blijft expliciet factless:

- de consumer retourneert `fact_revision_staged=false`;
- `DwdRvPipelineOutcomeAuthority` eist `fact_binding=None`;
- de composition-root fabriceert geen fact, regenstatus, track, snelheid,
  ground confirmation, ETA of forecast;
- een operationele fact kan uitsluitend via de bestaande exact gevalideerde
  `OperationalFactRevisionStore` en de bestaande `FactPublicationBinding`
  worden geselecteerd. Zonder zo'n durable exact binding blijft de snapshot
  unknown/factless.

Een lokaal als `published` gemarkeerde shadow-outboxrow betekent daarom alleen
dat de hash-only testsink de exacte bytes heeft aanvaard. Het is uitdrukkelijk
geen MQTT PUBACK, geen bewijs dat Home Assistant iets heeft ontvangen en mag
nooit als live-admission- of go-livebewijs worden gebruikt.

## Privacybegrensde diagnostiek

Compositiondiagnostiek is begrensd op 16 KiB en bevat uitsluitend:

- vaste contract-/statecodes en booleans;
- aantallen;
- config-, authority-, storebinding- en processauthority-SHA's;
- de laatste lokale envelope-SHA;
- de reeds privacybegrensde runtime-diagnostiek.

Paden, owner-/consumer-/stream-ID's, targetinformatie, ruwe payloads en
exceptiontekst worden niet opgenomen. Fouten verlaten de boundary als vaste
codes.

## Gedekte bewijzen

De geharde servicecanary supersedeert de eerdere activeringsgate van 204/204 met
een strict gecombineerde matrix van 242/242 passed. Aanvullende afzonderlijke
runs geven acquisitionruntime 93/93, de eerdere volledige hard-decoderfile
20/20, de toen afzonderlijke gerichte OS-spawn-boundary 1/1 en packaging plus
`.dockerignore` 28 passed met één verklaarde Windows-skip. Daarna is de
volledige hard-decoderfile inclusief die regressie definitief als één matrix
21/21 groen in `14.98s`. Dit zijn lokale post-RC1-tests, geen deploy-, live- of
productie-evidence.

`tests/test_worker_first_composition.py` gebruikt de echte storeklassen en
dekt met warnings-as-errors:

- exact configschema, canonieke roundtrip en configtamper;
- padalias/nested-aliasafkeur;
- idempotente create-once authoritybootstrap;
- canonieke authoritybytes en exact zes unieke contentidentities;
- profilewijziging → andere config-, process- en authorityidentity;
- echte unconfigured start/stop en heropening na proceslogische restart;
- actieve desired state met lege completion geeft exact één lokaal gevalideerde
  unknown-snapshot, één hash-only shadowreceipt en nul factrevisies;
- een lege factstore blijft factless;
- dubbele owner wordt afgewezen totdat de eerste exact gestopt is;
- authoritytamper en een corrupte, maar geldige SQLite-store falen gesloten;
- partial-constructionrollback releaset de fence;
- hash-only, pathvrije en begrensde diagnostiek;
- statische afwezigheid van service-, MQTT-, HA- en integratie-imports.

## Nog expliciet gesloten

Deze stap verandert niets aan de volgende roadmapblokkades:

- de default/ontbrekende servicemodus blijft exact `legacy` en construeert geen
  worker-first-root;
- alleen de expliciet gepinde `worker_first_canary` construeert de afzonderlijke
  v2-root voor exact `radar.dwd.radolan_rv`;
- desired-statecreatie/transport vanuit HA is niet aan deze root gewired;
- de autonome acquisition scheduler produceert nog niet via deze root;
- science, CellTrack/WeatherSystem en groundfusion zijn niet in deze
  completionroute opgenomen;
- DWD cells-output heeft geen meteorologische operationele toelating en geen
  factbinding;
- KMI-, KNMI-, OPERA- en groundproducten krijgen hierdoor geen toelating;
- een storeauthority bindt over procesrestarts pad plus semantisch
  schema/profile, niet een blijvend OS-inode. Corruptie en schemadrift worden
  geaudit, maar een bewust vervangen, nieuw geldige lege store is met de
  huidige storeschema's niet van een eerste lege store te onderscheiden;
- kill-, disk-full-, clockskew-, MQTT/PUBACK- en volledige end-to-end
  faultmatrix blijven releasewerk;
- de verplichte zeven-daagse NUC/HA/Z2M-soak en onafhankelijke
  meteorologische truth-evaluatie blijven onverminderd vereist.

De afzonderlijke versiegepinde servicecomposition bestaat nu lokaal en is
fail-closed getest, maar verleent geen productie- of meteorologische admission.
Deze v1-module zelf blijft shadow-only; de v2-canary blijft
`production_enable_ready=false`, science-shadow, factless en zonder reclaim
totdat de resterende operationele bewijzen zijn gesloten.
