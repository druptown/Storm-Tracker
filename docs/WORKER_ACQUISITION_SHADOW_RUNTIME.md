# Worker-owned acquisition shadowruntime

Status: lokale kandidaat, niet live geactiveerd en niet operationeel toegelaten.

## Afbakening

Deze kandidaat verbindt desired-state, de deterministische acquisitieplanner,
de schedulerbridge, de duurzame seriële `AutonomousScheduler`, de globale
raw-capaciteitsledger, de exacte raw-store, verse procesisolatie en duurzame
executionreceipts. `service.py` importeert of start deze runtime niet. De
runtime publiceert geen MQTT, operational snapshot of meteorologische toestand.

De gesloten activatiematrix verantwoordt alle 25 catalogusworkloads exact
eenmaal. Drie workloads zijn uitsluitend als shadow exact-raw uitvoerbaar:

1. `ground.metar.global`;
2. `radar.dwd.radolan_rv`;
3. `radar.knmi.rtcor`.

Alle andere regels blijven expliciet blocked. Een onbekende workload,
provider-/productdrift, veranderde scope, vervalste target-/regiobinding,
budgetdrift, onbekend secret-requirement of niet-geïmplementeerd
handlerpayloadcontract faalt gesloten.

## Exact uitvoerbare subset

### AviationWeather METAR

`ground.metar.global` gebruikt `capture_latest_metar_cache`. De handler bewaart
de exacte `metars.cache.csv.gz` vóór decompressie en verricht uitsluitend een
begrensde gzip/CSV-structuurinspectie. De payload is exact `{}`. Eén job
reserveert één `csv_gzip`-rol, maximaal 16 MiB en 45 seconden. De uitkomst blijft
`captured_not_decoded`, `weather_state=unknown` en
`operational_admission_eligible=false`.

### DWD RV

`radar.dwd.radolan_rv` gebruikt de nieuwe, afzonderlijke
`dwd_rv_native.capture_latest_dwd_rv_tar`. Deze module importeert noch
`radar.dwd_radolan_fetch`, noch HDF5/numpy/pyproj, noch een kwantitatieve
decoder. Het gecombineerde legacyproductiepad wordt dus nooit aangeroepen.

De wrapper:

- accepteert exact een lege handlerpayload;
- haalt de vaste DWD `composite_rv_LATEST.tar` op onder een harde 8 MiB-grens;
- schrijft de exacte tarbytes duurzaam vóór de eerste tar-layoutinspectie;
- inspecteert alleen maximaal 64 tarheaders om exact één veilig, canoniek
  `..._000-hd5`-lid en de bestandsnaamtimestamp te binden;
- opent of decodeert het HDF5-lid niet;
- commit exact één rol `rv_tar`;
- hervat op dezelfde acquisitiejob-ID vanaf raw receipt of complete capture;
- geeft alleen technische provenance/hashes terug en zet iedere
  kwantitatieve/operationele admission op `false`.

Het planner- en ledgerbudget is exact één artifact, 8 MiB en 45 seconden. Het
oude budget van 128 MiB/twee artifacts/90 seconden is verwijderd omdat dit niet
het werkelijke raw-transportcontract beschreef.

De eerder ontbrekende identiteitshandoff naar de bestaande kwantitatieve
decoder is nu lokaal gesloten in `dwd_rv_capture_handoff.py`. De adapter leest
uitsluitend een reeds duurzame raw-v2 capture, refetcht nooit, behoudt de exacte
tar-SHA en maakt in een afzonderlijke bounded handoffstore een versieerbare
`dwd_radolan`-capture. Alleen `+000` is observatie; `+005..+120` blijven elk
afzonderlijk gehashte, niet-gedecodeerde forecastlineage. De echte
`.analysis/dwd-rv-live.tar` bereikt via dit pad canonical evidence en de
bestaande cells-shadowketen. Dit pad is nog niet aan de acquisitieruntime,
science-orchestrator of live service gekoppeld; één basistijd bewijst bovendien
geen motion skill.

### KNMI RTCOR

`radar.knmi.rtcor` gebruikt de bestaande
`knmi_native.capture_latest_knmi_hdf5`. De native handler bewaart één exacte
HDF5-response vóór structurele HDF-inspectie, decodeert geen radarwaarden en
geeft `captured_not_decoded`, `weather_state=unknown` en
`operational_admission_eligible=false` terug.

De schedulerjob en childrequest bevatten uitsluitend het abstracte requirement
`worker_secret.knmi_api_key`; de secretwaarde wordt niet opgenomen. Een
injecteerbare `EnvironmentSecretResolver` controleert worker-local de vaste
omgevingbinding en retourneert alleen een constante presence-marker. De native
handler leest `STV3_KNMI_API_KEY` pas zelf in het verse childproces. De waarde
wordt niet gehasht, gelogd, in SQLite opgeslagen of naar diagnostiek
teruggestuurd. Ontbrekende/ongeldige aanwezigheid resulteert in een zichtbare,
retryable `secret_requirement_unavailable`.

Het planner- en ledgerbudget is gelijkgetrokken met het native contract: één
`hdf5`-rol, maximaal 40 MiB en 75 seconden.

## De drie prioritaire radarworkloads: exact verdict

| Workload | Shadow executable | Payloadbinding | Hard raw budget | Reden |
|---|---:|---|---:|---|
| `radar.dwd.radolan_rv` | ja | exact `{}` | 1 × 8 MiB, 45 s | aparte tar-only wrapper; geen legacydecode/import |
| `radar.knmi.rtcor` | ja, alleen met worker-local secretpresence | exact `{}` | 1 × 40 MiB, 75 s | bestaande exacte HDF5-capture; geen decode |
| `radar.opera.dbzh` | nee | canonieke vijfminutenslot-timestamp is beschreven maar niet dispatchbaar | 1 × 384 MiB, 300 s | schema-3 store vereist één volledig object in RAM en de native handler registreert zowel memory admission als gebruiksrechten als niet gecertificeerd |

De OPERA-catalogusbudgetten zijn wel gecorrigeerd naar de werkelijk hard
toegelaten 384 MiB/300 seconden. Er is bewust geen `process_target` aan de
OPERA-regel gekoppeld en er kan dus geen OPERA-child starten. Activatie vereist
eerst een bounded streaming/chunked raw-storepad plus expliciete
rechtenclearance; pas daarna kan hetzelfde canonieke slotcontract opnieuw
worden beoordeeld.

KMI valt buiten deze uitbreiding. `radar.kmi.appv4_visual` is gedecommissioned,
`radar.kmi.public_wcs_unknown_quantity` blijft semantisch en voor retentie
geblokkeerd, en `radar.kmi.radqpe_native` heeft status
`external_contract_blocked` zonder uitvoeringsbudget, intent of dispatch. Er
is geen impliciete KMI-fallback, geen promotie van de legacyadapter en geen
meteorologische claim.

## Capaciteit, procesgrens en bewijs

De runtime eist vóór startup provider-capaciteit voor iedere actieve handler:
`aviationweather`, `dwd` en `knmi`. De raw-storeobjectgrens moet exact 40 MiB
zijn, gelijk aan de grootste actieve transportlimiet. OPERA's 384 MiB wordt
niet gereserveerd of toegelaten zolang OPERA blocked is.

Per schedulerlease geldt:

1. volledige jobchecksum valideert provider, product, slot, budget, regio's en
   targets;
2. de activatiematrix bindt scope en exact handlerpayloadcontract;
3. alle maximale raw bytes en alle verplichte raw roles worden onder een
   stabiele reservation-ID gereserveerd;
4. één verse spawn-child draait met harde wall-, CPU-, RSS-, request- en
   resultgrenzen en een expliciet idempotent hard-kill-safe effectcontract;
5. de child laadt de gesloten handlerdispatch, controleert durable raw receipt
   én capture en geeft alleen hashes/tellers terug;
6. de ledger commit de werkelijke bytes/rolcount en evidencehashes;
7. een duurzaam terminal executionreceipt wordt geschreven;
8. pas daarna gaat de schedulerjob naar terminal `completed`.

### Gesloten capture-naar-ledger recovery

Tussen een succesvolle `RawArtifactStore`-capture en de parent-side
`RawCapacityLedger.commit` bestond nog een expliciet crashvenster: bytes en
capturemanifest konden al duurzaam zijn terwijl alleen het verdwenen
childresultaat de ledgercommit nog kon voeden. De runtime gebruikt daarom nu
`CaptureCapacityReconciler` als afzonderlijke, netwerk- en decodervrije
recovery-authoriteit.

Bij iedere lokale startup maakt die een bounded inventory van raw objects,
receipts en captures. Alleen de drie onveranderde actieve bindings worden
herkend: AviationWeather-ledgerprovider naar raw provider `metar`/`csv_gzip`,
DWD naar `dwd`/`rv_tar` en KNMI naar `knmi`/`hdf5`. Per receipt en capture worden
provider, raw product, fetch-ID, acquisitiejob-ID, rol, content type,
transportgrens, native-pre-decode provenance, upstreamidentiteit,
timestampbinding, object-SHA, receipthash, capturemanifest en
capture-receipthash opnieuw gecontroleerd.

Een capture mag uitsluitend een reeds vóór de netwerkactie gemaakte
reservering met exact provider/job/budget sluiten; recovery maakt nooit
achteraf capaciteit aan. De ledgercommit is een SQLite-CAS: twee processen die
dezelfde capture tegelijk herstellen krijgen dezelfde immutable commit terug
en tellen bytes/entries slechts eenmaal. Faults vóór commit laten `RESERVED`
duurzaam staan; een fault direct na de SQLite-commit wordt bij restart als
dezelfde `COMMITTED` receipt herkend. De route schrijft, verwijdert of
overschrijft geen raw object en heeft geen secretresolver nodig. Ook een
KNMI-capture wordt dus na een crash afgewerkt als de API-key intussen ontbreekt,
zonder refetch.

Een childfailure of hard kill leidt vóór retry eerst tot deze exacte raw-probe.
Is de capture compleet, dan commit de parent rechtstreeks uit de duurzame store
en start geen tweede providerrequest. Geeft het child wel een resultaat terug,
dan moeten byte-count, rolcount en alle raw hashes exact met dezelfde
onafhankelijk herladen store-evidence overeenkomen; anders volgt geen
successclaim. Een bestaand terminal successreceipt wordt bij
schedulercompletion-replay opnieuw tegen raw store én capacity commit
geverifieerd.

Onbekende producten, bytes zonder jobgebonden reservering, orphan
catalogusobjecten, stagingresten, ontbrekende captureleden, budgetdrift,
provenancedrift, ledgerconflict en gewijzigde raw bytes falen gesloten. Dat
geldt ook als jobvreemde bytes toevallig onder een andere ruime reservering
passen: capaciteit mag niet tussen jobidentiteiten worden geleend. Diagnostiek
bevat alleen vaste aggregaattellers; geen job-ID, pad, handlerpad, coördinaat of
credential.

Een crash na reservering maar vóór een complete capture hervat via dezelfde
native job-ID. Een crash na complete capture wordt zonder providerrefetch
gesloten. Een crash na het terminale executionreceipt maar vóór
schedulercompletion speelt alleen de completion opnieuw af. Blocked jobs
krijgen eveneens een duurzaam terminal receipt; secret-missing blijft zichtbaar
retryable met bounded backoff. Geen job wordt stil gedropt.

Vóór iedere `reconcile` maakt de runtime bovendien een bounded, payloadvrije
preview van bestaande schedulerrows die hun deadline bereikt hebben. Voor elk
ervan wordt eerst een terminal `deadline_expired` executionreceipt geschreven
en pas daarna mag de scheduler de row `expired` maken. Een huidig slot dat zijn
deadline al miste vóór de scheduler het ooit kon invoegen krijgt eveneens een
receipt met `attempt_count=0` vóór `missed_deadline_count` wordt verhoogd. Een
crash tussen receipt en schedulertransitie speelt alleen de transitie opnieuw
af. Als een lopend childproces de deadline overschrijdt, schrijft de runtime
ook eerst het terminale receipt en roept pas daarna de schedulerretry aan die
naar `expired` gaat.

## Privacy en diagnostiek

De activatiematrixsnapshot bevat geen `process_target`, Python-importpad,
omgevingvariabelenaam of credential. Runtime-diagnostiek bevat alleen bounded
aggregaten, checksums, capaciteitstellers en de laatste technische status; geen
targets, regio's, coördinaten, handlerpayloads of secretwaarden. Het
childrequest bevat evenmin schedulerlease-materiaal of credentials.

## Duurzame completion-outbox en bewijsretentie

De afzonderlijke, nog ongewirede
`acquisition_evidence_lifecycle.py` sluit nu de overdracht van een complete
shadow-acquisitie naar een latere consumer. Een immutable producerreceipt wordt
voor `ready` gecommit; een consumptionreceipt wordt voor terminale outboxstate
en cursor-CAS gecommit. Terminale cursor-past completions worden samen met de
exacte raw bytes in een hashgeketend sealed archief gezet en daarna uit de hot
mirror verwijderd via een hervatbaar child-first pruneplan.

De DWD-binding is concreet getest met `.analysis/dwd-rv-live.tar` en bindt
desired authority, scheduler terminal proof, alle executionreceipts, committed
capacity, source raw-v2 capture en de byte-identieke decoderhandoff. De module
refetcht en decodeert niet, blijft buiten `service.py` en maakt geen
meteorologische claim. Zie `docs/ACQUISITION_EVIDENCE_LIFECYCLE.md`.

## Nog open vóór live activatie

- geen import of wiring in `service.py`;
- geen MQTT- of snapshotpublicatie;
- geen meteorologische decoder/admission vanuit deze runtime;
- geen live NUC-soak voor CPU, RSS, temperatuur, disk-full en HA/Z2M-latency;
- de nieuwe lifecycle-mirror heeft bounded hotretentie en sealed archivering,
  maar scheduler-, execution-, ledger- en originele raw-store zijn nog niet
  archive-aware en worden daarom nog niet veilig gereclaimed;
- de DWD raw-v2 naar `dwd_radolan` handoff is nu afzonderlijk met de echte live
  tar bewezen; de completionbinding is nog niet aan service of sciencewiring
  gekoppeld en is dus geen live-admission;
- OPERA blijft blocked door zijn complete-object-memorypad en unresolved
  rights status;
- KMI RADQPE en alle andere providers blijven blocked tot hun volledige
  provider/product/payload/secret/resourcecontract afzonderlijk bewezen is.

Raw capture is hier bewijsvergaring, geen weerswaarheid.

## Lokaal testbewijs

De gerichte suite test matrixcompleetheid, exacte provider/product/budget- en
target/regiobinding, DWD tar-only isolatie, KNMI secretgating, payloadprivacy,
alle raw roles, capacity reservations, verse procesgrenzen, volledige blocked
accounting, retry, restart/crashreplay, duurzame terminale receipts,
diagnostiekgrootte en afwezigheid van `service.py`-wiring. De recovery-red-team
injecteert bovendien faults na capturevalidatie, vlak vóór commit en direct na
commit; forceert gelijktijdige reconcilers; corrumpeert raw bytes; test volledig
gereserveerde capaciteit, bestaande jobvreemde bytes, parent- en childcrashes,
exact-once restart, childbewijsdrift en bounded identity-/secretvrije
diagnostiek.
