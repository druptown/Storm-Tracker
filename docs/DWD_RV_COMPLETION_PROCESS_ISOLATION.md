# DWD RV completion: harde procesgrens v1

Status: lokale, niet-live shadowkandidaat. De implementatie staat in
`storm_tracker_worker/dwd_rv_completion_process_isolation.py` en wordt nergens
door `service.py`, de autonome scheduler, Home Assistant, MQTT, snapshot- of
admissioncode geimporteerd.

## Gesloten scope

Deze grens verwerkt uitsluitend een reeds geleased completion-event voor
`consumer=dwd-rv-quantitative-decoder-v1` en
`stream=radar.dwd.radolan_rv`. De bestaande
`AcquisitionEvidenceLifecycleStore` blijft de enige queue. De runner claimt,
renewt, reject of acknowledget geen lifecycle-event en voert geen netwerkfetch
uit. De vaste childtarget voert alleen deze keten uit:

```text
exact DWD handoff raw tar
  -> quantitative +000 observation decode
  -> canonical radar evidence
  -> officieel DWD DE1200-gridcertificaat
  -> analysis-grid/cells shadow
  -> atomisch outputreceipt + hard-processreceipt
```

Forecastleden `+005..+120` worden niet als observatie gemengd. De uitkomst
blijft `withheld`, `fact_revision=None` en verleent geen meteorologische of
operationele admission.

## Publieke compositie-API

```python
DwdRvCompletionIsolatedRunner(
    *,
    handoff_raw_store: RawArtifactStore,
    artifact_store: DwdRvDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    output_store: DwdRvCompletionOutputStore,
    profile: DwdRvShadowChainProfile,
    limits: DwdRvCompletionIsolationLimits = ...,
    context: multiprocessing.context.BaseContext | None = None,
)

runner.execute(
    *,
    lease: CompletionLease,
    binding: _LeaseBinding,
    cancel_event: threading.Event,
) -> DwdRvCompletionProcessResult

runner.raise_for_terminal(result) -> None
```

`DwdRvCompletionConsumer` vereist exact deze runner als `process_runner` en
controleert met `assert_parent_binding(...)` dat parent en child dezelfde raw-,
artifact-, canonical-, outputstore en hetzelfde chainprofiel gebruiken.
`process_leased(...)` blijft coordinator-owned en acknowledget niet.

De completion-authority wordt zo samengesteld:

```python
DwdRvPipelineOutcomeAuthority(
    output_store=output_store,
    profile=profile,
    process_isolation_authority_sha256=runner.authority_sha256,
)
```

De authority verifieert vlak voor de enige lifecycle-ack event/producerspoor,
desired-state en authority, consumer/stream/sequence, output-SHA, afzonderlijke
output- en processreceipt-SHA, isolation-authority, disposition en
`fact_binding=None`.

## Process-authority en request

De versieerbare `stv3-dwd-rv-hard-process-authority-v1` bindt:

- vaste consumer en stream;
- absolute, genormaliseerde paden van precies vier stores;
- alle harde caps van raw-, artifact-, canonical- en outputopslag;
- volledig chainprofiel en profiel-SHA;
- CPU-, RSS-, walltime-, request- en resultaatslimieten.

Het parentrequest bevat alleen die checksummed authority, een payloadvrije
leaseproof en de capturebinding. De leasecapability wordt nooit naar het kind
gestuurd; raw bytes worden content-addressed uit de gebonden raw-store gelezen.
Credentialachtige keys zijn verboden. Child- en parentdiagnostiek bevat alleen
bounded statuscodes, SHA's, exit/timinggegevens en technische
resourcecounters: geen lokaal pad, raw payload, exceptiontekst of secret. Een
succesvolle verse child rapporteert walltime, CPU-seconden, werkelijke peak RSS
en afzonderlijk de piek van de platformgebonden memory-limit-accounting.

## Harde grens

`HardDecoderExecutor` gebruikt altijd een verse `spawn`-context en een globale
seriele processslot. De defaultgrenzen zijn 90 s walltime, 60 s CPU,
1,5 GiB memory-boundary, 1 MiB request en 16 KiB resultaat. Het bestaande
configveld heet om compatibiliteitsredenen `rss_bytes`, maar de kernelgrens is
niet op elk platform letterlijk RSS: Linux limiteert virtual address space via
`RLIMIT_AS`; Windows limiteert de Job Object process-memory charge. Werkelijke
peak RSS wordt daarom apart gemeten (`VmHWM` op Linux,
`PeakWorkingSetSize` op Windows) en mag niet met de enforcementcounter worden
verwisseld.

- Linux: `RLIMIT_AS`, `RLIMIT_CPU`, `/proc`-accounting, parent-death signal en
  process-group kill.
- Windows: Job Object met process memory/time en kill-on-close.
- Ontbreekt een bewezen grens, dan is de uitkomst expliciet `boundary`; er is
  geen in-process fallback.

Het effectcontract is `transactional`, `hard_kill_safe=True` en
`spawns_descendants=False`. Deadline, CPU, RSS, resultaatcap, cancel, busy,
boundary, protocolcorruptie en childcrash worden afzonderlijk fail-closed
geterminaliseerd. Een terminale fout zonder bewezen durable receipt kan nooit
worden geacknowledged.

## Durable commit en crashreconciliatie

Het kind commit in een SQLite-transactie:

1. het immutable outputdocument en zijn output-SHA;
2. het outputreceipt dat output-SHA en commitmoment bindt;
3. een apart processreceipt dat event, output-SHA, outputreceipt-SHA,
   isolation-authority, effectcontract-SHA en limits-SHA bindt.

De parent herlaadt en herverifieert dit exacte receipt voor iedere spawn en na
ieder childresultaat, ook na crash, deadline, kill of verloren IPC. Een receipt
dat al gecommit is wint dus van een onbetrouwbaar exitresultaat en wordt zonder
refetch of tweede decode gereconcilieerd. Zonder receipt blijft het event
geleased en kan het pas na de normale lease-expiry opnieuw worden aangeboden.
Een afwijkende of corrupte durable row wordt nooit als 'niet aanwezig'
behandeld maar levert `durable_disposition=unknown` en een integrity failure.

## Bewijs en resterende blockers

De contracttests gebruiken de echte `.analysis/dwd-rv-live.tar`, testen een
echte verse spawn, elke technische terminalklasse, commit-voor-IPC-crash,
restart/duplicate, twee runtimes op een lifecyclelease, requesttamper,
output/processreceipt-tamper, capaciteit, cancellation en bounded diagnostics.
De generieke `hard_decoder_isolation`-suite bewijst daarnaast de werkelijke
CPU/RSS/wall/output- en kill-treegrenzen van het gebruikte executiemechanisme.

Nog niet opgelost door deze module:

- geen live worker/coordinator- of `service.py`-wiring;
- geen science/admission- of operationele snapshotpromotie;
- geen motion-skillbewijs uit een enkele basistijd;
- upstream raw/capacity/scheduler/executionstores hebben nog geen gezamenlijke
  archive-aware tombstone/reclaimauthority;
- KNMI en METAR hebben geen concrete completionconsumer; OPERA, KMI RADQPE en
  alle overige providers blijven blocked.
