# Cross-store bronreclamatieauthority

> **Historisch v1-document — sealed evidence.** Dit document beschrijft de
> oorspronkelijke withheld/prepare-only grens. Het is geen actuele
> apply-authority en mag niet als bewijs worden gebruikt dat positieve v2-
> reclamation ontbreekt. Het normatieve, nog steeds niet-live v2-contract staat
> in `ACQUISITION_SOURCE_RECLAMATION_APPLY_AUTHORITY.md`; de oorspronkelijke
> v1-receipts blijven niet-positief en worden niet opgewaardeerd.

Status: historisch v1; machineleesbare, content-addressed weigering plus
offline durable preparecoordinator; inert, apply hard-disabled en niet live
gekoppeld

Update 2026-08-09: het oorspronkelijke gapdocument hieronder blijft de
authority voor wat een *positieve* bronvrijgave nog mist. De preparehelft is nu
wel offline gerealiseerd: vijf durable sidecar prepare-CAS-receipts, vijf exacte
sealed resolvers en een OS-fenced/hash-chained coordinator die crashvast tot
`PREPARED` geraakt. `COMMITTED` en `APPLIED` zijn bewust onbereikbaar. Zie
`ACQUISITION_SOURCE_RECLAMATION_COORDINATOR.md` voor het actuele contract,
runbook en de resterende blockers.

## Uitkomst

Echte bronreclamatie is met de huidige store-API's niet bewijsbaar. De worker
mag daarom geen raw object, capture, receipt, schedulerrow, executionhistory,
capacitycommit of lifecyclebewijs vrijgeven.

`acquisition_source_reclamation_authority.py` maakt die beslissing expliciet.
De builder accepteert alleen een volledig opnieuw gevalideerd
`AcquisitionSourceRetentionPlan` van de bijbehorende
`AcquisitionSourceRetentionArchive` en diens gezonde, durable archivehead. De
uitkomst is altijd:

```json
{
  "status": "withheld",
  "reason_code": "cross_store_atomic_prune_contract_unavailable",
  "reclaim_authorized": false,
  "source_release_performed": false
}
```

De volledige authority bevat daarnaast de source-binding-SHA, source-view-SHA,
retention-plan-SHA, sealed-archive-anchor-SHA, kandidaten-/eligibility-/archive-
counts, een exact capabilitycatalogus en een SHA-256 over de canonieke volledige
inhoud. Parser en validator accepteren uitsluitend de exacte canonieke vorm.
Extra keys, whitespace, catalogusdrift, countdrift, hashwijziging of een op
`true` gezet reclaimveld falen gesloten.

De authority bevat geen job-ID's, target-ID's, coördinaten, paden, rawbytes of
credentials. Ze wordt niet geïmporteerd door `service.py` en heeft geen HA-,
MQTT-, provider- of live-wiring.

## Waarom sealed export nog geen reclaimtoken is

De source-retentionlaag bewijst inmiddels:

- exact dezelfde raw capture, receipt en bytes;
- de volledige execution-attempthistoriek en één terminale head;
- de terminale schedulerproof;
- de exacte committed capacityreceipt;
- normale downstreamconsumptie met hot/archive-aware proof;
- een content-addressed sealed kopie met crash-herstelbare anchor/manifestketen.

Dat sluit echter niet atomair uit dat een andere capture hetzelfde object deelt,
dat schedulerreconcile een verwijderde terminale slot opnieuw materialiseert,
dat capacity counters dubbel worden vrijgegeven, of dat een nog niet
geregistreerde downstreamconsumer de bron nodig heeft. Vijf afzonderlijke
SQLite/filesystemauthorities kunnen niet veilig worden gewijzigd door eerst in
één store en daarna in de volgende te verwijderen.

## Exact minimaal API-gat per participant

De capabilitycatalogus is vaste contractdata en wordt meegehashd. Er bestaan nu
backward-compatible durable sidecar-prepare-CAS-API's en exacte sealed
archive-resolvers. Wat nog ontbreekt zijn source-local prepare/applytransacties,
een raw unlinkjournal en een authoritative consumerregistry/epoch-root:

| Participant | Prepare-CAS | Apply na durable beslissing | Archive-resolver | Te bewijzen sluiting |
|---|---|---|---|---|
| raw artifacts | `prepare_reference_counted_capture_prune_cas_v1` | `apply_journaled_raw_prune_decision_v1` | `resolve_archived_capture_receipt_object_v1` | shared receipts/objects pas unlinken bij exact nul referenties |
| scheduler | `prepare_terminal_scheduler_prune_cas_v1` | `apply_terminal_scheduler_prune_decision_v1` | `resolve_archived_scheduler_terminal_v1` | reconcile mag een gearchiveerde terminale slot niet rematerialiseren |
| execution | `prepare_execution_history_prune_cas_v1` | `apply_execution_history_prune_decision_v1` | `resolve_archived_execution_history_v1` | terminal head en volledige attempthistoriek bewegen samen |
| capacity ledger | `prepare_committed_capacity_release_cas_v1` | `apply_committed_capacity_release_decision_v1` | `resolve_archived_capacity_commit_v1` | provider- en global counters dalen exact één keer |
| downstream lifecycle | `prepare_complete_source_reference_closure_cas_v1` | `apply_source_reference_closure_decision_v1` | `resolve_archived_source_reference_closure_v1` | alle geregistreerde consumers zijn terminaal én gearchiveerd |

De nieuwe prepare-antwoorden geven een immutable participantreceipt met exacte
sealed/current-view-root, child/reference-root en archivebinding. Ze zijn
bewust geen applytoken. Een toekomstige `apply` mag uitsluitend een
vooraf durable committed coordinatorbeslissing accepteren, moet idempotent zijn
en na processkill of hostrestart exact verdergaan. Resolver-API's moeten actieve
én gearchiveerde identiteiten transparant en checksum-geverifieerd kunnen lezen.

## Vereiste positieve cross-store coordinator

Losse participant-API's volstaan niet. Er ontbreekt ook één begrensd durable
coordinatorjournal met minimaal de toestanden `PREPARING`, `PREPARED`,
`COMMITTED` en `APPLIED`:

1. hercontroleer sealed archive anchor en record;
2. bind een complete downstream-consumerregistry aan één epoch/root;
3. prepare iedere participant via CAS op zijn exacte view-root;
4. hercontroleer alle preparetokens;
5. schrijf en fsync de gezamenlijke `COMMITTED` beslissing vóór iedere apply;
6. laat iedere participant idempotent toepassen;
7. hervat ontbrekende apply's na iedere restart;
8. rollback vóór `COMMITTED` bewaart alle bronnen;
9. raw filesystem-unlink gebruikt een eigen durable restartjournal.

Zonder al deze eigenschappen kan een crash leiden tot een ontbrekende rawbron
met nog geboekte capacity, een verwijderde schedulerproof met bestaande
executionhead, een verweesde shared objectreferentie of een opnieuw uitgevoerde
acquisitie.

## Testbewijs en bewuste beperking

De gecombineerde lokale testbatch telt nu 71 geslaagde tests:

- 30 tests voor retention/sealed export: restart, iedere publishgrens,
  content-tamper, manifestgap, source-viewdrift, vreemde sourceauthority,
  disk-full, caps, directory-fsync, concurrency/CAS, archive-aware downstream
  lookup en hard geen bronreclamatie;
- 41 testgevallen voor de reclamation-gap-authority en preparecoordinator:
  de oorspronkelijke sealed binding,
  withheld downstream, canonieke roundtrip, dataclass-/JSON-/catalogustamper,
  exacte afwezigheid van alle genoemde store-API's, privacy en 32 gelijktijdige
  identieke read-only builds, plus statische afwezigheid van mutators en live
  wiring.

Er is nog steeds bewust geen duizenden-record *pruning*test uitgevoerd: er is
geen bronpruning geimplementeerd. Wel zijn een 1.000-kandidaten
prepare-aggregaat en 2.000 idempotente applyweigeringen zonder journalgroei
bewezen. De oorspronkelijke positieve-apply-eis blijft:

Voor een toekomstige positieve apply blijven echte duizenden bronrecords,
gedeelde-objectfan-out, opslagvol op iedere applygrens, processkill/hostrestart
en power-lossproeven op het uiteindelijke filesystem verplicht.

Tot dan blijft volle opslag een expliciete capacity failure. De sealed export
is forensisch bewijs en een herstelbron, nooit een vrijgavebevel.
