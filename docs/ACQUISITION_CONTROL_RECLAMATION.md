# Acquisition control-store reclamation v2

Status: lokaal bewezen, inert en niet live gekoppeld. Dit document beschrijft
`storm_tracker_worker/acquisition_control_reclamation.py`; import of constructie
start geen scheduler, migreert geen live service en activeert geen HA-, MQTT-
of providerpad.

## Doel en scope

De module levert de drie native controlparticipants voor het frozen source-
reclamationprotocol:

| Rol | Participant | Sourcefeiten die exact samen bewegen |
|---|---|---|
| `execution_receipts` | `ExecutionReceiptReclamationParticipantV2` | volledige attemptgeschiedenis en terminal head |
| `scheduler` | `SchedulerReclamationParticipantV2` | terminale scheduler-outboxslot en reconcile-identiteit |
| `capacity_ledger` | `CapacityLedgerReclamationParticipantV2` | committed reservation plus provider- en globale counters |

Lifecycle en raw blijven afzonderlijke filesystemparticipants. De globale
applyvolgorde blijft:

```text
downstream_lifecycle
-> execution_receipts
-> scheduler
-> raw_artifacts
-> capacity_ledger
```

De controlmodule kan nooit zelfstandig een bronrecord vrijgeven. Iedere
participant is bij constructie fysiek gebonden aan precies één reeds
geïnitialiseerde bronstore, één `SourceReclamationCandidateAuthority` en één
`HeldCommittedDecisionAuthority`. Caller-provided hashes zijn uitsluitend
vergelijkingswaarden, nooit mutatiebevoegdheid.

## Same-database authority

De additieve reclamationextension leeft in dezelfde SQLite-database als de
sourcefeiten. Daardoor committen deze waarden in één `BEGIN IMMEDIATE`:

- de immutable request en candidate-members;
- PREPARED-fence en exact source-viewbewijs;
- sourceverwijdering en semantische tombstones;
- effect, post-apply-view en lokaal APPLIED-receipt;
- of, vóór COMMITTED, de duurzame ABORTED-fencerelease.

De persistente storeidentity bindt rol, bronstore-instance, volledige base-
schemafingerprint, candidate-authority, coordinator-authority, padwitness,
limieten en reclamationschema. De module inventariseert alle tabellen, indices
en triggers en vergelijkt genormaliseerde SQL met de canonieke productie-DDL.
Een extra, ontbrekend, gedeeltelijk gemigreerd of herschreven schemaobject,
een vervangen database op hetzelfde pad of een mislukte SQLite-integritycheck
faalt gesloten.

Reclamationtabellen zijn create-once/no-delete. Member- en tombstonerows zijn
no-update/no-delete; decisionrows mogen alleen de gesloten stateprogressie en
de daarbij behorende eenmalige velden invullen. Row-, inventorybyte- en
databasecaps begrenzen alle paden. Er wordt geen `VACUUM` uitgevoerd.

## Prepare, apply en abort

`prepare_source_local_v2(request)` houdt de actuele candidate-authority over
de volledige metadata-only pagina-inventory. Hij bouwt per kandidaat opnieuw
het role-local sourcefeit, vergelijkt de gedeelde viewbuilder met de sealed
rolhash en bevriest exact de members, source-view, child/reference-root,
releasecounts en source-fence. Een actieve fence laat alleen de byte-identieke
retry toe. Prepare verwijdert niets.

`apply_source_local_v1(request_sha256, predecessor_apply_receipt_sha256=...)`
werkt uitsluitend binnen de fysieke COMMITTED-decisionhold. De hold bewijst de
exacte request, lokale prepare, coordinatorreceipt en predecessorprefix vóór en
na de lokale transactie. De effecten zijn:

- execution: verwijder alle gebonden receipts en precies de terminal head;
- scheduler: verwijder precies de terminale outboxslot;
- capacity: verwijder de committed reservation en verlaag reservation-,
  committed-, byte- en entrycounters providerlokaal en globaal exact eenmaal.

De capacityparticipant reconcilieert counters eerst opnieuw uit de durable
reservationfacts. Voor de lokale delete wordt de historische no-release-
trigger alleen binnen dezelfde writertransactie vervangen; de canonieke trigger
moet vóór commit exact hersteld zijn. Een afwijkend rowcount, negatief of
inconsistent tegenbewijs rolt de transactie terug.

Iedere apply schrijft permanente tombstones voor de oude primaire en secundaire
semantische identiteiten. Source-insertguards blokkeren rematerialisatie al
vanaf PREPARED en blijven na APPLIED gelden. Een triggerbypass wordt bij
`load_source_local_apply_v1(...)` of replay ontdekt doordat de volledige
post-apply-closure opnieuw wordt bewezen.

`abort_source_local_v1(...)` vereist de fysieke ABORTING-decision en de exact
opgeslagen prepare/fence. Abort bewaart alle bronfeiten, schrijft een immutable
abortreceipt en geeft alleen de betrokken fence vrij. Een oude abort kan een
latere successor-fence niet vrijgeven. APPLIED kan niet worden geabort en
ABORTED kan niet worden toegepast.

## Restart en replay

De publieke readgrens bestaat uit:

- `load_source_local_prepare_v2(request_sha256)`;
- `load_source_local_apply_v1(request_sha256)`;
- `load_source_local_abort_v1(request_sha256)`;
- `source_store_identity_sha256` voor de fysiek herbewezen storebinding.

Exacte prepare/apply/abort-retries retourneren hetzelfde duurzame receipt.
Crashes vóór een lokale commit laten geen effect achter; crashes na commit
worden door de immutable receipt en postcondition als voltooid herkend. APPLIED
replay vereist dat sourcefeiten afwezig blijven, tombstones exact aanwezig zijn,
schema/guards ongewijzigd zijn en capacitycounters nog met de overblijvende
reservationfacts overeenkomen. Er volgt geen tweede delete of counterdecrement.

## Lokaal bewijs en activatiegrens

`tests/test_worker_acquisition_control_reclamation.py` dekt de drie echte
stores, prepare-CAS, geordende apply en replay, abortisolatie, crashgrenzen,
schema-/trigger-/base-DDL-tamper, tombstonebypass, forged authoritybindings,
counterreconciliatie, klokterugval, padvervanging en harde caps. De native
vijfrollenintegratie bewijst vervolgens de controlparticipants samen met
lifecycle, raw, retention-v2 en de echte coordinator; zie
`ACQUISITION_SOURCE_RECLAMATION_APPLY_AUTHORITY.md`.

Dit bewijs zit in de finale offline matrix van
`ROADMAP_EXECUTION_STATUS_2026-08-11.md`, maar activeert niets.
`SERVICE_WIRING_ENABLED=false`, `production_enable_ready=false` en
`deployment=false` blijven de norm. Live reclamation vereist nog de
filesystem-/power-loss-/resourcegates uit het apply-authoritydocument en een
afzonderlijke expliciete activatiebeslissing.
