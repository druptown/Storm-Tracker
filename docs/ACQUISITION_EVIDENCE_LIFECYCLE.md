# Acquisition evidence lifecycle en completion-outbox

Status: lokale, niet-live shadowkandidaat. Dit bestand beschrijft
`storm_tracker_worker/acquisition_evidence_lifecycle.py`. De module wordt niet
door `service.py` geimporteerd en publiceert niets naar Home Assistant, MQTT of
een operationele snapshot.

## Doel en veiligheidsgrens

De acquisition runtime had duurzame raw-, capacity-, execution- en
schedulerbewijzen, maar nog geen veilige overdracht naar een latere decoder en
geen begrensd archiefvenster voor een complete bewijsbundel. De lifecycle-store
voegt daarom een afzonderlijke, content-addressed bewijs-mirror toe. Hij:

1. bewaart de exacte raw bytes en een credential-vrij bewijsdocument;
2. commit eerst een immutable producerreceipt;
3. maakt pas in een tweede transactie een completion-event `ready`;
4. levert per stream uitsluitend de eerstvolgende sequence via een CAS-lease;
5. schrijft een immutable consumptionreceipt voordat event en cursor terminal
   worden;
6. archiveert alleen terminale, cursor-past events in een hashketen;
7. verwijdert hot mirror-data alleen via een durable child-first pruneplan.

Deze mirror verandert geen providerbytes, decodeert niets, doet geen refetch en
maakt geen meteorologische claim. De originele stores blijven voorlopig hun
eigen authority en worden door deze module nooit verwijderd.

## Schema-v4 reclamationparticipant (niet live)

Schema v4 voegt een lokale
`AcquisitionEvidenceLifecycleReclamationParticipantV2` toe voor de frozen
source-reclamationprotocolgrens. Migratie vanuit schema v3 gebeurt in een
SQLite-transactie, bewaart de bestaande store-instanceidentity en valideert na
DDL zowel de volledige tabel/index/triggerinventory als de exacte trigger-SQL.
Een partial schema of gedropte, vervangen of herschreven guard faalt bij open.

`prepare_source_local_v2(...)` houdt de candidate-authority fysiek vast, leest
metadata-only paginas van maximaal 64 records en eist een complete, geordende
archiveprefix. Iedere candidate wordt opnieuw aan lifecycle stream/sequence,
terminalreceipt, archiveobject en de actuele archivefloor gebonden. De durable
prepare bevat alleen hashes, counts, bytes en een source-fence. Onder dezelfde
`BEGIN IMMEDIATE` wordt het effect al in PREPARED bevroren: exacte member- en
archive-indexrows, segmenttombstones/ketenranden, object-refcounts, filejournal,
floor-before en de verwachte floor-after. SQL-guards blokkeren vanaf PREPARED
semantische reinserts en nieuwe gedeelde referenties. Prepare wist of unlikt
niets en exact replay retourneert hetzelfde receipt.

Objectreferenties gebruiken de requestgebonden v2-identiteit. Iedere
`reference_sha256` bindt request-SHA, exact objectcatalogusrecord, target- en
retained-refcounts, deletebesluit en een afzonderlijke root over de volledige
segment/index/file/floor-effectset. De durable tabel blijft append-only: een
ABORTED request houdt zijn bewijsrows, terwijl een exact successorrequest voor
hetzelfde object een andere effect- en reference-SHA krijgt. Binnen hetzelfde
request blijven content en reference-SHA door afzonderlijke composiete
uniquenessconstraints exact eenmalig.

`apply_source_local_v1(...)` werkt uitsluitend onder een gehouden COMMITTED
decision en exact predecessorbewijs. De enige logische floortransitie gebeurt
atomair bij PREPARED -> APPLYING: de gebonden index/objectrows verdwijnen en de
counters gaan exact eenmaal van floor-before naar de immutable floor-after.
Segmentrows blijven no-delete tombstones. Voor en na iedere afzonderlijke
file-unlink worden journal, rowabsence, objectsharing, segmenttombstones en
floor opnieuw onder een lokale databasehold bewezen. APPLYING -> APPLIED
verandert de counters niet; die finale transactie schrijft receipt/state en zet
alleen de `last_apply_receipt_sha256`-head.

APPLIED load/replay herbewijst dezelfde postconditie. Oude event-, producer-,
job- en stream/sequence-identiteiten blijven duurzaam getombstoned. Een later
object met dezelfde content-SHA mag uitsluitend terugkomen met exacte
size/filename/bytes en een actuele indexreferentie boven de floor; een orphan
reinsert blijft verboden. Een crash in ieder venster wordt bij dezelfde apply
op restart voorwaarts afgerond. Reclaimed sequences leveren expliciet
`CompletionWindowExpired`, nooit `unknown`.

Iedere fileverificatie bindt bovendien regular-filetype, inodewitness en
`st_nlink == 1` voor en na de bytehash. Een vooraf bestaande hardlinkalias
blokkeert daardoor de unlink; de durable APPLYING-state, journal en SQL-fences
blijven staan en iedere restart/replay faalt gesloten totdat de alias expliciet
is verwijderd. De portable Pythonroute kan echter geen geprivilegieerde
filesystemwriter uitsluiten die na de laatste witness nog een same-filesystem
alias maakt. Live wiring blijft daarom uit totdat OS-policy de lifecycle-root
en zijn ouders exclusief aan de workerprincipal geeft en externe hardlink-
creatie uit de authoritygrens verwijdert.

De exacte machineleesbare prewiringstate is:

```json
{
  "schema": "stv3-worker-acquisition-evidence-reclamation-prewiring-v1",
  "live_wiring": false,
  "prewiring_blockers": [
    "exclusive_filesystem_owner_dirfd_policy_required_for_crash_atomic_hardlink_exclusion"
  ]
}
```

De array wordt verbatim als
`LIFECYCLE_RECLAMATION_PREWIRING_BLOCKERS` geexporteerd en bevat exact een
entry.

`abort_source_local_v1(...)` vereist een gehouden ABORTING decision en de exact
opgeslagen prepare/fence. Abort is duurzaam en mutatievrij voor lifecycledata.
Na APPLIED kan niet meer worden geabort; na ABORTED kan niet meer worden
toegepast. Alle drie paden zijn begrensd door row-, file-, byte- en databasecaps
en gebruiken de views uit `acquisition_source_reclamation_views.py` zonder
lokale protocolvariant.

Dit is alleen de downstreamparticipant. De coordinator en vier andere
participants bepalen gezamenlijk of bronrelease mag plaatsvinden. Er is geen
service-, HA-, MQTT-, live- of deploywiring vanuit deze module.

## Bounded opslagprofiel

`AcquisitionEvidenceLimits` begrenst onafhankelijk:

- objectgrootte en totale hot bytes;
- aantallen hot objects en completions;
- canonieke proofgrootte en SQLite-grootte;
- totale archivebytes, archiveobjects, segmenten en archived completions;
- completions en bytes per sealed segment.

Werkelijke bestanden op disk worden voor admission meegeteld, ook als ze nog
niet in SQLite staan. Onbekende bestanden, een onverklaarde sequencegap,
capaciteitsuitputting of corrupte staging faalt gesloten. Er is geen LRU,
automatische eviction of stilzwijgende truncatie.

## Producerreceipt voor `ready`

`publish(...)` valideert identifiers, desired-state-SHA, authority-SHA, proof en
exacte bytes. Credentialachtige keys of waarden zijn verboden. Alleen
`secret_requirement_ids` mag voorkomen en bevat uitsluitend abstracte
`worker_secret.*` requirement-ID's, nooit een waarde.

De publicatievolgorde is:

1. content-SHA berekenen en exact object fsyncen;
2. hot-objectcatalogus, global/stream sequence en producerreceipt atomair
   committen;
3. een stabiele event-ID uit het producerreceipt afleiden;
4. het outboxevent in een afzonderlijke transactie `ready` maken.

Een crash na stap 2 maar voor stap 4 laat dus bewijs zonder ready-event achter,
nooit een ready-event zonder producerbewijs. De identieke retry maakt alleen
het ontbrekende event ready. Hergebruik van dezelfde acquisition-job-ID met
andere bytes, proof, authority, consumer of stream is een conflict.

## Completion-cursor en exact-once consumption

Elke stream heeft exact een vaste consumerauthority en een monotone sequence.
`claim_next(...)`:

- expireert alleen verlopen leases;
- reconcileert eerst durable consumptionreceipts;
- slaat uitsluitend checksum-gebonden `superseded` events over;
- weigert iedere gap;
- leaset exact `cursor + 1` met een gehashte capability in SQLite.

`load_leased(...)` herverifieert proof, catalogus en exacte object-SHA voordat
bytes de consumer bereiken. `acknowledge(...)` gebruikt twee transacties:

1. immutable consumptionreceipt met event-, producer- en output-SHA;
2. terminale eventtransitie en cursor-CAS.

Een crash tussen beide transacties wordt bij restart uit het receipt
gereconcilieerd; hetzelfde event wordt niet opnieuw geleverd. Een andere
output-SHA voor hetzelfde event faalt als conflict, ook nadat het event al is
gearchiveerd.

### Duurzame afwijzing van verouderde authority

Een coordinator mag een reeds geleased event waarvan de desired-state niet
meer actief is niet met een verzonnen pipeline-outputhash terminaliseren.
Daarom biedt de store aanvullend:

```python
reject_leased(
    event_id,
    lease_token,
    *,
    expected_event_sha256,
    reason_code,
    active_desired_state_sha256,
    now=None,
) -> ConsumptionReceipt
```

De methode bindt event-SHA, producerreceipt, consumer/stream/sequence, de
oorspronkelijke desired-state-SHA, de actuele afwijkende desired-state-SHA (of
expliciet `None`) en uitsluitend een reden uit `REJECTION_REASON_CODES` in een
checksummed `RejectionReceipt`. De `output_sha256` van het consumptionreceipt
is exact de SHA-256 van dat duurzame rejectionreceipt. Rejection- en
consumptionreceipt committen atomair voor outboxstatus en cursor bewegen.

De redenen voor een changed/superseded event vereisen een actuele SHA die
verschilt van de producer-SHA. Alleen
`active_desired_state_unavailable` staat `None` toe. Een verkeerde event-SHA,
lease, reden of authority-binding faalt gesloten. Replay na elk commitvenster
levert hetzelfde receipt op; een reeds normale completion kan niet achteraf
als rejection worden herschreven. Rejectionreceipts gaan mee in sealed archive
v2, worden daar opnieuw gehasht en pas child-first vóór consumption/outbox
gepruned. Oude archive-v1-segmenten blijven read-only valideerbaar en kunnen
per definitie geen rejectionreceipt bevatten.

## Supersession

`supersede(...)` vereist de verwachte event-SHA en een reeds bestaande, latere
replacement in dezelfde stream en onder dezelfde consumerauthority. Alleen een
ongeleased `ready` event kan worden gesupersede. De supersessionreceipt bindt
oude event-ID/SHA en replacement-ID. De cursor slaat de sequence pas over nadat
dat receipt opnieuw is gevalideerd.

## Sealed archief en tweefasen-prune

`rotate_archive(...)` archiveert uitsluitend een globale, gapvrije prefix van
terminale en cursor-past completions. Een later terminal event kan niet over
een ouder `ready` of `leased` event springen.

Voor iedere completion bevat het sealed segment:

- producerreceipt en desired/authority-SHA;
- payloadvrije scheduler terminal proof;
- alle executionreceipts van de acquisition job;
- committed capacityreceipt;
- raw capture/receipt/provenance zonder lokaal artifactpad;
- DWD handoffreceipt plus target capture/receipt/provenance waar van toepassing;
- completionevent, consumption- of supersessionreceipt;
- verwijzing naar de exacte content-addressed archivebytes.

Archiveobjects worden eerst exact gekopieerd en geverifieerd. Daarna wordt een
canoniek segment geschreven, gefsynct en atomair gepubliceerd. SQLite commit
vervolgens segmentmanifest, lookup-index, archive-anchor-CAS en een pruneplan.
Pas dan begint het child-first prunepad:

1. rejectionreceipts en consumptionreceipts;
2. completion-outboxrows;
3. producerreceipts;
4. markering van niet-meer-gerefereerde hot objects als `pruning`;
5. geverifieerde hot file unlink plus directorybarrier;
6. hot-objectcatalogusrow en terminal pruneplan.

Iedere fase heeft een afzonderlijke durable commit. `resume_pending_prunes()`
herverifieert voor iedere hervatting het sealed segment en de exacte
archivebytes. Een ontbrekend of getamperd archief blokkeert de prune; er wordt
dan niets verder verwijderd.

Complete stagingfiles na een power-loss worden bij restart alleen gepromoveerd
als naam, contenthash en canonieke segmentidentiteit exact kloppen. Een
onvolledige/corrupte stage wordt niet stil verwijderd maar blokkeert voor
expliciet herstel. Op Linux is directory-fsync de vereiste durabilitybarrier.
Windows-tests injecteren een barrier-double; Windows zelf levert via deze
Pythonroute geen bewijs van fysieke directory-fsync.

## Lookupsemantiek

`lookup_sequence(stream_id, sequence, include_archive=True)` geeft expliciet:

- `hot`: proof en bytes liggen in het actieve venster;
- `archived`: sealed proof en exact archiveobject zijn volledig herladen;
- `window_expired`: de hot lookup is verlopen, met archive-segmentidentiteit;
- `unknown`: de sequence is nooit gepubliceerd.

Een sequence onder de bekende streamhead die noch hot noch archived is, is
geen `unknown` of droog resultaat maar storagecorruptie.

## Concrete DWD-binding

`publish_dwd_completion(...)` is geen generiek protocol zonder providerbewijs.
De functie bindt concreet:

- een gevalideerd acquisitionplan en `radar.dwd.radolan_rv` job;
- exact dezelfde desired-state-SHA en authority epoch/generation/producer;
- een terminal `completed` schedulerbewijs zonder payload of lease;
- alle checksum-gevalideerde executionreceipts en exact een terminal
  `completed_shadow_raw` receipt;
- de committed DWD capacityreceipt met dezelfde content-, receipt- en
  capture-SHA en werkelijke byte/entrycount;
- de `provider=dwd` raw-capture-v2 met exact rol `rv_tar`;
- de versieerbare DWD handoff naar `provider=dwd_radolan`;
- bytegelijkheid van source tar, handoff tar en lifecycle payload.

De echte `.analysis/dwd-rv-live.tar` is de vaste testfixture: 880.640 bytes,
SHA-256
`08e0ef2539dde3c40d3d35d0f8750420d5f4c902712968abdcf47ffad583d8f5`,
25 leden `+000..+120`. Alleen `+000` blijft observatiekandidaat; de forecasts
blijven afzonderlijke raw lineage. Een enkele basistijd is geen bewijs van
motion skill.

## Concrete DWD completion-consumer

`storm_tracker_worker/dwd_rv_completion_consumer.py` consumeert uitsluitend
de stream `radar.dwd.radolan_rv` onder consumer
`dwd-rv-quantitative-decoder-v1`. Hij hergebruikt de lifecycle-outbox; er is
geen tweede queue. De coordinatorcompositie gebruikt:

```python
process_leased(
    lease: CompletionLease,
    desired_state: DesiredState,
    cancel_event: threading.Event,
) -> PipelineOutcome
```

`process_leased` claimt en acknowledget niets. Hij valideert de lease en
authority en delegeert daarna uitsluitend aan de vaste
`DwdRvCompletionIsolatedRunner`. Die start via `spawn` een vers proces onder
een OS-afgedwongen CPU-, RSS-, walltime- en resultaatslimiet. Het kind opent
alleen de exact in een checksummed process-authority gebonden stores,
hergebruikt exact raw bytes zonder refetch, doorloopt de bestaande DWD
quantitative -> canonical evidence -> officieel gridcertificaat ->
cells-shadowketen en commit atomair een begrensd outputreceipt plus een apart
hard-processreceipt. Het resultaat blijft `disposition="withheld"`, zonder
staged factrevision of operationele claim.
`run_once(owner_id, desired_state, lease_seconds=...)` bestaat alleen als lokaal
standalone harnas en composeert zelf claim -> `process_leased` -> verify -> ack.

Vóór de enige lifecycle-ack gebruikt de centrale coordinator
`DwdRvPipelineOutcomeAuthority.verify(lease=..., desired_state=...,
outcome=...)`. Die adapter herlaadt het outputreceipt en bewijst event-ID/SHA,
producerreceipt, desired/authority, consumer/stream/sequence, output-SHA,
output- én processreceipt-SHA, disposition en het exacte chain-profile. Beide
receipt-SHA's zijn bewust verschillend: de eerste
bewijst het immutable outputdocument; de tweede bindt dat receipt aan de
afgedwongen isolation-authority, effectcontract en limieten. `fact_binding` is
expliciet `None`.

De onderliggende store-API waarmee de adapter ieder veld opnieuw bewijst is:

```python
verify_for_completion(
    *,
    event_id,
    event_sha256,
    desired_state_sha256,
    authority_sha256,
    consumer_id,
    stream_id,
    durable_output_receipt_sha256,
    durable_process_receipt_sha256,
    process_isolation_authority_sha256,
    output_sha256,
    disposition,
    staged_fact_revision=None,
) -> DwdRvCompletionVerification
```

Een optionele factrevision moet volledig en byte-identiek aan de immutable
output zijn gebonden. De huidige DWD-output bindt `fact_revision=None`; een
losse 64-hexwaarde kan dus niet als voldoende completionbewijs gelden.

De processrunner reconcileert het exacte durable receipt zowel voor iedere
spawn als na elk terminal childresultaat. Daardoor wordt een commit gevolgd
door power-loss, IPC-verlies, hard kill of een verkeerd child-exitresultaat
zonder netwerkrefetch als dezelfde completion herkend. Een afwijkende
event-SHA, producerreceipt, desired/authority, profile, isolation-authority of
limiet-SHA faalt gesloten. De lifecycle-outbox blijft de enige queue.

## Herstelautoriteiten voor dispatchercrashes

Twee read-only API's sluiten herstel af zonder payload of lease-token naar de
dispatcher te kopiëren:

- `durable_clock_high_water()` geeft de hoogste daadwerkelijk duurzaam
  gebruikte lifecycle-tijd terug. Toekomstige `lease_expires_at`-waarden tellen
  bewust niet mee. Een dispatcher moet deze waarde bij start samenvoegen met
  zijn eigen clock-head en klokterugval daarna gesloten weigeren.
- `terminal_event_identity(stream_id, stream_sequence)` herverifieert exact een
  hot of gearchiveerd normaal, rejected, quarantined of superseded eindpunt.
  Ook een consumptionreceipt dat vlak vóór een crash gecommit is terwijl de
  outbox nog `leased` staat, geldt als bewezen terminal. Het resultaat bevat
  alleen hashes, streamvolgnummer, terminalsoort en tijd; nooit raw bytes,
  proofpayload, eigenaar, capability of lokaal pad.

De DWD outputstore is schema-v3. Iedere database heeft een persistente
`store_instance_sha256`; output- en metadatarijen hebben exact gevalideerde
no-update/no-delete-triggers. De v2-naar-v3-migratie is één SQLite-transactie en
herverifieert bestaande receipts vóór commit. `completion_reproof_lease(...)`
houdt bovendien een `BEGIN IMMEDIATE` writerfence vanaf de laatste exacte
outputreproof tot na de lifecycle-ACK. Een trigger-drop/update/delete/recreate
kan daardoor niet meer tussen die twee databases racen.

## Bewuste resterende blockers

De eerdere raw/control-gaten zijn voor de lokale v2-grens gesloten. De
`RawArtifactStore` heeft nu een fysiek aan candidate- en decision-authority
gebonden reference-counted participant met durable unlinkjournal en terminale
tombstones. Execution, scheduler en capacity hebben ieder een native
same-database participant; hun sourceverwijdering, tombstone en APPLIED-receipt
committen atomair. Capacity reconcilieert de feiten met provider- en globale
counters en verlaagt ze exact eenmaal. Scheduler- en executionidentiteiten
kunnen na APPLIED niet opnieuw worden gematerialiseerd. Het integratiebewijs
doorloopt alle vijf rollen in de vaste volgorde en heropent/replayt APPLIED
zonder refetch of dubbel effect. Zie
`ACQUISITION_SOURCE_RECLAMATION_APPLY_AUTHORITY.md` en
`ACQUISITION_CONTROL_RECLAMATION.md`.

Dat sluit alleen de offline protocol- en storegrens. Zowel deze lifecycle-
participant als raw publiceert nog exact de blocker
`exclusive_filesystem_owner_dirfd_policy_required_for_crash_atomic_hardlink_exclusion`.
Een al bestaande hardlinkalias wordt door inode-/linkcountreproof fail-closed
geweigerd, maar portable Python kan een extern bevoegde writer niet op iedere
instructiegrens uitsluiten. Live reclamation blijft daarom verboden tot de
worker exclusief eigenaar is van de roots en parentdirectories, de doel-OS-
directorybarrier is geaudit en echte power-loss-, disk-full-, WAL/SHM-, media-
en corruptieproeven plus resource-/thermal soak groen zijn. Archivecaps stoppen
intussen expliciet met een capacity error; sealed archief wordt nooit stil
weggegooid.

Verder blijven OPERA, KMI RADQPE en alle overige niet-geactiveerde providers
ongewijzigd blocked. KNMI en METAR hebben nog geen concrete completionbinding in
deze module. De DWD-consumer heeft nu wel een geteste hard
fresh-process-wrapper, maar blijft shadow/unwired: er is geen centrale
worker-lifecyclecompositie naar `service.py`, geen science/admissionpromotie en
geen service-, MQTT-, HA-, snapshot- of live wiring. De bestaande
cells-shadow blockernaam `hard_process_isolation_not_wired` blijft conservatief
in het generieke cells-contract staan; de completionreceipt bewijst alleen deze
lokale DWD-processgrens en mag die generieke operationele admission nog niet
opheffen.
