# Science-shadow checkpoint en retentiecontract v2

## Afbakening en status

`storm_tracker_worker/science_shadow_orchestrator.py` koppelt uitsluitend in
shadowmodus de bestaande gecertificeerde/candidate bouwstenen:

`canonical_radar_field_bridge -> analysis_grid_transform ->`
`evidence_to_cells_shadow -> CellTrackEngine`.

De module is niet geïmporteerd of geactiveerd vanuit `service.py`, Home
Assistant, MQTT of een operationele snapshotbuilder. De opgeslagen uitkomst is
`shadow_candidate_unadmitted`. Ze is dus geen live-readinessclaim en geen
meteorologische admission.

## Duurzame eenheid

Een succesvolle state-overgang schrijft in één SQLite
`BEGIN IMMEDIATE`-transactie:

1. het immutable, content-derived advanced receipt;
2. de exacte genormaliseerde `NormalizedRasterFrame` die tail-replay mogelijk
   maakt;
3. de CAS state-head met revision, advanced receipt-SHA en cumulatieve
   provenance-chain-SHA;
4. op de vaste checkpointcadans een expliciet geserialiseerde
   `CellTrackEngineState`;
5. de bounded expired-membershipfilter;
6. pas daarna de input- en receiptpruning die door dat checkpoint gedekt is.

`PRAGMA synchronous=FULL` en rollback-journalmodus `DELETE` zijn verplicht.
Ieder geïnjecteerd faultpoint vóór commit rolt alle zes delen terug. Daardoor
is de zichtbare toestand na een crash volledig oud of volledig nieuw, nooit
een receipt zonder input, een head zonder state, of pruning vóór dekking.
`VACUUM` staat niet in het hot path.

## Niet-uitvoerbare serialisatie

Checkpoint en tail-input gebruiken canonical ASCII JSON en zlib-compressie.
Er is geen pickle, module-import uit data, classname-resolutie of andere
uitvoerbare serialisatie. De decoder accepteert alleen een gesloten registry
van de bestaande CellTrack-, detector-, grid-, estimator- en WeatherSystem-
dataclasses. Type-, veld-, diepte-, string-, integer-, byte- en decompression-
bounds zijn expliciet. Onbekende velden of types falen gesloten.

Het checkpoint bindt exact:

- engine-ID en volledig runtime-profilefingerprint;
- CellTrack policyfingerprint en state-revision;
- advanced receipt-SHA en SHA van de gekopieerde canonical receiptbytes;
- cumulatieve provenance-chain-SHA;
- SHA van de canonical statebytes;
- SHA en exacte lengte van de compressed statebytes;
- de SHA van het vorige checkpoint.

Het retained-inputrecord bindt exact receipt-SHA, provenance-SHA,
detection-frame-SHA en de genormaliseerde framebytes.

## Restart

Een verse runtime:

1. valideert schema/profile, retentionchecksum, alle behouden receipts en de
   checkpointchecksums;
2. laadt het nieuwste checkpoint;
3. vereist daarna precies één contiguous advanced receipt/inputpaar per
   revision tot aan de durable head;
4. replayt alleen die tail door dezelfde `advance_cell_track_engine()`;
5. vergelijkt transition-ID, detection-frame-SHA, lineage, track/lifecycle-
   accounting en state-revision met het receipt.

Een gap, ontbrekende input, checksumwijziging, receipt/state/head-mismatch,
policy drift of provenance-chainbreuk stopt recovery. De canonical composer of
het netwerk wordt niet opnieuw aangeroepen voor reeds gecommitteerde frames.

## Concrete standaardgrenzen

- Checkpointcadans: iedere **32** advanced frames.
- Maximale restart-tail: **31** frames in normale werking; de afzonderlijke
  hard guard `max_replay_frames=512` blijft fail-closed bij databasecorruptie
  of configuratiedrift.
- Exact behouden receiptvenster: maximaal **2.048** receiptrows. Alle
  uncheckpointed advanced tailreceipts zijn beschermd; oudere gedekte rows
  worden oldest-first gepruned.
- Database: **64 MiB**; één checkpoint maximaal **16 MiB** canonical én
  compressed; één retained input maximaal **16 MiB** canonical én compressed.
- Expired-membershipfilter: **1 MiB**, zeven hashposities en maximaal
  **150.000** memberships. De configuratie weigert een theoretische
  false-positive bovengrens groter dan `1e-6`.

Een gepruned non-advanced receipt gebruikt één membership; een gepruned
advanced receipt gebruikt twee memberships (attempt plus canonical run). Met
uitsluitend advanced frames is de standaard dus minstens 75.000 geprunde
frames voordat de store expliciet fail-closed stopt. Bij vijfminutenframes is
dat ongeveer 260 dagen na het actuele receiptvenster. Dit is een concrete
retentiegrens, geen onbeperkte duurclaim.

Binnen het behouden receiptvenster is retry exact idempotent. Voor een
herkende identity buiten het venster is de uitkomst expliciet
`ScienceShadowWindowExpired.code == "window_expired"`. Een Bloom false
positive kan alleen een nieuwe identity conservatief blokkeren; hij kan geen
state muteren of een bestaande retained receipt veranderen.

## Privacy

Receipts bevatten uitsluitend IDs, hashes, counts, lineage en admissionstatus.
Tail-input en checkpoint bevatten noodzakelijk de genormaliseerde fysieke
rasterwaarden en afgeleide CellTrackstate, maar nooit raw artifacts,
providerresponsemetadata, headers, URLs, credentials of vrije foutdetails.
Frame-identifiers zijn bounded en secret-achtige labels (`authorization`, API
key, access token, secret, password) worden vóór persistence als expliciet
invalid receipted. Alle payloads blijven onder caller-owned bytebounds.

## Bewezen en nog geblokkeerd

De tests bewijzen happy path, unsupported/withheld/invalid/indeterminate,
idempotency, restart, checkpoint/tail, elke transactionele faultpositie,
tamper/gap/checksum/lineage mismatch, CAS-concurrency, privacy, retentie-expiry,
lifetime-capacity en een 2.100-frame opslagplateau ruim voorbij de voormalige
512-frame levensduurgrens.

De volgende blockers blijven bewust in ieder receipt:

- `hard_process_isolation_not_wired`;
- `tracking_deadline_not_preemptive_without_process_isolation`;
- `operational_admission_certificates_incomplete`;
- `operational_snapshot_publication_forbidden`.

Preëmptieve hard-kill trackingdeadlines horen bij process-isolation en vallen
buiten dit opslagcontract. De afzonderlijke, nog ongewirede
`ScienceShadowIsolatedRunner` voert die technische hard-killgrens inmiddels
wel uit en reconcilieert iedere terminalstatus opnieuw met deze receiptstore;
zie `docs/SCIENCE_SHADOW_PROCESS_ISOLATION.md`. De blockers blijven bewust in
de onderliggende receipt staan omdat die runner niet live is gewired, geen
procescertificaat in de receipt terugschrijft en geen wetenschappelijke
admission verleent. Zolang de operationele wiring en admissioncertificaten
ontbreken, blijft deze volledige keten shadow-only.
