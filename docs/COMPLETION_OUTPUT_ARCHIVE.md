# Completion-output reference archive

## Status en grens

`storm_tracker_worker.completion_output_archive` is een volledig inert en
provider-neutraal opslagcontract. De module is niet vanuit `service.py`,
`main.py`, Home Assistant, MQTT, de terminal-router, lifecycle of een
providerconsumer bedraad.

De kern doet uitsluitend het volgende:

- een gesloten bronprofielcatalogus en seal-authority valideren;
- canonical, privacy-veilige completion-outputdocumenten en bestaande
  receipthashes append-only bewaren;
- iedere record content-addressen en exact checksummen;
- een gesealde record opnieuw laden en verwachte hashes vergelijken;
- een read-only hot-windowplan berekenen.

De kern doet nadrukkelijk niet het volgende:

- raw-, evidence-, artifact- of factsetbytes kopiëren;
- een provider decoderen of opnieuw fetchen;
- een provideroutputstore lezen, wijzigen of prunen;
- lifecycle completion acknowledgen;
- meteorologische waarheid, fact publication, snapshot publication of
  admission claimen;
- geïntegreerde retentie of archive-only completionreplay claimen.

`verify_for_ack` betekent hier alleen: de callerhashes zijn exact gelijk aan
de checksummed archiefrecord. Een latere provideradapter moet daarnaast de
bestaande lease-, desired-state-, raw-, evidence-, artifact-, region- en/of
factsetauthority opnieuw bewijzen voordat lifecycle-ack mogelijk wordt.

## Publieke contracten

De centrale schema-identiteiten zijn:

```text
stv3-completion-output-reference-archive-v1
stv3-completion-output-reference-record-v1
stv3-completion-output-reference-seal-v1
stv3-completion-output-archive-source-profile-v1
stv3-completion-output-source-reproof-authority-v1
stv3-completion-output-hot-window-policy-v1
```

Een store kan alleen worden geopend met alle volgende expliciete authorities:

```python
CompletionOutputArchiveStore(
    absolute_archive_path,
    source_catalog=closed_catalog,
    source_reproof_authority=bound_reproof_authority,
    expected_source_reproof_authority_sha256=(
        bound_reproof_authority.fingerprint
    ),
    limits=explicit_limits,
    hot_policy=explicit_hot_policy,
    disjoint_paths=(source_store_path,),
)
```

Er zijn geen impliciete disk-, catalogus- of hot-windowdefaults in deze API.
Een wijziging aan catalogus, limits, hot policy of reproofauthority wijzigt de
archive-authority-SHA en maakt een bestaande DB onder de nieuwe configuratie
fail-closed.

## SourceProfile en DWD-verschil

`CompletionOutputArchiveSourceProfile` bindt exact:

- workload-, consumer- en stream-ID;
- acquisitioncadans en catalogus-/consumerbindinghashes;
- source-storecontract en source-schemaversie;
- outputdocument-, outputreceipt- en processreceipt-schema;
- source-limits- en effectcontracthash;
- source-row-reproofcontract;
- source-immutabilitymode.

De gesloten immutabilitymodes zijn:

```text
checksum_reproof_required
append_only_sqlite_triggers
```

Dat verschil is materieel. `checksum_reproof_required` blijft beschikbaar voor
een expliciet gebonden legacybron zonder immutable triggers. De huidige DWD
v3-, KNMI-, NOAA-, Met Office-, METAR- en raw-manifestauditstores hebben een
exacte no-update/no-delete-triggerauthority en worden door hun adapters als
`append_only_sqlite_triggers` geregistreerd. Beide modes vereisen nog steeds
een seal-time source-rowreproof; triggers alleen zijn geen substituut voor
checksumverificatie.

De archiefkern bevat nog geen concrete profielen. Een later, atomair
gereviewde provideradapter moet de bestaande providercontracten in een
expliciete `CompletionOutputArchiveSourceCatalog` aanleveren.

## Constructor-bound source reproof

Een caller kan geen proofcallback aan `seal()` meegeven. De resolverauthority
wordt eenmalig bij constructie gebonden:

```text
CompletionOutputArchiveSourceReproofAuthority
  -> exact één binding per source-profile-SHA
  -> resolver-ID
  -> resolver-code-SHA
  -> source-store-identity-SHA
  -> reproofcontract
  -> lokale resolverimplementatie
```

De outer caller moet de verwachte authority-SHA apart doorgeven. Die SHA wordt
in de archiefmetadata én indirect in elke seal gebonden. Reopen met een andere
resolvercatalogus faalt.

Een nieuwe seal volgt deze volgorde:

1. Archive schema, catalogus, limits, hot policy en resolverauthority valideren.
2. Onder `BEGIN IMMEDIATE` controleren of dezelfde source-profile/eventkey al
   bestaat.
3. Een bestaande exact gelijke record idempotent teruggeven zonder de source
   opnieuw nodig te hebben.
4. Voor een nieuwe key de constructor-bound reproofcontext openen.
5. De huidige volledige canonical sourcerow opnieuw hashen.
6. Source-profile, event, sourcerow-SHA, sourcerowbytes en reproofcontract exact
   met de geplande record vergelijken.
7. Alleen zolang die reproofcontext geldig blijft de archiefrow inserten en
   `FULL` committen.

Een directe SQL-mutatie van een onbeschermde sourcerow tussen planning en seal
levert daardoor een conflict op voordat een geldig archivehead bestaat. De
archive record bewaart alleen `source_row_sha256` en `source_row_bytes`; zij
kopieert de sourcerow niet.

## Record en seal

Een `CompletionOutputArchiveRecord` bevat:

- source-profile-SHA;
- event-, producerreceipt-, workload-, consumer- en streamidentiteit;
- stream sequence en oorspronkelijke `created_at`;
- output-, outputreceipt- en processreceipthash;
- sourcerow-SHA en sourcerowbytes;
- het volledige canonical completion-outputdocument.

Het outputdocument moet canonical ASCII JSON zijn. Paden, URL's, URI's,
credentials, tokens, authorizationvelden, raw payloads en raw bytes zijn
verboden. Alleen de twee exacte booleaanse auditasserties
`raw_bytes_verified=True` en `raw_bytes_copied=False` zijn toegestaan; ze
bevatten geen bytes en bewijzen respectievelijk een begrensde verificatie en
de afwezigheid van een kopie. Opposite waarden, strings, containers, aliases en
alle andere raw-bytes/raw-payloadvelden blijven fail-closed. Record- en
diagnosticsresultaten bevatten geen lokale storepaden.

```text
record_sha256 = SHA256(canonical_record_json)
archive_id    = "completion-output-" + record_sha256
seal_sha256   = SHA256(canonical_seal_body)
```

De sealbody bindt archive-authority-SHA, archive-ID, source-profile-SHA,
event-ID, record-SHA, recordbytes en de eerste duurzame `archived_at`.
Identieke replay behoudt die eerste timestamp. Dezelfde source-profile/eventkey
met andere canonical bytes is een conflict.

`load(source_profile_sha256, event_id)` verifieert eerst de volledige DB- en
authoritystructuur en daarna alle redundante kolommen, canonical bytes,
recordhash, content-ID en sealhash. Het resultaat is een
`CompletionOutputArchiveEntry(record, seal)`.

`verify_for_ack(...)` vergelijkt aanvullend exact:

- output-SHA;
- outputreceipt-SHA;
- processreceipt-SHA;
- optioneel archive-record-SHA.

## SQLite authority en caps

De store gebruikt:

```text
PRAGMA synchronous=FULL
PRAGMA journal_mode=DELETE
PRAGMA max_page_count=<max_database_bytes / page_size>
```

De limits vereisen expliciet:

- `max_records`;
- `max_record_bytes`;
- `max_total_record_bytes`;
- `max_database_bytes`;
- `busy_timeout_ms`.

De schema-inventory is exact en gesloten:

```text
archive_metadata
archive_records
archive_records_source_event
archive_records_record_sha256
archive_metadata_no_update
archive_metadata_no_delete
archive_records_no_update
archive_records_no_delete
```

Metadata en records zijn fysiek no-update/no-delete. De module bevat geen
DELETE-statement en geen triggerescape. Extra/missende tabellen, indexes of
triggers, gewijzigde trigger-SQL, metadata drift, niet-canonical JSON en
gewijzigde row- of sealhashes falen gesloten.

Count-, per-record-, total-recordbyte- en SQLite-pagecaps worden onafhankelijk
afgedwongen. Een volle archive stopt nieuwe seals; er is geen automatische
rotatie of verwijdering.

## Crash- en concurrencysemantiek

- Crash/exception vóór commit: SQLite rollback; er bestaat geen archivehead.
- Crash na commit maar vóór callerresultaat: de seal is duurzaam en een retry
  retourneert exact dezelfde seal.
- Gelijktijdige threads of processen met dezelfde record: één insert, alle
  callers zien dezelfde seal.
- Gelijktijdige verschillende bytes voor dezelfde source/eventkey: één
  content-addressed winnaar, conflict voor de andere.
- Een teruglopende archiefclock mag geen nieuwe head schrijven. Een exacte
  idempotente replay blijft wel leesbaar en behoudt haar eerste timestamp.

## Read-only hot-windowplanner

`CompletionOutputHotWindowPolicy` bevat expliciet:

- `hot_window_seconds`;
- `safety_records_per_stream`;
- `max_plan_records`.

Het minimum per sourceprofiel is:

```text
ceil(hot_window_seconds / cadence_seconds) + safety_records_per_stream
```

`plan_hot_window(...)` groepeert bounded lightweight candidates per exact
source-profile-SHA. De nieuwste minimumset blijft altijd retained. Oudere rows
zijn pas `eligible_for_archive` als ook hun `created_at` buiten de wall-clock
window valt. Een candidate uit de toekomst zet
`clock_rollback_detected=true` en blijft retained.

De planner weigert onbekende profielen, duplicate events, duplicate stream
sequences en een candidatecount boven de policycap. Hij retourneert alleen een
checksummed plan en muteert geen archive- of providerstore. Met name
`eligible_for_archive` betekent niet `source_reclaim_authorized` en is nooit op
zichzelf prunebewijs.

## Cadencehorizon

`calculate_cadence_horizon(catalog, limits)` berekent voor de expliciete
catalogus:

```text
records_per_day = sum(86400 / cadence_seconds)
count_cap_days  = max_records / records_per_day
byte_cap_days   = (max_total_record_bytes / max_record_bytes)
                  / records_per_day
logical_days    = min(count_cap_days, byte_cap_days)
```

Dit is een conservatieve logische horizon bij records van de maximaal
toegestane grootte. SQLite-pageoverhead kan de fysieke horizon verder
verkorten; de functie beweert daarom geen page-capduur.

## Testauthority

`tests/test_worker_completion_output_archive.py` bewijst onder `-W error`:

- content-addressing, exact load en ackhashverificatie;
- idempotentie en same-eventconflict;
- catalogus-, profiel-, limits-, policy- en resolverauthoritydrift;
- checksumreproof van een expliciet als mutable geprofileerde testbron vóór seal;
- afwijzing van een per-call forged proof;
- record-, count-, totalbyte- en SQLite-pagecaps;
- exacte schema-/index-/triggerinventaris en row/sealtamper;
- exception- en echte processcrash vóór/na commit;
- thread- en two-process-CAS;
- clock rollback;
- privacy-, absolute-path-, symlink- en aliasgrenzen;
- 5-/10-minutencadans, hot-windowplanning en caphorizon.

Deze tests creëren alleen tijdelijke lokale authorities. Ze bedraden geen
provider, lifecycle, Home Assistant, MQTT of netwerktoegang.
