# Canonical Ground Evidence Store — kandidaatcontract v1

## Status en afbakening

`storm_tracker_worker/canonical_ground_evidence_store.py` is een geïsoleerde,
lokale kandidaat voor roadmapstap 5. De module is **niet** gekoppeld aan de
worker-service, providers, Home Assistant, MQTT, dashboards of deployment. De
operationele toelating staat hard dicht:

- `operational_admission_eligible` moet letterlijk `false` zijn;
- `admission_status` moet `candidate_only_unadmitted` zijn;
- de SQLite-manifest legt dezelfde toestand append-only vast;
- onbekende contractvelden worden geweigerd;
- er bestaat geen API voor regen/droog, nat station, grondbevestiging,
  vertrouwen, forecast of waarschuwing.

Dit is bewust een afzonderlijke ground-store. Het radarcontract voor rasters en
radarcycli heeft andere tijd-, ruimte- en kwaliteitssemantiek en wordt niet
hergebruikt voor puntwaarnemingen.

## Meteorologisch feitenniveau

Een rij betekent uitsluitend:

> Deze exacte provider/product-decoder heeft uit dit exacte record in deze
> complete schema-3 raw-capture dit station-, tijd-, variabele-, waarde- en
> provider-QC-feit gelezen.

Een rij betekent uitdrukkelijk niet dat de waarde betrouwbaar genoeg is voor
fusie, dat neerslag aanwezig/afwezig is, dat een radarbeeld bevestigd is of dat
een voorspelling operationeel mag worden gebruikt. Zulke interpretaties horen
in latere, afzonderlijk geadmitteerde lagen met expliciete regels en bewijs.

## Enige bronautoriteit

De constructor vereist een `RawArtifactCaptureReader`. Voor `commit(single)` en
elke load wordt `load_capture_receipt(capture_id)` eenmaal aangeroepen. Voor
`commit_batch(facts)` wordt de raw authority exact eenmaal voor de volledige
batch geladen, niet eenmaal per fact. De store accepteert
uitsluitend een complete capture met:

- raw-store schema-versie 3;
- exact capture- en capture-receipt-schema;
- geldige `capture_id`, manifest-SHA-256 en capture-receipt-SHA-256;
- een volledig begrensde, unieke lijst van 1–64 leden;
- per lid exact `role/member_id`, raw receipt-ID, content-SHA-256,
  receipt-SHA-256 en `received_at`;
- een zelf herberekende capture-receipt-SHA-256 die overeenkomt met de raw
  authority;
- exact dezelfde provider, product, capture en leden als in het ground-feit.

De geselecteerde `source.member_role` moet in die volledige capture voorkomen.
`time.received_at` moet exact de receipt-tijd van dat geselecteerde lid zijn.
`time.ingested_at` mag niet vóór enig lid van de complete capture liggen. Een
ontbrekende, corrupte, incomplete of onbereikbare raw authority geeft een
getypeerde fout en nooit een weerstatus.

De ground-store kopieert geen payload en bewaart geen bestandspaden.
`source.member_content_sha256` en `source.member_receipt_sha256` moeten exact
overeenkomen met het geselecteerde, onafhankelijk raw-geverifieerde lid. De
`source.record_locator` is uitsluitend een begrensde, opaque locator *binnen*
het upstreamlid; `source.record_sha256` bindt de exacte door de decoder gekozen
bronrecordrepresentatie. Die subrecorddigest is een expliciete decoderclaim en
wordt pas opnieuw inhoudelijk bewezen wanneer dezelfde decoder de geverifieerde
raw bytes replayt; deze generieke store doet zelf geen provider-specifieke
recordparsing. De volledige member/capture- en receipthashes zijn wel op iedere
read onafhankelijk verifieerbaar. De raw bytes blijven uitsluitend in de
schema-3 raw-store.

## Station en site

Het contract bewaart provider-scoped en canonical-site-identiteit apart:

- `provider_station_id`: exacte stationsidentiteit van de provider;
- `site_id`: expliciete identiteit waarmee latere lagen dezelfde fysieke site
  kunnen herkennen;
- `network` en optionele originele `station_name`;
- positie met toestand `known` of `unknown`;
- hoogte met een afzonderlijke toestand `known` of `unknown`.

Bekende coördinaten en hoogte zijn exacte decimale tekst, niet afgeronde binary
floatmetadata. `unknown` vereist `null`; een fictieve `0,0`-positie of hoogte
wordt niet toegestaan. Lengte, Unicode/control-karakters, numerieke eindigheid
en geldige breedte-/lengtegraden worden begrensd.

## Vijf onafhankelijke tijdassen

De volgende tijden worden materieel in afzonderlijke SQLite-kolommen en in de
gehashte canonical JSON vastgelegd:

1. `observed_at`: het waarnemingstijdstip dat bij het providerrecord hoort;
2. `representative_interval_start`: begin van het interval dat de meting
   vertegenwoordigt;
3. `representative_interval_end`: einde van dat interval;
4. `received_at`: exacte ontvangsttijd van het geselecteerde raw lid;
5. `ingested_at`: tijd waarop de decoder de complete capture als input had.

Daarboven heeft de receipt een zesde, lokale `committed_at`. De store vereist
`interval_start <= observed_at <= interval_end`, `received_at <= ingested_at`
en `ingested_at <= committed_at`. De store vervangt, verschuift, rondt of
"corrigeert" geen providertijd. Er is bewust geen impliciete aanname dat een
providerklok nooit voorloopt op de ontvangstklok; een latere QC-laag kan
klokafwijkingen beoordelen zonder het oorspronkelijke feit te verliezen.

## Exacte waarde- en ontbrekendheidsemantiek

Een meting heeft altijd een exacte `variable`, `unit` en expliciete toestand.
Er bestaan zes toestanden:

| Toestand | Representatie | Exacte waarde | Censorgrens |
|---|---|---|---|
| `observed` | `decimal` of `text` | verplicht | verboden |
| `missing` | `none` | `null` | `null` |
| `trace` | `none` | `null` | `null` |
| `unknown` | `none` | `null` | `null` |
| `censored_below` | `none` | `null` | verplicht exact decimaal |
| `censored_above` | `none` | `null` | verplicht exact decimaal |

Decimale waarden worden als gevalideerde, eindige tekst bewaard. Daardoor
blijft bijvoorbeeld `0.10` exact `0.10` en wordt dit niet stilzwijgend `0.1`.
Categorische providerwaarden kunnen als begrensde exacte tekst worden bewaard.
`trace` wordt niet omgezet naar nul. `missing` en `unknown` worden niet tot
droog geïnterpreteerd. Een gecensureerde waarde wordt niet als de censorgrens
zelf voorgesteld.

## Oorspronkelijke provider-QC

`provider_qc` is een geordende lijst van maximaal 64 feiten met exact:

- `flag`;
- optionele originele `value` (ook een lege string blijft behouden);
- optionele originele beschrijvende `text`.

De volgorde en Unicode-tekst zijn deel van de canonical fact hash. De laag
neemt geen QC-besluit en verandert provider-QC niet in goed/fout, bruikbaar of
onbruikbaar. Alle velden en de totale canonical operatie hebben harde
tekst-/bytegrenzen.

## Identiteiten, idempotentie en conflicten

De store berekent drie onafhankelijke bindingen:

- `fact_sha256`: SHA-256 van het volledige canonical factdocument;
- `observation_key`: SHA-256-identiteit van capture, bronrecord, decoder/config,
  provider/product, station/site, variabele en representatieve tijd;
- `fact_id`: SHA-256-binding van `observation_key` en `fact_sha256`.

Een exacte retry met dezelfde `observation_key`, canonical bytes en hashes
retourneert dezelfde bestaande receipt. Dezelfde observation identity met een
gewijzigde waarde, QC, ontvangst- of ingesttijd geeft
`CanonicalGroundEvidenceConflict`; er wordt niets geschreven. Een expliciet
andere decoder-versie/config krijgt een andere observation identity, zodat
shadowvergelijking mogelijk blijft zonder een vorige kandidaat te muteren.

`commit_batch()` accepteert uitsluitend een concrete, begrensde `Sequence`.
Alle facts moeten exact dezelfde capture-ID, capture-receipt, provider, product
en geordende complete ledenbinding dragen. Vóór de eerste databasewrite worden
de volledige batch geparsed, alle canonical bytes geteld, duplicaten en
conflicten gecontroleerd en ieder fact intern tegen één onafhankelijk
geverifieerde capture-authority gebonden. Exact dubbele inputs zijn toegestaan:
zij krijgen op hun oorspronkelijke positie dezelfde receipt terug. Een mix van
reeds bestaande exacte facts en nieuwe facts is veilig en atomair; ieder
conflict weigert de volledige nieuwe batch.

`receipt_sha256` bindt fact-ID/hash, observation key, raw capture receipt,
capturemanifest en commit-tijd. Iedere load herberekent canonical JSON,
materialized kolommen, lid- en QC-tabellen, fact-/observation-/receipt-hashes en
controleert daarna opnieuw de complete raw authority.

## SQLite-duurzaamheid en append-only gedrag

De catalogus gebruikt:

- één `BEGIN IMMEDIATE`-transactie voor alle nieuwe facts van één batch, al hun
  captureleden en alle QC-rijen;
- `journal_mode=DELETE`, `synchronous=FULL`, foreign keys en
  `temp_store=MEMORY`;
- een geverifieerde schemafingerprint en exact append-only manifest;
- `BEFORE UPDATE`- en `BEFORE DELETE`-triggers op alle tabellen;
- `PRAGMA quick_check(1)` en `foreign_key_check` bij volledige audit;
- rollback van alle nieuwe batchfacts bij iedere exception vóór commit;
- een stabiele exacte retry na een crash direct na commit.

Subprocess-power-cut-tests beëindigen het proces met `os._exit` na de fact-,
member-, QC-, mid-batch- en precommitgrenzen. Heropenen toont geen partiële
batchrijen. Een power-cut na de ene SQLite-commit levert bij retry alle
oorspronkelijke receipts in dezelfde volgorde op. Ook eerste schema-aanmaak
wordt transactioneel getest.

Een exact-retrypad mag niet alleen de zichtbare SQLite-rij teruggeven: na een
crash tussen SQLite-commit en directory-fsync voert ook een volledig bestaande
exacte batchretry verplicht precies één bestandsenvelop- en
directory-fsyncbarrière uit vóór receipts worden geretourneerd. Nieuwe,
gemengde en volledig bestaande batches gebruiken ieder één durability barrier.
Een geforceerde barrierfout laat reeds gecommitteerde rijen intact maar weigert de
return, zodat een volgende idempotente retry de duurzaamheid opnieuw kan
voltooien.

WAL is hier bewust verboden. `max_page_count`, `wal_autocheckpoint` en
`journal_size_limit` bewijzen geen harde fysieke bovengrens wanneer een reader
een oud WAL-frame gepind houdt. Daarom accepteert de store geen `-wal`- of
`-shm`-sidecar, ook geen lege. DELETE rollback heeft één expliciet tijdelijk
`-journal`-bestand. Voor en na iedere open/duurzaamheidsbarrière worden main en
journal met `lstat` als niet-symlink reguliere bestanden gecontroleerd en samen
tegen de store-envelope geteld.

Op POSIX wordt de catalogusdirectory bij layoutcreatie en na iedere duurzame
nieuwe commit expliciet geopend en ge-fsynct. Open-, fsync- en closefouten worden
getypeerd doorgegeven en nooit genegeerd. Een fout ná de SQLite-commit laat een
ambigu maar veilig idempotent retrypunt achter: exact heropenen levert dezelfde
receipt. Python/Windows biedt in deze implementatie geen betrouwbare directory-
fsync; dat is expliciet zichtbaar als `directory_fsync_supported=0` en wordt
niet voorgesteld als uitgevoerde fsync.

## Immutable store-bound limietprofiel

`GroundEvidenceLimits` is een frozen dataclass. De volledige canonical mapping
wordt bij eerste initialisatie append-only opgeslagen als
`limits_profile_json`, met een onafhankelijke `limits_profile_sha256`. Het
profiel bindt:

- alle rij-, query-, operatie-, batch- en busy-timeoutgrenzen, inclusief
  `max_batch_facts` en `max_batch_canonical_bytes`;
- de totale fysieke `max_catalog_bytes`;
- DELETE journaling en de main-database-reserveverhouding;
- het POSIX-verplicht/Windows-niet-beschikbaar directory-fsyncbeleid.

Iedere volgende instantie moet byte-identiek hetzelfde profiel aanbieden. Ook
een strengere lokale grens mag niet stil op dezelfde database worden toegepast:
iedere drift faalt vóór gebruik. Daardoor kan geen tweede proces de page cap of
query/row-semantiek onder een andere configuratie openen.

## Harde capaciteit en begrensde queries

`GroundEvidenceLimits` begrenst minimaal:

- het expliciete main+DELETE-journal-bestandsenvelop via
  `max_catalog_bytes` (standaard 256 MiB);
- de main database via `PRAGMA max_page_count` tot maximaal een kwart van die
  envelope (standaard maximaal 64 MiB); de overige drie kwart blijft een ruime
  rollback-journal- en metadatareserve;
- totaal aantal facts, capture-memberlinks en QC-rijen;
- canonical factbytes (globale bovengrens 64 KiB);
- facts per batch (standaard en harde bovengrens 64.000) en totale canonical
  batchbytes (standaard en harde bovengrens 64 MiB);
- querypagina's (standaard maximaal 1000 receipts);
- SQLite busy-wachttijd (standaard maximaal 5 seconden).

De parser begrenst daarnaast ieder tekstveld, decimalen, timestamps, maximaal
64 captureleden en maximaal 64 QC-flags. Een nieuwe batch controleert de
geaggregeerde fact-, member- en QC-capaciteit vóór de eerste insert en rolt
volledig terug bij `SQLITE_FULL`.
Exacte retries blijven mogelijk wanneer de rijgrens bereikt is, omdat de
bestaande identiteit vóór de capaciteitscontrole wordt opgezocht.

`query()` vereist een positieve harde `limit`, gebruikt alleen een vaste set
SQL-kolommen en retourneert voor ieder resultaat een opnieuw raw-geverifieerde
receipt. `audit()` leest in begrensde batches en het totaal is zelf begrensd
door `max_observations`; er wordt geen onbeperkte resultset in RAM opgebouwd.

## Deadlinecontract

Commit, batchcommit, load, query, stats en audit accepteren een absolute monotone
`deadline_at`. De store controleert die deadline vóór en na de raw authority,
tussen batchfacts, op transactiestappen en per audit/queryresultaat. SQLite busy timeout wordt tot
de resterende tijd beperkt en een progress handler onderbreekt langere SQL.
Deadline-overschrijding is een getypeerde fout en nooit een observatie.

Dit is coöperatieve begrenzing: een defecte externe raw-reader die intern niet
terugkeert kan door Python niet veilig worden onderbroken. Voor een operationele
toelating blijft daarom een afzonderlijke hard begrensde child-process/RSS/CPU-
isolatielaag verplicht. Die toelating is in deze kandidaat niet aanwezig.

## Testbewijs

De gerichte suite staat in
`tests/test_worker_canonical_ground_evidence_store.py` met de crashhelper in
`tests/helpers/canonical_ground_evidence_crash_child.py`. Zij dekt onder meer:

- alle zes waarde-/missing-/trace-/censor-toestanden en contradicties;
- behoud van exacte decimalen en originele QC-tekst;
- positie-unknown zonder fictieve nulcoördinaten;
- onafhankelijke tijdassen en exacte raw receipt-tijd;
- complete schema-3 capture-, provider-, product-, lid- en hashbinding;
- idempotentie, conflict, append-only en interne tamperdetectie;
- rij-, member-, QC-, operatie-, database- en querygrenzen;
- immutable gehashte limits-profile en drift van ieder afzonderlijk veld;
- echte `SQLITE_FULL`-rollback en bestand-envelopecontrole;
- atomische batches met één raw reread, ordered receipts, exacte duplicaten,
  gemengde exact-bestaand/nieuw retries en preflight-conflicten;
- batchlimieten vóór raw/DB, geaggregeerde capaciteit, mid-batch deadline en
  faultrollback, concurrerende exacte retries en echte subprocess-power-cuts;
- een adversarial gepinde reader zonder WAL/SHM-groei;
- POSIX directory-fsync, foutpropagatie en de expliciete Windows-beperking;
- deadline vóór en tijdens raw-verificatie;
- exceptionrollback en echte subprocess-power-cuts;
- repositorybrede isolatie van service en Home Assistant.

De kandidaat mag pas naar decoderwiring of fusie wanneer afzonderlijke
meteorologische validatie, harde processisolatie, replay/soak en expliciete
admission gates zijn doorlopen.

### Lokaal verificatieresultaat (2 augustus 2026)

- gerichte ground-suite: **64 passed**;
- gecombineerd met de bestaande schema-3 raw-artifactstore, canonical
  radar-decoder-evidence en repositorybrede quantitative-candidate-isolatie:
  **156 passed, 1 skipped**;
- `py_compile` voor module, tests en subprocesshelper: geslaagd;
- `git diff --check` voor alle vier nieuwe bestanden: geslaagd.

Dit bewijs is uitsluitend shadow/lokaal. Er is niets gedeployed, herstart of
operationeel geadmitteerd.
